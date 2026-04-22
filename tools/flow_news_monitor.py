"""
Unusual Flow + News Sentiment Integrator

Combines two independent signals:
  1. Unusual stock volume / price action (institutional footprint proxy)
  2. Fresh news / sentiment (what's driving it)

Logic:
  For each ticker in the watchlist or universe:
    a. Check for unusual volume signal (INSTITUTIONAL_BUY/SELL, ACCUMULATION, etc.)
    b. If flagged, pull last 48h of news
    c. Classify news direction via existing analysis/news_drift classifier
    d. Output a combined conviction signal:
       - unusual_buy + bullish_news   -> HIGH CONVICTION CALL
       - unusual_sell + bearish_news  -> HIGH CONVICTION PUT
       - unusual volume, no news      -> INVESTIGATE (someone knows something)
       - conflicting signals          -> SKIP (noise or already-priced-in)

Why this matters:
  - Volume alone -> noisy. Lots of unusual volume is just end-of-day rebalancing.
  - News alone -> already priced in by the time you read it.
  - Combination -> you see institutional money moving BEFORE the headline fully
    propagates. That's the window where an edge exists.

Safe on any failure. Writes results to snapshots/flow_news/, updates
daily_summary, and fires email alerts for HIGH_CONVICTION signals only.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data.blocks import get_unusual_volume
from data.news import get_news_since
from analysis.news_drift import classify_article

# Tickers to monitor — kept small for speed
DEFAULT_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "NVDA", "META", "AMZN", "TSLA", "NFLX",
    "AMD", "INTC", "JPM", "BAC", "GS", "V", "MA",
    "SPY", "QQQ", "DIA", "IWM",
    "MSTR", "COIN", "PLTR", "RIOT", "MARA",
    "XOM", "CVX", "COP", "OXY", "SLB",
    "NKE", "SLV", "VXX",  # from current watchlist
]

# Combined signal thresholds
HIGH_CONVICTION = "HIGH_CONVICTION"
MODERATE = "MODERATE"
INVESTIGATE = "INVESTIGATE"
CONFLICTED = "CONFLICTED"
SKIP = "SKIP"

NEWS_LOOKBACK_HOURS = 48
OUTPUT_DIR = REPO_ROOT / "snapshots" / "flow_news"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _classify_news_direction(articles: list[dict]) -> tuple[str, float, list[dict]]:
    """
    Return (overall_direction, confidence, classified_articles).

    overall_direction: 'bullish' | 'bearish' | 'neutral'
    confidence: 0..1 based on article count + classification strength
    """
    if not articles:
        return "neutral", 0.0, []

    classified = []
    bull_count = 0
    bear_count = 0

    for art in articles:
        title = (art.get("title") or "").strip()
        summary = (art.get("summary") or "").strip()
        if not title:
            continue

        cat, direction = classify_article(title, summary)

        # Also use numeric sentiment when available
        sent = art.get("sentiment")
        if isinstance(sent, (int, float)):
            if sent > 0.3:
                direction = direction or "bullish"
            elif sent < -0.3:
                direction = direction or "bearish"

        if direction == "bullish":
            bull_count += 1
        elif direction == "bearish":
            bear_count += 1

        classified.append({
            "title": title[:120],
            "link": art.get("link", ""),
            "published": art.get("published"),
            "source": art.get("source", "rss"),
            "classified_category": cat,
            "classified_direction": direction,
            "numeric_sentiment": sent,
        })

    total = bull_count + bear_count
    if total == 0:
        return "neutral", 0.0, classified

    if bull_count > bear_count * 2:
        overall = "bullish"
    elif bear_count > bull_count * 2:
        overall = "bearish"
    else:
        overall = "neutral"

    # Confidence scales with sample size + dominance
    dominance = abs(bull_count - bear_count) / total
    sample_boost = min(total / 5, 1.0)  # caps at 5 articles
    confidence = dominance * sample_boost

    return overall, round(confidence, 2), classified


def _combine_signals(
    flow_signal: str,
    news_direction: str,
    news_confidence: float,
) -> tuple[str, str, str]:
    """
    Combine flow + news into (combined_signal, option_direction, rationale).

    option_direction: 'call' | 'put' | None
    """
    # Flow direction
    flow_bullish = flow_signal in ("INSTITUTIONAL_BUY", "ACCUMULATION")
    flow_bearish = flow_signal in ("INSTITUTIONAL_SELL", "DISTRIBUTION")

    # Unusual activity but no direction
    if flow_signal in ("INSTITUTIONAL_BUY", "INSTITUTIONAL_SELL",
                       "ACCUMULATION", "DISTRIBUTION") and news_direction == "neutral":
        return (
            INVESTIGATE,
            "call" if flow_bullish else "put" if flow_bearish else None,
            f"{flow_signal} but no news context — possibly informed positioning",
        )

    # Alignment -> high conviction
    if flow_bullish and news_direction == "bullish":
        if news_confidence >= 0.5:
            return HIGH_CONVICTION, "call", f"{flow_signal} + bullish news (conf={news_confidence})"
        return MODERATE, "call", f"{flow_signal} + bullish news (low conf={news_confidence})"

    if flow_bearish and news_direction == "bearish":
        if news_confidence >= 0.5:
            return HIGH_CONVICTION, "put", f"{flow_signal} + bearish news (conf={news_confidence})"
        return MODERATE, "put", f"{flow_signal} + bearish news (low conf={news_confidence})"

    # Disagreement -> skip
    if (flow_bullish and news_direction == "bearish") or (flow_bearish and news_direction == "bullish"):
        return CONFLICTED, None, f"{flow_signal} disagrees with news ({news_direction})"

    # No unusual flow, just news -> already priced in typically
    if flow_signal in ("NORMAL", "UNKNOWN") and news_direction != "neutral":
        return SKIP, None, f"News {news_direction} but no unusual flow — likely already priced"

    return SKIP, None, "No actionable signal"


def scan(universe: list[str] | None = None) -> list[dict]:
    """
    Scan the universe. Returns list of combined-signal results.
    """
    tickers = universe or DEFAULT_UNIVERSE
    since = datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)
    results: list[dict] = []

    for ticker in tickers:
        try:
            # Step 1: unusual flow check
            flow = get_unusual_volume(ticker)
            flow_signal = flow.get("signal", "UNKNOWN")
            vol_ratio = flow.get("volume_ratio")
            price_chg = flow.get("price_change_pct")

            # Only fetch news if flow is flagged — saves API calls
            if flow_signal == "NORMAL" or flow_signal == "UNKNOWN":
                results.append({
                    "ticker": ticker,
                    "flow_signal": flow_signal,
                    "volume_ratio": vol_ratio,
                    "price_change_pct": price_chg,
                    "combined": SKIP,
                    "rationale": "no unusual flow",
                    "news_count": 0,
                    "news_direction": "n/a",
                    "option_direction": None,
                })
                continue

            # Step 2: fetch fresh news
            articles = get_news_since(ticker, since, limit=10)
            news_dir, news_conf, classified = _classify_news_direction(articles)

            # Step 3: combine
            combined, opt_dir, rationale = _combine_signals(
                flow_signal, news_dir, news_conf
            )

            results.append({
                "ticker": ticker,
                "flow_signal": flow_signal,
                "volume_ratio": vol_ratio,
                "price_change_pct": price_chg,
                "news_count": len(articles),
                "news_direction": news_dir,
                "news_confidence": news_conf,
                "combined": combined,
                "option_direction": opt_dir,
                "rationale": rationale,
                "top_headlines": [a["title"] for a in classified[:3]],
            })

        except Exception as e:
            results.append({
                "ticker": ticker,
                "error": str(e),
                "combined": SKIP,
            })

    return results


def alert_high_conviction(results: list[dict]) -> int:
    """Fire email alerts for HIGH_CONVICTION signals. Returns count alerted."""
    try:
        from tools.error_alerting import trigger_alert
    except Exception:
        return 0

    count = 0
    for r in results:
        if r.get("combined") != HIGH_CONVICTION:
            continue
        try:
            trigger_alert(
                source="flow_news_monitor",
                error_code="high_conviction_signal",
                message=(
                    f"HIGH CONVICTION {r.get('option_direction', '?').upper()} "
                    f"signal on {r['ticker']}: {r['rationale']}. "
                    f"Volume ratio: {r.get('volume_ratio')}, "
                    f"price change: {r.get('price_change_pct')}%"
                ),
                symbol=r["ticker"],
                context={
                    "flow_signal": r["flow_signal"],
                    "news_direction": r["news_direction"],
                    "news_confidence": r.get("news_confidence"),
                    "top_headlines": r.get("top_headlines", []),
                    "option_direction": r["option_direction"],
                },
                severity="INFO",  # signal, not an error
            )
            count += 1
        except Exception:
            pass
    return count


def save_results(results: list[dict]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = OUTPUT_DIR / f"flow_news_{ts}.json"
    path.write_text(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }, indent=2))
    return path


def print_summary(results: list[dict]) -> None:
    high = [r for r in results if r.get("combined") == HIGH_CONVICTION]
    mod = [r for r in results if r.get("combined") == MODERATE]
    invst = [r for r in results if r.get("combined") == INVESTIGATE]
    conf = [r for r in results if r.get("combined") == CONFLICTED]

    print(f"\nScanned {len(results)} tickers")
    print(f"  HIGH CONVICTION: {len(high)}")
    print(f"  MODERATE:        {len(mod)}")
    print(f"  INVESTIGATE:     {len(invst)}")
    print(f"  CONFLICTED:      {len(conf)}")

    if high:
        print("\n=== HIGH CONVICTION SIGNALS ===")
        for r in high:
            print(f"  {r['ticker']}: {r['option_direction'].upper()} — {r['rationale']}")
            for h in r.get("top_headlines", [])[:2]:
                print(f"    - {h}")

    if mod:
        print("\n=== MODERATE SIGNALS ===")
        for r in mod:
            print(f"  {r['ticker']}: {r['option_direction']} — {r['rationale']}")

    if invst:
        print("\n=== INVESTIGATE (unusual flow, no news explanation) ===")
        for r in invst:
            print(f"  {r['ticker']}: {r['flow_signal']} "
                  f"(vol {r.get('volume_ratio')}x, chg {r.get('price_change_pct')}%)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Unusual flow + news sentiment integrator")
    parser.add_argument("--universe", nargs="*", help="Override ticker list")
    parser.add_argument("--quiet", action="store_true", help="Only show HIGH CONVICTION")
    parser.add_argument("--no-alert", action="store_true", help="Don't send email alerts")
    args = parser.parse_args()

    results = scan(args.universe)
    path = save_results(results)

    if not args.quiet:
        print_summary(results)

    alerted = 0
    if not args.no_alert:
        alerted = alert_high_conviction(results)

    # Write daily roll-up file (no SQLite dependency)
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        rollup_path = OUTPUT_DIR / f"daily_{today}.jsonl"
        summary_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "high_conviction": sum(1 for r in results if r.get("combined") == HIGH_CONVICTION),
            "moderate": sum(1 for r in results if r.get("combined") == MODERATE),
            "investigate": sum(1 for r in results if r.get("combined") == INVESTIGATE),
            "alerted": alerted,
        }
        with open(rollup_path, "a") as f:
            f.write(json.dumps(summary_entry) + "\n")
    except Exception as e:
        print(f"(could not update daily rollup: {e})")

    print(f"\nSaved: {path}")
    print(f"Alerted: {alerted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
