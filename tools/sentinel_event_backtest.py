"""
Sentinel-event-as-driver backtest.

Pulls events directly from `news_sentinel/sentinel.db` (NOT from
options-edge snapshots) and tests whether each event class predicts the
direction the underlying actually moves over the next N days.

Three event classes tested independently:

  1. divergence_events — explicit bullish/bearish flags from the sentinel
  2. strong-sentiment news_articles — sentiment_label in (very_negative,
     very_positive) with body length >= threshold (filters out one-line
     headlines)
  3. social_posts with extreme sentiment

For each event:
  - Anchor date = published_at (or flagged_at)
  - Anchor close = first underlying close on/after anchor date
  - Forward close = close N trading days later
  - Direction-aligned win? (bullish event + price up = win;
    bearish event + price down = win)

Output:
  - per-event-class win rate, mean return, and Spearman correlation between
    event score and outcome
  - per-ticker breakdown for high-volume names
  - histogram of latency between event and capturable price move
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

SENTINEL_DB = Path(r"C:\Users\dange\OneDrive\Documents\Claude Projects\news_sentinel\sentinel.db")
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _ro(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:/{str(db).replace(chr(92), '/')}?mode=ro", uri=True)


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except Exception:
            return None


def _first_close_on_or_after(prices: dict[date, float], target: date,
                              max_skip: int = 5) -> tuple[date, float] | None:
    for off in range(max_skip + 1):
        d = target + timedelta(days=off)
        if d in prices:
            return d, prices[d]
    return None


def _fetch_prices(symbols: set[str], start: date, end: date) -> dict[str, dict[date, float]]:
    import yfinance as yf
    out: dict[str, dict[date, float]] = {}
    for sym in sorted(symbols):
        try:
            h = yf.Ticker(sym).history(
                start=start.isoformat(),
                end=(end + timedelta(days=1)).isoformat(),
                auto_adjust=True,
            )
            if h is None or h.empty or "Close" not in h.columns:
                out[sym] = {}
                continue
            out[sym] = {idx.date(): float(c) for idx, c in h["Close"].dropna().items()}
        except Exception:
            out[sym] = {}
    return out


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None

    def ranks(vs):
        order = sorted(range(n), key=lambda i: vs[i])
        rs = [0.0] * n
        for r, i in enumerate(order, start=1):
            rs[i] = float(r)
        return rs

    rx = ranks(xs); ry = ranks(ys)
    mx = sum(rx) / n; my = sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    dx = sum((r - mx) ** 2 for r in rx) ** 0.5
    dy = sum((r - my) ** 2 for r in ry) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


# ── Event extractors ────────────────────────────────────────────────────────

def get_divergence_events(c: sqlite3.Connection, since: date) -> list[dict]:
    # archive table doesn't have has_recent_8k — substitute NULL
    rows = c.execute(
        """
        SELECT ticker, direction, news_sentiment_avg, social_sentiment_avg,
               divergence_score, article_count, flagged_at, has_recent_8k
        FROM divergence_events WHERE flagged_at >= ?
        UNION ALL
        SELECT ticker, direction, news_sentiment_avg, social_sentiment_avg,
               divergence_score, article_count, flagged_at, NULL AS has_recent_8k
        FROM divergence_events_archive WHERE flagged_at >= ?
        """,
        (since.isoformat(), since.isoformat()),
    ).fetchall()
    return [
        {
            "kind": "divergence",
            "ticker": r[0],
            "direction": r[1],   # 'bullish_convergence', 'bearish_divergence', etc.
            "news_sent": r[2],
            "soc_sent": r[3],
            "score": float(r[4] or 0),
            "n_articles": int(r[5] or 0),
            "ts": _parse_iso(r[6]),
            "has_8k": bool(r[7] or 0),
        }
        for r in rows if r[6]
    ]


def get_strong_news_events(c: sqlite3.Connection, since: date,
                            min_body_len: int = 200,
                            extreme_only: bool = True) -> list[dict]:
    """Substantive news articles (not just headlines) with strong sentiment."""
    # news_articles uses labels: 'bullish', 'bearish', 'neutral'.
    # When extreme_only is True we skip 'neutral' AND require strong score.
    label_filter = (
        "AND sentiment_label IN ('bullish','bearish') AND ABS(sentiment_score) >= 0.5"
        if extreme_only else ""
    )
    rows = c.execute(
        f"""
        SELECT ticker, sentiment_score, sentiment_label, headline, source,
               published_at, LENGTH(COALESCE(body, ''))
        FROM news_articles
        WHERE published_at >= ?
          AND sentiment_score IS NOT NULL
          AND LENGTH(COALESCE(body, '')) >= ?
          {label_filter}
        UNION ALL
        SELECT ticker, sentiment_score, sentiment_label, headline, source,
               published_at, LENGTH(COALESCE(body, ''))
        FROM news_articles_archive
        WHERE published_at >= ?
          AND sentiment_score IS NOT NULL
          AND LENGTH(COALESCE(body, '')) >= ?
          {label_filter}
        """,
        (since.isoformat(), min_body_len, since.isoformat(), min_body_len),
    ).fetchall()
    out = []
    for r in rows:
        ts = _parse_iso(r[5])
        if not ts:
            continue
        out.append({
            "kind": "news",
            "ticker": r[0],
            "score": float(r[1] or 0),    # -1.0 .. +1.0
            "label": r[2],
            "headline": r[3],
            "source": r[4],
            "ts": ts,
            "body_len": int(r[6] or 0),
        })
    return out


def get_extreme_social_events(c: sqlite3.Connection, since: date,
                                threshold: float = 0.7) -> list[dict]:
    rows = c.execute(
        """
        SELECT ticker, sentiment_score, platform, published_at
        FROM social_posts
        WHERE published_at >= ?
          AND sentiment_score IS NOT NULL
          AND ABS(sentiment_score) >= ?
        UNION ALL
        SELECT ticker, sentiment_score, platform, published_at
        FROM social_posts_archive
        WHERE published_at >= ?
          AND sentiment_score IS NOT NULL
          AND ABS(sentiment_score) >= ?
        """,
        (since.isoformat(), threshold, since.isoformat(), threshold),
    ).fetchall()
    out = []
    for r in rows:
        ts = _parse_iso(r[3])
        if ts:
            out.append({
                "kind": "social",
                "ticker": r[0],
                "score": float(r[1]),
                "platform": r[2],
                "ts": ts,
            })
    return out


# ── Outcome computation ─────────────────────────────────────────────────────

def _direction_of(event: dict) -> int:
    """Returns +1 (bullish), -1 (bearish), 0 (unclear)."""
    if event["kind"] == "divergence":
        d = (event.get("direction") or "").lower()
        if "bullish" in d:
            return 1
        if "bearish" in d:
            return -1
        return 0
    score = float(event.get("score") or 0)
    if score > 0.2:
        return 1
    if score < -0.2:
        return -1
    return 0


def attach_outcomes(events: list[dict], hold_days: int) -> list[dict]:
    if not events:
        return events
    symbols = {e["ticker"] for e in events if e.get("ticker")}
    earliest = min(e["ts"].date() for e in events)
    latest_needed = date.today()
    print(f"  fetching prices for {len(symbols)} symbols, {earliest} -> {latest_needed}")
    prices = _fetch_prices(symbols, earliest, latest_needed)

    enriched: list[dict] = []
    for e in events:
        sym = e["ticker"]
        prc = prices.get(sym, {})
        if not prc:
            continue
        d0 = e["ts"].date()
        anchor = _first_close_on_or_after(prc, d0)
        forward = _first_close_on_or_after(prc, d0 + timedelta(days=hold_days))
        if not anchor or not forward:
            continue
        ret = (forward[1] / anchor[1]) - 1
        d = _direction_of(e)
        if d == 0:
            continue
        directional = ret if d > 0 else -ret  # if event was bearish, "win" means underlying down
        enriched.append({
            **e,
            "underlying_return_pct": round(ret * 100, 2),
            "directional_return_pct": round(directional * 100, 2),
            "won": directional > 0,
            "direction_inferred": d,
        })
    return enriched


# ── Reporting ───────────────────────────────────────────────────────────────

def _stats(label: str, items: list[dict]) -> dict:
    if not items:
        print(f"  {label:<40} (no data)")
        return {}
    n = len(items)
    wins = sum(1 for x in items if x.get("won"))
    rets = [x["directional_return_pct"] for x in items]
    mean = statistics.fmean(rets)
    med = statistics.median(rets)
    rho = _spearman(
        [abs(float(x.get("score", 0))) for x in items],
        [x["directional_return_pct"] for x in items],
    )
    rho_str = f"{rho:+.2f}" if rho is not None else "  ?"
    print(f"  {label:<40} n={n:>4}  win={wins/n:>5.1%}  "
          f"mean={mean:>+6.2f}%  med={med:>+6.2f}%  rho={rho_str}")
    return {"n": n, "win_rate": wins / n, "mean": mean, "median": med, "rho": rho}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-days", type=int, default=60,
                    help="lookback window in days for sentinel events (default 60)")
    ap.add_argument("--hold-days", type=int, default=5,
                    help="forward horizon to measure outcome (default 5)")
    ap.add_argument("--news-min-body", type=int, default=200,
                    help="minimum body length for news (filter headlines-only) (default 200)")
    args = ap.parse_args()

    if not SENTINEL_DB.exists():
        print(f"sentinel.db not found at {SENTINEL_DB}")
        return 1
    c = _ro(SENTINEL_DB)
    since = date.today() - timedelta(days=args.since_days)
    print(f"backtest window: events since {since}, hold={args.hold_days}d")
    print()

    # 1. Divergence events
    print("=== divergence_events ===")
    divs = get_divergence_events(c, since)
    print(f"  raw events: {len(divs)}")
    divs = attach_outcomes(divs, args.hold_days)
    _stats("divergence (all directions)", divs)
    _stats("  bullish", [x for x in divs if x["direction_inferred"] > 0])
    _stats("  bearish", [x for x in divs if x["direction_inferred"] < 0])
    _stats("  with recent 8-K", [x for x in divs if x.get("has_8k")])
    _stats("  divergence_score >= 50", [x for x in divs if abs(x.get("score", 0)) >= 50])
    print()

    # 2. Strong news (substantive bodies)
    print("=== news_articles (strong sentiment, body >= "
          f"{args.news_min_body} chars) ===")
    news = get_strong_news_events(c, since, min_body_len=args.news_min_body)
    print(f"  raw events: {len(news)}")
    # Dedupe — keep one (most recent) per ticker per day to avoid re-counting
    by_key = {}
    for e in news:
        k = (e["ticker"], e["ts"].date(), 1 if _direction_of(e) > 0 else -1)
        if k not in by_key or e["ts"] > by_key[k]["ts"]:
            by_key[k] = e
    deduped = list(by_key.values())
    print(f"  deduped (1 per ticker-day-direction): {len(deduped)}")
    news_out = attach_outcomes(deduped, args.hold_days)
    _stats("news (all)", news_out)
    _stats("  positive label", [x for x in news_out if x["direction_inferred"] > 0])
    _stats("  negative label", [x for x in news_out if x["direction_inferred"] < 0])
    _stats("  abs(score) >= 0.5", [x for x in news_out if abs(x.get("score", 0)) >= 0.5])
    print()

    # 3. Social
    print("=== social_posts (|sentiment| >= 0.7) ===")
    soc = get_extreme_social_events(c, since, threshold=0.7)
    print(f"  raw events: {len(soc)}")
    by_key = {}
    for e in soc:
        k = (e["ticker"], e["ts"].date(), 1 if _direction_of(e) > 0 else -1)
        if k not in by_key or e["ts"] > by_key[k]["ts"]:
            by_key[k] = e
    soc_dd = list(by_key.values())
    print(f"  deduped: {len(soc_dd)}")
    soc_out = attach_outcomes(soc_dd, args.hold_days)
    _stats("social (all)", soc_out)
    _stats("  positive", [x for x in soc_out if x["direction_inferred"] > 0])
    _stats("  negative", [x for x in soc_out if x["direction_inferred"] < 0])
    print()

    # Save full output for further analysis
    out_path = LOG_DIR / f"sentinel-event-backtest-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    out_path.write_text(json.dumps({
        "ts": datetime.now().isoformat(),
        "params": {"since_days": args.since_days, "hold_days": args.hold_days,
                   "news_min_body": args.news_min_body},
        "divergence_events": divs,
        "news_events": news_out,
        "social_events": soc_out,
    }, indent=2, default=str), encoding="utf-8")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
