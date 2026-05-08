"""
Divergence-driven entry picker — the sentinel-as-gate prototype.

Reads ACTIVE divergence_events from news_sentinel/sentinel.db and selects
ONE option contract per flagged ticker that aligns with the divergence
direction. Skips everything else, including the highest-score scorer pick.

This is the primary-signal restructuring backed by the event backtest:
divergence_events showed 100% directional win rate (n=7, rho=+0.82) while
the IV/RV scorer was anti-correlated (rho=-0.32). The scorer's role is
demoted to contract selection within a divergence thesis, not entry trigger.

Output:
  - logs/divergence_picks_{date}.json  (the picks, machine-readable)
  - stdout summary
  - optional: submits paper orders via tools.paper_trade pipeline (--live)

Usage:
    python -m tools.divergence_picker                      # dry run, list picks
    python -m tools.divergence_picker --max-age-hours 24
    python -m tools.divergence_picker --live               # submit on paper
    python -m tools.divergence_picker --min-score 50       # filter contracts further
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

SENTINEL_DB = Path(r"C:\Users\dange\OneDrive\Documents\Claude Projects\news_sentinel\sentinel.db")
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _ro(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:/{str(db).replace(chr(92), '/')}?mode=ro", uri=True)


def _direction_to_option_type(direction: str) -> str | None:
    d = (direction or "").lower()
    if "bullish" in d:
        return "call"
    if "bearish" in d:
        return "put"
    return None


def fetch_active_divergences(max_age_hours: int = 24) -> list[dict]:
    """Pull divergence_events flagged within the lookback window.
    Excludes archived events — only currently active flags qualify."""
    if not SENTINEL_DB.exists():
        return []
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    c = _ro(SENTINEL_DB)
    rows = c.execute(
        """
        SELECT ticker, direction, news_sentiment_avg, social_sentiment_avg,
               divergence_score, article_count, flagged_at, has_recent_8k,
               key_concerns, key_positives, haiku_summary
        FROM divergence_events
        WHERE flagged_at >= ?
        ORDER BY flagged_at DESC
        """,
        (cutoff,),
    ).fetchall()
    return [
        {
            "ticker": r[0],
            "direction": r[1],
            "news_sent": r[2],
            "soc_sent": r[3],
            "divergence_score": float(r[4] or 0),
            "article_count": int(r[5] or 0),
            "flagged_at": r[6],
            "has_8k": bool(r[7] or 0),
            "key_concerns": r[8],
            "key_positives": r[9],
            "summary": r[10],
        }
        for r in rows
    ]


def pick_contract_for_ticker(ticker: str, target_type: str,
                              min_score: float = 0.0) -> dict | None:
    """Run analyze_ticker, find the best contract whose option_type matches
    the divergence direction. Returns a dict ready for paper_trade or None."""
    from analysis.scorer import analyze_ticker
    try:
        df, _trades, err, _meta = analyze_ticker(ticker)
    except Exception as e:
        return {"ticker": ticker, "error": f"analyze_ticker failed: {e}"}
    if err:
        return {"ticker": ticker, "error": err}
    if df is None or df.empty:
        return {"ticker": ticker, "error": "no contracts produced"}

    # Filter contracts to the right direction + decent quality
    eligible = df[df["type"].str.lower() == target_type]
    eligible = eligible[eligible["vol_signal"].isin(["BUY VOL", "FLOW BUY"])]
    if min_score > 0:
        eligible = eligible[eligible["score"] >= min_score]
    if eligible.empty:
        return {"ticker": ticker, "error": f"no {target_type} BUY VOL contracts after filter"}

    # Apply pre-trade filters (price, trend, etc.) AND the 2026-05-06 entry
    # gates (block_puts, regime, circuit-breaker, spread, score-cross-val).
    # Bug observed 2026-05-07: BCRX put entered through this path because
    # only _passes_pretrade_filters was called, not _all_new_gates — the
    # block_puts flag had no effect on divergence-driven entries. Now both
    # gates run; either can reject.
    from tools.paper_trade import _passes_pretrade_filters, _all_new_gates
    from risk.config import RISK
    min_score_for_gates = float(RISK.get("min_score_to_trade", 60))
    rows = []
    rejected: list[tuple[str, str]] = []
    for _, r in eligible.iterrows():
        d = r.to_dict()
        d["option_type"] = d["type"]
        d["stock_price_at_snap"] = d.get("stock_price")
        ok, reason = _passes_pretrade_filters(d)
        if not ok:
            rejected.append((d.get("type", "?"), reason))
            continue
        ok, reason = _all_new_gates(d, min_score_for_gates)
        if not ok:
            rejected.append((d.get("type", "?"), reason))
            continue
        rows.append(r)
    if not rows:
        # Surface the most informative reason in the error so the caller
        # logs the actual gate that fired (block_puts, regime, etc.)
        why = "all contracts blocked by pre-trade filters"
        if rejected:
            why = f"all blocked: {rejected[0][1]} (and {len(rejected)-1} more)"
        return {"ticker": ticker, "error": why}

    # Pick the highest-scoring one. (The scorer is anti-correlated overall,
    # but WITHIN a divergence-flagged ticker its job is "best contract for
    # this thesis" — DTE / spread / Greeks. Negative-correlation finding
    # was about whether to trade at all, not which contract to pick.)
    rows.sort(key=lambda r: -float(r.get("score", 0)))
    best = rows[0].to_dict()
    return {
        "ticker": ticker,
        "occ_ready": {
            "symbol": ticker,
            "option_type": best["type"],
            "strike": float(best["strike"]),
            "expiry": best["expiry"],
            "dte": int(best["dte"]),
            "score": float(best["score"]),
            "vol_signal": best.get("vol_signal"),
            "entry_price": best.get("entry_price"),
            "bid": best.get("bid"),
            "ask": best.get("ask"),
            "iv_rv_spread": best.get("iv_rv_spread"),
            "stock_price_at_snap": best.get("stock_price"),
        },
    }


def submit_picks(picks: list[dict], dry_run: bool = True) -> list[dict]:
    """Send picks through paper_trade-style submission. Each pick must include
    `occ_ready` keyed on the contract we want to buy.

    IMPORTANT: also appends each result to logs/paper_trades.jsonl so the
    EOD analysis (which reads that file for per-pipeline P&L) sees
    divergence-driven entries. tools/paper_trade.py only writes to that
    file from its run() wrapper, which we bypass here."""
    from broker import alpaca
    from tools.paper_trade import _execute_trade
    paper_log = REPO_ROOT / "logs" / "paper_trades.jsonl"
    paper_log.parent.mkdir(parents=True, exist_ok=True)
    submitted = []
    for p in picks:
        if "occ_ready" not in p:
            continue
        trade = dict(p["occ_ready"])
        trade.setdefault("option_type", trade.get("option_type"))
        try:
            res = _execute_trade(
                broker=alpaca,
                trade=trade,
                bankroll_remaining=300.0,
                dry_run=dry_run,
                max_per_trade=300.0,
                tag=f"divergence-{date.today().isoformat()}",
            )
            # Append to paper_trades.jsonl in the same shape paper_trade.run()
            # writes — so EOD analysis sees divergence entries
            try:
                with paper_log.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(res, default=str) + "\n")
            except Exception as e:
                res["jsonl_warning"] = f"paper_trades.jsonl append failed: {e}"
            submitted.append({"ticker": p["ticker"], **res})
        except Exception as e:
            submitted.append({"ticker": p["ticker"], "status": "failed", "error": str(e)})
    return submitted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-age-hours", type=int, default=24,
                    help="only consider divergence events flagged within this many hours")
    ap.add_argument("--min-score", type=float, default=0,
                    help="filter contracts by minimum scorer score (default 0 — divergence is the gate, not score)")
    ap.add_argument("--live", action="store_true",
                    help="actually submit paper orders (default: dry-run, list only)")
    ap.add_argument("--max-picks", type=int, default=3,
                    help="cap total picks per run (default 3)")
    ap.add_argument("--qty", type=int, default=1,
                    help="contracts per pick (default 1 — small, since n=7 sample)")
    args = ap.parse_args()

    print(f"divergence_picker: max_age_hours={args.max_age_hours} live={args.live}")
    events = fetch_active_divergences(max_age_hours=args.max_age_hours)
    print(f"active divergences (raw): {len(events)}")

    # Dedupe per ticker — keep the most recent flag with the most info.
    # Sentinel sometimes re-flags the same divergence multiple times when
    # rescanning. We trade once per ticker per session.
    by_ticker: dict[str, dict] = {}
    for ev in events:
        t = ev["ticker"]
        prev = by_ticker.get(t)
        if prev is None:
            by_ticker[t] = ev
            continue
        # keep the one with more data (8k flag, higher score, more articles)
        prev_score = (prev.get("has_8k", 0) * 1000
                      + prev.get("divergence_score", 0)
                      + prev.get("article_count", 0))
        cur_score = (ev.get("has_8k", 0) * 1000
                     + ev.get("divergence_score", 0)
                     + ev.get("article_count", 0))
        if cur_score > prev_score:
            by_ticker[t] = ev
    events = list(by_ticker.values())
    print(f"after dedup per ticker: {len(events)}")

    if not events:
        print("no active divergences — no trades today")
        return 0

    picks = []
    for ev in events:
        opt_type = _direction_to_option_type(ev["direction"])
        if not opt_type:
            print(f"  {ev['ticker']}: unclear direction '{ev['direction']}' — skip")
            continue
        print(f"  {ev['ticker']:<6} dir={ev['direction']:<24} "
              f"score={ev['divergence_score']:.0f} 8k={ev['has_8k']} -> {opt_type.upper()}")
        result = pick_contract_for_ticker(ev["ticker"], opt_type, min_score=args.min_score)
        if result is None or result.get("error"):
            print(f"      -> SKIP: {result.get('error', 'no result')}")
            continue
        picks.append({**result, "_event": ev})
        oc = result["occ_ready"]
        print(f"      -> PICK: {oc['option_type'].upper()} ${oc['strike']:.2f} "
              f"exp {oc['expiry']} ({oc['dte']}d) score={oc['score']:.0f} "
              f"@ mid=${oc.get('entry_price') or '?'}")

    # Cap picks
    if len(picks) > args.max_picks:
        picks.sort(
            key=lambda p: (
                p["_event"].get("has_8k", False),
                float(p["_event"].get("divergence_score", 0)),
                int(p["_event"].get("article_count", 0)),
            ),
            reverse=True,
        )
        picks = picks[:args.max_picks]
        print(f"capped to top {args.max_picks} picks by 8k+score+articles")

    print()
    print(f"final picks: {len(picks)}")

    if picks and args.live:
        print("\n=== submitting (LIVE paper) ===")
        # Send a heads-up to Telegram before firing
        try:
            from tools.notify import send
            summary = "\n".join(
                f"  {p['ticker']:<5} {p['occ_ready']['option_type'].upper()} "
                f"${p['occ_ready']['strike']:.2f} exp {p['occ_ready']['expiry']} "
                f"({p['occ_ready']['dte']}d)"
                for p in picks
            )
            send("ENTRY", f"divergence picks ({len(picks)}) submitting", summary)
        except Exception:
            pass
        # Patch qty into each pick before _execute_trade
        for p in picks:
            p["occ_ready"]["_qty_override"] = args.qty
        results = submit_picks(picks, dry_run=False)
        for r in results:
            print(f"  {r}")
    elif picks:
        print("\n=== dry run only — pass --live to submit ===")

    out = LOG_DIR / f"divergence_picks_{date.today().isoformat()}.json"
    out.write_text(json.dumps({
        "ts": datetime.now(timezone.utc).isoformat(),
        "events": events,
        "picks": picks,
    }, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
