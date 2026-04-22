#!/usr/bin/env python3
"""
Historical signal backtest CLI.

Pulls OHLCV for the curated universe, evaluates each signal's information
coefficient + per-bucket forward-return stats across 1/3/5/10-day horizons,
and writes the raw result to benchmarks/ for the optimizer to consume.

Usage:
    python -m tools.historical_backtest
    python -m tools.historical_backtest --days 730 --out benchmarks/hist_2yr.json
    python -m tools.historical_backtest --universe AAPL,MSFT,NVDA
"""

from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.hist_backtest import run_hist_backtest, FWD_HORIZONS
from data.universe import UNIVERSE

SEP = "=" * 78
SUB = "-" * 78


def _fmt(v, w=7, pct=False):
    if v is None:
        return "   n/a"
    if pct:
        return f"{v:>{w}.2f}%"
    return f"{v:>{w}.3f}"


def _print_signal_report(name: str, sig: dict) -> None:
    print()
    print(f"{name.upper()}")
    print(SUB)
    ics = sig.get("ic", {})
    ic_cells = "  ".join(f"{h}:{_fmt(ics.get(h))}" for h in sorted(ics))
    print(f"  IC (Spearman vs fwd returns):  {ic_cells}")

    by_bucket = sig.get("by_bucket", {})
    if not by_bucket:
        return
    horizons = sorted({h for b in by_bucket.values() for h in b})
    header = "  bucket".ljust(16) + "  ".join(f"{h:>18}" for h in horizons)
    print(header)
    for b, stats in sorted(by_bucket.items()):
        cells = []
        for h in horizons:
            s = stats.get(h) or {}
            n = s.get("n", 0)
            m = s.get("mean")
            hit = s.get("hit")
            if n == 0 or m is None:
                cells.append(f"{'—':>18}")
            else:
                cells.append(f"n={n:>4} μ={m:>+5.2f}% hit={hit:>4.1f}%")
        print(f"  {b:<14}" + "  ".join(f"{c:>18}" for c in cells))


def main() -> int:
    ap = argparse.ArgumentParser(description="Historical signal backtest.")
    ap.add_argument("--days", type=int, default=365,
                    help="lookback window (default 365)")
    ap.add_argument("--universe", default=None,
                    help="comma-separated symbols (default: curated universe)")
    ap.add_argument("--out", default=None,
                    help="output JSON path (default: benchmarks/hist_YYYY-MM-DD.json)")
    ap.add_argument("--horizons", default=None,
                    help="comma-separated horizons, e.g. 1,3,5,10")
    args = ap.parse_args()

    if args.universe:
        uni = [s.strip().upper() for s in args.universe.split(",") if s.strip()]
    else:
        uni = list(UNIVERSE)

    horizons = [int(x) for x in args.horizons.split(",")] if args.horizons else FWD_HORIZONS

    out = args.out or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "benchmarks",
        f"hist_{datetime.now():%Y-%m-%d}.json",
    )

    def cb(i, n, sym):
        print(f"  [{i+1:>3}/{n}] {sym}", flush=True)

    print(SEP)
    print(f"  HISTORICAL SIGNAL BACKTEST   days={args.days}   n_tickers={len(uni)}")
    print(SEP)

    result = run_hist_backtest(uni, period_days=args.days,
                               horizons=horizons, progress_cb=cb)

    report = result.to_dict()
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    print()
    print(SEP)
    print(f"  RESULT   ok={len(result.tickers_ok)}   fail={len(result.tickers_fail)}")
    print(SEP)
    if result.tickers_fail:
        print(f"  failed: {', '.join(result.tickers_fail[:20])}"
              + (" …" if len(result.tickers_fail) > 20 else ""))

    for name, sig in result.signals.items():
        _print_signal_report(name, sig)

    print()
    print(f"  Written: {out}")
    print(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
