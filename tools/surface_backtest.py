#!/usr/bin/env python3
"""
Surface backtest CLI — real options-level replay.

Given an entry date and an exit date (both must have persisted surfaces in
`chain_surface`), replays every contract that would have passed entry
filters on the entry date and computes mid-to-mid P&L on the exit date.

Usage:
    python -m tools.surface_backtest --entry 2026-04-22 --exit 2026-04-29
    python -m tools.surface_backtest --entry 2026-04-22 --exit 2026-04-29 \
        --universe AAPL,MSFT,NVDA
    python -m tools.surface_backtest --dates          # list dates on disk

Outputs a JSON summary to `benchmarks/surface_<entry>_<exit>.json` and prints
bucketed stats (overall, by type, by DTE, by moneyness).
"""

from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.surface_backtest import replay
from data.chain_surface import surface_dates

SEP = "=" * 78
SUB = "-" * 78

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH_DIR    = os.path.join(PROJECT_ROOT, "benchmarks")


def _print_bucket(name: str, stats: dict) -> None:
    n = stats.get("n", 0)
    if not n:
        print(f"  {name:<22}  n=0")
        return
    print(
        f"  {name:<22}  n={n:<4}  "
        f"hit={stats.get('hit_rate'):>5}%  "
        f"avg={stats.get('avg_pnl_pct'):+6.2f}%  "
        f"win={stats.get('avg_win_pct')}  loss={stats.get('avg_loss_pct')}  "
        f"total=${stats.get('total_pnl'):,.2f}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay real options P&L across two surface dates.")
    ap.add_argument("--entry", help="entry snapshot_date (YYYY-MM-DD)")
    ap.add_argument("--exit",  dest="exit_date", help="exit snapshot_date (YYYY-MM-DD)")
    ap.add_argument("--universe", default=None,
                    help="comma-separated symbols (default: all shared)")
    ap.add_argument("--dates", action="store_true",
                    help="list snapshot_dates on disk and exit")
    ap.add_argument("--out", default=None, help="override output JSON path")
    args = ap.parse_args()

    if args.dates:
        ds = surface_dates()
        print(SEP)
        print(f"  SURFACE DATES ON DISK  ({len(ds)})")
        print(SEP)
        for d in ds:
            print(f"  {d}")
        return 0

    if not args.entry or not args.exit_date:
        ap.error("--entry and --exit are required (or pass --dates)")

    syms = None
    if args.universe:
        syms = [s.strip().upper() for s in args.universe.split(",") if s.strip()]

    print(SEP)
    print(f"  SURFACE REPLAY   entry={args.entry}   exit={args.exit_date}")
    print(SEP)

    result = replay(args.entry, args.exit_date, symbols=syms)

    for w in result.warnings:
        print(f"  ! {w}")

    print(f"  symbols evaluated : {result.n_symbols}")
    print(f"  trades replayed   : {result.n_trades}")
    print()
    print("BUCKETS")
    print(SUB)
    buckets = result.by_bucket or {}
    _print_bucket("overall", buckets.get("overall", {}))
    print()
    for family in ("by_type", "by_dte", "by_moneyness"):
        sub = buckets.get(family) or {}
        if not sub:
            continue
        print(f"  [{family}]")
        for k, stats in sub.items():
            _print_bucket(f"  {k}", stats)
        print()

    # Top-10 winners / losers preview
    if result.trades:
        print("TOP 10 WINNERS")
        print(SUB)
        for t in result.trades[:10]:
            print(f"  {t.symbol:<6} {t.option_type:<4} K={t.strike:<7}  "
                  f"exp={t.expiry}  {t.entry_mid:>6} → {t.exit_mid:<6}  "
                  f"pnl={t.pnl_pct:+.2f}%  (${t.pnl_per_ct:+.2f}/ct)")
        print()
        print("TOP 10 LOSERS")
        print(SUB)
        for t in list(reversed(result.trades))[:10]:
            print(f"  {t.symbol:<6} {t.option_type:<4} K={t.strike:<7}  "
                  f"exp={t.expiry}  {t.entry_mid:>6} → {t.exit_mid:<6}  "
                  f"pnl={t.pnl_pct:+.2f}%  (${t.pnl_per_ct:+.2f}/ct)")
        print()

    os.makedirs(BENCH_DIR, exist_ok=True)
    out_path = args.out or os.path.join(
        BENCH_DIR, f"surface_{args.entry}_{args.exit_date}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result.to_dict(), f, indent=2)
    print(f"  Wrote summary → {out_path}")
    print(SEP)
    return 0 if result.n_trades > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
