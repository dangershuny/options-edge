#!/usr/bin/env python3
"""
Chain-surface snapshot CLI.

Pulls today's full options chain (every strike × every listed expiry up to
90 DTE) for the curated universe and persists it into the `chain_surface`
SQLite table. Idempotent — safe to re-run on the same day.

Usage:
    python -m tools.surface_snapshot                    # full universe
    python -m tools.surface_snapshot --universe AAPL,MSFT,NVDA
    python -m tools.surface_snapshot --status           # show what's on disk
"""

from __future__ import annotations
import argparse
import os
import sys
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.chain_surface import snapshot_symbol, surface_stats, surface_dates
from data.universe import UNIVERSE

SEP = "=" * 78


def _print_status() -> int:
    s = surface_stats()
    print(SEP)
    print("  CHAIN SURFACE STATUS")
    print(SEP)
    print(f"  total rows : {s['rows']:,}")
    print(f"  symbols    : {s['symbols']}")
    print(f"  dates      : {s['dates']}")
    print(f"  first date : {s['first_date']}")
    print(f"  last date  : {s['last_date']}")
    if s['dates']:
        print()
        print(f"  dates on disk: {', '.join(surface_dates()[-10:])}"
              + (" …" if s['dates'] > 10 else ""))
    print(SEP)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Snapshot option chain surfaces.")
    ap.add_argument("--universe", default=None,
                    help="comma-separated symbols (default: curated universe)")
    ap.add_argument("--status", action="store_true",
                    help="show persistence status and exit")
    ap.add_argument("--date", default=None,
                    help="override snapshot_date (YYYY-MM-DD, default: today)")
    ap.add_argument("--sleep", type=float, default=0.25,
                    help="seconds between tickers to avoid yfinance rate limits")
    args = ap.parse_args()

    if args.status:
        return _print_status()

    if args.universe:
        uni = [s.strip().upper() for s in args.universe.split(",") if s.strip()]
    else:
        uni = list(UNIVERSE)

    sd = args.date or date.today().isoformat()

    print(SEP)
    print(f"  SURFACE SNAPSHOT   date={sd}   n_tickers={len(uni)}")
    print(SEP)

    n_ok = n_fail = total_rows = 0
    failed: list[str] = []
    t0 = time.time()
    for i, sym in enumerate(uni, 1):
        res = snapshot_symbol(sym, snapshot_date=sd)
        rows = res["rows_written"]
        total_rows += rows
        if res["error"] or rows == 0:
            n_fail += 1
            failed.append(sym)
            print(f"  [{i:>3}/{len(uni)}] {sym:<6}  FAIL  ({res['error'] or 'no rows'})", flush=True)
        else:
            n_ok += 1
            spot = res["spot"] or 0
            print(f"  [{i:>3}/{len(uni)}] {sym:<6}  ok   {rows:>4} rows  spot ${spot:,.2f}", flush=True)
        if args.sleep > 0 and i < len(uni):
            time.sleep(args.sleep)

    elapsed = time.time() - t0
    print()
    print(SEP)
    print(f"  DONE   ok={n_ok}  fail={n_fail}  rows_written={total_rows:,}  elapsed={elapsed:.1f}s")
    if failed:
        print(f"  failed: {', '.join(failed[:20])}"
              + (" …" if len(failed) > 20 else ""))
    print(SEP)

    # Also print fresh status
    print()
    _print_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
