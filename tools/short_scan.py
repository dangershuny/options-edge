#!/usr/bin/env python3
"""
Short interest / squeeze-setup scan.

Usage:
    python tools/short_scan.py
    python tools/short_scan.py GME AMC
    python tools/short_scan.py --json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.short_interest import get_short_interest
from data.watchlist import load_watchlist


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    tickers = [t.upper() for t in args.tickers] or load_watchlist()
    if not tickers:
        print("No tickers.", file=sys.stderr)
        return 2

    results = {t: get_short_interest(t) for t in tickers}

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return 0

    print("=" * 78)
    print("  SHORT INTEREST SCAN")
    print("=" * 78)
    print(f"  {'Ticker':<7} {'Signal':<15} {'Float%':>7} {'D2C':>5}   Summary")
    print("-" * 78)
    for t, r in results.items():
        sf = r.get("short_float_pct")
        d2c = r.get("days_to_cover")
        sf_s = f"{sf:>6.1f}%" if isinstance(sf, (int, float)) else "  n/a "
        d2c_s = f"{d2c:>5.1f}" if isinstance(d2c, (int, float)) else " n/a "
        print(f"  {t:<7} {r.get('signal','UNKNOWN'):<15} {sf_s} {d2c_s}   {r.get('summary','')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
