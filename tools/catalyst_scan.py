#!/usr/bin/env python3
"""
Catalyst scan — events (FOMC, earnings, FDA) within a DTE window.

Usage:
    python tools/catalyst_scan.py                         # watchlist, 30d
    python tools/catalyst_scan.py --dte 45 NVDA AAPL
    python tools/catalyst_scan.py --json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.catalysts import catalysts_in_window
from data.watchlist import load_watchlist


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*")
    ap.add_argument("--dte", type=int, default=30, help="days-to-expiry horizon (default 30)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    tickers = [t.upper() for t in args.tickers] or load_watchlist()
    if not tickers:
        print("No tickers.", file=sys.stderr)
        return 2

    results = {t: catalysts_in_window(t, args.dte) for t in tickers}

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return 0

    print("=" * 78)
    print(f"  CATALYST SCAN  —  horizon {args.dte}d")
    print("=" * 78)
    for t, r in results.items():
        flag = "🔔" if r.get("has_catalyst") else "  "
        print(f"  {flag} {t:<6} {r.get('summary','')}")
        for e in r.get("events", []) or []:
            print(f"        · {e.get('kind')}  {e.get('date')}  — {e.get('desc','')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
