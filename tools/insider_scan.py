#!/usr/bin/env python3
"""
Insider Form-4 scan — SEC EDGAR filings over the last N days.

Usage:
    python tools/insider_scan.py                 # watchlist
    python tools/insider_scan.py NVDA AMD        # explicit
    python tools/insider_scan.py --days 30
    python tools/insider_scan.py --json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.insider import get_insider_activity
from data.watchlist import load_watchlist


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    tickers = [t.upper() for t in args.tickers] or load_watchlist()
    if not tickers:
        print("No tickers.", file=sys.stderr)
        return 2

    results = {t: get_insider_activity(t, days=args.days) for t in tickers}

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return 0

    print("=" * 78)
    print(f"  INSIDER FORM-4 SCAN  —  last {args.days}d")
    print("=" * 78)
    for t, r in results.items():
        sig = r.get("signal", "UNKNOWN")
        print(f"  {t:<6}  {sig:<14}  {r.get('summary', '')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
