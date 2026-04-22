#!/usr/bin/env python3
"""
Unusual-volume / block-proxy scan.

Usage:
    python tools/blocks_scan.py
    python tools/blocks_scan.py NVDA AMD AAPL
    python tools/blocks_scan.py --json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.blocks import get_unusual_volume
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

    results = {t: get_unusual_volume(t) for t in tickers}

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return 0

    print("=" * 78)
    print("  UNUSUAL VOLUME / BLOCK-PROXY SCAN")
    print("=" * 78)
    print(f"  {'Ticker':<7} {'Signal':<20} {'Vol×':>6} {'Px %':>7}")
    print("-" * 78)
    for t, r in results.items():
        vr = r.get("volume_ratio")
        pc = r.get("price_change_pct")
        vr_s = f"{vr:>5.1f}×" if isinstance(vr, (int, float)) else "  n/a"
        pc_s = f"{pc:>+6.2f}%" if isinstance(pc, (int, float)) else "  n/a"
        print(f"  {t:<7} {r.get('signal','UNKNOWN'):<20} {vr_s} {pc_s}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
