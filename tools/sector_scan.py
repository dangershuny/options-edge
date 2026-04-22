#!/usr/bin/env python3
"""
Sector Rotation Scan — groups today's watchlist signals by sector.

Usage:
    python tools/sector_scan.py                    # scan current watchlist
    python tools/sector_scan.py AAPL MSFT JPM GS   # scan explicit tickers
    python tools/sector_scan.py --json

Pulls BUY VOL / FLOW BUY signals via analyze_ticker, then runs
detect_rotation() to surface sector-concentrated flows.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.scorer import analyze_ticker
from analysis.sector_rotation import detect_rotation
from data.watchlist import load_watchlist


def _extract_rows(ticker: str) -> list[dict]:
    rows: list[dict] = []
    try:
        res = analyze_ticker(ticker)
    except Exception as e:
        print(f"  ! {ticker}: analyze failed ({e})", file=sys.stderr)
        return rows
    for t in (res or {}).get("trades", []) or []:
        sig = t.get("vol_signal")
        if sig not in ("BUY VOL", "FLOW BUY"):
            continue
        rows.append({
            "symbol":     ticker,
            "type":       t.get("option_type"),
            "vol_signal": sig,
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Detect sector-concentrated option flows.")
    ap.add_argument("tickers", nargs="*", help="tickers (default: current watchlist)")
    ap.add_argument("--json", action="store_true", help="emit raw JSON")
    args = ap.parse_args()

    tickers = [t.upper() for t in args.tickers] or load_watchlist()
    if not tickers:
        print("No tickers to scan.", file=sys.stderr)
        return 2

    all_rows: list[dict] = []
    for t in tickers:
        all_rows.extend(_extract_rows(t))

    report = detect_rotation(all_rows)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    print("=" * 78)
    print("  SECTOR ROTATION SCAN")
    print("=" * 78)
    print(f"  Signals collected: {len(all_rows)} across {len(tickers)} tickers")
    print(f"  {report.get('summary', '')}")
    print()

    rotations = report.get("rotations") or []
    if not rotations:
        print("  No concentrated rotation detected.")
        return 0

    for r in rotations:
        print(f"  [{r.get('strength','NORMAL')}] {r.get('sector')} → {r.get('direction')}")
        print(f"      tickers:  {', '.join(r.get('tickers', []))}")
        print(f"      contracts: {r.get('contract_count', 0)}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
