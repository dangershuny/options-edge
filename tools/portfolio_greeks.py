#!/usr/bin/env python3
"""
Portfolio Greeks aggregator CLI.

Reads an open-positions JSON file (default: positions.json in repo root) and
prints net delta/gamma/theta/vega + a risk-heat check.

positions.json format:
    [
      {"symbol":"AAPL","type":"call","strike":200,"expiry":"2026-06-18",
       "contracts":2,"entry_price":5.40},
      ...
    ]

Usage:
    python tools/portfolio_greeks.py
    python tools/portfolio_greeks.py --file my_book.json
    python tools/portfolio_greeks.py --json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from risk.portfolio import aggregate_greeks, check_portfolio_heat

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FILE = os.path.join(_ROOT, "positions.json")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=DEFAULT_FILE)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        print(f"No positions file at {args.file}.", file=sys.stderr)
        print("Create a positions.json (see tools/portfolio_greeks.py docstring).",
              file=sys.stderr)
        return 2

    try:
        with open(args.file, encoding="utf-8") as f:
            positions = json.load(f)
    except Exception as e:
        print(f"Failed to parse {args.file}: {e}", file=sys.stderr)
        return 2

    agg = aggregate_greeks(positions)
    heat = check_portfolio_heat(agg)

    if args.json:
        print(json.dumps({"aggregate": agg, "heat": heat}, indent=2, default=str))
        return 0

    print("=" * 78)
    print(f"  PORTFOLIO GREEKS  —  {len(positions)} positions "
          f"({agg.get('position_count', 0)} contracts)")
    print("=" * 78)
    print(f"  Net delta  : {agg['net_delta']:+.2f}")
    print(f"  Net gamma  : {agg['net_gamma']:+.4f}")
    print(f"  Net theta  : {agg['net_theta']:+.2f} / day")
    print(f"  Net vega   : {agg['net_vega']:+.2f} / vol-pt")
    print(f"  Max loss   : ${agg['total_max_loss']:.2f}")
    print()

    by_sym = agg.get("by_symbol") or {}
    if by_sym:
        print("  By symbol:")
        print(f"  {'Sym':<6} {'Delta':>10} {'Gamma':>10} {'Theta':>10} {'Vega':>10} {'Cost':>10}")
        for sym, b in sorted(by_sym.items()):
            print(f"  {sym:<6} {b['delta']:>10.2f} {b['gamma']:>10.4f} "
                  f"{b['theta']:>10.2f} {b['vega']:>10.2f} {b['cost']:>10.2f}")
        print()

    if heat.get("ok"):
        print("  Heat check: ✅ within risk limits")
    else:
        print("  Heat check: ⚠️")
        for w in heat.get("warnings", []):
            print(f"    ! {w}")

    for w in agg.get("warnings") or []:
        print(f"  note: {w}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
