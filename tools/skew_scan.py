#!/usr/bin/env python3
"""
Skew Scanner — standalone CLI tool.

Usage:
    python tools/skew_scan.py AAPL
    python tools/skew_scan.py TSLA NVDA SPY

Prints put/call ratio, 25-delta risk reversal, and term structure slope
for each ticker. These are leading indicators — smart money positioning
often shows up in the skew before the stock price moves.

Model: Volume-weighted IV comparison across OTM bands (3-10% from spot).
Normal equity skew is negative (puts more expensive than calls).
Unusually positive skew = unusual bullish conviction.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.market import get_current_price, get_options_chain
from analysis.skew import calculate_skew


def run(symbol: str) -> None:
    symbol = symbol.upper().strip()
    print(f"\n{'='*55}")
    print(f"  Skew Analysis: {symbol}")
    print(f"{'='*55}")

    price = get_current_price(symbol)
    if price is None:
        print(f"  ERROR: Could not fetch price for {symbol}")
        return
    print(f"  Spot: ${price:.2f}")

    chain, _, err = get_options_chain(symbol)
    if err or chain is None:
        print(f"  ERROR: {err}")
        return

    result = calculate_skew(chain, price)

    print(f"  Skew Signal    : {result['skew_signal']}")
    print(f"  P/C Ratio      : {result['pc_ratio']:.3f}" if result["pc_ratio"] else "  P/C Ratio      : N/A")

    if result["risk_reversal"] is not None:
        rr = result["risk_reversal"]
        interp = "calls > puts (bullish)" if rr > 0 else "puts > calls (bearish/normal)"
        print(f"  Risk Reversal  : {rr:+.2f} vol pts  ({interp})")
    else:
        print("  Risk Reversal  : N/A")

    if result["term_slope"] is not None:
        ts = result["term_slope"]
        struct = "contango (normal)" if ts >= 0 else "backwardation ⚠️ (near-term fear elevated)"
        print(f"  Term Slope     : {ts:+.2f} vol pts  ({struct})")
    else:
        print("  Term Slope     : N/A")

    print(f"  Summary        : {result['skew_summary']}")

    # Interpretation
    print()
    if result["skew_signal"] == "BULLISH":
        print("  ⚡ Unusual bullish skew — consider call buying or bull spreads")
    elif result["skew_signal"] == "BEARISH":
        print("  ⚠️  Heavy put demand — consider put spreads or protective positioning")

    if result["pc_ratio"] is not None:
        if result["pc_ratio"] < 0.60:
            print(f"  📈 P/C {result['pc_ratio']:.2f} — strong call-side flow (contrarian: may be overbought)")
        elif result["pc_ratio"] > 1.40:
            print(f"  📉 P/C {result['pc_ratio']:.2f} — heavy put buying (contrarian: potential reversal setup)")


def main() -> None:
    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["SPY"]
    for t in tickers:
        run(t)
    print()


if __name__ == "__main__":
    main()
