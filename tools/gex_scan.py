#!/usr/bin/env python3
"""
GEX Scanner — standalone CLI tool.

Usage:
    python tools/gex_scan.py AAPL
    python tools/gex_scan.py TSLA NVDA MSFT

Prints gamma exposure summary for each ticker: gamma wall, gamma flip,
regime signal, and a strike-by-strike GEX breakdown.

Model: Black-Scholes gamma (analysis/greeks.py) applied to the full
unfiltered options chain. No approximation — uses actual OI and IV per
contract. Dealer convention: long calls = +GEX, long puts = -GEX.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd
from data.market import get_current_price, get_options_chain
from analysis.gamma import calculate_gex


def run(symbol: str) -> None:
    symbol = symbol.upper().strip()
    print(f"\n{'='*55}")
    print(f"  GEX Analysis: {symbol}")
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

    result = calculate_gex(chain, price)

    print(f"  GEX Signal  : {result['gex_signal']}")
    print(f"  Total GEX   : ${result['total_gex']:,.0f}")
    if result["gamma_wall"]:
        dist = (result["gamma_wall"] - price) / price * 100
        print(f"  Gamma Wall  : ${result['gamma_wall']:.0f}  ({dist:+.1f}% from spot)")
    if result["gamma_flip"]:
        dist = (result["gamma_flip"] - price) / price * 100
        print(f"  Gamma Flip  : ${result['gamma_flip']:.0f}  ({dist:+.1f}% from spot)")
    print(f"  Summary     : {result['gex_summary']}")

    gex_df = result["gex_by_strike"]
    if not gex_df.empty:
        print(f"\n  Top strikes by |net GEX| (near ±10% of spot):")
        near = gex_df[abs(gex_df["strike"] - price) / price <= 0.10].copy()
        near = near.reindex(near["net_gex"].abs().sort_values(ascending=False).index)
        for _, r in near.head(8).iterrows():
            bar_len = int(abs(r["net_gex"]) / (near["net_gex"].abs().max() + 1e-9) * 20)
            bar = ("█" * bar_len).ljust(20)
            sign = "+" if r["net_gex"] >= 0 else "-"
            print(f"    ${r['strike']:>7.0f}  {sign}{bar}  ${abs(r['net_gex']):,.0f}")


def main() -> None:
    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["SPY"]
    for t in tickers:
        run(t)
    print()


if __name__ == "__main__":
    main()
