#!/usr/bin/env python3
"""
Earnings Edge Scanner — standalone CLI tool.

Usage:
    python tools/earnings_edge_scan.py AAPL
    python tools/earnings_edge_scan.py TSLA NVDA META AMZN

Compares the ATM straddle's implied expected move against the stock's
actual historical earnings moves. If options underestimate the typical
move by 30%+, there's a straddle-buy edge. If they overestimate by 30%+,
IV is rich and a credit spread is preferred.

Model: Expected move = (ATM call ask + ATM put ask) / spot.
Historical move = median absolute % change on first post-earnings close
across the last 6-8 quarters (yfinance earnings_dates).
Edge ratio = historical_avg / expected_move.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
import yfinance as yf
from data.market import get_current_price, get_options_chain, get_earnings_date
from analysis.earnings_vol import analyze_earnings_edge, _historical_earnings_moves


def run(symbol: str) -> None:
    symbol = symbol.upper().strip()
    print(f"\n{'='*55}")
    print(f"  Earnings Edge: {symbol}")
    print(f"{'='*55}")

    price = get_current_price(symbol)
    if price is None:
        print(f"  ERROR: Could not fetch price for {symbol}")
        return
    print(f"  Spot: ${price:.2f}")

    ticker = yf.Ticker(symbol)
    earnings_date = get_earnings_date(ticker)

    if earnings_date is None:
        print("  No upcoming earnings date found.")
        # Still show historical moves
        moves = _historical_earnings_moves(symbol)
        if moves:
            print(f"  Historical earnings moves: {', '.join(f'{m:.1f}%' for m in moves)}")
        return

    days = (earnings_date - datetime.now()).days
    print(f"  Earnings date : {earnings_date.strftime('%Y-%m-%d')}  ({days} days)")

    chain, _, err = get_options_chain(symbol)
    if err or chain is None:
        print(f"  ERROR: {err}")
        moves = _historical_earnings_moves(symbol)
        if moves:
            print(f"  Historical moves: {', '.join(f'{m:.1f}%' for m in moves)}")
        return

    result = analyze_earnings_edge(symbol, chain, price, earnings_date)

    if result is None:
        moves = _historical_earnings_moves(symbol)
        if moves:
            print(f"  Insufficient data for edge calc.")
            print(f"  Historical moves: {', '.join(f'{m:.1f}%' for m in moves)}")
        else:
            print("  No historical earnings data available.")
        return

    print(f"  Signal         : {result['signal']}")
    print(f"  Expected move  : {result['expected_move_pct']:.1f}%")
    print(f"  Avg historical : {result['avg_historical_move_pct']:.1f}%")
    print(f"  Median hist    : {result['median_historical_move_pct']:.1f}%")
    print(f"  Edge ratio     : {result['edge_ratio']:.2f}x  (>1.3 = buy, <0.7 = sell)")
    print(f"  Past moves     : {', '.join(f'{m:.1f}%' for m in result['historical_moves'])}")
    print(f"  Reason         : {result['reason']}")

    print()
    if result["signal"] == "STRADDLE BUY":
        print("  ✅ Buy ATM straddle (or strangle) before earnings")
        print(f"     Market pricing {result['expected_move_pct']:.1f}% move —")
        print(f"     stock historically moves {result['avg_historical_move_pct']:.1f}%")
    elif result["signal"] == "IV RICH":
        print("  ⚠️  IV is expensive pre-earnings — consider credit spread instead")
        print(f"     Market pricing {result['expected_move_pct']:.1f}% move —")
        print(f"     stock historically moves only {result['avg_historical_move_pct']:.1f}%")


def main() -> None:
    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["AAPL"]
    for t in tickers:
        run(t)
    print()


if __name__ == "__main__":
    main()
