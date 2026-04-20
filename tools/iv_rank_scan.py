#!/usr/bin/env python3
"""
IV Rank Scanner — standalone CLI tool.

Usage:
    python tools/iv_rank_scan.py AAPL
    python tools/iv_rank_scan.py TSLA NVDA MSFT AMZN

Computes IV rank for each ticker: where does today's front-month ATM IV
sit relative to the range of realized vols over the past 252 trading days?

IV Rank > 80th pct → historically expensive → lean sell vol / spreads
IV Rank < 20th pct → historically cheap     → lean buy vol / straddles

Model: Uses 252-day rolling 30-day realized volatility as the IV proxy
baseline. This is a free-data alternative to paid historical-IV databases.
Direction is reliable even if absolute levels differ from true IV history.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
from analysis.vol import calculate_rv, iv_rank
from data.market import get_current_price


def _atm_iv(symbol: str, spot: float) -> float | None:
    try:
        ticker = yf.Ticker(symbol)
        exps = ticker.options
        if not exps:
            return None
        chain = ticker.option_chain(exps[0])
        calls = chain.calls
        if calls.empty:
            return None
        atm = calls.iloc[(calls["strike"] - spot).abs().argsort()[:1]]
        iv = float(atm["impliedVolatility"].iloc[0])
        return iv if iv > 0.001 else None
    except Exception:
        return None


def run(symbol: str) -> None:
    symbol = symbol.upper().strip()
    print(f"\n  {symbol}", end="  ")

    spot = get_current_price(symbol)
    if spot is None:
        print("(price unavailable)")
        return

    # 252 trading days ≈ 1 year for proper IV rank baseline
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="252d")
        if hist.empty or len(hist) < 60:
            print("(insufficient history)")
            return
        prices = hist["Close"]
    except Exception:
        print("(history fetch failed)")
        return

    rv30 = calculate_rv(prices, window=30)
    if rv30 is None:
        print("(RV calc failed)")
        return

    # Use actual ATM IV if available, else use current RV as proxy
    iv = _atm_iv(symbol, spot) or rv30
    rank_data = iv_rank(iv, prices)

    rank_pct = rank_data.get("iv_rank_pct")
    label    = rank_data.get("iv_rank_label", "N/A")

    bar_len = int((rank_pct or 50) / 5)
    bar = ("█" * bar_len).ljust(20)
    lean = "→ BUY VOL" if (rank_pct or 50) < 20 else ("→ SELL VOL" if (rank_pct or 50) > 80 else "")

    print(f"${spot:>7.2f}  IV {iv*100:>5.1f}%  [{bar}] {rank_pct:>5.1f}th pct  {label}  {lean}")


def main() -> None:
    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["SPY", "QQQ", "AAPL", "NVDA", "TSLA"]
    print(f"\n{'='*80}")
    print(f"  IV Rank Scanner  (252-day RV baseline)")
    print(f"{'='*80}")
    print(f"  {'Ticker':<8} {'Spot':>8}  {'IV':>8}  {'Rank bar':<24}  {'Pct':>6}  Label")
    print(f"  {'-'*72}")
    for t in tickers:
        run(t)
    print()


if __name__ == "__main__":
    main()
