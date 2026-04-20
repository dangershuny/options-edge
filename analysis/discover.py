"""
Discovery scan: find the biggest IV vs realized-vol mismatches
across the full universe without requiring a pre-set watchlist.

Strategy (fast + cheap):
  1. Batch-download 90 days of daily closes for all universe tickers
     in a single yfinance call.
  2. Calculate 30-day realized vol for each.
  3. Fetch only the front-month ATM option for each ticker to get IV —
     one options call per ticker, one strike, one expiry.
  4. Rank by abs(IV - RV). Top candidates get a full analyze_ticker()
     pass for trade recommendations.
"""

import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from data.universe import UNIVERSE
from analysis.vol import calculate_rv, iv_rv_signal


def _atm_iv(symbol: str, current_price: float) -> float | None:
    """Return the front-month ATM implied vol for a single ticker."""
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None
        exp = expirations[0]
        chain = ticker.option_chain(exp)
        calls = chain.calls
        if calls.empty:
            return None
        atm = calls.iloc[(calls["strike"] - current_price).abs().argsort()[:1]]
        iv = float(atm["impliedVolatility"].iloc[0])
        return iv if iv > 0.001 else None
    except Exception:
        return None


def _quick_scan_ticker(symbol: str, prices: pd.Series) -> dict | None:
    """
    Given pre-fetched price history, compute RV and fetch ATM IV.
    Returns a lightweight dict or None if data is insufficient.
    """
    rv = calculate_rv(prices, window=30)
    if rv is None or rv <= 0:
        return None

    current_price = float(prices.iloc[-1])
    iv = _atm_iv(symbol, current_price)
    if iv is None:
        return None

    signal, spread, strength = iv_rv_signal(iv, rv)
    return {
        "symbol": symbol,
        "price": round(current_price, 2),
        "iv_pct": round(iv * 100, 1),
        "rv_pct": round(rv * 100, 1),
        "iv_rv_spread": round(spread * 100, 1),
        "abs_spread": abs(spread),
        "vol_signal": signal,
        "strength": strength,
    }


def run_discovery(top_n: int = 20, max_workers: int = 12) -> pd.DataFrame:
    """
    Scan the universe and return the top_n tickers ranked by
    absolute IV vs RV mismatch.

    Args:
        top_n:       how many top candidates to return
        max_workers: parallel threads for ATM IV fetching

    Returns:
        DataFrame sorted by abs_spread descending, NEUTRAL entries excluded.
    """
    # Step 1: batch price download — one network call for all tickers
    raw = yf.download(
        UNIVERSE,
        period="90d",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    if raw.empty:
        return pd.DataFrame()

    closes = raw["Close"] if "Close" in raw.columns else raw.xs("Close", axis=1, level=0)

    # Step 2: parallel ATM IV fetch + RV calculation
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for sym in UNIVERSE:
            if sym not in closes.columns:
                continue
            series = closes[sym].dropna()
            if len(series) < 35:
                continue
            futures[pool.submit(_quick_scan_ticker, sym, series)] = sym

        for fut in as_completed(futures):
            result = fut.result()
            if result is not None and result["vol_signal"] != "NEUTRAL":
                results.append(result)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results).sort_values("abs_spread", ascending=False)
    return df.head(top_n).reset_index(drop=True)
