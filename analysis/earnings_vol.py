"""
Earnings volatility edge detector.

Most retail and even institutional traders overpay or underpay for
options into earnings without benchmarking the implied expected move
against what the stock has *actually* done on past earnings days.

Strategy:
  expected_move  = ATM straddle price / spot   (what options are pricing)
  historical_avg = average abs % move post-earnings over last 6-8 quarters

  edge_ratio = historical_avg / expected_move

  edge_ratio > 1.3  → market underpricing the move  → BUY STRADDLE
  edge_ratio < 0.7  → market overpricing the move   → SELL STRADDLE (spread)
  else              → fairly priced; no earnings edge

Returns None if earnings are more than 45 days away, no history, or
data is insufficient to form a view.
"""

import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta


_MIN_HISTORY      = 3    # require at least 3 past earnings moves
_EDGE_BUY_RATIO   = 1.30
_EDGE_SELL_RATIO  = 0.70
_MAX_DAYS_TO_EARN = 45   # only relevant near earnings


def analyze_earnings_edge(
    symbol: str,
    chain: pd.DataFrame,
    spot: float,
    earnings_date: datetime | None,
) -> dict | None:
    """
    Returns an earnings-vol edge dict, or None if not applicable.

    Args:
        symbol:        ticker symbol
        chain:         full unfiltered options chain DataFrame
        spot:          current stock price
        earnings_date: next earnings datetime (from get_options_chain) or None
    """
    # Only compute when earnings are coming up
    if earnings_date is None:
        return None
    days_to_earnings = (earnings_date - datetime.now()).days
    if not (0 < days_to_earnings <= _MAX_DAYS_TO_EARN):
        return None

    # ── Historical moves ───────────────────────────────────────────────────────
    moves = _historical_earnings_moves(symbol)
    if len(moves) < _MIN_HISTORY:
        return None

    avg_move    = round(float(np.mean(moves)), 2)
    median_move = round(float(np.median(moves)), 2)

    # ── Expected move from ATM straddle ───────────────────────────────────────
    expected_move = _straddle_expected_move(chain, spot, days_to_earnings)
    if expected_move is None:
        return None

    edge_ratio = round(avg_move / expected_move, 3) if expected_move > 0 else None
    if edge_ratio is None:
        return None

    # ── Signal ────────────────────────────────────────────────────────────────
    if edge_ratio >= _EDGE_BUY_RATIO:
        signal = "STRADDLE BUY"
        reason = (
            f"Avg earnings move {avg_move:.1f}% vs market expecting "
            f"{expected_move:.1f}% — options underpriced by "
            f"{(edge_ratio - 1) * 100:.0f}%"
        )
    elif edge_ratio <= _EDGE_SELL_RATIO:
        signal = "IV RICH"
        reason = (
            f"Avg earnings move {avg_move:.1f}% vs market expecting "
            f"{expected_move:.1f}% — options overpriced by "
            f"{(1 - edge_ratio) * 100:.0f}%"
        )
    else:
        signal = "FAIRLY PRICED"
        reason = (
            f"Avg earnings move {avg_move:.1f}% vs expected {expected_move:.1f}% "
            f"(edge ratio {edge_ratio:.2f})"
        )

    return {
        "days_to_earnings":          days_to_earnings,
        "expected_move_pct":         expected_move,
        "avg_historical_move_pct":   avg_move,
        "median_historical_move_pct": median_move,
        "historical_moves":          moves,
        "edge_ratio":                edge_ratio,
        "signal":                    signal,
        "reason":                    reason,
    }


# ── private helpers ────────────────────────────────────────────────────────────

def _historical_earnings_moves(symbol: str) -> list[float]:
    """Return absolute % price moves on the day after each earnings date."""
    try:
        ticker = yf.Ticker(symbol)
        edates = ticker.earnings_dates
        if edates is None or edates.empty:
            return []

        prices = ticker.history(period="3y")
        if prices.empty:
            return []

        # Normalise timezone
        price_idx = prices.index
        if hasattr(price_idx, "tz") and price_idx.tz is not None:
            price_idx = price_idx.tz_localize(None)
        prices = prices.copy()
        prices.index = price_idx

        moves = []
        for raw_date in edates.index[:8]:
            try:
                ed = pd.Timestamp(raw_date)
                if ed.tzinfo is not None:
                    ed = ed.tz_localize(None)

                # Close before earnings
                before = prices[prices.index <= ed]
                after  = prices[prices.index >  ed]
                if before.empty or after.empty:
                    continue

                pre  = float(before["Close"].iloc[-1])
                post = float(after["Close"].iloc[0])
                move = abs((post - pre) / pre) * 100
                if 0.01 < move < 50:  # sanity filter
                    moves.append(round(move, 2))
            except Exception:
                continue

        return moves
    except Exception:
        return []


def _straddle_expected_move(
    chain: pd.DataFrame, spot: float, days_to_earnings: int
) -> float | None:
    """
    Expected move from the ATM straddle price closest to earnings date.
    Uses the expiry just after earnings (shortest DTE > days_to_earnings).
    """
    if chain.empty:
        return None

    # Pick expiry just after earnings
    valid = chain[chain["dte"] > days_to_earnings]
    if valid.empty:
        valid = chain  # fallback: use whatever is available

    target_dte = int(valid["dte"].min())
    front = chain[chain["dte"] == target_dte]

    calls = front[front["type"] == "call"]
    puts  = front[front["type"] == "put"]
    if calls.empty or puts.empty:
        return None

    # ATM strike
    atm_call = calls.iloc[(calls["strike"] - spot).abs().argsort()[:1]]
    atm_put  = puts.iloc[(puts["strike"] - spot).abs().argsort()[:1]]

    def _mid(row: pd.Series) -> float:
        bid = float(row.get("bid") or 0)
        ask = float(row.get("ask") or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return float(row.get("lastPrice") or ask or bid or 0)

    call_price = _mid(atm_call.iloc[0])
    put_price  = _mid(atm_put.iloc[0])
    straddle   = call_price + put_price

    if straddle <= 0 or spot <= 0:
        return None

    return round((straddle / spot) * 100, 2)
