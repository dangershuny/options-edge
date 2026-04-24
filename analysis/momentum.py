"""
Momentum signals: 14-period RSI (Wilder smoothing).

Used as an entry-timing filter on top of the vol-based signals.
Thresholds are deliberately conservative (25/75) — only extreme
readings move the score, avoiding noise in the 30–70 mid range.

Scoring (per-contract delta, ±5 cap):

  OVERSOLD  (RSI ≤ 25)
    BUY CALL           → +5   bounce candidate, entry is cheap
    BUY PUT            → -5   already oversold, reversion risk
    SELL VOL (put)     → -5   sold below support, bad R/R
    SELL VOL (call)    → +5   bounce likely stalls at resistance

  OVERBOUGHT (RSI ≥ 75)
    BUY CALL           → -5   trend exhausted, bad entry
    BUY PUT            → +5   reversal candidate
    SELL VOL (call)    → +5   sell into exhaustion
    SELL VOL (put)     → -5   rally could crack support

  NEUTRAL (25 < RSI < 75) → 0
"""

import pandas as pd


RSI_OVERSOLD   = 25.0
RSI_OVERBOUGHT = 75.0
RSI_PERIOD     = 14
MAX_DELTA      = 5.0


def calculate_rsi(prices: pd.Series, period: int = RSI_PERIOD) -> float | None:
    """
    Wilder's RSI on the given close-price series.

    Returns the latest RSI value (0–100) or None if there isn't enough
    history to produce a stable reading (need at least 2×period bars so
    the EWM has warmed up).
    """
    if prices is None or len(prices) < period * 2:
        return None

    delta = prices.diff().dropna()
    gains  = delta.clip(lower=0)
    losses = (-delta).clip(lower=0)

    # Wilder smoothing = EWM with alpha = 1/period, adjust=False
    avg_gain = gains.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, adjust=False).mean()

    last_gain = float(avg_gain.iloc[-1])
    last_loss = float(avg_loss.iloc[-1])

    if last_loss == 0:
        return 100.0 if last_gain > 0 else 50.0

    rs  = last_gain / last_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(float(rsi), 2)


def rsi_zone(rsi: float | None) -> str:
    if rsi is None:
        return "unknown"
    if rsi <= RSI_OVERSOLD:
        return "oversold"
    if rsi >= RSI_OVERBOUGHT:
        return "overbought"
    return "neutral"


def rsi_info(prices: pd.Series) -> dict:
    """Compute RSI once per ticker; cached into the per-contract rows."""
    rsi = calculate_rsi(prices)
    return {
        "rsi_14":   rsi,
        "rsi_zone": rsi_zone(rsi),
    }


def rsi_score_delta(info: dict | None, vol_signal: str, opt_type: str) -> float:
    """
    Return the per-contract RSI score adjustment.

    Only extreme readings move the score (±5 cap). Mid-range RSI returns 0.
    """
    if not info:
        return 0.0
    zone = info.get("rsi_zone")
    if zone not in ("oversold", "overbought"):
        return 0.0

    opt = (opt_type or "").lower()
    sig = vol_signal or ""

    if zone == "oversold":
        if sig in ("BUY VOL", "FLOW BUY"):
            return +MAX_DELTA if opt == "call" else -MAX_DELTA
        if sig == "SELL VOL":
            # Selling premium near support is better for calls (rally stalls
            # at resistance) than for puts (breakdown through support).
            return +MAX_DELTA if opt == "call" else -MAX_DELTA
        return 0.0

    if zone == "overbought":
        if sig in ("BUY VOL", "FLOW BUY"):
            return -MAX_DELTA if opt == "call" else +MAX_DELTA
        if sig == "SELL VOL":
            # Selling into exhaustion favours call credit spreads; bearish
            # reversal can break put-spread supports.
            return +MAX_DELTA if opt == "call" else -MAX_DELTA
        return 0.0

    return 0.0
