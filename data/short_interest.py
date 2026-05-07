"""
Short interest / squeeze setup detector.

Uses yfinance's `info` fields for short data (shortPercentOfFloat, shortRatio
aka days-to-cover). Free, requires no keys. Cached per process.

Returns a SAFE-DEFAULT dict on any failure.

Interpretation:
  short_float > 20%   — crowded short; squeeze potential on positive catalyst
  days_to_cover > 5   — illiquid short leg; cover-forced moves can be explosive
  Both together       — classic squeeze setup

This is a conditional edge — it only matters when there's a BUY CALL signal
with a catalyst. Alone it's not a trade.
"""

from __future__ import annotations

import time
import yfinance as yf

CACHE_TTL_SEC = 1800   # 30 min
_cache: dict[str, tuple[float, dict]] = {}


def get_short_interest(ticker: str) -> dict:
    """
    Returns:
        {
          'ticker': str,
          'short_float_pct': float | None   — % of float shorted (0–100)
          'days_to_cover':   float | None   — short interest / avg daily volume
          'signal':          'SQUEEZE_SETUP' | 'ELEVATED' | 'NORMAL' | 'UNKNOWN',
          'summary':         str,
          'source':          'yfinance' | 'degraded',
        }
    """
    ticker = ticker.upper().strip()
    now = time.time()
    cached = _cache.get(ticker)
    if cached and (now - cached[0]) < CACHE_TTL_SEC:
        return cached[1]

    result = _degraded(ticker, "unknown")
    try:
        info = yf.Ticker(ticker).info
        if not isinstance(info, dict):
            _cache[ticker] = (now, result)
            return result

        raw_pct   = info.get("shortPercentOfFloat")
        days_cov  = info.get("shortRatio")

        short_pct = float(raw_pct) * 100 if raw_pct is not None else None
        dtc       = float(days_cov) if days_cov is not None else None

        # Signal classification
        if short_pct is None and dtc is None:
            signal = "UNKNOWN"
        elif (short_pct is not None and short_pct >= 20) and (dtc is not None and dtc >= 5):
            signal = "SQUEEZE_SETUP"
        elif (short_pct is not None and short_pct >= 15) or (dtc is not None and dtc >= 4):
            signal = "ELEVATED"
        else:
            signal = "NORMAL"

        parts = []
        if short_pct is not None:
            parts.append(f"short {short_pct:.1f}% of float")
        if dtc is not None:
            parts.append(f"{dtc:.1f}d to cover")
        summary = ", ".join(parts) if parts else "no short data"

        result = {
            "ticker":          ticker,
            "short_float_pct": round(short_pct, 2) if short_pct is not None else None,
            "days_to_cover":   round(dtc, 2) if dtc is not None else None,
            "signal":          signal,
            "summary":         summary,
            "source":          "yfinance",
        }
    except Exception as e:
        result = _degraded(ticker, f"error: {e}")

    _cache[ticker] = (now, result)
    return result


def short_interest_score_delta(short: dict | None, opt_type: str, vol_signal: str) -> float:
    """
    Score adjustment from short interest.

    2026-05-06 BACKTEST RETUNE: signal_edge_backtest showed
    short_signal=SQUEEZE_SETUP at 52.6% win rate / +12.8% avg return at d1
    (n=38) — the strongest non-PINNED category in the dataset. Boosting
    its weight from +7 to +15 to reflect realized predictive power.

    SQUEEZE_SETUP + BUY CALL → +15 (was +7)
    ELEVATED     + BUY CALL → +5  (was +3)
    SQUEEZE_SETUP + BUY PUT → -8  (was -4) — strengthen the don't-fight-it
                                            penalty symmetrically
    Otherwise               → 0
    """
    if not short or not isinstance(short, dict):
        return 0.0
    sig = short.get("signal")
    opt = (opt_type or "").lower()
    if vol_signal not in ("BUY VOL", "FLOW BUY", "DIRECTIONAL BUY",
                           "MOMENTUM BUY"):
        return 0.0
    if opt == "call" and sig == "SQUEEZE_SETUP":
        return 15.0
    if opt == "call" and sig == "ELEVATED":
        return 5.0
    if opt == "put" and sig == "SQUEEZE_SETUP":
        return -8.0
    return 0.0


def _degraded(ticker: str, reason: str) -> dict:
    return {
        "ticker":          ticker,
        "short_float_pct": None,
        "days_to_cover":   None,
        "signal":          "UNKNOWN",
        "summary":         f"short data unavailable ({reason})",
        "source":          "degraded",
    }
