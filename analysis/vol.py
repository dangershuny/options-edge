"""
Volatility calculations: realized vol, IV rank, directional semi-deviation,
and DTE-matched window selection.
"""

import numpy as np
import pandas as pd


# ── Signal thresholds ──────────────────────────────────────────────────────────
# Relaxed from -20%/-25% to surface more tradeable candidates.
# Score does the ranking; thresholds just gate eligibility.
BUY_VOL_THRESHOLD  = -0.05   # IV is 5%+ below RV  → cheap options, buy (relaxed from -10%)
SELL_VOL_THRESHOLD =  0.15   # IV is 15%+ above RV  → expensive options, sell/spread


def calculate_rv(prices: pd.Series, window: int = 30) -> float | None:
    """Annualized realized vol from the last `window` trading days of close prices."""
    log_returns = np.log(prices / prices.shift(1)).dropna()
    if len(log_returns) < window:
        return None
    rv = float(log_returns.tail(window).std() * np.sqrt(252))
    return rv


def calculate_rv_for_dte(prices: pd.Series, dte: int) -> float | None:
    """
    Pick a RV window that matches the option's time horizon.

    Comparing a 7-DTE option's IV against 30-day RV overstates the
    mismatch — the 7-day realized vol is the right benchmark.
    """
    if dte <= 14:
        window = 10
    elif dte <= 30:
        window = 20
    elif dte <= 60:
        window = 30
    else:
        window = 45
    return calculate_rv(prices, window)


def calculate_directional_rv(prices: pd.Series, window: int = 30) -> dict:
    """
    Directional semi-deviation: separate upside and downside realized vol.

    For CALL options, compare IV to upside_rv.
    For PUT options, compare IV to downside_rv.

    Method: RMS (root-mean-square) of the relevant signed returns,
    annualized.  ×√2 corrects for using half the return distribution.

    Returns:
        upside_rv   float | None  — annualized vol of positive-return days
        downside_rv float | None  — annualized vol of negative-return days
        combined_rv float | None  — standard 2-sided RV for reference
    """
    log_returns = np.log(prices / prices.shift(1)).dropna().tail(window)

    if len(log_returns) < window // 2:
        return {"upside_rv": None, "downside_rv": None, "combined_rv": None}

    pos = log_returns[log_returns > 0]
    neg = log_returns[log_returns < 0]

    # RMS × √(annualisation) × √2 (half-sample correction)
    upside_rv   = float(np.sqrt(np.mean(pos ** 2)) * np.sqrt(252) * np.sqrt(2)) if len(pos) > 3 else None
    downside_rv = float(np.sqrt(np.mean(neg ** 2)) * np.sqrt(252) * np.sqrt(2)) if len(neg) > 3 else None
    combined_rv = float(log_returns.std() * np.sqrt(252))

    return {
        "upside_rv":   round(upside_rv,   4) if upside_rv   else None,
        "downside_rv": round(downside_rv, 4) if downside_rv else None,
        "combined_rv": round(combined_rv, 4),
    }


def iv_rv_signal(iv: float, rv: float, threshold_buy: float = BUY_VOL_THRESHOLD,
                 threshold_sell: float = SELL_VOL_THRESHOLD) -> tuple[str, float, float]:
    """
    Compare implied vol to realized vol.

    Returns:
        signal:   'BUY VOL' | 'SELL VOL' | 'NEUTRAL'
        spread:   IV - RV  (annualized decimal)
        strength: 0.0–1.0
    """
    spread   = iv - rv
    pct_diff = spread / rv if rv > 0 else 0.0

    if pct_diff > threshold_sell:
        return "SELL VOL", spread, min(abs(pct_diff), 1.0)
    elif pct_diff < threshold_buy:
        return "BUY VOL", spread, min(abs(pct_diff), 1.0)
    else:
        return "NEUTRAL", spread, 0.0


def iv_rv_signal_directional(
    iv: float,
    opt_type: str,
    directional: dict,
    dte: int,
) -> tuple[str, float, float]:
    """
    Direction-aware signal: compares call IV to upside RV, put IV to downside RV.
    Falls back to combined_rv if directional data is missing.

    Args:
        iv:          implied vol of this specific contract
        opt_type:    'call' or 'put'
        directional: output of calculate_directional_rv()
        dte:         days to expiry (used for additional DTE-match weighting)
    """
    if opt_type.lower() == "call":
        rv = directional.get("upside_rv") or directional.get("combined_rv")
    else:
        rv = directional.get("downside_rv") or directional.get("combined_rv")

    if rv is None or rv <= 0:
        return "NEUTRAL", 0.0, 0.0

    return iv_rv_signal(iv, rv)


def iv_percentile_label(iv: float, rv: float) -> str:
    """Human-readable premium/discount label."""
    if rv <= 0:
        return "N/A"
    pct = (iv - rv) / rv * 100
    if pct > 0:
        return f"+{pct:.0f}% over RV"
    return f"{pct:.0f}% under RV"


def iv_rank(iv: float, prices: pd.Series) -> dict:
    """
    Approximate IV rank: where does current IV sit relative to the range of
    30-day realized vols over the available price history?

    IV Rank > 0.8  → historically expensive → lean sell vol / spreads
    IV Rank < 0.2  → historically cheap      → lean buy vol

    Returns:
        iv_rank        float 0–1 | None
        iv_rank_pct    float 0–100 | None
        iv_rank_label  str
    """
    log_returns = np.log(prices / prices.shift(1)).dropna()

    rolling_rv = []
    for i in range(30, len(log_returns) + 1):
        window = log_returns.iloc[i - 30: i]
        rolling_rv.append(float(window.std() * np.sqrt(252)))

    if len(rolling_rv) < 10:
        return {"iv_rank": None, "iv_rank_pct": None, "iv_rank_label": "N/A"}

    lo, hi = min(rolling_rv), max(rolling_rv)
    if hi <= lo:
        return {"iv_rank": 0.5, "iv_rank_pct": 50.0, "iv_rank_label": "N/A"}

    rank     = max(0.0, min(1.0, (iv - lo) / (hi - lo)))
    rank_pct = round(rank * 100, 1)

    if rank > 0.80:
        label = f"HIGH ({rank_pct:.0f}th pct) — vol rich"
    elif rank > 0.50:
        label = f"ABOVE AVG ({rank_pct:.0f}th pct)"
    elif rank > 0.20:
        label = f"BELOW AVG ({rank_pct:.0f}th pct)"
    else:
        label = f"LOW ({rank_pct:.0f}th pct) — vol cheap"

    return {
        "iv_rank":       round(rank, 3),
        "iv_rank_pct":   rank_pct,
        "iv_rank_label": label,
    }
