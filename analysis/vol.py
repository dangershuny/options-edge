import numpy as np
import pandas as pd


def calculate_rv(prices: pd.Series, window: int = 30) -> float | None:
    """Annualized realized volatility from the last `window` trading days of close prices."""
    log_returns = np.log(prices / prices.shift(1)).dropna()
    if len(log_returns) < window:
        return None
    rv = float(log_returns.tail(window).std() * np.sqrt(252))
    return rv


def iv_rv_signal(iv: float, rv: float) -> tuple[str, float, float]:
    """
    Compare implied vol to realized vol.

    Returns:
        signal:   'BUY VOL' | 'SELL VOL' | 'NEUTRAL'
        spread:   IV - RV (annualized, as a decimal)
        strength: 0.0–1.0
    """
    spread = iv - rv
    pct_diff = spread / rv if rv > 0 else 0.0

    if pct_diff > 0.25:
        return "SELL VOL", spread, min(abs(pct_diff), 1.0)
    elif pct_diff < -0.20:
        return "BUY VOL", spread, min(abs(pct_diff), 1.0)
    else:
        return "NEUTRAL", spread, 0.0


def iv_percentile_label(iv: float, rv: float) -> str:
    """Human-readable premium/discount label."""
    if rv <= 0:
        return "N/A"
    pct = (iv - rv) / rv * 100
    if pct > 0:
        return f"+{pct:.0f}% over RV"
    return f"{pct:.0f}% under RV"
