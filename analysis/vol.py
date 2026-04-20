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


def iv_rank(iv: float, prices: pd.Series) -> dict:
    """
    Approximate IV rank: where does current IV sit relative to the range of
    30-day realized vols over the available price history?

    IV Rank > 0.8  → historically expensive  → lean sell vol / spreads
    IV Rank < 0.2  → historically cheap       → lean buy vol

    This is a proxy (uses RV history, not IV history) but directionally
    correct: when RV has been low, options that price in that RV are cheap;
    when RV has spiked, options are expensive.

    Returns:
        iv_rank        float 0–1 | None
        iv_rank_pct    float 0–100 | None
        iv_rank_label  str
    """
    log_returns = np.log(prices / prices.shift(1)).dropna()

    # Compute 30-day rolling RV at each point
    rolling_rv = []
    for i in range(30, len(log_returns) + 1):
        window = log_returns.iloc[i - 30: i]
        rolling_rv.append(float(window.std() * np.sqrt(252)))

    if len(rolling_rv) < 10:
        return {"iv_rank": None, "iv_rank_pct": None, "iv_rank_label": "N/A"}

    lo, hi = min(rolling_rv), max(rolling_rv)
    if hi <= lo:
        return {"iv_rank": 0.5, "iv_rank_pct": 50.0, "iv_rank_label": "N/A"}

    rank = max(0.0, min(1.0, (iv - lo) / (hi - lo)))
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
