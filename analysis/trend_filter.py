"""
Simple SMA-based trend regime.

We already have a TREND EXHAUSTION penalty (stock up 8% in 10d = stop
chasing calls). The counterpart that was missing: TREND CONFIRMATION — is
the stock in a healthy directional regime at all?

Regime definition:
  UPTREND     : SMA20 > SMA50 AND price > SMA20         — call-friendly
  DOWNTREND   : SMA20 < SMA50 AND price < SMA20         — put-friendly
  CHOPPY      : SMA20 vs SMA50 disagree with price       — neither
  INSUFFICIENT: too little history                       — neutral

The delta rewards BUY CALL in UPTREND and BUY PUT in DOWNTREND at +4,
penalises counter-trend at -3. It stays inside the scorer's ±15
volume-family cap so the combined technical overlay can't overwhelm the
IV vs RV edge.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class TrendRegime:
    sma20: float
    sma50: float
    last_price: float
    regime: str                # UPTREND | DOWNTREND | CHOPPY | INSUFFICIENT
    slope_20d: float           # %-change in SMA20 over the last 10 trading days


def classify_trend(prices: pd.Series) -> TrendRegime | None:
    """`prices` is a pandas Series of daily closes, chronological."""
    if prices is None or len(prices) < 55:
        if prices is None or prices.empty:
            return None
        last = float(prices.iloc[-1])
        return TrendRegime(
            sma20=last, sma50=last, last_price=last,
            regime="INSUFFICIENT", slope_20d=0.0,
        )

    p = prices.astype(float)
    sma20 = p.rolling(20).mean()
    sma50 = p.rolling(50).mean()
    last = float(p.iloc[-1])
    s20 = float(sma20.iloc[-1])
    s50 = float(sma50.iloc[-1])

    # Slope of SMA20 over 10 bars — detects a fading trend even when
    # SMA20 is still above SMA50.
    try:
        s20_prev = float(sma20.iloc[-11])
        slope = (s20 - s20_prev) / s20_prev if s20_prev > 0 else 0.0
    except Exception:
        slope = 0.0

    if s20 > s50 and last > s20:
        regime = "UPTREND"
    elif s20 < s50 and last < s20:
        regime = "DOWNTREND"
    else:
        regime = "CHOPPY"

    return TrendRegime(
        sma20=round(s20, 3), sma50=round(s50, 3),
        last_price=round(last, 3),
        regime=regime, slope_20d=round(slope, 4),
    )


def trend_score_delta(tr: TrendRegime | None, opt_type: str,
                      vol_signal: str, vix_regime: str | None = None) -> float:
    """Aligned: +4. Counter-trend: -3. Chop / insufficient: 0."""
    if tr is None or tr.regime in ("INSUFFICIENT", "CHOPPY"):
        return 0.0
    if vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    is_call = opt_type.lower().startswith("c")
    aligned = (is_call and tr.regime == "UPTREND") or (
        not is_call and tr.regime == "DOWNTREND"
    )
    from analysis.weights import w_regime
    if aligned:
        # Fading trend (slope turning over): discount the bonus.
        if is_call and tr.slope_20d < 0:
            return w_regime("trend.fading", vix_regime, 2.0)
        if not is_call and tr.slope_20d > 0:
            return w_regime("trend.fading", vix_regime, 2.0)
        return w_regime("trend.aligned", vix_regime, 4.0)
    return w_regime("trend.counter", vix_regime, -3.0)
