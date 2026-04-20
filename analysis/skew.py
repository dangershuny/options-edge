"""
Volatility surface / skew analysis.

Signals extracted from the options chain that go beyond raw IV vs RV:
  - Put/Call volume ratio  (sentiment from flow)
  - Risk reversal          (OTM call IV - OTM put IV → directional skew)
  - Vol term structure     (near vs far IV → contango vs backwardation)

These are leading indicators because smart money shows up in the
skew and P/C ratio *before* it shows up in the stock price.
"""

import numpy as np
import pandas as pd


# --- thresholds ---------------------------------------------------------------

# Equities normally carry negative risk reversal (puts cost more than calls).
# We flag when it departs meaningfully from the baseline.
_RR_BULLISH_THRESHOLD  =  3.0   # calls > puts by 3 vol pts → unusual bullish positioning
_RR_BEARISH_THRESHOLD  = -12.0  # puts > calls by 12 vol pts → heavy fear / hedging demand

_PC_BULLISH = 0.60   # heavily call-dominated flow
_PC_BEARISH = 1.40   # heavily put-dominated flow


# --- helpers ------------------------------------------------------------------

def _otm_band(chain: pd.DataFrame, spot: float, opt_type: str,
              near_pct: float, far_pct: float) -> pd.DataFrame:
    c = chain[chain["type"] == opt_type]
    if opt_type == "call":
        return c[(c["strike"] > spot * (1 + near_pct)) &
                 (c["strike"] < spot * (1 + far_pct))]
    else:
        return c[(c["strike"] < spot * (1 - near_pct)) &
                 (c["strike"] > spot * (1 - far_pct))]


# --- public API ---------------------------------------------------------------

def calculate_skew(chain: pd.DataFrame, spot: float) -> dict:
    """
    Compute volatility surface metrics from a full (unfiltered) options chain.

    Args:
        chain:  DataFrame from get_options_chain — must contain columns
                type, strike, impliedVolatility, volume, dte
        spot:   current stock price

    Returns:
        pc_ratio        float | None   — put vol / call vol
        risk_reversal   float | None   — OTM call IV% − OTM put IV%
        skew_signal     str            — 'BULLISH' | 'BEARISH' | 'NEUTRAL'
        term_slope      float | None   — far IV% − near IV% (positive = contango)
        skew_summary    str            — human-readable one-liner
    """
    calls = chain[chain["type"] == "call"]
    puts  = chain[chain["type"] == "put"]

    # ── Put/Call volume ratio ──────────────────────────────────────────────────
    call_vol = float(calls["volume"].sum())
    put_vol  = float(puts["volume"].sum())
    pc_ratio = round(put_vol / call_vol, 3) if call_vol > 0 else None

    # ── Risk reversal (25-delta proxy: 3-10% OTM) ─────────────────────────────
    otm_calls = _otm_band(chain, spot, "call", 0.03, 0.10)
    otm_puts  = _otm_band(chain, spot, "put",  0.03, 0.10)

    if not otm_calls.empty and not otm_puts.empty:
        avg_call_iv = float(otm_calls["impliedVolatility"].mean())
        avg_put_iv  = float(otm_puts["impliedVolatility"].mean())
        risk_reversal = round((avg_call_iv - avg_put_iv) * 100, 2)
    else:
        risk_reversal = None

    # ── Vol term structure ─────────────────────────────────────────────────────
    if "dte" in chain.columns:
        near_iv = chain[chain["dte"] <= 30]["impliedVolatility"].mean()
        far_iv  = chain[chain["dte"] >  45]["impliedVolatility"].mean()
        if not (np.isnan(near_iv) or np.isnan(far_iv)):
            term_slope = round((float(far_iv) - float(near_iv)) * 100, 2)
        else:
            term_slope = None
    else:
        term_slope = None

    # ── Composite signal ───────────────────────────────────────────────────────
    bullish_votes = 0
    bearish_votes = 0

    if risk_reversal is not None:
        if risk_reversal > _RR_BULLISH_THRESHOLD:
            bullish_votes += 2   # strong signal — unusual for equities
        elif risk_reversal < _RR_BEARISH_THRESHOLD:
            bearish_votes += 1   # heavy put hedging

    if pc_ratio is not None:
        if pc_ratio < _PC_BULLISH:
            bullish_votes += 1
        elif pc_ratio > _PC_BEARISH:
            bearish_votes += 1

    if term_slope is not None and term_slope < -3:
        bearish_votes += 1  # near-term vol elevated = near-term fear

    if bullish_votes >= 2:
        skew_signal = "BULLISH"
    elif bearish_votes >= 2:
        skew_signal = "BEARISH"
    else:
        skew_signal = "NEUTRAL"

    # ── Human-readable summary ─────────────────────────────────────────────────
    parts = []
    if pc_ratio is not None:
        parts.append(f"P/C {pc_ratio:.2f}")
    if risk_reversal is not None:
        parts.append(f"RR {risk_reversal:+.1f}vp")
    if term_slope is not None:
        label = "contango" if term_slope >= 0 else "backwardation"
        parts.append(f"term {term_slope:+.1f}vp ({label})")
    skew_summary = "  |  ".join(parts) if parts else "—"

    return {
        "pc_ratio":      pc_ratio,
        "risk_reversal": risk_reversal,
        "skew_signal":   skew_signal,
        "term_slope":    term_slope,
        "skew_summary":  skew_summary,
    }
