"""
Macro volatility context.

Fetches VIX (30-day implied vol for S&P 500) and VIX9D (9-day) to
determine the current vol regime and term structure shape.

Why this matters:
  - In LOW VIX regimes options are cheap → lean toward buying vol
  - In FEAR regimes options are expensive → lean toward selling / spreads
  - Backwardation (VIX9D > VIX) = near-term fear elevated, often reversal signal
  - Contango = normal / calm market

All calls are non-blocking: returns safe defaults if data unavailable.
"""

import yfinance as yf

_cache: dict | None = None


def get_vix_context(force_refresh: bool = False) -> dict:
    """
    Return VIX-based macro context. Cached for the session (one call per run).

    Returns:
        vix         float | None
        vix9d       float | None
        regime      str   — 'LOW' | 'NORMAL' | 'ELEVATED' | 'FEAR'
        term_slope  float | None  — VIX9D - VIX (negative = backwardation = near fear)
        lean        str   — 'BUY VOL' | 'SELL VOL' | 'NEUTRAL'
        summary     str   — human-readable one-liner
    """
    global _cache
    if _cache is not None and not force_refresh:
        return _cache

    vix_level  = _fetch_close("^VIX")
    vix9d_level = _fetch_close("^VIX9D")

    if vix_level is None:
        result = _unknown()
        _cache = result
        return result

    # ── Regime ────────────────────────────────────────────────────────────────
    if vix_level < 15:
        regime = "LOW"
        lean   = "SELL VOL"   # options historically cheap but this can persist
    elif vix_level < 20:
        regime = "NORMAL"
        lean   = "NEUTRAL"
    elif vix_level < 30:
        regime = "ELEVATED"
        lean   = "BUY VOL"    # fear elevated; IV often overpriced short-term but
    else:                       # mean-reverting medium-term
        regime = "FEAR"
        lean   = "SELL VOL"   # extreme fear → IV crush likely after event

    # ── Term structure ────────────────────────────────────────────────────────
    term_slope = None
    structure_label = ""
    if vix9d_level is not None:
        term_slope = round(vix9d_level - vix_level, 2)
        if term_slope < -2:
            structure_label = " · term backwardation (near-term fear)"
        elif term_slope > 2:
            structure_label = " · term contango (calm near-term)"

    summary = f"VIX {vix_level:.1f} ({regime}){structure_label}"

    result = {
        "vix":         round(vix_level, 2),
        "vix9d":       round(vix9d_level, 2) if vix9d_level else None,
        "regime":      regime,
        "term_slope":  term_slope,
        "lean":        lean,
        "summary":     summary,
    }
    _cache = result
    return result


def reset_cache() -> None:
    global _cache
    _cache = None


def _fetch_close(symbol: str) -> float | None:
    try:
        hist = yf.Ticker(symbol).history(period="2d")
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception:
        return None


def macro_score_delta(macro: dict | None, vol_signal: str) -> float:
    """
    Macro regime adjustment for a proposed long-premium trade.

    LOW VIX  + BUY VOL  → +4  (buying cheap options; favourable)
    NORMAL                → 0
    ELEVATED + BUY VOL  → −3  (paying elevated premium; unfavourable for long)
    FEAR     + BUY VOL  → −8  (very expensive premium + IV-crush risk)
    Backwardation term-slope overlay:
      slope < −3 (deep backwardation)    → additional −3 on long vol
    """
    if not macro or vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    regime = macro.get("regime")
    delta = 0.0
    if regime == "LOW":
        delta = +4.0
    elif regime == "NORMAL":
        delta = 0.0
    elif regime == "ELEVATED":
        delta = -3.0
    elif regime == "FEAR":
        delta = -8.0
    slope = macro.get("term_slope")
    if isinstance(slope, (int, float)) and slope is not None and slope < -3:
        delta -= 3.0
    return round(delta, 2)


def macro_size_multiplier(macro: dict | None) -> float:
    """
    Sizing multiplier that shrinks position size in hostile regimes. Applied
    inside sizer.size_trade() so every trade scales with regime quality.

      LOW      → 1.10  (tiny upsize; cheap premium)
      NORMAL   → 1.00
      ELEVATED → 0.75
      FEAR     → 0.50  (half size when VIX > 30)
      UNKNOWN  → 0.90
    """
    if not macro:
        return 0.90
    regime = macro.get("regime", "UNKNOWN")
    return {
        "LOW": 1.10, "NORMAL": 1.00, "ELEVATED": 0.75,
        "FEAR": 0.50, "UNKNOWN": 0.90,
    }.get(regime, 1.0)


def _unknown() -> dict:
    return {
        "vix":        None,
        "vix9d":      None,
        "regime":     "UNKNOWN",
        "term_slope": None,
        "lean":       "NEUTRAL",
        "summary":    "VIX data unavailable",
    }
