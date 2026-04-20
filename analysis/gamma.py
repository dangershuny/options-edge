"""
Dealer Gamma Exposure (GEX) analysis.

GEX measures the aggregate gamma position that market-makers hold.
When dealers are net-long gamma (positive GEX), they hedge by buying
dips and selling rallies → vol suppression, range-bound tape.
When GEX turns negative, dealers hedge PRO-cyclically → moves amplify.

Key outputs:
  gamma_wall  — strike with the largest +GEX; acts as price magnet / resistance
  gamma_flip  — strike where cumulative GEX crosses zero; below = explosive regime
  gex_signal  — 'PINNED' | 'SUPPORTIVE' | 'EXPLOSIVE'

Convention used here (standard for equity index GEX):
  Dealers assumed short what retail/institutions bought.
  Call GEX = +OI × gamma × spot (dealers short calls → long-delta → buy on up, sell on down)
  Put GEX  = -OI × gamma × spot (dealers short puts → short-delta → sell on down)
  Net GEX per strike = call_gex - put_gex

Positive total GEX = dealers long gamma = stabilising.
Negative total GEX = dealers short gamma = destabilising.
"""

import numpy as np
import pandas as pd

from analysis.greeks import bs_greeks, RISK_FREE_RATE

# Price must be within this % of gamma wall to be "pinned"
_PIN_THRESHOLD = 0.015   # 1.5%


def calculate_gex(chain: pd.DataFrame, spot: float) -> dict:
    """
    Compute Dealer Gamma Exposure from a full options chain.

    Args:
        chain:  DataFrame with columns impliedVolatility, strike, dte,
                openInterest, type  (unfiltered — use the raw chain)
        spot:   current stock price

    Returns:
        gex_by_strike   DataFrame (strike, call_gex, put_gex, net_gex)
        gamma_wall      float | None  — highest +GEX strike (nearest to spot if tie)
        gamma_flip      float | None  — lowest strike where cum GEX < 0
        total_gex       float         — sum of all net GEX
        gex_signal      str           — 'PINNED' | 'SUPPORTIVE' | 'EXPLOSIVE'
        gex_summary     str           — human-readable one-liner
    """
    rows = []

    for _, row in chain.iterrows():
        K     = float(row["strike"])
        sigma = float(row.get("impliedVolatility") or 0)
        dte   = int(row.get("dte") or 0)
        oi    = float(row.get("openInterest") or 0)
        opt   = str(row.get("type", "call")).lower()

        if sigma <= 0 or oi <= 0 or dte <= 0:
            continue

        T = max(dte / 365.0, 1 / 365.0)
        g = bs_greeks(spot, K, T, sigma, opt, RISK_FREE_RATE)["gamma"]

        # GEX in notional dollars per 1-point move in spot
        # = gamma × OI × 100 shares/contract × spot
        gex = g * oi * 100 * spot

        if opt == "call":
            rows.append({"strike": K, "call_gex": gex,  "put_gex": 0.0})
        else:
            rows.append({"strike": K, "call_gex": 0.0,  "put_gex": gex})

    if not rows:
        return _empty_result()

    df = (
        pd.DataFrame(rows)
        .groupby("strike", as_index=False)
        .sum()
        .sort_values("strike")
        .reset_index(drop=True)
    )
    df["net_gex"] = df["call_gex"] - df["put_gex"]

    # ── Gamma wall ────────────────────────────────────────────────────────────
    # Largest positive GEX near spot (within 15%) acts as price gravity
    near = df[abs(df["strike"] - spot) / spot <= 0.15]
    if not near.empty:
        wall_row    = near.loc[near["net_gex"].idxmax()]
        gamma_wall  = float(wall_row["strike"])
    else:
        gamma_wall = None

    # ── Gamma flip ────────────────────────────────────────────────────────────
    # Cumulative GEX going from high strikes down to low: first point < 0
    df_sorted     = df.sort_values("strike", ascending=False).copy()
    df_sorted["cum_gex"] = df_sorted["net_gex"].cumsum()
    flip_candidates = df_sorted[df_sorted["cum_gex"] < 0]
    gamma_flip = float(flip_candidates.iloc[-1]["strike"]) if not flip_candidates.empty else None

    total_gex = float(df["net_gex"].sum())

    # ── Signal ────────────────────────────────────────────────────────────────
    if gamma_wall is not None and abs(spot - gamma_wall) / spot < _PIN_THRESHOLD:
        gex_signal = "PINNED"
        gex_summary = f"Price pinned near ${gamma_wall:.0f} gamma wall — expect tight range"
    elif total_gex < 0 or (gamma_flip is not None and spot <= gamma_flip):
        gex_signal = "EXPLOSIVE"
        flip_str = f" (flip ${gamma_flip:.0f})" if gamma_flip else ""
        gex_summary = f"Negative GEX regime{flip_str} — dealer hedging amplifies moves"
    else:
        gex_signal = "SUPPORTIVE"
        wall_str = f", wall ${gamma_wall:.0f}" if gamma_wall else ""
        gex_summary = f"Positive GEX{wall_str} — dealers absorb volatility"

    return {
        "gex_by_strike": df,
        "gamma_wall":    gamma_wall,
        "gamma_flip":    gamma_flip,
        "total_gex":     round(total_gex, 0),
        "gex_signal":    gex_signal,
        "gex_summary":   gex_summary,
    }


def _empty_result() -> dict:
    return {
        "gex_by_strike": pd.DataFrame(),
        "gamma_wall":    None,
        "gamma_flip":    None,
        "total_gex":     0.0,
        "gex_signal":    "NEUTRAL",
        "gex_summary":   "Insufficient data for GEX",
    }
