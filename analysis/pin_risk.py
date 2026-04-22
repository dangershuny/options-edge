"""
Pin-risk / assignment-risk analyzer.

Near expiry, two effects dominate:
  1. Pinning: dealer gamma hedging pins price to a nearby high-OI strike.
     Options decay hard when the underlying sits on the strike.
  2. Assignment risk: short ITM options (not our focus here — we're BUY-only)
     can be assigned.

For BUY-side traders (us), pin risk is what matters: buying a 3-DTE OTM call
with a big gamma wall a dollar away can result in zero intrinsic at expiry
even if your thesis plays out slowly.

This module flags high-risk contracts and returns a safe default otherwise.
"""

from __future__ import annotations

import pandas as pd

# Windows where pin risk is material
HIGH_PIN_DTE    = 5       # inside 5d, dealer gamma dominates
CRITICAL_DTE    = 2       # inside 2d, theta is brutal

# Distance to wall that's considered "at risk"
PIN_DIST_PCT    = 0.015   # 1.5% of spot


def assess_pin_risk(
    chain: pd.DataFrame | None,
    spot: float,
    strike: float,
    dte: int,
    gex_result: dict | None = None,
) -> dict:
    """
    Assess pin/assignment risk for a specific contract.

    Args:
        chain:      full options chain DataFrame (for wall computation fallback)
        spot:       current underlying price
        strike:     option strike
        dte:        days to expiry
        gex_result: output of analysis.gamma.calculate_gex (preferred source
                    of gamma_wall); falls back to chain-derived wall.

    Returns:
        {
          'pin_risk': 'HIGH' | 'MODERATE' | 'LOW' | 'NONE',
          'days_to_expiry': int,
          'gamma_wall':     float | None,
          'distance_to_wall_pct': float | None,
          'summary':        str,
        }
    """
    try:
        if dte is None or dte > HIGH_PIN_DTE:
            return _none(dte)

        wall = None
        if gex_result and isinstance(gex_result, dict):
            wall = gex_result.get("gamma_wall")

        if wall is None and chain is not None and not chain.empty:
            try:
                # Fallback: highest-OI strike near the money
                near_money = chain[
                    (chain["strike"] >= spot * 0.90) &
                    (chain["strike"] <= spot * 1.10)
                ].copy()
                if not near_money.empty:
                    agg = near_money.groupby("strike")["openInterest"].sum()
                    if not agg.empty:
                        wall = float(agg.idxmax())
            except Exception:
                wall = None

        if wall is None or spot <= 0:
            return {
                "pin_risk":              "LOW",
                "days_to_expiry":        int(dte),
                "gamma_wall":            None,
                "distance_to_wall_pct":  None,
                "summary":               f"{dte}d to expiry, no wall data",
            }

        # Is the wall between current price and the strike?
        # If the strike is OTM and the wall sits between spot and strike,
        # the option often fails to cross through.
        dist_to_wall_pct = abs(spot - wall) / spot

        level = "LOW"
        if dte <= CRITICAL_DTE and dist_to_wall_pct <= PIN_DIST_PCT:
            level = "HIGH"
        elif dte <= HIGH_PIN_DTE and dist_to_wall_pct <= PIN_DIST_PCT:
            level = "MODERATE"

        # Extra upgrade: wall sits between spot and strike (blocking path)
        if wall != strike and (
            (strike > spot and spot < wall < strike) or
            (strike < spot and strike < wall < spot)
        ):
            level = "HIGH" if dte <= HIGH_PIN_DTE else level

        summary = (f"{dte}d to expiry, wall ${wall:.2f} "
                   f"({dist_to_wall_pct*100:.1f}% from spot) — {level}")

        return {
            "pin_risk":              level,
            "days_to_expiry":        int(dte),
            "gamma_wall":            round(wall, 2),
            "distance_to_wall_pct":  round(dist_to_wall_pct * 100, 2),
            "summary":               summary,
        }
    except Exception as e:
        return {
            "pin_risk":              "LOW",
            "days_to_expiry":        int(dte) if dte is not None else -1,
            "gamma_wall":            None,
            "distance_to_wall_pct":  None,
            "summary":               f"pin-risk check failed ({e})",
        }


def pin_risk_score_delta(pin: dict | None) -> float:
    """
    Score penalty from pin risk. Only penalizes — never rewards.
    HIGH      → -12
    MODERATE  → -5
    LOW/NONE  → 0
    """
    if not pin or not isinstance(pin, dict):
        return 0.0
    level = pin.get("pin_risk")
    if level == "HIGH":
        return -12.0
    if level == "MODERATE":
        return -5.0
    return 0.0


def _none(dte: int | None) -> dict:
    return {
        "pin_risk":              "NONE",
        "days_to_expiry":        int(dte) if dte is not None else -1,
        "gamma_wall":            None,
        "distance_to_wall_pct":  None,
        "summary":               f"{dte}d to expiry — outside pin-risk window",
    }
