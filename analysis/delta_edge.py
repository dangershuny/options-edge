"""
Delta-aware edge helpers.

The scorer treats every "BUY VOL, score 75, 30 DTE" contract identically,
but a 0.10-delta OTM call and a 0.35-delta ATM call have wildly different
P&L distributions:

  |delta| < 0.15  → lottery ticket. High leverage but ~85% chance of
                    expiring worthless; break-even move is often larger
                    than typical realized vol.
  0.25–0.45       → sweet spot for directional trades: enough leverage to
                    matter, enough delta to participate in moderate moves,
                    enough premium that IV crush is a lesser share of P&L.
  |delta| > 0.60  → deep ITM. Acts more like stock; leverage is gone.

We also expose Δ on the output row so the recalibrator can regress outcomes
against delta buckets — eventually this penalty/bonus gets tuned
empirically, not by hand.

Note: Greeks computation needs (S, K, T, σ). `analysis/greeks.bs_greeks`
already handles the math. We just wrap it with edge-adjustment logic.
"""

from __future__ import annotations

from analysis.greeks import bs_greeks


def contract_delta(spot: float, strike: float, dte: int, iv: float,
                   opt_type: str) -> float | None:
    """Thin wrapper that returns delta or None on bad inputs."""
    if any(x is None for x in (spot, strike, dte, iv)):
        return None
    try:
        T = max(dte, 1) / 365.0
        g = bs_greeks(spot, strike, T, iv, opt_type)
        return float(g.get("delta", 0.0))
    except Exception:
        return None


def delta_score_delta(delta: float | None, vol_signal: str) -> float:
    """
    Penalize lottery-ticket strikes; reward sweet-spot strikes. Only for
    long-premium directional trades (BUY VOL / FLOW BUY).

       |Δ| < 0.10   → −8   (deep OTM lottery)
       |Δ| < 0.15   → −4
       0.25 ≤ |Δ| ≤ 0.45 → +4  (sweet spot)
       |Δ| > 0.65   → −2   (deep ITM, leverage is dead)
       otherwise     → 0
    """
    if delta is None or vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    from analysis.weights import w
    ad = abs(delta)
    if ad < 0.10:
        return w("delta.lottery_hard", -8.0)
    if ad < 0.15:
        return w("delta.lottery_soft", -4.0)
    if 0.25 <= ad <= 0.45:
        return w("delta.sweet_spot", 4.0)
    if ad > 0.65:
        return w("delta.deep_itm", -2.0)
    return 0.0
