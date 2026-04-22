"""
Position sizing calculator.

Uses a fixed-fractional model (simplest robust approach for options):
  risk_per_trade = portfolio_size × risk_fraction
  contracts = floor(risk_per_trade / max_loss_per_contract)

A Kelly-criterion multiplier is computed for reference but NOT used
for live sizing — Kelly is aggressive and leads to ruin if edge
estimates are even slightly wrong. Use Kelly × 0.25 at most.

All sizing is subject to hard caps from risk/config.py.
"""

from __future__ import annotations
import math
from risk.config import RISK

try:
    from data.macro import macro_size_multiplier, get_vix_context
    _MACRO_AVAILABLE = True
except Exception:
    _MACRO_AVAILABLE = False


def size_trade(
    max_loss_per_contract: float,
    score: float,
    win_rate_estimate: float = 0.55,
    avg_win_multiplier: float = 1.8,
    portfolio_override: float | None = None,
    macro: dict | None = None,
) -> dict:
    """
    Calculate the number of contracts to trade.

    Args:
        max_loss_per_contract: Maximum dollar loss if option expires worthless.
                               For long options: entry_price × 100.
                               For spreads: (spread_width - credit) × 100.
        score:                 Signal score 0–100 from scorer.py.
        win_rate_estimate:     Historical win rate assumption (default 55%).
                               You'll refine this as you build a track record.
        avg_win_multiplier:    How many × the risk do you win on average.
                               e.g. 1.8 = win $1.80 per $1.00 risked.
        portfolio_override:    Override portfolio_size from config (for testing).

    Returns dict:
        contracts       int    — recommended number of contracts
        risk_dollar     float  — total dollars at risk for this trade
        kelly_fraction  float  — Kelly fraction (for reference only)
        rationale       str    — human-readable sizing explanation
    """
    portfolio = portfolio_override or RISK["portfolio_size"]
    max_daily = RISK["max_daily_loss"]
    max_total = RISK["max_total_open_risk"]
    max_cost  = RISK["max_cost_per_trade"]
    max_cts   = RISK["max_contracts_per_trade"]

    if max_loss_per_contract <= 0:
        return _zero("max_loss_per_contract must be > 0")

    # ── Score-based risk fraction ──────────────────────────────────────────────
    # High-conviction trades (score ≥ 80) get up to 3% of portfolio.
    # Low-conviction (score 60–70) get 1%.
    if score >= 80:
        risk_fraction = 0.03
    elif score >= 70:
        risk_fraction = 0.02
    else:
        risk_fraction = 0.01

    target_risk = portfolio * risk_fraction

    # ── Macro regime scaling ───────────────────────────────────────────
    # Shrink size in hostile regimes (FEAR → 0.50×, ELEVATED → 0.75×).
    # Upsize slightly in LOW VIX where options are cheap (1.10×).
    macro_mult = 1.0
    if _MACRO_AVAILABLE:
        try:
            m = macro if macro is not None else get_vix_context()
            macro_mult = macro_size_multiplier(m)
        except Exception:
            macro_mult = 1.0
    target_risk *= macro_mult

    # Cap by per-trade and daily limits
    target_risk = min(target_risk, max_cost, max_daily * 0.4)

    # ── Contract count ────────────────────────────────────────────────────────
    contracts = max(1, math.floor(target_risk / max_loss_per_contract))
    contracts = min(contracts, max_cts)

    # Recalculate actual risk after integer flooring
    actual_risk = contracts * max_loss_per_contract

    # Hard check: never exceed max_total_open_risk in one trade
    if actual_risk > max_total:
        contracts = max(1, math.floor(max_total / max_loss_per_contract))
        actual_risk = contracts * max_loss_per_contract

    # ── Kelly criterion (informational) ───────────────────────────────────────
    # Full Kelly = p - (1-p)/b  where p=win_rate, b=win/loss ratio
    b = avg_win_multiplier
    p = win_rate_estimate
    kelly = p - (1 - p) / b if b > 0 else 0
    kelly = max(0.0, round(kelly, 3))

    rationale = (
        f"Score {score:.0f} → {risk_fraction*100:.0f}% risk fraction  |  "
        f"Macro ×{macro_mult:.2f}  |  "
        f"Target risk ${target_risk:.0f}  |  "
        f"Max loss/contract ${max_loss_per_contract:.0f}  |  "
        f"Sized: {contracts} contract(s)  |  "
        f"Total risk: ${actual_risk:.0f}  |  "
        f"Kelly (ref only): {kelly:.1%}"
    )

    return {
        "contracts":      contracts,
        "risk_dollar":    round(actual_risk, 2),
        "kelly_fraction": kelly,
        "rationale":      rationale,
    }


def _zero(reason: str) -> dict:
    return {
        "contracts":      0,
        "risk_dollar":    0.0,
        "kelly_fraction": 0.0,
        "rationale":      reason,
    }
