"""
Risk configuration — all hard limits for the automated trading system.

Every parameter here acts as a guardrail. The trading engine must check
these before submitting any order. Exceeding a limit = order rejected,
not reduced. Reducing is the engine's job; enforcing limits is this module.

Edit this file to adjust your risk tolerance. Never override at call-site.

USAGE (future automated layer):
    from risk.config import RISK

    if trade_cost > RISK["max_cost_per_trade"]:
        raise RiskLimitExceeded(f"Trade cost ${trade_cost} > limit ${RISK['max_cost_per_trade']}")
"""

from __future__ import annotations


# ── Account-size tiers ────────────────────────────────────────────────────────
#
# Dollar limits in RISK are auto-scaled to the current account equity.
# `auto_select_mode(portfolio_size)` returns the right tier; `apply_mode()`
# merges it into RISK before each scan. Tiers:
#
#   MICRO    — <$1,000.    Single concurrent position, $75/trade max,
#              min_score 75, max contract premium $0.75. Stock price
#              capped at $75 so OTM contracts stay affordable without
#              relaxing max_otm_pct (which would kill the edge).
#   STANDARD — $1,000-$5,000. Up to 4 concurrent, $150/trade, normal
#              scorer thresholds.
#   FULL     — >$5,000. Original $10K-calibrated limits, 8+ concurrent
#              positions, full premium range.
#
# Never hand-edit the tier dicts at call site — use apply_mode().

MICRO_MODE = {
    "portfolio_size":               500,
    "max_cost_per_trade":            75,
    "max_total_open_risk":           150,
    "max_risk_per_ticker":            75,
    "max_positions_per_ticker":        1,
    "max_concurrent_positions":        2,
    "min_score_to_trade":             75,
    "max_contract_premium":         0.75,   # scorer filters contracts > this
    "max_underlying_price":           75,   # scorer skips TSLA/NVDA-priced names
    "max_daily_loss":                 75,
}

STANDARD_MODE = {
    "portfolio_size":             2_500,
    "max_cost_per_trade":           150,
    "max_total_open_risk":           600,
    "max_risk_per_ticker":          200,
    "max_positions_per_ticker":        2,
    "max_concurrent_positions":        4,
    "min_score_to_trade":             70,
    "max_contract_premium":         2.50,
    "max_underlying_price":          250,
    "max_daily_loss":                200,
}

FULL_MODE = {
    "portfolio_size":           10_000,
    "max_cost_per_trade":           300,
    "max_total_open_risk":         2_500,
    "max_risk_per_ticker":          600,
    "max_positions_per_ticker":        2,
    "max_concurrent_positions":       10,
    "min_score_to_trade":             65,
    "max_contract_premium":        15.00,
    "max_underlying_price":        1_000,
    "max_daily_loss":                500,
}


def auto_select_mode(portfolio_size: float) -> dict:
    """Pick the mode tier for an account of this size."""
    if portfolio_size < 1_000:
        return MICRO_MODE
    if portfolio_size < 5_000:
        return STANDARD_MODE
    return FULL_MODE


def apply_mode(mode_dict: dict) -> None:
    """Merge a mode tier into the live RISK dict. Call before each scan."""
    global RISK
    RISK.update(mode_dict)


RISK: dict = {

    # ── Portfolio-level limits ────────────────────────────────────────────────

    # Total capital allocated to this strategy ($)
    "portfolio_size": 500,

    # Maximum total open risk across ALL positions at once.
    # For long options: sum of all max_loss_per_contract × contracts.
    # For credit spreads: sum of all max_loss × contracts.
    # Set to ~20-30% of portfolio_size.
    "max_total_open_risk": 2_500,

    # Maximum daily loss before the engine halts all new trades.
    # Resets at midnight. Operator must manually re-enable after review.
    "max_daily_loss": 500,

    # Maximum drawdown from peak portfolio value before full halt.
    # At this point no new trades until manual review and reset.
    "max_drawdown_pct": 0.15,   # 15%


    # ── Per-trade limits ──────────────────────────────────────────────────────

    # Maximum cost (debit paid) for a single long-option trade.
    # = entry_price × 100 × contracts. Prevents over-sizing a single bet.
    "max_cost_per_trade": 300,

    # Maximum number of contracts per single order.
    "max_contracts_per_trade": 5,

    # Minimum required score before a trade is eligible for execution.
    # Below this score the signal is too weak — scan only, don't trade.
    "min_score_to_trade": 65,

    # Minimum required flow signal: 'ELEVATED' or 'STRONG'
    # 'NORMAL' flow = no unusual activity = don't trade.
    "min_flow_signal": "ELEVATED",


    # ── Per-ticker limits ─────────────────────────────────────────────────────

    # Maximum open risk in a single ticker at once.
    # Prevents over-concentration in one name.
    "max_risk_per_ticker": 600,

    # Maximum number of open positions in the same ticker simultaneously.
    "max_positions_per_ticker": 2,


    # ── Options-specific guards ───────────────────────────────────────────────

    # Minimum days to expiry. Never buy options with less than this.
    "min_dte": 10,

    # Maximum days to expiry. Avoid very long-dated options (too much theta
    # drag if the thesis doesn't play out quickly).
    "max_dte": 60,

    # Maximum OTM percentage. Never buy options more than this % OTM.
    "max_otm_pct": 0.08,   # 8%

    # Minimum bid-ask spread quality filter.
    # If (ask - bid) / ask > this ratio, the option is too illiquid to trade.
    "max_bid_ask_spread_ratio": 0.25,   # 25% spread = too wide

    # For credit spreads: minimum net credit as % of spread width.
    # If credit < this ratio × width, the risk/reward is too poor.
    "min_credit_to_width_ratio": 0.20,  # collect at least 20% of spread width


    # ── Execution ─────────────────────────────────────────────────────────────

    # Order type for entries. 'limit' = safer, may not fill.
    # 'market' = always fills but can get bad prices on illiquid options.
    "entry_order_type": "limit",

    # Limit price = midpoint × this multiplier.
    # 1.0 = exact mid. 1.05 = 5% above mid to improve fill odds.
    "limit_price_midpoint_multiplier": 1.02,

    # Automatically place a stop-loss GTC order at this multiple of debit paid.
    # 0.5 = stop at 50% of premium paid (max loss per contract × 0.5).
    # Set to None to disable auto-stops (manage manually).
    "auto_stop_loss_pct": 0.50,

    # Automatically take profit at this multiple of debit paid.
    # 2.0 = exit when position doubles. None = no auto-TP.
    "auto_take_profit_multiplier": 2.0,


    # ── Cash-account / settlement guards ──────────────────────────────────────

    # When True, the trading engine must NOT close a position on the same
    # trading day it was opened. Every trade holds overnight minimum.
    #
    # Rationale: this starts as a small cash account. Cash accounts settle
    # options T+1; same-day exits produce unsettled proceeds. If those
    # proceeds are then used to fund another buy that is also sold before
    # settlement, it's a good-faith violation. Three GFVs in 12 months =
    # 90-day cash-only lockout.
    #
    # Setting this to True means the engine will:
    #   - Convert intraday SL hits into "exit at next-day open" orders
    #   - Skip the trailing stop fire until the next session
    #   - Force-close the theta guard only at EOD of day 2+
    #
    # Flip to False once the account has margin and PDT tracking is safe,
    # or once cash balance is large enough that unsettled-proceeds timing
    # never matters in practice.
    "cash_account_no_same_day_exit": True,

    # Minimum settled cash buffer (as fraction of portfolio) that must
    # remain after each new buy. Prevents accidentally spending unsettled
    # proceeds on the next entry.
    "min_settled_cash_fraction": 0.20,

    # ── Account-size filters (set by apply_mode) ──────────────────────────────
    # Defaults below match FULL_MODE so behavior is identical to pre-mode code
    # when apply_mode() is never called.
    "max_concurrent_positions":   10,
    "max_contract_premium":    15.00,
    "max_underlying_price":    1_000,
}


class RiskLimitExceeded(Exception):
    """Raised when a proposed trade violates any RISK limit."""
    pass
