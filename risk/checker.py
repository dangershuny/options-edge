"""
Pre-trade risk checker.

This is the gatekeeper between signal generation and order submission.
When the automated trading layer is wired to a brokerage, every trade
must pass check_trade() before an order is placed.

The checker is stateless on its own — it needs the current portfolio
state (open positions, daily P&L) injected by the trading engine.
That engine doesn't exist yet; this module is ready to plug into it.
"""

from __future__ import annotations
from datetime import date, datetime
from risk.config import RISK, RiskLimitExceeded


# ── Pre-catalyst refusal rule ─────────────────────────────────────────────────
#
# A major catalyst (earnings / FOMC / PDUFA / guidance / product launch)
# landing inside the expiry window is nearly always priced in by the options
# market. With <10 DTE there's insufficient time for a directional single-leg
# thesis to play out around the event — you're mostly buying the IV premium
# that gets crushed when the event resolves.
#
# Rule: refuse any long single-leg trade where a major catalyst occurs
# between now and expiry AND DTE < PRE_CATALYST_MIN_DTE.
#
# Legitimate overrides (operator must pass explicit override reason):
#   "volatility_play"  — long straddle/strangle; thesis IS the move size
#   "post_event"       — catalyst already passed; this is a drift/continuation entry
#   "iv_underpriced"   — IV rank <20 going into a typically >5% mover
#   "defined_risk"     — multi-leg debit spread with capped max loss
#   "strong_informed"  — blocks_signal STRONG + insider_signal STRONG aligned
#
# Without one of those, the rule blocks. User confirmed: "catalyst is always
# priced in that close to expiry — not always but a vast majority of the time."
# Default-deny is the correct posture.

PRE_CATALYST_MIN_DTE = 10
VALID_CATALYST_OVERRIDES = {
    "volatility_play", "post_event", "iv_underpriced",
    "defined_risk", "strong_informed",
}


def _parse_catalyst_date(catalyst_summary) -> date | None:
    """Extract a date from catalyst_summary, which can be a dict or string."""
    if not catalyst_summary:
        return None
    if isinstance(catalyst_summary, dict):
        raw = catalyst_summary.get("date") or catalyst_summary.get("next_date")
    elif isinstance(catalyst_summary, str):
        # try ISO-ish prefix
        raw = catalyst_summary[:10] if len(catalyst_summary) >= 10 else None
    else:
        return None
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def check_pre_catalyst(
    dte: int,
    catalyst_summary=None,
    catalyst_override: str | None = None,
    today: date | None = None,
) -> tuple[bool, str]:
    """
    Returns (allowed, reason).

    Blocks if a catalyst lands between today and expiry AND DTE is short,
    unless the operator passes a recognized override reason.
    """
    today = today or date.today()
    cat_date = _parse_catalyst_date(catalyst_summary)

    # No catalyst detected → rule doesn't apply
    if cat_date is None:
        return True, "no catalyst in window"

    # Catalyst is in the past → rule doesn't apply (post-event entries are fine)
    if cat_date < today:
        return True, f"catalyst {cat_date} is in the past"

    # Catalyst is AFTER expiry → rule doesn't apply
    expiry_date = today.fromordinal(today.toordinal() + dte)
    if cat_date > expiry_date:
        return True, f"catalyst {cat_date} is after expiry {expiry_date}"

    # Catalyst lands inside window AND DTE is long enough to absorb it
    if dte >= PRE_CATALYST_MIN_DTE:
        return True, f"catalyst {cat_date} in window but {dte} DTE absorbs it"

    # Here we are: short-DTE position into a pending catalyst.
    if catalyst_override in VALID_CATALYST_OVERRIDES:
        return True, (
            f"catalyst {cat_date} pending with {dte} DTE — "
            f"allowed via override='{catalyst_override}'"
        )

    return False, (
        f"catalyst {cat_date} pending with only {dte} DTE "
        f"(limit ≥{PRE_CATALYST_MIN_DTE}) — premium is priced in. "
        f"Override with one of: {sorted(VALID_CATALYST_OVERRIDES)}"
    )


def check_trade(
    symbol: str,
    vol_signal: str,
    score: float,
    flow_signal: str,
    dte: int,
    otm_pct: float,
    max_loss_per_contract: int,
    contracts: int,
    bid: float,
    ask: float,
    net_credit: float | None = None,
    spread_width: float | None = None,
    catalyst_summary=None,
    catalyst_override: str | None = None,
    # Portfolio state — injected by the trading engine
    open_positions: list[dict] | None = None,
    daily_pnl: float = 0.0,
) -> dict:
    """
    Validate a proposed trade against all risk limits.

    Returns:
        {"approved": True, "warnings": [...]}
        {"approved": False, "reason": str, "warnings": [...]}

    Raises:
        RiskLimitExceeded for hard violations (daily loss, drawdown).
        Soft violations return approved=False without raising.
    """
    warnings: list[str] = []
    open_positions = open_positions or []

    # ── Hard limits (raise = engine must halt) ─────────────────────────────────

    if daily_pnl <= -RISK["max_daily_loss"]:
        raise RiskLimitExceeded(
            f"Daily loss limit hit (${abs(daily_pnl):.0f} ≥ ${RISK['max_daily_loss']}). "
            "Trading halted for today."
        )

    # ── Score and signal quality ───────────────────────────────────────────────

    if score < RISK["min_score_to_trade"]:
        return _reject(
            f"Score {score} below minimum {RISK['min_score_to_trade']}",
            warnings,
        )

    flow_rank = {"NORMAL": 0, "ELEVATED": 1, "STRONG": 2}
    min_flow_rank = flow_rank.get(RISK["min_flow_signal"], 0)
    if flow_rank.get(flow_signal, 0) < min_flow_rank:
        return _reject(
            f"Flow signal '{flow_signal}' below minimum '{RISK['min_flow_signal']}'",
            warnings,
        )

    # ── DTE checks ─────────────────────────────────────────────────────────────

    if dte < RISK["min_dte"]:
        return _reject(f"DTE {dte} below minimum {RISK['min_dte']}", warnings)

    if dte > RISK["max_dte"]:
        return _reject(f"DTE {dte} above maximum {RISK['max_dte']}", warnings)

    # ── Pre-catalyst refusal (IV already priced in with too little time) ──────
    allowed, why = check_pre_catalyst(dte, catalyst_summary, catalyst_override)
    if not allowed:
        return _reject(why, warnings)
    if catalyst_override:
        warnings.append(f"catalyst override active: {catalyst_override} — {why}")

    # ── OTM check ─────────────────────────────────────────────────────────────

    if otm_pct > RISK["max_otm_pct"]:
        return _reject(
            f"OTM {otm_pct*100:.1f}% above limit {RISK['max_otm_pct']*100:.1f}%",
            warnings,
        )

    # ── Liquidity / bid-ask spread ────────────────────────────────────────────

    if ask > 0:
        ba_ratio = (ask - bid) / ask
        if ba_ratio > RISK["max_bid_ask_spread_ratio"]:
            return _reject(
                f"Bid-ask spread {ba_ratio*100:.1f}% too wide "
                f"(limit {RISK['max_bid_ask_spread_ratio']*100:.0f}%)",
                warnings,
            )

    # ── Credit spread quality ─────────────────────────────────────────────────

    if vol_signal == "SELL VOL" and net_credit is not None and spread_width:
        min_credit = spread_width * RISK["min_credit_to_width_ratio"]
        if net_credit < min_credit:
            return _reject(
                f"Net credit ${net_credit:.2f} below minimum "
                f"{RISK['min_credit_to_width_ratio']*100:.0f}% of "
                f"spread width ${spread_width:.2f} (need ${min_credit:.2f})",
                warnings,
            )

    # ── Per-trade cost / risk ─────────────────────────────────────────────────

    trade_risk = max_loss_per_contract * contracts
    if trade_risk > RISK["max_cost_per_trade"]:
        return _reject(
            f"Trade risk ${trade_risk:.0f} exceeds max ${RISK['max_cost_per_trade']}",
            warnings,
        )

    if contracts > RISK["max_contracts_per_trade"]:
        return _reject(
            f"Contracts {contracts} exceeds max {RISK['max_contracts_per_trade']}",
            warnings,
        )

    # ── Concentration: per-ticker ─────────────────────────────────────────────

    ticker_positions = [p for p in open_positions if p.get("symbol") == symbol]

    if len(ticker_positions) >= RISK["max_positions_per_ticker"]:
        return _reject(
            f"Already have {len(ticker_positions)} open position(s) in {symbol} "
            f"(limit {RISK['max_positions_per_ticker']})",
            warnings,
        )

    ticker_open_risk = sum(p.get("open_risk", 0) for p in ticker_positions)
    if ticker_open_risk + trade_risk > RISK["max_risk_per_ticker"]:
        return _reject(
            f"Adding ${trade_risk:.0f} would push {symbol} exposure to "
            f"${ticker_open_risk + trade_risk:.0f} "
            f"(limit ${RISK['max_risk_per_ticker']})",
            warnings,
        )

    # ── Total portfolio open risk ─────────────────────────────────────────────

    total_open_risk = sum(p.get("open_risk", 0) for p in open_positions)
    if total_open_risk + trade_risk > RISK["max_total_open_risk"]:
        return _reject(
            f"Total open risk ${total_open_risk + trade_risk:.0f} would exceed "
            f"limit ${RISK['max_total_open_risk']}",
            warnings,
        )

    # ── Soft warnings (don't block, just flag) ────────────────────────────────

    if dte < 14:
        warnings.append(f"DTE {dte} is low — theta decay accelerates below 14 days")

    if trade_risk > RISK["max_cost_per_trade"] * 0.75:
        warnings.append(f"Trade risk ${trade_risk:.0f} is close to per-trade limit")

    return {"approved": True, "warnings": warnings}


def _reject(reason: str, warnings: list[str]) -> dict:
    return {"approved": False, "reason": reason, "warnings": warnings}
