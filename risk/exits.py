"""
Exit and entry-timing rules — empirically derived, rolling-calibrated.

# IMPORTANT: these values are a starting calibration, NOT frozen.
The defaults in this file reflect a grid-search on only 2 days of snapshot
data (04-20 + 04-21, n=77 priced contracts for exits; n=14 for entry
timing). As more days accumulate, re-run:

    python tools/recalibrate_exits.py

That tool replays every snapshot in `snapshots/`, re-runs the SL/TP grid
and the intraday-timing table, and writes the resulting tiers to
`risk/exits_calibration.json`. If that file exists, the live values are
loaded from it at import time — so the rules evolve as the dataset grows
without code edits. Each recalibration logs a diff vs the previous
calibration; if a tier flips direction or magnitude materially (e.g.
score 60-79 SL drifts from -20% to -10%), that's noted in the log and
surfaced at the next scan so the operator can review before the engine
adopts it.

Two sets of rules:

  1. ENTRY TIMING — when in the trading day to place the order.
     Current: buy at the open or not at all. Hit rate decays
     monotonically from 09:30 (64%) → 10:00 (50%) → 14:00 (31%) on the
     14-contract slate measured. Waiting until 10:00 cost −47% of
     open-entry P&L. See `tools/timing_analysis.py`.

  2. STOP-LOSS / TAKE-PROFIT — per-score-bucket exits.
     Current: grid search across SL ∈ {None, -50%..-10%} ×
     TP ∈ {None, +20%..+200%} over 77 contracts found:
       • Best overall:  SL -10%, no TP → +7.93% ROI vs +4.71% baseline
       • Score 80-100:  no exits at all → +26.15% ROI
         (tight TPs clip winners that run 50-80% intraday)
       • Score 60-79:   SL -20%, no TP
       • Score 0-59:    SL -10%, no TP

     These will be re-derived as the dataset grows. n=77 is thin;
     expect the top-tier "no exits" rule to get a hard stop once the
     sample includes a day with a sharp midday reversal in a high-score
     name.

LAYERED EXIT MODEL:
    The grid-search optimum is the FLOOR of what's acceptable, not the
    ceiling. Layers, tightest wins:

      1. Tiered SL/TP from grid search (exit_rules_for_score)
      2. HARD_STOP_FLOOR (-40%) always — gap-risk cap
      3. OVERNIGHT_STOP_FLOOR (-25%) if position held past today's close
      4. CATALYST_STOP_FLOOR (-15%) if earnings/FOMC/PDUFA within 3 days
      5. Short-DTE clamp (-15%) if ≤5 DTE
      6. Trailing stop armed at +25% (15% off peak) — replaces fixed TP
      7. Theta-decay guard: force-close at EOD if -15%+ with ≤10 DTE

    The grid said "no stop for 80+ score" was optimal INTRADAY. Layers
    2-5 exist because overnight gaps, earnings prints, and theta bleed
    are risks the optimizer couldn't see.

Usage (future automated engine):

    from risk.exits import entry_allowed_now, apply_safety_floors

    if not entry_allowed_now()[0]:
        skip_trade("outside entry window")

    sl_pct, tp_pct = apply_safety_floors(
        score=row["score"], dte=row["dte"],
        has_catalyst_in_window=row["catalyst_summary"] is not None,
        held_overnight=True,  # most trades will hold overnight
    )
    place_stop_loss(entry_price * (1 + sl_pct))
    if tp_pct is not None:
        place_take_profit(entry_price * (1 + tp_pct))
    # else: monitor with trailing_stop_state() on each tick
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

# ── Calibration file (auto-refreshed by tools/recalibrate_exits.py) ──────────
# If present, its values override the defaults below. Regenerate via:
#   python tools/recalibrate_exits.py
CALIBRATION_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "exits_calibration.json",
)

_CALIBRATION: dict | None = None
try:
    if os.path.exists(CALIBRATION_PATH):
        with open(CALIBRATION_PATH, "r", encoding="utf-8") as _f:
            _CALIBRATION = json.load(_f)
except Exception:
    _CALIBRATION = None


def calibration_info() -> dict:
    """Return metadata about the live calibration (for logging / UI)."""
    if not _CALIBRATION:
        return {
            "source": "defaults (n=77, 2026-04-20..21)",
            "n_contracts": 77,
            "last_updated": "2026-04-21",
        }
    return {
        "source": "exits_calibration.json",
        "n_contracts": _CALIBRATION.get("n_contracts"),
        "last_updated": _CALIBRATION.get("last_updated"),
        "snapshot_dates": _CALIBRATION.get("snapshot_dates"),
    }


# ── Entry-timing windows (America/New_York) ──────────────────────────────────

# These can be overridden by exits_calibration.json → entry_windows.
# See _load_entry_windows() below.
ENTRY_WINDOW_OPEN: time    # market open — ideal fill window
ENTRY_WINDOW_GRACE: time   # acceptable with ~15% ROI haircut
ENTRY_WINDOW_CUTOFF: time  # after this: skip the trade

# Late-day cutoff — never open new positions into the close.
LATE_DAY_CUTOFF = time(15, 30)

NY = ZoneInfo("America/New_York")


def _now_et() -> time:
    return datetime.now(tz=NY).time()


def entry_allowed_now(now: time | None = None) -> tuple[bool, str]:
    """
    Returns (allowed, reason). Use at order-submit time.

    The engine should NOT place new opening orders outside the window —
    the empirical edge from the scorer decays too quickly after 10:00 ET.
    """
    t = now or _now_et()
    if t < ENTRY_WINDOW_OPEN:
        return False, f"pre-market ({t.strftime('%H:%M')} ET) — wait for open"
    if t <= ENTRY_WINDOW_GRACE:
        return True, "open window (09:30-09:45 ET) — ideal fill"
    if t <= ENTRY_WINDOW_CUTOFF:
        return True, "grace window (09:45-10:00 ET) — expect ~15% ROI haircut"
    if t >= LATE_DAY_CUTOFF:
        return False, f"late-day cutoff ({t.strftime('%H:%M')} ET) — no new entries"
    return False, (
        f"after entry cutoff ({t.strftime('%H:%M')} ET > 10:00) — "
        "signal edge has decayed; re-score tomorrow"
    )


def entry_roi_haircut(now: time | None = None) -> float:
    """
    Expected ROI haircut vs 09:30 open entry, as a multiplier on expected P&L.
    1.00 = no haircut. 0.00 = break-even. Negative = expected loss.

    Derived from 04-21 timing table:
      09:30 → 1.00   09:45 → 0.55   10:00 → 0.11
    """
    t = now or _now_et()
    if t <= ENTRY_WINDOW_OPEN:
        return 1.00
    if t <= ENTRY_WINDOW_GRACE:
        return 0.55
    if t <= ENTRY_WINDOW_CUTOFF:
        return 0.11
    return 0.0


# ── Stop-loss / take-profit tiers by score ───────────────────────────────────

# Tier definitions. Each tuple is (stop_loss_pct, take_profit_pct) where:
#   stop_loss_pct   — negative fraction of entry; e.g. -0.10 = exit at -10%
#                     None = no hard stop (let it ride, reassess EOD)
#   take_profit_pct — positive fraction; e.g. 2.0 = exit at +200%
#                     None = no take-profit (grid search showed TPs hurt)
#
# Thresholds are inclusive on the upper bound of each bucket.
_DEFAULT_EXIT_TIERS: list[tuple[int, int, float | None, float | None]] = [
    # (score_min, score_max, sl_pct, tp_pct)
    # Grid-search optimum was (80-100, None, None) but that only measures
    # INTRADAY behavior. Overnight gap risk and theta bleed force a hard
    # safety floor even on top-tier signals — see HARD_STOP_FLOOR below
    # and apply_safety_floors() which overrides these values.
    (80, 100, -0.35, None),   # top — grid said None; safety floor forces -35%
    (60,  79, -0.20, None),   # mid — loose stop, grid-optimal
    ( 0,  59, -0.10, None),   # low — tight stop, grid-optimal
]

# ── HARD SAFETY FLOORS — always enforced regardless of tier/calibration ──────
#
# These exist because the grid-search optimizer measured intraday 5m bars
# on a trending tape. It cannot see:
#
#   • Overnight gaps — a position held to close can open -50% or worse
#     on bad news before you can react. A hard stop on the overnight
#     chain limits this.
#   • Theta decay on multi-day holds — "let it ride" for 80+ score
#     rules bleed premium every day. Caps on hold time matter.
#   • Catalyst proximity — earnings/FOMC/PDUFA inside the holding window
#     dominate the directional signal. Positions into catalysts need
#     tighter stops regardless of score.
#
# apply_safety_floors() enforces these on TOP of whatever the tiered
# rule returned — it only ever TIGHTENS a stop, never loosens it.

HARD_STOP_FLOOR = -0.40     # no position, regardless of score, is allowed past -40% intraday
OVERNIGHT_STOP_FLOOR = -0.25  # tighter stop must be in place for any position held overnight
CATALYST_STOP_FLOOR = -0.15   # tighter still if earnings/FOMC/PDUFA within CATALYST_WINDOW_DAYS
CATALYST_WINDOW_DAYS = 3      # any catalyst within this many days triggers catalyst floor

# Profit-protection: once a position reaches this gain, activate a trailing
# stop that rides with the position. This locks in gains without capping
# upside — addresses "no TP" risk from the grid search.
TRAILING_STOP_ARM_PCT = 0.25   # arm trailing stop once position is +25%
TRAILING_STOP_DISTANCE = 0.15  # trailing stop stays 15% below the high-water-mark

# Theta/time-decay guard: if the position has bled more than this fraction
# of premium AND has less than this DTE remaining, force-close at EOD
# regardless of PnL direction — you're paying to hold a decaying asset.
THETA_DECAY_PCT_TRIGGER = 0.15   # down 15%+
THETA_DECAY_DTE_TRIGGER = 10     # with <10 DTE remaining


def apply_safety_floors(
    score: float,
    dte: int,
    has_catalyst_in_window: bool = False,
    held_overnight: bool = False,
) -> tuple[float, float | None]:
    """
    Return the (stop_loss_pct, take_profit_pct) that the engine should
    actually use — grid-search tier TIGHTENED by the safety floors.

    This is the function the trading engine should call, NOT
    exit_rules_for_score() directly. exit_rules_for_score() returns the
    pure empirical optimum; this layer adds the risk-management floors
    the grid can't see.

    Args:
        score: scorer output 0-100
        dte: days to expiry at time of entry
        has_catalyst_in_window: True if earnings/FOMC/PDUFA within
            CATALYST_WINDOW_DAYS of the holding horizon
        held_overnight: True if position will be held past the close today

    Rules (tightest wins):
        - HARD_STOP_FLOOR (-40%) always applies
        - Overnight holds clamp to OVERNIGHT_STOP_FLOOR (-25%)
        - Catalyst-proximity holds clamp to CATALYST_STOP_FLOOR (-15%)
        - Short-DTE positions (≤ FORCE_CLOSE_UNDER_DTE) clamp to -15%
    """
    sl, tp = exit_rules_for_score(score)

    # Start with the floor; None becomes the hardest allowed value.
    if sl is None:
        sl = HARD_STOP_FLOOR
    else:
        sl = max(sl, HARD_STOP_FLOOR)  # max of negatives = closer to zero = TIGHTER

    if held_overnight:
        sl = max(sl, OVERNIGHT_STOP_FLOOR)

    if has_catalyst_in_window:
        sl = max(sl, CATALYST_STOP_FLOOR)

    if dte <= FORCE_CLOSE_UNDER_DTE:
        sl = max(sl, -0.15)

    return sl, tp


def trailing_stop_state(entry: float, peak: float, current: float) -> dict:
    """
    Compute trailing-stop state for an open position.

    Once the position reaches TRAILING_STOP_ARM_PCT (+25%), a trailing
    stop is armed at TRAILING_STOP_DISTANCE (15%) below the peak. This
    locks in gains without capping upside — the grid said "no TP" was
    optimal, and this is the safer way to honor that.

    Returns dict with:
        armed: bool — whether trailing stop is active
        stop_price: float | None — price to exit at (if armed)
        locked_in_pct: float — minimum gain locked in if stop fires
    """
    gain_pct = (current / entry) - 1
    peak_gain_pct = (peak / entry) - 1
    armed = peak_gain_pct >= TRAILING_STOP_ARM_PCT
    if not armed:
        return {"armed": False, "stop_price": None, "locked_in_pct": 0.0}
    stop_price = peak * (1 - TRAILING_STOP_DISTANCE)
    locked_in_pct = (stop_price / entry) - 1
    return {
        "armed": True,
        "stop_price": stop_price,
        "locked_in_pct": locked_in_pct,
        "triggered": current <= stop_price,
    }


def should_force_close_theta(pnl_pct: float, dte: int) -> bool:
    """
    EOD theta guard. Force-close any position that has bled
    THETA_DECAY_PCT_TRIGGER with fewer than THETA_DECAY_DTE_TRIGGER DTE.
    At that point you're paying theta every day hoping for a bounce that
    the scorer didn't predict — cut it.
    """
    return pnl_pct <= -THETA_DECAY_PCT_TRIGGER and dte <= THETA_DECAY_DTE_TRIGGER


# ── Cash-account same-day exit guard ─────────────────────────────────────────

def same_day_exit_allowed(
    entry_date: date | datetime,
    exit_trigger_date: date | datetime | None = None,
) -> tuple[bool, str]:
    """
    Returns (allowed, reason). Cash accounts settle options T+1; same-day
    buy+sell produces unsettled proceeds, and redeploying unsettled cash
    into another trade that also sells before settlement = good-faith
    violation. Three GFVs = 90-day cash-only lockout.

    If RISK['cash_account_no_same_day_exit'] is True, this returns
    (False, reason) whenever trigger occurs on the same calendar day as
    entry — the engine should hold the position to the next session and
    exit then.

    Pass a datetime to use actual dates; pass None for trigger_date to
    use today's date.
    """
    # Local import to avoid circular dependency at module load
    from risk.config import RISK

    if not RISK.get("cash_account_no_same_day_exit", False):
        return True, "same-day exits permitted (cash-account guard off)"

    if isinstance(entry_date, datetime):
        entry_d = entry_date.date()
    else:
        entry_d = entry_date

    trig = exit_trigger_date or date.today()
    if isinstance(trig, datetime):
        trig = trig.date()

    if trig > entry_d:
        return True, f"exit on {trig} after entry {entry_d} — settled OK"

    return False, (
        f"same-day exit blocked (entry {entry_d}, trigger {trig}) — "
        "cash account T+1 settlement. Queue exit for next session."
    )


def _load_tiers() -> list[tuple[int, int, float | None, float | None]]:
    if not _CALIBRATION or "tiers" not in _CALIBRATION:
        return _DEFAULT_EXIT_TIERS
    try:
        return [
            (int(t["score_min"]), int(t["score_max"]),
             (float(t["sl_pct"]) if t.get("sl_pct") is not None else None),
             (float(t["tp_pct"]) if t.get("tp_pct") is not None else None))
            for t in _CALIBRATION["tiers"]
        ]
    except Exception:
        return _DEFAULT_EXIT_TIERS


def _load_entry_windows() -> tuple[time, time, time]:
    if not _CALIBRATION or "entry_windows" not in _CALIBRATION:
        return time(9, 30), time(9, 45), time(10, 0)
    try:
        ew = _CALIBRATION["entry_windows"]
        parse = lambda s: time(*map(int, s.split(":")))
        return parse(ew["open"]), parse(ew["grace"]), parse(ew["cutoff"])
    except Exception:
        return time(9, 30), time(9, 45), time(10, 0)


EXIT_TIERS = _load_tiers()
ENTRY_WINDOW_OPEN, ENTRY_WINDOW_GRACE, ENTRY_WINDOW_CUTOFF = _load_entry_windows()


def exit_rules_for_score(score: float) -> tuple[float | None, float | None]:
    """
    Returns (stop_loss_pct, take_profit_pct) for a trade with given score.

    stop_loss_pct is negative (e.g. -0.10). take_profit_pct is positive
    (e.g. 2.0). Either may be None — in that case the engine should not
    place that side of the bracket order.
    """
    s = max(0, min(100, round(float(score))))
    for lo, hi, sl, tp in EXIT_TIERS:
        if lo <= s <= hi:
            return sl, tp
    # defensive fallback — should never hit
    return -0.10, None


def describe_exit_rule(score: float, dte: int = 30,
                       has_catalyst_in_window: bool = False,
                       held_overnight: bool = True) -> str:
    sl, tp = apply_safety_floors(score, dte, has_catalyst_in_window, held_overnight)
    tp_str = f"TP {tp*100:+.0f}%" if tp is not None else "no TP (trailing @ +25% arm)"
    return (f"score {score:.0f}, {dte}DTE → SL {sl*100:+.0f}%, {tp_str}"
            f"{'  [catalyst]' if has_catalyst_in_window else ''}")


# ── End-of-day / time-decay guard ────────────────────────────────────────────

# Force-close any open position with fewer than this many DTE at EOD.
# Protects against the gamma cliff in the last trading week.
FORCE_CLOSE_UNDER_DTE = 5


def should_force_close(dte: int) -> bool:
    return dte <= FORCE_CLOSE_UNDER_DTE


__all__ = [
    "ENTRY_WINDOW_OPEN", "ENTRY_WINDOW_GRACE", "ENTRY_WINDOW_CUTOFF",
    "LATE_DAY_CUTOFF", "EXIT_TIERS", "FORCE_CLOSE_UNDER_DTE",
    "HARD_STOP_FLOOR", "OVERNIGHT_STOP_FLOOR", "CATALYST_STOP_FLOOR",
    "CATALYST_WINDOW_DAYS", "TRAILING_STOP_ARM_PCT", "TRAILING_STOP_DISTANCE",
    "THETA_DECAY_PCT_TRIGGER", "THETA_DECAY_DTE_TRIGGER",
    "entry_allowed_now", "entry_roi_haircut",
    "exit_rules_for_score", "apply_safety_floors", "describe_exit_rule",
    "trailing_stop_state", "should_force_close", "should_force_close_theta",
    "same_day_exit_allowed", "calibration_info",
]
