"""
Strategy backtester — replay each candidate snapshot through different
selection + exit rules, score by REALIZED round-trip P&L.

Unlike signal_edge_backtest (which only correlates signals against
mid-to-mid return), this tool models full execution:

  Entry:  ASK side of snapshot day's quote (you cross the spread to BUY)
  Exit:   BID side of exit day's quote (you cross the spread to SELL)
  Hold:   N trading days OR stop-loss hits, whichever first

This is the honest answer to "would this strategy have made money?"
Spread cost is baked in.

Each strategy is a (selector, exit_rule) pair:

    selector(row)            -> bool       # would we have entered?
    exit_rule(row, forward)  -> dict       # when/at-what-price did we exit?

We sweep 12+ strategy variants, plus the current production scorer, and
rank them by:

  - n_trades
  - win_rate
  - avg_return (per trade, after round-trip spread)
  - expectancy ($ per $100 capital deployed)
  - max single-trade loss
  - sample-Sharpe (mean / stdev * sqrt(n))

Output: logs/strategy_backtest_report.md ranked table + per-strategy detail.

Usage:
    python -m tools.strategy_backtest                  # full sweep
    python -m tools.strategy_backtest --strategy NAME  # one variant
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

SNAPSHOT_DIR = REPO_ROOT / "snapshots"
DB_PATH = REPO_ROOT / "engine_state.db"
REPORT_PATH = REPO_ROOT / "logs" / "strategy_backtest_report.md"

# Realistic execution assumptions
MAX_HOLD_DAYS = 5          # cap any strategy at 5 trading days
ENTRY_USES_ASK = True       # cross spread on entry
EXIT_USES_BID  = True       # cross spread on exit
STARTING_CAPITAL = 4000.0   # match real account
MAX_PER_TRADE = 200.0       # cap position size
TRADES_PER_DAY_LIMIT = 3    # don't lever up infinitely on busy days


# ── Data loading ─────────────────────────────────────────────────────────────

def _trading_days_after(d: str, n: int) -> str:
    """N trading days after d (skip weekends only — holidays not modeled
    but this dataset is short enough they don't materially matter)."""
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    added = 0
    while added < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            added += 1
    return dt.isoformat()


def _occ_key(symbol: str, opt_type: str, strike: float, expiry: str) -> str:
    return f"{symbol}|{(opt_type or '').lower()[:1]}|{float(strike):.2f}|{expiry}"


def load_snapshots() -> list[dict]:
    rows: list[dict] = []
    for fpath in sorted(SNAPSHOT_DIR.glob("*_auto-*.json")):
        try:
            d = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        snap_date = d.get("snapshot_date")
        if not snap_date:
            continue
        cands = d.get("universe") or d.get("trades") or []
        for c in cands:
            sym = c.get("symbol")
            opt_type = c.get("type") or c.get("option_type")
            strike = c.get("strike")
            expiry = c.get("expiry")
            if not all([sym, opt_type, strike, expiry]):
                continue
            row = dict(c)
            row["snapshot_date"] = snap_date
            row["symbol"] = sym
            row["option_type"] = (opt_type or "").lower()
            row["strike"] = float(strike)
            row["expiry"] = expiry
            row["occ_key"] = _occ_key(sym, opt_type, strike, expiry)
            rows.append(row)
    return rows


def load_chain_surface() -> dict[tuple, dict]:
    if not DB_PATH.exists():
        return {}
    out: dict[tuple, dict] = {}
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        for r in c.execute(
            "SELECT snapshot_date, symbol, option_type, strike, expiry, "
            "bid, ask, last_price FROM chain_surface"
        ):
            key = _occ_key(r["symbol"], r["option_type"], r["strike"], r["expiry"])
            bid = float(r["bid"] or 0)
            ask = float(r["ask"] or 0)
            last = float(r["last_price"] or 0)
            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else (last or 0)
            if mid <= 0:
                continue
            out[(r["snapshot_date"], key)] = {
                "bid": bid, "ask": ask, "mid": mid, "last": last
            }
    return out


# ── Execution model ──────────────────────────────────────────────────────────

def simulate_trade(row: dict, surface: dict[tuple, dict],
                    exit_rule: Callable, max_hold: int = MAX_HOLD_DAYS) -> dict | None:
    """Simulate one trade: enter at snapshot day's ask, exit per rule.
    Returns {entry_px, exit_px, exit_day, hold_days, pnl_pct, ok}."""
    snap_date = row["snapshot_date"]
    # Entry — use ask if available, else snapshot's mid
    bid_s = float(row.get("bid") or 0)
    ask_s = float(row.get("ask") or 0)
    if ENTRY_USES_ASK and ask_s > 0:
        entry_px = ask_s
    elif bid_s > 0 and ask_s > 0:
        entry_px = (bid_s + ask_s) / 2.0
    else:
        entry_px = float(row.get("entry_price") or 0)
    if entry_px <= 0:
        return None

    # Spread sanity — if no bid at all, can't model exit
    if bid_s <= 0:
        return None
    spread_pct = (ask_s - bid_s) / ((ask_s + bid_s) / 2.0) if bid_s > 0 else 0

    # Walk forward. peak tracks MID (matches production daemon, which uses
    # mid for SL and trailing eval).
    entry_mid = _entry_basis(row)
    prior_peak = max(entry_mid, 0.0001)
    sl_pct = -0.12   # default SL; overrideable per-strategy
    trail_lock = None
    for n in range(1, max_hold + 1):
        future_date = _trading_days_after(snap_date, n)
        f = surface.get((future_date, row["occ_key"]))
        if not f:
            continue
        cur_bid = f["bid"]
        cur_mid = f["mid"]
        if cur_bid <= 0:
            continue
        # Exit rule decides
        result = exit_rule(row, n, prior_peak, cur_bid, cur_mid, sl_pct, trail_lock)
        if result is None:
            # Update trailing peak with current mid
            if cur_mid > prior_peak:
                prior_peak = cur_mid
            continue
        # exit_rule returned an exit decision
        exit_px = result.get("exit_px", cur_bid)
        return {
            "ok": True,
            "entry_px": round(entry_px, 4),
            "exit_px": round(exit_px, 4),
            "exit_day": result.get("exit_day", n),
            "hold_days": n,
            "pnl_pct": round((exit_px / entry_px) - 1, 4),
            "spread_pct": round(spread_pct, 3),
            "exit_reason": result.get("reason", "rule"),
        }
    # Hit max hold without exit — close at last available bid
    for n in range(max_hold, 0, -1):
        future_date = _trading_days_after(snap_date, n)
        f = surface.get((future_date, row["occ_key"]))
        if f and f["bid"] > 0:
            return {
                "ok": True,
                "entry_px": round(entry_px, 4),
                "exit_px": round(f["bid"], 4),
                "exit_day": n,
                "hold_days": n,
                "pnl_pct": round((f["bid"] / entry_px) - 1, 4),
                "spread_pct": round(spread_pct, 3),
                "exit_reason": "max_hold",
            }
    return None


# ── Exit rules ────────────────────────────────────────────────────────────────

# NOTE: SL/trailing triggers measure against MID (matches production daemon
# which uses q.mid for pnl_pct). EXIT executes at BID (we cross the spread
# to actually sell). This means an SL of -12% can fire while real liquidation
# value is worse (e.g., -25% if spread is 13% wide). That's exactly what
# happens in production and is the honest answer.

def _entry_basis(row: dict) -> float:
    """The reference price we mark SL against. Use snapshot MID so we don't
    immediately trigger SL just from crossing the spread on entry."""
    bid_s = float(row.get("bid") or 0)
    ask_s = float(row.get("ask") or 0)
    if bid_s > 0 and ask_s > 0:
        return (bid_s + ask_s) / 2.0
    return float(row.get("entry_price") or 0)


def make_sl_only(sl: float):
    """Fixed SL only; hold to max_hold otherwise. SL triggers on MID, exits at BID."""
    def rule(row, n, peak, cur_bid, cur_mid, sl_pct, trail_lock):
        entry_mid = _entry_basis(row)
        if entry_mid <= 0:
            return None
        pnl_at_mid = (cur_mid / entry_mid) - 1
        if pnl_at_mid <= sl:
            return {"exit_px": cur_bid, "reason": f"SL {sl*100:+.0f}% hit (mid)"}
        return None
    return rule


def make_trailing(trigger: float, trail: float):
    """Arm trailing-stop at peak (mid) >= trigger; exit if mid <= peak*(1-trail)."""
    def rule(row, n, peak, cur_bid, cur_mid, sl_pct, trail_lock):
        entry_mid = _entry_basis(row)
        if entry_mid <= 0:
            return None
        peak_pnl = (peak / entry_mid) - 1
        mid_pnl = (cur_mid / entry_mid) - 1
        if mid_pnl <= -0.12:
            return {"exit_px": cur_bid, "reason": "hard SL -12% (mid)"}
        if peak_pnl >= trigger:
            trail_stop_mid = peak * (1.0 - trail)
            if cur_mid <= trail_stop_mid:
                return {"exit_px": cur_bid,
                        "reason": f"trail (peak +{peak_pnl*100:.0f}%, "
                                   f"drop -{trail*100:.0f}%)"}
        return None
    return rule


def make_aggressive_trail():
    """The 2026-05-06 production tier — first tier @ peak +5%."""
    tiers = [(0.05, 0.00), (0.10, 0.05), (0.20, 0.12), (0.35, 0.25),
             (0.50, 0.38), (0.75, 0.60), (1.00, 0.85)]
    def rule(row, n, peak, cur_bid, cur_mid, sl_pct, trail_lock):
        entry_mid = _entry_basis(row)
        if entry_mid <= 0:
            return None
        peak_pnl = (peak / entry_mid) - 1
        mid_pnl = (cur_mid / entry_mid) - 1
        if mid_pnl <= -0.12:
            return {"exit_px": cur_bid, "reason": "hard SL -12% (mid)"}
        locked = None
        for trig, lock in tiers:
            if peak_pnl >= trig:
                locked = lock
        if locked is not None and mid_pnl <= locked:
            return {"exit_px": cur_bid,
                    "reason": f"ratchet (peak +{peak_pnl*100:.0f}%, "
                               f"lock {locked*100:+.0f}%)"}
        return None
    return rule


def make_no_sl():
    """Hold to max — no SL. Useful to see raw distribution."""
    def rule(row, n, peak, cur_bid, cur_mid, sl_pct, trail_lock):
        return None
    return rule


def make_force_exit_at_day(exit_day: int, sl: float = -0.12):
    """Force exit at day N, with optional SL fallback. Used for testing
    short-horizon strategies (e.g., PINNED gex is a d1 edge)."""
    def rule(row, n, peak, cur_bid, cur_mid, sl_pct, trail_lock):
        entry_mid = _entry_basis(row)
        if entry_mid <= 0:
            return None
        mid_pnl = (cur_mid / entry_mid) - 1
        if mid_pnl <= sl:
            return {"exit_px": cur_bid, "reason": f"SL {sl*100:+.0f}% hit (mid)"}
        if n >= exit_day:
            return {"exit_px": cur_bid, "reason": f"forced exit at d{exit_day}"}
        return None
    return rule


def make_spread_aware_sl(mid_sl: float, bid_sl: float):
    """Exit on the TIGHTER of two conditions: mid pnl <= mid_sl OR
    bid pnl <= bid_sl. Catches wide-spread names where the bid
    collapses well before mid does. Insight from 5/11-5/15 trades
    where all 9 losers had mid SL trigger at -12% but bid realized
    -23% to -94%."""
    def rule(row, n, peak, cur_bid, cur_mid, sl_pct, trail_lock):
        entry_mid = _entry_basis(row)
        if entry_mid <= 0:
            return None
        # Entry ask is what we actually paid
        ask_s = float(row.get("ask") or 0)
        if ask_s <= 0:
            ask_s = entry_mid
        mid_pnl = (cur_mid / entry_mid) - 1
        bid_pnl_vs_paid = (cur_bid / ask_s) - 1
        if mid_pnl <= mid_sl:
            return {"exit_px": cur_bid, "reason": f"mid SL {mid_sl*100:+.0f}%"}
        if bid_pnl_vs_paid <= bid_sl:
            return {"exit_px": cur_bid,
                    "reason": f"bid SL {bid_sl*100:+.0f}% (vs paid)"}
        return None
    return rule


# ── Strategies (selector + exit) ─────────────────────────────────────────────

def s_baseline_current(row): return row.get("score", 0) >= 60

def s_score_80plus(row): return row.get("score", 0) >= 80

def s_pinned_calls(row):
    return (row.get("gex_signal") == "PINNED"
            and row.get("option_type") == "call"
            and row.get("score", 0) >= 50)

def s_squeeze_calls(row):
    return (row.get("short_signal") == "SQUEEZE_SETUP"
            and row.get("option_type") == "call"
            and row.get("score", 0) >= 50)

def s_bullish_skew_calls(row):
    return (row.get("skew_signal") == "BULLISH"
            and row.get("option_type") == "call"
            and row.get("score", 0) >= 50)

def s_catalyst_calls(row):
    return (row.get("has_recent_8k") is True
            and row.get("option_type") == "call"
            and row.get("score", 0) >= 50)

def s_momentum_buy(row):
    return row.get("vol_signal") == "MOMENTUM BUY"

def s_reversion_buy(row):
    return row.get("vol_signal") == "REVERSION BUY"

def s_buy_vol_calls(row):
    return (row.get("vol_signal") == "BUY VOL"
            and row.get("option_type") == "call")

def s_buy_vol_calls_tight_spread(row):
    if not s_buy_vol_calls(row): return False
    bid = float(row.get("bid") or 0); ask = float(row.get("ask") or 0)
    if bid <= 0 or ask <= 0: return False
    mid = (bid + ask) / 2.0
    if mid <= 0: return False
    return ((ask - bid) / mid) <= 0.15   # max 15% spread

def s_insider_cluster_calls(row):
    return (row.get("insider_signal") == "CLUSTER_BUY"
            and row.get("option_type") == "call"
            and row.get("score", 0) >= 50)

def s_high_conviction_call_stack(row):
    """Multi-signal alignment — call MUST have:
      score >= 70 AND BUY VOL AND tight spread AND
      (PINNED or SQUEEZE_SETUP or BULLISH skew)"""
    if row.get("option_type") != "call": return False
    if row.get("score", 0) < 70: return False
    if row.get("vol_signal") != "BUY VOL": return False
    bid = float(row.get("bid") or 0); ask = float(row.get("ask") or 0)
    if bid <= 0 or ask <= 0: return False
    mid = (bid + ask) / 2.0
    if mid <= 0 or (ask - bid) / mid > 0.20: return False
    confirms = sum([
        row.get("gex_signal") == "PINNED",
        row.get("short_signal") == "SQUEEZE_SETUP",
        row.get("skew_signal") == "BULLISH",
    ])
    return confirms >= 1


# ── Spread-tightness gates (the single most explanatory factor) ──────────────

def _spread_pct(row: dict) -> float | None:
    bid = float(row.get("bid") or 0)
    ask = float(row.get("ask") or 0)
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    return (ask - bid) / mid if mid > 0 else None


def _liquid_call(row: dict, max_spread: float = 0.10,
                  min_volume: int = 0, min_oi: int = 0) -> bool:
    """Spread-first liquidity filter. Volume/OI defaults relaxed since
    most contracts in our universe have low absolute volume even when
    the spread is reasonable."""
    if row.get("option_type") != "call":
        return False
    sp = _spread_pct(row)
    if sp is None or sp > max_spread:
        return False
    if min_volume and int(row.get("volume") or 0) < min_volume:
        return False
    if min_oi and int(row.get("open_interest") or 0) < min_oi:
        return False
    return True


# Spread-only call filters (no extra volume/OI gate)
def s_tight10_anyscore(row): return _liquid_call(row, max_spread=0.10)
def s_tight15_anyscore(row): return _liquid_call(row, max_spread=0.15)
def s_tight20_anyscore(row): return _liquid_call(row, max_spread=0.20)
def s_tight5_anyscore(row):  return _liquid_call(row, max_spread=0.05)

# Spread + score
def s_tight15_score75(row):
    return _liquid_call(row, max_spread=0.15) and row.get("score", 0) >= 75
def s_tight15_score85(row):
    return _liquid_call(row, max_spread=0.15) and row.get("score", 0) >= 85

# Spread + categorical signal
def s_tight15_pinned(row):
    return _liquid_call(row, max_spread=0.15) and row.get("gex_signal") == "PINNED"
def s_tight15_squeeze(row):
    return (_liquid_call(row, max_spread=0.15)
            and row.get("short_signal") == "SQUEEZE_SETUP")
def s_tight15_buyvol(row):
    return _liquid_call(row, max_spread=0.15) and row.get("vol_signal") == "BUY VOL"
def s_tight15_bullskew(row):
    return _liquid_call(row, max_spread=0.15) and row.get("skew_signal") == "BULLISH"

# Cheap-premium bracket (at $4K, $200/trade only fits 1-2 contracts of cheap)
def s_tight15_cheap(row):
    if not _liquid_call(row, max_spread=0.15): return False
    bid = float(row.get("bid") or 0); ask = float(row.get("ask") or 0)
    mid = (bid + ask) / 2.0
    return 0.20 <= mid <= 1.00


# Liquid mega-cap whitelist (used by T14a / T14b)
LIQUID_SYMS = {"AAPL","MSFT","AMZN","GOOGL","AVGO","ORCL","CRM",
                "JPM","BAC","GS","LLY","UNH","WMT","COST","XOM",
                # also accept existing mega-cap-behavior names
                "TSLA","NVDA","AMD","META","PLTR","COIN","MSTR"}


STRATEGIES = [
    # Baseline / current production
    ("01_baseline_score60", s_baseline_current, make_sl_only(-0.12)),
    ("02_score80plus", s_score_80plus, make_sl_only(-0.12)),

    # Single-signal calls
    ("03_pinned_calls", s_pinned_calls, make_sl_only(-0.12)),
    ("04_squeeze_calls", s_squeeze_calls, make_sl_only(-0.12)),
    ("05_bullish_skew_calls", s_bullish_skew_calls, make_sl_only(-0.12)),
    ("06_catalyst_calls", s_catalyst_calls, make_sl_only(-0.12)),
    ("07_momentum_buy", s_momentum_buy, make_sl_only(-0.12)),
    ("08_reversion_buy", s_reversion_buy, make_sl_only(-0.12)),

    # BUY VOL bracket
    ("09_buy_vol_calls_anyspread", s_buy_vol_calls, make_sl_only(-0.12)),
    ("10_buy_vol_calls_tight", s_buy_vol_calls_tight_spread, make_sl_only(-0.12)),
    ("11_insider_cluster_calls", s_insider_cluster_calls, make_sl_only(-0.12)),
    ("12_high_conviction_stack", s_high_conviction_call_stack, make_sl_only(-0.12)),

    # Trailing variants
    ("12b_stack_aggressive_trail", s_high_conviction_call_stack, make_aggressive_trail()),
    ("10b_buyvol_tight_trail30", s_buy_vol_calls_tight_spread, make_trailing(0.10, 0.30)),
    ("04b_squeeze_trail30", s_squeeze_calls, make_trailing(0.10, 0.30)),

    # NEW: pure spread-tightness experiments
    ("13_tight5pct_any", s_tight5_anyscore, make_sl_only(-0.12)),
    ("14_tight10pct_any", s_tight10_anyscore, make_sl_only(-0.12)),
    ("15_tight15pct_any", s_tight15_anyscore, make_sl_only(-0.12)),
    ("16_tight20pct_any", s_tight20_anyscore, make_sl_only(-0.12)),

    # Spread + score
    ("17_tight15_score75", s_tight15_score75, make_sl_only(-0.12)),
    ("18_tight15_score85", s_tight15_score85, make_sl_only(-0.12)),

    # Spread + categorical
    ("19_tight15_pinned", s_tight15_pinned, make_sl_only(-0.12)),
    ("20_tight15_squeeze", s_tight15_squeeze, make_sl_only(-0.12)),
    ("21_tight15_buyvol", s_tight15_buyvol, make_sl_only(-0.12)),
    ("22_tight15_bullskew", s_tight15_bullskew, make_sl_only(-0.12)),

    # Spread + exit variants on the buyvol subset
    ("23_tight15_buyvol_nosl", s_tight15_buyvol, make_no_sl()),
    ("24_tight15_buyvol_trail30", s_tight15_buyvol, make_trailing(0.10, 0.30)),
    ("25_tight15_buyvol_aggtrail", s_tight15_buyvol, make_aggressive_trail()),

    # Spread + cheap premium
    ("26_tight15_cheap", s_tight15_cheap, make_sl_only(-0.12)),
    ("27_tight15_cheap_trail30", s_tight15_cheap, make_trailing(0.10, 0.30)),

    # ── BULLISH SKEW ZOOM — the only positive strategy found so far ──────────
    # Variants on the winner to find the sweet spot
    ("30_bullskew_tight20", lambda r: r.get("option_type")=="call"
                                      and r.get("skew_signal")=="BULLISH"
                                      and (_spread_pct(r) or 1) <= 0.20,
     make_sl_only(-0.12)),
    ("31_bullskew_tight15_loose_sl", s_tight15_bullskew, make_sl_only(-0.25)),
    ("32_bullskew_tight15_nosl", s_tight15_bullskew, make_no_sl()),
    ("33_bullskew_tight15_trail50", s_tight15_bullskew, make_trailing(0.30, 0.50)),
    # Concentrate: 1 trade/day on best bullish-skew pick
    # (handled via TRADES_PER_DAY_LIMIT global — separate run later)

    # Combined bullish layers
    ("34_bullskew_AND_pinned",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("gex_signal")=="PINNED"
               and (_spread_pct(r) or 1) <= 0.15,
     make_sl_only(-0.12)),
    ("35_bullskew_AND_buyvol",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.15,
     make_sl_only(-0.12)),

    # ── 2026-05-26: PATH A + PATH B candidates after the ML CV verdict ─────
    # Cross-validation killed the ML approach (single-split fluke). The
    # actual finding: universe mean d3 return is -30%, so the picker is
    # fishing in a poisoned pond. Two structural responses:
    #
    #   Path A (T14): pivot to LIQUID mega-cap underlyings (1-3% spreads
    #                 vs 30%+) to remove ~10pts of round-trip cost. No
    #                 historical data yet (universe just expanded) — this
    #                 strategy returns 0 trades on the backtest until
    #                 data accumulates. Wire it now so the moment data
    #                 lands, it starts firing.
    #
    #   Path B (T15): tighten v1.2 further within the existing universe.
    #                 spread <= 5% (was 10%), DTE >= 30 (was 14), and
    #                 mid >= $0.50 to ensure enough cushion per contract.

    # T14a: liquid mega-cap calls with BULLISH skew + BUY VOL + spread<=5%
    ("T14a_liquid_strict_v12",
     lambda r, _L=LIQUID_SYMS: (
         r.get("option_type") == "call"
         and r.get("symbol") in _L
         and r.get("skew_signal") == "BULLISH"
         and r.get("vol_signal") == "BUY VOL"
         and (_spread_pct(r) or 1) <= 0.05
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # T14b: liquid mega-cap calls with relaxed signal (any vol_signal),
    # since BUY VOL is rare on liquid names (IV is efficient). Test
    # whether liquidity alone + bullish positioning produces edge.
    ("T14b_liquid_relaxed",
     lambda r, _L=LIQUID_SYMS: (
         r.get("option_type") == "call"
         and r.get("symbol") in _L
         and r.get("skew_signal") in ("BULLISH", "NEUTRAL")
         and (_spread_pct(r) or 1) <= 0.05
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # T15: tightened v1.3 — spread<=5%, DTE>=30, mid>=$0.50
    ("T15_v13_strict",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") == "BULLISH"
         and r.get("vol_signal") == "BUY VOL"
         and (_spread_pct(r) or 1) <= 0.05
         and 30 <= int(r.get("dte") or 0) <= 45
         and ((float(r.get("bid") or 0) + float(r.get("ask") or 0)) / 2.0) >= 0.50),
     make_sl_only(-0.12)),

    # T15a: same as T15 but DTE relaxed back to 14+. Most strategy_v1.2
    # historical winners had DTE 14-30 (5-day max_hold within their life).
    ("T15a_v13_spread5_dte14",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") == "BULLISH"
         and r.get("vol_signal") == "BUY VOL"
         and (_spread_pct(r) or 1) <= 0.05
         and 14 <= int(r.get("dte") or 0) <= 45
         and ((float(r.get("bid") or 0) + float(r.get("ask") or 0)) / 2.0) >= 0.30),
     make_sl_only(-0.12)),

    # T15b: just tighten spread to 5%, keep all other v1.2 rules
    ("T15b_v13_spread5_only",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") == "BULLISH"
         and r.get("vol_signal") == "BUY VOL"
         and (_spread_pct(r) or 1) <= 0.05
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # T15c: spread<=7% (less aggressive than T15b's 5%)
    ("T15c_v13_spread7",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") == "BULLISH"
         and r.get("vol_signal") == "BUY VOL"
         and (_spread_pct(r) or 1) <= 0.07
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # ── 2026-05-28: EARLIER-ENTRY relaxations from 5/27 near-miss analysis ──
    # Yesterday: 2 full v1.2 qualifiers (QS, CLF). 27 near-misses (3-of-4).
    # The interesting subset:
    #   - 5 PLUG/BBAI contracts missed only because skew=NEUTRAL
    #   - 3 contracts (PATH, S) missed only because vol=MOMENTUM_BUY
    # PLUG 4C 6/18 is in our shadow ledger as a +48% winner — it fits this
    # exact pattern. Test relaxing each constraint:

    # T16: allow skew BULLISH OR NEUTRAL (catches PLUG/BBAI sooner)
    ("T16_skew_bullish_or_neutral",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") in ("BULLISH", "NEUTRAL")
         and r.get("vol_signal") == "BUY VOL"
         and (_spread_pct(r) or 1) <= 0.10
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # T17: allow vol BUY VOL OR MOMENTUM BUY (catches directional plays)
    ("T17_vol_buyvol_or_momentum",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") == "BULLISH"
         and r.get("vol_signal") in ("BUY VOL", "MOMENTUM BUY")
         and (_spread_pct(r) or 1) <= 0.10
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # T18: BOTH relaxations combined
    ("T18_skew_and_vol_relaxed",
     lambda r: (
         r.get("option_type") == "call"
         and r.get("skew_signal") in ("BULLISH", "NEUTRAL")
         and r.get("vol_signal") in ("BUY VOL", "MOMENTUM BUY")
         and (_spread_pct(r) or 1) <= 0.10
         and 14 <= int(r.get("dte") or 0) <= 45),
     make_sl_only(-0.12)),

    # ── 2026-05-22 TWEAK CANDIDATES — does removing legacy $5 floor help? ──
    # BBAI is in our backtest history as a +41% strategy_v1 winner but the
    # legacy min_underlying_price=$5 filter blocks BBAI ($4.28) and LAES
    # ($3.55) BEFORE strategy_v1's gate sees them. Today (5/22) both were
    # rejected on the price floor with no other gates evaluated.

    # T7: strategy_v1.1 rules (same as production T2) — baseline alias
    # for readability. (Identical to T2 above; included for clarity.)

    # T8: v1.1 + allow underlying >= $2 (instead of $5)
    ("T8_bullskew_buyvol_tight10_under5ok",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.10
               and float(r.get("stock_price") or 0) >= 2.0,
     make_sl_only(-0.12)),

    # T9: v1.1 + only sub-$5 underlyings (the inverted test — if T8 wins
    # because of the sub-$5 names, T9 should also win)
    ("T9_bullskew_buyvol_tight10_under5_only",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.10
               and 2.0 <= float(r.get("stock_price") or 0) < 5.0,
     make_sl_only(-0.12)),

    # ── 2026-05-26 NEW CANDIDATES — let EOD auto-A/B surface any winner
    # as data accumulates. These don't change production until they beat
    # the baseline T2 by n>=12 AND ending_equity delta >= $200.

    # T10: even tighter spread (≤8%) — eliminate more bid-side slippage
    ("T10_bullskew_buyvol_tight8",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.08,
     make_sl_only(-0.12)),

    # T11: small-cap focus — sub-$15 underlying. Matches the historical
    # strategy_v1 backtest winners (UEC, CLOV, SOUN, BBAI, MARA-style names)
    ("T11_bullskew_buyvol_tight10_small_cap",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.10
               and 2.0 <= float(r.get("stock_price") or 0) <= 15.0,
     make_sl_only(-0.12)),

    # T12: DTE window — only 14-45 day expiries. Hypothesis: <14 has too
    # much theta risk, >45 has too much time-decay drag on directional bets
    ("T12_bullskew_buyvol_tight10_dte_window",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.10
               and 14 <= int(r.get("dte") or 0) <= 45,
     make_sl_only(-0.12)),

    # T13: add score floor — only entries with score >= 70 from existing scorer.
    # We've shown score is mostly noise but at higher tiers it may have residual
    # value combined with the v1.1 gates
    ("T13_bullskew_buyvol_tight10_score70",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.10
               and float(r.get("score") or 0) >= 70.0,
     make_sl_only(-0.12)),

    # ── 2026-05-15 TWEAK CANDIDATES — A/B vs current production ─────────────
    # Production baseline: 35_bullskew_AND_buyvol (above) — 11 trades, 46%
    # wr, +14.6% avg. Tweaks to test:

    # T1: looser — drop BUY VOL requirement (BULLISH skew + tight spread only)
    ("T1_bullskew_tight15_only",
     lambda r: r.get("option_type")=="call"
               and r.get("skew_signal")=="BULLISH"
               and (_spread_pct(r) or 1) <= 0.15,
     make_sl_only(-0.12)),

    # T2: tighter spread (15% → 10%)
    ("T2_bullskew_buyvol_tight10",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.10,
     make_sl_only(-0.12)),

    # T3: spread-aware SL — mid -12% OR bid -25%, whichever first
    ("T3_bullskew_buyvol_spread_aware_sl",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.15,
     make_spread_aware_sl(mid_sl=-0.12, bid_sl=-0.25)),

    # T4: tighter mid SL (-12% → -8%)
    ("T4_bullskew_buyvol_sl8",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and r.get("vol_signal")=="BUY VOL"
               and (_spread_pct(r) or 1) <= 0.15,
     make_sl_only(-0.08)),

    # T5: PINNED gex d1 winner — short hold (force exit at d1)
    ("T5_pinned_d1hold",
     lambda r: r.get("option_type")=="call" and r.get("gex_signal")=="PINNED"
               and (_spread_pct(r) or 1) <= 0.20,
     make_force_exit_at_day(1, sl=-0.12)),

    # T6: hybrid — bullskew+buyvol OR (bullskew + PINNED gex)
    ("T6_bullskew_with_pinned_or_buyvol",
     lambda r: r.get("option_type")=="call" and r.get("skew_signal")=="BULLISH"
               and (r.get("vol_signal")=="BUY VOL"
                    or r.get("gex_signal")=="PINNED")
               and (_spread_pct(r) or 1) <= 0.15,
     make_sl_only(-0.12)),

    # Benchmark
    ("99_hold_5d_anyentry", lambda r: True, make_no_sl()),
]


# ── Sweep ────────────────────────────────────────────────────────────────────

def run_strategy(name: str, selector: Callable, exit_rule: Callable,
                  rows: list[dict], surface: dict) -> dict:
    trades: list[dict] = []
    # Pre-group by snapshot_date so we can cap trades-per-day
    by_day: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if selector(r):
            by_day[r["snapshot_date"]].append(r)

    for d in sorted(by_day):
        # Cap: take top N by score per day (no infinite leverage)
        candidates = sorted(by_day[d],
                              key=lambda r: r.get("score", 0), reverse=True)
        for r in candidates[:TRADES_PER_DAY_LIMIT]:
            t = simulate_trade(r, surface, exit_rule)
            if t and t.get("ok"):
                t["snapshot_date"] = d
                t["symbol"] = r["symbol"]
                t["score"] = r.get("score", 0)
                t["option_type"] = r["option_type"]
                trades.append(t)

    if not trades:
        return {"name": name, "n": 0, "win_rate": None,
                "avg_return": None, "expectancy_dollar": 0,
                "max_loss_pct": None, "sharpe": None,
                "trades": []}

    pnls = [t["pnl_pct"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    win_rate = len(wins) / len(pnls)
    avg_return = statistics.mean(pnls)
    median_return = statistics.median(pnls)
    max_loss = min(pnls)
    # Sharpe-ish: mean / stdev (no annualization since we're trade-by-trade)
    if len(pnls) >= 3:
        std = statistics.stdev(pnls)
        sharpe = (avg_return / std) * math.sqrt(len(pnls)) if std > 0 else None
    else:
        sharpe = None

    # Equity curve assuming $100 per trade
    equity = STARTING_CAPITAL
    capital_per_trade = min(MAX_PER_TRADE, STARTING_CAPITAL / 20)
    realized = 0.0
    max_equity = STARTING_CAPITAL
    max_drawdown = 0.0
    for t in trades:
        delta = capital_per_trade * t["pnl_pct"]
        equity += delta
        realized += delta
        if equity > max_equity:
            max_equity = equity
        dd = (equity - max_equity) / max_equity
        if dd < max_drawdown:
            max_drawdown = dd

    return {
        "name": name,
        "n": len(trades),
        "win_rate": round(win_rate, 3),
        "avg_return": round(avg_return, 4),
        "median_return": round(median_return, 4),
        "max_loss_pct": round(max_loss, 4),
        "expectancy_dollar": round(realized, 2),
        "ending_equity": round(equity, 2),
        "max_drawdown_pct": round(max_drawdown, 3),
        "sharpe": round(sharpe, 3) if sharpe is not None else None,
        "trades": trades,
    }


def render_report(results: list[dict]) -> str:
    lines = []
    lines.append("# Strategy backtest — full sweep")
    lines.append("")
    lines.append(f"- Snapshots span: every contract scored")
    lines.append(f"- Execution model: BUY at ask, SELL at bid (round-trip "
                 f"spread cost baked in)")
    lines.append(f"- Capital: ${STARTING_CAPITAL:,.0f} starting, "
                 f"${min(MAX_PER_TRADE, STARTING_CAPITAL/20):.0f} per trade, "
                 f"max {TRADES_PER_DAY_LIMIT} trades/day")
    lines.append(f"- Max hold: {MAX_HOLD_DAYS} trading days")
    lines.append("")
    lines.append("## Ranked by ending equity")
    lines.append("")
    lines.append("| rank | strategy | n | win% | avg ret | med ret | max loss | sharpe | dd% | end equity |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    ranked = sorted(
        [r for r in results if r["n"] > 0],
        key=lambda r: r["ending_equity"], reverse=True,
    )
    for i, r in enumerate(ranked, 1):
        win_p = f"{r['win_rate']*100:.0f}%" if r["win_rate"] is not None else "-"
        ar = f"{r['avg_return']*100:+.1f}%" if r["avg_return"] is not None else "-"
        mr = f"{r['median_return']*100:+.1f}%" if r["median_return"] is not None else "-"
        ml = f"{r['max_loss_pct']*100:+.0f}%" if r["max_loss_pct"] is not None else "-"
        sh = f"{r['sharpe']:+.2f}" if r["sharpe"] is not None else "-"
        dd = f"{r['max_drawdown_pct']*100:.1f}%"
        lines.append(f"| {i} | `{r['name']}` | {r['n']} | {win_p} | "
                     f"{ar} | {mr} | {ml} | {sh} | {dd} | "
                     f"${r['ending_equity']:,.0f} |")
    lines.append("")
    # Strategies that produced 0 trades
    zeros = [r for r in results if r["n"] == 0]
    if zeros:
        lines.append("## No-fire strategies (filters too tight for this dataset)")
        lines.append("")
        for r in zeros:
            lines.append(f"- `{r['name']}` — produced 0 trades")
        lines.append("")
    # Per-strategy sample trades for the top 3
    lines.append("## Top-3 sample trades")
    lines.append("")
    for r in ranked[:3]:
        lines.append(f"### {r['name']}")
        lines.append("")
        lines.append("| date | sym | type | score | hold | pnl | reason |")
        lines.append("|---|---|---|---:|---:|---:|---|")
        for t in r["trades"][:15]:
            lines.append(f"| {t['snapshot_date']} | {t['symbol']} | "
                         f"{t['option_type']} | {t['score']:.0f} | "
                         f"{t['hold_days']}d | {t['pnl_pct']*100:+.1f}% | "
                         f"{t.get('exit_reason','')} |")
        lines.append("")
    return "\n".join(lines)


# ── A/B test helper ─────────────────────────────────────────────────────────

def ab_test(rows: list[dict], surface: dict,
            baseline_name: str = "35_bullskew_AND_buyvol",
            challengers: list[tuple] | None = None) -> str:
    """Run the current production strategy_v1 (baseline) head-to-head
    against one or more proposed variants. Prints a side-by-side
    comparison. Use to vet any tweak BEFORE deploying it live."""
    baseline_tuple = next((s for s in STRATEGIES if s[0] == baseline_name), None)
    if baseline_tuple is None:
        raise ValueError(f"baseline {baseline_name} not in STRATEGIES")
    if not challengers:
        return "no challenger strategies supplied"

    print(f"\n── A/B Test ──")
    print(f"  baseline: {baseline_name}")
    print(f"  challengers: {[c[0] for c in challengers]}\n")
    results = [run_strategy(baseline_tuple[0], baseline_tuple[1],
                              baseline_tuple[2], rows, surface)]
    for name, sel, ext in challengers:
        results.append(run_strategy(name, sel, ext, rows, surface))

    out = ["# A/B Test\n", "| strategy | n | win% | avg ret | sharpe | dd | end equity |",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for r in results:
        if r["n"] == 0:
            out.append(f"| `{r['name']}` | 0 | — | — | — | — | — |")
            continue
        wr = f"{r['win_rate']*100:.0f}%" if r['win_rate'] is not None else "—"
        ar = f"{r['avg_return']*100:+.1f}%" if r['avg_return'] is not None else "—"
        sh = f"{r['sharpe']:+.2f}" if r['sharpe'] is not None else "—"
        dd = f"{r['max_drawdown_pct']*100:.1f}%"
        out.append(f"| `{r['name']}` | {r['n']} | {wr} | {ar} | {sh} | "
                   f"{dd} | ${r['ending_equity']:,.0f} |")
    txt = "\n".join(out) + "\n"
    print(txt)
    return txt


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", help="run only this named strategy")
    ap.add_argument("--ab", nargs="+",
                    help="A/B test mode: run current strategy_v1 baseline "
                          "head-to-head against these named strategies. "
                          "Example: --ab 30_bullskew_tight20 32_bullskew_tight15_nosl")
    args = ap.parse_args()

    print("loading snapshots...")
    rows = load_snapshots()
    print(f"  {len(rows):,} candidate rows")
    print("loading chain_surface...")
    surface = load_chain_surface()
    print(f"  {len(surface):,} (date, occ) entries")

    if args.ab:
        challengers = [s for s in STRATEGIES if s[0] in args.ab]
        missing = set(args.ab) - {s[0] for s in challengers}
        if missing:
            print(f"unknown strategies: {missing}")
            return 2
        ab_test(rows, surface, challengers=challengers)
        return 0

    selected = STRATEGIES
    if args.strategy:
        selected = [s for s in STRATEGIES if s[0] == args.strategy]
        if not selected:
            print(f"unknown strategy: {args.strategy}")
            return 2

    print(f"\nrunning {len(selected)} strategies...\n")
    results = []
    for name, sel, ext in selected:
        print(f"  {name}...")
        results.append(run_strategy(name, sel, ext, rows, surface))

    md = render_report(results)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(md, encoding="utf-8")
    print(f"\nwrote {REPORT_PATH}\n")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
