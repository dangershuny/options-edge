"""
Daily execution loop — the main production entry point.

Workflow (run once per trading day, typically 09:29 ET):

    1. Market-open sanity: is today a trading day? Is market actually open?
    2. Settle yesterday's T+1 proceeds (mark_settlements_settled)
    3. Scan → generate today's BUY VOL candidates (analysis/scorer)
    4. Filter candidates through risk.checker + cash availability
    5. Size each position, submit limit orders at 1.02 × mid
    6. Record opens to engine_state.db
    7. Loop through the session monitoring triggers (SL/TP/trailing/theta)
       — queue exits for next session if same-day (cash account rule)
    8. At EOD: fire queued exits scheduled for today, place next-day
       exit orders for anything that queued today

This file is wired to Alpaca; swapping brokers = replace the import.

Run modes:
    python -m engine.execute              # paper-trade a full session
    python -m engine.execute --dry-run    # scan + pre-trade checks, no orders
    python -m engine.execute --morning-only   # place entries, exit by monitor.py
"""

from __future__ import annotations

import argparse
import os
import sys
import time as _time
from datetime import date, datetime, time, timezone
from pathlib import Path

# Make sure we can import siblings (config_loader, broker, etc.) when invoked
# as `python -m engine.execute` from a scheduled task.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Load .env before importing any broker code below.
import config_loader  # noqa: F401

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker import alpaca
from broker.alpaca import BrokerError
from analysis.scorer import analyze_ticker
from analysis.discover import run_discovery
from risk.config import RISK, auto_select_mode, apply_mode
from risk.checker import check_trade
from risk.exits import (
    entry_allowed_now, apply_safety_floors, trailing_stop_state,
    should_force_close_theta, same_day_exit_allowed, describe_exit_rule,
    ratchet_stop_pct, EOD_PROFIT_LOCK_MINUTES_BEFORE_CLOSE,
    GAP_THRESHOLD_PAST_SL, GAP_RESET_BUFFER, GAP_HARD_FLOOR,
)
from risk.sizer import size_trade
from engine.state import (
    init_db, record_open, list_open, queue_exit, list_queued_exits,
    record_close, mark_settlements_settled, unsettled_cash,
    available_cash_for_new_trade, update_peak, OpenPositionRecord,
    mark_closing, revert_to_open, mark_phantom, list_closing,
    update_sl, record_monitor_check, increment_sl_reset,
)
from engine.news_monitor import (
    check_position_news, describe_signal, news_check_due,
)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Morning session ──────────────────────────────────────────────────────────

def morning_session(dry_run: bool = False) -> None:
    """Place today's entries. Call at or near 09:30 ET."""
    _log("=== MORNING SESSION ===")

    # 1. Broker + mode
    acct = alpaca.get_account()
    _log(f"Account: equity ${acct.equity:.2f}  cash ${acct.cash:.2f}  "
         f"unsettled ${acct.unsettled:.2f}  paper={acct.is_paper}")

    if acct.account_blocked:
        _log("ACCOUNT BLOCKED — halting.")
        return

    mode = auto_select_mode(acct.equity)
    apply_mode(mode)
    RISK["portfolio_size"] = acct.equity  # live value, not file value
    _log(f"Mode: {'MICRO' if acct.equity<1000 else 'STANDARD' if acct.equity<5000 else 'FULL'}  "
         f"(max/trade ${RISK['max_cost_per_trade']}, "
         f"max concurrent {RISK['max_concurrent_positions']})")

    # 2. Settle yesterday's proceeds
    settled = mark_settlements_settled()
    if settled:
        _log(f"Marked {settled} settlement(s) settled")

    # 3. Entry-window check
    clock = alpaca.get_clock()
    if not clock.is_open:
        _log(f"Market closed. Next open: {clock.next_open}. Halting morning session.")
        return
    allowed, reason = entry_allowed_now()
    if not allowed:
        _log(f"Entry window: {reason} — skipping entries.")
        return
    _log(f"Entry window: {reason}")

    # 4. How many slots are free?
    open_positions = list_open()
    slots = RISK["max_concurrent_positions"] - len(open_positions)
    if slots <= 0:
        _log(f"All {RISK['max_concurrent_positions']} concurrent slots in use — no new entries.")
        return
    _log(f"{slots} free slot(s)")

    # 5. Cash available (subtract unsettled to be safe)
    avail = available_cash_for_new_trade(acct.cash)
    _log(f"Available cash (settled only): ${avail:.2f}")
    if avail < RISK["max_cost_per_trade"]:
        _log(f"Below per-trade minimum ${RISK['max_cost_per_trade']} — skipping.")
        return

    # 6. Discover + analyse → candidate list
    _log("Running discovery…")
    disc = run_discovery(top_n=15)
    if disc.empty:
        _log("Discovery returned no tickers.")
        return
    tickers = disc["symbol"].tolist()
    _log(f"Discovery: {', '.join(tickers)}")

    candidates: list[dict] = []
    for sym in tickers:
        try:
            df, news, err, _ = analyze_ticker(sym)
            if err or df is None or df.empty:
                continue
            # Keep only BUY VOL / FLOW BUY with score ≥ min_score_to_trade
            buy = df[df["vol_signal"].isin(["BUY VOL", "FLOW BUY"])]
            buy = buy[buy["score"] >= RISK["min_score_to_trade"]]
            for _, row in buy.iterrows():
                candidates.append(row.to_dict())
        except Exception as e:
            _log(f"  {sym}: {e}")
    candidates.sort(key=lambda r: r["score"], reverse=True)
    _log(f"{len(candidates)} candidates at score ≥ {RISK['min_score_to_trade']}")

    # 7. Place orders one at a time, respecting slots and cash
    placed = 0
    per_symbol_taken: dict[str, int] = {}
    # Import the new 2026-05-06 gates (regime, circuit-breaker, spread,
    # score-cross-validation). Falls back to no-op if unavailable.
    try:
        from tools.paper_trade import _all_new_gates as _new_gates
    except Exception:
        _new_gates = None  # type: ignore
    try:
        from risk import regime as _regime
        _log(f"Market regime: {_regime.describe()}")
    except Exception:
        _regime = None  # type: ignore

    for c in candidates:
        if placed >= slots:
            break
        sym = c["symbol"]
        if per_symbol_taken.get(sym, 0) >= RISK["max_positions_per_ticker"]:
            continue

        entry = float(c.get("entry_price") or c.get("ask") or 0)
        if entry <= 0:
            continue

        # 2026-05-06 gates: regime / circuit-breaker / spread / score-cross-val
        if _new_gates is not None:
            # Build the dict shape paper_trade gates expect
            gate_t = {
                "symbol": sym,
                "option_type": c.get("type"),
                "score": float(c.get("score") or 0),
                "bid": float(c.get("bid") or 0),
                "ask": float(c.get("ask") or 0),
            }
            ok, reason = _new_gates(gate_t, float(RISK["min_score_to_trade"]))
            if not ok:
                _log(f"  SKIP {sym}: {reason}")
                continue
            if reason:
                _log(f"  note {sym}: {reason}")

        # Final pre-trade risk gate — includes live adverse-news check,
        # earnings-day hard block, OI floor.
        check = check_trade(
            symbol=sym, vol_signal=c["vol_signal"], score=float(c["score"]),
            flow_signal=c["flow_signal"], dte=int(c["dte"]),
            otm_pct=abs(float(c["strike"]) / float(c["stock_price"]) - 1),
            max_loss_per_contract=int(round(entry * 100)),
            contracts=1, bid=float(c["bid"]), ask=float(c["ask"]),
            catalyst_summary=c.get("catalyst_summary"),
            open_positions=[{"symbol": p["underlying"],
                             "open_risk": p["entry_price"] * 100 * p["qty"]}
                            for p in open_positions],
            open_interest=int(c.get("open_interest") or 0) or None,
            opt_type=c["type"],
            check_news=True,
        )
        if not check["approved"]:
            _log(f"  SKIP {sym}: {check['reason']}")
            continue

        # Position size — single contract in MICRO, sizer decides in FULL
        contracts = 1
        cost = entry * 100 * contracts
        if cost > avail:
            _log(f"  SKIP {sym}: cost ${cost:.0f} > available ${avail:.2f}")
            continue

        # Layered exit floors
        has_cat = bool(c.get("catalyst_summary"))
        sl_pct, tp_pct = apply_safety_floors(
            score=float(c["score"]), dte=int(c["dte"]),
            has_catalyst_in_window=has_cat, held_overnight=True,
        )
        _log(f"  {sym} {c['type'].upper()} ${c['strike']:.0f} exp {c['expiry']} "
             f"score {c['score']:.1f} → {describe_exit_rule(c['score'], int(c['dte']), has_cat, True)}")

        occ = alpaca.occ_symbol(sym, c["expiry"], c["type"], float(c["strike"]))
        limit_px = round(entry * RISK["limit_price_midpoint_multiplier"], 2)

        if dry_run:
            _log(f"  [DRY] BUY {occ} x{contracts} @ ${limit_px:.2f}")
            placed += 1
            continue

        try:
            order = alpaca.buy_option(occ, contracts, limit_price=limit_px)
            _log(f"  BUY submitted: {order.id} status={order.status}")
        except BrokerError as e:
            _log(f"  BROKER REJECT {sym}: {e}")
            continue

        # Record the open. We log entry_price at the LIMIT (will reconcile
        # to filled_avg_price later if partial/different).
        rec = OpenPositionRecord(
            occ_symbol=occ, underlying=sym, option_type=c["type"],
            strike=float(c["strike"]), expiry=c["expiry"], qty=contracts,
            entry_price=limit_px, entry_date=date.today().isoformat(),
            entry_order_id=order.id, score=float(c["score"]),
            dte_at_entry=int(c["dte"]), vol_signal=c["vol_signal"],
            sl_pct=sl_pct, tp_pct=tp_pct,
        )
        record_open(rec)
        avail -= cost
        placed += 1
        per_symbol_taken[sym] = per_symbol_taken.get(sym, 0) + 1

    _log(f"Morning session done. Placed {placed} entries. Remaining cash ${avail:.2f}.")


# ── Time-window helpers (added 2026-05-01) ──────────────────────────────────

def _in_eod_profit_lock_window() -> bool:
    """True if it's a weekday in ET and we're within the EOD profit-lock
    window (15:55-16:00 ET by default — see EOD_PROFIT_LOCK_MINUTES_BEFORE_CLOSE)."""
    from zoneinfo import ZoneInfo
    now = datetime.now(tz=ZoneInfo("America/New_York"))
    if now.weekday() >= 5:
        return False
    close_t = time(16, 0)
    cutoff_t = time(16 - 0, 0 - 0)  # placeholder, computed below
    # Compute cutoff: close - N minutes
    minutes_before = EOD_PROFIT_LOCK_MINUTES_BEFORE_CLOSE
    cutoff_minutes = (close_t.hour * 60 + close_t.minute) - minutes_before
    cutoff_t = time(cutoff_minutes // 60, cutoff_minutes % 60)
    return cutoff_t <= now.time() < close_t


def _is_first_check_after_session_break(position: dict) -> bool:
    """True if last_monitor_check was more than 6 hours ago (covers
    overnight + weekend gaps). Used by the gap-reset detector."""
    last = position.get("last_monitor_check")
    if not last:
        return False  # no prior check on record — can't tell, don't fire reset
    try:
        last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
    except Exception:
        return False
    delta = datetime.now(tz=timezone.utc) - last_dt
    return delta.total_seconds() > 6 * 3600


# ── Reconcile engine state with Alpaca truth ─────────────────────────────────

def reconcile_with_broker() -> None:
    """
    Pull engine_state.db into agreement with the Alpaca account.

    Three failure modes this fixes:
      1. PHANTOM open rows — paper_trade.py records on 'submitted' but the
         limit BUY can EXPIRE without filling. Result: row exists, no
         broker position. Mark phantom so monitor_tick stops processing it.
      2. UNCONFIRMED close orders — _execute_exit submits a SELL but the
         limit might not fill. Old code optimistically flipped status to
         'closed'; new code marks 'closing' and we finalize here once
         Alpaca confirms FILLED, or revert to 'open' on EXPIRED/CANCELED.
      3. UNTRACKED broker positions — a position exists on Alpaca but has
         no engine_state row. (Caused by past code paths that submitted
         without recording, or a manual buy from override_buy.) Logged so
         tools/backfill_positions can pick it up.
    """
    try:
        live_positions = {p.symbol: p for p in alpaca.get_positions()}
    except Exception as e:
        _log(f"  reconcile: skip — alpaca.get_positions failed: {e}")
        return

    # 1. Phantom check on 'open' rows
    open_rows = [r for r in list_open() if r["status"] == "open"]
    for r in open_rows:
        if r["occ_symbol"] not in live_positions:
            mark_phantom(r["id"])
            _log(f"  reconcile: phantom {r['occ_symbol']} (id={r['id']}) — "
                 f"engine has it open but Alpaca doesn't")

    # 2. Confirm-or-revert 'closing' rows
    closing = list_closing()
    if closing:
        try:
            tc = alpaca._trading_client()
        except Exception as e:
            _log(f"  reconcile: trading client unavailable: {e}")
            tc = None
        for r in closing:
            oid = r.get("exit_order_id")
            if not oid or tc is None:
                continue
            try:
                o = tc.get_order_by_id(oid)
            except Exception as e:
                _log(f"  reconcile: order lookup err {r['occ_symbol']}: {e}")
                continue
            status = str(o.status)
            if "FILLED" in status:
                fill_px = float(o.filled_avg_price or o.limit_price or 0)
                fill_date = (o.filled_at.date().isoformat()
                             if o.filled_at else date.today().isoformat())
                record_close(r["id"], fill_px, fill_date, oid,
                             r.get("exit_reason") or "exit filled")
                _log(f"  reconcile: ✅ closed {r['occ_symbol']} @ ${fill_px:.2f}")
                try:
                    from tools.notify import send
                    entry = float(r["entry_price"])
                    pnl_dollars = (fill_px - entry) * 100 * int(r["qty"])
                    pnl_pct = (fill_px / entry - 1) * 100 if entry else 0
                    send(
                        "EXIT",
                        f"{r['occ_symbol']} filled @ ${fill_px:.2f}",
                        f"entry=${entry:.2f} pl=${pnl_dollars:+.0f} ({pnl_pct:+.1f}%) "
                        f"reason={r.get('exit_reason','')}",
                    )
                except Exception:
                    pass
            elif any(t in status for t in ("EXPIRED", "CANCELED", "REJECTED")):
                revert_to_open(r["id"])
                _log(f"  reconcile: ↩ {r['occ_symbol']} close order {status} — "
                     f"reverting to open (next trigger will re-fire)")
            # else: still PENDING/ACCEPTED/PARTIAL — leave alone

    # 3. Untracked positions
    tracked = {r["occ_symbol"] for r in list_open()}
    for sym, p in live_positions.items():
        if sym not in tracked:
            _log(f"  reconcile: untracked broker position {sym} qty={p.qty} "
                 f"— run `python -m tools.backfill_positions` to manage")


# ── Intraday monitor ─────────────────────────────────────────────────────────

def monitor_tick() -> None:
    """
    Run once per N seconds during market hours. Steps:
      0. reconcile_with_broker — sync engine_state with Alpaca truth
      1. Flush queued exits whose entry_date is past T+1 (fire at next-session
         open, not at tomorrow's 15:45 EOD — saves a full session of bleed)
      2. For each remaining 'open' position:
         - Refresh quote, update trailing peak
         - Check SL / trailing / theta guard
         - If triggered: queue_exit or _execute_exit per cash-account rule
    """
    reconcile_with_broker()

    # Step 1: flush queued exits eligible to fire (entry_date < today)
    today = date.today()
    for p in list_queued_exits():
        try:
            entry_date = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if entry_date < today:
            _log(f"  flushing queued exit: {p['occ_symbol']} ({p['exit_reason']})")
            _execute_exit(p, p["exit_reason"] or "queued exit", urgent=False)

    open_positions = [p for p in list_open() if p["status"] == "open"]
    if not open_positions:
        return

    # Determine if we're inside the EOD profit-lock window (15:55-16:00 ET)
    eod_lock_active = _in_eod_profit_lock_window()
    today_iso = date.today().isoformat()

    for p in open_positions:
        try:
            q = alpaca.get_quote(p["occ_symbol"])
        except BrokerError as e:
            _log(f"  quote err {p['occ_symbol']}: {e}")
            continue
        mark = q.mid or q.bid
        if mark <= 0:
            continue
        entry = float(p["entry_price"])
        pnl_pct = (mark / entry) - 1

        # Update trailing peak
        update_peak(p["id"], mark)
        peak = max(float(p["trailing_peak"] or mark), mark)
        peak_pnl_pct = (peak / entry) - 1

        # ── Ratchet ladder: tightens SL upward as peak crosses tiers.
        # Never loosens (update_sl with only_tighter=True).
        ratchet = ratchet_stop_pct(peak_pnl_pct)
        if ratchet is not None:
            current_sl = float(p["sl_pct"]) if p["sl_pct"] is not None else -1.0
            if ratchet > current_sl:
                if update_sl(p["id"], ratchet, only_tighter=True):
                    _log(f"  {p['occ_symbol']}: ratchet promoted SL "
                         f"{current_sl*100:+.1f}% -> {ratchet*100:+.1f}% "
                         f"(peak {peak_pnl_pct*100:+.1f}%)")
                    p["sl_pct"] = ratchet  # use new SL in checks below

        # ── Overnight-gap detector: if pnl is significantly past SL AND the
        # last monitor check was on a previous session, reset SL once rather
        # than panic-sell at the gap-down spike. Hard floor at GAP_HARD_FLOOR.
        sl_pct = float(p["sl_pct"]) if p["sl_pct"] is not None else -0.25
        breach = pnl_pct <= sl_pct + GAP_THRESHOLD_PAST_SL  # both negative
        gap_eligible = breach and _is_first_check_after_session_break(p)
        if gap_eligible and pnl_pct > GAP_HARD_FLOOR:
            already_reset_today = (p.get("sl_reset_date") == today_iso
                                    and (p.get("sl_resets_today") or 0) >= 1)
            if not already_reset_today:
                new_sl = max(pnl_pct + GAP_RESET_BUFFER, GAP_HARD_FLOOR)
                update_sl(p["id"], new_sl, only_tighter=False)
                increment_sl_reset(p["id"], today_iso)
                _log(f"  {p['occ_symbol']}: GAP-RESET SL {sl_pct*100:+.1f}% "
                     f"-> {new_sl*100:+.1f}% (gap-open at pnl {pnl_pct*100:+.1f}%, "
                     f"giving {abs(GAP_RESET_BUFFER)*100:.0f}% buffer)")
                record_monitor_check(p["id"], datetime.now(tz=timezone.utc).isoformat())
                continue  # let it settle this tick; normal rules resume next tick

        # ── EOD profit-lock: 5 min before close, force-flatten any winner.
        # Captures intraday gains rather than holding overnight where theta
        # + gap risk can erase the profit. Losers ride to next session
        # under existing rules (gap-reset will protect against bad opens).
        if eod_lock_active and pnl_pct > 0:
            _handle_exit_trigger(p, f"EOD profit-lock at {pnl_pct*100:+.1f}% "
                                    f"({EOD_PROFIT_LOCK_MINUTES_BEFORE_CLOSE}min before close)")
            record_monitor_check(p["id"], datetime.now(tz=timezone.utc).isoformat())
            continue

        # SL check
        if p["sl_pct"] is not None and pnl_pct <= float(p["sl_pct"]):
            _handle_exit_trigger(p, f"SL {p['sl_pct']*100:+.0f}% hit at {pnl_pct*100:+.1f}%")
            record_monitor_check(p["id"], datetime.now(tz=timezone.utc).isoformat())
            continue

        # Trailing stop (kept as a safety net; ratchet usually fires first)
        tr = trailing_stop_state(entry, peak, mark)
        if tr["armed"] and tr["triggered"]:
            _handle_exit_trigger(p, f"trailing stop fired at {pnl_pct*100:+.1f}% "
                                    f"(locked +{tr['locked_in_pct']*100:.1f}%)")
            record_monitor_check(p["id"], datetime.now(tz=timezone.utc).isoformat())
            continue

        # Theta guard (only at close, but cheap to check)
        if should_force_close_theta(pnl_pct, int(p["dte_at_entry"])):
            _handle_exit_trigger(p, f"theta guard: {pnl_pct*100:.1f}% at low DTE")
            record_monitor_check(p["id"], datetime.now(tz=timezone.utc).isoformat())
            continue

        # ── Opposing-divergence exit (added 2026-05-06) ───────────────────────
        # If sentinel now shows a STRONG divergence in the OPPOSITE direction
        # of the position, exit rather than waiting for SL/floor. RIOT 17P
        # held 5/1-5/5 finally hit -50% hard floor — sentinel had been
        # showing positive RIOT divergence since 5/4. Earlier exit on the
        # flip would have saved most of the $106 loss.
        try:
            from sentinel_bridge import get_divergence
            div = get_divergence(p["underlying"], max_age_hours=12)
        except Exception:
            div = None
        if div:
            direction = (div.get("direction") or "").lower()
            div_score = float(div.get("divergence_score") or 0)
            opt_type = (p["option_type"] or "").lower()
            # Strong threshold: only fire on a confident reversal signal
            STRONG = 1.0
            opposing = False
            if div_score >= STRONG:
                if direction == "bullish_divergence" and opt_type == "put":
                    opposing = True
                elif direction == "bearish_divergence" and opt_type == "call":
                    opposing = True
            # Only trigger if position has been held long enough that
            # sentiment had a chance to change since entry. Same-session
            # whipsaw protection: must be open >2 hours.
            if opposing:
                try:
                    entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                    held_full_session = entry_dt < date.today()
                except Exception:
                    held_full_session = False
                if held_full_session:
                    _handle_exit_trigger(
                        p, f"opposing-divergence exit: sentinel says "
                           f"{direction} (score={div_score:.2f}) on "
                           f"{p['underlying']} but we hold {opt_type}"
                    )
                    record_monitor_check(
                        p["id"], datetime.now(tz=timezone.utc).isoformat()
                    )
                    continue

        record_monitor_check(p["id"], datetime.now(tz=timezone.utc).isoformat())


def news_tick() -> None:
    """
    Run every `news_check_interval_seconds` during the session. For each
    open position, pull fresh never-seen articles for the underlying,
    classify them relative to the position's direction, and fire an exit
    trigger if material adverse news hit.

    Runs independently of price-driven monitor_tick() because news shocks
    can outpace the 10-15 sec quote loop — e.g. a halted ticker still lets
    news in while the quote goes stale.
    """
    if not RISK.get("news_exit_enabled", True):
        return
    # Filter to status='open' only — positions already in 'closing' have a
    # SELL submitted (or queued); a duplicate _execute_exit would create a
    # second SELL order. monitor_tick filters the same way.
    open_positions = [p for p in list_open() if p["status"] == "open"]
    if not open_positions:
        return

    _log(f"News check: {len(open_positions)} position(s)")
    for p in open_positions:
        try:
            sig = check_position_news(p)
        except Exception as e:
            _log(f"  news err {p['underlying']}: {e}")
            continue
        if sig.articles:
            _log(f"  {p['underlying']}: {describe_signal(sig, p['option_type'])}")
        if sig.is_adverse:
            top = sig.adverse_articles[0]
            title = (top.get("title") or "")[:100]
            _handle_exit_trigger(
                p,
                f"adverse news ({len(sig.adverse_articles)} art, "
                f"agg={sig.sentiment_score:+.2f}): \"{title}\"",
            )


def _handle_exit_trigger(position: dict, reason: str) -> None:
    entry_date = datetime.strptime(position["entry_date"], "%Y-%m-%d").date()
    allowed, why = same_day_exit_allowed(entry_date)
    if not allowed:
        _log(f"  {position['occ_symbol']}: {reason} — QUEUED ({why})")
        queue_exit(position["id"], reason)
    else:
        _log(f"  {position['occ_symbol']}: {reason} — EXITING NOW")
        _execute_exit(position, reason, urgent=True)


def _execute_exit(position: dict, reason: str, urgent: bool = False) -> None:
    """
    Submit the closing SELL. Marks the row 'closing' (not 'closed') and
    stores exit_order_id so reconcile_with_broker can finalize once Alpaca
    confirms the fill, or revert to 'open' if the limit order expires.
    """
    try:
        q = alpaca.get_quote(position["occ_symbol"])
    except BrokerError as e:
        _log(f"  quote fail for exit {position['occ_symbol']}: {e}")
        return
    # Exit at mid (or bid-side for urgent) — avoid paying the spread twice
    px = round(q.bid if urgent else q.mid, 2)
    if px <= 0:
        px = round(q.mid or q.ask, 2)
    try:
        o = alpaca.sell_option(position["occ_symbol"], int(position["qty"]),
                               limit_price=px)
    except BrokerError as e:
        _log(f"  SELL REJECT {position['occ_symbol']}: {e}")
        return
    mark_closing(position["id"], o.id, reason)
    _log(f"  SELL submitted {o.id} @ ${px:.2f} — {reason} (awaiting fill)")
    try:
        from tools.notify import send
        send(
            "INFO",
            f"close submitted {position['occ_symbol']} @ ${px:.2f}",
            f"qty={position['qty']} reason={reason}",
        )
    except Exception:
        pass


# ── End-of-day ───────────────────────────────────────────────────────────────

def eod_session() -> None:
    """At ~15:45 ET: fire any queued exits whose entry_date is before today."""
    _log("=== EOD SESSION ===")
    reconcile_with_broker()
    today = date.today()
    queued = list_queued_exits()
    for p in queued:
        entry_date = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
        if entry_date < today:
            _execute_exit(p, p["exit_reason"] or "queued exit", urgent=False)
        else:
            _log(f"  {p['occ_symbol']}: queued today, wait until tomorrow's session")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="scan + pre-trade checks only, no orders")
    ap.add_argument("--morning-only", action="store_true",
                    help="run entries and exit (skip monitor loop)")
    ap.add_argument("--monitor-seconds", type=int, default=0,
                    help="if >0, run monitor_tick() every N seconds until close")
    ap.add_argument("--eod", action="store_true",
                    help="run EOD queued-exit firing only")
    ap.add_argument("--news-only", action="store_true",
                    help="run a single news check over open positions and exit "
                         "(for cron / manual spot-check)")
    ap.add_argument("--monitor-once", action="store_true",
                    help="run a single monitor_tick (SL/trailing/theta scan) and exit "
                         "(intended for cron-style scheduling every N minutes)")
    ap.add_argument("--monitor-only", action="store_true",
                    help="skip morning_session — go straight to the monitor loop "
                         "using --monitor-seconds N. Used by the daemon launcher "
                         "since MorningAutoRun handles entries on its own schedule.")
    args = ap.parse_args()

    init_db()

    if args.eod:
        eod_session()
        return

    if args.news_only:
        news_tick()
        return

    if args.monitor_once:
        monitor_tick()
        return

    # --monitor-only: skip morning_session entirely. Lets the daemon launch
    # ahead of MorningAutoRun without re-doing the morning's entry work.
    if args.monitor_only:
        if args.monitor_seconds <= 0:
            print("--monitor-only requires --monitor-seconds N (e.g., 15)")
            return 2
        # fall through to the monitor loop below
    else:
        morning_session(dry_run=args.dry_run)
        if args.morning_only or args.dry_run:
            return

    if args.monitor_seconds > 0:
        news_interval = int(RISK.get("news_check_interval_seconds", 600))
        _log(f"Monitoring every {args.monitor_seconds}s, news every "
             f"{news_interval}s, until market close…")
        last_news_run: datetime | None = None
        # Fire one news check immediately so positions opened this session
        # get scanned without waiting out the first interval.
        try:
            news_tick()
            last_news_run = datetime.now()
        except Exception as e:
            _log(f"initial news tick error (non-fatal): {e}")

        # ── Clock-check loop with transient-tolerance ─────────────────────────
        # Bug observed 2026-05-04: daemon launched at 09:30:01 ET, hit
        # is_open=False at 09:30:26 (transient during opening-bell second),
        # broke loop, ran eod_session, exited. Monitor never ran the rest
        # of the session — RIOT queued exit submitted at one stale price
        # then expired unfilled, SOUN ratchet never promoted from -12%.
        #
        # Fix: only break if (a) it's past the explicit close time we know
        # AND (b) is_open=False persists across multiple consecutive checks.
        # Transient pre-open / opening-bell / mid-day data hiccups don't
        # kill the daemon.
        from zoneinfo import ZoneInfo as _ZoneInfo
        _NY = _ZoneInfo("America/New_York")
        consecutive_closed_ticks = 0
        CLOSED_CONFIRM_TICKS = 4  # must see is_open=False 4 ticks in a row
        EARLIEST_CLOSE_HOUR_ET = 16  # don't trust closed reports before 16:00 ET

        while True:
            try:
                clock = alpaca.get_clock()
                now_et = datetime.now(tz=_NY)
                is_after_close = (now_et.hour >= EARLIEST_CLOSE_HOUR_ET
                                  or now_et.weekday() >= 5)
                if not clock.is_open:
                    consecutive_closed_ticks += 1
                    if is_after_close and consecutive_closed_ticks >= CLOSED_CONFIRM_TICKS:
                        _log(f"Market closed (confirmed across "
                             f"{consecutive_closed_ticks} ticks, ET={now_et.strftime('%H:%M')}); "
                             f"leaving monitor loop.")
                        break
                    # Transient false (pre-open, opening-bell second, mid-day
                    # data glitch) — keep monitoring.
                    if consecutive_closed_ticks == 1:
                        _log(f"clock reports closed at ET={now_et.strftime('%H:%M:%S')}; "
                             f"continuing (transient or pre-open)")
                else:
                    consecutive_closed_ticks = 0  # reset on any open report
                    monitor_tick()
                    if news_check_due(last_news_run):
                        news_tick()
                        last_news_run = datetime.now()
            except Exception as e:
                _log(f"monitor error (non-fatal): {e}")
            _time.sleep(args.monitor_seconds)
        eod_session()


if __name__ == "__main__":
    main()
