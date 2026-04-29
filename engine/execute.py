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
from datetime import date, datetime, time
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
)
from risk.sizer import size_trade
from engine.state import (
    init_db, record_open, list_open, queue_exit, list_queued_exits,
    record_close, mark_settlements_settled, unsettled_cash,
    available_cash_for_new_trade, update_peak, OpenPositionRecord,
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
    for c in candidates:
        if placed >= slots:
            break
        sym = c["symbol"]
        if per_symbol_taken.get(sym, 0) >= RISK["max_positions_per_ticker"]:
            continue

        entry = float(c.get("entry_price") or c.get("ask") or 0)
        if entry <= 0:
            continue

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


# ── Intraday monitor ─────────────────────────────────────────────────────────

def monitor_tick() -> None:
    """
    Run once per N seconds during market hours. For each open position:
      - Refresh quote
      - Update trailing peak
      - Check SL / trailing / theta guard
      - If triggered: queue_exit (cash account rule forces next-session fill)
    """
    open_positions = list_open()
    if not open_positions:
        return

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

        # SL check
        if p["sl_pct"] is not None and pnl_pct <= float(p["sl_pct"]):
            _handle_exit_trigger(p, f"SL {p['sl_pct']*100:+.0f}% hit at {pnl_pct*100:+.1f}%")
            continue

        # Trailing stop
        tr = trailing_stop_state(entry, peak, mark)
        if tr["armed"] and tr["triggered"]:
            _handle_exit_trigger(p, f"trailing stop fired at {pnl_pct*100:+.1f}% "
                                    f"(locked +{tr['locked_in_pct']*100:.1f}%)")
            continue

        # Theta guard (only at close, but cheap to check)
        if should_force_close_theta(pnl_pct, int(p["dte_at_entry"])):
            _handle_exit_trigger(p, f"theta guard: {pnl_pct*100:.1f}% at low DTE")


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
    open_positions = list_open()
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
    """Place the closing sell order."""
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
    record_close(position["id"], px, date.today().isoformat(), o.id, reason)
    _log(f"  SELL submitted {o.id} @ ${px:.2f} — {reason}")


# ── End-of-day ───────────────────────────────────────────────────────────────

def eod_session() -> None:
    """At ~15:45 ET: fire any queued exits whose entry_date is before today."""
    _log("=== EOD SESSION ===")
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

        while True:
            try:
                clock = alpaca.get_clock()
                if not clock.is_open:
                    _log("Market closed; leaving monitor loop.")
                    break
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
