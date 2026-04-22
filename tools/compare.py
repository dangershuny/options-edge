#!/usr/bin/env python3
"""
Comparison Tool — measures how snapshot recommendations performed.

Usage (run after market close the next day):
    python tools/compare.py                   # uses today's date snapshot
    python tools/compare.py 2026-04-20        # specify snapshot date

Fetches today's closing prices for each option, compares to entry,
computes P&L per contract, updates the snapshot JSON, and prints a
performance report.
"""

import sys
import os
import json
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "snapshots",
)


def _get_option_close(symbol: str, expiry: str, strike: float, opt_type: str) -> float | None:
    """Fetch last close price of a specific option contract."""
    try:
        ticker = yf.Ticker(symbol)
        chain = ticker.option_chain(expiry)
        df = chain.calls if opt_type.lower() == "call" else chain.puts
        match = df[df["strike"] == strike]
        if match.empty:
            # Try nearest strike
            match = df.iloc[(df["strike"] - strike).abs().argsort()[:1]]
        if match.empty:
            return None
        # Use lastPrice if bid/ask not available
        row = match.iloc[0]
        bid = float(row.get("bid") or 0)
        ask = float(row.get("ask") or 0)
        last = float(row.get("lastPrice") or 0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2, 2)
        return round(last, 2) if last > 0 else None
    except Exception:
        return None


def run_comparison(snap_date: str | None = None) -> None:
    snap_date = snap_date or date.today().isoformat()
    snap_path = os.path.join(SNAPSHOT_DIR, f"{snap_date}.json")

    if not os.path.exists(snap_path):
        print(f"No snapshot found for {snap_date} at {snap_path}")
        return

    with open(snap_path, encoding="utf-8") as f:
        snapshot = json.load(f)

    today = date.today().isoformat()
    print(f"\n{'═'*72}")
    print(f"  OPTIONS EDGE — Performance Comparison")
    print(f"  Snapshot date : {snap_date}")
    print(f"  Comparison    : {today} (today's close)")
    print(f"{'═'*72}\n")

    trades = snapshot["trades"]
    total_cost   = 0.0
    total_pnl    = 0.0
    winners      = 0
    losers       = 0
    no_data      = 0

    rows = []
    for trade in trades:
        symbol   = trade["symbol"]
        expiry   = trade["expiry"]
        strike   = trade["strike"]
        opt_type = trade["option_type"]
        sig      = trade["vol_signal"]

        entry_px = trade.get("entry_price_mid") or trade.get("ask") or 0

        # Skip trades with no entry price — we can't compute meaningful P&L
        if entry_px == 0:
            no_data += 1
            trade["outcome"] = "NO ENTRY"
            rows.append({**trade, "_close": None, "_pnl": None})
            continue

        print(f"  Fetching {symbol} ${strike:.0f} {opt_type.upper()} {expiry}…", end=" ", flush=True)
        close_px = _get_option_close(symbol, expiry, strike, opt_type)

        if close_px is None:
            print("no data")
            no_data += 1
            trade["close_price_next_day"] = None
            trade["pnl_per_contract"] = None
            trade["outcome"] = "NO DATA"
            rows.append({**trade, "_close": None, "_pnl": None})
            continue

        print(f"${close_px:.2f}")
        trade["close_price_next_day"] = close_px

        # Long option P&L: (exit - entry) × 100
        pnl  = round((close_px - entry_px) * 100, 2)
        cost = round(entry_px * 100, 2)

        trade["pnl_per_contract"] = pnl
        if pnl > 0:
            trade["outcome"] = "WIN"
            winners += 1
        elif pnl < 0:
            trade["outcome"] = "LOSS"
            losers += 1
        else:
            trade["outcome"] = "FLAT"

        total_pnl  += pnl
        total_cost += cost
        rows.append({**trade, "_close": close_px, "_pnl": pnl})

    # ── Save updated snapshot ─────────────────────────────────────────────────
    snapshot["comparison_date"] = today
    snapshot["total_pnl"]       = round(total_pnl, 2)
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    # ── Print report ──────────────────────────────────────────────────────────
    print(f"\n{'═'*72}")
    print(f"  PERFORMANCE REPORT")
    print(f"{'═'*72}")
    print(f"  {'Ticker':<6} {'Type':<5} {'Strike':>8} {'Expiry':<12} {'Sig':<9} "
          f"{'Entry':>7} {'Close':>7} {'P&L':>8} {'Score':>6} {'Outcome'}")
    print(f"  {'─'*68}")

    for r in sorted(rows, key=lambda x: x.get("_pnl") or 0, reverse=True):
        entry = r.get("entry_price_mid") or r.get("ask") or 0
        close = r.get("_close")
        pnl   = r.get("_pnl")
        sig   = r["vol_signal"][:7]
        outcome = r.get("outcome", "—")
        outcome_icon = "✅" if outcome == "WIN" else ("❌" if outcome == "LOSS" else ("⚠" if outcome == "NO ENTRY" else "—"))
        close_str  = f"${close:.2f}" if close is not None else "N/A"
        entry_str  = f"${entry:.2f}" if entry else "—"
        pnl_str    = f"${pnl:+.0f}" if pnl is not None else "—"

        print(f"  {r['symbol']:<6} {r['option_type'].upper():<5} "
              f"${r['strike']:>7.0f} {r['expiry']:<12} {sig:<9} "
              f"{entry_str:>7} {close_str:>7} {pnl_str:>8} "
              f"{r['score']:>6.1f} {outcome_icon} {outcome}")

    print(f"\n  {'─'*68}")
    tradeable = winners + losers
    total_trades = tradeable + no_data
    print(f"  Total tracked: {total_trades}  |  Wins: {winners}  |  Losses: {losers}  |  Skipped (no price): {no_data}")
    if total_cost > 0:
        roi = total_pnl / total_cost * 100
        print(f"  Total P&L    : ${total_pnl:+.2f}  |  Total cost: ${total_cost:.0f}  |  ROI: {roi:+.1f}%")
    else:
        print(f"  Total P&L    : ${total_pnl:+.2f}")
    print(f"\n  Snapshot updated → {snap_path}")
    print(f"{'═'*72}\n")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_comparison(arg)
