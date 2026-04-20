#!/usr/bin/env python3
"""
Snapshot Tool — records trade recommendations at a point in time.

Usage:
    python tools/snapshot.py                  # scan universe, take top 15
    python tools/snapshot.py AAPL TSLA NVDA  # scan specific tickers

Saves results to snapshots/YYYY-MM-DD.json.
Print a human-readable catalogue of every recommended contract,
including last-close option price — ready for next-day comparison.
"""

import sys
import os
import json
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from data.universe import UNIVERSE
from data.market import get_current_price, get_options_chain, check_market_cap
from analysis.vol import calculate_rv, iv_rv_signal
from analysis.scorer import analyze_ticker
from analysis.discover import run_discovery
from risk.config import RISK

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "snapshots",
)

SEPARATOR = "─" * 72


def _fmt_date() -> str:
    return date.today().isoformat()


def run_and_save(tickers: list[str] | None = None) -> None:
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap_date = _fmt_date()
    snap_path = os.path.join(SNAPSHOT_DIR, f"{snap_date}.json")

    print(f"\n{'═'*72}")
    print(f"  OPTIONS EDGE — Trade Recommendation Snapshot")
    print(f"  Date      : {snap_date}  (last market close: Fri Apr 18 2026)")
    print(f"  Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*72}\n")

    # ── Step 1: Discover candidates if no tickers given ────────────────────────
    if tickers is None:
        print("  Running discovery scan (~60s)…")
        disc = run_discovery(top_n=15)
        if disc.empty:
            print("  Discovery returned no results.")
            return
        tickers = disc["symbol"].tolist()
        print(f"  Discovery found: {', '.join(tickers)}\n")
    else:
        print(f"  Scanning: {', '.join(tickers)}\n")

    # ── Step 2: Full analysis per ticker ──────────────────────────────────────
    records = []
    errors  = []

    for symbol in tickers:
        print(f"  Analysing {symbol}…", end=" ", flush=True)
        try:
            df, news, err, earnings_edge = analyze_ticker(symbol)
            if err:
                errors.append(f"{symbol}: {err}")
                print(f"skipped ({err})")
                continue
            print(f"{len(df)} contracts")
            for _, row in df.iterrows():
                records.append(row.to_dict())
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
            print(f"error ({exc})")

    if not records:
        print("\n  No actionable contracts found.")
        return

    df_all = pd.DataFrame(records).sort_values("score", ascending=False)

    # ── Step 3: Print catalogue ────────────────────────────────────────────────
    _print_catalogue(df_all, snap_date)

    # ── Step 4: Save snapshot ─────────────────────────────────────────────────
    snapshot = {
        "snapshot_date": snap_date,
        "generated_at":  datetime.now().isoformat(),
        "last_close_date": "2026-04-18",  # last trading day
        "risk_settings": {k: RISK[k] for k in (
            "portfolio_size", "max_cost_per_trade", "min_score_to_trade"
        )},
        "trades": [],
    }

    # Only log BUY VOL — no naked/spread selling
    for _, row in df_all[df_all["vol_signal"] == "BUY VOL"].iterrows():
        trade = {
            "symbol":           row["symbol"],
            "company_name":     row["company_name"],
            "option_type":      row["type"],
            "strike":           row["strike"],
            "expiry":           row["expiry"],
            "dte":              row["dte"],
            "action":           row["action"],
            "vol_signal":       "BUY VOL",
            "stock_price_at_snap": row["stock_price"],
            "bid":              row["bid"],
            "ask":              row["ask"],
            "entry_price_mid":  row.get("entry_price"),
            "iv_pct":           row["iv_pct"],
            "rv_pct":           row["rv_pct"],
            "iv_rv_spread":     row["iv_rv_spread"],
            "score":            row["score"],
            "flow_signal":      row["flow_signal"],
            "gex_signal":       row.get("gex_signal"),
            "skew_signal":      row.get("skew_signal"),
            "iv_rank_label":    row.get("iv_rank_label"),
            "max_loss_per_contract": row.get("max_loss_per_contract"),
            "suggested_contracts":   row.get("suggested_contracts"),
            # Filled in tomorrow
            "close_price_next_day": None,
            "pnl_per_contract":     None,
            "outcome":              None,
        }
        snapshot["trades"].append(trade)

    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    print(f"\n  Snapshot saved → {snap_path}")
    if errors:
        print(f"\n  Skipped tickers:")
        for e in errors:
            print(f"    • {e}")
    print()


def _print_catalogue(df: pd.DataFrame, snap_date: str) -> None:
    # Only show buy-side — no naked/spread selling
    df = df[df["vol_signal"] == "BUY VOL"].copy()
    if df.empty:
        print("\n  No BUY VOL signals found in this scan.")
        return

    print(f"\n{'═'*72}")
    print(f"  CATALOGUE — {len(df)} BUY contracts across {df['symbol'].nunique()} tickers")
    print(f"  All prices as of last close ({snap_date})")
    print(f"  Buying 1 contract = 100 shares exposure  |  Max loss = premium paid")
    print(f"{'═'*72}")

    for symbol in df["symbol"].unique():
        tkr = df[df["symbol"] == symbol]
        name = tkr["company_name"].iloc[0]
        px   = tkr["stock_price"].iloc[0]
        print(f"\n  {'─'*68}")
        print(f"  {symbol}  —  {name}  —  stock ${px:.2f}")
        print(f"  {'─'*68}")

        for _, row in tkr.iterrows():
            sig      = row["vol_signal"]
            sig_icon = "🟢 BUY" if sig == "BUY VOL" else ("🔴 SPREAD" if sig == "SELL VOL" else "🟡 WATCH")
            opt      = row["type"].upper()
            strike   = row["strike"]
            expiry   = row["expiry"]
            dte      = row["dte"]
            score    = row["score"]
            flow     = row["flow_signal"]
            iv_rv    = row["iv_rv_spread"]

            entry_px   = row.get("entry_price") or row.get("ask") or 0
            max_loss   = round(entry_px * 100, 2)
            cost_str   = f"${entry_px:.2f}/contract  (max loss ${max_loss:.0f})"
            action_str = f"BUY 1 ${strike:.0f} {opt} @ ~${entry_px:.2f}"

            print(f"\n    {sig_icon}  {opt}  ${strike:.0f}  exp {expiry} ({dte}d)")
            print(f"    Score: {score}  |  Flow: {flow}  |  IV−RV: {iv_rv:+.1f}%")
            print(f"    Action: {action_str}")
            print(f"    Cost  : {cost_str}")

    print(f"\n{'═'*72}")
    print("  ASSUMPTIONS FOR TOMORROW'S COMPARISON:")
    print("  • Buying 1 contract of each signal at the mid-price shown")
    print("  • Max loss per contract = premium paid (no naked exposure)")
    print("  • P&L = (tomorrow close mid-price − today entry) × 100 per contract")
    print(f"{'═'*72}\n")


if __name__ == "__main__":
    tickers_arg = sys.argv[1:] if len(sys.argv) > 1 else None
    run_and_save(tickers_arg)
