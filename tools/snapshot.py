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
from risk.config import RISK, auto_select_mode, apply_mode, MICRO_MODE, STANDARD_MODE, FULL_MODE
from sentinel_bridge import ensure_sentinel_running, sentinel_last_error, scan_ticker as sentinel_scan_ticker
from risk.exits import calibration_info

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "snapshots",
)

SEPARATOR = "─" * 72


def _fmt_date() -> str:
    return date.today().isoformat()


def run_and_save(tickers: list[str] | None = None,
                 mode_override: str | None = None,
                 suffix: str | None = None) -> None:
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap_date = _fmt_date()
    fname = f"{snap_date}{('_' + suffix) if suffix else ''}.json"
    snap_path = os.path.join(SNAPSHOT_DIR, fname)

    print(f"\n{'═'*72}")
    print(f"  OPTIONS EDGE — Trade Recommendation Snapshot")
    print(f"  Date      : {snap_date}  (last market close: Fri Apr 18 2026)")
    print(f"  Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Mode: explicit override (for A/B comparisons) OR auto-select from portfolio.
    if mode_override:
        mode_map = {"MICRO": MICRO_MODE, "STANDARD": STANDARD_MODE, "FULL": FULL_MODE}
        mode = mode_map[mode_override.upper()]
        mode_name = mode_override.upper()
    else:
        mode = auto_select_mode(RISK["portfolio_size"])
        mode_name = ("MICRO" if RISK["portfolio_size"] < 1_000 else
                     "STANDARD" if RISK["portfolio_size"] < 5_000 else "FULL")
    apply_mode(mode)
    print(f"  Mode      : {mode_name}  "
          f"(portfolio ${RISK['portfolio_size']:,}, "
          f"max/trade ${RISK['max_cost_per_trade']}, "
          f"max premium ${RISK['max_contract_premium']:.2f}, "
          f"max underlying ${RISK['max_underlying_price']})")

    ci = calibration_info()
    print(f"  Exits cal : {ci['source']}  (n={ci['n_contracts']}, "
          f"last {ci['last_updated']})")
    print(f"{'═'*72}\n")

    # Ensure news sentinel is running — auto-launch if needed. Silent on success.
    if not ensure_sentinel_running():
        err = sentinel_last_error() or "server unreachable"
        print(f"  ⚠ News sentiment OFFLINE — scoring without it ({err})")
        print(f"    Sentiment can move scores by ±15. Start manually to include it.\n")

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
        # Refresh sentiment before analysis; failure is non-fatal (scorer
        # simply gets no sentiment_delta for this ticker).
        try:
            sentinel_scan_ticker(symbol)
        except Exception:
            pass
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

    # Log BUY VOL and FLOW BUY — no naked/spread selling
    for _, row in df_all[df_all["vol_signal"].isin(["BUY VOL", "FLOW BUY"])].iterrows():
        trade = {
            "symbol":           row["symbol"],
            "company_name":     row["company_name"],
            "option_type":      row["type"],
            "strike":           row["strike"],
            "expiry":           row["expiry"],
            "dte":              row["dte"],
            "action":           row["action"],
            "vol_signal":       row["vol_signal"],
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
            # New signal feeds wired into the scorer
            "sentiment_delta":  row.get("sentiment_delta"),
            "insider_delta":    row.get("insider_delta"),
            "short_delta":      row.get("short_delta"),
            "blocks_delta":     row.get("blocks_delta"),
            "catalyst_delta":   row.get("catalyst_delta"),
            "pin_delta":        row.get("pin_delta"),
            "insider_signal":   row.get("insider_signal"),
            "short_signal":     row.get("short_signal"),
            "blocks_signal":    row.get("blocks_signal"),
            "catalyst_summary": row.get("catalyst_summary"),
            "pin_risk":         row.get("pin_risk"),
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

    # ── Step 5: Auto-recalibrate exit rules from rolling snapshot history ────
    # Runs in a subprocess so a failure (yfinance hiccup, rate-limit) can't
    # corrupt the snapshot that was just saved. Silent unless tiers changed.
    try:
        _auto_recalibrate()
    except Exception as e:
        print(f"  ⚠ Exit recalibration failed (non-fatal): {e}")
    print()


def _auto_recalibrate() -> None:
    """Trigger tools/recalibrate_exits.py in the background-safe way.

    Uses --min-n 20 (the tool's own gate) so thin datasets no-op cleanly.
    Output is captured and only printed if tiers changed or on error —
    keeps the snapshot summary uncluttered on routine runs.
    """
    import subprocess
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "recalibrate_exits.py")
    if not os.path.exists(script):
        return
    print(f"\n  Recalibrating exit rules from snapshot history…")
    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True, text=True, timeout=600,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
    except subprocess.TimeoutExpired:
        print("    (recalibration timed out after 10m — skipped)")
        return
    out = (result.stdout or "") + (result.stderr or "")
    # Surface only the meaningful lines; swallow the yfinance warnings.
    interesting = [
        ln for ln in out.splitlines()
        if any(k in ln for k in (
            "Priced with intraday", "Below --min-n", "New calibration",
            "Diff vs previous", "REVIEW", "Wrote ", "score ",
        ))
    ]
    for ln in interesting:
        print(f"    {ln}")
    if any("REVIEW" in ln for ln in interesting):
        print("    ⚠ Tier drift detected — review above before next live run")


def _print_catalogue(df: pd.DataFrame, snap_date: str) -> None:
    # Only show buy-side — no naked/spread selling
    df = df[df["vol_signal"].isin(["BUY VOL", "FLOW BUY"])].copy()
    if df.empty:
        print("\n  No BUY VOL signals found in this scan.")
        return

    print(f"\n{'═'*72}")
    buy_ct  = int((df["vol_signal"] == "BUY VOL").sum())
    flow_ct = int((df["vol_signal"] == "FLOW BUY").sum())
    print(f"  CATALOGUE — {len(df)} contracts across {df['symbol'].nunique()} tickers  ({buy_ct} BUY VOL, {flow_ct} FLOW BUY)")
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
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*", help="explicit tickers (else discovery)")
    ap.add_argument("--mode", choices=["MICRO", "STANDARD", "FULL"],
                    help="override auto mode selection")
    ap.add_argument("--suffix", help="append to output filename, e.g. 'micro'")
    args = ap.parse_args()
    run_and_save(args.tickers or None, mode_override=args.mode, suffix=args.suffix)
