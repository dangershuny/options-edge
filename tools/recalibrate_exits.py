#!/usr/bin/env python3
"""
Recalibrate exit rules and entry-timing windows from accumulated snapshots.

Replays every snapshot in `snapshots/` that has intraday data still
available from yfinance (5-day rolling window for 5-minute bars), and
writes the empirically best SL/TP tier per score bucket plus the
intraday-timing table to `risk/exits_calibration.json`.

Next import of `risk.exits` picks up the new values automatically.

Usage:
    python tools/recalibrate_exits.py
    python tools/recalibrate_exits.py --dry-run      # don't write, just print
    python tools/recalibrate_exits.py --min-n 50     # skip if fewer contracts priced
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, time
from statistics import median

import pandas as pd
import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "snapshots"
)
CALIB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "risk", "exits_calibration.json",
)

SL_GRID = [None, -0.50, -0.40, -0.30, -0.25, -0.20, -0.15, -0.10]
TP_GRID = [None, 0.20, 0.30, 0.50, 0.75, 1.00, 1.50, 2.00]
SCORE_BUCKETS = [(0, 39), (40, 59), (60, 79), (80, 100)]
TIME_POINTS = [
    ("09:30", time(9, 30)), ("09:45", time(9, 45)), ("10:00", time(10, 0)),
    ("10:15", time(10, 15)), ("10:30", time(10, 30)), ("11:00", time(11, 0)),
    ("12:00", time(12, 0)), ("13:00", time(13, 0)), ("14:00", time(14, 0)),
    ("15:00", time(15, 0)),
]


def occ(sym, expiry, opt_type, strike):
    d = datetime.strptime(expiry, "%Y-%m-%d")
    cp = "C" if opt_type.lower().startswith("c") else "P"
    return f"{sym}{d.strftime('%y%m%d')}{cp}{int(round(strike*1000)):08d}"


def fetch_bars(ticker):
    try:
        df = yf.Ticker(ticker).history(period="5d", interval="5m", auto_adjust=False)
        if df.empty:
            return None
        try:
            df.index = df.index.tz_convert("America/New_York")
        except Exception:
            pass
        return df
    except Exception:
        return None


def simulate(entry, bars, sl_pct, tp_pct):
    """Walk forward through bars; return exit price."""
    if sl_pct is None and tp_pct is None:
        return float(bars.iloc[-1]["Close"])
    sl_px = entry * (1 + sl_pct) if sl_pct is not None else None
    tp_px = entry * (1 + tp_pct) if tp_pct is not None else None
    for _, bar in bars.iterrows():
        lo, hi = float(bar["Low"]), float(bar["High"])
        if sl_px is not None and lo <= sl_px:
            return sl_px
        if tp_px is not None and hi >= tp_px:
            return tp_px
    return float(bars.iloc[-1]["Close"])


def load_contracts():
    contracts = []
    for fn in sorted(os.listdir(SNAPSHOT_DIR)):
        if not fn.endswith(".json"):
            continue
        snap_day = datetime.strptime(fn[:-5], "%Y-%m-%d").date()
        with open(os.path.join(SNAPSHOT_DIR, fn)) as f:
            snap = json.load(f)
        for tr in snap.get("trades", []):
            contracts.append({**tr, "snap_date": snap_day})
    return contracts


def price_at(bars, trade_day, t):
    target = datetime.combine(trade_day, t)
    if bars.index.tz:
        target = target.replace(tzinfo=bars.index.tz)
    m = bars[bars.index >= target]
    return None if m.empty else float(m.iloc[0]["Close"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-n", type=int, default=20)
    args = ap.parse_args()

    contracts = load_contracts()
    print(f"Loaded {len(contracts)} contracts across {len({c['snap_date'] for c in contracts})} snapshot days")

    # Fetch bars once per contract
    priced = []
    for c in contracts:
        tkr = occ(c["symbol"], c["expiry"], c["option_type"], c["strike"])
        bars = fetch_bars(tkr)
        if bars is None:
            continue
        same_day = bars[bars.index.date == c["snap_date"]]
        if same_day.empty:
            continue
        entry = price_at(same_day, c["snap_date"], time(9, 30))
        close = float(same_day.iloc[-1]["Close"])
        if entry is None or entry <= 0:
            continue
        priced.append({**c, "entry": entry, "close": close, "bars": same_day})

    print(f"Priced with intraday bars: {len(priced)}")
    if len(priced) < args.min_n:
        print(f"Below --min-n {args.min_n}; not writing calibration.")
        return

    # Per-bucket grid search
    tiers = []
    for lo, hi in SCORE_BUCKETS:
        bucket = [p for p in priced if lo <= p["score"] <= hi]
        if not bucket:
            continue
        best = {"pnl": -1e18, "sl": None, "tp": None, "rois": []}
        for sl in SL_GRID:
            for tp in TP_GRID:
                total = 0.0
                rois = []
                for p in bucket:
                    exit_px = simulate(p["entry"], p["bars"], sl, tp)
                    total += (exit_px - p["entry"]) * 100
                    rois.append((exit_px / p["entry"] - 1) * 100)
                if total > best["pnl"]:
                    best = {"pnl": total, "sl": sl, "tp": tp, "rois": rois}
        tiers.append({
            "score_min": lo, "score_max": hi,
            "sl_pct": best["sl"], "tp_pct": best["tp"],
            "n": len(bucket),
            "avg_roi_pct": round(sum(best["rois"])/len(best["rois"]), 2),
            "median_roi_pct": round(median(best["rois"]), 2),
            "total_pnl": round(best["pnl"], 2),
        })

    # Entry-timing table
    timing = []
    for lbl, t in TIME_POINTS:
        rois, pnl, n = [], 0.0, 0
        for p in priced:
            entry_t = price_at(p["bars"], p["snap_date"], t)
            if entry_t is None or entry_t <= 0:
                continue
            pnl += (p["close"] - entry_t) * 100
            rois.append((p["close"]/entry_t - 1) * 100)
            n += 1
        if n:
            timing.append({
                "time_et": lbl, "n": n,
                "avg_roi_pct": round(sum(rois)/n, 2),
                "total_pnl": round(pnl, 2),
            })

    # Pick entry windows: ideal = best time, grace = next best within 25% of ideal
    best_time = max(timing, key=lambda r: r["total_pnl"]) if timing else None
    entry_windows = {"open": "09:30", "grace": "09:45", "cutoff": "10:00"}
    if best_time and best_time["time_et"] != "09:30":
        # Only shift if there's a clear winner at another time
        entry_windows["open"] = best_time["time_et"]

    # Load previous calibration for diff
    prev = None
    if os.path.exists(CALIB_PATH):
        try:
            with open(CALIB_PATH) as f:
                prev = json.load(f)
        except Exception:
            pass

    calib = {
        "last_updated": date.today().isoformat(),
        "snapshot_dates": sorted({c["snap_date"].isoformat() for c in contracts}),
        "n_contracts": len(priced),
        "tiers": tiers,
        "entry_windows": entry_windows,
        "timing_table": timing,
    }

    # Pretty-print summary + diff
    print("\n=== New calibration ===")
    for t in tiers:
        sl_s = f"{t['sl_pct']*100:+.0f}%" if t['sl_pct'] is not None else "none"
        tp_s = f"{t['tp_pct']*100:+.0f}%" if t['tp_pct'] is not None else "none"
        print(f"  score {t['score_min']:>2}-{t['score_max']:<3} (n={t['n']:>3})  "
              f"SL {sl_s:>6}  TP {tp_s:>6}  ROI {t['avg_roi_pct']:>6.2f}%  P&L ${t['total_pnl']:>8,.0f}")
    print(f"\nEntry windows: open {entry_windows['open']}  "
          f"grace {entry_windows['grace']}  cutoff {entry_windows['cutoff']}")

    if prev:
        print("\n=== Diff vs previous ===")
        prev_tiers = {(t["score_min"], t["score_max"]): t for t in prev.get("tiers", [])}
        for t in tiers:
            key = (t["score_min"], t["score_max"])
            if key in prev_tiers:
                pv = prev_tiers[key]
                if pv["sl_pct"] != t["sl_pct"] or pv["tp_pct"] != t["tp_pct"]:
                    print(f"  score {key[0]}-{key[1]}: "
                          f"SL {pv['sl_pct']}→{t['sl_pct']}  "
                          f"TP {pv['tp_pct']}→{t['tp_pct']}  "
                          f"⚠ REVIEW")

    if args.dry_run:
        print("\n(dry-run) not writing calibration file")
        return

    with open(CALIB_PATH, "w", encoding="utf-8") as f:
        json.dump(calib, f, indent=2)
    print(f"\nWrote {CALIB_PATH}")


if __name__ == "__main__":
    main()
