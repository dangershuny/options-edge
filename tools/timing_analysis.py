#!/usr/bin/env python3
"""
Entry-timing analysis — would waiting past the open improve fills?

For every trade in a snapshot, pull the 5-minute intraday bars for the
OCC option ticker and compute what would have happened if you had bought
at 09:30, 09:45, 10:00, 10:15, 10:30, 11:00, 12:00, 13:00, 14:00, 15:00
instead of at the snapshot's recorded entry. Mark-to-last bar for P&L.

Usage:
  python tools/timing_analysis.py <snapshot_date>
"""

import sys, os, json
from datetime import date, datetime, time
import pandas as pd
import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "snapshots"
)

TIME_POINTS = [
    ("09:30", time(9, 30)),
    ("09:45", time(9, 45)),
    ("10:00", time(10, 0)),
    ("10:15", time(10, 15)),
    ("10:30", time(10, 30)),
    ("11:00", time(11, 0)),
    ("12:00", time(12, 0)),
    ("13:00", time(13, 0)),
    ("14:00", time(14, 0)),
    ("15:00", time(15, 0)),
    ("15:55", time(15, 55)),  # closing mark
]


def occ_ticker(sym: str, expiry: str, opt_type: str, strike: float) -> str:
    d = datetime.strptime(expiry, "%Y-%m-%d")
    yymmdd = d.strftime("%y%m%d")
    cp = "C" if opt_type.lower().startswith("c") else "P"
    strk = f"{int(round(strike * 1000)):08d}"
    return f"{sym}{yymmdd}{cp}{strk}"


def fetch_intraday(occ: str) -> pd.DataFrame | None:
    try:
        t = yf.Ticker(occ)
        df = t.history(period="5d", interval="5m", auto_adjust=False)
        if df.empty:
            return None
        return df
    except Exception:
        return None


def price_at(df: pd.DataFrame, trade_day: date, t: time) -> float | None:
    """Midpoint of first 5m bar at/after target time on trade_day."""
    if df is None or df.empty:
        return None
    # normalize to naive datetime comparisons
    idx = df.index
    try:
        local = idx.tz_convert("America/New_York")
    except Exception:
        local = idx
    df2 = df.copy()
    df2.index = local
    target_dt = datetime.combine(trade_day, t)
    # find first bar at or after target
    matches = df2[df2.index.to_pydatetime() >= target_dt.replace(tzinfo=df2.index.tz) if df2.index.tz else target_dt]
    if matches.empty:
        return None
    row = matches.iloc[0]
    # use close of that 5m bar as executable fill
    return float(row["Close"])


def last_close(df: pd.DataFrame, trade_day: date) -> float | None:
    if df is None or df.empty:
        return None
    try:
        local = df.index.tz_convert("America/New_York")
    except Exception:
        local = df.index
    df2 = df.copy()
    df2.index = local
    same_day = df2[df2.index.date == trade_day]
    if same_day.empty:
        return None
    return float(same_day.iloc[-1]["Close"])


def main(snap_date_str: str):
    path = os.path.join(SNAPSHOT_DIR, f"{snap_date_str}.json")
    with open(path) as f:
        snap = json.load(f)
    trades = snap["trades"]
    trade_day = datetime.strptime(snap_date_str, "%Y-%m-%d").date()

    print(f"\nTiming analysis — {snap_date_str}  ({len(trades)} trades)")
    print("=" * 78)

    # Per-trade intraday curves + aggregate table
    agg = {lbl: {"pnl": 0.0, "wins": 0, "losses": 0, "n": 0, "rois": []} for lbl, _ in TIME_POINTS}
    priced = 0
    skipped = []

    for tr in trades:
        occ = occ_ticker(tr["symbol"], tr["expiry"], tr["option_type"], tr["strike"])
        df = fetch_intraday(occ)
        close_px = last_close(df, trade_day) if df is not None else None
        if df is None or close_px is None:
            skipped.append(tr["symbol"] + " " + tr["option_type"] + " " + str(tr["strike"]))
            continue
        priced += 1

        for lbl, t in TIME_POINTS:
            entry = price_at(df, trade_day, t)
            if entry is None or entry <= 0:
                continue
            pnl = (close_px - entry) * 100  # per contract
            roi = (close_px / entry - 1) * 100
            agg[lbl]["pnl"] += pnl
            agg[lbl]["n"] += 1
            agg[lbl]["rois"].append(roi)
            if pnl > 0:
                agg[lbl]["wins"] += 1
            elif pnl < 0:
                agg[lbl]["losses"] += 1

    print(f"\nPriced: {priced} / {len(trades)}   Skipped: {len(skipped)}")
    if skipped:
        print("  (no intraday data: " + ", ".join(skipped[:8]) + (")" if len(skipped) <= 8 else f", +{len(skipped)-8} more)"))

    print(f"\n{'Time':<8} {'N':>4} {'Avg ROI':>10} {'Med ROI':>10} {'Hit %':>8} {'Total P&L':>12}")
    print("-" * 60)
    best_lbl, best_pnl = None, -1e18
    for lbl, _ in TIME_POINTS:
        a = agg[lbl]
        if a["n"] == 0:
            continue
        avg = sum(a["rois"]) / a["n"]
        med = sorted(a["rois"])[a["n"] // 2]
        hit = a["wins"] / a["n"] * 100
        print(f"{lbl:<8} {a['n']:>4} {avg:>9.2f}% {med:>9.2f}% {hit:>7.1f}% ${a['pnl']:>10,.0f}")
        if a["pnl"] > best_pnl:
            best_pnl, best_lbl = a["pnl"], lbl

    print("-" * 60)
    print(f"\nBest entry time: {best_lbl}  (total P&L ${best_pnl:,.0f} across {agg[best_lbl]['n']} fills)")

    # Compare 09:30 vs 10:00 vs 10:30 head-to-head
    print("\nHead-to-head deltas vs 09:30:")
    base = agg["09:30"]["pnl"] if agg["09:30"]["n"] else 0
    for lbl, _ in TIME_POINTS:
        a = agg[lbl]
        if a["n"] == 0:
            continue
        delta = a["pnl"] - base
        sign = "+" if delta >= 0 else ""
        print(f"  {lbl}:  {sign}${delta:,.0f}  (n={a['n']})")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
