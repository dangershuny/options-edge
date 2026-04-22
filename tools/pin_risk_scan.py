#!/usr/bin/env python3
"""
Pin-risk scan — flag today's snapshot trades whose contracts sit near
a gamma wall close to expiry.

Usage:
    python tools/pin_risk_scan.py                   # today's snapshot
    python tools/pin_risk_scan.py 2026-04-21
    python tools/pin_risk_scan.py --json
"""

import argparse
import json
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf

from analysis.pin_risk import assess_pin_risk
from analysis.gamma import calculate_gex

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "snapshots",
)


def _dte(expiry: str) -> int | None:
    try:
        d = datetime.fromisoformat(str(expiry)[:10]).date()
        return (d - date.today()).days
    except Exception:
        return None


def _spot(symbol: str) -> float | None:
    try:
        h = yf.Ticker(symbol).history(period="1d")
        return float(h["Close"].iloc[-1]) if not h.empty else None
    except Exception:
        return None


def _chain(symbol: str, expiry: str):
    try:
        c = yf.Ticker(symbol).option_chain(str(expiry)[:10])
        import pandas as pd
        return pd.concat([c.calls, c.puts], ignore_index=True)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("snap_date", nargs="?", default=date.today().isoformat())
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    path = os.path.join(SNAPSHOT_DIR, f"{args.snap_date}.json")
    if not os.path.exists(path):
        print(f"No snapshot at {path}", file=sys.stderr)
        return 2

    with open(path, encoding="utf-8") as f:
        snap = json.load(f)

    results = []
    for t in snap.get("trades", []) or []:
        sym    = t.get("symbol")
        strike = t.get("strike")
        expiry = t.get("expiry")
        if not sym or strike is None or not expiry:
            continue
        dte = _dte(expiry)
        spot = _spot(sym)
        if spot is None:
            continue
        chain = _chain(sym, expiry)
        try:
            gex = calculate_gex(sym)
        except Exception:
            gex = None
        pin = assess_pin_risk(chain, spot, float(strike), dte or 0, gex)
        results.append({
            "symbol":  sym,
            "type":    t.get("option_type"),
            "strike":  strike,
            "expiry":  expiry,
            "dte":     dte,
            "pin":     pin,
        })

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return 0

    print("=" * 78)
    print(f"  PIN-RISK SCAN  —  {args.snap_date}")
    print("=" * 78)
    print(f"  {'Sym':<6} {'Typ':<4} {'Strike':>8} {'DTE':>4} {'Risk':<9}  Summary")
    print("-" * 78)
    for r in sorted(results, key=lambda x: {"HIGH":0,"MODERATE":1,"LOW":2,"NONE":3}.get(
            x["pin"].get("pin_risk", "NONE"), 4)):
        pin = r["pin"]
        print(f"  {r['symbol']:<6} {(r['type'] or '')[:4]:<4} "
              f"{r['strike']:>8.2f} {r['dte'] if r['dte'] is not None else '?':>4}  "
              f"{pin.get('pin_risk','?'):<9}  {pin.get('summary','')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
