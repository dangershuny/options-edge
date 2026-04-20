#!/usr/bin/env python3
"""
Macro Context Tool — standalone CLI tool.

Usage:
    python tools/macro_context.py

Prints current VIX level, regime, term structure shape, and the
market-wide vol lean (buy vs sell options). Useful for setting the
overall directional bias before scanning individual tickers.

Model:
  VIX < 15        LOW     → options historically cheap; lean sell vol
  15 ≤ VIX < 20   NORMAL  → no strong lean
  20 ≤ VIX < 30   ELEVATED → options mid-range; lean buy vol
  VIX ≥ 30        FEAR    → IV rich; lean sell vol (post-spike IV crush)

Term structure:
  VIX9D > VIX (backwardation) → near-term fear elevated; often a
  short-term reversal / capitulation signal.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.macro import get_vix_context, reset_cache
from data.market import get_current_price
from analysis.skew import calculate_skew
from data.market import get_options_chain


def run() -> None:
    print(f"\n{'='*55}")
    print("  Macro Volatility Context")
    print(f"{'='*55}")

    reset_cache()  # always fresh for CLI
    ctx = get_vix_context()

    if ctx["vix"] is None:
        print("  VIX data unavailable.")
        return

    print(f"  VIX         : {ctx['vix']:.2f}")
    if ctx["vix9d"]:
        print(f"  VIX9D       : {ctx['vix9d']:.2f}")
    print(f"  Regime      : {ctx['regime']}")

    if ctx["term_slope"] is not None:
        ts = ctx["term_slope"]
        struct = "backwardation (near-term fear ⚠️)" if ts < -2 else (
                 "contango (normal)" if ts > 2 else "flat")
        print(f"  Term slope  : {ts:+.2f}  ({struct})")

    print(f"  Vol lean    : {ctx['lean']}")
    print(f"  Summary     : {ctx['summary']}")

    # SPY skew as market-wide directional read
    print(f"\n  Fetching SPY skew…")
    spy_price = get_current_price("SPY")
    if spy_price:
        chain, _, err = get_options_chain("SPY")
        if chain is not None and not err:
            skew = calculate_skew(chain, spy_price)
            print(f"  SPY skew    : {skew['skew_signal']}  —  {skew['skew_summary']}")
            if skew["pc_ratio"] is not None:
                if skew["pc_ratio"] < 0.70:
                    print("  Note: Very low SPY P/C — market may be over-bullish (contrarian warning)")
                elif skew["pc_ratio"] > 1.30:
                    print("  Note: High SPY P/C — fear elevated, potential reversal setup")

    print()
    lean = ctx["lean"]
    regime = ctx["regime"]
    if lean == "BUY VOL":
        print(f"  → Regime {regime}: favour long options (calls/puts/straddles)")
    elif lean == "SELL VOL":
        print(f"  → Regime {regime}: favour credit spreads, iron condors")
    else:
        print(f"  → Regime {regime}: no strong macro lean — weight individual signals")


if __name__ == "__main__":
    run()
    print()
