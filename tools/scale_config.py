#!/usr/bin/env python3
"""
Scale risk config to current account equity.

Queries the broker (or a manual override) for current portfolio equity
and updates `risk/config.py` → `RISK["portfolio_size"]`. The scan layer
auto-selects the mode tier from that value, so scaling portfolio size
is the ONE knob that cascades all dollar limits correctly.

Usage:
    python tools/scale_config.py --equity 500        # manual override
    python tools/scale_config.py --broker alpaca     # query live equity
    python tools/scale_config.py --dry-run           # just print

The broker integration is stubbed until broker/alpaca.py lands; manual
override works today for the 2-week sprint.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "risk", "config.py",
)


def _query_broker_equity(broker: str) -> float:
    """Query live equity. Stub until broker/alpaca.py is wired."""
    if broker == "alpaca":
        try:
            from broker.alpaca import get_account_equity  # noqa: F401
            return float(get_account_equity())
        except ImportError:
            raise SystemExit(
                "broker/alpaca.py not yet implemented — pass --equity manually"
            )
    raise SystemExit(f"unknown broker: {broker}")


def _rewrite_portfolio_size(new_value: int) -> int:
    """
    Update the base RISK dict's `portfolio_size` key. Returns old value.

    Uses regex-on-file rather than loading/reserializing to preserve all
    comments and formatting in risk/config.py.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    # Only match the portfolio_size line inside RISK (the tier dicts also
    # define portfolio_size but those are tier-specific anchors we don't
    # want to edit). We anchor to the comment that precedes it.
    pattern = r'("portfolio_size":\s*)([\d_]+)(,\s*#.*capital allocated)'
    m = re.search(pattern, text)
    if not m:
        # Fall back: last portfolio_size in the file is the RISK dict's
        # (tier dicts appear first).
        matches = list(re.finditer(r'"portfolio_size":\s*([\d_]+)', text))
        if not matches:
            raise SystemExit("could not locate portfolio_size in risk/config.py")
        last = matches[-1]
        old = int(last.group(1).replace("_", ""))
        text = text[:last.start(1)] + str(new_value) + text[last.end(1):]
    else:
        old = int(m.group(2).replace("_", ""))
        text = text[:m.start(2)] + str(new_value) + text[m.end(2):]

    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        f.write(text)
    return old


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--equity", type=float, help="manual equity override")
    g.add_argument("--broker", choices=["alpaca"], help="query live broker")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    equity = args.equity if args.equity is not None else _query_broker_equity(args.broker)
    equity = int(round(equity))

    from risk.config import RISK, auto_select_mode
    old_size = int(RISK["portfolio_size"])
    mode = auto_select_mode(equity)
    mode_name = ("MICRO" if equity < 1_000 else
                 "STANDARD" if equity < 5_000 else "FULL")

    print(f"Current config portfolio_size: ${old_size:,}")
    print(f"Target (from equity):          ${equity:,}")
    print(f"Mode tier at this size:        {mode_name}")
    print(f"  max_cost_per_trade:          ${mode['max_cost_per_trade']}")
    print(f"  max_concurrent_positions:    {mode['max_concurrent_positions']}")
    print(f"  max_contract_premium:        ${mode['max_contract_premium']:.2f}")
    print(f"  max_underlying_price:        ${mode['max_underlying_price']}")
    print(f"  min_score_to_trade:          {mode['min_score_to_trade']}")

    if args.dry_run:
        print("\n(dry-run) not writing config")
        return

    if equity == old_size:
        print("\nNo change needed.")
        return

    _rewrite_portfolio_size(equity)
    print(f"\nWrote portfolio_size = ${equity:,} to {CONFIG_PATH}")


if __name__ == "__main__":
    main()
