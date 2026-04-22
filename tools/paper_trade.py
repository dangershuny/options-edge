"""
Paper-trade executor — consumes today's snapshot and places orders on Alpaca paper.

Flow:
  1. Load today's morning snapshot (BUY VOL / FLOW BUY candidates)
  2. Filter by score threshold, bankroll mode limits
  3. Fetch current live mid-price from Alpaca
  4. Size each trade against bankroll
  5. Submit limit orders at mid (or mid+buffer for wider spreads)
  6. Record every order to paper_trades.jsonl
  7. Print a summary

Safety:
  - Defaults to DRY RUN unless --live passed
  - Paper mode enforced (ALPACA_PAPER=true)
  - Never exceeds max_cost_per_trade or max_total_open_risk
  - Refuses to run if broker auth fails

Usage:
  python -m tools.paper_trade --snapshot snapshots/2026-04-22_morning-2026-04-25.json
  python -m tools.paper_trade --live   # actually submit orders
  python -m tools.paper_trade --bankroll 500 --max-trades 3
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

OUTPUT_PATH = REPO_ROOT / "logs" / "paper_trades.jsonl"
OUTPUT_PATH.parent.mkdir(exist_ok=True)


def _load_snapshot(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def _latest_snapshot() -> Path | None:
    """Most recent snapshot file in snapshots/"""
    snap_dir = REPO_ROOT / "snapshots"
    candidates = sorted(
        snap_dir.glob("*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    # Exclude index / subdirectory files
    for c in candidates:
        if c.is_file() and c.parent == snap_dir:
            return c
    return None


def _expiry_to_date(s: str) -> date:
    """YYYY-MM-DD -> date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def _rank_trades(trades: list[dict], min_score: float) -> list[dict]:
    """Filter + sort by score descending."""
    filtered = [
        t for t in trades
        if t.get("score", 0) >= min_score
        and t.get("vol_signal") in ("BUY VOL", "FLOW BUY")
    ]
    return sorted(filtered, key=lambda x: -x.get("score", 0))


def _fmt_currency(x: float) -> str:
    return f"${x:,.2f}"


def _execute_trade(
    broker,
    trade: dict,
    bankroll_remaining: float,
    dry_run: bool,
    max_per_trade: float,
) -> dict:
    """Execute a single trade. Returns result dict."""
    symbol = trade["symbol"]
    opt_type = trade["option_type"].lower()  # 'call' or 'put'
    strike = float(trade["strike"])
    expiry = _expiry_to_date(trade["expiry"])

    result = {
        "symbol": symbol,
        "option_type": opt_type,
        "strike": strike,
        "expiry": expiry.isoformat(),
        "score": trade.get("score"),
        "signal": trade.get("vol_signal"),
        "status": "pending",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Build OCC symbol
    try:
        occ = broker.occ_symbol(symbol, expiry, opt_type, strike)
        result["occ"] = occ
    except Exception as e:
        result["status"] = "failed"
        result["error"] = f"occ_symbol failed: {e}"
        return result

    # Get live quote
    try:
        quote = broker.get_quote(occ)
        mid = (quote.bid + quote.ask) / 2.0 if quote.bid and quote.ask else quote.mid
        result["bid"] = quote.bid
        result["ask"] = quote.ask
        result["mid"] = mid
    except Exception as e:
        result["status"] = "failed"
        result["error"] = f"get_quote failed: {e}"
        return result

    if mid is None or mid <= 0:
        result["status"] = "skipped"
        result["error"] = "no valid quote (market closed or illiquid)"
        return result

    # Cost per contract = mid * 100
    cost_per_contract = mid * 100

    # Size: fit within bankroll AND per-trade cap
    max_contracts = int(min(bankroll_remaining, max_per_trade) // cost_per_contract)
    if max_contracts < 1:
        result["status"] = "skipped"
        result["error"] = (
            f"cost ${cost_per_contract:.2f}/contract exceeds "
            f"per-trade cap ${max_per_trade:.2f} or bankroll ${bankroll_remaining:.2f}"
        )
        return result

    qty = 1  # conservative — 1 contract per signal
    total_cost = cost_per_contract * qty
    result["qty"] = qty
    result["cost_per_contract"] = round(cost_per_contract, 2)
    result["total_cost"] = round(total_cost, 2)

    # Limit price: mid + small buffer for spread crossing
    limit_price = round(mid + 0.02, 2)  # aggressive take-the-offer mid+0.02
    result["limit_price"] = limit_price

    if dry_run:
        result["status"] = "dry_run"
        result["note"] = f"Would BTO {qty}x {occ} at limit ${limit_price:.2f}"
        return result

    # Live submit
    try:
        order = broker.buy_option(occ, qty, limit_price=limit_price)
        result["status"] = "submitted"
        result["order_id"] = getattr(order, "order_id", None) or getattr(order, "id", None)
        result["order_status"] = getattr(order, "status", "submitted")
    except Exception as e:
        result["status"] = "failed"
        result["error"] = f"order submit failed: {e}"

    return result


def run(
    snapshot_path: Path,
    bankroll: float,
    min_score: float,
    max_trades: int,
    dry_run: bool,
    max_per_trade: float | None = None,
) -> dict:
    # Load broker lazily — if no keys, stop before doing anything
    try:
        import broker.alpaca as broker_mod
    except ImportError as e:
        return {"error": f"broker import failed: {e}", "orders": []}

    if not os.environ.get("ALPACA_API_KEY") or not os.environ.get("ALPACA_API_SECRET"):
        return {
            "error": "ALPACA_API_KEY / ALPACA_API_SECRET not set. "
            "See docs/ALPACA_SETUP.md for setup instructions.",
            "orders": [],
        }

    # Connect and verify paper mode
    try:
        acct = broker_mod.get_account()
    except Exception as e:
        return {"error": f"Alpaca connection failed: {e}", "orders": []}

    if not acct.is_paper and not dry_run:
        return {
            "error": "Broker is LIVE, not paper. Refusing to trade. Set ALPACA_PAPER=true.",
            "orders": [],
        }

    # Load snapshot
    snap = _load_snapshot(snapshot_path)
    all_trades = snap.get("trades", [])
    ranked = _rank_trades(all_trades, min_score)[:max_trades]

    if max_per_trade is None:
        max_per_trade = bankroll * 0.15  # 15% of bankroll per trade, default

    bankroll_remaining = bankroll
    orders = []

    print(f"\n=== PAPER TRADE SESSION ===")
    print(f"Mode: {'DRY RUN (no orders submitted)' if dry_run else 'LIVE paper trading'}")
    print(f"Broker: Alpaca {'PAPER' if acct.is_paper else 'LIVE'}")
    print(f"Account equity: {_fmt_currency(acct.equity)}")
    print(f"Account cash: {_fmt_currency(acct.cash)}")
    print(f"Bankroll (for this session): {_fmt_currency(bankroll)}")
    print(f"Per-trade cap: {_fmt_currency(max_per_trade)}")
    print(f"Min score: {min_score}")
    print(f"Max trades: {max_trades}")
    print(f"Snapshot: {snapshot_path.name}")
    print(f"Candidates passing filter: {len(ranked)}")
    print()

    if not ranked:
        print("No trades passed the score/signal filter.")
        return {"orders": [], "bankroll_used": 0, "bankroll_remaining": bankroll}

    for i, trade in enumerate(ranked, 1):
        print(f"--- [{i}/{len(ranked)}] {trade['symbol']} "
              f"{trade['option_type'].upper()} ${trade['strike']} "
              f"exp {trade['expiry']} (score {trade.get('score', 0):.1f}) ---")

        result = _execute_trade(
            broker_mod, trade, bankroll_remaining, dry_run, max_per_trade
        )
        orders.append(result)

        status = result["status"]
        if status == "dry_run":
            print(f"  {result['note']}")
            print(f"  Mid: {_fmt_currency(result['mid'])} | Total cost: {_fmt_currency(result['total_cost'])}")
        elif status == "submitted":
            print(f"  SUBMITTED order_id={result['order_id']} @ {_fmt_currency(result['limit_price'])}")
            bankroll_remaining -= result["total_cost"]
        elif status == "skipped":
            print(f"  SKIPPED: {result['error']}")
        else:
            print(f"  FAILED: {result.get('error', 'unknown')}")

        print()

    # Persist trade log
    with open(OUTPUT_PATH, "a") as f:
        for o in orders:
            f.write(json.dumps(o) + "\n")

    bankroll_used = bankroll - bankroll_remaining
    print("=== SUMMARY ===")
    print(f"Orders attempted: {len(orders)}")
    print(f"  Submitted:  {sum(1 for o in orders if o['status'] == 'submitted')}")
    print(f"  Dry-run:    {sum(1 for o in orders if o['status'] == 'dry_run')}")
    print(f"  Skipped:    {sum(1 for o in orders if o['status'] == 'skipped')}")
    print(f"  Failed:     {sum(1 for o in orders if o['status'] == 'failed')}")
    print(f"Bankroll used:      {_fmt_currency(bankroll_used)}")
    print(f"Bankroll remaining: {_fmt_currency(bankroll_remaining)}")
    print(f"Log: {OUTPUT_PATH}")

    return {
        "orders": orders,
        "bankroll_used": bankroll_used,
        "bankroll_remaining": bankroll_remaining,
        "paper": acct.is_paper,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper-trade executor")
    parser.add_argument("--snapshot", type=str, default=None,
                        help="Path to snapshot JSON (default: latest in snapshots/)")
    parser.add_argument("--bankroll", type=float, default=500.0,
                        help="Bankroll for this session (default: 500)")
    parser.add_argument("--min-score", type=float, default=60.0,
                        help="Minimum signal score (default: 60)")
    parser.add_argument("--max-trades", type=int, default=5,
                        help="Max trades to place (default: 5)")
    parser.add_argument("--max-per-trade", type=float, default=None,
                        help="Max cost per trade (default: 15%% of bankroll)")
    parser.add_argument("--live", action="store_true",
                        help="Actually submit orders (default is dry run)")
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot) if args.snapshot else _latest_snapshot()
    if not snapshot_path or not snapshot_path.exists():
        print(f"Snapshot not found: {snapshot_path}")
        return 1

    result = run(
        snapshot_path=snapshot_path,
        bankroll=args.bankroll,
        min_score=args.min_score,
        max_trades=args.max_trades,
        dry_run=not args.live,
        max_per_trade=args.max_per_trade,
    )

    if "error" in result:
        print(f"\nERROR: {result['error']}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
