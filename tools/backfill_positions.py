"""
One-shot backfill: import currently-open Alpaca paper positions into
engine_state.db so engine.execute.monitor_tick() can manage them.

This is needed because tools/paper_trade.py historically did not call
engine.state.record_open, so existing live positions are invisible to
the exit watcher. Run once after deploying the record_open fix.

Pulls:
  - position symbol, qty, avg_entry_price → record_open
  - earliest BUY-fill timestamp from Alpaca order history → entry_date
  - reads paper_trades.jsonl to recover score/vol_signal where possible
  - skips symbols that already have an OPEN row in engine_state.db
  - skips symbols that already have a SELL order pending (so we don't
    double-monitor a position that's already on its way out)

Usage:
    python -m tools.backfill_positions
    python -m tools.backfill_positions --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401  loads .env

from broker import alpaca
from engine.state import init_db, list_open, record_open, OpenPositionRecord
from risk.exits import apply_safety_floors


def _parse_occ(occ: str) -> tuple[str, str, str, float] | None:
    # NKE260508C00047500 → (NKE, 2026-05-08, call, 47.50)
    i = 0
    while i < len(occ) and occ[i].isalpha():
        i += 1
    if i == 0 or len(occ) - i < 15:
        return None
    underlying = occ[:i]
    ymd = occ[i:i + 6]
    cp = occ[i + 6]
    strike8 = occ[i + 7:i + 15]
    try:
        d = datetime.strptime(ymd, "%y%m%d").date().isoformat()
        opt_type = "call" if cp == "C" else "put"
        strike = int(strike8) / 1000.0
        return underlying, d, opt_type, strike
    except Exception:
        return None


def _earliest_buy_fill(symbol: str) -> datetime | None:
    """Search 30d of orders for the earliest BUY fill on this symbol."""
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        c = alpaca._trading_client()
        since = datetime.now(timezone.utc) - timedelta(days=45)
        orders = c.get_orders(GetOrdersRequest(status=QueryOrderStatus.ALL, after=since, limit=500))
    except Exception:
        return None
    earliest = None
    for o in orders:
        if o.symbol != symbol:
            continue
        if str(o.side) != "OrderSide.BUY":
            continue
        if not o.filled_at:
            continue
        if earliest is None or o.filled_at < earliest:
            earliest = o.filled_at
    return earliest


def _score_from_jsonl(symbol: str) -> tuple[float, str]:
    """Recover (score, vol_signal) from paper_trades.jsonl for this OCC."""
    log = REPO_ROOT / "logs" / "paper_trades.jsonl"
    if not log.exists():
        return 0.0, ""
    last_score, last_signal = 0.0, ""
    with log.open(encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("occ") != symbol:
                continue
            if rec.get("score") is not None:
                last_score = float(rec["score"])
            if rec.get("signal"):
                last_signal = str(rec["signal"])
    return last_score, last_signal


def _has_pending_sell(symbol: str) -> bool:
    """Direct SDK query (canonical broker.get_open_orders has an OrderStatus bug)."""
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        c = alpaca._trading_client()
        orders = c.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=200))
        for o in orders:
            if o.symbol == symbol and str(o.side) in ("sell", "OrderSide.SELL"):
                return True
    except Exception:
        pass
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    init_db()

    positions = alpaca.get_positions()
    print(f"Alpaca: {len(positions)} open positions")

    open_rows = list_open()
    already_tracked = {r["occ_symbol"] for r in open_rows}
    print(f"engine_state.db: {len(open_rows)} already tracked: {sorted(already_tracked) or '(none)'}")

    inserted = 0
    skipped = 0

    for p in positions:
        occ = p.symbol
        if occ in already_tracked:
            print(f"  {occ}: skip — already in engine state")
            skipped += 1
            continue
        if _has_pending_sell(occ):
            print(f"  {occ}: skip — already has pending SELL order")
            skipped += 1
            continue

        parsed = _parse_occ(occ)
        if not parsed:
            print(f"  {occ}: skip — non-option symbol")
            skipped += 1
            continue

        underlying, expiry_iso, opt_type, strike = parsed
        qty = abs(int(p.qty))
        entry_price = float(p.avg_entry)
        dte = (datetime.fromisoformat(expiry_iso).date() - datetime.now(timezone.utc).date()).days

        score, vol_signal = _score_from_jsonl(occ)
        first_fill = _earliest_buy_fill(occ)
        entry_date_iso = (first_fill.astimezone(timezone.utc).date().isoformat()
                          if first_fill else datetime.now(timezone.utc).date().isoformat())

        sl_pct, tp_pct = apply_safety_floors(
            score=score, dte=dte, has_catalyst_in_window=False, held_overnight=True,
        )

        record = OpenPositionRecord(
            occ_symbol=occ,
            underlying=underlying,
            option_type=opt_type,
            strike=strike,
            expiry=expiry_iso,
            qty=qty,
            entry_price=entry_price,
            entry_date=entry_date_iso,
            entry_order_id=None,
            score=score,
            dte_at_entry=dte,
            vol_signal=vol_signal,
            sl_pct=sl_pct,
            tp_pct=tp_pct,
        )

        print(f"  {occ}: insert qty={qty} entry=${entry_price:.2f} entry_date={entry_date_iso} "
              f"score={score:.0f} sig={vol_signal or '-'} dte={dte} sl={sl_pct:.0%}")

        if not args.dry_run:
            record_open(record)
            inserted += 1

    print(f"\nResult: inserted={inserted} skipped={skipped} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
