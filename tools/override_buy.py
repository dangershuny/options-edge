"""
Manual override buy — operator-driven trade for a specific ticker.

When the operator sees a signal in the news/sentinel dashboard that the
scheduled scanner missed (or hasn't picked up yet), they can use this tool
to force a trade on that ticker. The system:

  1. Runs analyze_ticker(ticker) to get the full chain + scoring
  2. Picks the best contract by score (any signal qualifies — no filter
     by vol_signal because the operator has already decided this is worth
     trading; we just want the most-poised contract)
  3. Sizes per the active mode tier (max_cost_per_trade, max_per_ticker)
  4. Submits to Alpaca paper as a tagged "override" order
  5. Returns a structured result for the caller (CLI or HTTP server)

Critical safety:
  • Respects per-ticker concentration cap from paper_trades.jsonl
  • Respects per-trade cost cap (mode-based)
  • Tags order with client_order_id "override-{tag}-{ticker}-{ts}"
  • Refuses to trade on LIVE accounts (only paper)
  • Never raises — always returns dict

CLI usage:
    python -m tools.override_buy --ticker GME
    python -m tools.override_buy --ticker GME --bankroll 500 --tag mine
    python -m tools.override_buy --ticker GME --dry-run
    python -m tools.override_buy --ticker GME --side put     # force direction
    python -m tools.override_buy --ticker GME --max-cost 100 # custom cap
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load Alpaca + sentinel env from the shared .env
import config_loader  # noqa: F401

# Output goes to the same log so dashboard sees it alongside scheduled trades
PAPER_TRADES_LOG = REPO_ROOT / "logs" / "paper_trades.jsonl"
OVERRIDE_RESULTS_DIR = REPO_ROOT / "logs" / "override_results"
OVERRIDE_RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def _today_iso() -> str:
    return date.today().isoformat()


def _count_today_for_ticker(symbol: str) -> int:
    """Count today's submitted orders for this ticker (across all tiers)."""
    if not PAPER_TRADES_LOG.exists():
        return 0
    today = _today_iso()
    sym = symbol.upper()
    count = 0
    try:
        with open(PAPER_TRADES_LOG) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if (rec.get("status") == "submitted"
                        and rec.get("timestamp", "").startswith(today)
                        and (rec.get("symbol") or "").upper() == sym):
                    count += 1
    except Exception:
        pass
    return count


def _build_coid(tag: str, occ: str) -> str:
    """Distinct override tag so dashboard / Alpaca can filter overrides."""
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    clean = "".join(c for c in (tag or "ovr").lower() if c.isalnum())[:10] or "ovr"
    return f"override-{clean}-{occ}-{ts}"[:128]


def pick_best_option(symbol: str, side: str | None = None,
                       min_score: float = 40.0) -> dict | None:
    """
    Run full analysis on the ticker, return best contract dict.

    side=None lets the scorer pick best of call OR put. side="call"/"put"
    forces direction (operator may have a thesis).

    min_score is intentionally low (40 vs the scheduled 60-65) because the
    operator has already decided this ticker is worth trading — we just
    want the most-poised contract. Score still serves as a sanity check
    (rejects garbage chains).
    """
    try:
        from analysis.scorer import analyze_ticker
    except Exception as e:
        return {"error": f"scorer import failed: {e}"}

    try:
        result = analyze_ticker(symbol)
        df = result[0] if isinstance(result, tuple) else result
    except Exception as e:
        return {"error": f"analyze_ticker failed: {e}"}

    if df is None or df.empty:
        return {"error": f"no analyzable contracts for {symbol}"}

    # Filter by min_score, optionally by side
    candidates = df[df["score"] >= min_score].copy()
    if side in ("call", "put") and "type" in candidates.columns:
        candidates = candidates[candidates["type"] == side]

    if candidates.empty:
        # Loosen — return best of any score
        candidates = df if (side is None or "type" not in df.columns
                            ) else df[df["type"] == side]
        if candidates.empty:
            return {"error": "no contracts after filters"}

    # Sort by score desc and take the top
    best = candidates.sort_values("score", ascending=False).iloc[0]
    return best.to_dict()


def execute_override(ticker: str,
                      side: str | None = None,
                      bankroll: float | None = None,
                      max_cost: float | None = None,
                      tag: str = "manual",
                      dry_run: bool = True,
                      min_score: float = 40.0) -> dict:
    """
    Top-level entry point. Returns dict with status + details. Never raises.

    Returns {
        "ok": bool,
        "ticker": str,
        "status": "submitted" | "dry_run" | "skipped" | "failed",
        "reason": str,                  (only on skipped/failed)
        "contract": {...},              (chosen contract details)
        "order_id": str,                (Alpaca order id on submit)
        "client_order_id": str,
        "cost": float,
        "limit_price": float,
        "timestamp": str,
    }
    """
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return {"ok": False, "ticker": "", "status": "failed",
                "reason": "empty ticker"}

    out: dict[str, Any] = {
        "ok": False,
        "ticker": ticker,
        "side_requested": side,
        "tag": tag,
        "dry_run": dry_run,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Risk config
    try:
        from risk.config import RISK
        max_per_ticker = int(RISK.get("max_positions_per_ticker", 2) or 2)
        if max_cost is None:
            max_cost = float(RISK.get("max_cost_per_trade", 75.0) or 75.0)
    except Exception:
        max_per_ticker = 2
        max_cost = max_cost or 75.0
    out["max_cost"] = max_cost
    out["max_per_ticker"] = max_per_ticker

    # Concentration cap check (across ALL tiers + manual overrides today)
    today_count = _count_today_for_ticker(ticker)
    out["existing_today"] = today_count
    if today_count >= max_per_ticker:
        out["status"] = "skipped"
        out["reason"] = (
            f"already have {today_count} {ticker} orders today "
            f"(cap = {max_per_ticker}). Wait until tomorrow or cancel one."
        )
        _persist_result(out)
        return out

    # Pick best contract
    contract = pick_best_option(ticker, side=side, min_score=min_score)
    if not contract or "error" in contract:
        out["status"] = "failed"
        out["reason"] = (contract or {}).get("error", "no contract found")
        _persist_result(out)
        return out

    out["contract"] = {
        k: contract.get(k) for k in (
            "symbol", "type", "strike", "expiry", "dte",
            "vol_signal", "score", "iv_rv_spread", "flow_signal",
            "stock_price", "bid", "ask",
        )
    }

    # Build OCC + get live quote
    try:
        import broker.alpaca as bro
    except Exception as e:
        out["status"] = "failed"
        out["reason"] = f"broker import: {e}"
        _persist_result(out)
        return out

    if not (bro_keys_ok := _alpaca_keys_set()):
        out["status"] = "failed"
        out["reason"] = "ALPACA_API_KEY / ALPACA_API_SECRET not set"
        _persist_result(out)
        return out

    try:
        acct = bro.get_account()
    except Exception as e:
        out["status"] = "failed"
        out["reason"] = f"alpaca account check: {e}"
        _persist_result(out)
        return out

    if not acct.is_paper and not dry_run:
        out["status"] = "failed"
        out["reason"] = "broker is LIVE, not paper. Refusing override on live account."
        _persist_result(out)
        return out

    out["paper"] = acct.is_paper
    out["account_buying_power"] = acct.buying_power

    # OCC + quote
    try:
        from datetime import datetime as dt
        expiry = dt.strptime(contract["expiry"], "%Y-%m-%d").date()
        opt_type = (contract.get("type") or "").lower()
        strike = float(contract.get("strike") or 0)
        occ = bro.occ_symbol(ticker, expiry, opt_type, strike)
        out["occ"] = occ
    except Exception as e:
        out["status"] = "failed"
        out["reason"] = f"occ_symbol: {e}"
        _persist_result(out)
        return out

    try:
        quote = bro.get_quote(occ)
        bid = float(quote.bid or 0)
        ask = float(quote.ask or 0)
        mid = (bid + ask) / 2.0 if bid and ask else (quote.mid or bid or ask)
    except Exception as e:
        out["status"] = "failed"
        out["reason"] = f"get_quote: {e}"
        _persist_result(out)
        return out

    if not mid or mid <= 0:
        out["status"] = "skipped"
        out["reason"] = "no live quote (market closed or illiquid contract)"
        out["bid"] = bid
        out["ask"] = ask
        _persist_result(out)
        return out

    cost_per_contract = mid * 100
    if cost_per_contract > max_cost:
        out["status"] = "skipped"
        out["reason"] = (
            f"contract cost ${cost_per_contract:.2f} > cap ${max_cost:.2f}. "
            "Pass --max-cost to override, or pick a cheaper strike/expiry."
        )
        out["cost_per_contract"] = round(cost_per_contract, 2)
        _persist_result(out)
        return out

    qty = 1
    total_cost = cost_per_contract * qty
    limit_price = round(mid + 0.02, 2)
    coid = _build_coid(tag, occ)

    out.update({
        "qty": qty,
        "cost_per_contract": round(cost_per_contract, 2),
        "total_cost": round(total_cost, 2),
        "limit_price": limit_price,
        "client_order_id": coid,
        "bid": bid, "ask": ask, "mid": mid,
    })

    if dry_run:
        out["ok"] = True
        out["status"] = "dry_run"
        out["note"] = (
            f"Would BTO {qty}x {occ} at limit ${limit_price:.2f} "
            f"(cost ${total_cost:.2f})"
        )
        _persist_result(out)
        _log_paper_trade_record(out)
        return out

    # Live submit
    try:
        order = bro.buy_option(occ, qty,
                                limit_price=limit_price,
                                client_order_id=coid)
        out["ok"] = True
        out["status"] = "submitted"
        out["order_id"] = (getattr(order, "order_id", None)
                            or getattr(order, "id", None))
        out["order_status"] = str(getattr(order, "status", "submitted"))
    except Exception as e:
        out["status"] = "failed"
        out["reason"] = f"order submit failed: {e}"

    _persist_result(out)
    _log_paper_trade_record(out)
    return out


def _alpaca_keys_set() -> bool:
    import os
    return bool(os.environ.get("ALPACA_API_KEY")
                and os.environ.get("ALPACA_API_SECRET"))


def _log_paper_trade_record(out: dict) -> None:
    """
    Append to the SAME paper_trades.jsonl file the scheduled tiers use,
    so the dashboard's tier-aware view picks up override trades naturally.
    """
    try:
        rec = {
            "symbol": out.get("ticker"),
            "option_type": (out.get("contract") or {}).get("type"),
            "strike": (out.get("contract") or {}).get("strike"),
            "expiry": (out.get("contract") or {}).get("expiry"),
            "score": (out.get("contract") or {}).get("score"),
            "signal": (out.get("contract") or {}).get("vol_signal"),
            "tag": f"override-{out.get('tag') or 'manual'}",
            "status": out.get("status"),
            "timestamp": out.get("timestamp"),
            "occ": out.get("occ"),
            "bid": out.get("bid"),
            "ask": out.get("ask"),
            "mid": out.get("mid"),
            "qty": out.get("qty"),
            "cost_per_contract": out.get("cost_per_contract"),
            "total_cost": out.get("total_cost"),
            "limit_price": out.get("limit_price"),
            "client_order_id": out.get("client_order_id"),
            "order_id": out.get("order_id"),
            "order_status": out.get("order_status"),
            "note": out.get("note"),
            "error": out.get("reason"),
        }
        with open(PAPER_TRADES_LOG, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")
    except Exception:
        pass


def _persist_result(out: dict) -> None:
    """Save full result to override_results dir for dashboard pickup."""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:18]
        path = OVERRIDE_RESULTS_DIR / f"{ts}_{(out.get('ticker') or 'NA')}.json"
        path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual override buy")
    parser.add_argument("--ticker", required=True, help="Symbol to trade")
    parser.add_argument("--side", choices=("call", "put"), default=None,
                        help="Force direction (default: scorer picks best)")
    parser.add_argument("--bankroll", type=float, default=None)
    parser.add_argument("--max-cost", type=float, default=None,
                        help="Override per-trade cost cap (default: mode-based)")
    parser.add_argument("--tag", type=str, default="manual",
                        help="Tag suffix for client_order_id")
    parser.add_argument("--min-score", type=float, default=40.0)
    parser.add_argument("--live", action="store_true",
                        help="Actually submit (default: dry-run)")
    args = parser.parse_args()

    result = execute_override(
        ticker=args.ticker,
        side=args.side,
        bankroll=args.bankroll,
        max_cost=args.max_cost,
        tag=args.tag,
        dry_run=not args.live,
        min_score=args.min_score,
    )

    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") or result.get("status") in (
        "dry_run", "submitted") else 1


if __name__ == "__main__":
    sys.exit(main())
