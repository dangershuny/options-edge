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

# Load .env before anything imports broker code
import config_loader  # noqa: F401  (side-effect: loads Alpaca keys)

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


DEFAULT_ALLOWED_SIGNALS = ("BUY VOL", "FLOW BUY")


def _occ_for(t: dict, broker_mod) -> str:
    """Best-effort OCC computation for a snapshot trade row, used by the
    blacklist filter. Returns '' if any field is missing."""
    try:
        return broker_mod.occ_symbol(
            t["symbol"], _expiry_to_date(t["expiry"]),
            t["option_type"].lower(), float(t["strike"]),
        )
    except Exception:
        return ""


_TREND_CACHE: dict[str, float] = {}


def _underlying_5d_return(symbol: str, lookback_days: int = 5) -> float | None:
    """Cached 5-day total return on the underlying. None if data unavailable.
    Used by the directional gate to refuse BUY CALLs into a downtrend (and
    PUTs into an uptrend). Cached per-process so tier loops don't re-fetch."""
    if symbol in _TREND_CACHE:
        return _TREND_CACHE[symbol]
    try:
        import yfinance as yf
        # Ticker.history returns a flat DataFrame (no MultiIndex like yf.download)
        hist = yf.Ticker(symbol).history(period=f"{lookback_days + 7}d", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist.columns:
            _TREND_CACHE[symbol] = float("nan")
            return None
        closes = hist["Close"].dropna().tolist()
        if len(closes) < 2:
            _TREND_CACHE[symbol] = float("nan")
            return None
        last = float(closes[-1])
        ref_idx = max(0, len(closes) - 1 - lookback_days)
        ref = float(closes[ref_idx])
        ret = (last / ref) - 1 if ref else None
        if ret is not None:
            _TREND_CACHE[symbol] = ret
        return ret
    except Exception:
        _TREND_CACHE[symbol] = float("nan")
        return None


def _passes_pretrade_filters(t: dict) -> tuple[bool, str]:
    """Apply min_underlying_price + directional-trend gates from RISK config.
    Returns (allowed, reason)."""
    from risk.config import RISK
    sym = (t.get("symbol") or "").upper()
    px = float(t.get("stock_price_at_snap") or 0)
    min_px = float(RISK.get("min_underlying_price") or 0)
    if px and min_px and px < min_px:
        return False, f"underlying ${px:.2f} < min_underlying_price ${min_px:.2f}"

    opt_type = (t.get("option_type") or "").lower()
    threshold = float(RISK.get("max_adverse_trend_pct") or 0)  # negative number
    lookback = int(RISK.get("trend_lookback_days") or 5)
    if opt_type in ("call", "put") and threshold:
        ret = _underlying_5d_return(sym, lookback) if sym else None
        if ret is None or (isinstance(ret, float) and (ret != ret)):
            # No data — allow through; rare ticker, don't artificially drop
            return True, ""
        if opt_type == "call" and ret < threshold:
            return False, f"5d return {ret*100:+.1f}% below threshold {threshold*100:+.1f}% (CALL into downtrend)"
        if opt_type == "put" and ret > -threshold:
            return False, f"5d return {ret*100:+.1f}% above threshold {-threshold*100:+.1f}% (PUT into uptrend)"
    return True, ""


# ── New gates added 2026-05-06 after 5/5 -$116 day ───────────────────────────

def _regime_gate(t: dict) -> tuple[bool, str]:
    """Block trades fighting a strong intraday SPY trend. Lets STRONG
    sentinel divergences override soft warnings. Hard blocks at SPY ≥±1%
    are not overridable."""
    try:
        from risk import regime
    except Exception as e:
        return True, f"regime gate import skip: {e}"

    opt_type = (t.get("option_type") or "").lower()
    sym = (t.get("symbol") or "").upper()

    # Try to fetch a divergence_score for STRONG-override path
    div_score: float | None = None
    try:
        from sentinel_bridge import get_divergence
        div = get_divergence(sym, max_age_hours=24) if sym else None
        if div:
            div_score = float(div.get("divergence_score") or 0)
    except Exception:
        div_score = None

    return regime.check(opt_type, divergence_score=div_score)


def _circuit_breaker_gate(t: dict, max_same_day_losses: int = 2) -> tuple[bool, str]:
    """Halt new entries after N same-day SL hits this session. Carryover
    losses don't count — only positions that BOTH entered and closed today
    with negative P&L."""
    try:
        from engine.state import count_same_day_losses_today
        n = count_same_day_losses_today()
    except Exception as e:
        return True, f"circuit breaker check failed (allow): {e}"
    if n >= max_same_day_losses:
        return False, (f"same-day-loss circuit breaker: {n} fresh losses "
                       f"already today (>= {max_same_day_losses}); halting "
                       f"new entries until next session")
    return True, ""


def _spread_gate(t: dict, max_spread_ratio: float = 0.25) -> tuple[bool, str]:
    """Refuse trades on contracts whose bid-ask spread is too wide relative
    to mid. Checks both snapshot-recorded bid/ask AND lets _execute_trade
    re-check against live quote (this gate is best-effort pre-screen)."""
    try:
        bid = float(t.get("bid") or 0)
        ask = float(t.get("ask") or 0)
    except Exception:
        return True, ""
    if bid <= 0 or ask <= 0:
        return True, ""  # missing quote info — don't block here
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return True, ""
    ratio = (ask - bid) / mid
    if ratio > max_spread_ratio:
        return False, (f"bid/ask spread too wide: bid=${bid:.2f} ask=${ask:.2f} "
                       f"spread/mid={ratio*100:.0f}% > {max_spread_ratio*100:.0f}%")
    return True, ""


def _score_cross_validation_gate(t: dict, min_score: float,
                                  contradiction_penalty: float = 20.0
                                  ) -> tuple[bool, str]:
    """If sentinel shows a divergence OPPOSING the trade direction, downgrade
    the score by `contradiction_penalty`. If new score < min_score, refuse.
    No penalty if sentinel is silent or aligned."""
    sym = (t.get("symbol") or "").upper()
    opt_type = (t.get("option_type") or "").lower()
    if not sym or opt_type not in ("call", "put"):
        return True, ""
    try:
        from sentinel_bridge import get_divergence
        div = get_divergence(sym, max_age_hours=24)
    except Exception:
        return True, ""  # sentinel unreachable — don't block
    if not div:
        return True, ""
    direction = (div.get("direction") or "").lower()
    if not direction or direction == "neutral":
        return True, ""
    # Aligned cases — no penalty
    if direction == "bullish_divergence" and opt_type == "call":
        return True, ""
    if direction == "bearish_divergence" and opt_type == "put":
        return True, ""
    # Convergence (consensus) is mildly aligned for matching direction
    if direction == "bullish_convergence" and opt_type == "call":
        return True, ""
    if direction == "bearish_convergence" and opt_type == "put":
        return True, ""
    # Contradiction — apply penalty
    score = float(t.get("score") or 0)
    new_score = score - contradiction_penalty
    if new_score < min_score:
        return False, (f"score-cross-validation: sentinel says {direction} on "
                       f"{sym} but trade is {opt_type}. score {score:.1f} - "
                       f"{contradiction_penalty:.0f} penalty = {new_score:.1f} "
                       f"< min {min_score:.1f}")
    # Penalty applied but still above threshold — allow with note
    return True, (f"score-cross-validation: -{contradiction_penalty:.0f} pts for "
                  f"{direction} on {opt_type} (effective {new_score:.1f})")


def _strategy_v1_gate(t: dict) -> tuple[bool, str]:
    """The first profitable strategy found by strategy_backtest sweep
    (2026-05-14, n=37 variants, only one with positive expectancy).

    Rule: take ONLY calls where ALL of these are true:
      • skew_signal == "BULLISH"     — chain shows bullish positioning
      • vol_signal  == "BUY VOL"      — cheap IV vs RV (long-vol setup)
      • spread_pct  <= 15%            — liquid enough to round-trip
      • option_type == "call"         — no puts (block_puts still on as safety)

    Backtest stats over 11 trades / 16 trading days:
      win_rate = 46%, avg_return = +14.6%, sharpe = +0.63,
      max drawdown = -8.7%, ending equity $4,321 (from $4,000)

    All other strategies LOST money. The current production scorer with
    score>=60 lost -54% drawdown over the same period.

    Disable this gate (set RISK['use_strategy_v1']=False) only when a
    new backtest demonstrates a better edge. Until then, this is the
    sole entry pathway with measured positive expectancy.
    """
    try:
        from risk.config import RISK
    except Exception:
        return True, ""
    if not bool(RISK.get("use_strategy_v1", True)):
        return True, ""

    if (t.get("option_type") or "").lower() != "call":
        return False, "strategy_v1: calls only"
    if t.get("skew_signal") != "BULLISH":
        return False, (f"strategy_v1: requires BULLISH skew "
                       f"(got {t.get('skew_signal')!r})")
    if t.get("vol_signal") != "BUY VOL":
        return False, (f"strategy_v1: requires BUY VOL signal "
                       f"(got {t.get('vol_signal')!r})")
    bid = float(t.get("bid") or 0)
    ask = float(t.get("ask") or 0)
    if bid <= 0 or ask <= 0:
        return False, "strategy_v1: missing bid/ask"
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return False, "strategy_v1: zero mid"
    spread_pct = (ask - bid) / mid
    if spread_pct > 0.15:
        return False, (f"strategy_v1: spread {spread_pct*100:.1f}% > 15% cap")
    return True, ""


def _direction_halt_gate(t: dict) -> tuple[bool, str]:
    """RISK['block_puts']/['block_calls'] hard halt. Built 2026-05-06 after
    forensic showed puts 0/6 over 2 weeks (-$345). Blanket direction halt
    until backtest demonstrates the blocked direction has edge again."""
    try:
        from risk.config import RISK
    except Exception:
        return True, ""
    opt_type = (t.get("option_type") or "").lower()
    if opt_type == "put" and bool(RISK.get("block_puts", False)):
        return False, ("PUTS BLOCKED globally (RISK['block_puts']=True; "
                       "0-for-6 over 4/24-5/5; flip to False after backtest "
                       "shows edge)")
    if opt_type == "call" and bool(RISK.get("block_calls", False)):
        return False, "CALLS BLOCKED globally (RISK['block_calls']=True)"
    return True, ""


def _all_new_gates(t: dict, min_score: float) -> tuple[bool, str]:
    """Run all 2026-05-06 entry gates in order. Short-circuit on first block.
    Direction halt runs first because it's a hard switch — no point
    computing regime/spread/cross-validation if we're not taking the
    direction at all."""
    # 2026-05-14: strategy_v1 gate runs FIRST. If it rejects, no other
    # gate matters — we only enter trades matching the backtest-proven
    # winning pattern.
    for gate in (_strategy_v1_gate, _direction_halt_gate, _regime_gate,
                 _circuit_breaker_gate, _spread_gate):
        ok, reason = gate(t)
        if not ok:
            return False, reason
    ok, reason = _score_cross_validation_gate(t, min_score)
    if not ok:
        return False, reason
    return True, ""


def _trade_qualifies(t: dict, allowed_signals: tuple[str, ...]) -> bool:
    """
    A trade qualifies for a tier if EITHER:
      • its primary vol_signal is in allowed_signals, OR
      • its secondary "experimental" tag matches a virtual signal in
        allowed_signals (DIRECTIONAL BUY, MOMENTUM BUY, REVERSION BUY).

    This lets BUY-VOL-flagged contracts that ALSO have strong directional
    or momentum setup show up in the d/x experimental tiers — the primary
    cheap-vol signal no longer "absorbs" them invisibly.
    """
    sig = t.get("vol_signal")
    if sig in allowed_signals:
        return True
    if "DIRECTIONAL BUY" in allowed_signals and t.get("is_directional_setup"):
        return True
    if "MOMENTUM BUY" in allowed_signals and t.get("is_momentum_setup"):
        return True
    if "REVERSION BUY" in allowed_signals and t.get("is_reversion_setup"):
        return True
    return False


def _rank_trades(trades: list[dict], min_score: float,
                 allowed_signals: tuple[str, ...] = DEFAULT_ALLOWED_SIGNALS) -> list[dict]:
    """Filter + sort by score descending.

    allowed_signals: which vol_signal values qualify. Defaults to the
    cheap-vol primary path. For directional-only tiers pass
    ("DIRECTIONAL BUY",) — also matches BUY VOL contracts with the
    is_directional_setup flag.
    """
    filtered = []
    rejected_filter: list[tuple[str, str]] = []
    for t in trades:
        if t.get("score", 0) < min_score:
            continue
        if not _trade_qualifies(t, allowed_signals):
            continue
        ok, reason = _passes_pretrade_filters(t)
        if not ok:
            rejected_filter.append((t.get("symbol", "?"), reason))
            continue
        # New 2026-05-06 gates: regime / circuit-breaker / spread / cross-val
        ok, reason = _all_new_gates(t, min_score)
        if not ok:
            rejected_filter.append((t.get("symbol", "?"), reason))
            continue
        if reason:  # pass-with-note (e.g., score-cross-validation soft penalty)
            print(f"  note {t.get('symbol', '?')}: {reason}")
        filtered.append(t)
    if rejected_filter:
        for sym, why in rejected_filter[:8]:
            print(f"  filter-skip {sym}: {why}")
        if len(rejected_filter) > 8:
            print(f"  ... and {len(rejected_filter) - 8} more")
    return sorted(filtered, key=lambda x: -x.get("score", 0))


def _fmt_currency(x: float) -> str:
    return f"${x:,.2f}"


def _build_client_order_id(tag: str, occ: str) -> str:
    """
    Build an Alpaca client_order_id that encodes the bankroll tier.

    Format: {tag}-{occ}-{timestamp}
    Alpaca limit: 128 chars, alphanumeric + dashes + underscores only.
    Tag appears in Alpaca dashboard -> Orders -> "Client Order ID" column,
    so you can filter your paper orders by tier.
    """
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    # Sanitize tag: lowercase, alphanumeric only
    clean_tag = "".join(c for c in tag.lower() if c.isalnum())[:12] or "sim"
    coid = f"{clean_tag}-{occ}-{ts}"
    return coid[:128]


def _execute_trade(
    broker,
    trade: dict,
    bankroll_remaining: float,
    dry_run: bool,
    max_per_trade: float,
    tag: str = "",
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
        "tag": tag,
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

    # Live spread re-check (snapshot bid/ask can be stale; this catches it
    # at order time). FUBO 5/5 phantomed twice with bid=0.35/ask=0.96 on
    # the snapshot — spread/mid 92% — would have been caught here.
    try:
        from risk.config import RISK as _R
        max_ratio = float(_R.get("max_bid_ask_spread_ratio", 0.25))
    except Exception:
        max_ratio = 0.25
    if quote.bid and quote.ask:
        live_ratio = (quote.ask - quote.bid) / mid if mid else 999
        if live_ratio > max_ratio:
            result["status"] = "skipped"
            result["error"] = (f"live spread too wide: bid=${quote.bid:.2f} "
                               f"ask=${quote.ask:.2f} mid=${mid:.2f} "
                               f"ratio={live_ratio*100:.0f}% > "
                               f"{max_ratio*100:.0f}%")
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

    # Build client_order_id tagged with the bankroll tier for Alpaca dashboard
    coid = _build_client_order_id(tag, occ) if tag else None
    result["client_order_id"] = coid

    if dry_run:
        result["status"] = "dry_run"
        result["note"] = f"Would BTO {qty}x {occ} at limit ${limit_price:.2f}"
        if coid:
            result["note"] += f" (COID: {coid})"
        return result

    # Live submit
    try:
        order = broker.buy_option(occ, qty, limit_price=limit_price,
                                  client_order_id=coid)
        result["status"] = "submitted"
        result["order_id"] = getattr(order, "order_id", None) or getattr(order, "id", None)
        result["order_status"] = getattr(order, "status", "submitted")
        # Telegram update for the operator (silent — non-loud severity)
        try:
            from tools.notify import send
            send(
                "ENTRY",
                f"{qty}x {occ} @ ${limit_price:.2f}",
                f"score={float(trade.get('score') or 0):.0f} "
                f"sig={trade.get('vol_signal','-')} cost=${total_cost:.0f} "
                f"tag={tag or '-'}",
            )
        except Exception:
            pass
        # Record into engine_state.db so monitor_tick() can manage exits.
        # Without this the buyer (paper_trade) and the exit-watcher
        # (engine.execute.monitor_tick) talk to two different stores —
        # exits never fire on positions opened here. Failure here must
        # never block submission, so it's wrapped + logged silently.
        try:
            from risk.exits import apply_safety_floors
            from engine.state import (
                record_open, OpenPositionRecord, tag_strategy,
            )
            sl_pct, tp_pct = apply_safety_floors(
                score=float(trade.get("score") or 0),
                dte=(expiry - date.today()).days,
                has_catalyst_in_window=bool(trade.get("catalyst_summary")),
                held_overnight=True,
            )
            position_id = record_open(OpenPositionRecord(
                occ_symbol=occ,
                underlying=symbol,
                option_type=opt_type,
                strike=strike,
                expiry=expiry.isoformat(),
                qty=qty,
                entry_price=float(limit_price),
                entry_date=date.today().isoformat(),
                entry_order_id=str(result["order_id"]) if result.get("order_id") else None,
                score=float(trade.get("score") or 0),
                dte_at_entry=(expiry - date.today()).days,
                vol_signal=str(trade.get("vol_signal") or ""),
                sl_pct=sl_pct,
                tp_pct=tp_pct,
            ))

            # Strategy tagging — record which strategy selected this trade
            # plus the full signal context at entry. Lets strategy_tracker
            # compute per-signal performance breakdowns later, without
            # depending on snapshot files remaining intact.
            try:
                from risk.config import RISK
                strat_id = "strategy_v1" if RISK.get("use_strategy_v1") else "scorer_legacy"
                strat_version = "v1.0"  # bump when the gate rule changes
                # Capture every signal the strategy could be tweaked against
                entry_context = {
                    k: trade.get(k) for k in (
                        "symbol", "strike", "expiry", "type", "option_type",
                        "bid", "ask", "score", "dte",
                        "vol_signal", "flow_signal", "skew_signal", "gex_signal",
                        "iv_pct", "rv_pct", "iv_rv_spread", "iv_rank",
                        "insider_signal", "short_signal", "blocks_signal",
                        "sentiment_delta", "sentiment_composite",
                        "news_drift_delta", "has_recent_8k", "recent_8k_count",
                        "trend_pct", "trend_3d", "rsi14",
                        "stock_price",
                    ) if k in trade
                }
                if position_id:
                    tag_strategy(position_id, strat_id, strat_version, entry_context)
            except Exception as e:
                result.setdefault("warnings", []).append(f"tag_strategy: {e}")
        except Exception as e:
            # state failure must not break submission
            result["state_record_warning"] = f"record_open failed: {e}"
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
    tag: str = "",
    allowed_signals: tuple[str, ...] = DEFAULT_ALLOWED_SIGNALS,
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

    # Health-runner halt flag — auto_remediate writes this when daily-loss
    # cap is hit. Refuse new buys for the rest of the session.
    halt_flag = REPO_ROOT / "logs" / f"halt_buys_{date.today().isoformat()}.flag"
    if halt_flag.exists() and not dry_run:
        return {
            "error": f"halt_buys flag set ({halt_flag.name}); skipping all entries",
            "orders": [],
        }

    # Health-runner contract blacklist — OCCs that had N+ EXPIRED BUYs today.
    # Skip them on subsequent intraday runs to stop wasting bankroll on
    # contracts whose limit price won't fill.
    blacklist_path = REPO_ROOT / "logs" / f"contract_blacklist_{date.today().isoformat()}.json"
    blacklist: set[str] = set()
    if blacklist_path.exists():
        try:
            blacklist = set(json.loads(blacklist_path.read_text(encoding="utf-8")))
        except Exception:
            blacklist = set()

    # Load snapshot
    snap = _load_snapshot(snapshot_path)

    # Refuse to run on stale data — biggest contributor to 5/5 -$116 day
    # was a snapshot frozen at 2026-04-18 still being used on 2026-05-05.
    try:
        from tools.snapshot import is_snapshot_stale
        stale, reason = is_snapshot_stale(snap, max_calendar_days=5)
        if stale:
            # Loud Telegram alert — operator must intervene
            try:
                from tools.notify import send
                send("CRIT", "Snapshot STALE — picker halted",
                     f"{reason}\nFile: {snapshot_path.name}\nFix: regenerate "
                     f"snapshot or check yfinance/Alpaca clock data.")
            except Exception:
                pass
            return {
                "error": f"snapshot stale: {reason}",
                "orders": [],
            }
    except ImportError:
        pass  # snapshot module unavailable — don't block legacy callers

    all_trades = snap.get("trades", [])
    ranked = _rank_trades(all_trades, min_score, allowed_signals)
    if blacklist:
        before = len(ranked)
        ranked = [t for t in ranked if _occ_for(t, broker_mod) not in blacklist]
        if before != len(ranked):
            print(f"Blacklist: dropped {before - len(ranked)} trades whose OCC is blacklisted today")

    # Per-ticker concentration cap — counted across ALL tiers run today.
    # Without this, all 9 tiers can pick the same dominant ticker (today
    # was 9 NKE call orders across 3 strikes = full session in one name).
    from datetime import date as _date
    try:
        from risk.config import RISK
        max_per_ticker = int(RISK.get("max_positions_per_ticker", 2) or 2)
    except Exception:
        max_per_ticker = 2

    today_iso = _date.today().isoformat()
    ticker_count_today: dict[str, int] = {}
    if OUTPUT_PATH.exists():
        try:
            with open(OUTPUT_PATH) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    if (rec.get("status") == "submitted"
                            and rec.get("timestamp", "").startswith(today_iso)):
                        sym = (rec.get("symbol") or "").upper()
                        if sym:
                            ticker_count_today[sym] = ticker_count_today.get(sym, 0) + 1
        except Exception:
            pass

    # Filter ranked candidates by concentration cap (track in-session adds too)
    filtered_ranked = []
    in_session_count = dict(ticker_count_today)
    for t in ranked:
        sym = (t.get("symbol") or "").upper()
        if not sym:
            continue
        if in_session_count.get(sym, 0) >= max_per_ticker:
            continue
        filtered_ranked.append(t)
        in_session_count[sym] = in_session_count.get(sym, 0) + 1
        if len(filtered_ranked) >= max_trades:
            break
    ranked = filtered_ranked

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
    print(f"Signals:   {'+'.join(allowed_signals)}")
    print(f"Tag: {tag if tag else '(none)'}")
    print(f"Per-ticker cap: {max_per_ticker} (across all tiers today)")
    if ticker_count_today:
        print(f"Already taken today: {dict(ticker_count_today)}")
    print(f"Snapshot: {snapshot_path.name}")
    print(f"Candidates passing all filters: {len(ranked)}")
    print()

    if not ranked:
        print("No trades passed the score/signal filter.")
        return {"orders": [], "bankroll_used": 0, "bankroll_remaining": bankroll}

    for i, trade in enumerate(ranked, 1):
        print(f"--- [{i}/{len(ranked)}] {trade['symbol']} "
              f"{trade['option_type'].upper()} ${trade['strike']} "
              f"exp {trade['expiry']} (score {trade.get('score', 0):.1f}) ---")

        result = _execute_trade(
            broker_mod, trade, bankroll_remaining, dry_run, max_per_trade, tag=tag,
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
    parser.add_argument("--tag", type=str, default="",
                        help="Tier tag, e.g. 'sim500'. Prefixes Alpaca client_order_id "
                             "so you can identify which bankroll each order was for.")
    parser.add_argument("--signals", type=str, default="BUY VOL,FLOW BUY",
                        help="Comma-separated vol_signal values to accept. "
                             "Default: 'BUY VOL,FLOW BUY'. For directional-only "
                             "tiers use 'DIRECTIONAL BUY'.")
    args = parser.parse_args()

    # Resolve and STRICTLY validate. str(Path("")) == "." is the classic
    # Windows trap — Path(".").exists() is True (it's the cwd), but opening
    # it crashes with PermissionError. Require a real file.
    raw = (args.snapshot or "").strip()
    if raw and raw not in (".", "./", "/", "\\"):
        snapshot_path = Path(raw)
    else:
        snapshot_path = _latest_snapshot()
    if (snapshot_path is None
            or not snapshot_path.exists()
            or not snapshot_path.is_file()):
        print(f"Snapshot not found or not a file: {snapshot_path}",
              file=sys.stderr)
        return 1

    allowed_signals = tuple(s.strip() for s in args.signals.split(",") if s.strip())

    result = run(
        snapshot_path=snapshot_path,
        bankroll=args.bankroll,
        min_score=args.min_score,
        max_trades=args.max_trades,
        dry_run=not args.live,
        max_per_trade=args.max_per_trade,
        tag=args.tag,
        allowed_signals=allowed_signals,
    )

    if "error" in result:
        print(f"\nERROR: {result['error']}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
