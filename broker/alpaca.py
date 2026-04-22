"""
Alpaca broker integration — thin wrapper over alpaca-py for the trading
engine. Paper and live use the same code path; only env vars differ.

Environment variables (required):
    ALPACA_API_KEY     — your key ID
    ALPACA_API_SECRET  — your secret
    ALPACA_PAPER       — "true" (default) or "false" for live

Setup:
    1. Sign up at https://alpaca.markets
    2. Apply for options Level 2 (buy calls/puts) — usually auto-approved
    3. Paper-trading keys appear immediately; save them:
         setx ALPACA_API_KEY "PKxxxxxxxxxxxxxxxxxx"
         setx ALPACA_API_SECRET "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
         setx ALPACA_PAPER "true"
    4. Verify connectivity:
         python -m broker.alpaca

This module only knows how to talk to the broker. The DECISION logic
(what/when/how much to trade) lives in engine/execute.py — separation
keeps broker swappable (e.g. adding TastyTrade later = new module, no
engine changes).

All functions raise BrokerError on any unrecoverable condition rather
than returning None-on-failure — silent broker failures during live
trading are catastrophic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import (
        MarketOrderRequest, LimitOrderRequest, StopOrderRequest,
        StopLimitOrderRequest, TrailingStopOrderRequest,
        GetOrdersRequest, OptionLegRequest,
    )
    from alpaca.trading.enums import (
        OrderSide, TimeInForce, OrderStatus, OrderClass, OrderType,
        PositionIntent,
    )
    from alpaca.data.historical.option import OptionHistoricalDataClient
    from alpaca.data.requests import OptionLatestQuoteRequest
    _ALPACA_AVAILABLE = True
except ImportError:
    _ALPACA_AVAILABLE = False


class BrokerError(Exception):
    """Any unrecoverable broker condition — auth, network, rejected order."""


# ── Module-level lazy singletons ─────────────────────────────────────────────

_trading: TradingClient | None = None
_data: OptionHistoricalDataClient | None = None


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y"}


def _require_keys() -> tuple[str, str, bool]:
    key = os.environ.get("ALPACA_API_KEY")
    sec = os.environ.get("ALPACA_API_SECRET")
    if not key or not sec:
        raise BrokerError(
            "ALPACA_API_KEY and ALPACA_API_SECRET must be set. "
            "See broker/alpaca.py docstring for setup steps."
        )
    paper = _env_bool("ALPACA_PAPER", True)
    return key, sec, paper


def _trading_client() -> TradingClient:
    global _trading
    if _trading is not None:
        return _trading
    if not _ALPACA_AVAILABLE:
        raise BrokerError("alpaca-py not installed. pip install alpaca-py")
    key, sec, paper = _require_keys()
    _trading = TradingClient(api_key=key, secret_key=sec, paper=paper)
    return _trading


def _data_client() -> OptionHistoricalDataClient:
    global _data
    if _data is not None:
        return _data
    if not _ALPACA_AVAILABLE:
        raise BrokerError("alpaca-py not installed. pip install alpaca-py")
    key, sec, _ = _require_keys()
    _data = OptionHistoricalDataClient(api_key=key, secret_key=sec)
    return _data


# ── Account ──────────────────────────────────────────────────────────────────

@dataclass
class AccountSnapshot:
    equity: float          # total account value (cash + positions at mark)
    cash: float            # settled cash only
    buying_power: float    # cash account: == settled cash
    unsettled: float       # proceeds from recent sales not yet settled
    day_trade_count: int   # 0 for cash accounts
    account_blocked: bool
    is_paper: bool
    account_number: str = ""  # Alpaca account ID (e.g. PA3NZ2BBJVOY)


def get_account() -> AccountSnapshot:
    tc = _trading_client()
    a = tc.get_account()
    equity = float(a.equity)
    cash = float(a.cash)
    bp = float(a.buying_power)
    unsettled = float(getattr(a, "non_marginable_buying_power", cash)) - cash
    # For cash accounts unsettled cash is (cash - buying_power) or tracked via
    # a.cash_withdrawable. Be defensive across API versions.
    try:
        unsettled = float(a.cash) - float(a.cash_withdrawable)
    except Exception:
        unsettled = max(0.0, unsettled)
    return AccountSnapshot(
        equity=equity, cash=cash, buying_power=bp,
        unsettled=max(0.0, unsettled),
        day_trade_count=int(getattr(a, "daytrade_count", 0) or 0),
        account_blocked=bool(getattr(a, "account_blocked", False)),
        is_paper=_env_bool("ALPACA_PAPER", True),
        account_number=str(getattr(a, "account_number", "") or ""),
    )


def get_account_equity() -> float:
    """Used by tools/scale_config.py to auto-pick the mode tier."""
    return get_account().equity


# ── Positions ────────────────────────────────────────────────────────────────

@dataclass
class PositionSnapshot:
    symbol: str            # OCC option symbol, e.g. "AAPL260515C00200000"
    qty: int               # contracts held (positive = long)
    avg_entry: float       # per-contract avg fill
    mark: float            # last mark
    unrealized_pl: float
    unrealized_pl_pct: float
    market_value: float


def get_positions() -> list[PositionSnapshot]:
    tc = _trading_client()
    raw = tc.get_all_positions()
    out: list[PositionSnapshot] = []
    for p in raw:
        try:
            qty = int(float(p.qty))
            avg = float(p.avg_entry_price)
            mark = float(getattr(p, "current_price", 0) or 0)
            upl = float(getattr(p, "unrealized_pl", 0) or 0)
            upl_pct = float(getattr(p, "unrealized_plpc", 0) or 0)
            mv = float(getattr(p, "market_value", 0) or 0)
            out.append(PositionSnapshot(
                symbol=p.symbol, qty=qty, avg_entry=avg, mark=mark,
                unrealized_pl=upl, unrealized_pl_pct=upl_pct,
                market_value=mv,
            ))
        except Exception as e:
            # Don't silently swallow in live trading — surface the problem.
            raise BrokerError(f"malformed position {p}: {e}")
    return out


# ── OCC symbol helpers ───────────────────────────────────────────────────────

def occ_symbol(underlying: str, expiry: date | datetime | str,
               option_type: str, strike: float) -> str:
    """
    Build an OCC-format option symbol.
    e.g. AAPL, 2026-05-15, 'call', 200.0 → 'AAPL260515C00200000'
    """
    if isinstance(expiry, str):
        expiry = datetime.strptime(expiry[:10], "%Y-%m-%d").date()
    if isinstance(expiry, datetime):
        expiry = expiry.date()
    cp = "C" if option_type.lower().startswith("c") else "P"
    strk = f"{int(round(strike * 1000)):08d}"
    return f"{underlying.upper()}{expiry.strftime('%y%m%d')}{cp}{strk}"


# ── Quotes ───────────────────────────────────────────────────────────────────

@dataclass
class OptionQuote:
    symbol: str
    bid: float
    ask: float
    mid: float
    timestamp: datetime | None


def get_quotes(occs: list[str]) -> dict[str, OptionQuote]:
    """Batch NBBO lookup — useful for pricing multi-leg combos in one call."""
    if not occs:
        return {}
    dc = _data_client()
    req = OptionLatestQuoteRequest(symbol_or_symbols=list(occs))
    try:
        resp = dc.get_option_latest_quote(req)
    except Exception as e:
        raise BrokerError(f"batch quote fetch failed: {e}")
    out: dict[str, OptionQuote] = {}
    for occ in occs:
        q = resp.get(occ) if isinstance(resp, dict) else None
        if q is None:
            continue
        bid = float(getattr(q, "bid_price", 0) or 0)
        ask = float(getattr(q, "ask_price", 0) or 0)
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else max(bid, ask)
        out[occ] = OptionQuote(
            symbol=occ, bid=bid, ask=ask, mid=mid,
            timestamp=getattr(q, "timestamp", None),
        )
    return out


def get_quote(occ: str) -> OptionQuote:
    """Latest NBBO for one option contract by OCC symbol."""
    dc = _data_client()
    req = OptionLatestQuoteRequest(symbol_or_symbols=occ)
    try:
        resp = dc.get_option_latest_quote(req)
    except Exception as e:
        raise BrokerError(f"quote fetch failed for {occ}: {e}")
    q = resp.get(occ) if isinstance(resp, dict) else resp[occ]
    if q is None:
        raise BrokerError(f"no quote returned for {occ}")
    bid = float(getattr(q, "bid_price", 0) or 0)
    ask = float(getattr(q, "ask_price", 0) or 0)
    mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else max(bid, ask)
    ts = getattr(q, "timestamp", None)
    return OptionQuote(symbol=occ, bid=bid, ask=ask, mid=mid, timestamp=ts)


# ── Orders ───────────────────────────────────────────────────────────────────

@dataclass
class OrderResult:
    id: str
    symbol: str
    side: str
    qty: int
    order_type: str
    limit_price: float | None
    status: str
    submitted_at: datetime | None
    filled_avg_price: float | None
    filled_qty: int


def _to_result(o) -> OrderResult:
    return OrderResult(
        id=str(o.id),
        symbol=str(o.symbol),
        side=str(o.side),
        qty=int(float(o.qty)),
        order_type=str(o.order_type),
        limit_price=(float(o.limit_price) if getattr(o, "limit_price", None) else None),
        status=str(o.status),
        submitted_at=getattr(o, "submitted_at", None),
        filled_avg_price=(float(o.filled_avg_price) if getattr(o, "filled_avg_price", None) else None),
        filled_qty=int(float(o.filled_qty)) if getattr(o, "filled_qty", None) else 0,
    )


# Sides & intents — the engine currently uses only BUY_TO_OPEN / SELL_TO_CLOSE,
# but the broker layer understands every option side Alpaca supports so we
# don't have to revisit this when we add spreads, covered calls, or
# cash-secured puts.
SIDE_BTO = "buy_to_open"      # long call / long put (debit)
SIDE_STC = "sell_to_close"    # close the long leg
SIDE_STO = "sell_to_open"     # short call / short put (credit) — requires L3+
SIDE_BTC = "buy_to_close"     # buy back a short leg

_INTENT_MAP = {
    SIDE_BTO: (OrderSide.BUY,  PositionIntent.BUY_TO_OPEN),
    SIDE_STC: (OrderSide.SELL, PositionIntent.SELL_TO_CLOSE),
    SIDE_STO: (OrderSide.SELL, PositionIntent.SELL_TO_OPEN),
    SIDE_BTC: (OrderSide.BUY,  PositionIntent.BUY_TO_CLOSE),
}


def _tif(tif: str) -> TimeInForce:
    t = tif.lower()
    return {
        "day": TimeInForce.DAY,
        "gtc": TimeInForce.GTC,
        "ioc": TimeInForce.IOC,
        "fok": TimeInForce.FOK,
        "opg": TimeInForce.OPG,
        "cls": TimeInForce.CLS,
    }.get(t, TimeInForce.DAY)


def submit_option_order(
    occ: str,
    qty: int,
    side: str = SIDE_BTO,
    order_type: str = "limit",
    limit_price: float | None = None,
    stop_price: float | None = None,
    trail_percent: float | None = None,
    trail_price: float | None = None,
    time_in_force: str = "day",
    extended_hours: bool = False,
    client_order_id: str | None = None,
) -> OrderResult:
    """
    Unified single-leg option order. Supports every order type Alpaca
    exposes and every BTO/STC/STO/BTC intent.

        side         : SIDE_BTO | SIDE_STC | SIDE_STO | SIDE_BTC
        order_type   : 'market' | 'limit' | 'stop' | 'stop_limit' | 'trailing_stop'
        time_in_force: 'day' | 'gtc' | 'ioc' | 'fok' | 'opg' | 'cls'

    For single-name options Alpaca rejects market orders for cash accounts;
    prefer 'limit' with a slightly-above-mid price (RISK['limit_price_
    midpoint_multiplier']).
    """
    if side not in _INTENT_MAP:
        raise BrokerError(f"unknown side {side!r}; use SIDE_BTO/STC/STO/BTC")
    tc = _trading_client()
    aside, intent = _INTENT_MAP[side]
    tif = _tif(time_in_force)
    common = dict(
        symbol=occ, qty=qty, side=aside,
        time_in_force=tif,
        extended_hours=extended_hours,
        position_intent=intent,
    )
    if client_order_id:
        common["client_order_id"] = client_order_id

    ot = order_type.lower()
    if ot == "market":
        req = MarketOrderRequest(**common)
    elif ot == "limit":
        if limit_price is None:
            raise BrokerError("limit order requires limit_price")
        req = LimitOrderRequest(limit_price=round(limit_price, 2), **common)
    elif ot == "stop":
        if stop_price is None:
            raise BrokerError("stop order requires stop_price")
        req = StopOrderRequest(stop_price=round(stop_price, 2), **common)
    elif ot == "stop_limit":
        if stop_price is None or limit_price is None:
            raise BrokerError("stop_limit needs stop_price + limit_price")
        req = StopLimitOrderRequest(
            stop_price=round(stop_price, 2),
            limit_price=round(limit_price, 2), **common,
        )
    elif ot == "trailing_stop":
        if trail_percent is None and trail_price is None:
            raise BrokerError("trailing_stop needs trail_percent or trail_price")
        kw = dict(common)
        if trail_percent is not None:
            kw["trail_percent"] = trail_percent
        else:
            kw["trail_price"] = round(trail_price, 2)
        req = TrailingStopOrderRequest(**kw)
    else:
        raise BrokerError(f"unsupported order_type {order_type!r}")

    try:
        o = tc.submit_order(req)
    except Exception as e:
        raise BrokerError(f"{side} {occ} x{qty} ({ot}) rejected: {e}")
    return _to_result(o)


# ── Multi-leg (spreads, straddles, strangles, condors, butterflies) ──────────
#
# Alpaca supports up to 4-leg option combos in a single atomic order via
# order_class=MLEG. Legs are declared per-leg with (symbol, ratio, side+intent).
# The top-level price is the NET debit (positive) or credit for the combo.
#
# Ratio quantity: each leg's qty = ratio_qty × top-level qty. For a standard
# vertical spread both legs have ratio_qty=1.

@dataclass
class OptionLeg:
    occ: str
    side: str              # SIDE_BTO | SIDE_STC | SIDE_STO | SIDE_BTC
    ratio_qty: int = 1

    def to_alpaca(self) -> Any:
        aside, intent = _INTENT_MAP[self.side]
        return OptionLegRequest(
            symbol=self.occ,
            ratio_qty=self.ratio_qty,
            side=aside,
            position_intent=intent,
        )


def submit_multileg_order(
    legs: list[OptionLeg],
    qty: int,
    order_type: str = "limit",
    net_price: float | None = None,  # debit positive, credit negative
    time_in_force: str = "day",
    client_order_id: str | None = None,
) -> OrderResult:
    """
    Submit a 2–4 leg option combo as one atomic order.

    Examples of valid structures:
        - Vertical spread (call debit, put debit, call credit, put credit)
        - Calendar / diagonal
        - Straddle, strangle
        - Iron condor, iron butterfly
        - Custom ratio / backspread (up to 4 legs)

    net_price is the NET debit (positive number for a debit spread, negative
    for a credit spread). For a $1.50 debit vertical: net_price=1.50.
    """
    if not 2 <= len(legs) <= 4:
        raise BrokerError(f"multi-leg requires 2–4 legs, got {len(legs)}")
    for lg in legs:
        if lg.side not in _INTENT_MAP:
            raise BrokerError(f"bad leg side {lg.side!r}")
    tc = _trading_client()
    tif = _tif(time_in_force)
    kw: dict[str, Any] = dict(
        qty=qty,
        order_class=OrderClass.MLEG,
        time_in_force=tif,
        legs=[lg.to_alpaca() for lg in legs],
    )
    if client_order_id:
        kw["client_order_id"] = client_order_id

    ot = order_type.lower()
    if ot == "market":
        req = MarketOrderRequest(**kw)
    elif ot == "limit":
        if net_price is None:
            raise BrokerError("multi-leg limit requires net_price (debit +, credit −)")
        req = LimitOrderRequest(limit_price=round(float(net_price), 2), **kw)
    else:
        raise BrokerError(
            f"multi-leg order_type {order_type!r} not supported "
            "(Alpaca accepts market or limit for combos)"
        )

    try:
        o = tc.submit_order(req)
    except Exception as e:
        raise BrokerError(f"multi-leg ({len(legs)} legs) rejected: {e}")
    return _to_result(o)


# ── Pre-built combo constructors (ergonomic wrappers) ────────────────────────
#
# Each returns a list[OptionLeg] you can hand to submit_multileg_order.
# Directions default to the most common retail use (debit long premium on
# the directional combos) — flip the sides for the credit variant.

def vertical_spread(long_occ: str, short_occ: str, qty: int = 1) -> list[OptionLeg]:
    """Debit vertical: long one strike + short another (same expiry, same type)."""
    return [OptionLeg(long_occ, SIDE_BTO), OptionLeg(short_occ, SIDE_STO)]


def calendar_spread(short_near_occ: str, long_far_occ: str) -> list[OptionLeg]:
    """Calendar/diagonal: sell near-term, buy longer-dated (same or different strike)."""
    return [OptionLeg(short_near_occ, SIDE_STO), OptionLeg(long_far_occ, SIDE_BTO)]


def straddle(call_occ: str, put_occ: str, long: bool = True) -> list[OptionLeg]:
    s = SIDE_BTO if long else SIDE_STO
    return [OptionLeg(call_occ, s), OptionLeg(put_occ, s)]


def strangle(otm_call_occ: str, otm_put_occ: str, long: bool = True) -> list[OptionLeg]:
    s = SIDE_BTO if long else SIDE_STO
    return [OptionLeg(otm_call_occ, s), OptionLeg(otm_put_occ, s)]


def iron_condor(long_put_occ: str, short_put_occ: str,
                short_call_occ: str, long_call_occ: str) -> list[OptionLeg]:
    """Credit iron condor: short strangle inside long strangle (defined risk)."""
    return [
        OptionLeg(long_put_occ,   SIDE_BTO),
        OptionLeg(short_put_occ,  SIDE_STO),
        OptionLeg(short_call_occ, SIDE_STO),
        OptionLeg(long_call_occ,  SIDE_BTO),
    ]


def iron_butterfly(long_put_occ: str, short_atm_put_occ: str,
                   short_atm_call_occ: str, long_call_occ: str) -> list[OptionLeg]:
    return [
        OptionLeg(long_put_occ,        SIDE_BTO),
        OptionLeg(short_atm_put_occ,   SIDE_STO),
        OptionLeg(short_atm_call_occ,  SIDE_STO),
        OptionLeg(long_call_occ,       SIDE_BTO),
    ]


def call_butterfly(low_occ: str, mid_occ: str, high_occ: str,
                   long: bool = True) -> list[OptionLeg]:
    """Long butterfly: +1 low, −2 mid, +1 high (ratio 1-2-1)."""
    outer, inner = (SIDE_BTO, SIDE_STO) if long else (SIDE_STO, SIDE_BTO)
    return [
        OptionLeg(low_occ,  outer, ratio_qty=1),
        OptionLeg(mid_occ,  inner, ratio_qty=2),
        OptionLeg(high_occ, outer, ratio_qty=1),
    ]


# ── Back-compat single-leg helpers ───────────────────────────────────────────
# The engine still calls buy_option/sell_option by name; keep them working.

def buy_option(occ: str, qty: int, limit_price: float | None = None,
               time_in_force: str = "day") -> OrderResult:
    """Back-compat: buy-to-open a long call/put. Prefer submit_option_order."""
    return submit_option_order(
        occ, qty, side=SIDE_BTO,
        order_type=("limit" if limit_price is not None else "market"),
        limit_price=limit_price, time_in_force=time_in_force,
    )


def sell_option(occ: str, qty: int, limit_price: float | None = None,
                time_in_force: str = "day") -> OrderResult:
    """Back-compat: sell-to-close an existing long. Prefer submit_option_order."""
    return submit_option_order(
        occ, qty, side=SIDE_STC,
        order_type=("limit" if limit_price is not None else "market"),
        limit_price=limit_price, time_in_force=time_in_force,
    )


def cancel_order(order_id: str) -> None:
    tc = _trading_client()
    try:
        tc.cancel_order_by_id(order_id)
    except Exception as e:
        raise BrokerError(f"cancel {order_id} failed: {e}")


def get_open_orders() -> list[OrderResult]:
    tc = _trading_client()
    req = GetOrdersRequest(status=OrderStatus.OPEN)
    try:
        orders = tc.get_orders(filter=req)
    except Exception as e:
        raise BrokerError(f"get_orders failed: {e}")
    return [_to_result(o) for o in orders]


# ── Clock / session ──────────────────────────────────────────────────────────

@dataclass
class ClockSnapshot:
    timestamp: datetime
    is_open: bool
    next_open: datetime | None
    next_close: datetime | None


def get_clock() -> ClockSnapshot:
    tc = _trading_client()
    c = tc.get_clock()
    return ClockSnapshot(
        timestamp=c.timestamp, is_open=c.is_open,
        next_open=c.next_open, next_close=c.next_close,
    )


# ── Smoke test when run as script ────────────────────────────────────────────

def main():
    print("Alpaca connectivity check…")
    try:
        acct = get_account()
    except BrokerError as e:
        print(f"  FAIL: {e}")
        return
    print(f"  Paper account: {acct.is_paper}")
    print(f"  Equity      : ${acct.equity:,.2f}")
    print(f"  Cash        : ${acct.cash:,.2f}")
    print(f"  Buying power: ${acct.buying_power:,.2f}")
    print(f"  Unsettled   : ${acct.unsettled:,.2f}")
    print(f"  Blocked     : {acct.account_blocked}")
    try:
        clk = get_clock()
        print(f"  Market open : {clk.is_open}  (next open {clk.next_open})")
    except BrokerError as e:
        print(f"  clock err: {e}")
    try:
        pos = get_positions()
        print(f"  Positions   : {len(pos)} open")
        for p in pos[:5]:
            print(f"    {p.symbol} x{p.qty} @ ${p.avg_entry:.2f} "
                  f"(mark ${p.mark:.2f}, P&L ${p.unrealized_pl:+.2f})")
    except BrokerError as e:
        print(f"  positions err: {e}")


if __name__ == "__main__":
    main()
