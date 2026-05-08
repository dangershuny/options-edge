"""
Market-regime gate — block entries that fight the broader index move.

Built 2026-05-06 after 5/5 took 3 puts (BNTX/GM/WFC) on a rally day; all
three hit SL same session. The puts may have had directionally-correct
divergence signals over a longer horizon, but on an SPY rally day the
intraday momentum steamrolled them inside hours.

Asymmetric gate (2026-05-08): operator removed the CALL soft-block.
Reasoning: a mildly-down SPY day still produces decent individual
long-side setups (sector rotations, single-name news, etc.). Don't
filter out an entire signal class because the index drifted -0.5%.

Gate logic (defaults; overridable via RISK):

    SPY intraday return        | calls allowed?  | puts allowed?
    ---------------------------+-----------------+----------------
    +1.0% or more              | yes             | NO (block)
    +0.3% to +1.0%             | yes             | only if div ≥ STRONG_DIV
    -0.3% to +0.3% (chop)      | yes             | yes
    -0.3% to -1.0%             | YES (no block)  | yes
    -1.0% or worse             | NO (block)      | yes

The CALL HARD block at SPY ≤ -1% is retained — at that magnitude
selloffs are usually market-wide and individual long-side setups tend
to follow. PUT soft+hard blocks unchanged (rally days still penalize
shorts), though `block_puts=True` in RISK currently overrides anyway.

"Intraday return" = (last_price / prev_close) - 1, freshly fetched at
entry-decision time (cached for 60s so repeated calls within a tier loop
don't hammer the broker).

API:
    regime = current_regime()                # dict with spy_pct, qqq_pct, label
    ok, reason = check(opt_type, divergence_score=None)   # gate

`divergence_score` is the raw score from sentinel (typical range 0..2.5).
Passing it lets a STRONG divergence override a soft regime-warn — but
nothing overrides a hard block.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

# Thresholds (overridable via RISK dict if user wants finer tuning later)
SOFT_BLOCK_PCT = 0.003   # 0.3% — opposing puts/calls require strong divergence
HARD_BLOCK_PCT = 0.010   # 1.0% — opposing puts/calls blocked outright
STRONG_DIV_SCORE = 1.5   # divergence_score above this overrides SOFT block

CACHE_TTL_SEC = 60       # SPY/QQQ quote cache TTL


@dataclass
class Regime:
    spy_pct: float
    qqq_pct: float
    label: str          # "STRONG_RALLY" | "RALLY" | "CHOP" | "SELLOFF" | "STRONG_SELLOFF"
    fetched_at: float


_cache: Optional[Regime] = None


def _label(spy_pct: float) -> str:
    if spy_pct >= HARD_BLOCK_PCT:
        return "STRONG_RALLY"
    if spy_pct >= SOFT_BLOCK_PCT:
        return "RALLY"
    if spy_pct <= -HARD_BLOCK_PCT:
        return "STRONG_SELLOFF"
    if spy_pct <= -SOFT_BLOCK_PCT:
        return "SELLOFF"
    return "CHOP"


def _intraday_pct(symbol: str) -> Optional[float]:
    """Return intraday % change vs prior close. None on any failure.

    Tries broker first (Alpaca latest quote + bar history), falls back to
    yfinance. Both are slow — caller MUST cache results."""
    # Try Alpaca via the broker module
    try:
        from broker import alpaca
        q = alpaca.get_stock_quote(symbol) if hasattr(alpaca, "get_stock_quote") else None
        last_px = None
        if q is not None:
            last_px = (getattr(q, "ask_price", None) or
                       getattr(q, "bid_price", None) or
                       getattr(q, "last_price", None))
        if last_px is None:
            # Try last trade
            t = alpaca.get_stock_latest_trade(symbol) if hasattr(alpaca, "get_stock_latest_trade") else None
            if t is not None:
                last_px = getattr(t, "price", None)
        # Prior close from a 2-day daily bar
        if last_px:
            try:
                bars = alpaca.get_stock_bars(symbol, timeframe="1Day", limit=3) \
                    if hasattr(alpaca, "get_stock_bars") else None
                if bars and len(bars) >= 2:
                    prev_close = float(bars[-2].close)
                    if prev_close:
                        return (float(last_px) / prev_close) - 1
            except Exception:
                pass
    except Exception:
        pass

    # Fallback: yfinance
    try:
        import yfinance as yf
        hist = yf.Ticker(symbol).history(period="3d", interval="1d", auto_adjust=False)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        closes = hist["Close"].dropna().tolist()
        if len(closes) < 2:
            return None
        # Latest tick via 1m bar
        intra = yf.Ticker(symbol).history(period="1d", interval="1m", auto_adjust=False)
        if intra is None or intra.empty or "Close" not in intra.columns:
            last = closes[-1]
        else:
            last = float(intra["Close"].dropna().iloc[-1])
        prev_close = float(closes[-2]) if len(closes) >= 2 else float(closes[-1])
        if not prev_close:
            return None
        return (last / prev_close) - 1
    except Exception:
        return None


def current_regime(force_refresh: bool = False) -> Regime:
    """Return cached or fresh Regime. Cache TTL 60s.
    Falls back to a CHOP label (0% / 0%) if both data sources fail — we'd
    rather under-block than over-block on a transient data hiccup."""
    global _cache
    now = time.time()
    if (not force_refresh and _cache is not None
            and now - _cache.fetched_at < CACHE_TTL_SEC):
        return _cache
    spy = _intraday_pct("SPY")
    qqq = _intraday_pct("QQQ")
    spy = 0.0 if spy is None else spy
    qqq = 0.0 if qqq is None else qqq
    r = Regime(spy_pct=spy, qqq_pct=qqq, label=_label(spy), fetched_at=now)
    _cache = r
    return r


def check(opt_type: str,
          divergence_score: Optional[float] = None) -> tuple[bool, str]:
    """Gate an entry by current SPY intraday move.
    Returns (allowed, reason).

    opt_type: 'call' | 'put'
    divergence_score: optional sentinel divergence_score (raw, ~0..2.5).
        A STRONG signal can override a SOFT block (rally-day put with
        divergence ≥ STRONG_DIV_SCORE is permitted). Hard blocks are never
        overridable.
    """
    ot = (opt_type or "").lower()
    if ot not in ("call", "put"):
        return True, "non-directional"
    r = current_regime()
    spy = r.spy_pct
    div = float(divergence_score or 0.0)

    # Hard blocks (no override)
    if ot == "put" and spy >= HARD_BLOCK_PCT:
        return False, (f"regime hard-block: PUT vs SPY +{spy*100:.2f}% "
                       f"(>= {HARD_BLOCK_PCT*100:.2f}%)")
    if ot == "call" and spy <= -HARD_BLOCK_PCT:
        return False, (f"regime hard-block: CALL vs SPY {spy*100:+.2f}% "
                       f"(<= -{HARD_BLOCK_PCT*100:.2f}%)")

    # Soft blocks (override allowed if divergence is strong).
    # 2026-05-08: CALL soft-block removed at operator's direction. A mild
    # SPY drawdown shouldn't filter out the entire long-side signal set —
    # individual setups can still rip. The CALL hard block at SPY ≤ -1%
    # above is retained for catastrophic regimes where everything follows.
    if ot == "put" and spy >= SOFT_BLOCK_PCT:
        if div >= STRONG_DIV_SCORE:
            return True, (f"regime soft-warn (SPY +{spy*100:.2f}%) "
                          f"OVERRIDDEN by div_score={div:.2f}")
        return False, (f"regime soft-block: PUT vs SPY +{spy*100:.2f}% "
                       f"and divergence {div:.2f} < {STRONG_DIV_SCORE}")

    return True, f"regime ok (SPY {spy*100:+.2f}%, label={r.label})"


def describe() -> str:
    """One-line summary for logs."""
    r = current_regime()
    return f"SPY {r.spy_pct*100:+.2f}% QQQ {r.qqq_pct*100:+.2f}% [{r.label}]"
