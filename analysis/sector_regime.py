"""
Sector-regime dampener.

Problem: scorer evaluates each ticker independently. But heavily-correlated
sectors (crypto miners moving with BTC, EV makers moving with TSLA) produce
correlated signals — when 9 crypto miners all flag bullish on a day BTC is
dropping, those 9 "independent" bets are 1 correlated bet against the
sector trend.

Today's evidence: MARA, APLD, GLXY all flagged BUY VOL CALL → all dropped
~4% with BTC. 3 wrong-direction trades from 1 sector-level miss.

Fix: identify sector members and a leader proxy. If a contract's signal
direction CONTRADICTS the sector trend, apply a dampener delta. If the
trend strongly supports the signal, apply a small confirmation bonus.

Sector regime is computed from a sector "leader" — for crypto, BTC-USD;
for EV, TSLA. Sectors with no clean leader are skipped.

Cached per scan run (single yfinance call per leader).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import yfinance as yf

# ── Sector membership ───────────────────────────────────────────────────────

CRYPTO_MINERS = {
    "MARA", "RIOT", "HUT", "HIVE", "IREN", "GLXY", "APLD", "CLSK",
    "BTBT", "BTDR", "WULF", "CIFR", "MSTR", "COIN",
}
# COIN/MSTR are crypto-adjacent rather than miners but track the same
# regime. Including them so an entire crypto-bullish day boost is consistent.

EV_MAKERS = {"LCID", "RIVN", "NIO", "XPEV"}
# TSLA is also EV but is the leader, not a follower.

CLEAN_ENERGY = {"PLUG", "QS", "RUN", "SEDG", "ENPH", "FSLR",
                "OKLO", "SMR", "EOSE", "CHPT", "EVGO", "BLNK"}
# No clean leader (no single ticker drives clean energy moves like BTC drives
# crypto). Skip dampener for now — kept here for documentation/future use.


# ── Leaders & cache ─────────────────────────────────────────────────────────

LEADER_BTC = "BTC-USD"
LEADER_TSLA = "TSLA"

_LEADER_LOOKBACK_DAYS = 3     # how far back to compute trend
_BULL_THRESHOLD = 0.020       # 2%+ over lookback = bullish regime
_BEAR_THRESHOLD = -0.020      # -2% over lookback = bearish regime
# Tuned for catching rapid reversals: today (2026-04-27) BTC was -0.95% over
# 5 days but -3% over 3 days, while crypto miners all dropped 4-5%. The
# longer/looser combo missed it; this combo catches it without being too
# noisy on day-to-day chop.

_DAMPENER_OPPOSED = -12.0     # signal direction opposes sector trend
_DAMPENER_ALIGNED = 4.0       # signal direction aligned with sector trend

_CACHE_TTL_SEC = 1800   # 30 min — sector trend doesn't change quickly
_cache: dict[str, tuple[float, float | None]] = {}   # leader -> (epoch, pct_return)


def _leader_return(leader: str) -> float | None:
    """5-day return of the leader. Cached. Returns None on yfinance failure."""
    now = time.time()
    cached = _cache.get(leader)
    if cached and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]
    try:
        df = yf.download(
            leader,
            period=f"{_LEADER_LOOKBACK_DAYS + 2}d",
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
        if df is None or df.empty:
            _cache[leader] = (now, None)
            return None
        closes = df["Close"]
        if hasattr(closes, "columns") and leader in closes.columns:
            closes = closes[leader]
        closes = closes.dropna()
        if len(closes) < 2:
            _cache[leader] = (now, None)
            return None
        first = float(closes.iloc[0])
        last = float(closes.iloc[-1])
        if first == 0:
            _cache[leader] = (now, None)
            return None
        pct = (last - first) / first
        _cache[leader] = (now, pct)
        return pct
    except Exception:
        _cache[leader] = (now, None)
        return None


def _regime_from_return(pct: float | None) -> str:
    if pct is None:
        return "UNKNOWN"
    if pct >= _BULL_THRESHOLD:
        return "BULLISH"
    if pct <= _BEAR_THRESHOLD:
        return "BEARISH"
    return "NEUTRAL"


# ── Public API ──────────────────────────────────────────────────────────────

def get_sector_for(symbol: str) -> str | None:
    sym = symbol.upper()
    if sym in CRYPTO_MINERS:
        return "crypto"
    if sym in EV_MAKERS:
        return "ev"
    return None


def get_sector_regime(sector: str) -> dict:
    """Return regime info for a sector. Always returns a dict; never raises."""
    if sector == "crypto":
        pct = _leader_return(LEADER_BTC)
        return {
            "sector": sector,
            "leader": LEADER_BTC,
            "leader_return_5d": pct,
            "regime": _regime_from_return(pct),
        }
    if sector == "ev":
        pct = _leader_return(LEADER_TSLA)
        return {
            "sector": sector,
            "leader": LEADER_TSLA,
            "leader_return_5d": pct,
            "regime": _regime_from_return(pct),
        }
    return {"sector": sector, "leader": None,
            "leader_return_5d": None, "regime": "UNKNOWN"}


def sector_dampener_delta(symbol: str, opt_type: str,
                           vol_signal: str) -> tuple[float, dict | None]:
    """
    Score delta from sector regime alignment.

    Returns (delta, regime_info_dict_or_None).
      delta = -12 if opt direction opposes sector trend (e.g. BUY CALL on a
                  crypto miner when BTC is dropping)
              +4  if opt direction aligns with sector trend (small bonus —
                  the underlying current is helping)
              0   if regime is UNKNOWN/NEUTRAL or ticker isn't in a tracked
                  sector or vol_signal isn't a buy-side path

    Skips SELL VOL signals — those generate credit spreads where the
    direction logic is inverted. Sector dampener only applies to long-
    premium buys (BUY VOL, FLOW BUY, DIRECTIONAL BUY, MOMENTUM BUY,
    REVERSION BUY).
    """
    if vol_signal not in ("BUY VOL", "FLOW BUY", "DIRECTIONAL BUY",
                          "MOMENTUM BUY", "REVERSION BUY"):
        return 0.0, None

    sector = get_sector_for(symbol)
    if not sector:
        return 0.0, None

    info = get_sector_regime(sector)
    regime = info.get("regime", "UNKNOWN")
    if regime in ("UNKNOWN", "NEUTRAL"):
        return 0.0, info

    ot = (opt_type or "").lower()
    if ot not in ("call", "put"):
        return 0.0, info

    # REVERSION BUY trades AGAINST sector momentum by design — flip the
    # alignment logic so reversion calls in a bearish sector regime are
    # treated as ALIGNED (we're fading the down-move).
    is_reversion = vol_signal == "REVERSION BUY"

    aligned: bool
    if regime == "BULLISH":
        aligned = (ot == "call") if not is_reversion else (ot == "put")
    else:  # BEARISH
        aligned = (ot == "put") if not is_reversion else (ot == "call")

    delta = _DAMPENER_ALIGNED if aligned else _DAMPENER_OPPOSED
    return delta, info
