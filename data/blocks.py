"""
Block-trade / unusual-volume proxy.

True dark-pool prints aren't available on free data feeds. But *unusual
stock volume* combined with a price move in one direction is a very close
proxy — it's what most institutional activity shows up as on the tape.

Heuristic:
  - Pull 30-day average daily volume from yfinance
  - Compare today's volume to that average
  - If volume > 2x avg AND price moved > 1% → flag as institutional
  - Direction = sign of price move

Safe on any failure.
"""

from __future__ import annotations

import time
import yfinance as yf

CACHE_TTL_SEC = 600   # 10 min
_cache: dict[str, tuple[float, dict]] = {}


def get_unusual_volume(ticker: str) -> dict:
    """
    Returns:
        {
          'ticker': str,
          'volume_ratio':  float | None   — today vs 30-day avg
          'price_change_pct': float | None
          'signal':        'INSTITUTIONAL_BUY' | 'INSTITUTIONAL_SELL' |
                           'ACCUMULATION' | 'DISTRIBUTION' | 'NORMAL' | 'UNKNOWN'
          'summary':       str
          'source':        'yfinance' | 'degraded'
        }
    """
    ticker = ticker.upper().strip()
    now = time.time()
    cached = _cache.get(ticker)
    if cached and (now - cached[0]) < CACHE_TTL_SEC:
        return cached[1]

    result = _degraded(ticker, "unknown")
    try:
        hist = yf.Ticker(ticker).history(period="31d")
        if hist is None or hist.empty or len(hist) < 10:
            _cache[ticker] = (now, result)
            return result

        today_vol  = float(hist["Volume"].iloc[-1])
        avg_vol    = float(hist["Volume"].iloc[:-1].mean())
        if avg_vol <= 0 or today_vol <= 0:
            _cache[ticker] = (now, result)
            return result

        vol_ratio  = today_vol / avg_vol
        close_px   = float(hist["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2])
        pct_change = (close_px - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

        # Classify
        if vol_ratio >= 2.5 and pct_change >= 1.5:
            signal = "INSTITUTIONAL_BUY"
        elif vol_ratio >= 2.5 and pct_change <= -1.5:
            signal = "INSTITUTIONAL_SELL"
        elif vol_ratio >= 1.5 and pct_change >= 0.5:
            signal = "ACCUMULATION"
        elif vol_ratio >= 1.5 and pct_change <= -0.5:
            signal = "DISTRIBUTION"
        else:
            signal = "NORMAL"

        summary = f"vol {vol_ratio:.1f}× avg, px {pct_change:+.2f}%"

        result = {
            "ticker":           ticker,
            "volume_ratio":     round(vol_ratio, 2),
            "price_change_pct": round(pct_change, 2),
            "signal":           signal,
            "summary":          summary,
            "source":           "yfinance",
        }
    except Exception as e:
        result = _degraded(ticker, f"error: {e}")

    _cache[ticker] = (now, result)
    return result


def blocks_score_delta(blocks: dict | None, opt_type: str, vol_signal: str) -> float:
    """
    Score adjustment from unusual-volume classification.

    INSTITUTIONAL_BUY  + BUY CALL → +6
    ACCUMULATION       + BUY CALL → +3
    INSTITUTIONAL_SELL + BUY PUT  → +6
    DISTRIBUTION       + BUY PUT  → +3
    Direction mismatch             → -3
    Otherwise                      → 0
    """
    if not blocks or not isinstance(blocks, dict):
        return 0.0
    if vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    sig = blocks.get("signal")
    opt = (opt_type or "").lower()

    call_bull_map = {"INSTITUTIONAL_BUY": 6.0, "ACCUMULATION": 3.0}
    put_bear_map  = {"INSTITUTIONAL_SELL": 6.0, "DISTRIBUTION": 3.0}

    if opt == "call" and sig in call_bull_map:
        return call_bull_map[sig]
    if opt == "put" and sig in put_bear_map:
        return put_bear_map[sig]
    # Opposing direction
    if opt == "call" and sig in ("INSTITUTIONAL_SELL", "DISTRIBUTION"):
        return -3.0
    if opt == "put" and sig in ("INSTITUTIONAL_BUY", "ACCUMULATION"):
        return -3.0
    return 0.0


def _degraded(ticker: str, reason: str) -> dict:
    return {
        "ticker":           ticker,
        "volume_ratio":     None,
        "price_change_pct": None,
        "signal":           "UNKNOWN",
        "summary":          f"block data unavailable ({reason})",
        "source":           "degraded",
    }
