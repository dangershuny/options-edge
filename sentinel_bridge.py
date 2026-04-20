"""
HTTP client bridge between options-edge and the news_sentinel server (localhost:8502).

The sentinel server must be running: `python news_sentinel/server.py`
If offline, all calls return safe empty defaults — options-edge degrades gracefully.
"""

import json
from urllib.request import urlopen
from urllib.error import URLError

SENTINEL_URL = "http://localhost:8502"
TIMEOUT = 1.5  # fail fast; don't block a scan

_server_up: bool | None = None


def _probe() -> bool:
    global _server_up
    if _server_up is not None:
        return _server_up
    try:
        urlopen(f"{SENTINEL_URL}/health", timeout=TIMEOUT)
        _server_up = True
    except Exception:
        _server_up = False
    return _server_up


def _get(path: str) -> dict | None:
    if not _probe():
        return None
    try:
        with urlopen(f"{SENTINEL_URL}{path}", timeout=TIMEOUT) as r:
            return json.loads(r.read())
    except Exception:
        return None


def get_divergence(ticker: str) -> dict | None:
    """Returns the latest divergence event for ticker, or None."""
    return _get(f"/divergence?ticker={ticker.upper()}")


def divergence_score_adjustment(divergence: dict | None, vol_signal: str) -> float:
    """
    Returns score delta (-15 to +15) based on sentiment/vol alignment.

    bearish_divergence + SELL VOL → confirms overpriced options → +boost
    bearish_divergence + BUY VOL  → contradicts cheap-IV thesis → -penalty
    bullish_divergence + BUY VOL  → confirms cheap options in recovery → +boost
    bullish_divergence + SELL VOL → contradicts → -penalty
    """
    if not divergence or not divergence.get("direction"):
        return 0.0

    direction = divergence["direction"]
    div_score = float(divergence.get("divergence_score", 0))
    strength = min(div_score / 1.5, 1.0)
    max_delta = 15.0

    if direction == "bearish_divergence":
        if vol_signal == "SELL VOL":
            return round(strength * max_delta, 1)
        elif vol_signal == "BUY VOL":
            return round(-strength * max_delta, 1)
    elif direction == "bullish_divergence":
        if vol_signal == "BUY VOL":
            return round(strength * max_delta, 1)
        elif vol_signal == "SELL VOL":
            return round(-strength * max_delta, 1)
    return 0.0


def sentinel_status() -> str:
    if _server_up is None:
        return "not checked"
    return "connected" if _server_up else "offline"
