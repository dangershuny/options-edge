"""
HTTP client bridge between options-edge and the news_sentinel server (localhost:8502).

`ensure_sentinel_running()` auto-launches the server if it's not already up.
If it cannot be launched or stays unreachable, calls return None — scoring
proceeds without sentiment and a warning is surfaced to the user.
"""

import json
import os
import subprocess
import sys
import time
from urllib.request import urlopen

SENTINEL_URL  = "http://localhost:8502"
TIMEOUT       = 1.5           # per-request timeout for cheap GETs (health, /divergence)
SCAN_TIMEOUT  = 45.0          # /scan does full news+social fetch; allow longer
STARTUP_WAIT  = 10.0          # seconds to wait for a freshly-launched server
POLL_INTERVAL = 0.5

# Resolve news_sentinel/server.py next to this repo (sibling under Claude Projects).
SENTINEL_DIR = os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "OneDrive", "Documents", "Claude Projects", "news_sentinel",
))
SENTINEL_SERVER_SCRIPT = os.path.join(SENTINEL_DIR, "server.py")

_server_up: bool | None = None
_launch_attempted = False
_last_error: str | None = None


def _ping(timeout: float = TIMEOUT) -> bool:
    try:
        urlopen(f"{SENTINEL_URL}/health", timeout=timeout)
        return True
    except Exception:
        return False


def _launch_server() -> bool:
    """Start news_sentinel server in background. Returns True if it came up."""
    global _last_error
    if not os.path.exists(SENTINEL_SERVER_SCRIPT):
        _last_error = f"server.py not found at {SENTINEL_SERVER_SCRIPT}"
        return False
    try:
        creationflags = 0
        if sys.platform == "win32":
            # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP — survive parent exit, no console window
            creationflags = 0x00000008 | 0x00000200
        subprocess.Popen(
            [sys.executable, SENTINEL_SERVER_SCRIPT],
            cwd=SENTINEL_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
            close_fds=True,
        )
    except Exception as e:
        _last_error = f"failed to launch server: {e}"
        return False

    # Poll until healthy or timeout
    deadline = time.time() + STARTUP_WAIT
    while time.time() < deadline:
        if _ping(timeout=0.5):
            return True
        time.sleep(POLL_INTERVAL)
    _last_error = f"server launched but did not respond within {STARTUP_WAIT}s"
    return False


def ensure_sentinel_running() -> bool:
    """
    Ensure the sentinel server is reachable. Call this once at scan start.
    If already up → no-op. If down → attempt launch, wait for health.
    Subsequent calls are cached (cheap).
    """
    global _server_up, _launch_attempted, _last_error

    if _server_up is True:
        return True

    if _ping():
        _server_up = True
        return True

    if _launch_attempted:
        # Already tried this session; don't keep retrying.
        _server_up = False
        return False

    _launch_attempted = True
    if _launch_server():
        _server_up = True
        return True
    _server_up = False
    return False


def _probe() -> bool:
    """Legacy probe — now defers to ensure_sentinel_running()."""
    return ensure_sentinel_running()


def _get(path: str, timeout: float = TIMEOUT) -> dict | None:
    if not _probe():
        return None
    try:
        with urlopen(f"{SENTINEL_URL}{path}", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def get_divergence(ticker: str, max_age_hours: int = 24) -> dict | None:
    """
    Returns the latest FRESH divergence event for ticker, or None.

    Server filters by flagged_at >= now - max_age_hours so a stale divergence
    flagged days ago can't keep scoring today's trades. Default 24h.
    """
    return _get(f"/divergence?ticker={ticker.upper()}&max_age_hours={max_age_hours}")


def scan_ticker(ticker: str) -> dict | None:
    """
    Trigger a fresh news_sentinel scan for `ticker`. Blocks until complete.
    Uses a longer timeout since /scan does full news+social+Claude analysis.
    """
    return _get(f"/scan?ticker={ticker.upper()}", timeout=SCAN_TIMEOUT)


def _freshness_multiplier(divergence: dict | None) -> float:
    """
    Fresh signals get higher weight. Pre-market divergences (flagged < 3h ago)
    are most valuable — they captured overnight news the market hasn't fully
    priced yet. As events age, their informational edge decays.

    < 3h  (pre-market / first 90m of session) : 1.30x — full freshness bonus
    < 6h  (late morning same day)              : 1.10x
    < 12h (same trading day)                   : 1.00x — baseline
    < 24h (yesterday's signal)                 : 0.80x — mostly priced in
    ≥ 24h                                      : 0.60x — stale
    """
    if not divergence:
        return 1.0
    ts = divergence.get("flagged_at")
    if not ts:
        return 1.0
    try:
        from datetime import datetime, timezone
        # flagged_at stored as ISO UTC
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:
        return 1.0
    if age_hours < 3:   return 1.30
    if age_hours < 6:   return 1.10
    if age_hours < 12:  return 1.00
    if age_hours < 24:  return 0.80
    return 0.60


def divergence_score_adjustment(divergence: dict | None, vol_signal: str,
                                 option_type: str | None = None) -> float:
    """
    Returns score delta (-20 to +20) based on sentiment/vol/direction alignment.

    Direction matters: bearish news on a PUT is aligned (boost), bearish news
    on a CALL is contradicted (penalty).

    Freshness matters: divergences flagged during pre-market have the highest
    weight (1.3x), decaying to 0.6x by 24h. This makes pre-market sentinel
    signals more impactful — they caught news before the market priced it in.

    bullish_divergence (news bullish, stock under-reacted):
        BUY VOL CALL   → aligned: cheap call + bullish thesis     → +boost
        BUY VOL PUT    → contradicted: cheap put vs bullish news  → -penalty
        SELL VOL *     → contradicted: stock should move, not bleed → -penalty

    bearish_divergence (news bearish, stock under-reacted):
        BUY VOL PUT    → aligned: cheap put + bearish thesis      → +boost
        BUY VOL CALL   → contradicted: cheap call vs bearish news → -penalty
        SELL VOL *     → contradicted: stock should move          → -penalty

    If option_type is not provided we fall back to the older symmetric logic
    so old callers (e.g. tests) don't break — but the scorer now always
    passes option_type.
    """
    if not divergence or not divergence.get("direction"):
        return 0.0

    direction = divergence["direction"]
    div_score = float(divergence.get("divergence_score", 0))
    strength = min(div_score / 1.5, 1.0)
    freshness = _freshness_multiplier(divergence)
    max_delta = 15.0 * freshness
    ot = (option_type or "").lower()

    # Direction-aware path (preferred)
    if ot in ("call", "put"):
        if direction == "bullish_divergence":
            if vol_signal == "BUY VOL" and ot == "call":
                return round(strength * max_delta, 1)
            if vol_signal == "BUY VOL" and ot == "put":
                return round(-strength * max_delta, 1)
            if vol_signal == "SELL VOL":
                return round(-strength * max_delta, 1)
        elif direction == "bearish_divergence":
            if vol_signal == "BUY VOL" and ot == "put":
                return round(strength * max_delta, 1)
            if vol_signal == "BUY VOL" and ot == "call":
                return round(-strength * max_delta, 1)
            if vol_signal == "SELL VOL":
                return round(-strength * max_delta, 1)
        return 0.0

    # Legacy path (option_type unknown) — preserve old behaviour
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


def prewarm_universe(tickers: list[str], timeout_total: float = 300.0,
                      per_ticker_timeout: float = SCAN_TIMEOUT) -> dict:
    """
    Bulk-scan the universe at scan start. If the sentinel's pre-market cron
    has already scanned these tickers today, /scan is ~instant (cached). If
    not, this populates divergence events BEFORE analyze_ticker reads them,
    avoiding per-ticker blocking on expensive sentinel calls during the main
    scan.

    Returns {"scanned": N, "skipped": N, "errors": N, "elapsed_sec": float,
             "tickers_with_divergence": [list]}.
    Safe on offline sentinel — returns zeros.
    """
    if not ensure_sentinel_running():
        return {"scanned": 0, "skipped": len(tickers), "errors": 0,
                "elapsed_sec": 0.0, "tickers_with_divergence": [],
                "sentinel": "offline"}

    import time
    t0 = time.time()
    hits = []
    scanned = errors = 0
    for sym in tickers:
        if time.time() - t0 > timeout_total:
            break
        try:
            result = _get(f"/scan?ticker={sym.upper()}", timeout=per_ticker_timeout)
            if result is None:
                errors += 1
                continue
            scanned += 1
            # If scan produced a divergence event, note it
            if result.get("direction") or (result.get("divergence") or {}).get("direction"):
                hits.append(sym.upper())
        except Exception:
            errors += 1

    return {
        "scanned": scanned,
        "skipped": max(0, len(tickers) - scanned - errors),
        "errors": errors,
        "elapsed_sec": round(time.time() - t0, 1),
        "tickers_with_divergence": hits,
        "sentinel": "connected",
    }


def sentinel_status() -> str:
    if _server_up is None:
        return "not checked"
    return "connected" if _server_up else "offline"


def sentinel_last_error() -> str | None:
    """Reason the last launch/probe failed, if any."""
    return _last_error
