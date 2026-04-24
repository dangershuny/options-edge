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


def get_divergence(ticker: str, max_age_hours: int = 24,
                    strategy: str | None = None) -> dict | None:
    """
    Returns the latest FRESH divergence event for ticker, or None.

    When `strategy` is provided, uses Tier-2 tunable thresholds appropriate
    for that trading style. Otherwise uses the server's default (options-
    trading profile, stored-event fast path):

      strategy="cheap_vol" (default for BUY VOL)   — tight thresholds,
                                                     require strong disagreement.
      strategy="directional"                        — looser + detect_convergence,
                                                     catches continuation too.
      strategy="reversion"                          — wide + detect_convergence,
                                                     flags overreactions to fade.

    Server filters by flagged_at >= now - max_age_hours. Default 24h.
    """
    url = f"/divergence?ticker={ticker.upper()}&max_age_hours={max_age_hours}"
    if strategy == "directional":
        url += ("&market_bullish=0.2&market_bearish=-0.2"
                "&news_bullish=0.03&news_bearish=-0.03&detect_convergence=1")
    elif strategy == "reversion":
        url += ("&market_bullish=0.15&market_bearish=-0.15"
                "&news_bullish=0.03&news_bearish=-0.03&detect_convergence=1")
    return _get(url)


def get_sentiment(ticker: str, hours: int = 24,
                   weights: str | None = None) -> dict | None:
    """
    Weighted composite sentiment over the last N hours.

    Returns a dict with components (news_avg, social_avg, haiku) and a
    composite_score from -1.0 to +1.0. More informative than /divergence
    alone — we get the raw sentiment even when there's no divergence event.

    weights override format: "news:0.7,social:0.3". Defaults: 0.5/0.3/0.2.
    """
    q = f"/sentiment?ticker={ticker.upper()}&hours={hours}"
    if weights:
        q += f"&weights={weights}"
    return _get(q)


def get_sentiment_series(ticker: str, days: int = 5,
                          bucket: str = "hourly") -> dict | None:
    """
    Time-bucketed sentiment for trend/velocity analysis.

    bucket: "hourly" or "daily". Spans up to 30 days (archive + live).
    Used for the sentiment-velocity signal: is news momentum accelerating?
    """
    return _get(f"/sentiment/series?ticker={ticker.upper()}&days={days}&bucket={bucket}")


def get_catalysts(ticker: str, hours_8k: int = 72) -> dict | None:
    """
    Standalone catalyst signals — recent 8-K SEC filings + market context.

    Useful independently of divergence: a fresh 8-K is a catalyst even
    when sentiment hasn't diverged from analyst ratings yet.
    """
    return _get(f"/catalysts?ticker={ticker.upper()}&hours_8k={hours_8k}")


def get_attention(ticker: str) -> dict | None:
    """
    Retail attention level — primarily WSB mention count.

    Spikes in retail attention can lead/lag real moves depending on regime.
    """
    return _get(f"/attention?ticker={ticker.upper()}")


def composite_sentiment_delta(sentiment: dict | None, vol_signal: str,
                               opt_type: str | None,
                               has_divergence: bool) -> float:
    """
    Small sentiment tilt for contracts with no divergence event.

    When a divergence is present, div_delta already captures the sentiment
    signal (± up to ~20 points). When absent, this helper provides a gentle
    tilt (±4 max) from the raw composite score — so even quiet names with
    mildly bullish or bearish sentiment nudge the score.

    Suppresses itself when has_divergence=True (prevents double-counting).
    Requires ≥8 news articles in the window for statistical weight.
    """
    if has_divergence or not sentiment:
        return 0.0
    composite = sentiment.get("composite_score")
    sample = sentiment.get("sample_sizes", {}) or {}
    if composite is None:
        return 0.0
    if (sample.get("news") or 0) < 8:
        return 0.0

    ot = (opt_type or "").lower()
    if ot not in ("call", "put"):
        return 0.0

    # Normalize composite (-1..+1) to a sign + magnitude, cap at ±4
    direction_score = float(composite)
    if abs(direction_score) < 0.10:   # too close to zero — no signal
        return 0.0

    # Aligned: bullish composite + call, or bearish composite + put
    aligned = ((direction_score > 0 and ot == "call") or
               (direction_score < 0 and ot == "put"))
    magnitude = min(abs(direction_score) * 8.0, 4.0)  # cap at ±4
    # For BUY VOL / BUY-path signals only — SELL VOL spreads have different logic
    if vol_signal in ("BUY VOL", "FLOW BUY", "DIRECTIONAL BUY",
                       "MOMENTUM BUY"):
        return round(magnitude if aligned else -magnitude, 1)
    if vol_signal == "REVERSION BUY":
        # REVERSION trades AGAINST momentum — flip the alignment logic
        return round(magnitude if not aligned else -magnitude, 1)
    return 0.0


def fresh_8k_delta(catalysts: dict | None, vol_signal: str,
                    opt_type: str | None) -> float:
    """
    Score delta for recent 8-K filings (unplanned catalysts).

    8-Ks represent material corporate events — acquisitions, management
    changes, guidance revisions, etc. Distinct from scheduled earnings:
    the existing `catalyst_score_delta` handles earnings proximity; this
    handles "something just happened that markets may not have absorbed."

    Returns:
      +6 on any long-option BUY path (call or put) when a fresh 8-K hits —
         the event creates vol expansion opportunity regardless of direction.
      0 when no fresh 8-K.

    The scorer's other directional signals (insider, drift, convergence)
    determine whether the 8-K is bullish or bearish. We just upweight the
    whole setup because the catalyst is real and recent.
    """
    if not catalysts:
        return 0.0
    if not catalysts.get("has_recent_8k"):
        return 0.0
    count = int(catalysts.get("recent_8k_count") or 0)
    if count < 1:
        return 0.0
    if vol_signal not in ("BUY VOL", "FLOW BUY", "DIRECTIONAL BUY",
                           "MOMENTUM BUY", "REVERSION BUY"):
        return 0.0
    # Multiple 8-Ks in 72h = cluster of news → bigger bump, capped at +9
    return float(min(count * 3, 9))


def sentiment_velocity(ticker: str, window_hours: int = 12) -> float | None:
    """
    Rate-of-change of news sentiment over the last N hours.

    Returns a float: positive = accelerating bullish, negative = accelerating
    bearish, near-zero = flat. None if insufficient data.

    Computed as: (avg of last half of window) - (avg of first half of window).
    Magnitude bounded by the underlying sentiment range [-1, +1], so values
    outside ±0.4 are unusually strong.
    """
    days = max(1, (window_hours + 23) // 24)
    series = get_sentiment_series(ticker, days=days, bucket="hourly")
    if not series:
        return None
    rows = [r for r in (series.get("series") or []) if r.get("news_avg") is not None]
    if len(rows) < 4:
        return None
    rows = rows[-window_hours:] if len(rows) > window_hours else rows
    mid = len(rows) // 2
    first = [r["news_avg"] for r in rows[:mid]]
    second = [r["news_avg"] for r in rows[mid:]]
    if not first or not second:
        return None
    return round((sum(second) / len(second)) - (sum(first) / len(first)), 3)


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
        # Convergence (both market + news agree strongly) — weaker signal than
        # divergence but confirms the direction. Scaled to 0.6× because
        # consensus is less actionable than contradiction.
        elif direction == "bullish_convergence":
            conv_delta = max_delta * 0.6
            if ot == "call" and vol_signal in ("BUY VOL", "MOMENTUM BUY",
                                                "DIRECTIONAL BUY"):
                return round(strength * conv_delta, 1)
            # For REVERSION BUY of puts (fading an up-move), convergence at
            # overbought levels actually strengthens the fade case.
            if ot == "put" and vol_signal == "REVERSION BUY":
                return round(strength * conv_delta, 1)
            if ot == "put" and vol_signal == "BUY VOL":
                return round(-strength * conv_delta, 1)
        elif direction == "bearish_convergence":
            conv_delta = max_delta * 0.6
            if ot == "put" and vol_signal in ("BUY VOL", "MOMENTUM BUY",
                                               "DIRECTIONAL BUY"):
                return round(strength * conv_delta, 1)
            if ot == "call" and vol_signal == "REVERSION BUY":
                return round(strength * conv_delta, 1)
            if ot == "call" and vol_signal == "BUY VOL":
                return round(-strength * conv_delta, 1)
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
