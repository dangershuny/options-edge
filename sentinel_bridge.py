"""
Bridge between options-edge and news_sentinel.

Triggers a sentinel scan for a ticker and returns divergence + article data.
Results are cached in sentinel.db — rescans only if data is older than CACHE_MINUTES.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

SENTINEL_PATH = Path(__file__).parent.parent / "news_sentinel"
CACHE_MINUTES = 30

# Add news_sentinel to import path
if str(SENTINEL_PATH) not in sys.path:
    sys.path.insert(0, str(SENTINEL_PATH))


def _is_fresh(fetched_at_iso: str, max_age_minutes: int = CACHE_MINUTES) -> bool:
    try:
        fetched = datetime.fromisoformat(fetched_at_iso).replace(tzinfo=timezone.utc)
        return (datetime.now(tz=timezone.utc) - fetched) < timedelta(minutes=max_age_minutes)
    except Exception:
        return False


def scan_ticker(ticker: str) -> dict:
    """
    Scans ticker with news_sentinel if cache is stale.
    Returns:
        {
            divergence: Row | None,
            articles: list[Row],
            social: list[Row],
            scanned: bool,
        }
    """
    ticker = ticker.upper()
    result = {"divergence": None, "articles": [], "social": [], "scanned": False}

    try:
        from database import init_db, query_latest_divergence, query_articles, query_social
        init_db()

        # Check if we have fresh data
        div = query_latest_divergence(ticker)
        articles = query_articles(ticker, limit=10)
        social = query_social(ticker, limit=10)

        needs_scan = True
        if div and articles and _is_fresh(div["flagged_at"]):
            needs_scan = False
        elif articles and _is_fresh(articles[0]["fetched_at"]):
            needs_scan = False

        if needs_scan:
            from main import cmd_scan
            cmd_scan([ticker])
            div = query_latest_divergence(ticker)
            articles = query_articles(ticker, limit=10)
            social = query_social(ticker, limit=10)
            result["scanned"] = True

        result["divergence"] = div
        result["articles"] = list(articles)
        result["social"] = list(social)

    except Exception as e:
        print(f"  [sentinel_bridge] {ticker}: {e}")

    return result


def divergence_score_adjustment(divergence_row, vol_signal: str) -> float:
    """
    Returns a score delta (-15 to +15) based on whether the sentiment divergence
    aligns with or contradicts the options vol signal.

    bearish_divergence + SELL VOL → sentiment confirms expensive options → +boost
    bearish_divergence + BUY VOL  → sentiment contradicts cheap-IV thesis → -penalty
    bullish_divergence + BUY VOL  → sentiment supports upside move thesis → +boost
    bullish_divergence + SELL VOL → sentiment contradicts → -penalty
    """
    if divergence_row is None:
        return 0.0

    direction = divergence_row["direction"]
    div_score = float(divergence_row["divergence_score"])
    # normalize to 0-1 (max realistic divergence ~1.5)
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
