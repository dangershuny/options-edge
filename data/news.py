"""
News data for options-edge.

Primary:  calls the news-tool API (http://localhost:8502) if it's running.
Fallback: fetches Yahoo Finance RSS directly if the news tool is offline.

Contract the news tool must satisfy:
  GET /health                         -> any 200 response
  GET /news?ticker=AAPL&limit=6       -> JSON body:
  {
    "ticker": "AAPL",
    "articles": [
      {
        "title":     "...",
        "link":      "https://...",
        "published": "2026-04-19T10:00:00+00:00",  // ISO-8601 or null
        "summary":   "...",
        "sentiment": 0.65   // float -1..1, or null
      }
    ],
    "sentiment_label": "bullish" | "bearish" | "neutral" | null
  }
"""

import json
import feedparser
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen
from urllib.error import URLError

NEWS_TOOL_URL = "http://localhost:8502"
_TIMEOUT = 1.5  # seconds — fail fast, don't slow down a scan

# Cached per session: None=untested, True=up, False=down
_news_tool_up: bool | None = None


def _probe() -> bool:
    global _news_tool_up
    if _news_tool_up is not None:
        return _news_tool_up
    try:
        urlopen(f"{NEWS_TOOL_URL}/health", timeout=_TIMEOUT)
        _news_tool_up = True
    except Exception:
        _news_tool_up = False
    return _news_tool_up


def _call_news_tool(ticker: str, limit: int) -> list[dict] | None:
    try:
        url = f"{NEWS_TOOL_URL}/news?ticker={ticker}&limit={limit}"
        with urlopen(url, timeout=_TIMEOUT) as r:
            data = json.loads(r.read())
        out = []
        for a in data.get("articles", [])[:limit]:
            pub = a.get("published")
            out.append({
                "title":     (a.get("title") or "").strip(),
                "link":      a.get("link") or "",
                "published": datetime.fromisoformat(pub) if pub else None,
                "summary":   (a.get("summary") or "")[:250].strip(),
                "sentiment": a.get("sentiment"),
                "source":    "news-tool",
            })
        return out
    except Exception:
        return None


_YAHOO = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"


def _call_rss(ticker: str, max_age_days: int, limit: int) -> list[dict]:
    feed = feedparser.parse(_YAHOO.format(ticker=ticker))
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)
    out = []
    for entry in feed.entries[:limit * 2]:
        pub = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            if pub < cutoff:
                continue
        out.append({
            "title":     (entry.get("title") or "").strip(),
            "link":      entry.get("link") or "",
            "published": pub,
            "summary":   (entry.get("summary") or "")[:250].strip(),
            "sentiment": None,
            "source":    "rss",
        })
        if len(out) >= limit:
            break
    return out


def get_news(ticker: str, max_age_days: int = 5, limit: int = 6) -> list[dict]:
    if _probe():
        result = _call_news_tool(ticker, limit)
        if result is not None:
            return result
    return _call_rss(ticker, max_age_days, limit)


def news_tool_status() -> str:
    if _news_tool_up is None:
        return "not checked"
    return "connected" if _news_tool_up else "offline — using RSS fallback"
