"""
News data for options-edge.

Primary:  calls the news-tool API (http://localhost:8502) if it's running.
Fallback: fetches multiple free RSS/feed sources if the news tool is offline:
  1. Yahoo Finance headlines      (institutional news, earnings, macro)
  2. Yahoo Finance conversations  (retail sentiment from comment boards)
  3. InvestorsHub                 (penny stocks + active trader discussion)
  4. Seeking Alpha                (analysis community, early thesis formation)
  5. StockTwits RSS               (real-time trader micro-posts)

Social sources like InvestorsHub and StockTwits often surface retail
thesis formation BEFORE it shows up in headline news — useful for
catching momentum before it's priced in.

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
from urllib.request import urlopen, Request
from urllib.error import URLError

NEWS_TOOL_URL = "http://localhost:8502"
_TIMEOUT = 1.5  # seconds — fail fast, don't slow down a scan

# Cached per session: None=untested, True=up, False=down
_news_tool_up: bool | None = None

# Feed templates — {ticker} is replaced at call time
_FEEDS: list[dict] = [
    {
        "name": "yahoo_finance",
        "url":  "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US",
        "source_label": "Yahoo Finance",
    },
    {
        "name": "seeking_alpha",
        "url":  "https://seekingalpha.com/api/sa/combined/{ticker}.xml",
        "source_label": "Seeking Alpha",
    },
    {
        "name": "stocktwits_rss",
        "url":  "https://stocktwits.com/symbol/{ticker}/rss",
        "source_label": "StockTwits",
    },
    {
        "name": "investorshub",
        # InvestorsHub has a board per ticker; the board ID varies but the search feed works
        "url":  "https://investorshub.advfn.com/rss/rss.aspx?type=boardalert&id={ticker}",
        "source_label": "InvestorsHub",
    },
]


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


def _fetch_feed(url: str, source_label: str, max_age_days: int, limit: int) -> list[dict]:
    """Parse a single RSS/Atom feed and return normalised article dicts."""
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 options-edge/1.0"})
        with urlopen(req, timeout=3) as r:
            raw = r.read()
        feed = feedparser.parse(raw)
    except Exception:
        return []

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)
    out = []

    for entry in feed.entries[: limit * 3]:
        pub = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            if pub < cutoff:
                continue

        title = (entry.get("title") or "").strip()
        if not title:
            continue

        out.append({
            "title":     title,
            "link":      entry.get("link") or "",
            "published": pub,
            "summary":   (entry.get("summary") or "")[:250].strip(),
            "sentiment": None,
            "source":    source_label,
        })
        if len(out) >= limit:
            break

    return out


def _call_rss_multi(ticker: str, max_age_days: int, limit: int) -> list[dict]:
    """
    Pull from all configured feed sources, deduplicate by title,
    return the freshest `limit` articles sorted by recency.
    """
    combined: list[dict] = []
    per_source = max(2, limit // len(_FEEDS))

    for feed_cfg in _FEEDS:
        url = feed_cfg["url"].format(ticker=ticker)
        articles = _fetch_feed(url, feed_cfg["source_label"], max_age_days, per_source)
        combined.extend(articles)

    # Deduplicate by lowercased title
    seen: set[str] = set()
    deduped: list[dict] = []
    for a in combined:
        key = a["title"].lower()[:60]
        if key not in seen:
            seen.add(key)
            deduped.append(a)

    # Sort by recency (None published goes last)
    deduped.sort(
        key=lambda x: x["published"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return deduped[:limit]


def get_news(ticker: str, max_age_days: int = 5, limit: int = 6) -> list[dict]:
    """
    Return up to `limit` recent articles for `ticker`.

    Tries the news-tool API first (includes sentiment scoring).
    Falls back to multi-source RSS aggregation if offline.
    """
    if _probe():
        result = _call_news_tool(ticker, limit)
        if result is not None:
            return result
    return _call_rss_multi(ticker, max_age_days, limit)


def news_tool_status() -> str:
    if _news_tool_up is None:
        return "not checked"
    return "connected" if _news_tool_up else "offline — multi-source RSS fallback"
