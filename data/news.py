import feedparser
from datetime import datetime, timedelta, timezone


YAHOO_FEED = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"


def get_news(ticker: str, max_age_days: int = 5, limit: int = 6) -> list[dict]:
    url = YAHOO_FEED.format(ticker=ticker)
    feed = feedparser.parse(url)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)
    articles = []

    for entry in feed.entries[:limit * 2]:
        published = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            if published < cutoff:
                continue

        articles.append({
            "title": entry.get("title", "").strip(),
            "link": entry.get("link", ""),
            "published": published,
            "summary": (entry.get("summary", "") or "")[:250].strip(),
        })

        if len(articles) >= limit:
            break

    return articles
