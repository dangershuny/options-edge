"""
Live intraday news monitor.

Every `news_check_interval_seconds` (default 600s = 10 min) during market
hours, poll fresh headlines for each underlying we hold. If a material
article's sentiment runs *against* the position, surface an adverse-news
exit trigger — same machinery as SL/trailing/theta, same cash-account
same-day gating.

Direction rule:
  long CALL position  → bearish news is adverse
  long PUT position   → bullish news is adverse

Sentiment source:
  1. If the news-tool returns `sentiment` ∈ [-1, +1], use it directly.
  2. Otherwise fall back to keyword classification over title + summary.

Material filter:
  By default StockTwits / InvestorsHub (purely social) articles cannot fire
  an exit on their own (`news_ignore_social_only=True`). That threshold
  exists because retail chatter is noisy; acting on it alone leads to
  getting whipsawed out of positions that recover five minutes later. We
  still RECORD social articles so they're not re-evaluated next tick.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from data.news import get_news_since
from engine.state import (
    news_already_seen, record_news_seen, last_news_check_time,
)
from risk.config import RISK


# ── Keyword fallback ─────────────────────────────────────────────────────────
# Used only when the news-tool doesn't provide a numeric sentiment. Tuned
# toward event-driven language that actually moves a stock intraday — not
# the generic positive/negative words a general NLP model would catch.

_BEARISH = re.compile(
    r"\b("
    r"downgrade[ds]?|cuts?\s+(?:target|guidance|price)|"
    r"miss(?:es|ed)?\s+(?:estimate|earnings|expectations)|"
    r"guid(?:es|ed)\s+(?:lower|below|down)|warning|profit\s+warning|"
    r"lawsuit|suit\s+filed|class[- ]action|sec\s+probe|sec\s+investigation|"
    r"subpoena|doj|fraud|scandal|restat(?:e|ed|ement)|"
    r"recall|halt(?:ed)?|bankrupt(?:cy)?|chapter\s+11|"
    r"resign(?:s|ed)?|step[s]?\s+down|fire[ds]?|ousted|"
    r"plunge[ds]?|tumble[ds]?|slump[ds]?|crash(?:es|ed)?|"
    r"selloff|sell[- ]off|short\s+report|fda\s+(?:reject|crl)"
    r")\b",
    re.IGNORECASE,
)

_BULLISH = re.compile(
    r"\b("
    r"upgrade[ds]?|rais(?:es|ed)\s+(?:target|guidance|price)|"
    r"beat[s]?\s+(?:estimate|earnings|expectations)|"
    r"guid(?:es|ed)\s+(?:higher|above|up)|"
    r"record\s+(?:revenue|earnings|quarter)|blowout|"
    r"fda\s+(?:approv|clearance)|breakthrough|"
    r"acqui(?:re[ds]?|sition)|buyout|tender\s+offer|"
    r"buyback|dividend\s+(?:rais|increas)|"
    r"partnership|contract\s+win|major\s+order|"
    r"surge[ds]?|rall(?:y|ies|ied)|soars?|spikes?"
    r")\b",
    re.IGNORECASE,
)

_SOCIAL_SOURCES = {"StockTwits", "InvestorsHub"}


@dataclass
class NewsSignal:
    underlying: str
    articles: list[dict]      # fresh articles pulled this tick
    adverse_articles: list[dict]  # subset classified as against the position
    sentiment_score: float    # −1..+1 aggregate of fresh articles
    is_adverse: bool          # meets threshold + material-source rule


def _article_sentiment(article: dict) -> float:
    """Return a signed score in [-1, +1]. Uses news-tool score when given,
    else keyword fallback. Neutral → 0."""
    s = article.get("sentiment")
    if isinstance(s, (int, float)):
        return max(-1.0, min(1.0, float(s)))
    text = f"{article.get('title', '')} {article.get('summary', '')}"
    bear = bool(_BEARISH.search(text))
    bull = bool(_BULLISH.search(text))
    if bear and not bull:
        return -0.7
    if bull and not bear:
        return +0.7
    return 0.0


def _is_material(article: dict) -> bool:
    """Social-only sources don't count on their own if configured that way."""
    if not RISK.get("news_ignore_social_only", True):
        return True
    return article.get("source") not in _SOCIAL_SOURCES


def classify_for_position(articles: list[dict], option_type: str) -> NewsSignal:
    """
    option_type: 'call' or 'put' (the position we hold).
    Adverse direction flips: bearish articles hurt a long call, bullish
    articles hurt a long put.
    """
    cutoff = float(RISK.get("news_sentiment_adverse_cutoff", -0.45))
    min_n = int(RISK.get("news_require_min_articles", 1))
    is_call = option_type.lower().startswith("c")

    scored = [(_article_sentiment(a), a) for a in articles]
    # Aggregate — mean of article scores. Empty → 0.
    agg = sum(s for s, _ in scored) / len(scored) if scored else 0.0

    adverse: list[dict] = []
    for score, a in scored:
        against = (score <= cutoff) if is_call else (score >= -cutoff)
        if against and _is_material(a):
            adverse.append(a)

    return NewsSignal(
        underlying="",  # caller fills in
        articles=articles,
        adverse_articles=adverse,
        sentiment_score=agg,
        is_adverse=len(adverse) >= min_n,
    )


# ── Public entry point ───────────────────────────────────────────────────────

def check_position_news(position: dict) -> NewsSignal:
    """
    Pull fresh (never-seen) articles for this position's underlying, classify
    them, and persist them to `news_seen` so next tick skips them.

    `position` dict matches engine.state.list_open() row — needs at least
    'underlying' and 'option_type'.
    """
    underlying = position["underlying"]

    # Anchor "since" at the newest article we've already recorded, or
    # fallback to interval-ago so the first tick has something to compare.
    last = last_news_check_time(underlying)
    if last is None:
        last = datetime.now(tz=timezone.utc) - timedelta(
            seconds=int(RISK.get("news_check_interval_seconds", 600))
        )

    try:
        articles = get_news_since(underlying, last, limit=20)
    except Exception:
        articles = []

    # Filter out any articles we've already persisted (dedupe by link).
    fresh: list[dict] = []
    for a in articles:
        link = a.get("link")
        if not link:
            # No link → fall back to title+published for the dedupe key.
            link = f"__nolink__::{(a.get('title') or '')[:80]}::{a.get('published')}"
            a["link"] = link
        if news_already_seen(underlying, link):
            continue
        fresh.append(a)

    signal = classify_for_position(fresh, position["option_type"])
    signal.underlying = underlying

    # Record every fresh article — even neutral/non-adverse — so we
    # don't pay the classification cost next tick.
    for a in fresh:
        record_news_seen(underlying, a)

    return signal


def describe_signal(sig: NewsSignal, option_type: str) -> str:
    """Human-readable one-liner for the engine log."""
    if not sig.articles:
        return "no fresh news"
    direction = "bearish" if option_type.lower().startswith("c") else "bullish"
    if sig.is_adverse:
        top = sig.adverse_articles[0]
        title = (top.get("title") or "")[:80]
        return (f"ADVERSE ({direction} needed): {len(sig.adverse_articles)}/"
                f"{len(sig.articles)} articles, agg={sig.sentiment_score:+.2f} "
                f"— \"{title}\"")
    return (f"{len(sig.articles)} fresh articles, agg={sig.sentiment_score:+.2f}"
            f" (below adverse threshold)")


def news_check_due(last_check: datetime | None, now: datetime | None = None) -> bool:
    """Scheduler helper: has the interval elapsed since the last run?"""
    if last_check is None:
        return True
    now = now or datetime.now()
    interval = int(RISK.get("news_check_interval_seconds", 600))
    return (now - last_check).total_seconds() >= interval
