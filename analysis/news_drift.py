"""
News-drift edge — the tool's founding thesis made concrete.

Core idea: after a material headline or strong sentiment signal, the stock
tends to keep drifting in the news direction for days (post-earnings-
announcement drift, PEAD). If the market has only partially reacted, the
*residual* drift is the edge. We size that residual, decay it by time since
the headline, and turn it into a per-contract score delta.

This replaces the crude `divergence_score_adjustment` as the primary
sentiment input. Divergence stays as a fallback when no event classifies.

Pipeline:
  1. classify_articles(articles)      → tag each article with event category
  2. measure_reaction(symbol, event)  → price-at-headline vs price-now
  3. residual_drift(event, reaction)  → how much move is still unpriced
  4. news_drift_delta(events, ...)    → final score delta for the scorer

Event baselines (typical day-1 pop, typical 5-day drift, half-life in days)
come from PEAD literature + retail-options experience. They're conservative
and meant to be recalibrated from your own `snapshots/` + `chain_surface`
history once you have enough samples. See `tools/calibrate_news_drift.py`
(pending) for that loop.

All functions are SAFE-DEFAULT: missing data → zero delta, never an exception
that bubbles up into the scorer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── Event catalogue ──────────────────────────────────────────────────────────
# Each entry:
#   direction:      +1 = bullish event, -1 = bearish
#   expected_pct:   typical TOTAL move magnitude (day-1 + drift combined)
#   half_life_h:    hours over which the *unrealized* drift decays toward 0
#   confidence:     0..1 — how reliable this category's drift is historically
#   min_drift_pct:  expected residual drift (on top of day-1 reaction)
#
# Numbers are conservative starting points. The calibration pipeline can
# tighten them once we've logged 30+ events per category.

@dataclass(frozen=True)
class EventSpec:
    category:      str
    direction:     int
    expected_pct:  float   # total expected move (absolute %)
    min_drift_pct: float   # expected residual drift on top of initial pop
    half_life_h:   float
    confidence:    float


_CATALOGUE: dict[str, EventSpec] = {
    # Strong-signal earnings
    "earnings_beat_guide_raise": EventSpec("earnings_beat_guide_raise", +1, 5.5, 2.0, 72, 0.9),
    "earnings_miss_guide_cut":   EventSpec("earnings_miss_guide_cut",   -1, 6.5, 2.5, 72, 0.9),
    "earnings_beat":             EventSpec("earnings_beat",             +1, 2.0, 0.8, 36, 0.7),
    "earnings_miss":             EventSpec("earnings_miss",             -1, 3.5, 1.2, 48, 0.8),
    "guidance_raise":            EventSpec("guidance_raise",            +1, 4.0, 1.5, 60, 0.8),
    "guidance_cut":              EventSpec("guidance_cut",              -1, 5.0, 2.0, 60, 0.85),

    # Analyst actions — smaller, faster-decaying
    "analyst_upgrade":           EventSpec("analyst_upgrade",           +1, 2.0, 0.8, 36, 0.55),
    "analyst_downgrade":         EventSpec("analyst_downgrade",         -1, 2.5, 1.0, 36, 0.6),

    # Regulatory / binary events
    "fda_approval":              EventSpec("fda_approval",              +1, 20.0, 5.0, 96, 0.9),
    "fda_reject":                EventSpec("fda_reject",                -1, 25.0, 6.0, 96, 0.95),

    # M&A / corporate action
    "acquisition_target":        EventSpec("acquisition_target",        +1, 15.0, 3.0, 72, 0.85),
    "buyback_new":               EventSpec("buyback_new",               +1, 1.5, 0.6, 36, 0.5),
    "dividend_raise":            EventSpec("dividend_raise",            +1, 1.0, 0.4, 24, 0.45),

    # Business events
    "contract_win":              EventSpec("contract_win",              +1, 3.0, 1.2, 48, 0.65),
    "recall":                    EventSpec("recall",                    -1, 3.5, 1.5, 60, 0.65),
    "lawsuit_major":             EventSpec("lawsuit_major",             -1, 4.0, 1.5, 72, 0.65),
    "executive_departure":       EventSpec("executive_departure",       -1, 3.0, 1.2, 48, 0.55),
    "short_report":              EventSpec("short_report",              -1, 6.0, 2.0, 96, 0.7),

    # Social / retail sentiment — kept separate, weaker signal
    "social_bullish_surge":      EventSpec("social_bullish_surge",      +1, 3.0, 1.0, 12, 0.35),
    "social_bearish_surge":      EventSpec("social_bearish_surge",      -1, 3.0, 1.0, 12, 0.35),
}


# ── Classifier ───────────────────────────────────────────────────────────────
# Regex-based — fast, zero-deps, deterministic. Ordering matters: more specific
# patterns go first so "beat + raise" doesn't get caught by the generic "beat".

_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("earnings_beat_guide_raise", re.compile(
        r"\bbeat(?:s|ing)?\b.*\b(rais(?:e[ds]?|ing)|lift[s]?|hike[s]?|boost[s]?)\b.*?\b(guid|outlook|forecast)", re.I)),
    ("earnings_miss_guide_cut", re.compile(
        r"\bmiss(?:es|ed)?\b.*\b(cut[s]?|lower[s]?|slash[es]?|reduc[es]?|trim[s]?)\b.*?\b(guid|outlook|forecast)", re.I)),
    ("guidance_raise", re.compile(
        r"\b(rais(?:es|ed|ing)|lift[s]?|hike[s]?|boost[s]?)\s+(?:full[- ]year\s+)?(?:guidance|outlook|forecast|fy\s*guid)\b", re.I)),
    ("guidance_cut", re.compile(
        r"\b(cut[s]?|slash[es]?|lower[s]?|reduc[es]?|trim[s]?)\s+(?:full[- ]year\s+)?(?:guidance|outlook|forecast)\b|profit\s+warning|warns?\s+on\s+(?:revenue|profit|earnings)", re.I)),
    ("earnings_beat", re.compile(
        r"\bbeat[s]?\b.*?\b(estimate|earnings|expectations|revenue|eps|consensus)|tops?\s+(?:estimate|forecast)|q[1-4]\s+beat", re.I)),
    ("earnings_miss", re.compile(
        r"\bmiss(?:es|ed)?\b.*?\b(estimate|earnings|expectations|revenue|eps|consensus)|short(?:falls?)?\s+of\s+(?:estimate|forecast)", re.I)),

    ("fda_approval", re.compile(
        r"\bfda\s+(?:approval|approve[ds]?|clearance|grants?\s+approval|nod)|breakthrough\s+designation", re.I)),
    ("fda_reject", re.compile(
        r"\bfda\s+(?:reject|denied|crl|complete\s+response\s+letter)|clinical\s+hold|trial\s+fail", re.I)),

    ("acquisition_target", re.compile(
        r"\bto\s+be\s+acquired|acquir(?:es|ing)\s+\w+\s+for|tender\s+offer|buyout\s+offer|take[- ]private", re.I)),
    ("buyback_new", re.compile(
        r"\b(announces?|authorizes?|approves?)\s+\$?\d*\.?\d*\s*[bm]?\s*(?:share\s+)?(?:buyback|repurchase)", re.I)),
    ("dividend_raise", re.compile(
        r"\b(rais(?:es|ed)|increas(?:es|ed)|boost(?:s|ed)?|hikes?)\s+(?:quarterly\s+)?dividend", re.I)),

    ("analyst_upgrade", re.compile(
        r"\bupgrade[ds]?\s+(?:to\s+)?(?:buy|outperform|overweight|strong\s+buy)|price\s+target\s+rais", re.I)),
    ("analyst_downgrade", re.compile(
        r"\bdowngrade[ds]?\s+(?:to\s+)?(?:sell|underperform|underweight|hold)|price\s+target\s+(?:cut|lower)", re.I)),

    ("contract_win", re.compile(
        r"\b(wins?|awarded|secures?|lands?)\b.*?\b(contract|deal|order|agreement)\b", re.I)),
    ("recall", re.compile(
        r"\brecall[s]?\s+\d|issues?\s+recall|safety\s+recall|voluntary\s+recall", re.I)),
    ("lawsuit_major", re.compile(
        r"\bclass[- ]action|sec\s+(?:probe|investigation|charges)|doj\s+(?:probe|investigation)|fraud\s+(?:charges|suit)|subpoena", re.I)),
    ("executive_departure", re.compile(
        r"\b(ceo|cfo|coo|president|chairman)\b.*?\b(resign[s]?|resigned|stepp?ing\s+down|stepp?ed\s+down|step[s]?\s+down|depart(?:s|ed|ing|ure)?|fired|ousted|leaves?|leaving)\b|unexpected(?:ly)?\s+(?:depart|resign|leav)", re.I)),
    ("short_report", re.compile(
        r"\b(hindenburg|muddy\s+waters|citron|kerrisdale|spruce\s+point|culper)|short\s+(?:report|seller\s+target)", re.I)),
]


def classify_article(title: str, summary: str = "") -> Optional[str]:
    """
    Return an event category if the article matches one of our patterns,
    else None. Title is weighted higher than summary (more specific).
    """
    if not title:
        return None
    blob_title = title.strip()
    blob_sum = (summary or "").strip()
    for cat, pat in _PATTERNS:
        if pat.search(blob_title) or pat.search(blob_sum):
            return cat
    return None


def classify_articles(articles: list[dict]) -> list[dict]:
    """
    Enrich each article with 'event_category' + 'event_spec' if classifiable.
    Returns the same list (mutated). Un-classified articles are left as-is.
    """
    out = []
    for a in articles or []:
        cat = classify_article(a.get("title", ""), a.get("summary", ""))
        enriched = dict(a)
        if cat and cat in _CATALOGUE:
            enriched["event_category"] = cat
            enriched["event_spec"] = _CATALOGUE[cat]
        out.append(enriched)
    return out


# ── Reaction measurement ─────────────────────────────────────────────────────

def _price_at(prices_df, ts: datetime) -> float | None:
    """
    Given a DataFrame of OHLCV indexed by timestamp (pandas), return the
    close of the bar at/just-before `ts`. None if unavailable.
    """
    if prices_df is None or prices_df.empty:
        return None
    try:
        import pandas as pd
        # Normalize index tz so we can compare
        idx = prices_df.index
        if hasattr(idx, "tz") and idx.tz is None:
            ts = ts.replace(tzinfo=None)
        # Clamp ts to range
        if ts < idx[0]:
            return float(prices_df.iloc[0]["Close"])
        if ts > idx[-1]:
            return float(prices_df.iloc[-1]["Close"])
        # Find nearest bar ≤ ts
        mask = idx <= ts
        if not mask.any():
            return float(prices_df.iloc[0]["Close"])
        return float(prices_df[mask].iloc[-1]["Close"])
    except Exception:
        return None


def measure_reaction(symbol: str, event_ts: datetime,
                     price_now: float) -> dict | None:
    """
    Returns {'price_at_event', 'pct_move_since', 'hours_elapsed'} or None
    if we can't measure. Uses 1-minute intraday bars when ≤5 days old,
    falls back to daily close for older events.
    """
    if event_ts is None or price_now <= 0:
        return None
    now = datetime.now(timezone.utc)
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=timezone.utc)
    elapsed_h = max(0.0, (now - event_ts).total_seconds() / 3600.0)

    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        if elapsed_h <= 24 * 5:
            # 1-minute bars (yfinance limits 1m to last 7d)
            df = t.history(period="7d", interval="1m")
        else:
            df = t.history(period="1mo", interval="1d")
    except Exception:
        return None

    p_event = _price_at(df, event_ts)
    if p_event is None or p_event <= 0:
        return None

    pct = (price_now - p_event) / p_event * 100.0
    return {
        "price_at_event":  round(p_event, 4),
        "pct_move_since":  round(pct, 3),
        "hours_elapsed":   round(elapsed_h, 2),
    }


# ── Residual drift ───────────────────────────────────────────────────────────

def residual_drift(spec: EventSpec, pct_move_since: float,
                   hours_elapsed: float) -> dict:
    """
    Core math: how much directional drift is left?

      signed_move   = pct_move_since × direction   (positive if stock has
                                                    moved WITH the event)
      consumed      = max(0, signed_move)
      remaining_pct = max(0, expected_pct − consumed)   # cap at 0 so we
                                                        # don't chase over-
                                                        # reactions (those
                                                        # are a different
                                                        # signal: mean-revert)
      decayed       = remaining_pct × 2^(−hours / half_life_h)
      adjusted      = decayed × confidence × direction

    Returns dict with diagnostic fields the scorer can log.
    """
    signed_move = pct_move_since * spec.direction
    consumed = max(0.0, signed_move)
    remaining = max(0.0, spec.expected_pct - consumed)
    # Decay
    decay = 2.0 ** (-hours_elapsed / max(spec.half_life_h, 1.0))
    decayed = remaining * decay
    # Floor at the minimum-drift spec (drift that's expected AFTER initial pop)
    # — even if initial pop was full, the PEAD literature says some drift continues.
    min_residual = spec.min_drift_pct * decay
    effective = max(decayed, min_residual * 0.5)  # soft floor
    adjusted = effective * spec.confidence * spec.direction
    return {
        "expected_pct":   spec.expected_pct,
        "consumed_pct":   round(consumed, 3),
        "remaining_pct":  round(remaining, 3),
        "decay_factor":   round(decay, 3),
        "decayed_pct":    round(decayed, 3),
        "adjusted":       round(adjusted, 3),   # signed, + = bullish drift left
        "confidence":     spec.confidence,
        "direction":      spec.direction,
    }


# ── Score delta for the scorer ───────────────────────────────────────────────
#
# Mapping residual → delta:
#   residual = 0.5pct  → barely   → ~1 pt
#   residual = 2.0pct  → decent   → ~5 pts
#   residual = 5.0pct  → strong   → ~12 pts
#   residual = 10pct+  → huge     → capped at 15
#
# Formula: delta_magnitude = min(abs(adjusted) * 2.5, 15)
# Sign determined by option_type vs direction alignment.

MAX_DELTA = 15.0

def _delta_from_residual(adjusted_residual: float,
                         vol_signal: str, option_type: str) -> float:
    direction = 1 if adjusted_residual > 0 else (-1 if adjusted_residual < 0 else 0)
    if direction == 0:
        return 0.0
    magnitude = min(abs(adjusted_residual) * 2.5, MAX_DELTA)

    ot = (option_type or "").lower()
    aligned_call = direction > 0 and ot == "call"
    aligned_put  = direction < 0 and ot == "put"
    aligned = aligned_call or aligned_put

    # BUY VOL + aligned direction → full boost
    # BUY VOL + mis-aligned → half penalty (we're buying vol that fights news)
    # SELL VOL → opposite logic (we'd be selling vol into a moving event — bad)
    if vol_signal == "BUY VOL" or vol_signal == "FLOW BUY":
        return round(magnitude if aligned else -magnitude * 0.5, 1)
    if vol_signal == "SELL VOL":
        return round(-magnitude if aligned else magnitude * 0.5, 1)
    return 0.0


@dataclass
class NewsDriftResult:
    delta:        float = 0.0
    best_event:   Optional[dict] = None
    events_used:  list[dict] = field(default_factory=list)
    note:         str = ""


def news_drift_delta(articles: list[dict], symbol: str,
                      vol_signal: str, option_type: str,
                      price_now: float) -> NewsDriftResult:
    """
    Primary entry-point the scorer calls. Safe-defaults everywhere — any
    failure in data / classification / yfinance just returns delta=0.

    `articles` should come from data.news.get_news(symbol). We do the
    classification here so callers don't need to know the schema.
    """
    if not articles or price_now <= 0:
        return NewsDriftResult(note="no articles or price")

    try:
        classified = classify_articles(articles)
    except Exception as e:
        return NewsDriftResult(note=f"classify failed: {e}")

    events = [a for a in classified if a.get("event_category")]
    if not events:
        return NewsDriftResult(note="no classifiable events")

    # Take only events with a timestamp — can't measure reaction without it.
    # Allow up to 5 events so we can combine (e.g. upgrade + buyback).
    timed = [e for e in events if e.get("published")][:5]
    if not timed:
        return NewsDriftResult(note="events had no timestamps")

    total_delta = 0.0
    top: tuple[float, dict] | None = None
    used: list[dict] = []

    for ev in timed:
        spec: EventSpec = ev["event_spec"]
        try:
            reaction = measure_reaction(symbol, ev["published"], price_now)
        except Exception:
            reaction = None
        if reaction is None:
            continue
        rd = residual_drift(spec, reaction["pct_move_since"],
                            reaction["hours_elapsed"])
        delta = _delta_from_residual(rd["adjusted"], vol_signal, option_type)

        ev_summary = {
            "title":         ev.get("title"),
            "category":      spec.category,
            "published":     ev["published"].isoformat() if ev.get("published") else None,
            "hours_elapsed": reaction["hours_elapsed"],
            "pct_move_since": reaction["pct_move_since"],
            "residual":      rd,
            "delta":         delta,
        }
        used.append(ev_summary)
        total_delta += delta
        if top is None or abs(delta) > abs(top[0]):
            top = (delta, ev_summary)

    # Cap cumulative delta
    total_delta = max(-20.0, min(20.0, total_delta))
    return NewsDriftResult(
        delta=round(total_delta, 1),
        best_event=top[1] if top else None,
        events_used=used,
        note=f"classified {len(events)} / timed {len(timed)} / used {len(used)}",
    )
