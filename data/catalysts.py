"""
Catalyst calendar — known event dates that matter for options pricing.

Currently covers:
  - FOMC meeting dates (Fed rate decisions)   — hardcoded through 2026
  - Earnings dates per ticker (via yfinance)  — best-effort, cached
  - Optional: FDA PDUFA dates                  — hook provided, populated manually

Why this matters:
  - IV is often elevated going into a catalyst and crushes after
  - An expiry that straddles an FOMC date has a different vol profile
  - Scorer can warn or down-weight trades that would be held through an event

Every call is defensive: returns empty list / None on failure.
"""

from __future__ import annotations

from datetime import datetime, date, timedelta
import time
import yfinance as yf

# ── FOMC 2025–2026 schedule (public) ──────────────────────────────────────────
# Source: federalreserve.gov. Update when Fed publishes 2027 calendar.
FOMC_DATES: list[date] = [
    date(2025, 1, 29),  date(2025, 3, 19),  date(2025, 5,  7),
    date(2025, 6, 18),  date(2025, 7, 30),  date(2025, 9, 17),
    date(2025, 11, 5),  date(2025, 12, 17),
    date(2026, 1, 28),  date(2026, 3, 18),  date(2026, 4, 29),
    date(2026, 6, 17),  date(2026, 7, 29),  date(2026, 9, 16),
    date(2026, 11, 4),  date(2026, 12, 16),
]

# ── FDA PDUFA calendar: populate ad-hoc for names you're actively trading ──
# Keys are upper-case tickers; each value is a list of {'date': ISO, 'desc': str}.
FDA_PDUFA: dict[str, list[dict]] = {
    # Example:
    # "MRNA": [{"date": "2026-07-14", "desc": "mRNA-1345 RSV vaccine"}],
}

CACHE_TTL_SEC = 3600  # 1 hour for earnings lookups
_earnings_cache: dict[str, tuple[float, date | None]] = {}


def next_fomc(from_date: date | None = None) -> date | None:
    """Next FOMC meeting on or after `from_date` (defaults to today)."""
    from_date = from_date or date.today()
    future = [d for d in FOMC_DATES if d >= from_date]
    return min(future) if future else None


def next_earnings(ticker: str) -> date | None:
    """
    Next earnings date for `ticker` via yfinance `calendar`. Cached 1h.
    Returns None on any failure.
    """
    ticker = ticker.upper().strip()
    now = time.time()
    cached = _earnings_cache.get(ticker)
    if cached and (now - cached[0]) < CACHE_TTL_SEC:
        return cached[1]

    result: date | None = None
    try:
        cal = yf.Ticker(ticker).calendar
        if cal is None:
            pass
        elif isinstance(cal, dict) and "Earnings Date" in cal:
            dates = cal["Earnings Date"]
            if isinstance(dates, list) and dates:
                result = _coerce_date(dates[0])
        elif hasattr(cal, "empty") and not cal.empty:
            row = cal.get("Earnings Date")
            if row is not None and len(row) > 0:
                result = _coerce_date(row.iloc[0])
    except Exception:
        result = None

    _earnings_cache[ticker] = (now, result)
    return result


def _coerce_date(value) -> date | None:
    try:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        # Pandas Timestamp or ISO string
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def upcoming_fda(ticker: str) -> list[dict]:
    """Return hardcoded upcoming FDA PDUFA events for `ticker`, if any."""
    entries = FDA_PDUFA.get(ticker.upper(), [])
    today = date.today()
    out = []
    for e in entries:
        try:
            d = datetime.fromisoformat(e["date"]).date()
            if d >= today:
                out.append({"date": d, "desc": e.get("desc", "FDA event")})
        except Exception:
            continue
    return sorted(out, key=lambda x: x["date"])


def catalysts_in_window(ticker: str, dte: int) -> dict:
    """
    Return catalyst events that fall within a given DTE window from today.

    Args:
        ticker: stock symbol
        dte:    days-to-expiry of the option in question

    Returns:
        {
          'has_catalyst': bool,
          'events': [ {kind, date, desc}, ...],
          'most_urgent_days': int | None,
          'summary': str,
        }
    """
    events = []
    today = date.today()
    horizon = today + timedelta(days=max(dte, 0))

    # FOMC
    fomc = next_fomc(today)
    if fomc and today <= fomc <= horizon:
        events.append({"kind": "FOMC", "date": fomc, "desc": "Fed rate decision"})

    # Earnings
    earn = next_earnings(ticker)
    if earn and today <= earn <= horizon:
        events.append({"kind": "EARNINGS", "date": earn, "desc": f"{ticker} earnings"})

    # FDA
    for e in upcoming_fda(ticker):
        if today <= e["date"] <= horizon:
            events.append({"kind": "FDA", "date": e["date"], "desc": e["desc"]})

    if not events:
        return {
            "has_catalyst":     False,
            "events":           [],
            "most_urgent_days": None,
            "summary":          "no catalysts in window",
        }

    events.sort(key=lambda e: e["date"])
    most_urgent = (events[0]["date"] - today).days
    summary_parts = [f"{e['kind']} in {(e['date']-today).days}d" for e in events]

    return {
        "has_catalyst":     True,
        "events":           [{**e, "date": e["date"].isoformat()} for e in events],
        "most_urgent_days": most_urgent,
        "summary":          "; ".join(summary_parts),
    }


def catalyst_score_delta(catalyst: dict | None, vol_signal: str) -> float:
    """
    Score adjustment based on catalyst presence in the contract's life.

    BUY VOL + catalyst present   → +4   (events = vol expansion)
    FLOW BUY + catalyst present  → +5   (flow often front-runs catalysts)
    Any signal + no catalyst      → 0

    Note: the `analyze_earnings_edge` module already handles earnings specifically;
    this catalyst_score_delta adds extra weight when NON-earnings events (FOMC,
    FDA) sit inside the window.
    """
    if not catalyst or not catalyst.get("has_catalyst"):
        return 0.0
    # Skip pure-earnings cases; those are handled by earnings_vol module.
    non_earnings = [e for e in catalyst.get("events", []) if e.get("kind") != "EARNINGS"]
    if not non_earnings:
        return 0.0
    if vol_signal == "FLOW BUY":
        return 5.0
    if vol_signal == "BUY VOL":
        return 4.0
    return 0.0
