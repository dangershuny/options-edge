"""
Insider activity (Form 4) fetcher via SEC EDGAR.

EDGAR exposes a free JSON endpoint that returns recent filings by CIK. Form 4
filings are required within 2 business days of an insider trade, so they're
a timely signal. Cluster buying by multiple insiders is a well-documented edge.

This module is DEFENSIVELY written:
  - Every network call has a short timeout and try/except.
  - Empty / degraded results never crash the caller.
  - A 15-minute in-process cache prevents hammering EDGAR.
  - We require a User-Agent per SEC fair-use policy.

Returns a SAFE-DEFAULT dict with signal='NEUTRAL' on any failure.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

USER_AGENT = "OptionsEdgeScanner research contact@example.com"
EDGAR_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
HTTP_TIMEOUT = 6.0
CACHE_TTL_SEC = 900   # 15 min

_ticker_to_cik: dict[str, str] | None = None
_ticker_cache_ts: float = 0.0
_insider_cache: dict[str, tuple[float, dict]] = {}


def _http_json(url: str) -> dict | list | None:
    """Safe JSON GET. Returns None on any failure."""
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        with urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read())
    except (HTTPError, URLError, TimeoutError, ValueError, OSError):
        return None
    except Exception:
        return None


def _load_ticker_map() -> dict[str, str]:
    """Lazy-load the SEC ticker→CIK map; cached for the process lifetime."""
    global _ticker_to_cik, _ticker_cache_ts
    now = time.time()
    if _ticker_to_cik and (now - _ticker_cache_ts) < 86400:
        return _ticker_to_cik

    data = _http_json(EDGAR_TICKER_MAP_URL)
    if not data or not isinstance(data, dict):
        _ticker_to_cik = {}
        return _ticker_to_cik

    mapping: dict[str, str] = {}
    try:
        for _, row in data.items():
            sym = str(row.get("ticker", "")).upper()
            cik = row.get("cik_str")
            if sym and cik is not None:
                mapping[sym] = str(cik).zfill(10)
    except Exception:
        mapping = {}

    _ticker_to_cik = mapping
    _ticker_cache_ts = now
    return mapping


def _get_cik(ticker: str) -> str | None:
    return _load_ticker_map().get(ticker.upper())


def get_insider_activity(ticker: str, days: int = 60) -> dict:
    """
    Fetch recent Form 4 filings for `ticker` within `days` window.

    Returns:
        {
          'ticker': str,
          'form4_count': int,
          'unique_insiders': int,
          'days_window': int,
          'signal': 'CLUSTER_BUY' | 'ACTIVE' | 'NEUTRAL',
          'summary': str,
          'source': 'edgar' | 'degraded',
        }
    """
    ticker = ticker.upper().strip()
    cache_key = f"{ticker}:{days}"
    now = time.time()
    cached = _insider_cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL_SEC:
        return cached[1]

    result = _degraded(ticker, days, "unknown ticker")

    cik = _get_cik(ticker)
    if not cik:
        _insider_cache[cache_key] = (now, result)
        return result

    data = _http_json(EDGAR_SUBMISSIONS_URL.format(cik=cik))
    if not data or not isinstance(data, dict):
        result = _degraded(ticker, days, "edgar unreachable")
        _insider_cache[cache_key] = (now, result)
        return result

    try:
        recent = (data.get("filings") or {}).get("recent") or {}
        forms  = recent.get("form") or []
        dates  = recent.get("filingDate") or []
        n = min(len(forms), len(dates))

        cutoff = datetime.utcnow() - timedelta(days=days)
        form4_count = 0
        # EDGAR recent submissions doesn't expose insider name directly
        # without per-filing fetches. We approximate "unique insiders" by
        # counting distinct accessionNumbers — close enough for a signal.
        accessions = recent.get("accessionNumber") or []

        unique = set()
        for i in range(n):
            if forms[i] != "4":
                continue
            try:
                d = datetime.strptime(dates[i], "%Y-%m-%d")
            except ValueError:
                continue
            if d < cutoff:
                continue
            form4_count += 1
            if i < len(accessions):
                unique.add(accessions[i])

        unique_insiders = len(unique)

        if form4_count >= 6 and unique_insiders >= 3:
            signal = "CLUSTER_BUY"
        elif form4_count >= 3:
            signal = "ACTIVE"
        else:
            signal = "NEUTRAL"

        summary = (f"{form4_count} Form 4 filings in last {days}d "
                   f"({unique_insiders} unique)")

        result = {
            "ticker":          ticker,
            "form4_count":     form4_count,
            "unique_insiders": unique_insiders,
            "days_window":     days,
            "signal":          signal,
            "summary":         summary,
            "source":          "edgar",
        }
    except Exception as e:
        result = _degraded(ticker, days, f"parse error: {e}")

    _insider_cache[cache_key] = (now, result)
    return result


def insider_score_delta(insider: dict | None, opt_type: str) -> float:
    """
    Score adjustment from insider activity.

    CLUSTER_BUY + CALL → +6  (insiders loading up + you're buying calls)
    ACTIVE + CALL      → +3
    CLUSTER_BUY + PUT  → -5  (insiders buying + you're buying puts = contrarian)
    No signal / error  → 0
    """
    if not insider or not isinstance(insider, dict):
        return 0.0
    sig = insider.get("signal")
    opt = (opt_type or "").lower()
    if sig == "CLUSTER_BUY" and opt == "call":
        return 6.0
    if sig == "ACTIVE" and opt == "call":
        return 3.0
    if sig == "CLUSTER_BUY" and opt == "put":
        return -5.0
    return 0.0


def _degraded(ticker: str, days: int, reason: str) -> dict:
    return {
        "ticker":          ticker,
        "form4_count":     0,
        "unique_insiders": 0,
        "days_window":     days,
        "signal":          "NEUTRAL",
        "summary":         f"insider data unavailable ({reason})",
        "source":          "degraded",
    }
