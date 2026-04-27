"""
Local HTTP API server for the dashboard's on-demand ticker lookup.

The static dashboard.html ships with bars for the curated universe (~98
tickers). When the user types a ticker NOT in that set (e.g. CTXR), the
dashboard JS calls /api/bars on this server, which fetches live data
from Alpaca's IEX feed and returns JSON.

Endpoints:
  GET  /api/bars?sym=CTXR     →  {"sym": "CTXR", "bars": {"intraday": [], "daily": [], "last": ..., "last_ts": ...}}
  GET  /api/news?sym=CTXR     →  {"sym": "CTXR", "articles": [...]}
  GET  /api/health            →  {"ok": true, "uptime_s": 123.4}

CORS-permissive (Access-Control-Allow-Origin: *) so the dashboard.html
can fetch from file:// origins in any browser.

Listens on localhost:8503 by default. Single-threaded BaseHTTPServer is
fine — typical use is ad-hoc lookups, not high concurrency.

Run:
    python -m tools.dashboard_server
    python -m tools.dashboard_server --port 8504
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Side-effect: load .env so Alpaca creds are visible to broker.alpaca
try:
    import config_loader  # noqa: F401
except Exception:
    pass

# We reuse the dashboard's bar-fetching logic for consistency
from tools.build_dashboard import _fetch_ticker_bars

_SERVER_START = time.time()


# ── Cache: sym -> (fetched_at_epoch, bars_dict) ─────────────────────────────
# Bars fetched within the last 90s are reused. Keeps clicks snappy and
# avoids hammering Alpaca when user hops tickers.
_CACHE_TTL_SEC = 90
_bars_cache: dict[str, tuple[float, dict]] = {}


def _get_bars(sym: str) -> dict:
    sym = sym.upper().strip()
    now = time.time()
    cached = _bars_cache.get(sym)
    if cached and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]
    fetched = _fetch_ticker_bars([sym])
    bars = fetched.get(sym, {"intraday": [], "daily": [], "last": None, "last_ts": None})
    _bars_cache[sym] = (now, bars)
    return bars


def _get_news(sym: str) -> dict:
    """Fetch recent news for a ticker. Best-effort; returns empty list on failure."""
    try:
        from data.news import get_news
        articles = get_news(sym, max_age_days=7, limit=20) or []
    except Exception:
        articles = []
    return {"sym": sym.upper(), "articles": articles}


# ── HTTP handler ────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        # Quieter than the default "GET / HTTP/1.1 200" stderr spam
        pass

    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # CORS — dashboard runs from file:// or any local path
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError):
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_GET(self):
        url = urlparse(self.path)
        qs = parse_qs(url.query or "")
        path = url.path

        if path == "/api/health":
            self._send_json(200, {
                "ok": True,
                "uptime_s": round(time.time() - _SERVER_START, 1),
                "cached_tickers": len(_bars_cache),
            })
            return

        if path == "/api/bars":
            sym = (qs.get("sym", [""])[0] or "").upper().strip()
            if not sym or len(sym) > 8 or not sym.replace(".", "").isalnum():
                self._send_json(400, {"error": "invalid sym"})
                return
            try:
                bars = _get_bars(sym)
                self._send_json(200, {"sym": sym, "bars": bars})
            except Exception as e:
                self._send_json(500, {"error": str(e)[:200], "sym": sym})
            return

        if path == "/api/news":
            sym = (qs.get("sym", [""])[0] or "").upper().strip()
            if not sym or len(sym) > 8 or not sym.replace(".", "").isalnum():
                self._send_json(400, {"error": "invalid sym"})
                return
            try:
                self._send_json(200, _get_news(sym))
            except Exception as e:
                self._send_json(500, {"error": str(e)[:200], "sym": sym})
            return

        self._send_json(404, {"error": "not found", "path": path})


# ── Entry point ─────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8503)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Dashboard API server on http://{args.host}:{args.port}")
    print(f"  /api/health")
    print(f"  /api/bars?sym=CTXR")
    print(f"  /api/news?sym=CTXR")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
