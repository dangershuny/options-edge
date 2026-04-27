"""
Lightweight HTTP server for the dashboard's manual-override buy button.

Listens on localhost:8503 (separate from news_sentinel:8502 to avoid
any conflict). Designed to be safe alongside the existing scheduled
tasks — single-threaded, no shared state mutation.

Endpoints:
  GET  /health                       — liveness check
  GET  /recent?limit=20              — recent override results from log
  POST /override                     — body: {"ticker": "GME",
                                              "side": "call"|"put"|null,
                                              "tag": "manual",
                                              "max_cost": 100,
                                              "live": true|false (default false),
                                              "min_score": 40 }
                                       Returns the result dict from
                                       tools.override_buy.execute_override.

Dashboard JS calls POST /override with the form input. Server runs
analyze_ticker + best-contract pick + Alpaca order, returns the result
synchronously. Single request takes 30-90s typically (analyze_ticker is
the slow step).

Start manually:
    python -m tools.override_server

Or run via the launch task `tools/override_server.bat` registered in
Task Scheduler at user logon.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401  (loads .env)

from tools.override_buy import execute_override, OVERRIDE_RESULTS_DIR

PORT = 8503


def _json_bytes(d) -> bytes:
    return json.dumps(d, default=str).encode("utf-8")


def _cors_headers(handler: BaseHTTPRequestHandler) -> None:
    """Allow dashboard.html (file:// origin) to call us."""
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")


def _read_recent(limit: int = 20) -> list[dict]:
    """Read the last N override results from disk, newest first."""
    if not OVERRIDE_RESULTS_DIR.exists():
        return []
    files = sorted(OVERRIDE_RESULTS_DIR.glob("*.json"), reverse=True)[:limit]
    out = []
    for f in files:
        try:
            out.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            continue
    return out


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"  [override] {self.address_string()} {fmt % args}",
              file=sys.stderr)

    def do_OPTIONS(self):
        self.send_response(204)
        _cors_headers(self)
        self.end_headers()

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/health":
            self._send(200, {"ok": True, "service": "override_server",
                              "port": PORT})
            return
        if u.path == "/recent":
            qs = parse_qs(u.query)
            try:
                limit = int(qs.get("limit", ["20"])[0])
            except Exception:
                limit = 20
            self._send(200, {"results": _read_recent(min(max(limit, 1), 100))})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        if u.path != "/override":
            self._send(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length") or 0)
        try:
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            body = json.loads(raw or "{}")
        except Exception as e:
            self._send(400, {"error": f"bad JSON: {e}"})
            return

        ticker = (body.get("ticker") or "").strip().upper()
        if not ticker:
            self._send(400, {"error": "ticker is required"})
            return

        side = body.get("side") or None
        if side not in ("call", "put", None):
            self._send(400, {"error": "side must be 'call', 'put', or null"})
            return

        live = bool(body.get("live", False))
        tag = body.get("tag") or "manual"
        max_cost = body.get("max_cost")
        try:
            max_cost = float(max_cost) if max_cost is not None else None
        except Exception:
            max_cost = None
        min_score = body.get("min_score", 40.0)
        try:
            min_score = float(min_score)
        except Exception:
            min_score = 40.0

        # Run synchronously — the dashboard JS shows a spinner while waiting
        result = execute_override(
            ticker=ticker, side=side, max_cost=max_cost, tag=tag,
            dry_run=not live, min_score=min_score,
        )
        self._send(200, result)

    def _send(self, code: int, payload):
        body = _json_bytes(payload)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        _cors_headers(self)
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    addr = ("127.0.0.1", PORT)
    server = HTTPServer(addr, Handler)
    print(f"Override server running on http://127.0.0.1:{PORT}")
    print("  POST /override   — submit a manual buy")
    print("  GET  /health     — liveness")
    print("  GET  /recent     — recent results")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
