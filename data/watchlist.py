"""
Watchlist persistence — shared by Streamlit app + CLI tools.

File lives at `<repo-root>/watchlist.json`. Safe on missing/malformed file.
"""

from __future__ import annotations

import json
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WATCHLIST_FILE = os.path.join(_ROOT, "watchlist.json")


def load_watchlist() -> list[str]:
    if not os.path.exists(WATCHLIST_FILE):
        return []
    try:
        with open(WATCHLIST_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return [str(t).upper().strip() for t in data if t]
    except Exception:
        return []


def save_watchlist(tickers: list[str]) -> None:
    try:
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted({str(t).upper().strip() for t in tickers if t}), f)
    except Exception:
        pass
