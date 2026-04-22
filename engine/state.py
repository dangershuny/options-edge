"""
Position and settlement state — SQLite-backed, resumable across restarts.

This is the bookkeeping layer between the broker (which only knows
current positions) and the engine (which needs to know trade history,
unsettled cash, pending exits, settlement timing).

Two tables:

    positions      — every open or recently-closed position
    settlements    — each sell and its T+1 settle_date

Why both? The broker tells us "you have X contracts of AAPL250515C00200"
but not "those came from the trade you placed on Monday at 9:30 with
score 82 into a catalyst-pending window with exit-queued-for-tomorrow
because SL fired intraday." All of that context lives here.

Schema is intentionally simple — no migrations framework yet. If the
schema changes before go-live, wipe the DB file and replay snapshot
history to rebuild. After go-live, migrate carefully.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from typing import Iterator

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "engine_state.db",
)

SETTLEMENT_DAYS = 1  # T+1 for options since May 2024


# ── Schema ───────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    occ_symbol     TEXT    NOT NULL,
    underlying     TEXT    NOT NULL,
    option_type    TEXT    NOT NULL,
    strike         REAL    NOT NULL,
    expiry         TEXT    NOT NULL,
    qty            INTEGER NOT NULL,
    entry_price    REAL    NOT NULL,
    entry_date     TEXT    NOT NULL,
    entry_order_id TEXT,
    score          REAL,
    dte_at_entry   INTEGER,
    vol_signal     TEXT,
    sl_pct         REAL,
    tp_pct         REAL,
    trailing_peak  REAL,
    exit_queued    INTEGER DEFAULT 0,   -- 1 = exit trigger fired, queued for next session
    exit_reason    TEXT,
    exit_price     REAL,
    exit_date      TEXT,
    exit_order_id  TEXT,
    realized_pl    REAL,
    status         TEXT    NOT NULL     -- 'open' | 'closing' | 'closed'
);
CREATE INDEX IF NOT EXISTS ix_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS ix_positions_occ ON positions(occ_symbol);

CREATE TABLE IF NOT EXISTS settlements (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date   TEXT    NOT NULL,
    settle_date  TEXT    NOT NULL,
    amount       REAL    NOT NULL,        -- proceeds from the sell
    occ_symbol   TEXT    NOT NULL,
    settled      INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_settlements_settle ON settlements(settle_date, settled);

CREATE TABLE IF NOT EXISTS news_seen (
    underlying   TEXT    NOT NULL,
    link         TEXT    NOT NULL,
    title        TEXT,
    published    TEXT,
    sentiment    REAL,
    source       TEXT,
    seen_at      TEXT    NOT NULL,
    PRIMARY KEY (underlying, link)
);
CREATE INDEX IF NOT EXISTS ix_news_seen_underlying ON news_seen(underlying, seen_at);
"""


@contextmanager
def _db() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _db() as c:
        c.executescript(_SCHEMA)


# ── Position lifecycle ───────────────────────────────────────────────────────

@dataclass
class OpenPositionRecord:
    occ_symbol: str
    underlying: str
    option_type: str
    strike: float
    expiry: str
    qty: int
    entry_price: float
    entry_date: str
    entry_order_id: str | None
    score: float
    dte_at_entry: int
    vol_signal: str
    sl_pct: float | None
    tp_pct: float | None


def record_open(p: OpenPositionRecord) -> int:
    init_db()
    with _db() as c:
        cur = c.execute(
            """
            INSERT INTO positions (
                occ_symbol, underlying, option_type, strike, expiry, qty,
                entry_price, entry_date, entry_order_id, score, dte_at_entry,
                vol_signal, sl_pct, tp_pct, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
            """,
            (
                p.occ_symbol, p.underlying, p.option_type, p.strike, p.expiry,
                p.qty, p.entry_price, p.entry_date, p.entry_order_id,
                p.score, p.dte_at_entry, p.vol_signal, p.sl_pct, p.tp_pct,
            ),
        )
        return int(cur.lastrowid)


def list_open() -> list[dict]:
    init_db()
    with _db() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM positions WHERE status IN ('open','closing') ORDER BY entry_date"
        )]


def update_peak(position_id: int, new_peak: float) -> None:
    with _db() as c:
        c.execute(
            "UPDATE positions SET trailing_peak = ? WHERE id = ? "
            "AND (trailing_peak IS NULL OR trailing_peak < ?)",
            (new_peak, position_id, new_peak),
        )


def queue_exit(position_id: int, reason: str) -> None:
    """Flag a position for exit on the next session — cash account can't
    round-trip same day (see risk.exits.same_day_exit_allowed)."""
    with _db() as c:
        c.execute(
            "UPDATE positions SET exit_queued = 1, exit_reason = ?, status = 'closing' "
            "WHERE id = ? AND status = 'open'",
            (reason, position_id),
        )


def list_queued_exits() -> list[dict]:
    with _db() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM positions WHERE status = 'closing' AND exit_queued = 1"
        )]


def record_close(position_id: int, exit_price: float, exit_date: str,
                 exit_order_id: str | None, reason: str) -> None:
    with _db() as c:
        row = c.execute(
            "SELECT entry_price, qty FROM positions WHERE id = ?", (position_id,)
        ).fetchone()
        if not row:
            return
        pl = (exit_price - float(row["entry_price"])) * 100 * int(row["qty"])
        c.execute(
            """UPDATE positions SET exit_price = ?, exit_date = ?,
               exit_order_id = ?, realized_pl = ?, exit_reason = ?,
               status = 'closed' WHERE id = ?""",
            (exit_price, exit_date, exit_order_id, pl, reason, position_id),
        )
        # Record settlement
        settle = (datetime.strptime(exit_date, "%Y-%m-%d").date()
                  + timedelta(days=SETTLEMENT_DAYS)).isoformat()
        proceeds = exit_price * 100 * int(row["qty"])
        c.execute(
            """INSERT INTO settlements (trade_date, settle_date, amount, occ_symbol)
               SELECT ?, ?, ?, occ_symbol FROM positions WHERE id = ?""",
            (exit_date, settle, proceeds, position_id),
        )


# ── Settlement tracking ──────────────────────────────────────────────────────

def unsettled_cash(as_of: date | None = None) -> float:
    """Proceeds from sells not yet past their settle_date."""
    as_of = as_of or date.today()
    init_db()
    with _db() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total FROM settlements "
            "WHERE settled = 0 AND settle_date > ?",
            (as_of.isoformat(),),
        ).fetchone()
        return float(row["total"])


def mark_settlements_settled(as_of: date | None = None) -> int:
    """Call daily. Marks settlements whose settle_date has passed as settled."""
    as_of = as_of or date.today()
    init_db()
    with _db() as c:
        cur = c.execute(
            "UPDATE settlements SET settled = 1 WHERE settled = 0 AND settle_date <= ?",
            (as_of.isoformat(),),
        )
        return cur.rowcount


def available_cash_for_new_trade(equity_cash: float,
                                 as_of: date | None = None) -> float:
    """
    equity_cash = what the broker reports as cash balance.
    Subtract any still-unsettled proceeds so we never spend cash that
    we *know* can't fund another round-trip before it settles.

    Returns the amount safe to deploy in a new buy today.
    """
    return max(0.0, equity_cash - unsettled_cash(as_of))


# ── News dedupe / monitor state ──────────────────────────────────────────────

def news_already_seen(underlying: str, link: str) -> bool:
    init_db()
    with _db() as c:
        row = c.execute(
            "SELECT 1 FROM news_seen WHERE underlying = ? AND link = ?",
            (underlying, link),
        ).fetchone()
        return row is not None


def record_news_seen(underlying: str, article: dict) -> None:
    """Persist that we've processed this article — prevents re-triggering on
    the same headline across monitor ticks / restarts."""
    pub = article.get("published")
    pub_str = pub.isoformat() if pub else None
    with _db() as c:
        c.execute(
            """INSERT OR IGNORE INTO news_seen
               (underlying, link, title, published, sentiment, source, seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                underlying,
                article.get("link") or "",
                (article.get("title") or "")[:300],
                pub_str,
                article.get("sentiment"),
                article.get("source"),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )


def last_news_check_time(underlying: str) -> datetime | None:
    """Latest seen_at across all articles we've logged for this ticker."""
    init_db()
    with _db() as c:
        row = c.execute(
            "SELECT MAX(seen_at) AS t FROM news_seen WHERE underlying = ?",
            (underlying,),
        ).fetchone()
        if not row or not row["t"]:
            return None
        try:
            return datetime.fromisoformat(row["t"])
        except ValueError:
            return None
