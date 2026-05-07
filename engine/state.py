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

CREATE TABLE IF NOT EXISTS chain_surface (
    symbol         TEXT    NOT NULL,
    snapshot_date  TEXT    NOT NULL,       -- YYYY-MM-DD
    expiry         TEXT    NOT NULL,       -- YYYY-MM-DD
    strike         REAL    NOT NULL,
    option_type    TEXT    NOT NULL,       -- 'call' | 'put'
    dte            INTEGER,
    bid            REAL,
    ask            REAL,
    last_price     REAL,
    volume         INTEGER,
    open_interest  INTEGER,
    iv             REAL,
    spot           REAL,                   -- underlying close that day
    PRIMARY KEY (symbol, snapshot_date, expiry, strike, option_type)
);
CREATE INDEX IF NOT EXISTS ix_surface_sym_date ON chain_surface(symbol, snapshot_date);
CREATE INDEX IF NOT EXISTS ix_surface_date ON chain_surface(snapshot_date);
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
        # Lightweight migrations — add columns added after initial schema.
        # Idempotent (PRAGMA table_info check).
        cols = {r["name"] for r in c.execute("PRAGMA table_info(positions)")}
        if "last_monitor_check" not in cols:
            c.execute("ALTER TABLE positions ADD COLUMN last_monitor_check TEXT")
        if "sl_resets_today" not in cols:
            c.execute("ALTER TABLE positions ADD COLUMN sl_resets_today INTEGER DEFAULT 0")
        if "sl_reset_date" not in cols:
            c.execute("ALTER TABLE positions ADD COLUMN sl_reset_date TEXT")


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
    """
    Idempotent insert. If a row already exists with the same occ_symbol in
    status 'open' or 'closing', returns its id WITHOUT inserting a duplicate.
    The multi-tier paper_trade loop submits the same OCC across bankroll
    tiers; without dedupe we'd write a fresh row per tier and pollute the
    monitor's position list.
    """
    init_db()
    with _db() as c:
        existing = c.execute(
            "SELECT id FROM positions WHERE occ_symbol = ? AND status IN ('open','closing') LIMIT 1",
            (p.occ_symbol,),
        ).fetchone()
        if existing:
            return int(existing["id"])
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


def mark_closing(position_id: int, exit_order_id: str, reason: str) -> None:
    """
    Submitted a SELL but it hasn't filled yet — flag the row so monitor_tick
    knows not to fire another exit and reconcile_with_broker knows to
    finalize once Alpaca confirms the fill.
    """
    with _db() as c:
        c.execute(
            "UPDATE positions SET exit_order_id = ?, exit_reason = ?, "
            "status = 'closing', exit_queued = 0 WHERE id = ?",
            (exit_order_id, reason, position_id),
        )


def revert_to_open(position_id: int) -> None:
    """Close order expired/canceled — re-arm so next trigger can re-fire."""
    with _db() as c:
        c.execute(
            "UPDATE positions SET exit_order_id = NULL, exit_queued = 0, "
            "status = 'open' WHERE id = ?",
            (position_id,),
        )


def mark_phantom(position_id: int) -> None:
    """Engine has the row but Alpaca has no position — record never landed.
    Soft-delete: keep the row for audit but stop monitor_tick processing it.

    Fires a Telegram WARN so silent broker rejections (FUBO 5/5) surface
    immediately rather than after EOD analysis. Idempotent on the alert
    side: if this row is already phantom, skip the alert."""
    occ = None
    score = None
    entry_order_id = None
    with _db() as c:
        row = c.execute(
            "SELECT occ_symbol, score, entry_order_id, status "
            "FROM positions WHERE id = ?",
            (position_id,),
        ).fetchone()
        if row is None:
            return
        if row["status"] == "phantom":
            return  # already phantomed; don't double-alert
        occ = row["occ_symbol"]
        score = row["score"]
        entry_order_id = row["entry_order_id"]
        c.execute(
            "UPDATE positions SET status = 'phantom', exit_queued = 0 WHERE id = ?",
            (position_id,),
        )

    # Fire WARN — order placed but never landed at broker. Best-effort,
    # never blocks the DB update.
    try:
        from tools.notify import send
        score_str = f"score={float(score):.0f} " if score is not None else ""
        order_str = f"order={entry_order_id[:8]}" if entry_order_id else "order=?"
        send(
            "WARN",
            f"PHANTOM ORDER: {occ} (id={position_id})",
            f"{score_str}{order_str} — engine recorded entry but broker has "
            f"no position. Likely cause: limit didn't fill, broker rejected, "
            f"or wide bid-ask spread. Check Alpaca dashboard.",
        )
    except Exception:
        pass


# ── Same-day-loss circuit breaker ─────────────────────────────────────────────

def count_same_day_losses_today(today_iso: str | None = None) -> int:
    """Count positions that BOTH entered AND closed today with negative P&L.
    Used by the entry-side circuit-breaker: after 2 same-day SL hits, halt
    new entries for the rest of the session.

    Carryover losses (entered prior day, closed today) DON'T count — those
    just mean a stop did its job on a stale position.
    """
    if today_iso is None:
        today_iso = date.today().isoformat()
    init_db()
    with _db() as c:
        row = c.execute(
            "SELECT COUNT(*) AS n FROM positions "
            "WHERE status = 'closed' "
            "  AND date(entry_date) = ? "
            "  AND date(exit_date)  = ? "
            "  AND COALESCE(realized_pl, 0) < 0",
            (today_iso, today_iso),
        ).fetchone()
        return int(row["n"] if row else 0)


def count_phantoms_today(today_iso: str | None = None) -> int:
    """Count rows phantomed today (orders placed but never landed)."""
    if today_iso is None:
        today_iso = date.today().isoformat()
    init_db()
    with _db() as c:
        row = c.execute(
            "SELECT COUNT(*) AS n FROM positions "
            "WHERE status = 'phantom' AND date(entry_date) = ?",
            (today_iso,),
        ).fetchone()
        return int(row["n"] if row else 0)


def find_phantom_for_occ(occ_symbol: str, max_age_days: int = 2) -> dict | None:
    """Return the most recent phantom row for `occ_symbol` if its entry_date
    is within `max_age_days` of today, else None.

    Used by reconcile_with_broker: when a broker position appears that has
    no tracked engine row, we first check if there's a recent phantom for
    the same OCC. If yes, the phantom was a paper_trade row whose order
    eventually filled — un-phantom it instead of treating it as untracked.
    """
    init_db()
    cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
    with _db() as c:
        row = c.execute(
            "SELECT * FROM positions "
            "WHERE occ_symbol = ? AND status = 'phantom' "
            "  AND date(entry_date) >= ? "
            "ORDER BY id DESC LIMIT 1",
            (occ_symbol, cutoff),
        ).fetchone()
        return dict(row) if row else None


def unphantom(position_id: int, broker_qty: int | None = None) -> bool:
    """Restore a phantom row back to status='open'. Optionally update qty
    to match broker truth (when 2 paper_trade tier limits both filled,
    the broker may show qty=2 while the phantom row had qty=1).
    Returns True if the row was updated."""
    with _db() as c:
        if broker_qty is not None:
            cur = c.execute(
                "UPDATE positions SET status = 'open', qty = ?, exit_queued = 0 "
                "WHERE id = ? AND status = 'phantom'",
                (int(broker_qty), position_id),
            )
        else:
            cur = c.execute(
                "UPDATE positions SET status = 'open', exit_queued = 0 "
                "WHERE id = ? AND status = 'phantom'",
                (position_id,),
            )
        return cur.rowcount > 0


def list_closing() -> list[dict]:
    """Rows that have submitted a SELL and are awaiting fill confirmation."""
    init_db()
    with _db() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM positions WHERE status = 'closing' AND exit_order_id IS NOT NULL"
        )]


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


def update_sl(position_id: int, new_sl: float, *, only_tighter: bool = True) -> bool:
    """
    Update sl_pct for a position. By default ratchets — only TIGHTENS
    (raises sl_pct toward 0). Pass only_tighter=False to allow loosening
    (used by overnight-gap reset which intentionally widens the SL to
    avoid panic-selling at the gap-down spike).
    Returns True if the row was updated.
    """
    with _db() as c:
        if only_tighter:
            cur = c.execute(
                "UPDATE positions SET sl_pct = ? WHERE id = ? "
                "AND (sl_pct IS NULL OR sl_pct < ?)",
                (new_sl, position_id, new_sl),
            )
        else:
            cur = c.execute(
                "UPDATE positions SET sl_pct = ? WHERE id = ?",
                (new_sl, position_id),
            )
        return cur.rowcount > 0


def record_monitor_check(position_id: int, ts_iso: str) -> None:
    """Stamp last_monitor_check so the gap detector can see how long since
    the last successful tick on this position."""
    with _db() as c:
        c.execute(
            "UPDATE positions SET last_monitor_check = ? WHERE id = ?",
            (ts_iso, position_id),
        )


def increment_sl_reset(position_id: int, today_iso: str) -> None:
    """Bump sl_resets_today counter (zeroed if last reset was a previous day)."""
    with _db() as c:
        row = c.execute(
            "SELECT sl_reset_date, sl_resets_today FROM positions WHERE id = ?",
            (position_id,),
        ).fetchone()
        if row is None:
            return
        prev_date = row["sl_reset_date"]
        n = (row["sl_resets_today"] or 0) if prev_date == today_iso else 0
        c.execute(
            "UPDATE positions SET sl_reset_date = ?, sl_resets_today = ? WHERE id = ?",
            (today_iso, n + 1, position_id),
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
