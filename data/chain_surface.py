"""
Chain-surface persistence.

Daily snapshot of the full options chain for a symbol (every strike, every
listed expiry within 90 DTE) into the `chain_surface` SQLite table. Once
60+ days of surfaces are on disk, we can replay hypothetical entries against
actual historical bid/ask/IV — real options-level backtests, not underlying-
return proxies.

Schema — see engine/state.py → CREATE TABLE chain_surface.

Usage:
    from data.chain_surface import snapshot_symbol, load_surface

    snapshot_symbol("AAPL")            # writes today's rows for AAPL
    df = load_surface("AAPL", "2026-04-22")   # read back

The snapshotter is idempotent — re-running on the same day upserts so you
never end up with duplicate rows, and you can re-grab a ticker whose chain
failed to load mid-run.
"""

from __future__ import annotations
import os
import sqlite3
from datetime import datetime, date
from typing import Optional

import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None  # type: ignore

from engine.state import DB_PATH, init_db


MAX_DTE = 90                # skip expiries beyond 3 months
MAX_EXPIRIES_PER_TICKER = 8  # cap network calls per ticker


def _today() -> str:
    return date.today().isoformat()


def _fetch_chain(symbol: str) -> tuple[pd.DataFrame | None, float | None]:
    """
    Pull every listed expiry (capped) under MAX_DTE. Returns (df, spot).
    df columns: expiry, strike, type, bid, ask, last_price, volume,
    open_interest, iv, dte.
    """
    if yf is None:
        return None, None
    try:
        t = yf.Ticker(symbol)
        spot_hist = t.history(period="1d")
        if spot_hist is None or spot_hist.empty:
            return None, None
        spot = float(spot_hist["Close"].iloc[-1])
        expiries = list(t.options or [])[:MAX_EXPIRIES_PER_TICKER]
    except Exception:
        return None, None

    today_dt = datetime.now()
    rows: list[dict] = []
    for exp in expiries:
        try:
            exp_dt = datetime.strptime(exp, "%Y-%m-%d")
        except ValueError:
            continue
        dte = (exp_dt - today_dt).days
        if dte < 1 or dte > MAX_DTE:
            continue
        try:
            chain = t.option_chain(exp)
        except Exception:
            continue
        for leg_df, otype in ((chain.calls, "call"), (chain.puts, "put")):
            if leg_df is None or leg_df.empty:
                continue
            # Fill NaNs before per-row conversion — yfinance often returns NaN
            # for volume/openInterest on illiquid strikes.
            leg_df = leg_df.fillna(0)
            for _, r in leg_df.iterrows():
                rows.append({
                    "expiry":        exp,
                    "strike":        float(r.get("strike") or 0),
                    "type":          otype,
                    "bid":           float(r.get("bid") or 0),
                    "ask":           float(r.get("ask") or 0),
                    "last_price":    float(r.get("lastPrice") or 0),
                    "volume":        int(r.get("volume") or 0),
                    "open_interest": int(r.get("openInterest") or 0),
                    "iv":            float(r.get("impliedVolatility") or 0),
                    "dte":           dte,
                })
    if not rows:
        return None, spot
    return pd.DataFrame(rows), spot


def snapshot_symbol(symbol: str, snapshot_date: str | None = None) -> dict:
    """
    Pull today's chain and upsert into chain_surface.

    Returns {'symbol', 'rows_written', 'spot', 'error'}.
    """
    sd = snapshot_date or _today()
    symbol = symbol.upper().strip()
    df, spot = _fetch_chain(symbol)
    if df is None or df.empty:
        return {"symbol": symbol, "rows_written": 0, "spot": spot,
                "error": "no chain data"}

    init_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO chain_surface (
                symbol, snapshot_date, expiry, strike, option_type, dte,
                bid, ask, last_price, volume, open_interest, iv, spot
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (symbol, sd, r["expiry"], r["strike"], r["type"], r["dte"],
                 r["bid"], r["ask"], r["last_price"], r["volume"],
                 r["open_interest"], r["iv"], spot)
                for r in df.to_dict("records")
            ],
        )
        conn.commit()
        n = cur.rowcount
    finally:
        conn.close()

    return {"symbol": symbol, "rows_written": len(df), "spot": spot,
            "error": None}


def load_surface(symbol: str, snapshot_date: str) -> pd.DataFrame:
    """Read back a surface as a DataFrame. Empty if missing."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """SELECT expiry, strike, option_type, dte,
                      bid, ask, last_price, volume, open_interest, iv, spot
               FROM chain_surface
               WHERE symbol = ? AND snapshot_date = ?""",
            conn, params=(symbol.upper(), snapshot_date),
        )
    finally:
        conn.close()
    return df


def find_contract(symbol: str, snapshot_date: str, expiry: str,
                  strike: float, opt_type: str) -> Optional[dict]:
    """Lookup a specific contract on a specific date. None if not persisted."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """SELECT * FROM chain_surface
               WHERE symbol = ? AND snapshot_date = ? AND expiry = ?
                 AND strike = ? AND option_type = ?""",
            (symbol.upper(), snapshot_date, expiry, float(strike),
             opt_type.lower()),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def surface_dates(symbol: str | None = None) -> list[str]:
    """All distinct snapshot_dates on disk (optionally filtered by symbol)."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        if symbol:
            rows = conn.execute(
                "SELECT DISTINCT snapshot_date FROM chain_surface "
                "WHERE symbol = ? ORDER BY snapshot_date",
                (symbol.upper(),),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT snapshot_date FROM chain_surface "
                "ORDER BY snapshot_date"
            ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def surface_stats() -> dict:
    """Summary of what's on disk — for the CLI status readout."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        tot = cur.execute("SELECT COUNT(*) FROM chain_surface").fetchone()[0]
        syms = cur.execute(
            "SELECT COUNT(DISTINCT symbol) FROM chain_surface").fetchone()[0]
        dates = cur.execute(
            "SELECT COUNT(DISTINCT snapshot_date) FROM chain_surface").fetchone()[0]
        first = cur.execute(
            "SELECT MIN(snapshot_date) FROM chain_surface").fetchone()[0]
        last = cur.execute(
            "SELECT MAX(snapshot_date) FROM chain_surface").fetchone()[0]
    finally:
        conn.close()
    return {"rows": tot, "symbols": syms, "dates": dates,
            "first_date": first, "last_date": last}
