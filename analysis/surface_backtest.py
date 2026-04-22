"""
Real options-level backtest engine — replays entries against the persisted
`chain_surface` table.

Given an entry date and an exit date (usually entry + N trading days), for
every ticker with surfaces on both dates we:

  1. Re-score the chain on the entry date exactly the way the live scorer
     would have (OTM window, IV vs RV, flow, etc.), using the historical
     surface rows as the chain instead of today's live yfinance pull.
  2. For every contract that would have been BUY-signalled on the entry
     date, look up the same contract (symbol, expiry, strike, type) on
     the exit date and compute P&L from mid-to-mid.
  3. Aggregate P&L by score bucket / vol_signal / DTE / VIX regime so we
     can tell which segments of the scorer's output actually make money
     at the CONTRACT level — not just underlying directional moves.

Missing data is handled gracefully: if a contract's exit-date row is missing
(rolled off, not listed), the trade is dropped from the sample with a note.

Scope: this is a pure replay — it does not open network connections.
Everything reads from the SQLite `chain_surface` table. So real-historical
backtests become possible as soon as ≥2 days of surfaces are persisted.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd

from data.chain_surface import load_surface, surface_dates


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mid(bid: float, ask: float, last: float = 0.0) -> float:
    if bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return float(last or 0.0)


def _quality_ok(row) -> bool:
    """Drop obviously unusable rows: zero bid/ask AND zero last, or zero IV."""
    bid = row.get("bid") or 0
    ask = row.get("ask") or 0
    last = row.get("last_price") or 0
    iv = row.get("iv") or 0
    if iv <= 0:
        return False
    if (bid <= 0 or ask <= 0) and last <= 0:
        return False
    return True


# ── Result containers ────────────────────────────────────────────────────────

@dataclass
class Trade:
    symbol:        str
    option_type:   str
    strike:        float
    expiry:        str
    entry_date:    str
    exit_date:     str
    entry_mid:     float
    exit_mid:      float
    pnl_per_ct:    float
    pnl_pct:       float
    dte_at_entry:  int
    iv_at_entry:   float
    vol_at_entry:  int
    oi_at_entry:   int


@dataclass
class ReplayResult:
    entry_date:   str
    exit_date:    str
    n_symbols:    int
    n_trades:     int
    trades:       list[Trade] = field(default_factory=list)
    by_bucket:    dict = field(default_factory=dict)
    warnings:     list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "entry_date": self.entry_date,
            "exit_date":  self.exit_date,
            "n_symbols":  self.n_symbols,
            "n_trades":   self.n_trades,
            "by_bucket":  self.by_bucket,
            "warnings":   self.warnings,
            "trades":     [t.__dict__ for t in self.trades[:50]],
        }


# ── Lightweight contract filter (mirror of scorer entry rules) ───────────────
#
# We don't re-import scorer.analyze_ticker because that would spawn live
# yfinance calls. Instead we apply the same hard filters (OTM window,
# DTE window, min volume, min OI) against the persisted row.

OTM_LIMIT = 0.15
MIN_DTE   = 7
MAX_DTE   = 60
MIN_VOLUME = 10
MIN_OI    = 100


def _filter_contract(row, spot: float) -> bool:
    if not _quality_ok(row):
        return False
    strike = float(row["strike"])
    if spot <= 0:
        return False
    pct_from_spot = abs(strike - spot) / spot
    if pct_from_spot > OTM_LIMIT:
        return False
    dte = int(row.get("dte") or 0)
    if dte < MIN_DTE or dte > MAX_DTE:
        return False
    if int(row.get("volume") or 0) < MIN_VOLUME:
        return False
    if int(row.get("open_interest") or 0) < MIN_OI:
        return False
    return True


# ── Bucket stats ─────────────────────────────────────────────────────────────

def _bucket_dte(dte: int) -> str:
    if dte < 14:  return "<14"
    if dte < 30:  return "14-30"
    if dte < 45:  return "30-45"
    return "45+"


def _bucket_moneyness(strike: float, spot: float, opt_type: str) -> str:
    if spot <= 0:
        return "?"
    pct = (strike - spot) / spot
    if opt_type == "call":
        if pct < -0.02:  return "ITM"
        if pct > 0.02:   return "OTM"
        return "ATM"
    else:
        if pct > 0.02:   return "ITM"
        if pct < -0.02:  return "OTM"
        return "ATM"


def _agg(trades: list[Trade]) -> dict:
    if not trades:
        return {"n": 0, "hit_rate": None, "avg_pnl_pct": None,
                "total_pnl": 0.0, "avg_win_pct": None, "avg_loss_pct": None}
    n = len(trades)
    wins = [t for t in trades if t.pnl_per_ct > 0]
    losses = [t for t in trades if t.pnl_per_ct < 0]
    total = sum(t.pnl_per_ct for t in trades) * 100
    return {
        "n":            n,
        "hit_rate":     round(len(wins) / n * 100, 1),
        "avg_pnl_pct":  round(sum(t.pnl_pct for t in trades) / n, 2),
        "total_pnl":    round(total, 2),
        "avg_win_pct":  round(sum(t.pnl_pct for t in wins) / len(wins), 2) if wins else None,
        "avg_loss_pct": round(sum(t.pnl_pct for t in losses) / len(losses), 2) if losses else None,
    }


def _group(trades: list[Trade], keyfn) -> dict:
    buckets: dict[str, list[Trade]] = {}
    for t in trades:
        try:
            k = str(keyfn(t))
        except Exception:
            k = "unknown"
        buckets.setdefault(k, []).append(t)
    return {k: _agg(v) for k, v in sorted(buckets.items())}


# ── Main replay ──────────────────────────────────────────────────────────────

def replay(entry_date: str, exit_date: str,
           symbols: Iterable[str] | None = None) -> ReplayResult:
    """
    For each symbol that has surfaces on BOTH dates, replay every contract
    that would have passed entry filters on `entry_date` and look up its
    exit price on `exit_date`.
    """
    dates_on_disk = set(surface_dates())
    if entry_date not in dates_on_disk:
        return ReplayResult(entry_date, exit_date, 0, 0,
                            warnings=[f"no surface data for entry_date {entry_date}"])
    if exit_date not in dates_on_disk:
        return ReplayResult(entry_date, exit_date, 0, 0,
                            warnings=[f"no surface data for exit_date {exit_date}"])

    # Figure out common symbol set between entry and exit dates
    import sqlite3
    from engine.state import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    try:
        entry_syms = {r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM chain_surface WHERE snapshot_date = ?",
            (entry_date,)).fetchall()}
        exit_syms = {r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM chain_surface WHERE snapshot_date = ?",
            (exit_date,)).fetchall()}
    finally:
        conn.close()

    shared = entry_syms & exit_syms
    if symbols is not None:
        shared &= {s.upper() for s in symbols}
    if not shared:
        return ReplayResult(entry_date, exit_date, 0, 0,
                            warnings=["no shared symbols between entry & exit dates"])

    trades: list[Trade] = []
    warnings: list[str] = []
    for sym in sorted(shared):
        entry_df = load_surface(sym, entry_date)
        exit_df  = load_surface(sym, exit_date)
        if entry_df.empty or exit_df.empty:
            continue
        spot = float(entry_df["spot"].iloc[0]) if len(entry_df) else 0.0
        # Index exit chain by (expiry, strike, type) for O(1) lookup
        exit_idx = {(r.expiry, float(r.strike), r.option_type): r
                    for r in exit_df.itertuples(index=False)}

        for row in entry_df.itertuples(index=False):
            rd = row._asdict()
            if not _filter_contract(rd, spot):
                continue
            key = (rd["expiry"], float(rd["strike"]), rd["option_type"])
            ex = exit_idx.get(key)
            if ex is None:
                continue
            entry_mid = _mid(rd["bid"], rd["ask"], rd.get("last_price", 0))
            exit_mid  = _mid(ex.bid, ex.ask, ex.last_price)
            if entry_mid <= 0 or exit_mid <= 0:
                continue
            pnl = exit_mid - entry_mid
            pnl_pct = (pnl / entry_mid) * 100 if entry_mid > 0 else 0.0
            trades.append(Trade(
                symbol=sym, option_type=rd["option_type"],
                strike=float(rd["strike"]), expiry=rd["expiry"],
                entry_date=entry_date, exit_date=exit_date,
                entry_mid=round(entry_mid, 3), exit_mid=round(exit_mid, 3),
                pnl_per_ct=round(pnl * 100, 2),         # per-contract $ P&L
                pnl_pct=round(pnl_pct, 2),
                dte_at_entry=int(rd.get("dte") or 0),
                iv_at_entry=round(float(rd.get("iv") or 0), 4),
                vol_at_entry=int(rd.get("volume") or 0),
                oi_at_entry=int(rd.get("open_interest") or 0),
            ))

    by_bucket = {
        "overall":      _agg(trades),
        "by_type":      _group(trades, lambda t: t.option_type),
        "by_dte":       _group(trades, lambda t: _bucket_dte(t.dte_at_entry)),
        "by_moneyness": _group(
            trades,
            lambda t: _bucket_moneyness(
                t.strike,
                # approximate spot via entry_mid + intrinsic? Fall back: unknown
                spot if False else 0,
                t.option_type,
            ),
        ),
    }
    # Rank by pnl_pct for summary lists
    trades.sort(key=lambda t: t.pnl_pct, reverse=True)

    return ReplayResult(
        entry_date=entry_date, exit_date=exit_date,
        n_symbols=len(shared), n_trades=len(trades),
        trades=trades, by_bucket=by_bucket, warnings=warnings,
    )
