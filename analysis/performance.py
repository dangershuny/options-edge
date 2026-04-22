"""
Backtester / signal performance analyzer.

Reads every `snapshots/YYYY-MM-DD.json` file that has been closed out by
`tools/compare.py`, aggregates the P&L outcomes, and computes per-feature
hit rates.

Goal: tell us *which signals actually predict* so we can weight the scorer
correctly. Without this the scorer is theory; with this we know what works.

Per-feature reports include:
  - Overall hit rate, ROI, avg winner/loser
  - By vol_signal (BUY VOL vs FLOW BUY)
  - By score bucket (0-40, 40-60, 60-80, 80-100)
  - By option type (call vs put)
  - By DTE bucket (<14, 14-30, 30-60, 60+)
  - By IV rank label
  - By skew signal / GEX signal
  - By sector

Safe on malformed files — skips them and logs warnings.
"""

from __future__ import annotations

import json
import os
import glob
from collections import defaultdict
from datetime import datetime

from data.sectors import get_sector


def _load_all_snapshots(snapshots_dir: str) -> tuple[list[dict], list[str]]:
    """Load every *.json under snapshots_dir. Returns (trades, warnings)."""
    trades: list[dict] = []
    warnings: list[str] = []

    pattern = os.path.join(snapshots_dir, "*.json")
    for path in sorted(glob.glob(pattern)):
        try:
            with open(path, encoding="utf-8") as f:
                snap = json.load(f)
        except Exception as e:
            warnings.append(f"{os.path.basename(path)}: could not parse ({e})")
            continue

        snap_date = snap.get("snapshot_date") or os.path.basename(path).replace(".json", "")
        for t in snap.get("trades", []) or []:
            if not isinstance(t, dict):
                continue
            # Only count closed-out trades (have outcome + pnl)
            outcome = t.get("outcome")
            pnl     = t.get("pnl_per_contract")
            if outcome in (None, "NO DATA", "NO ENTRY") or pnl is None:
                continue
            t2 = dict(t)
            t2["_snap_date"] = snap_date
            t2["_sector"]    = get_sector(t.get("symbol", ""))
            trades.append(t2)

    return trades, warnings


def _bucket_score(score: float | None) -> str:
    if score is None:
        return "unknown"
    if score < 40:
        return "0-40"
    if score < 60:
        return "40-60"
    if score < 80:
        return "60-80"
    return "80-100"


def _bucket_dte(dte: int | None) -> str:
    if dte is None:
        return "unknown"
    if dte < 14:
        return "<14"
    if dte < 30:
        return "14-30"
    if dte < 60:
        return "30-60"
    return "60+"


def _stats(trades: list[dict]) -> dict:
    """Aggregate win/loss/roi from a bucket of trades."""
    if not trades:
        return {"n": 0, "wins": 0, "losses": 0, "hit_rate": None,
                "total_pnl": 0.0, "total_cost": 0.0, "roi_pct": None,
                "avg_win": None, "avg_loss": None}
    n = len(trades)
    wins = sum(1 for t in trades if (t.get("pnl_per_contract") or 0) > 0)
    losses = sum(1 for t in trades if (t.get("pnl_per_contract") or 0) < 0)

    pnls  = [float(t.get("pnl_per_contract") or 0) for t in trades]
    costs = [float(t.get("entry_price_mid") or 0) * 100 for t in trades]
    total_pnl = sum(pnls)
    total_cost = sum(costs)
    wins_pnls = [p for p in pnls if p > 0]
    losses_pnls = [p for p in pnls if p < 0]

    return {
        "n":          n,
        "wins":       wins,
        "losses":     losses,
        "hit_rate":   round(wins / n * 100, 1) if n > 0 else None,
        "total_pnl":  round(total_pnl, 2),
        "total_cost": round(total_cost, 2),
        "roi_pct":    round(total_pnl / total_cost * 100, 2) if total_cost > 0 else None,
        "avg_win":    round(sum(wins_pnls) / len(wins_pnls), 2) if wins_pnls else None,
        "avg_loss":   round(sum(losses_pnls) / len(losses_pnls), 2) if losses_pnls else None,
    }


def _group_by(trades: list[dict], keyfn) -> dict:
    buckets = defaultdict(list)
    for t in trades:
        try:
            key = keyfn(t)
        except Exception:
            key = "unknown"
        buckets[str(key) if key is not None else "unknown"].append(t)
    return {k: _stats(v) for k, v in sorted(buckets.items())}


def analyze_performance(snapshots_dir: str) -> dict:
    """
    Run every breakdown. Returns a dict safe for JSON dump / pretty printing.
    """
    trades, warnings = _load_all_snapshots(snapshots_dir)

    if not trades:
        return {
            "n_snapshots_parsed": 0,
            "n_closed_trades":    0,
            "warnings":           warnings or ["no closed-out trades found"],
            "overall":            _stats([]),
            "by_vol_signal":      {},
            "by_score_bucket":    {},
            "by_option_type":     {},
            "by_dte_bucket":      {},
            "by_iv_rank_label":   {},
            "by_gex_signal":      {},
            "by_skew_signal":     {},
            "by_sector":          {},
            "top_wins":           [],
            "top_losses":         [],
        }

    overall = _stats(trades)

    by_vol_signal    = _group_by(trades, lambda t: t.get("vol_signal"))
    by_score_bucket  = _group_by(trades, lambda t: _bucket_score(t.get("score")))
    by_option_type   = _group_by(trades, lambda t: (t.get("option_type") or "").upper())
    by_dte_bucket    = _group_by(trades, lambda t: _bucket_dte(t.get("dte")))
    by_iv_rank_label = _group_by(trades, lambda t: (t.get("iv_rank_label") or "unknown").split(" (")[0])
    by_gex_signal    = _group_by(trades, lambda t: t.get("gex_signal"))
    by_skew_signal   = _group_by(trades, lambda t: t.get("skew_signal"))
    by_sector        = _group_by(trades, lambda t: t.get("_sector") or "Unknown")

    ranked = sorted(trades, key=lambda t: float(t.get("pnl_per_contract") or 0), reverse=True)

    def _summ(t: dict) -> dict:
        return {
            "date":    t.get("_snap_date"),
            "symbol":  t.get("symbol"),
            "type":    t.get("option_type"),
            "strike":  t.get("strike"),
            "expiry":  t.get("expiry"),
            "score":   t.get("score"),
            "entry":   t.get("entry_price_mid"),
            "close":   t.get("close_price_next_day"),
            "pnl":     t.get("pnl_per_contract"),
        }

    # Distinct snapshots parsed
    snap_dates = {t.get("_snap_date") for t in trades}

    return {
        "n_snapshots_parsed":  len(snap_dates),
        "n_closed_trades":     len(trades),
        "warnings":            warnings,
        "overall":             overall,
        "by_vol_signal":       by_vol_signal,
        "by_score_bucket":     by_score_bucket,
        "by_option_type":      by_option_type,
        "by_dte_bucket":       by_dte_bucket,
        "by_iv_rank_label":    by_iv_rank_label,
        "by_gex_signal":       by_gex_signal,
        "by_skew_signal":      by_skew_signal,
        "by_sector":           by_sector,
        "top_wins":            [_summ(t) for t in ranked[:5]],
        "top_losses":          [_summ(t) for t in ranked[-5:][::-1]],
    }
