"""
Signal-edge backtest — find which signal components actually predict P&L.

Uses two data sources we've been logging the whole time:

  1. snapshots/*.json — every contract scored each day, with all signal
     deltas, score components, vol_signal, flow_signal, and entry-time
     bid/ask.

  2. chain_surface table in engine_state.db — historical option prices
     (bid, ask, last_price, iv, spot) snapshotted each trading day.
     460k rows / 11 dates / 191 symbols as of 2026-05-06.

For every contract that appears in BOTH sources, this tool:

  - Reads the score + signal contributions from snapshot day D.
  - Looks up the same OCC's mid price in chain_surface on day D+N.
  - Computes realized return at horizons N=1, 3, 5 trading days.
  - Spreads cost, theta decay, and gap exposure are baked into the mid-
    to-mid return.

Then it computes per-signal regression — which signals correlated with
profitable returns vs which were noise, and at which horizon. Output is
a markdown report identifying:

  - Signals with significant positive predictive value (keep / weight up)
  - Signals near zero (noise — drop or downweight)
  - Signals with negative predictive value (anti-signal — invert or drop)
  - Best signal combinations (interaction effects)

Usage:
  python -m tools.signal_edge_backtest             # full report to stdout + md
  python -m tools.signal_edge_backtest --horizon 1 # 1-day return only
  python -m tools.signal_edge_backtest --csv       # also write per-trade CSV
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

SNAPSHOT_DIR = REPO_ROOT / "snapshots"
DB_PATH = REPO_ROOT / "engine_state.db"
REPORT_PATH = REPO_ROOT / "logs" / "signal_edge_report.md"
PER_TRADE_CSV = REPO_ROOT / "logs" / "signal_edge_trades.csv"


# Signal columns to analyze. Each one is computed at scoring time and
# stored in the snapshot. We test their predictive value against
# realized N-day returns.
SIGNAL_COLS = [
    "score",
    "iv_pct",
    "rv_pct",
    "iv_rv_spread",
    "sentiment_delta",
    "news_drift_delta",
    "insider_delta",
    "short_delta",
    "blocks_delta",
    "catalyst_delta",
    "pin_delta",
]
CATEGORICAL_COLS = [
    "vol_signal",
    "flow_signal",
    "skew_signal",
    "gex_signal",
    "iv_rank_label",
    "insider_signal",
    "short_signal",
    "blocks_signal",
    "pin_risk",
]


def _trading_days_after(d: str, n: int) -> str:
    """Return YYYY-MM-DD that is n trading days after d (skip weekends)."""
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    added = 0
    while added < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            added += 1
    return dt.isoformat()


def _occ_key(symbol: str, opt_type: str, strike: float, expiry: str) -> str:
    return f"{symbol}|{(opt_type or '').lower()[:1]}|{float(strike):.2f}|{expiry}"


def load_snapshots() -> list[dict]:
    """Return every (snapshot_date, candidate) row. Each candidate has
    score, all signal_deltas, bid/ask, expiry, strike, type."""
    rows: list[dict] = []
    for fpath in sorted(SNAPSHOT_DIR.glob("*_auto-*.json")):
        try:
            d = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        snap_date = d.get("snapshot_date")
        if not snap_date:
            continue
        # Use 'universe' if present (every analyzed contract); fallback to
        # 'trades' (scorer-passed only). Universe is more honest for
        # backtest because it includes contracts the scorer rejected —
        # we want to test whether those rejections were correct.
        cands = d.get("universe") or d.get("trades") or []
        for c in cands:
            sym = c.get("symbol")
            opt_type = c.get("type") or c.get("option_type")
            strike = c.get("strike")
            expiry = c.get("expiry")
            if not all([sym, opt_type, strike, expiry]):
                continue
            row = {
                "snapshot_date": snap_date,
                "symbol": sym,
                "option_type": (opt_type or "").lower(),
                "strike": float(strike),
                "expiry": expiry,
                "occ_key": _occ_key(sym, opt_type, strike, expiry),
            }
            # Capture every signal we'll regress against
            for col in SIGNAL_COLS + CATEGORICAL_COLS + [
                "bid", "ask", "entry_price", "stock_price",
                "stock_price_at_snap", "dte",
            ]:
                row[col] = c.get(col)
            rows.append(row)
    return rows


def load_chain_surface() -> dict[tuple, dict]:
    """Index chain_surface by (snapshot_date, occ_key) → {bid, ask, mid, ...}."""
    if not DB_PATH.exists():
        return {}
    out: dict[tuple, dict] = {}
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        for r in c.execute(
            "SELECT snapshot_date, symbol, option_type, strike, expiry, "
            "bid, ask, last_price, iv, spot "
            "FROM chain_surface"
        ):
            key = _occ_key(r["symbol"], r["option_type"], r["strike"],
                           r["expiry"])
            mid = (float(r["bid"] or 0) + float(r["ask"] or 0)) / 2.0
            if mid <= 0:
                mid = float(r["last_price"] or 0)
            if mid <= 0:
                continue
            out[(r["snapshot_date"], key)] = {
                "bid": float(r["bid"] or 0),
                "ask": float(r["ask"] or 0),
                "mid": mid,
                "iv": float(r["iv"] or 0),
                "spot": float(r["spot"] or 0),
            }
    return out


def merge_returns(snapshots: list[dict],
                  surface: dict[tuple, dict],
                  horizons: list[int]) -> list[dict]:
    """For each snapshot row, look up forward-day prices and compute return.
    Returns rows that have AT LEAST one horizon priced. Each row gets
    pnl_d1, pnl_d3, pnl_d5 (any may be None)."""
    enriched: list[dict] = []
    for r in snapshots:
        d = r["snapshot_date"]
        # Entry price = snapshot mid
        bid0, ask0 = r.get("bid"), r.get("ask")
        if bid0 is None or ask0 is None or float(bid0) <= 0 or float(ask0) <= 0:
            # Fall back to entry_price field if bid/ask missing
            entry = r.get("entry_price")
            if entry is None or float(entry) <= 0:
                continue
            mid_d0 = float(entry)
        else:
            mid_d0 = (float(bid0) + float(ask0)) / 2.0

        if mid_d0 <= 0:
            continue  # cannot compute return without entry mid

        any_horizon_ok = False
        for n in horizons:
            d_future = _trading_days_after(d, n)
            future = surface.get((d_future, r["occ_key"]))
            if not future:
                r[f"pnl_d{n}"] = None
                continue
            mid_dn = future["mid"]
            if mid_dn <= 0:
                r[f"pnl_d{n}"] = None
                continue
            ret = (mid_dn / mid_d0) - 1
            if math.isnan(ret) or math.isinf(ret):
                r[f"pnl_d{n}"] = None
                continue
            # Clip extreme outliers — option prices can quote 0.01 to 5.00
            # for the same contract within minutes; cap at ±300% per horizon.
            r[f"pnl_d{n}"] = max(min(ret, 3.0), -0.95)
            any_horizon_ok = True
        if any_horizon_ok:
            r["mid_d0"] = mid_d0
            enriched.append(r)
    return enriched


def _avg_ranks(values: list[float]) -> list[float]:
    """Return ranks 1..n with TIES averaged. The naive {value: rank}
    dict approach (a) silently drops duplicates and (b) gives every
    duplicate the same arbitrary rank. For signals like short_delta or
    pin_delta where most rows are 0 with a few non-zero values, the
    naive method computes a bogus rho near ±1. This implementation
    matches scipy.stats.spearmanr's tie handling."""
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j+1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j + 2) / 2  # average of (i+1) .. (j+1)
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def spearman_rho(pairs: list[tuple[float, float]]) -> float | None:
    """Spearman rank correlation with proper tie handling. Computes
    Pearson correlation on the average-ranked values. Returns None if
    n<3 or if either rank list is constant (no variance → undefined)."""
    n = len(pairs)
    if n < 3:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    rx = _avg_ranks(xs)
    ry = _avg_ranks(ys)
    mean_x = sum(rx) / n
    mean_y = sum(ry) / n
    cov = sum((rx[i] - mean_x) * (ry[i] - mean_y) for i in range(n))
    var_x = sum((r - mean_x) ** 2 for r in rx)
    var_y = sum((r - mean_y) ** 2 for r in ry)
    if var_x == 0 or var_y == 0:
        return None  # constant signal — Spearman undefined
    return cov / ((var_x * var_y) ** 0.5)


def analyze(rows: list[dict], horizons: list[int]) -> dict:
    """Produce the per-signal report dict."""
    out: dict = {"n_total": len(rows), "horizons": horizons, "signals": {}}

    for h in horizons:
        col = f"pnl_d{h}"
        priced = [r for r in rows if r.get(col) is not None]
        if not priced:
            continue

        # Headline at this horizon
        avg = sum(r[col] for r in priced) / len(priced)
        wins = [r for r in priced if r[col] > 0]
        out.setdefault("headline", {})[f"d{h}"] = {
            "n": len(priced),
            "win_rate": len(wins) / len(priced),
            "avg_return": avg,
            "median_return": sorted(r[col] for r in priced)[len(priced)//2],
        }

        # Numeric signals — Spearman correlation
        for sig in SIGNAL_COLS:
            pairs = [(float(r[sig]), float(r[col])) for r in priced
                     if r.get(sig) is not None
                     and isinstance(r[sig], (int, float))
                     and not (isinstance(r[sig], float) and math.isnan(r[sig]))]
            rho = spearman_rho(pairs) if len(pairs) >= 5 else None
            if rho is None:
                continue
            # Bucket by quartile
            sorted_pairs = sorted(pairs, key=lambda p: p[0])
            n = len(sorted_pairs)
            q1 = sorted_pairs[:n//4]
            q4 = sorted_pairs[3*n//4:]
            q1_avg = sum(p[1] for p in q1) / len(q1) if q1 else 0
            q4_avg = sum(p[1] for p in q4) / len(q4) if q4 else 0
            out["signals"].setdefault(sig, {})[f"d{h}"] = {
                "n": len(pairs),
                "rho": round(rho, 4),
                "q1_low_avg_return": round(q1_avg, 4),
                "q4_high_avg_return": round(q4_avg, 4),
                "spread_q4_minus_q1": round(q4_avg - q1_avg, 4),
            }

        # Categorical signals — group means
        for sig in CATEGORICAL_COLS:
            groups: dict[str, list[float]] = defaultdict(list)
            for r in priced:
                v = r.get(sig)
                if v is None:
                    continue
                groups[str(v)].append(float(r[col]))
            if not groups:
                continue
            out["signals"].setdefault(sig, {})[f"d{h}_groups"] = {
                k: {"n": len(v),
                    "avg_return": round(sum(v)/len(v), 4),
                    "win_rate": round(sum(1 for x in v if x > 0)/len(v), 3)}
                for k, v in groups.items() if len(v) >= 3
            }
    return out


def render_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append(f"# Signal-edge backtest")
    lines.append(f"")
    lines.append(f"- Total snapshot×forward-pricing pairs: **{report['n_total']:,}**")
    lines.append(f"- Horizons tested: {report['horizons']}")
    lines.append(f"")
    lines.append(f"## Headline returns by horizon")
    lines.append(f"")
    lines.append(f"| horizon | n | win_rate | avg_return | median_return |")
    lines.append(f"|---|---:|---:|---:|---:|")
    for h, hd in (report.get("headline") or {}).items():
        lines.append(f"| {h} | {hd['n']:,} | {hd['win_rate']*100:.1f}% | "
                     f"{hd['avg_return']*100:+.2f}% | "
                     f"{hd['median_return']*100:+.2f}% |")
    lines.append(f"")

    lines.append(f"## Numeric signals (Spearman ρ vs realized return)")
    lines.append(f"")
    lines.append(f"Q1 = bottom 25% on signal value, Q4 = top 25%. "
                 f"`spread = Q4_avg - Q1_avg`. Positive ρ = signal predicts "
                 f"winners. Near-zero ρ = noise. Negative ρ = anti-signal.")
    lines.append(f"")
    for h in report["horizons"]:
        lines.append(f"### Horizon d{h}")
        lines.append(f"")
        lines.append(f"| signal | n | ρ | Q1 avg | Q4 avg | Q4-Q1 spread |")
        lines.append(f"|---|---:|---:|---:|---:|---:|")
        rows = []
        for sig, hdata in report["signals"].items():
            d = hdata.get(f"d{h}")
            if not d or "rho" not in d:
                continue
            rows.append((d["spread_q4_minus_q1"], sig, d))
        # Sort by absolute spread descending — highest-leverage signals first
        rows.sort(key=lambda r: -abs(r[0]))
        for spread, sig, d in rows:
            lines.append(f"| {sig} | {d['n']:,} | {d['rho']:+.3f} | "
                         f"{d['q1_low_avg_return']*100:+.2f}% | "
                         f"{d['q4_high_avg_return']*100:+.2f}% | "
                         f"{spread*100:+.2f}% |")
        lines.append(f"")

    lines.append(f"## Categorical signals (group win rates)")
    lines.append(f"")
    for h in report["horizons"]:
        lines.append(f"### Horizon d{h}")
        lines.append(f"")
        for sig, hdata in report["signals"].items():
            groups = hdata.get(f"d{h}_groups")
            if not groups:
                continue
            lines.append(f"**{sig}**")
            lines.append(f"")
            lines.append(f"| value | n | avg_return | win_rate |")
            lines.append(f"|---|---:|---:|---:|")
            for k, v in sorted(groups.items(), key=lambda x: -x[1]["avg_return"]):
                lines.append(f"| {k} | {v['n']:,} | "
                             f"{v['avg_return']*100:+.2f}% | "
                             f"{v['win_rate']*100:.1f}% |")
            lines.append(f"")

    return "\n".join(lines)


def write_per_trade_csv(rows: list[dict], horizons: list[int]) -> None:
    if not rows:
        return
    import csv
    cols = ["snapshot_date", "symbol", "option_type", "strike", "expiry",
            "mid_d0"] + [f"pnl_d{h}" for h in horizons] + SIGNAL_COLS + CATEGORICAL_COLS
    PER_TRADE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with PER_TRADE_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizons", type=str, default="1,3,5",
                    help="comma-separated trading-day horizons (default 1,3,5)")
    ap.add_argument("--csv", action="store_true",
                    help="also write per-trade CSV to logs/")
    args = ap.parse_args()

    horizons = [int(x) for x in args.horizons.split(",")]
    print(f"loading snapshots...")
    snaps = load_snapshots()
    print(f"  {len(snaps):,} snapshot rows")
    print(f"loading chain_surface...")
    surface = load_chain_surface()
    print(f"  {len(surface):,} (date, occ) entries")
    print(f"merging at horizons {horizons}...")
    rows = merge_returns(snaps, surface, horizons)
    print(f"  {len(rows):,} rows have at least one horizon priced")

    report = analyze(rows, horizons)
    md = render_markdown(report)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(md, encoding="utf-8")
    print(f"\nwrote {REPORT_PATH}")
    if args.csv:
        write_per_trade_csv(rows, horizons)
        print(f"wrote {PER_TRADE_CSV}")

    print()
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
