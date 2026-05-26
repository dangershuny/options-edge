"""
Strategy tracker — live performance vs backtest baseline + signal-bucket
breakdowns to surface insightful tweaks.

Reads engine_state.db for every closed trade tagged with a strategy_id
(see tag_strategy in engine.state). Computes:

  1. Headline live stats: n_trades, win_rate, avg_return, expectancy,
     Sharpe, max drawdown, ending equity (assuming $200/trade).

  2. Backtest comparison: how live performance matches what the
     strategy_backtest predicted. Big divergence = strategy isn't
     behaving as modeled.

  3. Signal-bucket breakdowns — for each captured entry signal
     (spread_pct, score, vol_signal, gex_signal, dte, etc.), break the
     trades into buckets and compute per-bucket win rate + avg return.
     Surfaces "tighter spread = more wins" or "shorter DTE underperforms"
     so the operator can make a data-driven tweak.

  4. Outliers — trades that performed wildly differently from the median.
     Worth a manual look to understand WHY they were different.

  5. Open positions — what's currently live under this strategy and how
     they're doing relative to the backtest expectations.

Usage:
    python -m tools.strategy_tracker                       # full report
    python -m tools.strategy_tracker --strategy strategy_v1
    python -m tools.strategy_tracker --since 2026-05-14    # date filter
    python -m tools.strategy_tracker --csv                 # dump per-trade csv

Output: logs/strategy_tracker_report.md
"""
from __future__ import annotations

import argparse
import csv as _csv
import json
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

REPORT_PATH = REPO_ROOT / "logs" / "strategy_tracker_report.md"
PER_TRADE_CSV = REPO_ROOT / "logs" / "strategy_tracker_trades.csv"

# Backtest baseline expectations for strategy_v1 (from 5/14 sweep). Live
# numbers diverging materially from these indicate the strategy isn't
# behaving as the backtest projected — investigate before tweaking.
BACKTEST_BASELINE = {
    "strategy_v1": {
        # v1.2 baseline (2026-05-26 retune, 1,794 candidates):
        # added DTE 14-45 window. 10 trades, 40% wr, +26.3% avg,
        # sharpe +1.20, max DD -4.3%. WARN fires in EOD if live wr
        # drops >15pts below 40% (i.e., below 25%).
        "n_trades": 10,
        "win_rate": 0.40,
        "avg_return": 0.263,
        "median_return": 0.0,
        "max_drawdown": -0.043,
        "sharpe": 1.20,
    },
}

CAPITAL_PER_TRADE = 200.0
STARTING_CAPITAL = 4000.0


# ── Data loading ─────────────────────────────────────────────────────────────

def load_trades(strategy_id: str | None = None,
                 since: str | None = None) -> list[dict]:
    db = REPO_ROOT / "engine_state.db"
    if not db.exists():
        return []
    where = ["status IN ('closed','open','closing')"]
    params: list = []
    if strategy_id:
        where.append("strategy_id = ?")
        params.append(strategy_id)
    if since:
        where.append("date(entry_date) >= ?")
        params.append(since)
    sql = "SELECT * FROM positions WHERE " + " AND ".join(where) + " ORDER BY entry_date, id"
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        rows = [dict(r) for r in c.execute(sql, params)]
    for r in rows:
        try:
            r["context"] = json.loads(r.get("entry_context_json") or "{}")
        except Exception:
            r["context"] = {}
        if r["status"] == "closed" and r.get("realized_pl") is not None and r.get("entry_price"):
            # Compute pct return from realized P&L
            entry_total = float(r["entry_price"]) * int(r.get("qty") or 1) * 100
            if entry_total > 0:
                r["pnl_pct"] = float(r["realized_pl"]) / entry_total
            else:
                r["pnl_pct"] = None
        else:
            r["pnl_pct"] = None
    return rows


# ── Stats ────────────────────────────────────────────────────────────────────

def compute_headline(trades: list[dict]) -> dict:
    closed = [t for t in trades if t["status"] == "closed" and t["pnl_pct"] is not None]
    if not closed:
        return {"n_closed": 0, "n_open": sum(1 for t in trades if t["status"] != "closed")}

    pls = [float(t["realized_pl"] or 0) for t in closed]
    pcts = [t["pnl_pct"] for t in closed]
    wins = [p for p in pls if p > 0]
    losses = [p for p in pls if p < 0]

    win_rate = len(wins) / len(closed)
    avg_return = statistics.mean(pcts)
    median_return = statistics.median(pcts)
    total_realized = sum(pls)

    # Sharpe-ish on per-trade returns
    sharpe = None
    if len(pcts) >= 3:
        std = statistics.stdev(pcts)
        if std > 0:
            sharpe = (statistics.mean(pcts) / std) * math.sqrt(len(pcts))

    # Equity curve
    equity = STARTING_CAPITAL
    max_eq = STARTING_CAPITAL
    max_dd = 0.0
    curve = [STARTING_CAPITAL]
    for t in closed:
        delta = CAPITAL_PER_TRADE * t["pnl_pct"]
        equity += delta
        if equity > max_eq:
            max_eq = equity
        if max_eq > 0:
            dd = (equity - max_eq) / max_eq
            if dd < max_dd:
                max_dd = dd
        curve.append(equity)

    return {
        "n_closed": len(closed),
        "n_open": sum(1 for t in trades if t["status"] != "closed"),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "win_rate": win_rate,
        "avg_return": avg_return,
        "median_return": median_return,
        "max_win_pct": max(pcts),
        "max_loss_pct": min(pcts),
        "total_realized_dollar": total_realized,
        "expectancy_dollar": CAPITAL_PER_TRADE * avg_return,
        "ending_equity": equity,
        "max_drawdown": max_dd,
        "sharpe": sharpe,
    }


def compare_to_baseline(strategy_id: str, headline: dict) -> dict:
    base = BACKTEST_BASELINE.get(strategy_id)
    if not base or headline.get("n_closed", 0) == 0:
        return {}
    out = {}
    for k in ("win_rate", "avg_return", "max_drawdown", "sharpe"):
        live = headline.get(k)
        bkt = base.get(k)
        if live is None or bkt is None:
            continue
        out[k] = {"live": live, "backtest": bkt, "delta": live - bkt}
    return out


# ── Signal-bucket breakdowns ────────────────────────────────────────────────

NUMERIC_SIGNALS = [
    ("spread_pct",  lambda c: _spread(c),       [(0, 0.05), (0.05, 0.10),
                                                  (0.10, 0.15), (0.15, 1.0)]),
    ("score",       lambda c: c.get("score"),   [(0, 50), (50, 70), (70, 85), (85, 100)]),
    ("dte_entry",   lambda c: c.get("dte"),     [(0, 14), (14, 30), (30, 60), (60, 999)]),
    ("iv_rank",     lambda c: c.get("iv_rank"), [(0, 0.25), (0.25, 0.50),
                                                  (0.50, 0.75), (0.75, 1.0)]),
    ("trend_pct",   lambda c: c.get("trend_pct"),
                                                  [(-1, -0.05), (-0.05, 0.0),
                                                   (0.0, 0.05), (0.05, 1.0)]),
]
CATEGORICAL_SIGNALS = [
    "vol_signal", "flow_signal", "skew_signal", "gex_signal",
    "insider_signal", "short_signal",
]


def _spread(c: dict) -> float | None:
    bid = c.get("bid"); ask = c.get("ask")
    try:
        bid = float(bid or 0); ask = float(ask or 0)
    except Exception:
        return None
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2
    if mid <= 0:
        return None
    return (ask - bid) / mid


def numeric_buckets(trades: list[dict], name: str,
                     getter, bucket_edges: list[tuple]) -> list[dict]:
    out: list[dict] = []
    closed = [t for t in trades if t["status"] == "closed" and t["pnl_pct"] is not None]
    for lo, hi in bucket_edges:
        bucket = [t for t in closed
                   if getter(t.get("context") or {}) is not None
                   and lo <= getter(t["context"]) < hi]
        if not bucket:
            continue
        pcts = [t["pnl_pct"] for t in bucket]
        out.append({
            "bucket": f"{lo}-{hi}",
            "n": len(bucket),
            "win_rate": sum(1 for p in pcts if p > 0) / len(pcts),
            "avg_return": statistics.mean(pcts),
            "median_return": statistics.median(pcts),
        })
    return out


def categorical_buckets(trades: list[dict], field: str) -> list[dict]:
    out: list[dict] = []
    closed = [t for t in trades if t["status"] == "closed" and t["pnl_pct"] is not None]
    groups: dict = defaultdict(list)
    for t in closed:
        v = (t.get("context") or {}).get(field)
        if v is None:
            continue
        groups[str(v)].append(t["pnl_pct"])
    for k, pcts in sorted(groups.items(), key=lambda kv: -statistics.mean(kv[1])):
        if len(pcts) == 0:
            continue
        out.append({
            "value": k,
            "n": len(pcts),
            "win_rate": sum(1 for p in pcts if p > 0) / len(pcts),
            "avg_return": statistics.mean(pcts),
        })
    return out


# ── Open-position health ─────────────────────────────────────────────────────

def open_position_status(trades: list[dict]) -> list[dict]:
    """Compute live mark-to-market on currently open positions."""
    open_t = [t for t in trades if t["status"] in ("open", "closing")]
    if not open_t:
        return []
    out: list[dict] = []
    try:
        from broker import alpaca
    except Exception:
        return out
    for t in open_t:
        try:
            q = alpaca.get_quote(t["occ_symbol"])
            mid = (q.bid + q.ask) / 2 if (q.bid > 0 and q.ask > 0) else (q.bid or q.ask)
        except Exception:
            mid = None
        entry = float(t["entry_price"] or 0)
        pnl_pct = ((mid / entry) - 1) if (mid and entry > 0) else None
        held = (date.today() - datetime.strptime(t["entry_date"], "%Y-%m-%d").date()).days
        out.append({
            "id": t["id"],
            "occ_symbol": t["occ_symbol"],
            "entry": entry,
            "current_mid": mid,
            "pnl_pct": pnl_pct,
            "held_days": held,
            "qty": t["qty"],
        })
    return out


# ── Outlier detection ───────────────────────────────────────────────────────

def find_outliers(trades: list[dict]) -> dict:
    closed = [t for t in trades if t["status"] == "closed" and t["pnl_pct"] is not None]
    if len(closed) < 3:
        return {}
    pcts = [t["pnl_pct"] for t in closed]
    med = statistics.median(pcts)
    sorted_t = sorted(closed, key=lambda t: t["pnl_pct"])
    best = sorted_t[-1]
    worst = sorted_t[0]
    return {"best": best, "worst": worst, "median": med}


# ── Render ──────────────────────────────────────────────────────────────────

def fmt_pct(v): return f"{v*100:+.1f}%" if v is not None else "—"
def fmt_dollar(v): return f"${v:+,.0f}" if v is not None else "—"


def render_md(strategy_id: str, headline: dict, comparison: dict,
              num_breakdowns: dict, cat_breakdowns: dict,
              opens: list, outliers: dict, trades: list) -> str:
    lines: list[str] = []
    lines.append(f"# Strategy tracker — `{strategy_id}`")
    lines.append("")
    lines.append(f"_Run at {datetime.now().isoformat(timespec='seconds')}_")
    lines.append("")

    # ── Headline ──
    lines.append("## Live performance")
    lines.append("")
    if headline.get("n_closed", 0) == 0:
        lines.append(f"- **No closed trades yet** under `{strategy_id}`.")
        lines.append(f"- Open positions: {headline.get('n_open', 0)}")
        if not opens:
            lines.append("")
            lines.append("Nothing to report yet — strategy hasn't produced its first trade.")
            return "\n".join(lines)
    else:
        lines.append(f"| metric | value |")
        lines.append(f"|---|---:|")
        lines.append(f"| Closed trades | {headline['n_closed']} |")
        lines.append(f"| Open positions | {headline['n_open']} |")
        lines.append(f"| Wins / Losses | {headline['n_wins']} / {headline['n_losses']} |")
        lines.append(f"| Win rate | {fmt_pct(headline['win_rate'])} |")
        lines.append(f"| Avg return / trade | {fmt_pct(headline['avg_return'])} |")
        lines.append(f"| Median return | {fmt_pct(headline['median_return'])} |")
        lines.append(f"| Max single win | {fmt_pct(headline['max_win_pct'])} |")
        lines.append(f"| Max single loss | {fmt_pct(headline['max_loss_pct'])} |")
        lines.append(f"| Total realized | {fmt_dollar(headline['total_realized_dollar'])} |")
        lines.append(f"| Expectancy / trade (assumes ${CAPITAL_PER_TRADE:.0f}) | {fmt_dollar(headline['expectancy_dollar'])} |")
        lines.append(f"| Ending equity (from ${STARTING_CAPITAL:.0f}) | {fmt_dollar(headline['ending_equity'])} |")
        lines.append(f"| Max drawdown | {fmt_pct(headline['max_drawdown'])} |")
        sh = f"{headline['sharpe']:+.2f}" if headline.get("sharpe") is not None else "—"
        lines.append(f"| Sharpe-ish | {sh} |")
        lines.append("")

    # ── Backtest comparison ──
    if comparison:
        lines.append("## Live vs backtest baseline")
        lines.append("")
        lines.append("| metric | live | backtest | delta |")
        lines.append("|---|---:|---:|---:|")
        for k in ("win_rate", "avg_return", "max_drawdown", "sharpe"):
            if k not in comparison: continue
            c = comparison[k]
            live = c["live"]; bkt = c["backtest"]; d = c["delta"]
            fmt = fmt_pct if k in ("win_rate","avg_return","max_drawdown") else lambda v: f"{v:+.2f}"
            lines.append(f"| {k} | {fmt(live)} | {fmt(bkt)} | {fmt(d)} |")
        lines.append("")
        lines.append("> Material divergence (live diverging from backtest by >20% on win rate, "
                     "or expectancy flipping sign) is a signal that the strategy isn't behaving "
                     "as modeled. Investigate before tweaking.")
        lines.append("")

    # ── Open positions ──
    if opens:
        lines.append("## Currently open under this strategy")
        lines.append("")
        lines.append("| id | OCC | qty | entry | current mid | pnl | held |")
        lines.append("|---|---|---:|---:|---:|---:|---:|")
        for o in opens:
            lines.append(f"| {o['id']} | {o['occ_symbol']} | {o['qty']} | "
                         f"${o['entry']:.2f} | "
                         f"{('$'+format(o['current_mid'],'.2f')) if o['current_mid'] else '—'} | "
                         f"{fmt_pct(o['pnl_pct'])} | {o['held_days']}d |")
        lines.append("")

    # ── Numeric breakdowns ──
    if num_breakdowns:
        lines.append("## Performance by numeric signal bucket")
        lines.append("")
        for name, rows in num_breakdowns.items():
            if not rows:
                continue
            lines.append(f"### {name}")
            lines.append("")
            lines.append("| bucket | n | win rate | avg ret | median ret |")
            lines.append("|---|---:|---:|---:|---:|")
            for r in rows:
                lines.append(f"| {r['bucket']} | {r['n']} | "
                             f"{fmt_pct(r['win_rate'])} | "
                             f"{fmt_pct(r['avg_return'])} | "
                             f"{fmt_pct(r['median_return'])} |")
            lines.append("")

    # ── Categorical breakdowns ──
    if cat_breakdowns:
        lines.append("## Performance by categorical signal")
        lines.append("")
        for name, rows in cat_breakdowns.items():
            if not rows or len(rows) < 2:
                continue
            lines.append(f"### {name}")
            lines.append("")
            lines.append("| value | n | win rate | avg ret |")
            lines.append("|---|---:|---:|---:|")
            for r in rows:
                lines.append(f"| {r['value']} | {r['n']} | "
                             f"{fmt_pct(r['win_rate'])} | "
                             f"{fmt_pct(r['avg_return'])} |")
            lines.append("")

    # ── Outliers ──
    if outliers:
        lines.append("## Outliers worth inspecting")
        lines.append("")
        for label in ("best", "worst"):
            t = outliers.get(label)
            if not t:
                continue
            ctx = t.get("context") or {}
            lines.append(f"### {label}: {t['occ_symbol']} (id={t['id']}, "
                         f"{fmt_pct(t['pnl_pct'])})")
            lines.append("")
            lines.append(f"- Entry: {t['entry_date']} @ ${t['entry_price']:.2f}")
            lines.append(f"- Exit:  {t.get('exit_date','?')} @ "
                         f"${t.get('exit_price') or 0:.2f}  ({t.get('exit_reason','')})")
            lines.append(f"- Signals: skew={ctx.get('skew_signal')}, "
                         f"vol={ctx.get('vol_signal')}, score={ctx.get('score')}, "
                         f"iv_pct={ctx.get('iv_pct')}, dte={ctx.get('dte')}")
            lines.append("")

    # ── Full trade list ──
    closed = [t for t in trades if t["status"] == "closed"]
    if closed:
        lines.append("## Every closed trade")
        lines.append("")
        lines.append("| entry | exit | OCC | qty | entry $ | exit $ | pnl% | $pl | reason |")
        lines.append("|---|---|---|---:|---:|---:|---:|---:|---|")
        for t in closed:
            lines.append(f"| {t['entry_date']} | {t.get('exit_date','-')} | "
                         f"{t['occ_symbol']} | {t['qty']} | "
                         f"${t['entry_price']:.2f} | "
                         f"${t.get('exit_price') or 0:.2f} | "
                         f"{fmt_pct(t.get('pnl_pct'))} | "
                         f"${t.get('realized_pl') or 0:+.0f} | "
                         f"{(t.get('exit_reason') or '')[:40]} |")
        lines.append("")

    return "\n".join(lines)


# ── Per-trade CSV ────────────────────────────────────────────────────────────

def write_csv(trades: list[dict]) -> None:
    closed = [t for t in trades if t["status"] == "closed"]
    if not closed:
        return
    PER_TRADE_CSV.parent.mkdir(parents=True, exist_ok=True)
    base_cols = ["id", "entry_date", "exit_date", "occ_symbol", "qty",
                 "entry_price", "exit_price", "realized_pl", "pnl_pct",
                 "exit_reason", "strategy_id", "strategy_version"]
    ctx_cols = sorted({k for t in closed for k in (t.get("context") or {}).keys()})
    with PER_TRADE_CSV.open("w", encoding="utf-8", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=base_cols + ctx_cols, extrasaction="ignore")
        w.writeheader()
        for t in closed:
            row = {k: t.get(k) for k in base_cols}
            for k in ctx_cols:
                row[k] = (t.get("context") or {}).get(k)
            w.writerow(row)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="strategy_v1",
                    help="strategy_id to filter on (default strategy_v1)")
    ap.add_argument("--since", help="YYYY-MM-DD lower bound on entry_date")
    ap.add_argument("--csv", action="store_true", help="also write per-trade CSV")
    args = ap.parse_args()

    trades = load_trades(strategy_id=args.strategy, since=args.since)
    print(f"Loaded {len(trades)} trades for strategy='{args.strategy}'"
          + (f" since {args.since}" if args.since else ""))

    headline = compute_headline(trades)
    comparison = compare_to_baseline(args.strategy, headline)

    num_breakdowns = {name: numeric_buckets(trades, name, getter, edges)
                       for name, getter, edges in NUMERIC_SIGNALS}
    cat_breakdowns = {name: categorical_buckets(trades, name)
                       for name in CATEGORICAL_SIGNALS}
    opens = open_position_status(trades)
    outliers = find_outliers(trades)

    md = render_md(args.strategy, headline, comparison,
                    num_breakdowns, cat_breakdowns, opens, outliers, trades)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(md, encoding="utf-8")
    print(f"\nWrote {REPORT_PATH}\n")
    print(md)

    if args.csv:
        write_csv(trades)
        print(f"\nWrote {PER_TRADE_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
