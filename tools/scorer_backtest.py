"""
Scorer backtest — replays snapshot trades against historical prices to
measure whether `score` actually correlates with profitable outcomes.

Method:
  1. Walk every snapshot in `snapshots/YYYY-MM-DD_auto-*.json`
  2. For each trade row, fetch the underlying's close on the snapshot date
     and N days forward (default 5; capped at expiry)
  3. Compute realized return on the underlying
  4. Classify the trade outcome:
       - CALL wins if underlying up; loses if down
       - PUT wins if underlying down; loses if up
  5. Aggregate by score bucket; report win rate, mean return, sharpe-ish
  6. Compute correlation coefficient (Spearman rank) score → return

Output:
  logs/backtest-{ts}.json — full per-trade table
  prints summary table to stdout

This does NOT compute exact option P&L (would need historical option chains
which Alpaca free tier doesn't have). Underlying directional return is a
reasonable proxy for a short-DTE single-leg directional trade — theta and
IV crush would only deepen losses, not reverse winners.

Usage:
    python -m tools.scorer_backtest
    python -m tools.scorer_backtest --hold-days 5 --min-snapshots-per-bucket 5
"""
from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOTS = REPO_ROOT / "snapshots"

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _isnum(x: Any) -> bool:
    try:
        v = float(x)
        return not (math.isnan(v) or math.isinf(v))
    except Exception:
        return False


def _snapshot_date(path: Path) -> date | None:
    m = DATE_RE.search(path.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def _load_trades() -> list[dict]:
    rows: list[dict] = []
    for fp in sorted(SNAPSHOTS.glob("*_auto*.json")):
        snap_date = _snapshot_date(fp)
        if snap_date is None:
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for t in data.get("trades", []) or []:
            t = dict(t)
            t["_snapshot_date"] = snap_date.isoformat()
            t["_snapshot_path"] = str(fp.name)
            rows.append(t)
    return rows


def _fetch_close_series(symbols: set[str], start: date, end: date) -> dict[str, dict[date, float]]:
    """Pull daily closes for every symbol in one batched yfinance call."""
    import yfinance as yf
    if not symbols:
        return {}
    df = yf.download(
        list(symbols), start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
        progress=False, group_by="ticker", auto_adjust=True, threads=True,
    )
    out: dict[str, dict[date, float]] = {}
    if df is None or df.empty:
        return out
    if len(symbols) == 1:
        sym = next(iter(symbols))
        out[sym] = {idx.date(): float(row["Close"]) for idx, row in df.iterrows()
                    if not math.isnan(row.get("Close", float("nan")))}
        return out
    for sym in symbols:
        try:
            sub = df[sym]
        except Exception:
            continue
        out[sym] = {idx.date(): float(row["Close"]) for idx, row in sub.iterrows()
                    if "Close" in row and not math.isnan(float(row["Close"]))}
    return out


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None

    def _ranks(vs: list[float]) -> list[float]:
        order = sorted(range(n), key=lambda i: vs[i])
        ranks = [0.0] * n
        for r, i in enumerate(order, start=1):
            ranks[i] = float(r)
        return ranks

    rx = _ranks(xs)
    ry = _ranks(ys)
    mx = sum(rx) / n
    my = sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    denx = sum((r - mx) ** 2 for r in rx) ** 0.5
    deny = sum((r - my) ** 2 for r in ry) ** 0.5
    if denx == 0 or deny == 0:
        return None
    return num / (denx * deny)


def _next_trading_close(prices: dict[date, float], target: date,
                         max_skip: int = 5) -> tuple[date, float] | None:
    """Find the first available close on or after `target`."""
    for offset in range(max_skip + 1):
        d = target + timedelta(days=offset)
        if d in prices:
            return d, prices[d]
    return None


def run(hold_days: int, min_snapshots_per_bucket: int = 5) -> dict:
    rows = _load_trades()
    print(f"loaded {len(rows)} trade rows from snapshots")
    if not rows:
        return {"error": "no trades"}

    symbols = {t["symbol"] for t in rows if t.get("symbol")}
    earliest = min(t["_snapshot_date"] for t in rows)
    latest_needed = date.today()
    print(f"fetching prices for {len(symbols)} symbols from {earliest} to {latest_needed}")
    prices = _fetch_close_series(symbols, datetime.strptime(earliest, "%Y-%m-%d").date(),
                                  latest_needed)
    print(f"price coverage: {sum(1 for p in prices.values() if p)} symbols with data")

    enriched: list[dict] = []
    for t in rows:
        sym = t.get("symbol")
        if sym not in prices or not prices[sym]:
            continue
        snap_d = datetime.strptime(t["_snapshot_date"], "%Y-%m-%d").date()
        # Spot at snap (use snap_d or first trading day on/after)
        spot_now = _next_trading_close(prices[sym], snap_d)
        target_d = snap_d + timedelta(days=hold_days)
        spot_then = _next_trading_close(prices[sym], target_d)
        if not spot_now or not spot_then:
            continue
        ret = (spot_then[1] / spot_now[1]) - 1
        opt_type = (t.get("option_type") or "").lower()
        # underlying-direction outcome
        if opt_type == "call":
            won = ret > 0
        elif opt_type == "put":
            won = ret < 0
        else:
            continue
        score = float(t.get("score") or 0)
        enriched.append({
            "symbol": sym,
            "snap_date": t["_snapshot_date"],
            "score": score,
            "vol_signal": t.get("vol_signal"),
            "option_type": opt_type,
            "strike": t.get("strike"),
            "expiry": t.get("expiry"),
            "spot_at_snap": spot_now[1],
            "spot_after": spot_then[1],
            "underlying_return_pct": round(ret * 100, 2),
            # Trade-direction return (positive = profit on direction)
            "directional_return_pct": round((ret if opt_type == "call" else -ret) * 100, 2),
            "won": won,
        })

    if not enriched:
        return {"error": "no enriched rows (price coverage problem)"}

    # Bucket by score
    buckets = [(0, 50), (50, 60), (60, 70), (70, 80), (80, 90), (90, 101)]
    by_bucket: dict[str, list[dict]] = {f"{lo:>2}-{hi:>2}": [] for lo, hi in buckets}
    for e in enriched:
        for lo, hi in buckets:
            if lo <= e["score"] < hi:
                by_bucket[f"{lo:>2}-{hi:>2}"].append(e)
                break

    # Spearman rank correlation across all rows
    rho = _spearman(
        [e["score"] for e in enriched],
        [e["directional_return_pct"] for e in enriched],
    )

    print()
    print(f"{'bucket':<10} {'n':>4}  {'wins':>5}  {'win_rate':>8}  {'mean_dir_ret%':>14}  {'med_dir_ret%':>14}")
    print("-" * 65)
    summary: dict[str, dict] = {}
    for label, items in by_bucket.items():
        n = len(items)
        if n == 0:
            continue
        wins = sum(1 for x in items if x["won"])
        rates = [x["directional_return_pct"] for x in items]
        mean = statistics.fmean(rates)
        med = statistics.median(rates)
        win_rate = wins / n
        summary[label] = {
            "n": n, "wins": wins, "win_rate": round(win_rate, 3),
            "mean_dir_return_pct": round(mean, 2),
            "median_dir_return_pct": round(med, 2),
        }
        flag = " *low-n*" if n < min_snapshots_per_bucket else ""
        print(f"{label:<10} {n:>4}  {wins:>5}  {win_rate:>7.1%}  "
              f"{mean:>13.2f}  {med:>13.2f}{flag}")

    overall_n = len(enriched)
    overall_wins = sum(1 for e in enriched if e["won"])
    overall_mean = statistics.fmean(e["directional_return_pct"] for e in enriched)

    print()
    print(f"OVERALL: n={overall_n}  win_rate={overall_wins/overall_n:.1%}  "
          f"mean_dir_return={overall_mean:.2f}%")
    if rho is not None:
        print(f"Spearman rank correlation (score vs directional return): rho = {rho:+.3f}")
        print("  (~0 = no signal · |rho|>0.3 = something real · negative = inverse)")
    print()
    print("Interpretation:")
    if rho is not None and abs(rho) < 0.1:
        print("  >>> score is NOT correlated with outcome — backtest confirms the diagnosis.")
        print("  >>> raising min_score won't help; rebuild required.")
    elif rho is not None and rho > 0.2:
        print(f"  >>> weak-to-moderate positive correlation (rho={rho:.2f}); raising threshold may help.")
    elif rho is not None and rho < -0.1:
        print(f"  >>> NEGATIVE correlation (rho={rho:.2f}) — high-score picks lose more often. "
              "Inverting the score might be a free signal.")
    else:
        print(f"  >>> rho = {rho}, n too small or noisy.")

    out = {
        "ts": datetime.now().isoformat(),
        "hold_days": hold_days,
        "n_trades": overall_n,
        "win_rate": overall_wins / overall_n,
        "mean_dir_return_pct": overall_mean,
        "spearman_score_vs_return": rho,
        "by_bucket": summary,
        "trades": enriched,
    }
    out_path = LOG_DIR / f"backtest-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    out_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {out_path}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hold-days", type=int, default=5,
                    help="how many trading days forward to measure outcome (default 5)")
    ap.add_argument("--min-snapshots-per-bucket", type=int, default=5)
    args = ap.parse_args()
    run(args.hold_days, args.min_snapshots_per_bucket)
    return 0


if __name__ == "__main__":
    sys.exit(main())
