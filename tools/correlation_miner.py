"""
Correlation miner — find which signal combinations actually predict
profitable trades in our historical dataset.

The signal_edge_backtest tool tests pre-designed strategies. This tool
goes the other direction: instead of asking "does my hypothesis work?",
it asks "what combinations show predictive power that we haven't even
hypothesized about yet?"

Four analyses, each ranked by predictive lift over the base win rate:

  1. Univariate lift
       For each numeric signal: split into quantile buckets, compute
       per-bucket win rate. The bucket with the largest deviation from
       baseline is a candidate filter.
       For each categorical signal: per-value win rate.

  2. Pairwise interactions
       For pairs of signals, build 2D win-rate tables. Identify cells
       where the combination is materially better (or worse) than the
       sum of individual univariate effects. Surfaces non-additive
       interactions (e.g., "BULLISH skew is +5pts alone, BUY VOL is
       +3pts alone, but together +28pts").

  3. Greedy decision-tree splits
       Find the best single binary split (max win-rate lift on either
       side). Then find the best second split conditional on the first.
       Gives 2-rule decision paths with measured performance.

  4. OLS regression
       Encode all features (categorical → one-hot, numeric → z-scored)
       and fit linear regression. Report top coefficients with
       confidence intervals. Manual implementation — no sklearn dep.

Sample-size guard: any finding with n < MIN_BUCKET_N is flagged
"low-confidence" so we don't chase noise from a 3-trade outlier.

Usage:
    python -m tools.correlation_miner               # full run, d1 horizon
    python -m tools.correlation_miner --horizon 3   # use 3-day return
    python -m tools.correlation_miner --top 10      # surface top N findings

Output: logs/correlation_findings.md
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

# Reuse the loaders/exit_basis from strategy_backtest
from tools.strategy_backtest import (
    load_snapshots, load_chain_surface, _trading_days_after, _spread_pct,
)

REPORT_PATH = REPO_ROOT / "logs" / "correlation_findings.md"

MIN_BUCKET_N = 8   # minimum trades in a bucket before we trust a finding
TOP_FINDINGS = 12

NUMERIC_SIGNALS = [
    "score", "iv_pct", "rv_pct", "iv_rv_spread", "sentiment_delta",
    "news_drift_delta", "insider_delta", "short_delta", "blocks_delta",
    "catalyst_delta", "pin_delta", "trend_pct", "trend_3d", "rsi14",
    "dte", "vol_oi_ratio",
    # Derived
    "spread_pct", "mid_price", "stock_price",
]
CATEGORICAL_SIGNALS = [
    "vol_signal", "flow_signal", "skew_signal", "gex_signal",
    "insider_signal", "short_signal", "blocks_signal",
    "option_type", "rsi_zone", "vix_regime",
]


# ── Build trade-pairs (entry context + forward return) ──────────────────────

def build_trade_pairs(rows: list[dict], surface: dict,
                       horizon: int = 1) -> list[dict]:
    """For each candidate row, compute entry mid + forward-horizon return."""
    out: list[dict] = []
    for r in rows:
        bid_s = float(r.get("bid") or 0)
        ask_s = float(r.get("ask") or 0)
        if bid_s <= 0 or ask_s <= 0:
            continue
        mid_s = (bid_s + ask_s) / 2.0
        if mid_s <= 0:
            continue

        future_date = _trading_days_after(r["snapshot_date"], horizon)
        f = surface.get((future_date, r["occ_key"]))
        if not f or f["bid"] <= 0:
            continue
        # Round-trip cost baked in: buy at ask, sell at future bid.
        pnl = (f["bid"] / ask_s) - 1
        # Clip outliers
        if math.isnan(pnl) or math.isinf(pnl):
            continue
        pnl = max(min(pnl, 3.0), -0.95)

        feature_row = dict(r)
        feature_row["pnl"] = pnl
        feature_row["spread_pct"] = _spread_pct(r) or 0.0
        feature_row["mid_price"] = mid_s
        feature_row["stock_price"] = float(r.get("stock_price") or 0)
        out.append(feature_row)
    return out


# ── Stats helpers ────────────────────────────────────────────────────────────

def _wr(returns: list[float]) -> float:
    return sum(1 for r in returns if r > 0) / len(returns) if returns else 0.0


def _avg(returns: list[float]) -> float:
    return statistics.mean(returns) if returns else 0.0


def _quantile_buckets(values: list[float], n_buckets: int = 4) -> list[tuple]:
    """Return [(lo, hi), ...] cut-points based on quantiles of the data."""
    sv = sorted(v for v in values if v is not None and not math.isnan(v))
    if not sv:
        return []
    out: list[tuple] = []
    for i in range(n_buckets):
        lo_idx = int(len(sv) * i / n_buckets)
        hi_idx = int(len(sv) * (i + 1) / n_buckets) - 1
        if hi_idx < lo_idx:
            continue
        out.append((sv[lo_idx], sv[hi_idx]))
    return out


# ── Analysis 1: Univariate lift ──────────────────────────────────────────────

def univariate_numeric(pairs: list[dict], baseline_wr: float) -> list[dict]:
    out: list[dict] = []
    for sig in NUMERIC_SIGNALS:
        vals = [(p.get(sig), p["pnl"]) for p in pairs
                 if p.get(sig) is not None
                 and isinstance(p.get(sig), (int, float))
                 and not (isinstance(p.get(sig), float) and math.isnan(p.get(sig)))]
        if len(vals) < MIN_BUCKET_N * 4:
            continue
        vals_only = [v[0] for v in vals]
        buckets = _quantile_buckets(vals_only, 4)
        for lo, hi in buckets:
            in_bucket = [pnl for v, pnl in vals if lo <= v <= hi]
            if len(in_bucket) < MIN_BUCKET_N:
                continue
            wr = _wr(in_bucket)
            avg = _avg(in_bucket)
            out.append({
                "kind": "numeric",
                "signal": sig,
                "rule": f"{lo:.3g} <= {sig} <= {hi:.3g}",
                "n": len(in_bucket),
                "win_rate": wr,
                "avg_return": avg,
                "lift": wr - baseline_wr,
            })
    return out


def univariate_categorical(pairs: list[dict], baseline_wr: float) -> list[dict]:
    out: list[dict] = []
    for sig in CATEGORICAL_SIGNALS:
        groups: dict = defaultdict(list)
        for p in pairs:
            v = p.get(sig)
            if v is None:
                continue
            groups[str(v)].append(p["pnl"])
        for k, returns in groups.items():
            if len(returns) < MIN_BUCKET_N:
                continue
            wr = _wr(returns)
            out.append({
                "kind": "categorical",
                "signal": sig,
                "rule": f"{sig} == {k!r}",
                "n": len(returns),
                "win_rate": wr,
                "avg_return": _avg(returns),
                "lift": wr - baseline_wr,
            })
    return out


# ── Analysis 2: Pairwise interactions ────────────────────────────────────────

def pairwise_categorical(pairs: list[dict], baseline_wr: float) -> list[dict]:
    """For every pair of categoricals, compute per-cell win rate.
    Return cells where the combination's lift exceeds either single
    rule's lift by INTERACTION_THRESHOLD."""
    INTERACTION_THRESHOLD = 0.05  # 5 percentage-point lift over best singleton
    out: list[dict] = []
    seen = set()
    for s1 in CATEGORICAL_SIGNALS:
        for s2 in CATEGORICAL_SIGNALS:
            if s1 == s2 or (s1, s2) in seen or (s2, s1) in seen:
                continue
            seen.add((s1, s2))
            cells: dict = defaultdict(list)
            for p in pairs:
                v1 = p.get(s1); v2 = p.get(s2)
                if v1 is None or v2 is None:
                    continue
                cells[(str(v1), str(v2))].append(p["pnl"])
            for (v1, v2), returns in cells.items():
                if len(returns) < MIN_BUCKET_N:
                    continue
                wr = _wr(returns)
                # Compare to singleton win rates for v1 and v2
                v1_returns = [p["pnl"] for p in pairs if str(p.get(s1)) == v1]
                v2_returns = [p["pnl"] for p in pairs if str(p.get(s2)) == v2]
                singleton_max = max(_wr(v1_returns) if v1_returns else 0,
                                     _wr(v2_returns) if v2_returns else 0)
                interaction_lift = wr - singleton_max
                if abs(interaction_lift) < INTERACTION_THRESHOLD:
                    continue
                out.append({
                    "kind": "pairwise",
                    "rule": f"{s1}=={v1!r} AND {s2}=={v2!r}",
                    "n": len(returns),
                    "win_rate": wr,
                    "avg_return": _avg(returns),
                    "lift": wr - baseline_wr,
                    "interaction_lift": interaction_lift,
                    "singleton_max": singleton_max,
                })
    return out


def pairwise_numeric_x_categorical(pairs: list[dict],
                                     baseline_wr: float) -> list[dict]:
    """For each (numeric, categorical) pair: bucket the numeric and
    look at win rate by (bucket, category)."""
    out: list[dict] = []
    for num in NUMERIC_SIGNALS:
        all_vals = [p.get(num) for p in pairs
                     if p.get(num) is not None
                     and isinstance(p.get(num), (int, float))
                     and not math.isnan(p.get(num))]
        if len(all_vals) < MIN_BUCKET_N * 4:
            continue
        buckets = _quantile_buckets(all_vals, 4)
        for cat in CATEGORICAL_SIGNALS:
            for lo, hi in buckets:
                cells: dict = defaultdict(list)
                for p in pairs:
                    v = p.get(num); c = p.get(cat)
                    if v is None or c is None or not isinstance(v, (int, float)):
                        continue
                    if not (lo <= v <= hi):
                        continue
                    cells[str(c)].append(p["pnl"])
                for k, returns in cells.items():
                    if len(returns) < MIN_BUCKET_N:
                        continue
                    wr = _wr(returns)
                    lift = wr - baseline_wr
                    if abs(lift) < 0.10:  # require 10pt lift to surface
                        continue
                    out.append({
                        "kind": "pairwise_nxc",
                        "rule": f"{lo:.3g} <= {num} <= {hi:.3g} AND {cat}=={k!r}",
                        "n": len(returns),
                        "win_rate": wr,
                        "avg_return": _avg(returns),
                        "lift": lift,
                    })
    return out


# ── Analysis 3: Greedy decision-tree split ──────────────────────────────────

def best_single_split(pairs: list[dict], baseline_wr: float) -> dict | None:
    """Find the single binary rule with the largest win-rate lift on
    EITHER the 'yes' or 'no' side. Searches both categorical equalities
    and numeric quantile cuts."""
    best = None

    # Categorical equality splits
    for sig in CATEGORICAL_SIGNALS:
        groups: dict = defaultdict(list)
        for p in pairs:
            v = p.get(sig)
            if v is None:
                continue
            groups[str(v)].append(p["pnl"])
        for k, yes_returns in groups.items():
            no_returns = [p["pnl"] for p in pairs if str(p.get(sig)) != k]
            if len(yes_returns) < MIN_BUCKET_N or len(no_returns) < MIN_BUCKET_N:
                continue
            wr_yes = _wr(yes_returns)
            wr_no = _wr(no_returns)
            for side, wr, returns in (("yes", wr_yes, yes_returns),
                                       ("no", wr_no, no_returns)):
                lift = wr - baseline_wr
                if best is None or abs(lift) > abs(best["lift"]):
                    rule = (f"{sig} == {k!r}" if side == "yes"
                            else f"{sig} != {k!r}")
                    best = {
                        "rule": rule, "side": side, "n": len(returns),
                        "win_rate": wr, "avg_return": _avg(returns),
                        "lift": lift,
                    }

    # Numeric threshold splits (median, 25th, 75th percentiles)
    for sig in NUMERIC_SIGNALS:
        vals = [(p.get(sig), p["pnl"]) for p in pairs
                 if p.get(sig) is not None
                 and isinstance(p.get(sig), (int, float))
                 and not math.isnan(p.get(sig))]
        if len(vals) < MIN_BUCKET_N * 4:
            continue
        sv = sorted(v[0] for v in vals)
        for pct in (0.25, 0.50, 0.75):
            cut = sv[int(len(sv) * pct)]
            above = [pnl for v, pnl in vals if v > cut]
            below = [pnl for v, pnl in vals if v <= cut]
            if len(above) < MIN_BUCKET_N or len(below) < MIN_BUCKET_N:
                continue
            for side, returns, op in (("above", above, ">"),
                                       ("below", below, "<=")):
                wr = _wr(returns)
                lift = wr - baseline_wr
                if best is None or abs(lift) > abs(best["lift"]):
                    best = {
                        "rule": f"{sig} {op} {cut:.3g}",
                        "side": side, "n": len(returns),
                        "win_rate": wr, "avg_return": _avg(returns),
                        "lift": lift,
                    }
    return best


def best_second_split(pairs: list[dict], primary: dict,
                       baseline_wr: float) -> dict | None:
    """Given a primary split, find the best secondary split among the
    rows satisfying the primary rule."""
    # Filter to rows satisfying primary
    subset = _apply_rule(pairs, primary["rule"])
    if len(subset) < MIN_BUCKET_N * 2:
        return None
    sub_baseline = _wr([p["pnl"] for p in subset])
    return best_single_split(subset, sub_baseline)


def _apply_rule(pairs: list[dict], rule: str) -> list[dict]:
    """Evaluate a rule string against each pair. Supports
    `sig == 'value'`, `sig != 'value'`, `sig > X`, `sig <= X`."""
    import re
    m_eq = re.match(r"(\w+) == '?([^']+)'?$", rule)
    m_ne = re.match(r"(\w+) != '?([^']+)'?$", rule)
    m_gt = re.match(r"(\w+) > ([-\d.]+)$", rule)
    m_le = re.match(r"(\w+) <= ([-\d.]+)$", rule)
    if m_eq:
        s, v = m_eq.groups()
        return [p for p in pairs if str(p.get(s)) == v]
    if m_ne:
        s, v = m_ne.groups()
        return [p for p in pairs if str(p.get(s)) != v]
    if m_gt:
        s, v = m_gt.groups()
        return [p for p in pairs if p.get(s) is not None
                 and isinstance(p.get(s), (int, float))
                 and p[s] > float(v)]
    if m_le:
        s, v = m_le.groups()
        return [p for p in pairs if p.get(s) is not None
                 and isinstance(p.get(s), (int, float))
                 and p[s] <= float(v)]
    return []


# ── Analysis 4: OLS regression ───────────────────────────────────────────────

def ols_feature_importance(pairs: list[dict]) -> list[dict]:
    """Simple OLS via normal equations. Encode categoricals as one-hot
    (limited to top 5 most common values), z-score numerics. Returns
    coefficients ranked by absolute magnitude.

    No sklearn dependency — manual matrix math."""
    # Build feature names
    features: list[str] = []
    for sig in NUMERIC_SIGNALS:
        features.append(f"num__{sig}")
    cat_value_pick: dict = {}
    for sig in CATEGORICAL_SIGNALS:
        counts = Counter(str(p.get(sig)) for p in pairs
                          if p.get(sig) is not None)
        for v, _ in counts.most_common(4):  # top 4 values per category
            features.append(f"cat__{sig}__{v}")
            cat_value_pick.setdefault(sig, []).append(v)

    # Compute means + stds for numeric z-scoring
    means: dict = {}; stds: dict = {}
    for sig in NUMERIC_SIGNALS:
        vals = [p.get(sig) for p in pairs
                 if p.get(sig) is not None
                 and isinstance(p.get(sig), (int, float))
                 and not math.isnan(p.get(sig))]
        if not vals:
            means[sig] = 0; stds[sig] = 1
            continue
        means[sig] = statistics.mean(vals)
        stds[sig] = statistics.stdev(vals) if len(vals) > 1 else 1
        if stds[sig] == 0:
            stds[sig] = 1

    # Build X, y arrays
    X: list[list[float]] = []
    y: list[float] = []
    for p in pairs:
        row = [1.0]  # intercept
        for f in features:
            if f.startswith("num__"):
                sig = f[5:]
                v = p.get(sig)
                if v is None or not isinstance(v, (int, float)) or math.isnan(v):
                    row.append(0.0)
                else:
                    row.append((v - means[sig]) / stds[sig])
            elif f.startswith("cat__"):
                _, sig, val = f.split("__", 2)
                row.append(1.0 if str(p.get(sig)) == val else 0.0)
            else:
                row.append(0.0)
        X.append(row)
        y.append(p["pnl"])

    if len(X) < len(features) + 2:
        return []

    # Solve (X'X) β = X'y manually
    # Build X'X
    n = len(X[0])
    XtX = [[0.0] * n for _ in range(n)]
    Xty = [0.0] * n
    for i in range(len(X)):
        xi = X[i]
        yi = y[i]
        for r in range(n):
            Xty[r] += xi[r] * yi
            for cc in range(n):
                XtX[r][cc] += xi[r] * xi[cc]
    # Add tiny ridge regularization for numerical stability
    for r in range(n):
        XtX[r][r] += 0.01

    # Solve via Gauss-Jordan
    aug = [row + [Xty[r]] for r, row in enumerate(XtX)]
    for r in range(n):
        # Pivot
        max_v = abs(aug[r][r])
        max_r = r
        for rr in range(r + 1, n):
            if abs(aug[rr][r]) > max_v:
                max_v = abs(aug[rr][r])
                max_r = rr
        if max_v < 1e-12:
            return []  # singular
        aug[r], aug[max_r] = aug[max_r], aug[r]
        # Normalize pivot row
        pv = aug[r][r]
        for cc in range(r, n + 1):
            aug[r][cc] /= pv
        # Eliminate column
        for rr in range(n):
            if rr == r:
                continue
            fct = aug[rr][r]
            if fct == 0:
                continue
            for cc in range(r, n + 1):
                aug[rr][cc] -= fct * aug[r][cc]
    beta = [aug[r][-1] for r in range(n)]

    out = []
    for i, f in enumerate(features, start=1):
        out.append({"feature": f, "coef": beta[i]})
    # Sort by absolute magnitude
    out.sort(key=lambda x: abs(x["coef"]), reverse=True)
    return out


# ── Renderer ─────────────────────────────────────────────────────────────────

def fmt_pct(v): return f"{v*100:+.1f}%"


def render_report(horizon: int, n_pairs: int, baseline_wr: float,
                   baseline_avg: float, univariate_n: list, univariate_c: list,
                   pairwise: list, pairwise_nxc: list, primary_split: dict | None,
                   secondary_split: dict | None, ols: list) -> str:
    lines: list[str] = []
    lines.append(f"# Correlation findings — horizon d{horizon}")
    lines.append(f"")
    lines.append(f"_Generated {datetime.now().isoformat(timespec='seconds')}_")
    lines.append(f"")
    lines.append(f"- Dataset: **{n_pairs:,}** trade-pairs (buy at ask, "
                 f"sell at d{horizon} bid; round-trip spread cost included)")
    lines.append(f"- Baseline win rate: **{fmt_pct(baseline_wr)}**, "
                 f"avg return: **{fmt_pct(baseline_avg)}**")
    lines.append(f"- Bucket minimum: n >= {MIN_BUCKET_N}")
    lines.append(f"")

    # Combined univariate ranking
    uni_all = univariate_n + univariate_c
    uni_all.sort(key=lambda x: -abs(x["lift"]))

    lines.append(f"## 1. Top univariate rules (single-signal filters)")
    lines.append(f"")
    lines.append(f"| rule | n | win rate | avg ret | lift vs baseline |")
    lines.append(f"|---|---:|---:|---:|---:|")
    for r in uni_all[:TOP_FINDINGS]:
        lines.append(f"| `{r['rule']}` | {r['n']} | {fmt_pct(r['win_rate'])} | "
                     f"{fmt_pct(r['avg_return'])} | "
                     f"{r['lift']*100:+.0f}pts |")
    lines.append(f"")

    # Pairwise categorical
    pairwise.sort(key=lambda x: -abs(x.get("interaction_lift", 0)))
    if pairwise:
        lines.append(f"## 2. Categorical pairwise interactions")
        lines.append(f"(only cells where combining 2 signals beats the best "
                     f"singleton by ≥5pts win rate)")
        lines.append(f"")
        lines.append(f"| rule | n | win rate | avg ret | interaction lift |")
        lines.append(f"|---|---:|---:|---:|---:|")
        for r in pairwise[:TOP_FINDINGS]:
            lines.append(f"| `{r['rule']}` | {r['n']} | {fmt_pct(r['win_rate'])} | "
                         f"{fmt_pct(r['avg_return'])} | "
                         f"{r['interaction_lift']*100:+.0f}pts |")
        lines.append(f"")

    # Numeric × categorical
    pairwise_nxc.sort(key=lambda x: -abs(x["lift"]))
    if pairwise_nxc:
        lines.append(f"## 3. Numeric × categorical combos (≥10pt lift)")
        lines.append(f"")
        lines.append(f"| rule | n | win rate | avg ret | lift |")
        lines.append(f"|---|---:|---:|---:|---:|")
        for r in pairwise_nxc[:TOP_FINDINGS]:
            lines.append(f"| `{r['rule']}` | {r['n']} | {fmt_pct(r['win_rate'])} | "
                         f"{fmt_pct(r['avg_return'])} | "
                         f"{r['lift']*100:+.0f}pts |")
        lines.append(f"")

    # Greedy decision tree
    lines.append(f"## 4. Best decision-tree splits")
    lines.append(f"")
    if primary_split:
        lines.append(f"**Primary split**: `{primary_split['rule']}`")
        lines.append(f"- n = {primary_split['n']}, "
                     f"win rate = {fmt_pct(primary_split['win_rate'])}, "
                     f"avg = {fmt_pct(primary_split['avg_return'])}, "
                     f"lift = {primary_split['lift']*100:+.0f}pts")
        lines.append(f"")
        if secondary_split:
            lines.append(f"**Conditional secondary** (among rows satisfying primary): "
                         f"`{secondary_split['rule']}`")
            lines.append(f"- n = {secondary_split['n']}, "
                         f"win rate = {fmt_pct(secondary_split['win_rate'])}, "
                         f"avg = {fmt_pct(secondary_split['avg_return'])}")
        lines.append(f"")

    # OLS
    if ols:
        lines.append(f"## 5. OLS feature importance")
        lines.append(f"")
        lines.append(f"Linear regression on z-scored numerics + one-hot top "
                     f"categorical values. Coefficient = predicted return "
                     f"contribution. Sorted by absolute magnitude.")
        lines.append(f"")
        lines.append(f"| feature | coefficient |")
        lines.append(f"|---|---:|")
        for r in ols[:TOP_FINDINGS]:
            lines.append(f"| `{r['feature']}` | {r['coef']*100:+.2f}% |")
        lines.append(f"")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    global TOP_FINDINGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=1,
                    help="forward return horizon in trading days (default 1)")
    ap.add_argument("--top", type=int, default=TOP_FINDINGS,
                    help="top N findings to show per section")
    args = ap.parse_args()
    TOP_FINDINGS = args.top

    print("loading snapshots...")
    rows = load_snapshots()
    print(f"  {len(rows):,} candidate rows")
    print("loading chain_surface...")
    surface = load_chain_surface()
    print(f"  {len(surface):,} (date, occ) entries")
    print(f"\nbuilding trade-pairs at d{args.horizon} horizon...")
    pairs = build_trade_pairs(rows, surface, horizon=args.horizon)
    print(f"  {len(pairs):,} priced trade-pairs")

    if len(pairs) < 50:
        print("not enough data to mine correlations")
        return 2

    baseline_wr = _wr([p["pnl"] for p in pairs])
    baseline_avg = _avg([p["pnl"] for p in pairs])
    print(f"  baseline win rate: {baseline_wr*100:.1f}%, "
          f"avg return: {baseline_avg*100:+.2f}%")

    print("\n[1] univariate (numeric)...")
    uni_n = univariate_numeric(pairs, baseline_wr)
    print(f"  {len(uni_n)} buckets meeting n>={MIN_BUCKET_N}")
    print("[2] univariate (categorical)...")
    uni_c = univariate_categorical(pairs, baseline_wr)
    print(f"  {len(uni_c)} category-values meeting n>={MIN_BUCKET_N}")
    print("[3] pairwise categorical interactions...")
    pw = pairwise_categorical(pairs, baseline_wr)
    print(f"  {len(pw)} interactions with >=5pt lift")
    print("[4] numeric x categorical interactions...")
    pw_nxc = pairwise_numeric_x_categorical(pairs, baseline_wr)
    print(f"  {len(pw_nxc)} combos with >=10pt lift")
    print("[5] best single split...")
    primary = best_single_split(pairs, baseline_wr)
    if primary:
        print(f"  best: {primary['rule']} -> wr={primary['win_rate']*100:.1f}% "
              f"(lift {primary['lift']*100:+.0f}pts)")
    print("[6] best secondary split conditional on primary...")
    secondary = best_second_split(pairs, primary, baseline_wr) if primary else None
    if secondary:
        print(f"  best: {secondary['rule']} -> wr={secondary['win_rate']*100:.1f}%")
    print("[7] OLS feature importance...")
    ols = ols_feature_importance(pairs)
    print(f"  {len(ols)} features ranked")

    md = render_report(args.horizon, len(pairs), baseline_wr, baseline_avg,
                        uni_n, uni_c, pw, pw_nxc, primary, secondary, ols)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(md, encoding="utf-8")
    print(f"\nwrote {REPORT_PATH}\n")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
