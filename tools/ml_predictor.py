"""
ML predictor — train models on every historical candidate to find
combinations of signals that actually predict profitable trades.

This is the proper ML pipeline that the strategy_backtest tool's
hand-crafted rules don't capture. Workflow:

  1. Load every snapshot candidate as a row in a feature matrix:
       - Numeric features: score, iv_pct, rv_pct, sentiment_delta,
         news_drift_delta, blocks_delta, insider_delta, catalyst_delta,
         pin_delta, trend_pct, trend_3d, rsi14, dte, vol_oi_ratio,
         spread_pct, stock_price, mid_price, dollar_premium
       - One-hot categoricals: vol_signal, flow_signal, skew_signal,
         gex_signal, insider_signal, short_signal, option_type
       - Sentiment/composite numeric: sentiment_composite, news_event_*

  2. Compute realized return from chain_surface:
       entry_ask  = snapshot's ask
       exit_bid   = chain_surface bid N trading days later (target = d3)
       return     = (exit_bid / entry_ask) - 1
       Clip to [-0.95, +3.0] to bound outliers.

  3. Time-series split (no look-ahead):
       train  = oldest 70% of trade-pairs by snapshot_date
       test   = newest 30%
       Crucial: never train on data later than what we test on.

  4. Train a slate of models on the train set:
       - Logistic regression (binary: return > 0)
       - Gradient boosted trees (binary)
       - Random forest (binary)
       - Linear regression (continuous return)
       - Gradient boosted trees (continuous return)

  5. Evaluate on test set, ranked by EXPECTED VALUE on top-decile picks
     (i.e., if we take only the top 10% of predicted-positive candidates,
     what's the average return?). Compare to strategy_v1.2's filter
     applied to the same test set.

  6. If the best ML model has a top-decile expected return that beats
     v1.2 by 5 percentage points AND the test sample is >= 50 rows,
     emit a "ship" recommendation with the model's threshold and the
     features it relies on.

Usage:
    python -m tools.ml_predictor                  # full pipeline, d3 horizon
    python -m tools.ml_predictor --horizon 1
    python -m tools.ml_predictor --horizon 5
"""
from __future__ import annotations

import argparse
import json
import math
import pickle
import statistics
import sys
import warnings
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.ensemble import (
    GradientBoostingClassifier, RandomForestClassifier,
    GradientBoostingRegressor,
)
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, roc_auc_score, log_loss, mean_squared_error,
    precision_score, recall_score,
)

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

SNAP_DIR = REPO_ROOT / "snapshots"
DB_PATH = REPO_ROOT / "engine_state.db"
REPORT_PATH = REPO_ROOT / "logs" / "ml_predictor_report.md"
MODEL_PATH = REPO_ROOT / "logs" / "ml_predictor_v2.pkl"


NUMERIC_FEATURES = [
    "score", "iv_pct", "rv_pct", "iv_rv_spread",
    "sentiment_delta", "sentiment_composite",
    "news_drift_delta", "sentiment_velocity",
    "insider_delta", "short_delta", "blocks_delta", "catalyst_delta",
    "pin_delta", "rvol_delta", "agg_delta", "dir_bias_delta",
    "vwap_delta", "trend_delta", "delta_delta", "macro_delta",
    "confluence_delta", "sector_delta", "rsi_delta",
    "trend_pct", "trend_3d", "rsi14",
    "dte", "vol_oi_ratio", "delta",
    "iv_rank", "vix",
]
CATEGORICAL_FEATURES = [
    "vol_signal", "flow_signal", "skew_signal", "gex_signal",
    "insider_signal", "short_signal", "blocks_signal",
    "option_type", "rsi_zone", "vix_regime",
]


def _trading_days_after(d: str, n: int) -> str:
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    added = 0
    while added < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            added += 1
    return dt.isoformat()


def _occ_key(symbol: str, opt_type: str, strike: float, expiry: str) -> str:
    return f"{symbol}|{(opt_type or '').lower()[:1]}|{float(strike):.2f}|{expiry}"


# ── Build feature matrix ────────────────────────────────────────────────────

def load_dataset(horizon: int = 3) -> pd.DataFrame:
    """For every snapshot candidate, compute its realized return at
    `horizon` trading days. Returns a DataFrame indexed chronologically."""
    import sqlite3

    print(f"Loading snapshots from {SNAP_DIR}...")
    rows = []
    for f in sorted(SNAP_DIR.glob("*_auto-*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        snap_date = d.get("snapshot_date")
        if not snap_date:
            continue
        for t in (d.get("universe") or d.get("trades") or []):
            sym = t.get("symbol")
            opt_type = (t.get("type") or t.get("option_type") or "").lower()
            strike = t.get("strike")
            exp = t.get("expiry")
            if not all([sym, opt_type, strike, exp]):
                continue
            bid = float(t.get("bid") or 0)
            ask = float(t.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                continue
            mid = (bid + ask) / 2.0
            if mid <= 0:
                continue
            row = {
                "snapshot_date": snap_date,
                "symbol": sym,
                "occ_key": _occ_key(sym, opt_type, strike, exp),
                "entry_ask": ask, "entry_bid": bid, "entry_mid": mid,
                "spread_pct": (ask - bid) / mid,
                "mid_price": mid,
                "dollar_premium": mid * 100,
            }
            # Copy all candidate fields we'll use as features
            for k in NUMERIC_FEATURES + CATEGORICAL_FEATURES:
                if k in row:
                    continue
                row[k] = t.get(k)
            row["stock_price"] = float(t.get("stock_price") or 0)
            rows.append(row)

    df = pd.DataFrame(rows)
    print(f"  {len(df):,} candidate rows from {df['snapshot_date'].nunique()} dates")

    print(f"Loading chain_surface and computing {horizon}-day forward returns...")
    if not DB_PATH.exists():
        raise RuntimeError("engine_state.db not found")
    with sqlite3.connect(DB_PATH) as c:
        surface = pd.read_sql_query(
            "SELECT snapshot_date, symbol, option_type, strike, expiry, "
            "bid AS surf_bid FROM chain_surface",
            c,
        )
    surface["occ_key"] = surface.apply(
        lambda r: _occ_key(r["symbol"], r["option_type"], r["strike"], r["expiry"]),
        axis=1,
    )
    surf_map = {(r["snapshot_date"], r["occ_key"]): r["surf_bid"]
                for _, r in surface.iterrows()}

    def realized_return(row):
        future_date = _trading_days_after(row["snapshot_date"], horizon)
        future_bid = surf_map.get((future_date, row["occ_key"]))
        if future_bid is None or future_bid <= 0:
            return np.nan
        ret = (future_bid / row["entry_ask"]) - 1
        return max(min(ret, 3.0), -0.95)

    df["forward_return"] = df.apply(realized_return, axis=1)
    df = df.dropna(subset=["forward_return"]).reset_index(drop=True)
    print(f"  {len(df):,} rows have valid {horizon}-day forward return")

    df = df.sort_values("snapshot_date").reset_index(drop=True)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df


# ── Featurization ──────────────────────────────────────────────────────────

def featurize(df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    """Convert DataFrame rows to a numeric feature matrix.
    - Numeric features: median-imputed, z-scored
    - Categoricals: one-hot top-N values per column
    Returns (X, feature_names).
    """
    # Numeric block: replace non-numeric with NaN, median-impute, z-score
    numeric_cols = []
    for c in NUMERIC_FEATURES + ["spread_pct", "mid_price", "dollar_premium",
                                   "stock_price"]:
        if c not in df.columns:
            continue
        # Coerce
        s = pd.to_numeric(df[c], errors="coerce")
        med = s.median()
        if pd.isna(med):
            med = 0.0
        s = s.fillna(med)
        std = s.std()
        if std == 0 or pd.isna(std):
            std = 1.0
        df[f"_n_{c}"] = (s - s.mean()) / std
        numeric_cols.append(f"_n_{c}")

    # Categorical block: one-hot top-6 values per column (rest dropped)
    cat_cols = []
    for c in CATEGORICAL_FEATURES:
        if c not in df.columns:
            continue
        # Top values
        vals = df[c].dropna().astype(str)
        top = vals.value_counts().head(6).index.tolist()
        for v in top:
            col = f"_c_{c}__{v}"
            df[col] = (vals.reindex(df.index) == v).astype(int)
            cat_cols.append(col)

    feature_cols = numeric_cols + cat_cols
    X = df[feature_cols].values.astype(np.float32)
    return X, feature_cols


# ── Train + evaluate ────────────────────────────────────────────────────────

def time_split(df: pd.DataFrame, test_frac: float = 0.30) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Chronological split: train on earlier rows, test on later ones."""
    cut_idx = int(len(df) * (1 - test_frac))
    return df.iloc[:cut_idx].copy(), df.iloc[cut_idx:].copy()


def time_series_cv(df: pd.DataFrame, model_factory, n_folds: int = 4,
                    k_frac: float = 0.10) -> list[dict]:
    """Expanding-window time-series cross-validation. Fold k trains on
    rows [0:end_k], tests on the next 1/n_folds chunk. Reports top-k_frac
    metrics on each fold. Confirms a single-split win isn't a fluke."""
    dates = sorted(df["snapshot_date"].unique())
    if len(dates) < n_folds + 1:
        return []
    fold_size = len(dates) // (n_folds + 1)
    results = []
    for k in range(1, n_folds + 1):
        train_end_date = dates[k * fold_size]
        test_end_date = dates[min((k + 1) * fold_size, len(dates) - 1)]
        train_mask = df["snapshot_date"] < train_end_date
        test_mask = ((df["snapshot_date"] >= train_end_date) &
                     (df["snapshot_date"] < test_end_date))
        train = df[train_mask].copy()
        test = df[test_mask].copy()
        if len(train) < 50 or len(test) < 20:
            continue
        try:
            df_all = pd.concat([train.assign(_split="train"),
                                  test.assign(_split="test")],
                                 ignore_index=True)
            X_all, _ = featurize(df_all)
            X_train = X_all[:len(train)]
            X_test = X_all[len(train):]
            y_train_ret = train["forward_return"].values.astype(np.float32)
            y_test_ret = test["forward_return"].values.astype(np.float32)
            model, score_kind = model_factory()
            if score_kind == "binary":
                y_train = (y_train_ret > 0).astype(int)
                model.fit(X_train, y_train)
                scores = model.predict_proba(X_test)[:, 1]
            else:
                model.fit(X_train, y_train_ret)
                scores = model.predict(X_test)
            tk = evaluate_topk(test, scores, k_frac)
            results.append({
                "fold": k,
                "n_train": len(train), "n_test": len(test),
                "test_dates": f"{train_end_date} → {test_end_date}",
                **tk,
            })
        except Exception as e:
            results.append({"fold": k, "error": str(e)})
    return results


def evaluate_topk(df_test: pd.DataFrame, scores: np.ndarray,
                   k_frac: float = 0.10) -> dict:
    """Take the top-k_frac highest-scored predictions in the test set.
    Compute their average realized return + win rate. This is the
    metric that actually matters: 'if I trade only the highest-confidence
    picks, what do I earn?'."""
    n = len(df_test)
    k = max(1, int(n * k_frac))
    order = np.argsort(scores)[::-1]
    top_idx = order[:k]
    top_returns = df_test["forward_return"].values[top_idx]
    wins = (top_returns > 0).sum()
    return {
        "n_picks": k,
        "win_rate": float(wins / k) if k else 0.0,
        "avg_return": float(top_returns.mean()),
        "median_return": float(np.median(top_returns)),
        "max_return": float(top_returns.max()),
        "min_return": float(top_returns.min()),
    }


def evaluate_baseline_v12(df_test: pd.DataFrame) -> dict:
    """Apply strategy_v1.2's rule on the test set: BULLISH skew +
    BUY VOL + spread<=10% + dte 14-45 + call only. Report metrics
    for matched rows."""
    mask = (
        (df_test["option_type"] == "call") &
        (df_test["skew_signal"] == "BULLISH") &
        (df_test["vol_signal"] == "BUY VOL") &
        (df_test["spread_pct"] <= 0.10) &
        (df_test["dte"] >= 14) &
        (df_test["dte"] <= 45)
    )
    sub = df_test[mask]
    if len(sub) == 0:
        return {"n_picks": 0, "win_rate": None,
                "avg_return": None, "note": "no rows matched v1.2 in test"}
    returns = sub["forward_return"].values
    return {
        "n_picks": len(sub),
        "win_rate": float((returns > 0).mean()),
        "avg_return": float(returns.mean()),
        "median_return": float(np.median(returns)),
    }


def train_and_score(df_train: pd.DataFrame, df_test: pd.DataFrame,
                     horizon: int) -> dict:
    """Train every model in the slate. Return a dict with per-model
    test metrics + the best model object."""
    # Featurize jointly to keep column alignment
    df_all = pd.concat([df_train.assign(_split="train"),
                         df_test.assign(_split="test")],
                        ignore_index=True)
    X_all, feature_names = featurize(df_all)
    n_train = len(df_train)
    X_train = X_all[:n_train]
    X_test = X_all[n_train:]
    y_train_ret = df_train["forward_return"].values.astype(np.float32)
    y_test_ret = df_test["forward_return"].values.astype(np.float32)
    y_train_bin = (y_train_ret > 0).astype(int)
    y_test_bin = (y_test_ret > 0).astype(int)

    print(f"\nTrain set: {n_train:,} rows  (win rate "
          f"{y_train_bin.mean()*100:.1f}%, "
          f"mean return {y_train_ret.mean()*100:+.2f}%)")
    print(f"Test set:  {len(df_test):,} rows  (win rate "
          f"{y_test_bin.mean()*100:.1f}%, "
          f"mean return {y_test_ret.mean()*100:+.2f}%)")

    models = []

    # 1. Logistic regression (classification)
    try:
        clf = LogisticRegression(max_iter=2000, C=1.0,
                                  class_weight="balanced")
        clf.fit(X_train, y_train_bin)
        proba = clf.predict_proba(X_test)[:, 1]
        topk = evaluate_topk(df_test, proba, k_frac=0.10)
        models.append({
            "name": "LogisticRegression",
            "type": "classification",
            "score_type": "proba_win",
            "test_auc": float(roc_auc_score(y_test_bin, proba))
                if len(np.unique(y_test_bin)) > 1 else None,
            "test_top10pct": topk,
            "model": clf,
        })
    except Exception as e:
        print(f"  LR failed: {e}")

    # 2. Random Forest (classification)
    try:
        rf = RandomForestClassifier(
            n_estimators=200, max_depth=6, min_samples_leaf=8,
            class_weight="balanced", random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train_bin)
        proba = rf.predict_proba(X_test)[:, 1]
        topk = evaluate_topk(df_test, proba, k_frac=0.10)
        # Feature importance
        fi = sorted(zip(feature_names, rf.feature_importances_),
                    key=lambda x: -x[1])[:10]
        models.append({
            "name": "RandomForest",
            "type": "classification",
            "score_type": "proba_win",
            "test_auc": float(roc_auc_score(y_test_bin, proba))
                if len(np.unique(y_test_bin)) > 1 else None,
            "test_top10pct": topk,
            "top_features": [(n, float(v)) for n, v in fi],
            "model": rf,
        })
    except Exception as e:
        print(f"  RF failed: {e}")

    # 3. Gradient Boosting (classification)
    try:
        gbc = GradientBoostingClassifier(
            n_estimators=300, max_depth=3, learning_rate=0.03,
            min_samples_leaf=10, random_state=42)
        gbc.fit(X_train, y_train_bin)
        proba = gbc.predict_proba(X_test)[:, 1]
        topk = evaluate_topk(df_test, proba, k_frac=0.10)
        fi = sorted(zip(feature_names, gbc.feature_importances_),
                    key=lambda x: -x[1])[:10]
        models.append({
            "name": "GradientBoosting",
            "type": "classification",
            "score_type": "proba_win",
            "test_auc": float(roc_auc_score(y_test_bin, proba))
                if len(np.unique(y_test_bin)) > 1 else None,
            "test_top10pct": topk,
            "top_features": [(n, float(v)) for n, v in fi],
            "model": gbc,
        })
    except Exception as e:
        print(f"  GBC failed: {e}")

    # 4. Gradient Boosting Regressor (predicting return value directly)
    try:
        gbr = GradientBoostingRegressor(
            n_estimators=300, max_depth=3, learning_rate=0.03,
            min_samples_leaf=10, random_state=42)
        gbr.fit(X_train, y_train_ret)
        pred = gbr.predict(X_test)
        topk = evaluate_topk(df_test, pred, k_frac=0.10)
        fi = sorted(zip(feature_names, gbr.feature_importances_),
                    key=lambda x: -x[1])[:10]
        models.append({
            "name": "GradientBoostingRegressor",
            "type": "regression",
            "score_type": "predicted_return",
            "test_top10pct": topk,
            "top_features": [(n, float(v)) for n, v in fi],
            "model": gbr,
            "test_rmse": float(np.sqrt(mean_squared_error(y_test_ret, pred))),
        })
    except Exception as e:
        print(f"  GBR failed: {e}")

    return {"models": models, "features": feature_names,
            "n_train": n_train, "n_test": len(df_test)}


# ── Report ──────────────────────────────────────────────────────────────────

def fmt_pct(v): return f"{v*100:+.1f}%" if v is not None else "—"


def render_report(horizon: int, df: pd.DataFrame, results: dict,
                   baseline: dict) -> str:
    lines = []
    lines.append(f"# ML predictor — horizon d{horizon}")
    lines.append("")
    lines.append(f"_Generated {datetime.now().isoformat(timespec='seconds')}_")
    lines.append("")
    lines.append(f"## Dataset")
    lines.append(f"")
    lines.append(f"- Total rows: **{len(df):,}** trade-pairs with valid d{horizon} return")
    lines.append(f"- Date range: {df['snapshot_date'].min().date()} → "
                 f"{df['snapshot_date'].max().date()}")
    lines.append(f"- Train: {results['n_train']:,}  Test: {results['n_test']:,}")
    lines.append(f"- Universe win rate at d{horizon}: "
                 f"{(df['forward_return'] > 0).mean()*100:.1f}%")
    lines.append(f"- Universe mean return at d{horizon}: "
                 f"{df['forward_return'].mean()*100:+.2f}%")
    lines.append("")

    lines.append("## Strategy_v1.2 baseline on test set")
    lines.append("")
    if baseline.get("n_picks", 0) > 0:
        lines.append(f"- Rule: call + BULLISH skew + BUY VOL + spread<=10% + DTE 14-45")
        lines.append(f"- Matched rows in test: **{baseline['n_picks']}**")
        lines.append(f"- Win rate: {fmt_pct(baseline['win_rate'])}")
        lines.append(f"- Avg return: {fmt_pct(baseline['avg_return'])}")
        lines.append(f"- Median return: {fmt_pct(baseline['median_return'])}")
    else:
        lines.append(f"- {baseline.get('note', 'no matches')}")
    lines.append("")

    lines.append("## Models (each model picks the top 10% of test rows by score)")
    lines.append("")
    lines.append("| model | type | AUC | top-10% n | win rate | avg return | median |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for m in results["models"]:
        tk = m["test_top10pct"]
        auc = f"{m.get('test_auc', 0):.3f}" if m.get("test_auc") is not None else "—"
        lines.append(f"| `{m['name']}` | {m['type']} | {auc} | "
                     f"{tk['n_picks']} | {fmt_pct(tk['win_rate'])} | "
                     f"{fmt_pct(tk['avg_return'])} | "
                     f"{fmt_pct(tk['median_return'])} |")
    lines.append("")

    # Rank winners
    ranked = sorted(results["models"],
                    key=lambda m: m["test_top10pct"]["avg_return"], reverse=True)
    best = ranked[0]
    lines.append(f"## Top model: `{best['name']}`")
    lines.append("")
    lines.append(f"- Top-10% picks on test set: "
                 f"avg return {fmt_pct(best['test_top10pct']['avg_return'])}, "
                 f"win rate {fmt_pct(best['test_top10pct']['win_rate'])}")
    if baseline.get("avg_return") is not None:
        delta = best["test_top10pct"]["avg_return"] - baseline["avg_return"]
        sign = "+" if delta >= 0 else ""
        lines.append(f"- vs strategy_v1.2 baseline: **{sign}{delta*100:.1f}pts** "
                     f"avg return ({fmt_pct(best['test_top10pct']['avg_return'])} "
                     f"vs {fmt_pct(baseline['avg_return'])})")
    if "top_features" in best:
        lines.append("")
        lines.append("Top 10 features by importance:")
        lines.append("")
        lines.append("| feature | importance |")
        lines.append("|---|---:|")
        for n, v in best["top_features"]:
            lines.append(f"| `{n}` | {v:.4f} |")
    lines.append("")

    # Recommendation
    lines.append("## Ship recommendation")
    lines.append("")
    SIG_DELTA = 0.05
    MIN_TEST_N = 50
    if baseline.get("avg_return") is None:
        lines.append("⚠ Cannot compare — strategy_v1.2 baseline produced 0 test matches.")
    elif results["n_test"] < MIN_TEST_N:
        lines.append(f"⚠ Test set too small (n={results['n_test']} < {MIN_TEST_N}). "
                     f"Collect more data before shipping a model.")
    else:
        delta = best["test_top10pct"]["avg_return"] - baseline["avg_return"]
        if delta >= SIG_DELTA:
            lines.append(f"✅ **SHIP** `{best['name']}` as strategy_v2_ml. "
                         f"Top-10% picks beat v1.2 by {delta*100:+.1f} percentage "
                         f"points on the test set (n={best['test_top10pct']['n_picks']}).")
        else:
            lines.append(f"❌ HOLD. Best model beats v1.2 by only "
                         f"{delta*100:+.1f}pts (need >= {SIG_DELTA*100:.0f}pts). "
                         f"Keep collecting data; re-train weekly.")
    lines.append("")
    return "\n".join(lines)


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=3,
                    help="forward return horizon (trading days)")
    ap.add_argument("--test-frac", type=float, default=0.30)
    args = ap.parse_args()

    df = load_dataset(horizon=args.horizon)
    if len(df) < 100:
        print(f"Only {len(df)} rows with valid forward return — not enough.")
        return 2

    df_train, df_test = time_split(df, test_frac=args.test_frac)
    print(f"\nChronological split:")
    print(f"  train: {df_train['snapshot_date'].min().date()} → "
          f"{df_train['snapshot_date'].max().date()}  ({len(df_train):,} rows)")
    print(f"  test:  {df_test['snapshot_date'].min().date()} → "
          f"{df_test['snapshot_date'].max().date()}  ({len(df_test):,} rows)")

    baseline = evaluate_baseline_v12(df_test)
    print(f"\nstrategy_v1.2 baseline on test set: n={baseline.get('n_picks')}, "
          f"avg={fmt_pct(baseline.get('avg_return'))}")

    results = train_and_score(df_train, df_test, args.horizon)
    if not results["models"]:
        print("No models trained successfully.")
        return 2

    print(f"\nResults sorted by top-10% avg return:")
    for m in sorted(results["models"],
                     key=lambda m: m["test_top10pct"]["avg_return"], reverse=True):
        tk = m["test_top10pct"]
        print(f"  {m['name']:30s}  top-10% avg={fmt_pct(tk['avg_return'])}  "
              f"win={fmt_pct(tk['win_rate'])}  AUC={m.get('test_auc')}")

    # ── Time-series CV to verify the single-split win isn't a fluke ─────────
    # Includes a SIMPLE baseline (logistic regression) which is less prone to
    # overfit on small data. Tighter regularization on GBM too.
    print(f"\n=== Time-series CV (3 folds, regularized) ===")
    cv_factories = {
        "LogisticReg (simple)": lambda: (
            LogisticRegression(max_iter=2000, C=0.3, penalty="l2",
                                 class_weight="balanced"),
            "binary"),
        "GradientBoosting (tight)": lambda: (
            GradientBoostingClassifier(n_estimators=100, max_depth=2,
                                         learning_rate=0.05,
                                         min_samples_leaf=20, random_state=42),
            "binary"),
        "GradientBoostingReg (tight)": lambda: (
            GradientBoostingRegressor(n_estimators=100, max_depth=2,
                                       learning_rate=0.05,
                                       min_samples_leaf=20, random_state=42),
            "regression"),
        "RandomForest (tight)": lambda: (
            RandomForestClassifier(n_estimators=200, max_depth=4,
                                     min_samples_leaf=20, class_weight="balanced",
                                     random_state=42, n_jobs=-1),
            "binary"),
    }
    cv_summary = {}
    for name, factory in cv_factories.items():
        folds = time_series_cv(df, factory, n_folds=3)
        ok_folds = [f for f in folds if "error" not in f]
        if not ok_folds:
            cv_summary[name] = {"folds": folds, "mean_avg_return": None}
            continue
        mean_ar = statistics.mean(f["avg_return"] for f in ok_folds)
        mean_wr = statistics.mean(f["win_rate"] for f in ok_folds)
        cv_summary[name] = {
            "folds": ok_folds,
            "mean_avg_return": mean_ar,
            "mean_win_rate": mean_wr,
            "n_folds": len(ok_folds),
        }
        print(f"  {name:30s}  CV mean: avg={fmt_pct(mean_ar)}  "
              f"win={fmt_pct(mean_wr)}  ({len(ok_folds)}/{len(folds)} folds ok)")

    results["cv_summary"] = cv_summary

    md = render_report(args.horizon, df, results, baseline)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(md, encoding="utf-8")
    print(f"\nWrote {REPORT_PATH}\n")

    # Save the best model
    best = sorted(results["models"],
                  key=lambda m: m["test_top10pct"]["avg_return"], reverse=True)[0]
    save_obj = {
        "model": best["model"],
        "model_name": best["name"],
        "feature_names": results["features"],
        "horizon": args.horizon,
        "trained_at": datetime.now().isoformat(),
        "train_n": results["n_train"],
        "test_n": results["n_test"],
        "test_top10pct": best["test_top10pct"],
        "baseline_v12": baseline,
    }
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open("wb") as f:
        pickle.dump(save_obj, f)
    print(f"Saved best model to {MODEL_PATH}")
    print()
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
