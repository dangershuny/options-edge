"""
Historical signal backtester.

Purpose: measure how well each individual signal predicts *forward* price
moves on historical data, so we can optimize score weights.

Scope limitation — yfinance gives us stock OHLCV history but NOT historical
options chains. So we can only backtest signals that are derivable from
daily OHLCV (+ VIX). Those are:

  RVOL            (volume / 20d-avg volume)
  Trend regime    (SMA20/50 + slope)
  Momentum        (5/10/20d return)
  VWAP proxy      (close vs typical-price 20-day)
  Short reversal  (5d return)
  VIX regime      (scaled by ^VIX history)

For each (signal_bucket, forward_horizon_days) we compute:

  n            number of observations
  hit_rate     % of forwards with positive return (or negative if the
               signal predicts down-moves)
  mean_fwd     average forward return %
  ic           Spearman rank correlation between signal value and
               forward return (information coefficient — the key metric
               for whether a signal has ANY predictive edge)

Output is a dict that `optimize_weights.py` reads to suggest weight tweaks.
"""

from __future__ import annotations
import math
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


# ── Horizons we score at ──────────────────────────────────────────────────────
FWD_HORIZONS = [1, 3, 5, 10]


# ── Signal extractors ────────────────────────────────────────────────────────
# Each takes an OHLCV DataFrame (index=date, cols: Open High Low Close Volume)
# plus optional VIX series (aligned by date) and returns a DataFrame with
# columns: ["value", "bucket"] indexed the same as input.
# `bucket` is a string label used for grouping; `value` is the raw numeric
# signal used for IC calculation.


def _sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=max(2, n // 2)).mean()


def extract_rvol(df: pd.DataFrame) -> pd.DataFrame:
    vol = df["Volume"]
    avg20 = vol.rolling(20, min_periods=10).mean()
    rvol = vol / avg20
    def bucket(x):
        if pd.isna(x): return "unknown"
        if x >= 2.0: return "hot"
        if x >= 1.5: return "elevated"
        if x < 0.7:  return "quiet"
        return "normal"
    return pd.DataFrame({"value": rvol, "bucket": rvol.apply(bucket)})


def extract_trend(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]
    sma20 = _sma(close, 20)
    sma50 = _sma(close, 50)
    slope = (sma20 - sma20.shift(10)) / sma20.shift(10)

    def bucket(row):
        c, s20, s50, sl = row["c"], row["s20"], row["s50"], row["sl"]
        if any(pd.isna(x) for x in (c, s20, s50, sl)):
            return "unknown"
        if c > s20 > s50 and sl > 0.01:
            return "uptrend"
        if c < s20 < s50 and sl < -0.01:
            return "downtrend"
        return "choppy"

    # value = normalized distance from SMA50 × slope sign
    value = ((close - sma50) / sma50)
    lbl = pd.DataFrame({"c": close, "s20": sma20, "s50": sma50, "sl": slope})
    return pd.DataFrame({"value": value, "bucket": lbl.apply(bucket, axis=1)})


def extract_momentum(df: pd.DataFrame, lookback: int = 10) -> pd.DataFrame:
    close = df["Close"]
    mom = close / close.shift(lookback) - 1.0

    def bucket(x):
        if pd.isna(x): return "unknown"
        if x > 0.08:   return "hot"
        if x > 0.03:   return "up"
        if x < -0.08:  return "crash"
        if x < -0.03:  return "down"
        return "flat"
    return pd.DataFrame({"value": mom, "bucket": mom.apply(bucket)})


def extract_short_reversal(df: pd.DataFrame) -> pd.DataFrame:
    """5d return — reversal tends to show up at extremes."""
    close = df["Close"]
    r5 = close / close.shift(5) - 1.0

    def bucket(x):
        if pd.isna(x): return "unknown"
        if x > 0.07:   return "overbought"
        if x < -0.07:  return "oversold"
        return "neutral"
    return pd.DataFrame({"value": r5, "bucket": r5.apply(bucket)})


def extract_vwap_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Daily VWAP proxy: compare close to 20-day volume-weighted mean of typical
    price. Serves as the closest offline analogue to intraday VWAP alignment.
    """
    typ = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vol = df["Volume"]
    num = (typ * vol).rolling(20, min_periods=10).sum()
    den = vol.rolling(20, min_periods=10).sum()
    vwap20 = num / den
    dev = (df["Close"] - vwap20) / vwap20

    def bucket(x):
        if pd.isna(x): return "unknown"
        if x > 0.02:   return "above"
        if x < -0.02:  return "below"
        return "at"
    return pd.DataFrame({"value": dev, "bucket": dev.apply(bucket)})


def extract_vix_regime(df: pd.DataFrame, vix: pd.Series | None) -> pd.DataFrame:
    if vix is None:
        return pd.DataFrame({"value": np.nan, "bucket": "unknown"}, index=df.index)
    aligned = vix.reindex(df.index, method="ffill")

    def bucket(x):
        if pd.isna(x): return "unknown"
        if x < 15:   return "low"
        if x < 22:   return "normal"
        if x < 30:   return "elevated"
        return "fear"
    return pd.DataFrame({"value": aligned, "bucket": aligned.apply(bucket)})


SIGNALS = {
    "rvol":          extract_rvol,
    "trend":         extract_trend,
    "momentum_10d":  lambda df: extract_momentum(df, 10),
    "momentum_5d":   lambda df: extract_momentum(df, 5),
    "short_reversal": extract_short_reversal,
    "vwap_proxy":    extract_vwap_proxy,
    # vix_regime handled separately (needs VIX series)
}


# ── Forward returns ──────────────────────────────────────────────────────────
def forward_returns(df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    close = df["Close"]
    out = pd.DataFrame(index=df.index)
    for h in horizons:
        out[f"fwd_{h}d"] = close.shift(-h) / close - 1.0
    return out


# ── Stats ────────────────────────────────────────────────────────────────────
def _spearman(a: pd.Series, b: pd.Series) -> float | None:
    """Spearman rank correlation. Returns None if insufficient data."""
    mask = a.notna() & b.notna()
    if mask.sum() < 30:
        return None
    ar = a[mask].rank()
    br = b[mask].rank()
    if ar.std() == 0 or br.std() == 0:
        return None
    return float(ar.corr(br))


def _bucket_stats(values: pd.DataFrame, fwd: pd.DataFrame) -> dict:
    """
    values: DataFrame with columns ['value', 'bucket']
    fwd:    DataFrame with forward-return columns fwd_Xd
    """
    out = {"ic": {}, "by_bucket": {}}
    for col in fwd.columns:
        out["ic"][col] = _spearman(values["value"], fwd[col])

    buckets: dict[str, dict] = defaultdict(dict)
    for b, idx in values.groupby("bucket").groups.items():
        sub = fwd.loc[idx]
        for col in fwd.columns:
            s = sub[col].dropna()
            if len(s) < 10:
                buckets[b][col] = {"n": int(len(s)), "mean": None, "hit": None}
                continue
            buckets[b][col] = {
                "n":    int(len(s)),
                "mean": round(float(s.mean()) * 100, 3),
                "hit":  round(float((s > 0).mean()) * 100, 2),
            }
    out["by_bucket"] = dict(buckets)
    return out


# ── Main entry point ─────────────────────────────────────────────────────────
@dataclass
class HistBacktestResult:
    period_days:    int
    universe:       list[str]
    tickers_ok:     list[str] = field(default_factory=list)
    tickers_fail:   list[str] = field(default_factory=list)
    horizons:       list[int] = field(default_factory=lambda: FWD_HORIZONS)
    signals:        dict[str, dict] = field(default_factory=dict)
    # signals["rvol"] = {"ic": {"fwd_5d": 0.06, ...},
    #                    "by_bucket": {"hot": {"fwd_5d": {"n":..,"mean":..,"hit":..}}}}

    def to_dict(self) -> dict:
        return {
            "period_days":   self.period_days,
            "universe_size": len(self.universe),
            "tickers_ok":    self.tickers_ok,
            "tickers_fail":  self.tickers_fail,
            "horizons":      self.horizons,
            "signals":       self.signals,
        }


def _fetch_vix(period_days: int) -> pd.Series | None:
    try:
        v = yf.Ticker("^VIX").history(period=f"{period_days}d")
        if v is None or v.empty:
            return None
        s = v["Close"]
        s.index = s.index.normalize().tz_localize(None)
        return s
    except Exception:
        return None


def _fetch_ohlcv(symbol: str, period_days: int) -> pd.DataFrame | None:
    try:
        t = yf.Ticker(symbol)
        h = t.history(period=f"{period_days}d")
        if h is None or h.empty or len(h) < 60:
            return None
        h.index = h.index.normalize().tz_localize(None)
        return h[["Open", "High", "Low", "Close", "Volume"]]
    except Exception:
        return None


def run_hist_backtest(
    universe: list[str],
    period_days: int = 365,
    horizons: list[int] | None = None,
    progress_cb = None,
) -> HistBacktestResult:
    """
    For each ticker in `universe`, pull `period_days` of OHLCV, compute each
    signal's value + bucket, compute forward returns at each horizon, and
    aggregate across the universe.

    Signals get evaluated ACROSS the full pooled sample (every ticker-day).
    """
    horizons = horizons or FWD_HORIZONS
    vix = _fetch_vix(period_days)

    # Pooled containers
    pooled_signals: dict[str, list[pd.DataFrame]] = defaultdict(list)
    pooled_vix_signal: list[pd.DataFrame] = []
    pooled_fwd: list[pd.DataFrame] = []

    result = HistBacktestResult(period_days=period_days, universe=list(universe),
                                 horizons=horizons)

    for i, sym in enumerate(universe):
        if progress_cb:
            progress_cb(i, len(universe), sym)
        df = _fetch_ohlcv(sym, period_days)
        if df is None:
            result.tickers_fail.append(sym)
            continue
        fwd = forward_returns(df, horizons)

        try:
            for name, fn in SIGNALS.items():
                pooled_signals[name].append(fn(df))
            pooled_vix_signal.append(extract_vix_regime(df, vix))
            pooled_fwd.append(fwd)
            result.tickers_ok.append(sym)
        except Exception:
            result.tickers_fail.append(sym)
            continue

    if not pooled_fwd:
        return result

    fwd_all = pd.concat(pooled_fwd, axis=0, ignore_index=True)
    for name, parts in pooled_signals.items():
        vals = pd.concat(parts, axis=0, ignore_index=True)
        result.signals[name] = _bucket_stats(vals, fwd_all)

    vix_vals = pd.concat(pooled_vix_signal, axis=0, ignore_index=True)
    result.signals["vix_regime"] = _bucket_stats(vix_vals, fwd_all)

    return result
