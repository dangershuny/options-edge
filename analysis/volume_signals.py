"""
Volume-based edge signals.

Four complementary lenses on "is real money actually here, and in which
direction?" — each returning a bounded score delta the main composite can
absorb without blowing up calibration.

  1. Relative volume (RVOL)      — today's stock volume vs 20-day average.
                                    Institutions leave fingerprints in vol.
                                    RVOL >1.5 = something is happening.
  2. Aggressive option flow      — where did the last print land in the
                                    bid–ask range? Prints near the ask are
                                    buyers lifting offers (bullish for calls,
                                    the inverse for puts).
  3. Chain directional bias      — total call volume vs total put volume
                                    across the front-month chain. Unweighted
                                    dollar bias is cruder than live tape but
                                    free and directionally correct.
  4. VWAP alignment              — is price above or below today's
                                    volume-weighted average price? Trading
                                    calls above VWAP / puts below VWAP rides
                                    the intraday tape; fighting it is
                                    penalised.

Design rules:
  - Every function accepts a pre-fetched DataFrame or Series when possible so
    scoring doesn't spawn extra yfinance calls per contract.
  - Every function returns bounded floats; the scorer caps the sum at ±15
    points so a noisy day can't swamp the fundamental IV vs RV edge.
  - All network calls are isolated behind small try/except — failures are
    silent and return neutral (0.0).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except Exception:  # pragma: no cover - yfinance is a hard dep elsewhere
    yf = None  # type: ignore

from analysis.weights import w, w_regime


# ── 1. Relative volume ───────────────────────────────────────────────────────

@dataclass
class RVOLResult:
    rvol: float                # today / avg (1.0 = normal)
    avg_volume: float
    today_volume: float
    label: str                 # HOT | ELEVATED | NORMAL | QUIET

    @property
    def is_hot(self) -> bool:
        return self.rvol >= 1.5


def _label_rvol(rvol: float) -> str:
    if rvol >= 2.0:
        return "HOT"
    if rvol >= 1.5:
        return "ELEVATED"
    if rvol >= 0.7:
        return "NORMAL"
    return "QUIET"


def relative_volume(volume_series: pd.Series, lookback: int = 20) -> RVOLResult | None:
    """
    RVOL from a daily-volume pandas Series. Last row = today.
    Returns None if insufficient data.
    """
    if volume_series is None or len(volume_series) < lookback + 1:
        return None
    s = volume_series.dropna().astype(float)
    if len(s) < lookback + 1:
        return None
    today = float(s.iloc[-1])
    avg = float(s.iloc[-(lookback + 1):-1].mean())
    if avg <= 0:
        return None
    rvol = today / avg
    return RVOLResult(
        rvol=round(rvol, 3),
        avg_volume=avg,
        today_volume=today,
        label=_label_rvol(rvol),
    )


def fetch_rvol(symbol: str, lookback: int = 20) -> RVOLResult | None:
    """Convenience: pull 30 days and compute RVOL. Caller should prefer
    passing a pre-fetched series to avoid duplicate network hits."""
    if yf is None:
        return None
    try:
        h = yf.Ticker(symbol).history(period=f"{lookback + 10}d")
        if h.empty:
            return None
        return relative_volume(h["Volume"], lookback=lookback)
    except Exception:
        return None


def rvol_score_delta(rv: RVOLResult | None, vol_signal: str,
                     vix_regime: str | None = None) -> float:
    """
    Map RVOL to a small score delta. We only reward directional signals
    (BUY VOL / FLOW BUY) — a noisy-but-neutral stock is just noise. Penalise
    QUIET days mildly: low RVOL usually means nobody's confirming the signal.

    If `vix_regime` is provided, regime-specific overrides in weights_override
    take precedence (e.g. `rvol.hot@fear`).
    """
    if rv is None or vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    if rv.label == "HOT":
        return w_regime("rvol.hot", vix_regime, 6.0)
    if rv.label == "ELEVATED":
        return w_regime("rvol.elevated", vix_regime, 3.0)
    if rv.label == "QUIET":
        return w_regime("rvol.quiet", vix_regime, -3.0)
    return 0.0


# ── 2. Aggressive option flow (at-ask vs at-bid proxy) ───────────────────────
#
# Without true time-and-sales we can't know which side of the book each
# trade crossed. But the last print vs the current bid/ask gives a decent
# proxy on liquid contracts: if last > mid and volume is heavy, buyers
# lifted offers; if last < mid, sellers hit bids. Returns a score in
# [-1, +1] where positive = aggressive buying.

def aggressive_flow_ratio(bid: float, ask: float, last: float) -> float | None:
    """
    Map (bid, ask, last) → [-1, +1]. +1 = print at ask, -1 = print at bid,
    0 = at mid. None if spread is unusable.
    """
    if bid <= 0 or ask <= 0 or ask <= bid or last <= 0:
        return None
    span = ask - bid
    if span < 0.01:
        return None
    # Clamp last inside the bid-ask envelope (print can be slightly outside
    # on illiquid contracts — those aren't meaningful either way).
    last = min(max(last, bid - span * 0.2), ask + span * 0.2)
    mid = (bid + ask) / 2
    # Scale so last == mid -> 0, last == ask -> +1, last == bid -> -1.
    return max(-1.0, min(1.0, (last - mid) / (span / 2)))


def aggressive_flow_delta(bid: float, ask: float, last: float,
                          opt_type: str, vol_oi_ratio: float) -> float:
    """
    Reward aligned aggression: calls bought aggressively, puts sold into
    aggressively. Require actual volume (vol/OI ≥ 0.3) — an "at-ask" print
    on 2 contracts is tape noise. Cap at ±5.
    """
    ratio = aggressive_flow_ratio(bid, ask, last)
    if ratio is None or vol_oi_ratio < 0.3:
        return 0.0
    # Calls: positive ratio (at-ask) is bullish alignment → reward.
    # Puts: negative ratio (at-bid) isn't actually adverse to a long put —
    # what matters for long puts is that PUT flow is aggressive on the buy
    # side, same direction. So we read ratio the same way: ratio > 0 on a
    # put print means puts are being lifted at the ask = put demand =
    # aligned with a BUY PUT thesis.
    return round(ratio * w("agg_flow.max", 5.0), 2)


# ── 3. Chain directional bias (call vol vs put vol) ──────────────────────────

@dataclass
class DirectionalBias:
    call_volume: int
    put_volume: int
    ratio: float               # call_vol / put_vol, or inf if no puts
    label: str                 # CALL_HEAVY | PUT_HEAVY | BALANCED


def chain_directional_bias(chain: pd.DataFrame,
                           min_total_volume: int = 500) -> DirectionalBias | None:
    """
    Aggregate put vs call volume across the filtered chain. Requires a
    meaningful sample (default 500 contracts across all strikes) — thin
    chains give unstable ratios.
    """
    if chain is None or chain.empty or "type" not in chain.columns:
        return None
    vol = pd.to_numeric(chain["volume"], errors="coerce").fillna(0)
    call_vol = int(vol[chain["type"] == "call"].sum())
    put_vol = int(vol[chain["type"] == "put"].sum())
    total = call_vol + put_vol
    if total < min_total_volume:
        return None
    ratio = (call_vol / put_vol) if put_vol > 0 else float("inf")
    if ratio >= 1.5:
        label = "CALL_HEAVY"
    elif ratio <= 0.67:
        label = "PUT_HEAVY"
    else:
        label = "BALANCED"
    return DirectionalBias(
        call_volume=call_vol, put_volume=put_vol,
        ratio=round(ratio, 2) if ratio != float("inf") else ratio,
        label=label,
    )


def directional_bias_delta(bias: DirectionalBias | None,
                           vol_signal: str, opt_type: str) -> float:
    """Aligned: call heavy + buying calls (or put heavy + buying puts). ±3."""
    if bias is None or bias.label == "BALANCED":
        return 0.0
    if vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    db = w("dir_bias.max", 3.0)
    if opt_type.startswith("c"):
        return +db if bias.label == "CALL_HEAVY" else -db
    else:
        return +db if bias.label == "PUT_HEAVY" else -db


# ── 4. VWAP alignment ────────────────────────────────────────────────────────

@dataclass
class VWAPResult:
    vwap: float
    last_price: float
    pct_from_vwap: float       # (last - vwap) / vwap
    side: str                  # ABOVE | BELOW | AT


def compute_vwap(intraday: pd.DataFrame) -> VWAPResult | None:
    """
    Session VWAP from an intraday OHLCV DataFrame (typically 5m bars for
    today). Requires at least 3 bars of data with volume. Returns None
    pre-open or on bad data.
    """
    if intraday is None or intraday.empty:
        return None
    need = {"High", "Low", "Close", "Volume"}
    if not need.issubset(intraday.columns):
        return None
    vol = pd.to_numeric(intraday["Volume"], errors="coerce").fillna(0)
    if vol.sum() <= 0 or len(intraday) < 3:
        return None
    typical = (intraday["High"] + intraday["Low"] + intraday["Close"]) / 3.0
    vwap = float((typical * vol).sum() / vol.sum())
    last = float(intraday["Close"].iloc[-1])
    if vwap <= 0:
        return None
    pct = (last - vwap) / vwap
    if abs(pct) < 0.001:
        side = "AT"
    else:
        side = "ABOVE" if pct > 0 else "BELOW"
    return VWAPResult(
        vwap=round(vwap, 4), last_price=round(last, 4),
        pct_from_vwap=round(pct, 4), side=side,
    )


def fetch_vwap(symbol: str) -> VWAPResult | None:
    """Convenience: pull today's 5m bars and compute VWAP."""
    if yf is None:
        return None
    try:
        h = yf.Ticker(symbol).history(period="1d", interval="5m")
        return compute_vwap(h)
    except Exception:
        return None


def vwap_alignment_delta(vw: VWAPResult | None, opt_type: str,
                         vol_signal: str) -> float:
    """
    Calls above VWAP: riding the tape, +4. Calls below VWAP but <0.3%
    below: neutral (noise). Calls well below VWAP: -3 (fighting tape).
    Mirror for puts.
    """
    if vw is None or vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    is_call = opt_type.lower().startswith("c")
    pct = vw.pct_from_vwap
    if abs(pct) < 0.003:
        return 0.0
    aligned = (is_call and pct > 0) or (not is_call and pct < 0)
    magnitude = min(abs(pct) / 0.01, 1.5)  # scales up to ±1.5×
    # NOTE: regime-aware via compute_volume_deltas → vix_regime kwarg
    bonus = w("vwap.aligned", 4.0) if aligned else w("vwap.fighting", -3.0)
    return round(bonus * magnitude, 2)


def vwap_alignment_delta_r(vw: VWAPResult | None, opt_type: str,
                           vol_signal: str, vix_regime: str | None) -> float:
    """Regime-aware VWAP delta (thin wrapper)."""
    if vw is None or vol_signal not in ("BUY VOL", "FLOW BUY"):
        return 0.0
    is_call = opt_type.lower().startswith("c")
    pct = vw.pct_from_vwap
    if abs(pct) < 0.003:
        return 0.0
    aligned = (is_call and pct > 0) or (not is_call and pct < 0)
    magnitude = min(abs(pct) / 0.01, 1.5)
    bonus = (w_regime("vwap.aligned", vix_regime, 4.0) if aligned
             else w_regime("vwap.fighting", vix_regime, -3.0))
    return round(bonus * magnitude, 2)


# ── Combined helper: bundle all four deltas for the scorer ───────────────────

def compute_volume_deltas(
    bid: float, ask: float, last: float,
    opt_type: str, vol_signal: str, vol_oi_ratio: float,
    *,
    rvol: RVOLResult | None = None,
    bias: DirectionalBias | None = None,
    vwap: VWAPResult | None = None,
    vix_regime: str | None = None,
    cap: float | None = None,
) -> dict:
    if cap is None:
        cap = w("vol_bundle.cap", 15.0)
    """
    One-call helper for scorer.py. Returns a dict with each individual
    delta (for debugging / display) plus the capped total.
    """
    d_rvol = rvol_score_delta(rvol, vol_signal, vix_regime=vix_regime)
    d_agg  = aggressive_flow_delta(bid, ask, last, opt_type, vol_oi_ratio)
    d_bias = directional_bias_delta(bias, vol_signal, opt_type)
    d_vwap = vwap_alignment_delta_r(vwap, opt_type, vol_signal, vix_regime)
    total = d_rvol + d_agg + d_bias + d_vwap
    # Symmetric cap so one noisy lens can't dominate.
    total = max(-cap, min(cap, total))
    return {
        "rvol_delta":   round(d_rvol, 2),
        "agg_delta":    round(d_agg, 2),
        "dir_bias_delta": round(d_bias, 2),
        "vwap_delta":   round(d_vwap, 2),
        "volume_delta_total": round(total, 2),
    }
