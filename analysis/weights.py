"""
Centralized score-weight registry.

Every tunable score delta in the system ultimately resolves to a multiplier
that lives HERE. The historical-backtest + optimizer pipeline writes back to
`weights_override.json` in the project root, and this module merges that
override on import so calibration is hot-swappable without touching code.

Design:
  WEIGHTS["rvol.hot"]        = 6.0     # default
  WEIGHTS["vwap.aligned"]    = 4.0
  ...
  load_overrides() merges weights_override.json → WEIGHTS (if present)

Call sites should use `w("rvol.hot")` instead of hard-coded constants so that
a future `optimize_weights.py` run can re-tune live weights without touching
signal code.

Safe defaults: a missing key returns the provided fallback (or 0.0).
"""

from __future__ import annotations
import json
import os
from typing import Any

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OVERRIDE_PATH = os.path.join(_PROJECT_ROOT, "weights_override.json")

# ── Default weight table ─────────────────────────────────────────────────────
# Keys use dotted namespacing: <family>.<condition>
# Values are deltas ADDED to the base score (signed).
WEIGHTS: dict[str, float] = {
    # ── Volume family (analysis/volume_signals.py) ──
    "rvol.hot":            6.0,   # RVOL ≥ 2.0
    "rvol.elevated":       3.0,   # 1.5 ≤ RVOL < 2.0
    "rvol.quiet":         -3.0,   # RVOL < 0.7
    "agg_flow.max":        5.0,   # aggressive-flow cap (±)
    "dir_bias.max":        3.0,   # chain directional bias cap (±)
    "vwap.aligned":        4.0,   # stock vs VWAP aligned w/ option direction
    "vwap.fighting":      -3.0,   # fighting the tape
    "vol_bundle.cap":     15.0,   # symmetric cap on volume-family total

    # ── Trend regime (analysis/trend_filter.py) ──
    "trend.aligned":       4.0,
    "trend.counter":      -3.0,
    "trend.fading":        2.0,

    # ── Delta edge (analysis/delta_edge.py) ──
    "delta.lottery_hard": -8.0,   # |Δ| < 0.10
    "delta.lottery_soft": -4.0,   # 0.10 ≤ |Δ| < 0.15
    "delta.sweet_spot":    4.0,   # 0.25 ≤ |Δ| ≤ 0.45
    "delta.deep_itm":     -2.0,   # |Δ| > 0.65

    # ── Macro (data/macro.py) ──
    "macro.vix_low":       4.0,
    "macro.vix_elevated": -3.0,
    "macro.vix_fear":     -8.0,
    "macro.backwardation": -3.0,

    # ── Confluence (analysis/confluence.py) ──
    "confluence.strong":  10.0,   # ≥5 lenses agree
    "confluence.moderate": 6.0,   # ≥4 lenses agree
    "confluence.weak":     3.0,   # 3 lenses agree
    "confluence.contradicted": -5.0,

    # ── Insider (data/insider.py) ──
    "insider.cluster_buy":  8.0,
    "insider.buy":          4.0,
    "insider.cluster_sell":-6.0,
    "insider.sell":        -3.0,

    # ── Short interest (data/short_interest.py) ──
    "short.squeeze_risk":   6.0,   # high SI, BUY CALL tailwind
    "short.heavy":         -3.0,   # puts face squeeze risk

    # ── Blocks / unusual volume (data/blocks.py) ──
    "blocks.confirm":       5.0,
    "blocks.contra":       -3.0,

    # ── Catalysts (data/catalysts.py) ──
    "catalyst.supports":    6.0,
    "catalyst.against":    -4.0,

    # ── Pin risk (analysis/pin_risk.py) ──
    "pin.at_pin":          -6.0,  # strike is the pin
    "pin.near_pin":        -3.0,

    # ── Scorer base coefficients ──
    "base.vol_strength_mult": 50.0,
    "base.flow_strong":       35.0,
    "base.flow_elevated":     15.0,
    "base.dte_sweet":         10.0,   # 21-45 DTE
    "base.dte_ok":             5.0,   # 14-60 DTE
    "base.flow_buy_floor":    30.0,

    # ── Contrarian dampeners (scorer.py) ──
    "contra.peak_fear_put":   -15.0,
    "contra.peak_greed_call": -10.0,
    "contra.trend_exhaust":   -12.0,
    "contra.premium_penalty_per_5":  -4.0,  # per $5 over $5 entry
    "contra.premium_penalty_cap":   -15.0,
}


# Snapshot pristine defaults BEFORE any override is applied, so the
# optimizer can re-compute scaling from the true baseline rather than
# compounding atop the previous run's overrides.
DEFAULTS: dict[str, float] = dict(WEIGHTS)


# ── Override loader ──────────────────────────────────────────────────────────
def _load_overrides() -> dict[str, Any]:
    """Load weights_override.json into WEIGHTS (if it exists). Silent on miss."""
    try:
        if os.path.exists(_OVERRIDE_PATH):
            with open(_OVERRIDE_PATH, encoding="utf-8") as f:
                ov = json.load(f)
            if isinstance(ov, dict):
                for k, v in ov.items():
                    if isinstance(v, (int, float)):
                        WEIGHTS[k] = float(v)
                return ov
    except Exception:
        pass
    return {}


_loaded_overrides = _load_overrides()


def w(key: str, default: float = 0.0) -> float:
    """Return weight for `key`, or `default` if missing."""
    return float(WEIGHTS.get(key, default))


# ── Regime-conditional lookup ────────────────────────────────────────────────
# Overrides for a specific VIX regime live at `{key}@{regime}` where regime
# is one of: low | normal | elevated | fear. If no regime-specific override
# exists, falls back to the plain `key`.
#
# Example weights_override.json snippet:
#   { "rvol.hot": 2.88,
#     "rvol.hot@fear": 6.50,      # RVOL hot matters MORE in fear regimes
#     "rvol.hot@low":  1.50 }
_REGIMES = {"low", "normal", "elevated", "fear"}


def w_regime(key: str, regime: str | None, default: float = 0.0) -> float:
    """Return regime-specific weight if present, else plain weight, else default."""
    if regime:
        r = regime.lower()
        if r in _REGIMES:
            rkey = f"{key}@{r}"
            if rkey in WEIGHTS:
                return float(WEIGHTS[rkey])
    return float(WEIGHTS.get(key, default))


def write_overrides(overrides: dict[str, float], path: str | None = None) -> str:
    """Persist overrides to disk. Returns absolute path written."""
    path = path or _OVERRIDE_PATH
    with open(path, "w", encoding="utf-8") as f:
        json.dump(overrides, f, indent=2, sort_keys=True)
    # Merge into live table immediately
    for k, v in overrides.items():
        if isinstance(v, (int, float)):
            WEIGHTS[k] = float(v)
    return os.path.abspath(path)


def current_overrides() -> dict[str, float]:
    """Return whatever overrides were loaded on import (may be empty)."""
    return dict(_loaded_overrides)


def diff_vs_default() -> dict[str, tuple[float, float]]:
    """
    For debugging: which keys have been overridden vs their default?
    Returns {key: (default, override)}.
    Requires re-reading defaults from source — approximate by comparing
    against whatever was in WEIGHTS before overrides were applied.
    """
    return {k: (WEIGHTS[k], v) for k, v in _loaded_overrides.items() if k in WEIGHTS}
