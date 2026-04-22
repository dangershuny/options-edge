#!/usr/bin/env python3
"""
Weight optimizer.

Reads the most recent `benchmarks/hist_*.json` produced by
`tools/historical_backtest.py`, computes a scaling factor for each weight
based on the signal's observed information coefficient + bucket-mean
forward-return, and writes `weights_override.json` which `analysis/weights.py`
merges on import.

Principle: don't trust a single signal's point estimate. Scale each weight
by a function of its Spearman IC, clipped to a safe range (0.2–2.0×) so
noisy signals get shrunk toward zero but never silently flipped.

Scaling rule per weight key:

  ic_score   = max(|ic_5d|, |ic_10d|)        (best-horizon IC)
  scale      = clip(ic_score × 20, 0.2, 2.0)
  new_weight = sign(default) × |default| × scale × regime_adjust

`regime_adjust` inspects the signal's bucket-mean to detect sign flips
(e.g. if "rvol.hot" showed NEGATIVE mean forward returns we shrink the
positive reward to 0.3× rather than flip it outright — flipping on limited
data is dangerous).

Usage:
    python -m tools.optimize_weights
    python -m tools.optimize_weights --benchmark benchmarks/hist_2026-04-21.json
    python -m tools.optimize_weights --dry-run
"""

from __future__ import annotations
import argparse
import glob
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.weights import WEIGHTS, write_overrides

SEP = "=" * 78
SUB = "-" * 78

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH_DIR    = os.path.join(PROJECT_ROOT, "benchmarks")


# ── Key → (signal_name, bucket, direction) ───────────────────────────────────
# direction: +1 = default weight is positive (bonus); -1 = negative (penalty).
# Only keys mapped here get auto-tuned. The rest stay at default.
KEY_MAP: dict[str, tuple[str, str, int]] = {
    # rvol family
    "rvol.hot":        ("rvol", "hot",      +1),
    "rvol.elevated":   ("rvol", "elevated", +1),
    "rvol.quiet":      ("rvol", "quiet",    -1),
    # trend
    "trend.aligned":   ("trend", "uptrend", +1),   # aligned with uptrend
    "trend.counter":   ("trend", "downtrend", -1),
    # momentum / trend-exhaustion — use the "crash" / "hot" buckets
    "contra.trend_exhaust": ("momentum_10d", "hot", -1),
    # VWAP proxy
    "vwap.aligned":    ("vwap_proxy", "above", +1),
    "vwap.fighting":   ("vwap_proxy", "below", -1),
    # macro — map to vix_regime buckets
    "macro.vix_low":   ("vix_regime", "low",      +1),
    "macro.vix_elevated": ("vix_regime", "elevated", -1),
    "macro.vix_fear":  ("vix_regime", "fear",     -1),
}


def _latest_benchmark() -> str | None:
    files = sorted(glob.glob(os.path.join(BENCH_DIR, "hist_*.json")))
    return files[-1] if files else None


def _best_ic(sig: dict) -> float:
    ics = sig.get("ic") or {}
    mags = [abs(v) for v in ics.values() if isinstance(v, (int, float))]
    return max(mags) if mags else 0.0


def _bucket_signal(sig: dict, bucket: str) -> dict:
    return (sig.get("by_bucket") or {}).get(bucket, {}) or {}


def _bucket_mean_avg(bucket_stats: dict) -> float | None:
    means = [s.get("mean") for s in bucket_stats.values()
             if isinstance(s, dict) and isinstance(s.get("mean"), (int, float))]
    return sum(means) / len(means) if means else None


def compute_overrides(benchmark: dict, explain: bool = True) -> tuple[dict, list[str]]:
    """Return (overrides, explanations)."""
    overrides: dict[str, float] = {}
    notes: list[str] = []

    signals = benchmark.get("signals") or {}

    for key, (sig_name, bucket, direction) in KEY_MAP.items():
        default = WEIGHTS.get(key)
        if default is None:
            continue
        sig = signals.get(sig_name)
        if not sig:
            notes.append(f"  {key:<24}  skip (no signal data for '{sig_name}')")
            continue

        ic = _best_ic(sig)
        ic_scale = max(0.2, min(2.0, ic * 20))

        b_stats = _bucket_signal(sig, bucket)
        b_mean = _bucket_mean_avg(b_stats)

        # Regime check: if bucket mean has OPPOSITE sign to what `direction`
        # expects, shrink toward zero (don't flip outright on limited data).
        regime_mult = 1.0
        expected_sign = direction  # +1 expects positive fwd returns
        if b_mean is not None:
            actual_sign = 1 if b_mean > 0 else (-1 if b_mean < 0 else 0)
            if actual_sign != 0 and actual_sign != expected_sign:
                regime_mult = 0.3

        scale = ic_scale * regime_mult
        new_w = round(default * scale, 2) if default != 0 else 0.0

        # Floor magnitude at 0.5 so we never write effectively-dead weights
        if abs(new_w) < 0.5 and default != 0:
            new_w = round(0.5 * (1 if default > 0 else -1), 2)

        overrides[key] = new_w
        if explain:
            b_mean_str = f"{b_mean:+.2f}%" if b_mean is not None else "n/a"
            notes.append(
                f"  {key:<24}  default={default:+6.2f}  ic={ic:.3f}  "
                f"bucket='{bucket}'({b_mean_str})  scale={scale:.2f}  "
                f"→ {new_w:+6.2f}"
            )

    return overrides, notes


def main() -> int:
    ap = argparse.ArgumentParser(description="Tune score weights from hist backtest.")
    ap.add_argument("--benchmark", default=None,
                    help="path to hist_*.json (default: most recent in benchmarks/)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print proposed overrides but do not write the file")
    ap.add_argument("--out", default=None,
                    help="override path (default: <project>/weights_override.json)")
    args = ap.parse_args()

    bench_path = args.benchmark or _latest_benchmark()
    if not bench_path or not os.path.exists(bench_path):
        print("No benchmark file found. Run `python -m tools.historical_backtest` first.",
              file=sys.stderr)
        return 2

    with open(bench_path, encoding="utf-8") as f:
        benchmark = json.load(f)

    print(SEP)
    print(f"  OPTIMIZE WEIGHTS   benchmark={os.path.basename(bench_path)}")
    print(SEP)

    overrides, notes = compute_overrides(benchmark, explain=True)

    for n in notes:
        print(n)

    if not overrides:
        print("\nNo overrides computed — check benchmark contents.")
        return 1

    if args.dry_run:
        print()
        print("(dry-run — no file written)")
        return 0

    path = write_overrides(overrides, path=args.out)
    print()
    print(f"  Wrote {len(overrides)} overrides → {path}")
    print(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
