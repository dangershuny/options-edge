"""
Signal confluence / agreement scoring.

One strong signal is noise-prone. Four weak-but-agreeing signals is a real
edge. This module inspects the per-contract signal bundle the scorer has
already computed and rewards trades where several *independent* lenses all
point the same way.

Why separate from the main scorer: the existing `score_contract()` function
sums individual contributions. Two signals contributing +8 each sum to +16,
but that's just additivity — it doesn't actually *reward agreement*. A
confluence bonus (non-linear in agreement count) gives us the "when five
people at the bar all tell you to buy, listen" effect that pure additivity
misses.

We look at 8 lenses:
  1. IV vs RV direction          (the core thesis)
  2. Flow (vol/OI)               (unusual activity)
  3. Skew                        (smile asymmetry)
  4. GEX regime                  (dealer positioning)
  5. Insider trades              (informed buying/selling)
  6. Blocks / unusual stock vol  (big money tape prints)
  7. Chain directional bias      (put-vs-call volume aggregate)
  8. Trend regime                (SMA20/SMA50 technical)

Each lens votes +1 / 0 / -1 relative to the proposed direction. Agreement
count determines the bonus; strong disagreement (multiple lenses against)
produces a warning and a small penalty.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ConfluenceResult:
    agree: int
    disagree: int
    neutral: int
    votes: dict[str, int]   # lens name -> {-1, 0, +1}
    score_delta: float      # additive bonus/penalty, bounded
    label: str              # STRONG / MODERATE / MIXED / CONTRADICTED


def _vote_iv(vol_signal: str, opt_type: str) -> int:
    if vol_signal in ("BUY VOL", "FLOW BUY"):
        return +1
    return 0  # SELL VOL / NEUTRAL contribute no directional vote here


def _vote_flow(flow_signal: str) -> int:
    if flow_signal == "STRONG":
        return +1
    if flow_signal == "ELEVATED":
        return 0  # only STRONG gets a yes vote — we want clear signals
    return 0


def _vote_skew(skew: dict | None, opt_type: str) -> int:
    if not skew:
        return 0
    s = skew.get("skew_signal", "NEUTRAL")
    if opt_type.lower().startswith("c"):
        return +1 if s == "BULLISH" else (-1 if s == "BEARISH" else 0)
    return +1 if s == "BEARISH" else (-1 if s == "BULLISH" else 0)


def _vote_gex(gex: dict | None) -> int:
    if not gex:
        return 0
    g = gex.get("gex_signal", "NEUTRAL")
    if g == "EXPLOSIVE":
        return +1
    if g == "PINNED":
        return -1
    return 0


def _vote_insider(insider_info: dict | None, opt_type: str) -> int:
    if not insider_info:
        return 0
    sig = insider_info.get("signal")
    if sig in (None, "NORMAL"):
        return 0
    is_call = opt_type.lower().startswith("c")
    # Insider buying → bullish; selling → bearish.
    if sig in ("BUYING", "ACCUMULATION", "ACTIVE_BUY"):
        return +1 if is_call else -1
    if sig in ("SELLING", "DISTRIBUTION", "ACTIVE_SELL"):
        return -1 if is_call else +1
    return 0


def _vote_blocks(blocks_info: dict | None, opt_type: str) -> int:
    if not blocks_info:
        return 0
    sig = blocks_info.get("signal")
    if sig in (None, "NORMAL"):
        return 0
    is_call = opt_type.lower().startswith("c")
    if sig in ("INSTITUTIONAL_BUY", "ACCUMULATION"):
        return +1 if is_call else -1
    if sig in ("INSTITUTIONAL_SELL", "DISTRIBUTION"):
        return -1 if is_call else +1
    return 0


def _vote_chain_bias(bias, opt_type: str) -> int:
    """bias is DirectionalBias dataclass or None."""
    if bias is None or getattr(bias, "label", "BALANCED") == "BALANCED":
        return 0
    is_call = opt_type.lower().startswith("c")
    if bias.label == "CALL_HEAVY":
        return +1 if is_call else -1
    if bias.label == "PUT_HEAVY":
        return -1 if is_call else +1
    return 0


def _vote_trend(trend, opt_type: str) -> int:
    """trend is TrendRegime dataclass or None."""
    if trend is None or getattr(trend, "regime", "CHOPPY") in ("CHOPPY", "INSUFFICIENT"):
        return 0
    is_call = opt_type.lower().startswith("c")
    if trend.regime == "UPTREND":
        return +1 if is_call else -1
    if trend.regime == "DOWNTREND":
        return -1 if is_call else +1
    return 0


def evaluate_confluence(
    *,
    vol_signal: str,
    flow_signal: str,
    opt_type: str,
    skew: dict | None = None,
    gex: dict | None = None,
    insider_info: dict | None = None,
    blocks_info: dict | None = None,
    chain_bias: Any = None,   # DirectionalBias
    trend: Any = None,        # TrendRegime
) -> ConfluenceResult:
    """
    Compute confluence bonus. Non-linear in agreement count so that 4+
    aligned lenses feel qualitatively different from 2.

    Score deltas:
       5+ agree, 0 disagree → +10   (STRONG)
       4+ agree, ≤1 disagree → +6   (MODERATE)
       3  agree, 0 disagree → +3    (MODERATE)
       ≥2 disagree           → −5   (CONTRADICTED)
       otherwise              → 0    (MIXED / insufficient info)

    We never give a confluence bonus if the core IV vs RV thesis itself
    isn't confirmed — BUY VOL / FLOW BUY only.
    """
    if vol_signal not in ("BUY VOL", "FLOW BUY"):
        return ConfluenceResult(
            agree=0, disagree=0, neutral=0, votes={},
            score_delta=0.0, label="MIXED",
        )

    votes = {
        "iv":        _vote_iv(vol_signal, opt_type),
        "flow":      _vote_flow(flow_signal),
        "skew":      _vote_skew(skew, opt_type),
        "gex":       _vote_gex(gex),
        "insider":   _vote_insider(insider_info, opt_type),
        "blocks":    _vote_blocks(blocks_info, opt_type),
        "chain_bias": _vote_chain_bias(chain_bias, opt_type),
        "trend":     _vote_trend(trend, opt_type),
    }
    agree = sum(1 for v in votes.values() if v > 0)
    disagree = sum(1 for v in votes.values() if v < 0)
    neutral = sum(1 for v in votes.values() if v == 0)

    if agree >= 5 and disagree == 0:
        delta, label = 10.0, "STRONG"
    elif agree >= 4 and disagree <= 1:
        delta, label = 6.0, "MODERATE"
    elif agree >= 3 and disagree == 0:
        delta, label = 3.0, "MODERATE"
    elif disagree >= 2:
        delta, label = -5.0, "CONTRADICTED"
    else:
        delta, label = 0.0, "MIXED"

    return ConfluenceResult(
        agree=agree, disagree=disagree, neutral=neutral,
        votes=votes, score_delta=delta, label=label,
    )
