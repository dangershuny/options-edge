"""
Test the hypothesis: sentinel/event signals should drive trade selection,
not the IV/RV scorer. Splits historical scored trades by "had a real
sentinel signal" vs "no signal", compares directional win rates.
"""
from __future__ import annotations

import json
import math
import re
import statistics
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def isnum(x):
    try:
        v = float(x)
        return not (math.isnan(v) or math.isinf(v))
    except Exception:
        return False


def thesis_present(t: dict) -> bool:
    """Any non-trivial sentinel/event signal in this snapshot row?"""
    for f in ("sentiment_delta", "news_drift_delta", "insider_delta",
              "blocks_delta", "catalyst_delta"):
        v = t.get(f)
        if isnum(v) and abs(float(v)) >= 1:
            return True
    ne = t.get("news_event")
    if ne and not (isinstance(ne, float) and str(ne) == "nan"):
        return True
    if t.get("catalyst_summary"):
        return True
    return False


def main():
    rows = []
    for fp in sorted((REPO_ROOT / "snapshots").glob("*_auto*.json")):
        m = DATE_RE.search(fp.name)
        if not m:
            continue
        snap_d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for t in data.get("trades", []) or []:
            t = dict(t)
            t["_snap"] = snap_d.isoformat()
            rows.append(t)
    print(f"snapshot trades: {len(rows)}")

    symbols = sorted({r["symbol"] for r in rows if r.get("symbol")})

    import yfinance as yf
    prices = {}
    for sym in symbols:
        try:
            h = yf.Ticker(sym).history(period="30d", auto_adjust=True)
            prices[sym] = {idx.date(): float(c)
                           for idx, c in h["Close"].dropna().items()}
        except Exception:
            prices[sym] = {}

    def first_close_on_or_after(prc, d, max_skip=5):
        for off in range(max_skip + 1):
            x = d + timedelta(days=off)
            if x in prc:
                return prc[x]
        return None

    for r in rows:
        sym = r["symbol"]
        sd = datetime.strptime(r["_snap"], "%Y-%m-%d").date()
        p = prices.get(sym, {})
        spot_now = first_close_on_or_after(p, sd)
        spot_then = first_close_on_or_after(p, sd + timedelta(days=5))
        if not spot_now or not spot_then:
            r["_dir_ret"] = None
            continue
        ret = (spot_then / spot_now) - 1
        ot = (r.get("option_type") or "").lower()
        r["_dir_ret"] = ret if ot == "call" else (-ret if ot == "put" else None)
        r["_won"] = r["_dir_ret"] is not None and r["_dir_ret"] > 0

    eligible = [r for r in rows if r.get("_dir_ret") is not None]
    print(f"analyzable: {len(eligible)}")

    with_t = [r for r in eligible if thesis_present(r)]
    without_t = [r for r in eligible if not thesis_present(r)]
    print(f"with thesis: {len(with_t)}  |  without thesis: {len(without_t)}")
    print()

    def stats(label, group):
        if not group:
            print(f"  {label}: empty"); return
        n = len(group)
        wins = sum(1 for x in group if x["_won"])
        rets = [x["_dir_ret"] * 100 for x in group]
        mean = statistics.fmean(rets)
        med = statistics.median(rets)
        print(f"  {label:<32} n={n:>3}  win_rate={wins/n:>6.1%}  "
              f"mean_dir_ret={mean:>+6.2f}%  median={med:>+6.2f}%")

    print("=== Bulk comparison ===")
    stats("WITH sentinel/event thesis", with_t)
    stats("WITHOUT thesis", without_t)
    print()

    print("=== With-thesis by score bucket ===")
    for lo, hi in [(0, 50), (50, 60), (60, 70), (70, 80), (80, 101)]:
        sub = [r for r in with_t if lo <= r.get("score", 0) < hi]
        if sub:
            stats(f"  score {lo}-{hi}", sub)

    print()
    print("=== Without-thesis by score bucket ===")
    for lo, hi in [(0, 50), (50, 60), (60, 70), (70, 80), (80, 101)]:
        sub = [r for r in without_t if lo <= r.get("score", 0) < hi]
        if sub:
            stats(f"  score {lo}-{hi}", sub)

    # By individual signal type
    print()
    print("=== Single-signal slices ===")
    signals = {
        "sentiment_delta nonzero": lambda t: isnum(t.get("sentiment_delta")) and abs(float(t.get("sentiment_delta", 0))) >= 1,
        "news_drift_delta nonzero": lambda t: isnum(t.get("news_drift_delta")) and abs(float(t.get("news_drift_delta", 0))) >= 1,
        "insider_delta nonzero": lambda t: isnum(t.get("insider_delta")) and abs(float(t.get("insider_delta", 0))) >= 1,
        "blocks_delta nonzero": lambda t: isnum(t.get("blocks_delta")) and abs(float(t.get("blocks_delta", 0))) >= 1,
        "catalyst_delta nonzero": lambda t: isnum(t.get("catalyst_delta")) and abs(float(t.get("catalyst_delta", 0))) >= 1,
        "news_event present": lambda t: bool(t.get("news_event")) and not (isinstance(t.get("news_event"), float) and str(t.get("news_event")) == "nan"),
        "catalyst_summary present": lambda t: bool(t.get("catalyst_summary")),
        "flow_signal=STRONG": lambda t: t.get("flow_signal") == "STRONG",
        "gex_signal=EXPLOSIVE": lambda t: t.get("gex_signal") == "EXPLOSIVE",
        "skew_signal in (BULLISH,BEARISH)": lambda t: t.get("skew_signal") in ("BULLISH", "BEARISH"),
    }
    for label, pred in signals.items():
        sub = [r for r in eligible if pred(r)]
        if sub:
            stats(f"  {label}", sub)


if __name__ == "__main__":
    main()
