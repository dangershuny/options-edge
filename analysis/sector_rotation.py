"""
Sector rotation detector.

Consumes a list of per-contract signal rows (from analyze_ticker output) and
identifies sectors where *multiple tickers* show the same directional signal.
Concentrated same-direction activity is a much stronger signal than any
single-ticker reading because it suggests a macro/sector-wide thesis.

Returns a SAFE-DEFAULT dict on any failure — never raises.
"""

from __future__ import annotations

from collections import defaultdict
import pandas as pd

from data.sectors import get_sector


# Minimum distinct tickers in a sector showing the same signal to call it
# a rotation. 2 = suggestive, 3+ = strong.
ROTATION_MIN_TICKERS  = 3
ROTATION_STRONG_MIN   = 4


def detect_rotation(signal_rows: list[dict] | pd.DataFrame) -> dict:
    """
    Detect sector-concentrated buying signals.

    Input: list of dicts or DataFrame rows with at minimum:
           {symbol, type (call/put), vol_signal}

    Returns:
        {
          'rotations': [
            {
              'sector': 'Finance',
              'direction': 'BULLISH' | 'BEARISH',
              'tickers': ['JPM', 'GS', 'MS'],
              'contract_count': 12,
              'strength': 'STRONG' | 'NORMAL'
            }, ...
          ],
          'summary': str
        }
    """
    try:
        if isinstance(signal_rows, pd.DataFrame):
            rows = signal_rows.to_dict("records")
        else:
            rows = list(signal_rows or [])

        if not rows:
            return _empty("no signal rows supplied")

        # Bucket: (sector, direction) → {tickers: set, count: int}
        buckets: dict[tuple[str, str], dict] = defaultdict(
            lambda: {"tickers": set(), "count": 0}
        )

        for r in rows:
            try:
                sym = str(r.get("symbol", "")).upper()
                vol_sig = r.get("vol_signal")
                opt_type = str(r.get("type", "")).lower()
                if not sym or vol_sig not in ("BUY VOL", "FLOW BUY"):
                    continue
                if opt_type not in ("call", "put"):
                    continue

                sector = get_sector(sym)
                if sector in ("Unknown", "ETF-Broad"):
                    # Broad-ETF flow is not rotation; skip.
                    continue

                direction = "BULLISH" if opt_type == "call" else "BEARISH"
                key = (sector, direction)
                buckets[key]["tickers"].add(sym)
                buckets[key]["count"] += 1
            except Exception:
                # Bad row — skip it, don't crash the rotation check
                continue

        rotations = []
        for (sector, direction), data in buckets.items():
            n_tickers = len(data["tickers"])
            if n_tickers < ROTATION_MIN_TICKERS:
                continue
            strength = "STRONG" if n_tickers >= ROTATION_STRONG_MIN else "NORMAL"
            rotations.append({
                "sector":          sector,
                "direction":       direction,
                "tickers":         sorted(data["tickers"]),
                "contract_count":  int(data["count"]),
                "strength":        strength,
            })

        # Sort by ticker count (most concentrated first)
        rotations.sort(key=lambda r: (-len(r["tickers"]), -r["contract_count"]))

        if not rotations:
            summary = "No sector-concentrated signals detected."
        else:
            parts = []
            for r in rotations[:4]:
                parts.append(f"{r['sector']} {r['direction']} "
                             f"({len(r['tickers'])}t, {r['strength']})")
            summary = "Rotations: " + "; ".join(parts)

        return {"rotations": rotations, "summary": summary}

    except Exception as e:
        return _empty(f"rotation detector error: {e}")


def sector_confirms_signal(rotations: list[dict], symbol: str, opt_type: str) -> dict:
    """
    Given a ticker+option-type, check whether a detected sector rotation
    aligns with it. Used by the scorer to award a bonus when an individual
    contract is part of a broader sector move.

    Returns:
        {'confirmed': bool, 'strength': 'STRONG'|'NORMAL'|None, 'sector': str|None}
    """
    try:
        sector = get_sector(symbol)
        if sector in ("Unknown", "ETF-Broad"):
            return {"confirmed": False, "strength": None, "sector": sector}

        direction = "BULLISH" if opt_type.lower() == "call" else "BEARISH"
        for r in rotations or []:
            if r.get("sector") == sector and r.get("direction") == direction:
                return {
                    "confirmed": True,
                    "strength":  r.get("strength", "NORMAL"),
                    "sector":    sector,
                }
        return {"confirmed": False, "strength": None, "sector": sector}
    except Exception:
        return {"confirmed": False, "strength": None, "sector": None}


def _empty(reason: str = "") -> dict:
    return {"rotations": [], "summary": f"rotation: {reason}" if reason else ""}
