"""
Portfolio Greeks aggregator.

Given a list of open positions (long option contracts), compute net
delta, gamma, theta, and vega. This tells you your *real* exposure —
max-loss-per-trade is only one dimension of risk.

Why it matters:
  - Net delta tells you your directional exposure (+500 delta ≈ long 500 shares)
  - Net gamma tells you how fast your delta changes (convexity)
  - Net theta tells you daily decay cost of holding the book
  - Net vega tells you IV exposure (would a vol-spike help or hurt?)

Used by the risk checker before approving a new trade — prevents stacking
too much same-direction exposure.

Safe on any failure: returns zeros.
"""

from __future__ import annotations

import yfinance as yf
from datetime import datetime

from analysis.greeks import bs_greeks
from risk.config import RISK


def aggregate_greeks(positions: list[dict]) -> dict:
    """
    Args:
        positions: list of dicts, each with:
          {
            'symbol', 'type' ('call'|'put'), 'strike', 'expiry' (ISO),
            'contracts' (int, default 1), 'entry_price' (float, optional)
          }

    Returns:
        {
          'net_delta':  float  — per-share, × 100 × contracts summed
          'net_gamma':  float
          'net_theta':  float  — $ per day
          'net_vega':   float  — $ per 1 vol-point move
          'total_max_loss': float
          'position_count': int
          'by_symbol':  { ticker: {delta, gamma, theta, vega, cost} },
          'warnings':   [str, ...],
        }
    """
    net = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
    by_sym: dict[str, dict] = {}
    warnings: list[str] = []
    total_max_loss = 0.0

    for pos in positions or []:
        try:
            sym      = str(pos.get("symbol", "")).upper()
            opt_type = str(pos.get("type", "")).lower()
            strike   = float(pos.get("strike", 0))
            expiry   = pos.get("expiry")
            n        = int(pos.get("contracts", 1) or 1)
            entry    = float(pos.get("entry_price", 0) or 0)

            if not sym or opt_type not in ("call", "put") or not expiry:
                warnings.append(f"skipping malformed position: {pos}")
                continue

            exp_dt = datetime.fromisoformat(str(expiry)[:10])
            dte = max((exp_dt - datetime.now()).days, 0)
            T = max(dte / 365.0, 1/365.0)

            spot = _safe_spot(sym)
            if spot is None:
                warnings.append(f"{sym}: no spot — skipping in Greeks aggregate")
                continue

            iv = _safe_iv(sym, expiry, strike, opt_type)
            if iv is None:
                # Fall back to a 40% default — better than crashing
                iv = 0.40
                warnings.append(f"{sym}: IV unavailable, using 40% fallback")

            g = bs_greeks(spot, strike, T, iv, opt_type)
            mult = 100.0 * n   # 100 shares per contract

            sym_bucket = by_sym.setdefault(sym, {
                "delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "cost": 0.0
            })
            sym_bucket["delta"] += g["delta"] * mult
            sym_bucket["gamma"] += g["gamma"] * mult
            sym_bucket["theta"] += g["theta"] * mult
            sym_bucket["vega"]  += g["vega"]  * mult
            sym_bucket["cost"]  += entry * mult

            net["delta"] += g["delta"] * mult
            net["gamma"] += g["gamma"] * mult
            net["theta"] += g["theta"] * mult
            net["vega"]  += g["vega"]  * mult

            total_max_loss += entry * mult
        except Exception as e:
            warnings.append(f"error aggregating position {pos}: {e}")
            continue

    # Round for display
    for k in net:
        net[k] = round(net[k], 2)
    for sym, bucket in by_sym.items():
        for k in bucket:
            bucket[k] = round(bucket[k], 2)

    return {
        "net_delta":       net["delta"],
        "net_gamma":       net["gamma"],
        "net_theta":       net["theta"],
        "net_vega":        net["vega"],
        "total_max_loss":  round(total_max_loss, 2),
        "position_count":  sum(int(p.get("contracts", 1) or 1) for p in positions or []),
        "by_symbol":       by_sym,
        "warnings":        warnings,
    }


def check_portfolio_heat(agg: dict) -> dict:
    """
    Check aggregated Greeks against risk config limits.

    Returns {'ok': bool, 'warnings': [str, ...]}
    """
    out: list[str] = []
    try:
        max_risk = float(RISK.get("max_total_open_risk", 0) or 0)
        port_size = float(RISK.get("portfolio_size", 0) or 0)

        if max_risk > 0 and agg.get("total_max_loss", 0) > max_risk:
            out.append(
                f"Total open risk ${agg['total_max_loss']:.0f} exceeds "
                f"limit ${max_risk:.0f}"
            )

        # Delta heuristic: net delta > 40% of portfolio / avg-share-equivalent
        # = heavily directional. Tune as needed.
        if port_size > 0 and abs(agg.get("net_delta", 0)) * 100 > port_size * 0.40:
            out.append(
                f"Net delta {agg['net_delta']:+.0f} represents directional "
                f"exposure > 40% of portfolio; consider hedging"
            )

        # Theta > 2% of portfolio/day = rapid bleed
        if port_size > 0 and agg.get("net_theta", 0) < -port_size * 0.02:
            out.append(
                f"Net theta {agg['net_theta']:+.0f}/day is steep vs "
                f"portfolio size ${port_size:.0f}"
            )
    except Exception as e:
        out.append(f"heat check error: {e}")

    return {"ok": len(out) == 0, "warnings": out}


def _safe_spot(symbol: str) -> float | None:
    try:
        h = yf.Ticker(symbol).history(period="1d")
        if h is None or h.empty:
            return None
        return float(h["Close"].iloc[-1])
    except Exception:
        return None


def _safe_iv(symbol: str, expiry: str, strike: float, opt_type: str) -> float | None:
    try:
        chain = yf.Ticker(symbol).option_chain(str(expiry)[:10])
        df = chain.calls if opt_type == "call" else chain.puts
        row = df[df["strike"] == strike]
        if row.empty:
            row = df.iloc[(df["strike"] - strike).abs().argsort()[:1]]
        if row.empty:
            return None
        iv = float(row["impliedVolatility"].iloc[0])
        return iv if iv > 0 else None
    except Exception:
        return None
