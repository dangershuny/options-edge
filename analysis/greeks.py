"""
Black-Scholes Greeks calculator.

Used by gamma exposure (GEX) analysis and any module that needs
per-contract delta/gamma/theta/vega without paying for a data feed.
"""

import numpy as np
from scipy.stats import norm

RISK_FREE_RATE = 0.05  # approximate current Fed Funds rate


def bs_greeks(
    S: float, K: float, T: float, sigma: float,
    option_type: str, r: float = RISK_FREE_RATE
) -> dict:
    """
    Black-Scholes European option Greeks.

    S:           spot price
    K:           strike price
    T:           time to expiry in years (e.g. 30/365)
    sigma:       implied volatility as decimal (e.g. 0.30 for 30%)
    option_type: 'call' or 'put'
    r:           risk-free rate as decimal

    Returns dict with keys: delta, gamma, theta, vega
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    pdf_d1 = norm.pdf(d1)
    gamma = pdf_d1 / (S * sigma * sqrt_T)
    vega  = S * pdf_d1 * sqrt_T / 100  # per 1% change in vol

    if option_type.lower() == "call":
        delta = norm.cdf(d1)
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            - r * K * np.exp(-r * T) * norm.cdf(d2)
        ) / 365
    else:
        delta = norm.cdf(d1) - 1
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            + r * K * np.exp(-r * T) * norm.cdf(-d2)
        ) / 365

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta, 4),
        "vega":  round(vega, 4),
    }
