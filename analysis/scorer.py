import pandas as pd
from datetime import datetime

from data.market import get_current_price, get_historical_prices, get_options_chain, check_market_cap
from data.news import get_news
from analysis.vol import calculate_rv, iv_rv_signal, iv_percentile_label, iv_rank
from analysis.flow import enrich_flow, classify_flow
from analysis.skew import calculate_skew
from analysis.gamma import calculate_gex
from analysis.earnings_vol import analyze_earnings_edge
from sentinel_bridge import get_divergence, divergence_score_adjustment
from risk.sizer import size_trade


OTM_LIMIT = 0.10
MIN_VOLUME = 25
MIN_DTE = 7
MAX_DTE = 90


def _midpoint(bid: float, ask: float) -> float:
    if bid > 0 and ask > 0:
        return round((bid + ask) / 2, 2)
    return round(ask or bid, 2)


def _find_protection_leg(short_row: pd.Series, group: pd.DataFrame) -> pd.Series | None:
    strike = float(short_row["strike"])
    if short_row["type"] == "call":
        candidates = group[group["strike"] > strike].sort_values("strike")
    else:
        candidates = group[group["strike"] < strike].sort_values("strike", ascending=False)
    return candidates.iloc[0] if not candidates.empty else None


def _buy_trade_detail(row: pd.Series) -> dict:
    bid = float(row.get("bid") or 0)
    ask = float(row.get("ask") or 0)
    entry = _midpoint(bid, ask)
    max_loss = round(entry * 100, 2)
    return {
        "leg1_strike": float(row["strike"]),
        "leg1_action": "BUY",
        "leg2_strike": None,
        "leg2_action": None,
        "entry_price": entry,
        "net_credit": None,
        "spread_width": None,
        "max_profit": None,
        "max_loss_per_contract": max_loss,
        "breakeven": None,
        "trade_detail": (
            f"BUY ${row['strike']:.0f} {row['type'].upper()} "
            f"@ ~${entry:.2f}  |  Max loss: ${max_loss:.0f}/contract"
        ),
    }


def _spread_trade_detail(short_row: pd.Series, long_row: pd.Series) -> dict:
    short_bid = float(short_row.get("bid") or 0)
    long_ask  = float(long_row.get("ask") or 0)
    net_credit = round(short_bid - long_ask, 2)
    spread_width = round(abs(float(short_row["strike"]) - float(long_row["strike"])), 2)
    max_loss = round((spread_width - max(net_credit, 0)) * 100, 2)
    max_profit = round(max(net_credit, 0) * 100, 2)

    opt_type = short_row["type"]
    if opt_type == "call":
        breakeven = round(float(short_row["strike"]) + net_credit, 2)
        leg_desc = f"SELL ${short_row['strike']:.0f} CALL  /  BUY ${long_row['strike']:.0f} CALL"
    else:
        breakeven = round(float(short_row["strike"]) - net_credit, 2)
        leg_desc = f"SELL ${short_row['strike']:.0f} PUT  /  BUY ${long_row['strike']:.0f} PUT"

    credit_str = f"${net_credit:.2f} credit" if net_credit > 0 else f"${abs(net_credit):.2f} debit (legs too wide)"
    detail = (
        f"{leg_desc}  |  {credit_str}  |  "
        f"Max profit: ${max_profit:.0f}  |  Max loss: ${max_loss:.0f}  |  BE: ${breakeven:.2f}"
    )
    return {
        "leg1_strike": float(short_row["strike"]),
        "leg1_action": "SELL",
        "leg2_strike": float(long_row["strike"]),
        "leg2_action": "BUY",
        "entry_price": None,
        "net_credit": net_credit,
        "spread_width": spread_width,
        "max_profit": max_profit,
        "max_loss_per_contract": max_loss,
        "breakeven": breakeven,
        "trade_detail": detail,
    }


def score_contract(
    iv: float,
    rv: float,
    vol_oi_ratio: float,
    dte: int,
    skew: dict | None = None,
    gex: dict | None = None,
    ivr: dict | None = None,
    vol_signal: str = "NEUTRAL",
) -> float:
    """
    Composite score 0–100.

    Base components (unchanged):
      vol mismatch × 50 pts
      flow signal   up to 35 pts
      DTE bonus     up to 10 pts

    New enhancement bonuses (additive, capped at 100 total):
      +8  IV rank confirms signal direction
      +7  skew direction aligns with vol signal
      +5  GEX EXPLOSIVE regime (dealers amplify moves → options plays pay more)
      -5  GEX PINNED (price clamped by gamma wall → opts may expire worthless)
    """
    vol_sig, _, vol_strength = iv_rv_signal(iv, rv)
    flow_sig = classify_flow(vol_oi_ratio)

    score = 0.0

    # ── Base ──────────────────────────────────────────────────────────────────
    if vol_sig != "NEUTRAL":
        score += vol_strength * 50

    if flow_sig == "STRONG":
        score += 35
    elif flow_sig == "ELEVATED":
        score += 15

    if 21 <= dte <= 45:
        score += 10
    elif 14 <= dte <= 60:
        score += 5

    # ── IV rank alignment ─────────────────────────────────────────────────────
    if ivr and ivr.get("iv_rank") is not None:
        rank = ivr["iv_rank"]
        if vol_signal == "BUY VOL" and rank < 0.30:
            score += 8   # IV cheap historically → confirms buy-vol thesis
        elif vol_signal == "SELL VOL" and rank > 0.70:
            score += 8   # IV rich historically → confirms sell-vol thesis

    # ── Skew alignment ────────────────────────────────────────────────────────
    if skew:
        skew_sig = skew.get("skew_signal", "NEUTRAL")
        if vol_signal == "BUY VOL" and skew_sig == "BULLISH":
            score += 7
        elif vol_signal == "SELL VOL" and skew_sig == "BEARISH":
            score += 7

    # ── GEX regime ────────────────────────────────────────────────────────────
    if gex:
        g_sig = gex.get("gex_signal", "NEUTRAL")
        if g_sig == "EXPLOSIVE":
            score += 5   # moves amplify → long options pay
        elif g_sig == "PINNED":
            score -= 5   # gamma wall suppresses movement

    return round(min(score, 100), 1)


def analyze_ticker(symbol: str) -> tuple[pd.DataFrame | None, list[dict], str | None]:
    symbol = symbol.upper().strip()

    eligible, cap, company_name = check_market_cap(symbol)
    if not eligible:
        cap_fmt = f"${cap/1e9:.2f}B" if cap >= 1e9 else f"${cap/1e6:.0f}M"
        return None, [], f"{symbol} market cap ({cap_fmt}) is below the $100M minimum"

    price = get_current_price(symbol)
    if price is None:
        return None, [], f"Could not fetch price for {symbol}"

    prices = get_historical_prices(symbol, days=90)
    if prices is None:
        return None, [], f"Could not fetch historical prices for {symbol}"

    rv = calculate_rv(prices, window=30)
    if rv is None:
        return None, [], f"Not enough price history to calculate realized vol for {symbol}"

    # IV rank (uses full price history)
    ivr = iv_rank(rv, prices)   # uses current rv as the IV proxy for ranking

    chain_filtered, earnings_date, err = get_options_chain(symbol)
    if err:
        return None, [], err

    # Need full unfiltered chain for skew + GEX + earnings edge
    # get_options_chain already filters earnings-adjacent expiries but keeps all strikes
    chain_full = chain_filtered.copy()

    news = get_news(symbol)
    divergence = get_divergence(symbol)

    # ── Skew + GEX from full chain ─────────────────────────────────────────────
    skew = calculate_skew(chain_full, price)
    gex  = calculate_gex(chain_full, price)

    # ── Earnings edge ─────────────────────────────────────────────────────────
    earnings_edge = analyze_earnings_edge(symbol, chain_full, price, earnings_date)

    # ── Filter chain for tradeable contracts ──────────────────────────────────
    lower = price * (1 - OTM_LIMIT)
    upper = price * (1 + OTM_LIMIT)
    chain = chain_filtered[
        (chain_filtered["strike"] >= lower) &
        (chain_filtered["strike"] <= upper) &
        (chain_filtered["dte"] >= MIN_DTE) &
        (chain_filtered["dte"] <= MAX_DTE)
    ].copy()

    chain = chain[chain["impliedVolatility"].notna() & (chain["impliedVolatility"] > 0.001)]
    chain = enrich_flow(chain)
    chain = chain[chain["volume"] >= MIN_VOLUME].reset_index(drop=True)

    if chain.empty:
        return None, news, f"No contracts passed filters for {symbol} (price=${price:.2f}, RV={rv*100:.1f}%)"

    rows = []
    for _, row in chain.iterrows():
        iv = float(row["impliedVolatility"])
        vol_oi = float(row["vol_oi_ratio"])
        dte = int(row["dte"])
        vol_signal, iv_rv_spread, _ = iv_rv_signal(iv, rv)

        if vol_signal == "BUY VOL":
            trade = _buy_trade_detail(row)
            action = f"BUY {row['type'].upper()}"
        elif vol_signal == "SELL VOL":
            group = chain[(chain["expiry"] == row["expiry"]) & (chain["type"] == row["type"])]
            long_leg = _find_protection_leg(row, group)
            if long_leg is not None:
                trade = _spread_trade_detail(row, long_leg)
                action = f"SPREAD ({row['type'].upper()} credit)"
            else:
                trade = {k: None for k in [
                    "leg1_strike", "leg1_action", "leg2_strike", "leg2_action",
                    "entry_price", "net_credit", "spread_width", "max_profit",
                    "max_loss_per_contract", "breakeven",
                ]}
                trade["trade_detail"] = f"SELL VOL — no protection leg available for ${row['strike']:.0f}"
                action = "SELL VOL (no spread)"
        else:
            trade = {k: None for k in [
                "leg1_strike", "leg1_action", "leg2_strike", "leg2_action",
                "entry_price", "net_credit", "spread_width", "max_profit",
                "max_loss_per_contract", "breakeven", "trade_detail",
            ]}
            trade["trade_detail"] = "—"
            action = "WATCH"

        base_score = score_contract(iv, rv, vol_oi, dte,
                                    skew=skew, gex=gex, ivr=ivr,
                                    vol_signal=vol_signal)
        sentiment_delta = divergence_score_adjustment(divergence, vol_signal)
        final_score = round(min(max(base_score + sentiment_delta, 0), 100), 1)

        # Position sizing (uses max_loss from trade detail)
        max_loss = trade.get("max_loss_per_contract") or 0
        sizing = size_trade(max_loss_per_contract=max_loss, score=final_score) if max_loss > 0 else None

        divergence_flag = "—"
        if divergence and divergence.get("direction"):
            if divergence["direction"] == "bearish_divergence":
                divergence_flag = "⚠️ BEAR DIV"
            elif divergence["direction"] == "bullish_divergence":
                divergence_flag = "📈 BULL DIV"

        rows.append({
            "symbol":        symbol,
            "company_name":  company_name,
            "type":          row["type"],
            "strike":        float(row["strike"]),
            "expiry":        row["expiry"],
            "dte":           dte,
            "stock_price":   round(price, 2),
            "bid":           round(float(row.get("bid") or 0), 2),
            "ask":           round(float(row.get("ask") or 0), 2),
            "iv_pct":        round(iv * 100, 1),
            "rv_pct":        round(rv * 100, 1),
            "iv_rv_spread":  round(iv_rv_spread * 100, 1),
            "vol_signal":    vol_signal,
            "action":        action,
            "volume":        int(row["volume"]),
            "open_interest": int(row["openInterest"]),
            "vol_oi_ratio":  vol_oi,
            "flow_signal":   row["flow_signal"],
            "score":         final_score,
            "sentiment_delta": sentiment_delta,
            "divergence_flag": divergence_flag,
            "earnings":      earnings_date.strftime("%Y-%m-%d") if earnings_date else "—",
            # ── New surface / regime data ──────────────────────────────────────
            "iv_rank_label": ivr.get("iv_rank_label", "N/A"),
            "iv_rank":       ivr.get("iv_rank"),
            "skew_signal":   skew.get("skew_signal", "NEUTRAL"),
            "skew_summary":  skew.get("skew_summary", "—"),
            "pc_ratio":      skew.get("pc_ratio"),
            "risk_reversal": skew.get("risk_reversal"),
            "gex_signal":    gex.get("gex_signal", "NEUTRAL"),
            "gex_summary":   gex.get("gex_summary", "—"),
            "gamma_wall":    gex.get("gamma_wall"),
            "gamma_flip":    gex.get("gamma_flip"),
            # Position sizing
            "suggested_contracts": sizing["contracts"] if sizing else None,
            "suggested_risk_dollar": sizing["risk_dollar"] if sizing else None,
            "sizing_rationale": sizing["rationale"] if sizing else "—",
            **trade,
        })

    # Attach earnings edge at ticker level (same for all contracts of this ticker)
    result_df = (
        pd.DataFrame(rows)
        .sort_values("score", ascending=False)
        .head(3)
        .reset_index(drop=True)
    )

    return result_df, news, None, earnings_edge
