import pandas as pd
from datetime import datetime

from data.market import get_current_price, get_historical_prices, get_options_chain, check_market_cap
from data.news import get_news
from analysis.vol import calculate_rv, iv_rv_signal, iv_percentile_label
from analysis.flow import enrich_flow, classify_flow
from sentinel_bridge import get_divergence, divergence_score_adjustment


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


def score_contract(iv: float, rv: float, vol_oi_ratio: float, dte: int) -> float:
    vol_signal, _, vol_strength = iv_rv_signal(iv, rv)
    flow_sig = classify_flow(vol_oi_ratio)
    score = 0.0
    if vol_signal != "NEUTRAL":
        score += vol_strength * 50
    if flow_sig == "STRONG":
        score += 35
    elif flow_sig == "ELEVATED":
        score += 15
    if 21 <= dte <= 45:
        score += 10
    elif 14 <= dte <= 60:
        score += 5
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

    chain, earnings_date, err = get_options_chain(symbol)
    if err:
        return None, [], err

    news = get_news(symbol)

    # Pull divergence from news sentinel (non-blocking — returns {} if server offline)
    divergence = get_divergence(symbol)

    # Filter: ATM range, DTE, volume
    lower = price * (1 - OTM_LIMIT)
    upper = price * (1 + OTM_LIMIT)
    chain = chain[
        (chain["strike"] >= lower) &
        (chain["strike"] <= upper) &
        (chain["dte"] >= MIN_DTE) &
        (chain["dte"] <= MAX_DTE)
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

        base_score = score_contract(iv, rv, vol_oi, dte)
        sentiment_delta = divergence_score_adjustment(divergence, vol_signal)
        final_score = round(min(max(base_score + sentiment_delta, 0), 100), 1)

        divergence_flag = "—"
        if divergence and divergence.get("direction"):
            if divergence["direction"] == "bearish_divergence":
                divergence_flag = "⚠️ BEAR DIV"
            elif divergence["direction"] == "bullish_divergence":
                divergence_flag = "📈 BULL DIV"

        rows.append({
            "symbol": symbol,
            "company_name": company_name,
            "type": row["type"],
            "strike": float(row["strike"]),
            "expiry": row["expiry"],
            "dte": dte,
            "stock_price": round(price, 2),
            "bid": round(float(row.get("bid") or 0), 2),
            "ask": round(float(row.get("ask") or 0), 2),
            "iv_pct": round(iv * 100, 1),
            "rv_pct": round(rv * 100, 1),
            "iv_rv_spread": round(iv_rv_spread * 100, 1),
            "vol_signal": vol_signal,
            "action": action,
            "volume": int(row["volume"]),
            "open_interest": int(row["openInterest"]),
            "vol_oi_ratio": vol_oi,
            "flow_signal": row["flow_signal"],
            "score": final_score,
            "sentiment_delta": sentiment_delta,
            "divergence_flag": divergence_flag,
            "earnings": earnings_date.strftime("%Y-%m-%d") if earnings_date else "—",
            **trade,
        })

    result_df = (
        pd.DataFrame(rows)
        .sort_values("score", ascending=False)
        .head(3)
        .reset_index(drop=True)
    )
    return result_df, news, None
