"""
Ticker analysis and contract scoring.

Signal hierarchy (all are BUY-side only — no naked selling):
  BUY VOL    IV is 10%+ below directional RV (cheap options)
  FLOW BUY   STRONG unusual flow + EXPLOSIVE GEX regardless of IV level
             Smart money is positioning; IV may not be cheap yet but
             the activity itself signals informed directional conviction.
  NEUTRAL    No strong signal — tracked but not recommended
"""

import pandas as pd
from datetime import datetime

from data.market import get_current_price, get_historical_prices, get_options_chain, check_market_cap
from data.news import get_news
from analysis.vol import (
    calculate_rv, calculate_rv_for_dte, calculate_directional_rv,
    iv_rv_signal, iv_rv_signal_directional, iv_rank,
)
from analysis.flow import enrich_flow, classify_flow
from analysis.skew import calculate_skew
from analysis.gamma import calculate_gex
from analysis.earnings_vol import analyze_earnings_edge
from sentinel_bridge import get_divergence, divergence_score_adjustment
from risk.sizer import size_trade
from risk.config import RISK

# ── New signal feeds (all SAFE-DEFAULT on failure) ───────────────────────────
from data.insider import get_insider_activity, insider_score_delta
from data.short_interest import get_short_interest, short_interest_score_delta
from data.blocks import get_unusual_volume, blocks_score_delta
from data.catalysts import catalysts_in_window, catalyst_score_delta
from analysis.pin_risk import assess_pin_risk, pin_risk_score_delta


# ── Filters ────────────────────────────────────────────────────────────────────
OTM_LIMIT  = 0.15   # 15% OTM (was 10%) — captures more directional plays
MIN_VOLUME = 10     # (was 25) — lower bar; flow signals need fewer contracts
MIN_DTE    = 7
MAX_DTE    = 90

# Premium ceiling — reject expensive contracts outright. On Apr 20 snapshot the
# two worst losses were $24.70 and $21.80 entries (JPM PUT, AMD CALL).
# The 7 winners were all $2.81–$15.52. Cap at $15 unless score > 80.
MAX_PREMIUM_HARD     = 15.0   # per-contract cap for normal scores
MAX_PREMIUM_HIGH_BAR = 25.0   # absolute ceiling even for high-conviction trades

# Trend-exhaustion window (for contrarian check)
TREND_WINDOW_DAYS     = 10
TREND_STRETCHED_PCT   = 0.08   # 8% move in 10d = stretched

# FLOW BUY composite signal: unusual activity even without IV<RV edge
FLOW_BUY_MIN_VOL_OI  = 1.5   # vol/OI ≥ 1.5× = very strong unusual activity
FLOW_BUY_GEX_SIGNALS = {"EXPLOSIVE"}  # GEX regime that amplifies moves


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
    bid   = float(row.get("bid") or 0)
    ask   = float(row.get("ask") or 0)
    entry = _midpoint(bid, ask)
    # Fallback to lastPrice when markets are closed and bid/ask are both 0
    if entry == 0:
        last = float(row.get("lastPrice") or row.get("last_price") or 0)
        if last > 0:
            entry = round(last, 2)
    max_loss = round(entry * 100, 2)
    return {
        "leg1_strike": float(row["strike"]),
        "leg1_action": "BUY",
        "leg2_strike": None,
        "leg2_action": None,
        "entry_price": entry,
        "net_credit":  None,
        "spread_width": None,
        "max_profit":   None,
        "max_loss_per_contract": max_loss,
        "breakeven":    None,
        "trade_detail": (
            f"BUY ${row['strike']:.0f} {row['type'].upper()} "
            f"@ ~${entry:.2f}  |  Max loss: ${max_loss:.0f}/contract"
        ),
    }


def _spread_trade_detail(short_row: pd.Series, long_row: pd.Series) -> dict:
    short_bid    = float(short_row.get("bid") or 0)
    long_ask     = float(long_row.get("ask") or 0)
    net_credit   = round(short_bid - long_ask, 2)
    spread_width = round(abs(float(short_row["strike"]) - float(long_row["strike"])), 2)
    max_loss     = round((spread_width - max(net_credit, 0)) * 100, 2)
    max_profit   = round(max(net_credit, 0) * 100, 2)

    opt_type = short_row["type"]
    if opt_type == "call":
        breakeven = round(float(short_row["strike"]) + net_credit, 2)
        leg_desc  = f"SELL ${short_row['strike']:.0f} CALL  /  BUY ${long_row['strike']:.0f} CALL"
    else:
        breakeven = round(float(short_row["strike"]) - net_credit, 2)
        leg_desc  = f"SELL ${short_row['strike']:.0f} PUT  /  BUY ${long_row['strike']:.0f} PUT"

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
        "net_credit":  net_credit,
        "spread_width": spread_width,
        "max_profit":   max_profit,
        "max_loss_per_contract": max_loss,
        "breakeven":   breakeven,
        "trade_detail": detail,
    }


def score_contract(
    iv: float,
    rv: float,
    vol_oi_ratio: float,
    dte: int,
    vol_signal: str = "NEUTRAL",
    skew: dict | None = None,
    gex: dict | None = None,
    ivr: dict | None = None,
    opt_type: str = "call",
    entry_price: float = 0.0,
    trend_pct: float | None = None,  # last-10d % return of underlying
) -> float:
    """
    Composite score 0–100.  See module docstring for loss-pattern notes that
    motivated the contrarian/premium/trend penalties below.
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

    if vol_signal == "FLOW BUY":
        score = max(score, 30.0)

    # ── Enhancement bonuses ───────────────────────────────────────────────────
    if ivr and ivr.get("iv_rank") is not None:
        rank = ivr["iv_rank"]
        if vol_signal == "BUY VOL" and rank < 0.30:
            score += 8
        elif vol_signal in ("SELL VOL", "FLOW BUY") and rank > 0.70:
            score += 8

    if skew:
        skew_sig = skew.get("skew_signal", "NEUTRAL")
        if vol_signal == "BUY VOL" and skew_sig == "BULLISH":
            score += 7
        elif vol_signal in ("SELL VOL",) and skew_sig == "BEARISH":
            score += 7
        elif vol_signal == "FLOW BUY":
            if skew_sig in ("BULLISH", "BEARISH"):
                score += 5

    if gex:
        g_sig = gex.get("gex_signal", "NEUTRAL")
        if g_sig == "EXPLOSIVE":
            score += 5
        elif g_sig == "PINNED":
            score -= 5

    # ── Contrarian dampeners (lessons from Apr 20 snapshot losses) ────────────
    # 1. Peak fear / peak greed: extreme IV rank + aligned directional signal
    #    often marks capitulation (bounce imminent). The Apr 20 JPM PUT at
    #    score 53 lost $525 — classic high-IV-rank BUY PUT at bottom.
    if ivr and ivr.get("iv_rank") is not None:
        rank = ivr["iv_rank"]
        if vol_signal == "BUY VOL" and rank > 0.80 and opt_type == "put":
            score -= 15  # peak fear — puts likely to collapse on bounce
        elif vol_signal == "BUY VOL" and rank < 0.15 and opt_type == "call":
            score -= 10  # peak complacency — calls likely to fade

    # 2. Trend exhaustion: buying puts on a stock already down sharply (or
    #    calls on one already up sharply) is chasing a move that's priced in.
    if trend_pct is not None and vol_signal in ("BUY VOL", "FLOW BUY"):
        if opt_type == "put" and trend_pct < -TREND_STRETCHED_PCT:
            score -= 12  # stock already down >8% in 10d — late on puts
        elif opt_type == "call" and trend_pct > TREND_STRETCHED_PCT:
            score -= 12  # stock already up >8% in 10d — late on calls

    # 3. Premium efficiency: expensive contracts have identical "max loss =
    #    premium" framing but much larger absolute-dollar risk. Winning
    #    contracts on Apr 20 averaged $8 entry; the two worst losses were
    #    $24.70 and $21.80. Penalize every $5 over $5 threshold.
    if entry_price > 5.0:
        score -= min((entry_price - 5.0) / 5.0 * 4, 15)

    return round(max(min(score, 100), 0), 1)


def _is_flow_buy(vol_oi_ratio: float, gex: dict | None) -> bool:
    """
    Composite FLOW BUY: unusual institutional/smart-money activity signal.
    Triggers when:
      - vol/OI ≥ 1.5 (very fresh, aggressive positioning) AND
      - GEX is EXPLOSIVE (dealer hedging amplifies the move)
    """
    if vol_oi_ratio < FLOW_BUY_MIN_VOL_OI:
        return False
    if gex and gex.get("gex_signal") in FLOW_BUY_GEX_SIGNALS:
        return True
    return False


def analyze_ticker(symbol: str) -> tuple[pd.DataFrame | None, list[dict], str | None, dict | None]:
    symbol = symbol.upper().strip()

    eligible, cap, company_name = check_market_cap(symbol)
    if not eligible:
        cap_fmt = f"${cap/1e9:.2f}B" if cap >= 1e9 else f"${cap/1e6:.0f}M"
        return None, [], f"{symbol} market cap ({cap_fmt}) is below the $100M minimum", None

    price = get_current_price(symbol)
    if price is None:
        return None, [], f"Could not fetch price for {symbol}", None

    prices = get_historical_prices(symbol, days=90)
    if prices is None:
        return None, [], f"Could not fetch historical prices for {symbol}", None

    rv30 = calculate_rv(prices, window=30)
    if rv30 is None:
        return None, [], f"Not enough price history to calculate realized vol for {symbol}", None

    # Directional RV (upside for calls, downside for puts)
    dir_rv = calculate_directional_rv(prices, window=30)

    # IV rank proxy
    ivr = iv_rank(rv30, prices)

    # 10-day trend — used for trend-exhaustion contrarian check
    trend_pct = None
    if len(prices) >= TREND_WINDOW_DAYS + 1:
        trend_pct = float(prices.iloc[-1] / prices.iloc[-(TREND_WINDOW_DAYS + 1)] - 1)

    # Mode-based underlying-price filter — small accounts skip mega-cap names
    # whose OTM contracts cost too much even at the 8% cap. See
    # risk/config.py → MICRO_MODE / STANDARD_MODE.
    max_underlying = RISK.get("max_underlying_price", 10_000)
    if price > max_underlying:
        return None, [], (
            f"underlying ${price:.0f} above mode limit ${max_underlying} "
            f"— use a larger account tier to trade this name"
        ), None

    chain_filtered, earnings_date, err = get_options_chain(symbol)
    if err:
        return None, [], err, None

    chain_full = chain_filtered.copy()

    news      = get_news(symbol)
    divergence = get_divergence(symbol)

    skew = calculate_skew(chain_full, price)
    gex  = calculate_gex(chain_full, price)

    # ── Ticker-level enrichments (one yfinance / EDGAR call per ticker) ──────
    # Every helper returns a SAFE-DEFAULT dict on failure, so None-guards below
    # aren't strictly required — but we're defensive anyway.
    try:    insider_info = get_insider_activity(symbol)
    except Exception: insider_info = None
    try:    short_info   = get_short_interest(symbol)
    except Exception: short_info = None
    try:    blocks_info  = get_unusual_volume(symbol)
    except Exception: blocks_info = None

    earnings_edge = analyze_earnings_edge(symbol, chain_full, price, earnings_date)

    # ── Filter to tradeable contracts ─────────────────────────────────────────
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
        return None, news, f"No contracts passed filters for {symbol} (price=${price:.2f}, RV={rv30*100:.1f}%)", None

    rows = []
    for _, row in chain.iterrows():
        iv      = float(row["impliedVolatility"])
        vol_oi  = float(row["vol_oi_ratio"])
        dte     = int(row["dte"])
        opt_type = row["type"]

        # DTE-matched RV for accurate per-contract comparison
        rv_dte = calculate_rv_for_dte(prices, dte)
        if rv_dte is None:
            rv_dte = rv30

        # Directional IV vs RV signal
        dir_signal, dir_spread, _ = iv_rv_signal_directional(iv, opt_type, dir_rv, dte)
        # Also run combined signal as tiebreaker / cross-check
        combined_signal, combined_spread, _ = iv_rv_signal(iv, rv_dte)

        # Prefer directional signal; if neutral, check combined
        if dir_signal != "NEUTRAL":
            vol_signal   = dir_signal
            iv_rv_spread = dir_spread
        else:
            vol_signal   = combined_signal
            iv_rv_spread = combined_spread

        # FLOW BUY override: unusual activity even without IV edge
        if vol_signal == "NEUTRAL" and _is_flow_buy(vol_oi, gex):
            vol_signal = "FLOW BUY"

        # Build trade detail
        if vol_signal in ("BUY VOL", "FLOW BUY"):
            trade  = _buy_trade_detail(row)
            action = f"BUY {opt_type.upper()}"
        elif vol_signal == "SELL VOL":
            group    = chain[(chain["expiry"] == row["expiry"]) & (chain["type"] == opt_type)]
            long_leg = _find_protection_leg(row, group)
            if long_leg is not None:
                trade  = _spread_trade_detail(row, long_leg)
                action = f"SPREAD ({opt_type.upper()} credit)"
            else:
                trade = {k: None for k in [
                    "leg1_strike", "leg1_action", "leg2_strike", "leg2_action",
                    "entry_price", "net_credit", "spread_width", "max_profit",
                    "max_loss_per_contract", "breakeven",
                ]}
                trade["trade_detail"] = f"SELL VOL — no protection leg for ${row['strike']:.0f}"
                action = "SELL VOL (no spread)"
        else:
            trade = {k: None for k in [
                "leg1_strike", "leg1_action", "leg2_strike", "leg2_action",
                "entry_price", "net_credit", "spread_width", "max_profit",
                "max_loss_per_contract", "breakeven", "trade_detail",
            ]}
            trade["trade_detail"] = "—"
            action = "WATCH"

        entry_px_for_score = float(trade.get("entry_price") or 0)
        # Hard premium ceiling: drop absurdly expensive contracts outright.
        # Max-loss = premium, so a $25 contract risks $2,500 per lot.
        if vol_signal in ("BUY VOL", "FLOW BUY") and entry_px_for_score > MAX_PREMIUM_HIGH_BAR:
            continue

        # Mode-based per-contract premium cap. In MICRO mode this is $0.75;
        # filters out contracts the account can't size into prudently. Keeps
        # the OTM% filter intact (deeper OTM has worse risk-adjusted EV).
        mode_premium_cap = RISK.get("max_contract_premium")
        if (mode_premium_cap is not None
                and vol_signal in ("BUY VOL", "FLOW BUY")
                and entry_px_for_score > mode_premium_cap):
            continue

        base_score     = score_contract(iv, rv_dte, vol_oi, dte,
                                        vol_signal=vol_signal, skew=skew, gex=gex, ivr=ivr,
                                        opt_type=opt_type, entry_price=entry_px_for_score,
                                        trend_pct=trend_pct)
        sentiment_delta = divergence_score_adjustment(divergence, vol_signal)

        # ── Per-contract enrichments ─────────────────────────────────────────
        try:
            catalyst_info = catalysts_in_window(symbol, dte)
        except Exception:
            catalyst_info = None
        try:
            pin_info = assess_pin_risk(chain_full, price, float(row["strike"]), dte, gex)
        except Exception:
            pin_info = None

        # Score deltas — each returns 0.0 on unknown / neutral
        try:    insider_delta = insider_score_delta(insider_info, opt_type)
        except Exception: insider_delta = 0.0
        try:    short_delta   = short_interest_score_delta(short_info, opt_type)
        except Exception: short_delta = 0.0
        try:    blocks_delta  = blocks_score_delta(blocks_info, opt_type, vol_signal)
        except Exception: blocks_delta = 0.0
        try:    catalyst_delta = catalyst_score_delta(catalyst_info, vol_signal)
        except Exception: catalyst_delta = 0.0
        try:    pin_delta     = pin_risk_score_delta(pin_info)
        except Exception: pin_delta = 0.0

        extras_delta = (insider_delta + short_delta + blocks_delta
                        + catalyst_delta + pin_delta)
        final_score = round(min(max(
            base_score + sentiment_delta + extras_delta, 0), 100), 1)

        # Secondary soft filter: keep >$15 contracts only if score is strong
        if (vol_signal in ("BUY VOL", "FLOW BUY")
                and entry_px_for_score > MAX_PREMIUM_HARD
                and final_score < 80):
            continue

        divergence_flag = "—"
        if divergence and divergence.get("direction"):
            if divergence["direction"] == "bearish_divergence":
                divergence_flag = "⚠️ BEAR DIV"
            elif divergence["direction"] == "bullish_divergence":
                divergence_flag = "📈 BULL DIV"

        max_loss = trade.get("max_loss_per_contract") or 0
        sizing   = size_trade(max_loss_per_contract=max_loss, score=final_score) if max_loss > 0 else None

        rows.append({
            "symbol":        symbol,
            "company_name":  company_name,
            "type":          opt_type,
            "strike":        float(row["strike"]),
            "expiry":        row["expiry"],
            "dte":           dte,
            "stock_price":   round(price, 2),
            "bid":           round(float(row.get("bid") or 0), 2),
            "ask":           round(float(row.get("ask") or 0), 2),
            "iv_pct":        round(iv * 100, 1),
            "rv_pct":        round(rv_dte * 100, 1),
            "iv_rv_spread":  round(iv_rv_spread * 100, 1),
            "vol_signal":    vol_signal,
            "action":        action,
            "volume":        int(row["volume"]),
            "open_interest": int(row["openInterest"]),
            "vol_oi_ratio":  vol_oi,
            "flow_signal":   row["flow_signal"],
            "score":         final_score,
            "sentiment_delta": sentiment_delta,
            "insider_delta": insider_delta,
            "short_delta":   short_delta,
            "blocks_delta":  blocks_delta,
            "catalyst_delta": catalyst_delta,
            "pin_delta":     pin_delta,
            "insider_signal":  (insider_info or {}).get("signal"),
            "short_signal":    (short_info or {}).get("signal"),
            "blocks_signal":   (blocks_info or {}).get("signal"),
            "catalyst_summary": (catalyst_info or {}).get("summary"),
            "pin_risk":        (pin_info or {}).get("pin_risk"),
            "divergence_flag": divergence_flag,
            "earnings":      earnings_date.strftime("%Y-%m-%d") if earnings_date else "—",
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
            "suggested_contracts":   sizing["contracts"] if sizing else None,
            "suggested_risk_dollar": sizing["risk_dollar"] if sizing else None,
            "sizing_rationale":      sizing["rationale"] if sizing else "—",
            **trade,
        })

    result_df = (
        pd.DataFrame(rows)
        .sort_values("score", ascending=False)
        .head(5)           # was 3; show top 5 per ticker
        .reset_index(drop=True)
    )
    return result_df, news, None, earnings_edge
