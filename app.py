import json
import os
import streamlit as st
import pandas as pd

from analysis.scorer import analyze_ticker
from analysis.discover import run_discovery
from data.news import news_tool_status
from data.macro import get_vix_context, reset_cache as reset_vix_cache
from sentinel_bridge import sentinel_status, ensure_sentinel_running, sentinel_last_error, scan_ticker as sentinel_scan_ticker
from risk.config import RISK

WATCHLIST_FILE = "watchlist.json"

st.set_page_config(
    page_title="Options Edge Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .signal-buy  { color: #00e676; font-weight: bold; }
  .signal-sell { color: #ff5252; font-weight: bold; }
  .signal-watch{ color: #ffab40; }
  .flow-strong { color: #ffd740; font-weight: bold; }
  .flow-elev   { color: #ffe082; }
  .score-high  { color: #00e676; font-weight: bold; }
  .score-mid   { color: #ffab40; }
  .div-bear    { color: #ff5252; font-weight: bold; }
  .div-bull    { color: #00e676; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


def load_watchlist() -> list[str]:
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def save_watchlist(tickers: list[str]) -> None:
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(tickers, f)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 Options Edge")
    st.caption("IV vs RV · Unusual Flow · Sentiment Divergence")
    st.divider()

    # ── Market context (VIX) ─────────────────────────────────────────────────
    vix_ctx = get_vix_context()
    regime  = vix_ctx["regime"]
    vix_val = vix_ctx["vix"]

    regime_color = {
        "LOW": "🟢", "NORMAL": "⚪", "ELEVATED": "🟡", "FEAR": "🔴", "UNKNOWN": "⚫",
    }.get(regime, "⚫")

    st.subheader("Market Context")
    if vix_val:
        st.markdown(f"{regime_color} **VIX {vix_val:.1f}** — {regime}")
        if vix_ctx.get("vix9d"):
            slope = vix_ctx["term_slope"] or 0
            struct = "backwardation ⚠️" if slope < -2 else ("contango" if slope > 2 else "flat")
            st.caption(f"VIX9D {vix_ctx['vix9d']:.1f}  ·  term {struct}")
        lean = vix_ctx["lean"]
        if lean == "BUY VOL":
            st.caption("Regime lean: **buy vol / long options**")
        elif lean == "SELL VOL":
            st.caption("Regime lean: **sell vol / credit spreads**")
    else:
        st.caption("VIX unavailable")

    if st.button("🔄 Refresh VIX", use_container_width=True):
        reset_vix_cache()
        st.rerun()

    st.divider()
    st.subheader("Watchlist")
    watchlist = load_watchlist()

    new_ticker = st.text_input("Add ticker", placeholder="e.g. AAPL").upper().strip()
    if st.button("➕ Add", use_container_width=True) and new_ticker:
        if new_ticker not in watchlist:
            watchlist.append(new_ticker)
            save_watchlist(watchlist)
            st.rerun()
        else:
            st.warning(f"{new_ticker} already in watchlist")

    if watchlist:
        to_remove = st.selectbox("Remove", ["—"] + watchlist)
        if st.button("🗑 Remove", use_container_width=True) and to_remove != "—":
            watchlist.remove(to_remove)
            save_watchlist(watchlist)
            st.rerun()

        st.divider()
        st.markdown("**Tickers:**")
        for t in watchlist:
            st.markdown(f"• `{t}`")

    st.divider()
    st.subheader("Risk Settings")
    st.caption(f"Portfolio: **${RISK['portfolio_size']:,}**")
    st.caption(f"Max risk / trade: **${RISK['max_cost_per_trade']}**")
    st.caption(f"Max total open risk: **${RISK['max_total_open_risk']:,}**")
    st.caption(f"Min score to trade: **{RISK['min_score_to_trade']}**")
    st.caption("Edit `risk/config.py` to adjust limits")

    st.divider()
    st.caption("**Signal guide**")
    st.caption("🟢 BUY CALL/PUT — IV cheap vs RV, buy the move")
    st.caption("🔴 SPREAD — IV rich, consider credit spread")
    st.caption("🟡 WATCH — No strong vol mismatch yet")
    st.caption("⚡ STRONG flow — Vol/OI ≥ 1×")
    st.caption("⚠️ BEAR DIV — Market bullish, news/social bearish")
    st.caption("📈 BULL DIV — Market bearish, news/social bullish")
    st.caption("Score adjusts ±15 pts on divergence alignment (±20 with 8-K)")
    st.caption("📊 RSI 14 — ±5 pts at extremes (≤25 oversold / ≥75 overbought)")
    st.caption("⚠️ Earnings-adjacent expiries excluded automatically")
    st.divider()
    st.caption(f"📡 News tool: **{news_tool_status()}**")
    # Sentiment status is silent on success; failure is surfaced by the top banner.


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("Options Edge Scanner")

# Auto-start news sentinel. Cached so reruns don't re-launch.
if "sentinel_checked" not in st.session_state:
    st.session_state["sentinel_checked"] = True
    st.session_state["sentinel_ok"] = ensure_sentinel_running()
    st.session_state["sentinel_err"] = sentinel_last_error()

# Only show a banner when sentiment is missing — silent on success.
if not st.session_state.get("sentinel_ok", False):
    st.warning(
        f"⚠️ **News sentiment offline** — scoring without the sentiment signal "
        f"(can move scores by ±15). Reason: "
        f"_{st.session_state.get('sentinel_err') or 'server unreachable'}_"
    )

tab_watchlist, tab_discover = st.tabs(["📋 Watchlist", "🔭 Discover"])


# ═══════════════════════════════════════════════════════════════════════════════
# DISCOVER TAB
# ═══════════════════════════════════════════════════════════════════════════════
with tab_discover:
    st.markdown(
        "Scans ~100 high-liquidity tickers. Returns the **top 10** by IV vs RV mismatch — "
        "each with the **top 3 contracts** for full analysis. Takes ~60–90 seconds."
    )

    if st.button("🔭 Run Discovery Scan", type="primary"):
        with st.spinner("Batch-downloading price history and scanning options chains…"):
            disc_df = run_discovery(top_n=10)

        if disc_df.empty:
            st.error("Discovery scan returned no results. Try again later.")
        else:
            st.success(f"Found {len(disc_df)} tickers with significant vol mismatches.")

            def color_disc(row):
                styles = [""] * len(row)
                col_list = list(row.index)
                for field, fn in [
                    ("vol_signal", lambda v: "color: #00e676; font-weight: bold" if v == "BUY VOL" else "color: #ff5252; font-weight: bold"),
                    ("iv_rv_spread", lambda v: "color: #00e676" if v < 0 else "color: #ff5252"),
                ]:
                    if field in col_list:
                        styles[col_list.index(field)] = fn(row[field])
                return styles

            show_cols = ["symbol", "price", "iv_pct", "rv_pct", "iv_rv_spread",
                         "vol_signal", "atm_strike", "atm_entry", "atm_expiry"]
            show_cols = [c for c in show_cols if c in disc_df.columns]
            labels = {"symbol": "Ticker", "price": "Price", "iv_pct": "IV %",
                      "rv_pct": "RV %", "iv_rv_spread": "IV−RV", "vol_signal": "Signal",
                      "atm_strike": "ATM Strike", "atm_entry": "Entry $",
                      "atm_expiry": "Expiry"}
            st.dataframe(
                disc_df[show_cols].rename(columns=labels).style.apply(color_disc, axis=1),
                use_container_width=True, hide_index=True,
            )

            st.divider()
            st.subheader("Add discoveries to watchlist")
            to_add = st.multiselect(
                "Select tickers to add to your watchlist for full analysis:",
                disc_df["symbol"].tolist(),
            )
            if st.button("➕ Add to Watchlist") and to_add:
                for t in to_add:
                    if t not in watchlist:
                        watchlist.append(t)
                save_watchlist(watchlist)
                st.success(f"Added {', '.join(to_add)} to watchlist. Switch to the Watchlist tab to scan.")
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLIST TAB
# ═══════════════════════════════════════════════════════════════════════════════
with tab_watchlist:

    if not watchlist:
        st.info("Add tickers in the sidebar, or use the Discover tab to find candidates.")
        st.stop()

    col_scan, col_sel = st.columns([1, 3])
    with col_scan:
        scan_all = st.button("🔍 Scan All", type="primary", use_container_width=True)
    with col_sel:
        selected = st.multiselect(
            "Or choose specific tickers:",
            watchlist,
            default=watchlist,
            label_visibility="collapsed",
        )

    tickers_to_scan = watchlist if scan_all else selected

    if not tickers_to_scan:
        st.warning("Select at least one ticker to scan.")
        st.stop()

    if not (scan_all or st.button("🔍 Scan Selected", use_container_width=False)):
        st.stop()

    # ── Run analysis ──────────────────────────────────────────────────────────
    all_results: list[pd.DataFrame] = []
    all_news: dict[str, list] = {}
    all_earnings_edge: dict[str, dict] = {}
    errors: list[str] = []

    progress_bar = st.progress(0, text="Initializing…")

    for i, ticker in enumerate(tickers_to_scan):
        progress_bar.progress(i / len(tickers_to_scan), text=f"Scanning {ticker}…")
        # Refresh sentiment ahead of analysis; failure is silent/non-fatal.
        try:
            sentinel_scan_ticker(ticker)
        except Exception:
            pass
        df, news, err, earn_edge = analyze_ticker(ticker)
        if err:
            errors.append(f"**{ticker}**: {err}")
        else:
            all_results.append(df)
            if news:
                all_news[ticker] = news
            if earn_edge:
                all_earnings_edge[ticker] = earn_edge

    progress_bar.progress(1.0, text="Done.")

    for e in errors:
        st.warning(e)

    if not all_results:
        st.error("No actionable contracts found. Try different tickers or check warnings above.")
        st.stop()

    combined = pd.concat(all_results, ignore_index=True).sort_values("score", ascending=False)

    # ── Divergence alerts ─────────────────────────────────────────────────────
    has_div = "divergence_flag" in combined.columns
    if has_div:
        bear_divs = combined[combined["divergence_flag"] == "⚠️ BEAR DIV"]["symbol"].unique()
        bull_divs = combined[combined["divergence_flag"] == "📈 BULL DIV"]["symbol"].unique()
        if len(bear_divs):
            st.warning(f"⚠️ **Bearish divergence** detected: {', '.join(bear_divs)}")
        if len(bull_divs):
            st.success(f"📈 **Bullish divergence** detected: {', '.join(bull_divs)}")

    # ── Earnings edge alerts ──────────────────────────────────────────────────
    for sym, ee in all_earnings_edge.items():
        if ee["signal"] == "STRADDLE BUY":
            st.info(f"🎯 **{sym} earnings edge**: {ee['reason']} — {ee['days_to_earnings']}d to earnings")
        elif ee["signal"] == "IV RICH":
            st.warning(f"💰 **{sym} earnings IV rich**: {ee['reason']} — {ee['days_to_earnings']}d to earnings")

    # ── Summary bar ────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tickers", len(combined["symbol"].unique()))
    col2.metric("Contracts", len(combined))
    col3.metric("Buy Signals", int(combined["vol_signal"].isin(["BUY VOL", "FLOW BUY"]).sum()))
    col4.metric("Strong Flow", int((combined["flow_signal"] == "STRONG").sum()))
    st.caption("Score = vol mismatch (50) + flow (35) + DTE bonus (10) + IV rank (8) + skew (7) + GEX (±5) ± sentiment (±15/±20) ± RSI extremes (±5). OTM ≤15%. BUY VOL = IV 10%+ below RV. FLOW BUY = unusual activity + explosive GEX.")

    st.divider()

    # ── Per-ticker cards ───────────────────────────────────────────────────────
    def _render_contract(row, rank):
        sig      = row["vol_signal"]
        opt_type = row["type"].upper()
        c_icon   = "🟢" if sig == "BUY VOL" else "🔴"
        div_flag = row.get("divergence_flag", "—") if has_div else "—"
        sent_dlt = row.get("sentiment_delta", 0.0) if has_div else 0.0
        score_str = f"{row['score']}" + (f" ({sent_dlt:+.0f})" if sent_dlt != 0 else "")

        if sig == "BUY VOL":
            option_price = f"${row['entry_price']:.2f}" if row.get("entry_price") else f"${row['ask']:.2f} ask"
            price_label  = "Option price (mid)"
            detail_line  = f"Max loss / contract: **${row['max_loss_per_contract']:.0f}**" if row.get("max_loss_per_contract") else ""
        else:
            option_price = f"${row['net_credit']:.2f} credit" if row.get("net_credit") is not None else "—"
            price_label  = "Net credit"
            parts = []
            if row.get("max_profit") is not None:
                parts.append(f"Max profit: **${row['max_profit']:.0f}**")
            if row.get("max_loss_per_contract") is not None:
                parts.append(f"Max loss: **${row['max_loss_per_contract']:.0f}**")
            if row.get("breakeven") is not None:
                parts.append(f"BE: **${row['breakeven']:.2f}**")
            detail_line = "  |  ".join(parts)

        r1, r2, r3, r4, r5 = st.columns([1, 1, 1, 1, 2])
        r1.metric(f"#{rank} {c_icon} {opt_type}", f"${row['strike']:.0f} strike")
        r2.metric("Action", row["action"])
        r3.metric("Expiry", f"{row['expiry']} ({row['dte']}d)")
        r4.metric(price_label, option_price)
        with r5:
            st.caption(f"Score: **{score_str}**  |  Flow: **{row['flow_signal']}**")
            if detail_line:
                st.caption(detail_line)
            # Sizing recommendation
            n_ct  = row.get("suggested_contracts")
            r_dlr = row.get("suggested_risk_dollar")
            if n_ct and r_dlr:
                ct_label = f"{n_ct} contract{'s' if n_ct != 1 else ''}"
                trade_ok = score_str and float(row["score"]) >= RISK["min_score_to_trade"]
                if trade_ok:
                    st.caption(f"📐 Size: **{ct_label}** (~${r_dlr:.0f} at risk)")
                else:
                    st.caption(f"📐 Size: {ct_label} — ⚠️ score below auto-trade threshold")
            if div_flag != "—":
                if sent_dlt > 0:
                    st.success(f"{div_flag} — aligns ↑ {sent_dlt:+.0f} pts", icon=None)
                else:
                    st.warning(f"{div_flag} — contradicts ↓ {sent_dlt:+.0f} pts", icon=None)
            # RSI entry-timing note — only surface when RSI actually moved the score
            rsi_dlt = row.get("rsi_delta", 0.0) or 0.0
            rsi_zn  = row.get("rsi_zone")
            rsi_vl  = row.get("rsi_14")
            if rsi_dlt and rsi_zn in ("oversold", "overbought"):
                rsi_msg = f"RSI {rsi_vl:.1f} ({rsi_zn})"
                if rsi_dlt > 0:
                    st.success(f"📊 {rsi_msg} — favors entry {rsi_dlt:+.0f} pts", icon=None)
                else:
                    st.warning(f"📊 {rsi_msg} — fights entry {rsi_dlt:+.0f} pts", icon=None)

    for symbol in combined["symbol"].unique():
        tkr    = combined[combined["symbol"] == symbol]
        name   = tkr["company_name"].iloc[0]
        px     = tkr["stock_price"].iloc[0]
        top_iv = tkr["iv_rv_spread"].iloc[0]
        top_sg = tkr["vol_signal"].iloc[0]
        hdr_icon = "🟢" if top_sg == "BUY VOL" else ("⚡" if top_sg == "FLOW BUY" else ("🔴" if top_sg == "SELL VOL" else "🟡"))

        st.markdown(f"## {hdr_icon} {symbol} &nbsp;·&nbsp; {name}")
        st.caption(
            f"Stock: **${px:.2f}** &nbsp;·&nbsp; "
            f"IV vs 30d RV: **{top_iv:+.1f}%** &nbsp;·&nbsp; "
            f"Signal: **{top_sg}** &nbsp;·&nbsp; "
            f"Earnings: **{tkr['earnings'].iloc[0]}**"
        )

        # ── Surface signals row ───────────────────────────────────────────────
        first = tkr.iloc[0]
        ivr_label   = first.get("iv_rank_label", "N/A")
        skew_sig    = first.get("skew_signal", "—")
        skew_summ   = first.get("skew_summary", "—")
        gex_sig     = first.get("gex_signal", "—")
        gex_summ    = first.get("gex_summary", "—")
        gamma_wall  = first.get("gamma_wall")
        gamma_flip  = first.get("gamma_flip")
        rsi_val     = first.get("rsi_14")
        rsi_zn      = first.get("rsi_zone") or "unknown"

        sc1, sc2, sc3, sc4 = st.columns(4)
        with sc1:
            ivr_icon = "🔴" if "HIGH" in ivr_label else ("🟢" if "LOW" in ivr_label else "⚪")
            st.caption(f"**IV Rank:** {ivr_icon} {ivr_label}")
        with sc2:
            sk_icon = "🟢" if skew_sig == "BULLISH" else ("🔴" if skew_sig == "BEARISH" else "⚪")
            st.caption(f"**Skew:** {sk_icon} {skew_summ}")
        with sc3:
            gx_icon = "⚡" if gex_sig == "EXPLOSIVE" else ("🧲" if gex_sig == "PINNED" else "🛡️")
            gex_detail = gex_summ
            if gamma_wall:
                gex_detail += f"  ·  wall ${gamma_wall:.0f}"
            if gamma_flip:
                gex_detail += f"  ·  flip ${gamma_flip:.0f}"
            st.caption(f"**GEX:** {gx_icon} {gex_detail}")
        with sc4:
            rsi_icon = "🟢" if rsi_zn == "oversold" else ("🔴" if rsi_zn == "overbought" else "⚪")
            rsi_txt  = f"{rsi_val:.1f} — {rsi_zn}" if rsi_val is not None else "N/A"
            st.caption(f"**RSI 14:** {rsi_icon} {rsi_txt}")

        # ── Earnings edge banner ──────────────────────────────────────────────
        ee = all_earnings_edge.get(symbol)
        if ee:
            ee_cols = st.columns([3, 1])
            with ee_cols[0]:
                if ee["signal"] == "STRADDLE BUY":
                    st.success(
                        f"🎯 **Earnings edge** ({ee['days_to_earnings']}d): {ee['reason']}  "
                        f"|  Historical moves: {', '.join(f'{m:.1f}%' for m in ee['historical_moves'][:4])}",
                        icon=None,
                    )
                elif ee["signal"] == "IV RICH":
                    st.warning(
                        f"💰 **Earnings IV rich** ({ee['days_to_earnings']}d): {ee['reason']}  "
                        f"|  Historical moves: {', '.join(f'{m:.1f}%' for m in ee['historical_moves'][:4])}",
                        icon=None,
                    )
                else:
                    st.caption(
                        f"📅 Earnings in {ee['days_to_earnings']}d: {ee['reason']}"
                    )

        # ── News dropdown ─────────────────────────────────────────────────────
        articles = all_news.get(symbol, [])
        src   = articles[0].get("source", "rss") if articles else "rss"
        badge = "🔗 sentinel" if src not in ("rss",) else "📰 RSS"
        with st.expander(f"📰 News — {len(articles)} item(s)  {badge}" if articles else "📰 News — none found"):
            if not articles:
                st.caption("No recent news found.")
            for a in articles:
                pub = a["published"] if isinstance(a["published"], str) else (
                    a["published"].strftime("%b %d  %H:%M UTC") if a["published"] else "—"
                )
                sent = a.get("sentiment")
                sent_str = (f" 🟢 {sent:+.2f}" if sent > 0.2 else (f" 🔴 {sent:+.2f}" if sent < -0.2 else f" ⚪ {sent:+.2f}")) if sent is not None else ""
                link  = a.get("link", "")
                title = a["title"]
                st.markdown(f"**[{title}]({link})** &nbsp; `{pub}`{sent_str}" if link else f"**{title}** &nbsp; `{pub}`{sent_str}")
                if a.get("summary"):
                    st.caption(a["summary"])

        # ── Contracts split by strategy ───────────────────────────────────────
        buys     = tkr[tkr["vol_signal"] == "BUY VOL"]
        flow_buys = tkr[tkr["vol_signal"] == "FLOW BUY"]
        spreads  = tkr[tkr["vol_signal"] == "SELL VOL"]
        watches  = tkr[tkr["vol_signal"] == "NEUTRAL"]

        if not buys.empty:
            st.markdown("**🟢 Buy Options** *(IV cheap vs realized vol)*")
            for rank, (_, row) in enumerate(buys.iterrows(), start=1):
                _render_contract(row, rank)

        if not flow_buys.empty:
            st.markdown("**⚡ Flow Buy** *(unusual institutional activity — smart money positioning)*")
            for rank, (_, row) in enumerate(flow_buys.iterrows(), start=1):
                _render_contract(row, rank)

        if not spreads.empty:
            st.markdown("**🔴 Credit Spreads** *(IV rich — sell the expensive vol, buy protection)*")
            for rank, (_, row) in enumerate(spreads.iterrows(), start=1):
                _render_contract(row, rank)

        if not watches.empty and buys.empty and spreads.empty and flow_buys.empty:
            st.markdown("**🟡 Watch Only**")
            for rank, (_, row) in enumerate(watches.iterrows(), start=1):
                _render_contract(row, rank)

        st.divider()
