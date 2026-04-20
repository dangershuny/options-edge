import json
import os
import streamlit as st
import pandas as pd

from analysis.scorer import analyze_ticker
from analysis.discover import run_discovery
from data.news import news_tool_status

WATCHLIST_FILE = "watchlist.json"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Options Edge Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .signal-buy  { color: #00e676; font-weight: bold; }
  .signal-sell { color: #ff5252; font-weight: bold; }
  .signal-watch{ color: #ffab40; }
  .flow-strong { color: #ffd740; font-weight: bold; }
  .flow-elev   { color: #ffe082; }
  .score-high  { color: #00e676; font-weight: bold; }
  .score-mid   { color: #ffab40; }
</style>
""", unsafe_allow_html=True)


# ── Watchlist helpers ─────────────────────────────────────────────────────────
def load_watchlist() -> list[str]:
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


def save_watchlist(tickers: list[str]) -> None:
    with open(WATCHLIST_FILE, "w") as f:
        json.dump(tickers, f)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 Options Edge")
    st.caption("IV vs RV · Unusual Flow · No naked exposure")
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
    st.caption("**Signal guide**")
    st.caption("🟢 BUY CALL/PUT — IV cheap vs RV, buy the move")
    st.caption("🔴 SPREAD — IV rich, consider credit spread")
    st.caption("🟡 WATCH — No strong vol mismatch yet")
    st.caption("⚡ STRONG flow — Vol/OI ≥ 1×")
    st.caption("⚠️ Earnings-adjacent expiries are excluded automatically")
    st.divider()
    st.caption(f"📡 News: **{news_tool_status()}**")


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("Options Edge Scanner")

tab_watchlist, tab_discover = st.tabs(["📋 Watchlist", "🔭 Discover"])


# ═══════════════════════════════════════════════════════════════════════════════
# DISCOVER TAB — autonomous universe scan
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

            show_cols = ["symbol", "price", "iv_pct", "rv_pct", "iv_rv_spread", "vol_signal"]
            labels = {"symbol": "Ticker", "price": "Price", "iv_pct": "IV %",
                      "rv_pct": "RV %", "iv_rv_spread": "IV−RV", "vol_signal": "Signal"}
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
# WATCHLIST TAB — targeted scan on user's list
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
    errors: list[str] = []

    progress_bar = st.progress(0, text="Initializing…")

    for i, ticker in enumerate(tickers_to_scan):
        progress_bar.progress((i) / len(tickers_to_scan), text=f"Scanning {ticker}…")
        df, news, err = analyze_ticker(ticker)
        if err:
            errors.append(f"**{ticker}**: {err}")
        else:
            all_results.append(df)
            if news:
                all_news[ticker] = news

    progress_bar.progress(1.0, text="Done.")

    for e in errors:
        st.warning(e)

    if not all_results:
        st.error("No actionable contracts found. Try different tickers or check warnings above.")
        st.stop()

    combined = pd.concat(all_results, ignore_index=True).sort_values("score", ascending=False)

    # ── Summary bar ────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tickers", len(combined["symbol"].unique()))
    col2.metric("Contracts", len(combined))
    col3.metric("Buy Signals", int((combined["vol_signal"] == "BUY VOL").sum()))
    col4.metric("Strong Flow", int((combined["flow_signal"] == "STRONG").sum()))

    st.divider()

    # ── Per-ticker cards ───────────────────────────────────────────────────────
    for symbol in combined["symbol"].unique():
        tkr_rows = combined[combined["symbol"] == symbol]
        company  = tkr_rows["company_name"].iloc[0]
        stock_px = tkr_rows["stock_price"].iloc[0]
        top_sig  = tkr_rows["vol_signal"].iloc[0]
        icon     = "🟢" if top_sig == "BUY VOL" else ("🔴" if top_sig == "SELL VOL" else "🟡")

        st.markdown(f"## {icon} {symbol} &nbsp;·&nbsp; {company}")
        st.caption(f"Stock price: **${stock_px:.2f}** &nbsp;|&nbsp; IV vs 30d RV: **{tkr_rows['iv_rv_spread'].iloc[0]:+.1f}%** &nbsp;|&nbsp; Signal: **{top_sig}**")

        # ── News dropdown ──────────────────────────────────────────────────────
        articles = all_news.get(symbol, [])
        src   = articles[0].get("source", "rss") if articles else "rss"
        badge = "🔗 news-tool" if src == "news-tool" else "📰 RSS"
        news_label = f"📰 News — {len(articles)} article(s)  {badge}" if articles else "📰 News — none found"
        with st.expander(news_label):
            if not articles:
                st.caption("No recent news found for this ticker.")
            for a in articles:
                pub = a["published"].strftime("%b %d  %H:%M UTC") if a["published"] else "—"
                sent = a.get("sentiment")
                if sent is not None:
                    sent_str = f" 🟢 {sent:+.2f}" if sent > 0.2 else (f" 🔴 {sent:+.2f}" if sent < -0.2 else f" ⚪ {sent:+.2f}")
                else:
                    sent_str = ""
                st.markdown(f"**[{a['title']}]({a['link']})** &nbsp; `{pub}`{sent_str}")
                if a["summary"]:
                    st.caption(a["summary"])

        # ── Contracts grouped by strategy type ────────────────────────────────
        def _render_contract(row, rank):
            sig        = row["vol_signal"]
            opt_type   = row["type"].upper()          # CALL or PUT
            action     = row["action"]
            c_icon     = "🟢" if sig == "BUY VOL" else "🔴"

            if sig == "BUY VOL":
                option_price = f"${row['entry_price']:.2f}" if row.get("entry_price") else f"${row['ask']:.2f} ask"
                price_label  = "Option price (mid)"
                detail_line  = f"Max loss / contract: **${row['max_loss_per_contract']:.0f}**" if row.get("max_loss_per_contract") else ""
            else:
                option_price = f"${row['net_credit']:.2f} credit" if row.get("net_credit") is not None else "—"
                price_label  = "Net credit received"
                detail_line  = ""
                if row.get("max_profit") is not None:
                    detail_line = f"Max profit: **${row['max_profit']:.0f}**"
                if row.get("breakeven") is not None:
                    detail_line += f"  |  Max loss: **${row['max_loss_per_contract']:.0f}**  |  BE: **${row['breakeven']:.2f}**"

            r1, r2, r3, r4, r5 = st.columns([1, 1, 1, 1, 2])
            r1.metric(f"#{rank} {c_icon} {opt_type}", f"${row['strike']:.0f} strike")
            r2.metric("Action", action)
            r3.metric("Expiry", f"{row['expiry']} ({row['dte']}d)")
            r4.metric(price_label, option_price)
            with r5:
                if detail_line:
                    st.caption(detail_line)
                st.caption(row.get("trade_detail") or "")

        buys    = tkr_rows[tkr_rows["vol_signal"] == "BUY VOL"]
        spreads = tkr_rows[tkr_rows["vol_signal"] == "SELL VOL"]
        watches = tkr_rows[tkr_rows["vol_signal"] == "NEUTRAL"]

        if not buys.empty:
            st.markdown("**🟢 Buy Options**")
            for rank, (_, row) in enumerate(buys.iterrows(), start=1):
                _render_contract(row, rank)

        if not spreads.empty:
            st.markdown("**🔴 Credit Spreads** *(defined risk — sell the expensive vol)*")
            for rank, (_, row) in enumerate(spreads.iterrows(), start=1):
                _render_contract(row, rank)

        if not watches.empty and buys.empty and spreads.empty:
            st.markdown("**🟡 Watch Only**")
            for rank, (_, row) in enumerate(watches.iterrows(), start=1):
                _render_contract(row, rank)

        st.divider()
