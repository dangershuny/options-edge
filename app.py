import json
import os
import streamlit as st
import pandas as pd

from analysis.scorer import analyze_ticker
from analysis.discover import run_discovery
from data.news import news_tool_status
from sentinel_bridge import sentinel_status

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
    st.caption("IV vs RV · Unusual Flow · Sentiment Divergence")
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
    st.caption("⚠️ BEAR DIV — Market bullish, news/social bearish")
    st.caption("📈 BULL DIV — Market bearish, news/social bullish")
    st.caption("Score adjusts ±15 pts on divergence alignment")
    st.caption("⚠️ Earnings-adjacent expiries excluded automatically")
    st.divider()
    st.caption(f"📡 News tool: **{news_tool_status()}**")
    st.caption(f"📡 Sentiment: **{sentinel_status()}**")


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("Options Edge Scanner")

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
    errors: list[str] = []

    progress_bar = st.progress(0, text="Initializing…")

    for i, ticker in enumerate(tickers_to_scan):
        progress_bar.progress(i / len(tickers_to_scan), text=f"Scanning {ticker}…")
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

    # ── Divergence alerts ─────────────────────────────────────────────────────
    if "divergence_flag" in combined.columns:
        bear_divs = combined[combined["divergence_flag"] == "⚠️ BEAR DIV"]["symbol"].unique()
        bull_divs = combined[combined["divergence_flag"] == "📈 BULL DIV"]["symbol"].unique()
        if len(bear_divs):
            st.warning(f"⚠️ **Bearish divergence** — market bullish, news/social bearish: {', '.join(bear_divs)}")
        if len(bull_divs):
            st.success(f"📈 **Bullish divergence** — market bearish, news/social bullish: {', '.join(bull_divs)}")

    # ── Results header ────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Contracts Found", len(combined))
    col2.metric("Buy Signals", int((combined["vol_signal"] == "BUY VOL").sum()))
    col3.metric("Strong Flow", int((combined["flow_signal"] == "STRONG").sum()))
    col4.metric("Tickers Scanned", len(all_results))

    st.divider()

    # ── Filter controls ───────────────────────────────────────────────────────
    st.subheader("Results")
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        min_score = st.slider("Min score", 0, 100, 0, 5)
    with fcol2:
        sig_filter = st.multiselect(
            "Vol signal",
            ["BUY VOL", "SELL VOL", "NEUTRAL"],
            default=["BUY VOL", "SELL VOL", "NEUTRAL"],
        )
    with fcol3:
        type_filter = st.multiselect("Type", ["call", "put"], default=["call", "put"])

    filtered = combined[
        (combined["score"] >= min_score) &
        (combined["vol_signal"].isin(sig_filter)) &
        (combined["type"].isin(type_filter))
    ]

    if filtered.empty:
        st.info("No contracts match the current filters.")
    else:
        has_divergence = "divergence_flag" in filtered.columns

        summary_cols = [
            "symbol", "type", "strike", "expiry", "dte", "stock_price",
            "iv_pct", "rv_pct", "iv_rv_spread",
            "vol_signal", "action",
            "volume", "open_interest", "vol_oi_ratio", "flow_signal",
            "score",
        ]
        col_labels = {
            "symbol": "Ticker", "type": "Type", "strike": "Strike",
            "expiry": "Expiry", "dte": "DTE", "stock_price": "Stock $",
            "iv_pct": "IV %", "rv_pct": "RV %", "iv_rv_spread": "IV−RV",
            "vol_signal": "Vol Signal", "action": "Action",
            "volume": "Vol", "open_interest": "OI", "vol_oi_ratio": "Vol/OI",
            "flow_signal": "Flow", "score": "Score",
            "sentiment_delta": "Sent Δ", "divergence_flag": "Divergence",
            "earnings": "Earnings",
        }
        if has_divergence:
            summary_cols += ["sentiment_delta", "divergence_flag"]
        summary_cols.append("earnings")

        def color_row(row):
            styles = [""] * len(row)
            col_list = list(row.index)
            rules = [
                ("Score",      lambda v: "color: #00e676; font-weight: bold" if v >= 70 else ("color: #ffab40" if v >= 40 else "")),
                ("Vol Signal", lambda v: "color: #00e676; font-weight: bold" if v == "BUY VOL" else ("color: #ff5252; font-weight: bold" if v == "SELL VOL" else "")),
                ("Flow",       lambda v: "color: #ffd740; font-weight: bold" if v == "STRONG" else ("color: #ffe082" if v == "ELEVATED" else "")),
                ("Action",     lambda v: "color: #00e676; font-weight: bold" if str(v).startswith("BUY") else ("color: #ff7043" if str(v).startswith("SPREAD") else "")),
            ]
            if has_divergence:
                rules += [
                    ("Divergence", lambda v: "color: #ff5252; font-weight: bold" if "BEAR" in str(v) else ("color: #00e676; font-weight: bold" if "BULL" in str(v) else "")),
                    ("Sent Δ",     lambda v: "color: #00e676" if v > 0 else ("color: #ff5252" if v < 0 else "")),
                ]
            for field, fn in rules:
                if field in col_list:
                    styles[col_list.index(field)] = fn(row[field])
            return styles

        display_df = filtered[summary_cols].rename(columns=col_labels)
        st.dataframe(display_df.style.apply(color_row, axis=1), use_container_width=True, height=420, hide_index=True)
        st.caption(
            "**Score** = vol mismatch (50) + flow (35) + DTE bonus (10) ± sentiment divergence (±15 when sentinel running). "
            "Earnings-adjacent expiries excluded. Max OTM: 10%."
        )

        # ── Trade recommendations ─────────────────────────────────────────────
        st.divider()
        st.subheader("Trade Recommendations")
        st.caption("Click any row below to expand the full trade detail.")

        actionable = filtered[filtered["vol_signal"] != "NEUTRAL"].sort_values("score", ascending=False)

        if actionable.empty:
            st.info("No actionable signals — all contracts are NEUTRAL.")
        else:
            for _, row in actionable.iterrows():
                detail = row.get("trade_detail") or "—"
                sig = row["vol_signal"]
                score = row["score"]
                div_flag = row.get("divergence_flag", "—") if has_divergence else "—"
                sent_delta = row.get("sentiment_delta", 0.0) if has_divergence else 0.0

                icon = "🟢" if sig == "BUY VOL" else ("🔴" if sig == "SELL VOL" else "🟡")
                div_label = f" &nbsp;|&nbsp; {div_flag}" if div_flag != "—" else ""
                delta_label = f" ({sent_delta:+.0f})" if sent_delta != 0 else ""

                header = (
                    f"{icon} **{row['symbol']}** &nbsp;|&nbsp; "
                    f"{row['action']} &nbsp;|&nbsp; "
                    f"Score: **{score}**{delta_label}{div_label} &nbsp;|&nbsp; "
                    f"Expiry: {row['expiry']} ({row['dte']} DTE)"
                )
                with st.expander(header):
                    st.markdown(f"#### `{detail}`")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Stock Price", f"${row['stock_price']:.2f}")
                    c2.metric("IV", f"{row['iv_pct']}%")
                    c3.metric("30d RV", f"{row['rv_pct']}%")
                    c4.metric("IV − RV", f"{row['iv_rv_spread']:+.1f}%")

                    c5, c6, c7, c8 = st.columns(4)
                    if sig == "BUY VOL":
                        c5.metric("Entry (midpoint)", f"${row['entry_price']:.2f}" if row.get("entry_price") else "—")
                        c6.metric("Max Loss / contract", f"${row['max_loss_per_contract']:.0f}" if row.get("max_loss_per_contract") else "—")
                        c7.metric("Volume", f"{int(row['volume']):,}")
                        c8.metric("Flow", row["flow_signal"])
                    else:
                        c5.metric("Net Credit", f"${row['net_credit']:.2f}" if row.get("net_credit") is not None else "—")
                        c6.metric("Max Profit / contract", f"${row['max_profit']:.0f}" if row.get("max_profit") is not None else "—")
                        c7.metric("Max Loss / contract", f"${row['max_loss_per_contract']:.0f}" if row.get("max_loss_per_contract") is not None else "—")
                        c8.metric("Breakeven", f"${row['breakeven']:.2f}" if row.get("breakeven") is not None else "—")

                    if div_flag != "—" and sent_delta != 0:
                        if sent_delta > 0:
                            st.success(f"{div_flag} — Sentiment aligns with signal. Score boosted {sent_delta:+.0f} pts.")
                        else:
                            st.warning(f"{div_flag} — Sentiment contradicts signal. Score penalized {sent_delta:+.0f} pts.")

    # ── News & Social ─────────────────────────────────────────────────────────
    if all_news:
        st.divider()
        st.subheader("Recent News & Social")
        for ticker, articles in all_news.items():
            if not articles:
                continue
            src = articles[0].get("source", "rss") if articles else "rss"
            badge = "🔗 sentinel" if src not in ("rss",) else "📰 RSS"
            with st.expander(f"{ticker} — {len(articles)} item(s)  {badge}"):
                for a in articles:
                    pub = a["published"] if isinstance(a["published"], str) else (
                        a["published"].strftime("%b %d, %Y %H:%M UTC") if a["published"] else "Unknown"
                    )
                    sent = a.get("sentiment")
                    if sent is not None:
                        sent_str = f" 🟢 {sent:+.2f}" if sent > 0.2 else (f" 🔴 {sent:+.2f}" if sent < -0.2 else f" ⚪ {sent:+.2f}")
                    else:
                        sent_str = ""
                    link = a.get("link", "")
                    title = a["title"]
                    if link:
                        st.markdown(f"**[{title}]({link})** &nbsp; `{pub}`{sent_str}")
                    else:
                        st.markdown(f"**{title}** &nbsp; `{pub}`{sent_str}")
                    if a.get("summary"):
                        st.caption(a["summary"])
                    st.divider()
