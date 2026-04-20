import json
import os
import streamlit as st
import pandas as pd

from analysis.scorer import analyze_ticker
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

if not watchlist:
    st.info("Add at least one ticker in the sidebar to begin.")
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

# ── Run analysis ──────────────────────────────────────────────────────────────
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
    st.error("No actionable contracts found across all tickers. Try different tickers or check the warnings above.")
    st.stop()

combined = pd.concat(all_results, ignore_index=True).sort_values("score", ascending=False)

# ── Results header ────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Contracts Found", len(combined))
col2.metric("Buy Signals", int((combined["vol_signal"] == "BUY VOL").sum()))
col3.metric("Strong Flow", int((combined["flow_signal"] == "STRONG").sum()))
col4.metric("Tickers Scanned", len(all_results))

st.divider()

# ── Filter controls ───────────────────────────────────────────────────────────
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
    # ── Summary table (scannable) ─────────────────────────────────────────────
    summary_cols = [
        "symbol", "type", "strike", "expiry", "dte", "stock_price",
        "iv_pct", "rv_pct", "iv_rv_spread",
        "vol_signal", "action",
        "volume", "open_interest", "vol_oi_ratio", "flow_signal",
        "score", "earnings",
    ]
    col_labels = {
        "symbol": "Ticker", "type": "Type", "strike": "Strike",
        "expiry": "Expiry", "dte": "DTE", "stock_price": "Stock $",
        "iv_pct": "IV %", "rv_pct": "RV %", "iv_rv_spread": "IV−RV",
        "vol_signal": "Vol Signal", "action": "Action",
        "volume": "Vol", "open_interest": "OI", "vol_oi_ratio": "Vol/OI",
        "flow_signal": "Flow", "score": "Score", "earnings": "Earnings",
    }

    def color_row(row):
        styles = [""] * len(row)
        col_list = list(row.index)
        for field, rules in [
            ("score",      lambda v: "color: #00e676; font-weight: bold" if v >= 70 else ("color: #ffab40" if v >= 40 else "")),
            ("vol_signal", lambda v: "color: #00e676; font-weight: bold" if v == "BUY VOL" else ("color: #ff5252; font-weight: bold" if v == "SELL VOL" else "")),
            ("flow_signal",lambda v: "color: #ffd740; font-weight: bold" if v == "STRONG" else ("color: #ffe082" if v == "ELEVATED" else "")),
            ("action",     lambda v: "color: #00e676; font-weight: bold" if str(v).startswith("BUY") else ("color: #ff7043" if str(v).startswith("SPREAD") else "")),
        ]:
            if field in col_list:
                styles[col_list.index(field)] = rules(row[field])
        return styles

    display_df = filtered[summary_cols].rename(columns=col_labels)
    styled = display_df.style.apply(color_row, axis=1)
    st.dataframe(styled, use_container_width=True, height=420, hide_index=True)

    st.caption(
        "**Score** = vol mismatch (up to 50) + unusual flow (up to 35) + DTE sweet-spot bonus (up to 10). "
        "Earnings-adjacent expiries excluded. Max OTM: 10%."
    )

    # ── Trade recommendations ─────────────────────────────────────────────────
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

            if sig == "BUY VOL":
                icon = "🟢"
                color = "#00e676"
            elif sig == "SELL VOL":
                icon = "🔴"
                color = "#ff7043"
            else:
                icon = "🟡"
                color = "#ffab40"

            header = (
                f"{icon} **{row['symbol']}** &nbsp;|&nbsp; "
                f"{row['action']} &nbsp;|&nbsp; "
                f"Score: **{score}** &nbsp;|&nbsp; "
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
                    c5.metric("Entry (midpoint)", f"${row['entry_price']:.2f}" if row.get('entry_price') else "—")
                    c6.metric("Max Loss / contract", f"${row['max_loss_per_contract']:.0f}" if row.get('max_loss_per_contract') else "—")
                    c7.metric("Volume", f"{int(row['volume']):,}")
                    c8.metric("Flow", row["flow_signal"])
                else:
                    c5.metric("Net Credit", f"${row['net_credit']:.2f}" if row.get('net_credit') is not None else "—")
                    c6.metric("Max Profit / contract", f"${row['max_profit']:.0f}" if row.get('max_profit') is not None else "—")
                    c7.metric("Max Loss / contract", f"${row['max_loss_per_contract']:.0f}" if row.get('max_loss_per_contract') is not None else "—")
                    c8.metric("Breakeven", f"${row['breakeven']:.2f}" if row.get('breakeven') is not None else "—")

# ── News ──────────────────────────────────────────────────────────────────────
if all_news:
    st.divider()
    st.subheader("Recent News")
    for ticker, articles in all_news.items():
        if not articles:
            continue
        src = articles[0].get("source", "rss") if articles else "rss"
        badge = "🔗 news-tool" if src == "news-tool" else "📰 RSS"
        with st.expander(f"{ticker} — {len(articles)} article(s)  {badge}"):
            for a in articles:
                pub = a["published"].strftime("%b %d, %Y %H:%M UTC") if a["published"] else "Unknown date"
                sent = a.get("sentiment")
                if sent is not None:
                    if sent > 0.2:
                        sent_str = f" 🟢 {sent:+.2f}"
                    elif sent < -0.2:
                        sent_str = f" 🔴 {sent:+.2f}"
                    else:
                        sent_str = f" ⚪ {sent:+.2f}"
                else:
                    sent_str = ""
                st.markdown(f"**[{a['title']}]({a['link']})** &nbsp; `{pub}`{sent_str}")
                if a["summary"]:
                    st.caption(a["summary"])
                st.divider()
