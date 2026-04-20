import json
import os
import streamlit as st
import pandas as pd

from analysis.scorer import analyze_ticker

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
  .div-bear    { color: #ff5252; font-weight: bold; }
  .div-bull    { color: #00e676; font-weight: bold; }
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
    st.caption("Score adjusts ±15 pts when divergence aligns/contradicts signal")
    st.caption("⚠️ Earnings-adjacent expiries are excluded automatically")


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

# ── Divergence alerts ─────────────────────────────────────────────────────────
bear_divs = combined[combined["divergence_flag"] == "⚠️ BEAR DIV"]["symbol"].unique()
bull_divs = combined[combined["divergence_flag"] == "📈 BULL DIV"]["symbol"].unique()
if len(bear_divs):
    st.warning(f"⚠️ **Bearish divergence detected** — market bullish but news/social bearish: {', '.join(bear_divs)}")
if len(bull_divs):
    st.success(f"📈 **Bullish divergence detected** — market bearish but news/social bullish: {', '.join(bull_divs)}")

# ── Results header ────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Contracts Found", len(combined))
col2.metric("Buy Signals", int((combined["vol_signal"] == "BUY VOL").sum()))
col3.metric("Strong Flow", int((combined["flow_signal"] == "STRONG").sum()))
col4.metric("Tickers Scanned", len(all_results))

st.divider()

# ── Filter controls ───────────────────────────────────────────────────────────
st.subheader("Results")
fcol1, fcol2, fcol3, fcol4 = st.columns(4)
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
with fcol4:
    div_filter = st.multiselect(
        "Divergence",
        ["⚠️ BEAR DIV", "📈 BULL DIV", "—"],
        default=["⚠️ BEAR DIV", "📈 BULL DIV", "—"],
    )

filtered = combined[
    (combined["score"] >= min_score) &
    (combined["vol_signal"].isin(sig_filter)) &
    (combined["type"].isin(type_filter)) &
    (combined["divergence_flag"].isin(div_filter))
]

if filtered.empty:
    st.info("No contracts match the current filters.")
else:
    # ── Summary table ─────────────────────────────────────────────────────────
    summary_cols = [
        "symbol", "type", "strike", "expiry", "dte", "stock_price",
        "iv_pct", "rv_pct", "iv_rv_spread",
        "vol_signal", "action",
        "volume", "open_interest", "vol_oi_ratio", "flow_signal",
        "score", "sentiment_delta", "divergence_flag", "earnings",
    ]
    col_labels = {
        "symbol": "Ticker", "type": "Type", "strike": "Strike",
        "expiry": "Expiry", "dte": "DTE", "stock_price": "Stock $",
        "iv_pct": "IV %", "rv_pct": "RV %", "iv_rv_spread": "IV−RV",
        "vol_signal": "Vol Signal", "action": "Action",
        "volume": "Vol", "open_interest": "OI", "vol_oi_ratio": "Vol/OI",
        "flow_signal": "Flow", "score": "Score", "sentiment_delta": "Sent Δ",
        "divergence_flag": "Divergence", "earnings": "Earnings",
    }

    def color_row(row):
        styles = [""] * len(row)
        col_list = list(row.index)
        for field, rules in [
            ("Score",      lambda v: "color: #00e676; font-weight: bold" if v >= 70 else ("color: #ffab40" if v >= 40 else "")),
            ("Vol Signal", lambda v: "color: #00e676; font-weight: bold" if v == "BUY VOL" else ("color: #ff5252; font-weight: bold" if v == "SELL VOL" else "")),
            ("Flow",       lambda v: "color: #ffd740; font-weight: bold" if v == "STRONG" else ("color: #ffe082" if v == "ELEVATED" else "")),
            ("Action",     lambda v: "color: #00e676; font-weight: bold" if str(v).startswith("BUY") else ("color: #ff7043" if str(v).startswith("SPREAD") else "")),
            ("Divergence", lambda v: "color: #ff5252; font-weight: bold" if "BEAR" in str(v) else ("color: #00e676; font-weight: bold" if "BULL" in str(v) else "")),
            ("Sent Δ",     lambda v: "color: #00e676" if v > 0 else ("color: #ff5252" if v < 0 else "")),
        ]:
            if field in col_list:
                styles[col_list.index(field)] = rules(row[field])
        return styles

    display_df = filtered[summary_cols].rename(columns=col_labels)
    styled = display_df.style.apply(color_row, axis=1)
    st.dataframe(styled, use_container_width=True, height=420, hide_index=True)

    st.caption(
        "**Score** = vol mismatch (50) + flow (35) + DTE bonus (10) ± sentiment divergence (±15). "
        "**Sent Δ** shows the sentiment adjustment applied. Max OTM: 10%. Earnings-adjacent excluded."
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
            div_flag = row.get("divergence_flag", "—")
            sent_delta = row.get("sentiment_delta", 0.0)

            if sig == "BUY VOL":
                icon = "🟢"
            elif sig == "SELL VOL":
                icon = "🔴"
            else:
                icon = "🟡"

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
                    c5.metric("Entry (midpoint)", f"${row['entry_price']:.2f}" if row.get('entry_price') else "—")
                    c6.metric("Max Loss / contract", f"${row['max_loss_per_contract']:.0f}" if row.get('max_loss_per_contract') else "—")
                    c7.metric("Volume", f"{int(row['volume']):,}")
                    c8.metric("Flow", row["flow_signal"])
                else:
                    c5.metric("Net Credit", f"${row['net_credit']:.2f}" if row.get('net_credit') is not None else "—")
                    c6.metric("Max Profit / contract", f"${row['max_profit']:.0f}" if row.get('max_profit') is not None else "—")
                    c7.metric("Max Loss / contract", f"${row['max_loss_per_contract']:.0f}" if row.get('max_loss_per_contract') is not None else "—")
                    c8.metric("Breakeven", f"${row['breakeven']:.2f}" if row.get('breakeven') is not None else "—")

                if div_flag != "—" and sent_delta != 0:
                    if sent_delta > 0:
                        st.success(f"{div_flag} — Sentiment aligns with signal. Score boosted by {sent_delta:+.0f} pts.")
                    else:
                        st.warning(f"{div_flag} — Sentiment contradicts signal. Score penalized by {sent_delta:+.0f} pts.")

# ── News & Social ─────────────────────────────────────────────────────────────
if all_news:
    st.divider()
    st.subheader("News & Social Sentiment")
    for ticker, articles in all_news.items():
        if not articles:
            continue
        with st.expander(f"{ticker} — {len(articles)} item(s)"):
            for a in articles:
                pub = a["published"] if a["published"] else "Unknown date"
                label = a.get("sentiment_label", "neutral").upper()
                score = a.get("sentiment_score", 0.0)

                if label == "BULLISH":
                    badge = "🟢"
                elif label == "BEARISH":
                    badge = "🔴"
                else:
                    badge = "⚪"

                title = a["title"]
                link = a.get("link", "")
                if link:
                    st.markdown(f"{badge} **[{title}]({link})** &nbsp; `{score:+.3f}` &nbsp; `{pub}`")
                else:
                    st.markdown(f"{badge} **{title}** &nbsp; `{score:+.3f}` &nbsp; `{pub}`")
                if a.get("summary"):
                    st.caption(a["summary"])
                st.divider()
