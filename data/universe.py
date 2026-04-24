"""
Curated universe of US equities for options scanning.

Selection thesis: edge is in names where retail/crowd flow dominates IV
pricing, not analyst-driven institutional flow. Two paths qualify:

  1. Small/mid-cap (<$10B market cap) — natural edge zone with thinner
     analyst coverage and slower news propagation.

  2. Large/mega-cap that BEHAVES like a small-cap because of retail/WSB
     speculation flow — meme stocks (GME, AMC, RDDT), crypto plays
     (COIN, MSTR, MARA), narrative-driven (PLTR, TSLA, NVDA, RKLB),
     and pure-speculation tickers (DJT, DKNG). These names are large
     by market cap but their IV is set by retail crowd dynamics, not
     by HFT-tightened institutional pricing.

Excluded: large-caps with deep analyst coverage AND no retail crowd
flow — INTC, ROKU, CHWY, ETSY, MRNA, FSLR, KTOS, etc. Too efficient.

ETFs are intentionally excluded:
  • No idiosyncratic catalysts (no 8-Ks, insiders, earnings)
  • Sentinel divergence/sentiment signals don't apply
  • Options markets are already hyper-efficient (institutional)
  • Macro/VIX regime is handled separately by data/macro.py

Inclusion rules:
  • Active options chain (≥5 expiries on yfinance verified)
  • Market cap ≥ $400M (chains thin out below that)
  • Live ticker (not renamed/delisted)

Total: ~110 individual equity tickers.
"""

UNIVERSE: list[str] = [

    # ── WSB / retail-driven mega+large (kept despite size) ────────────────
    # These trade on crowd flow, not analyst consensus. IV is set by
    # retail option-buying, which mis-prices regularly.
    "TSLA", "NVDA", "AMD", "META",        # mega — narrative-driven
    "PLTR", "COIN", "MSTR",                # WSB favorites
    "RDDT", "HOOD", "DKNG",                # retail platforms
    "RKLB", "LCID", "RIVN", "NIO", "XPEV", # space + EV speculation
    "SOFI", "AFRM",                        # retail fintech
    "IONQ",                                # quantum speculation

    # ── AI / SaaS / disruptive tech (mid + small) ─────────────────────────
    "AI", "BBAI", "SOUN", "PATH", "S", "GTLB", "MNDY", "BILL",
    "ESTC", "FROG", "FRSH", "GLBE", "RPD",
    "RGTI", "QUBT", "LAES",                # quantum / chip-edge speculation

    # ── Crypto-adjacent (high beta to BTC, retail-driven) ─────────────────
    "MARA", "RIOT", "CIFR", "BTBT",
    "HUT", "HIVE", "IREN", "GLXY", "APLD", "CLSK",
    "BTDR", "WULF",

    # ── EV / clean energy / nuclear (speculation-heavy) ───────────────────
    "PLUG", "QS", "RUN", "SEDG",
    "OKLO", "SMR", "EOSE", "UEC", "DNN",   # nuclear / battery speculation
    "EVGO", "BLNK", "CHPT",                # EV charging small

    # ── Fintech edge (retail-watched) ─────────────────────────────────────
    "UPST", "NU", "PYPL", "LMND", "ROOT", "COMP",
    "DLO",                                 # LatAm fintech

    # ── Sentiment / meme / speculation small ──────────────────────────────
    "GME", "AMC", "DJT", "BB",             # legacy WSB / meme
    "CLOV",                                # micro speculation

    # ── Consumer internet / sentiment-heavy mid ───────────────────────────
    "OPEN", "RBLX", "UPWK", "SG",          # mid-cap retail-followed
    "HIMS",                                # recent WSB favorite

    # ── Biotech catalyst small/mid (high IV, news-driven) ─────────────────
    "IOVA", "BEAM", "CRSP", "NTLA", "TGTX", "IMRX",
    "AGEN", "OCGN", "ATAI",                # micro biotech volatility
    "BCRX", "CLDX", "DAWN",                # mid biotech catalyst

    # ── Materials / commodities (cyclical, retail-followed) ───────────────
    "CLF", "MP", "TMC",                    # commodity speculation
    "HCC", "UAMY",                         # specialty mining

    # ── Cannabis (high IV, sentiment-driven) ──────────────────────────────
    "TLRY", "CGC", "CRON",

    # ── Recent IPO / SPAC volatility ──────────────────────────────────────
    "LUNR", "CART",                        # space + recent IPO

    # ── Other catalyst-prone small ────────────────────────────────────────
    "FUBO",
]
