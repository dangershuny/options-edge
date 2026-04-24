"""
Curated universe of US equities for options scanning.

Bias: mid-cap and small-cap names where retail with $500-$2K bankrolls
has structural edge — less analyst coverage, slower news propagation,
fewer market-makers tightening IV/RV spreads, and bigger % moves on
catalysts. Mega-caps with 20+ analysts following (AAPL/MSFT/GOOGL/etc.)
are intentionally excluded — pricing is too efficient there.

ETFs are also intentionally excluded:
  • No idiosyncratic catalysts (8-Ks, insider trades, earnings)
  • Sentinel divergence/sentiment signals don't apply
  • Options markets are already hyper-efficient (SPY is the most-traded
    options contract in the world; spreads are pinned by HFT)
  • Sector rotation is captured indirectly via individual sector stocks
  • Macro/VIX regime is handled separately by data/macro.py

Inclusion rules:
  • Active options chain (≥5 expiries on yfinance verified)
  • Market cap ≥ $400M (chains thin out below that)
  • Either narrative/sentiment-driven OR thematic (AI/EV/crypto/biotech)
  • Live ticker (not renamed/delisted)

Total: ~120 individual equity tickers.
"""

UNIVERSE: list[str] = [

    # ── Volatile mega/large-caps where edge still exists ──────────────────
    # (kept because retail flow & narrative dynamics dominate IV pricing)
    "TSLA", "NVDA", "AMD", "INTC", "META", "NFLX", "DIS", "SPOT",
    "BA", "F", "GM", "RTX", "CAT", "OXY", "LULU", "NKE", "CMCSA",

    # ── AI / SaaS / disruptive tech (mid + a few large) ───────────────────
    "AI", "BBAI", "IONQ", "SOUN", "PATH", "S", "GTLB", "U", "MNDY",
    "BILL", "PLTR", "SNOW", "DDOG", "NET", "OKTA", "DOCU", "ZS", "MDB",
    "TWLO", "ESTC", "FROG", "FRSH", "GLBE", "RPD", "TEAM", "ZM",

    # ── Crypto-adjacent (high beta to BTC, retail-driven) ─────────────────
    "COIN", "MARA", "RIOT", "MSTR", "CIFR", "BTBT",
    "HUT", "HIVE", "IREN", "GLXY", "APLD", "CLSK",

    # ── EV / lithium / clean energy ───────────────────────────────────────
    "RIVN", "LCID", "NIO", "XPEV", "ALB", "PLUG", "RKLB",
    "BE", "CHPT", "QS", "ENPH", "FSLR", "RUN", "SEDG",

    # ── Fintech (retail-watched) ──────────────────────────────────────────
    "SOFI", "AFRM", "UPST", "PYPL", "NU", "HOOD",
    "LMND", "ROOT", "COMP",

    # ── Consumer internet / retail (sentiment-heavy) ──────────────────────
    "SNAP", "RDDT", "DKNG", "PINS", "ROKU", "CHWY", "ETSY", "ABNB",
    "CART", "OPEN", "RBLX", "UBER", "LYFT", "UPWK",

    # ── Travel / leisure (cyclical, news-driven) ──────────────────────────
    "CCL", "RCL",

    # ── Media / sentiment-prone ───────────────────────────────────────────
    "WBD",

    # ── Biotech / pharma (catalyst-driven, high IV) ───────────────────────
    "MRNA", "BNTX", "IOVA", "NBIX", "VRTX", "BIIB", "HIMS",
    "ARWR", "BEAM", "CRSP", "CYTK", "NTLA", "AXSM", "TGTX", "IMRX",

    # ── Materials / commodities (cyclical, news-driven) ───────────────────
    "AA", "CLF", "FCX", "MP", "CCJ",

    # ── Energy single-names (sector ETF excluded by design) ───────────────
    "XOM", "CVX",

    # ── Defense (small/mid, news-driven) ──────────────────────────────────
    "KTOS", "AVAV",

    # ── Asia tech (Chinese / Asian, less US analyst coverage) ─────────────
    "PDD", "SE",

    # ── Other catalyst-prone names ────────────────────────────────────────
    "TLRY", "CGC", "FUBO",
]
