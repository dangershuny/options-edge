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
  • Active options chain (≥4 expiries on yfinance verified)
  • Market cap ≥ $400M (chains thin out below that)
  • Either narrative/sentiment-driven OR thematic (AI/EV/crypto/biotech)

Total: ~83 individual equity tickers. All names verified to have ≥4
option expiries.
"""

UNIVERSE: list[str] = [

    # ── Volatile mega/large-caps where edge still exists ──────────────────
    # (kept because retail flow & narrative dynamics dominate IV pricing)
    "TSLA", "NVDA", "AMD", "INTC", "META", "NFLX", "DIS", "SPOT",
    "BA", "F", "GM", "RTX", "CAT", "OXY", "LULU", "NKE", "CMCSA",

    # ── AI / disruptive tech (mid-small) ──────────────────────────────────
    "AI", "BBAI", "IONQ", "SOUN", "PATH", "S", "GTLB", "U", "MNDY",
    "BILL", "PLTR", "SNOW", "DDOG", "NET", "OKTA", "DOCU", "ZS", "MDB",
    "TWLO",

    # ── Crypto-adjacent (high beta to BTC, retail-driven) ─────────────────
    "COIN", "MARA", "RIOT", "MSTR", "CIFR", "BTBT",

    # ── EV / lithium / clean energy ───────────────────────────────────────
    "RIVN", "LCID", "NIO", "XPEV", "ALB", "PLUG", "RKLB",

    # ── Fintech (retail-watched) ──────────────────────────────────────────
    "SOFI", "AFRM", "UPST", "PYPL", "NU", "HOOD",

    # ── Consumer internet / retail (sentiment-heavy) ──────────────────────
    "SNAP", "RDDT", "DKNG", "PINS", "ROKU", "CHWY", "ETSY", "ABNB",
    "CART", "OPEN", "RBLX", "UBER", "LYFT", "UPWK",

    # ── Biotech / pharma (catalyst-driven, high IV) ───────────────────────
    "MRNA", "BNTX", "IOVA", "NBIX", "VRTX", "BIIB", "HIMS",

    # ── Energy / commodities (catalyst-prone names only) ──────────────────
    "XOM", "CVX", "CCJ",

    # ── Other catalyst-prone names ────────────────────────────────────────
    "TLRY", "CGC", "FUBO",
]
