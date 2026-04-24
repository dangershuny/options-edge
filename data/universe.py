"""
Curated universe of US equities and ETFs for options scanning.

Bias: mid-cap and small-cap names where retail with $500-$2K bankrolls
has structural edge — less analyst coverage, slower news propagation,
fewer market-makers tightening IV/RV spreads, and bigger % moves on
catalysts. Mega-caps with 20+ analysts following (AAPL/MSFT/GOOGL/etc.)
are intentionally excluded — pricing is too efficient there.

Inclusion rules:
  • Active options chain (≥4 expiries on yfinance verified)
  • Market cap ≥ $400M (chains thin out below that)
  • Either narrative/sentiment-driven OR thematic (AI/EV/crypto/biotech)
  • ETFs limited to broad-market + most-liquid sectors

Total: ~95 tickers. All names verified to have ≥4 option expiries.
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

    # ── Energy (limited; sector ETF covers most) ──────────────────────────
    "XOM", "CVX", "CCJ",

    # ── Other catalyst-prone names ────────────────────────────────────────
    "TLRY", "CGC", "FUBO",

    # ── Broad-market ETFs (always tradeable, hedge candidates) ────────────
    "SPY", "QQQ", "IWM", "VXX",

    # ── Sector ETFs (most-active only) ────────────────────────────────────
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLY",

    # ── Thematic / commodity / rate ETFs ──────────────────────────────────
    "ARKK", "GLD", "SLV", "TLT", "EEM",
]
