"""
Curated universe of high-liquidity US equities and ETFs suitable for
options scanning. All names have significant options volume, market
caps well above $100M, and active retail/institutional interest.
"""

UNIVERSE: list[str] = [
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "META", "AMZN", "TSLA",
    "AMD", "INTC", "QCOM", "AVGO", "MU", "AMAT", "LRCX", "KLAC",
    "CRM", "ORCL", "NOW", "ADBE", "SNOW", "PLTR", "UBER", "LYFT",
    "NFLX", "SPOT", "RBLX", "COIN", "HOOD",

    # Finance
    "JPM", "BAC", "GS", "MS", "C", "WFC", "BLK", "SCHW", "AXP",
    "V", "MA", "PYPL", "SQ", "NU",

    # Healthcare / pharma
    "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "AMGN", "GILD",
    "MRNA", "BNTX", "UNH", "CVS", "HUM",

    # Energy
    "XOM", "CVX", "COP", "OXY", "SLB", "HAL", "MPC", "VLO",

    # Consumer / retail
    "WMT", "TGT", "COST", "HD", "LOW", "MCD", "SBUX", "NKE",
    "LULU", "DIS", "CMCSA",

    # Industrials / macro
    "BA", "CAT", "DE", "GE", "HON", "LMT", "RTX", "F", "GM",

    # Broad ETFs (high options liquidity)
    "SPY", "QQQ", "IWM", "DIA", "VXX",

    # Sector ETFs
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLP", "XLRE",
    "XLB", "XLU", "ARKK", "GLD", "SLV", "TLT", "HYG", "EEM",
]
