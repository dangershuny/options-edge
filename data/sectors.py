"""
Static ticker → sector mapping for the scanning universe.

Hardcoded (rather than fetched via yfinance `info`) so it's fast, offline-safe,
and doesn't add another network dependency. When a ticker is missing from the
map we return 'Unknown' — callers should treat this as a non-signal.
"""

SECTOR_MAP: dict[str, str] = {
    # Technology
    "AAPL": "Tech", "MSFT": "Tech", "NVDA": "Tech", "GOOGL": "Tech", "GOOG": "Tech",
    "META": "Tech", "AMZN": "Tech", "TSLA": "Tech", "AMD": "Tech", "INTC": "Tech",
    "QCOM": "Tech", "AVGO": "Tech", "MU": "Tech", "AMAT": "Tech", "LRCX": "Tech",
    "KLAC": "Tech", "CRM": "Tech", "ORCL": "Tech", "NOW": "Tech", "ADBE": "Tech",
    "SNOW": "Tech", "PLTR": "Tech", "NFLX": "Tech", "SPOT": "Tech", "RBLX": "Tech",
    "UBER": "Tech", "LYFT": "Tech",

    # Crypto-adjacent
    "COIN": "Crypto", "HOOD": "Crypto", "MSTR": "Crypto",

    # Finance
    "JPM": "Finance", "BAC": "Finance", "GS": "Finance", "MS": "Finance", "C": "Finance",
    "WFC": "Finance", "BLK": "Finance", "SCHW": "Finance", "AXP": "Finance",
    "V": "Finance", "MA": "Finance", "PYPL": "Finance", "SQ": "Finance", "NU": "Finance",

    # Healthcare / pharma
    "JNJ": "Healthcare", "PFE": "Healthcare", "MRK": "Healthcare", "ABBV": "Healthcare",
    "LLY": "Healthcare", "BMY": "Healthcare", "AMGN": "Healthcare", "GILD": "Healthcare",
    "MRNA": "Healthcare", "BNTX": "Healthcare", "UNH": "Healthcare", "CVS": "Healthcare",
    "HUM": "Healthcare",

    # Energy
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "OXY": "Energy",
    "SLB": "Energy", "HAL": "Energy", "MPC": "Energy", "VLO": "Energy",

    # Consumer / retail
    "WMT": "Consumer", "TGT": "Consumer", "COST": "Consumer", "HD": "Consumer",
    "LOW": "Consumer", "MCD": "Consumer", "SBUX": "Consumer", "NKE": "Consumer",
    "LULU": "Consumer", "DIS": "Consumer", "CMCSA": "Consumer",

    # Industrials
    "BA": "Industrials", "CAT": "Industrials", "DE": "Industrials", "GE": "Industrials",
    "HON": "Industrials", "LMT": "Industrials", "RTX": "Industrials",
    "F": "Auto", "GM": "Auto",

    # ETFs — map to their dominant exposure
    "SPY": "ETF-Broad", "QQQ": "ETF-Tech", "IWM": "ETF-SmallCap",
    "XLF": "Finance", "XLE": "Energy", "XLK": "Tech", "XLV": "Healthcare",
}


def get_sector(ticker: str) -> str:
    """Return sector label, or 'Unknown' if the ticker isn't mapped."""
    return SECTOR_MAP.get(ticker.upper(), "Unknown")


def tickers_in_sector(sector: str) -> list[str]:
    """Return every known ticker tagged to the given sector."""
    return [t for t, s in SECTOR_MAP.items() if s == sector]


def all_sectors() -> list[str]:
    """All unique sector labels currently in the map."""
    return sorted(set(SECTOR_MAP.values()))
