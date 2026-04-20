import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime

MIN_MARKET_CAP = 100_000_000  # $100M


def get_ticker_meta(symbol: str) -> tuple[float | None, str]:
    """Single info call returning (market_cap, company_name). Avoids duplicate API hits."""
    try:
        info = yf.Ticker(symbol).info
        cap = info.get("marketCap") or info.get("market_cap")
        name = info.get("longName") or info.get("shortName") or symbol
        return cap, name
    except Exception:
        return None, symbol


def check_market_cap(symbol: str) -> tuple[bool, float | None, str]:
    cap, name = get_ticker_meta(symbol)
    if cap is None:
        return True, None, name
    return cap >= MIN_MARKET_CAP, cap, name


def get_current_price(symbol: str) -> float | None:
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="1d")
    if hist.empty:
        return None
    return float(hist["Close"].iloc[-1])


def get_historical_prices(symbol: str, days: int = 90) -> pd.Series | None:
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=f"{days}d")
    if hist.empty or len(hist) < 10:
        return None
    return hist["Close"]


def get_earnings_date(ticker: yf.Ticker) -> datetime | None:
    try:
        cal = ticker.calendar
        if cal is None:
            return None
        if isinstance(cal, dict) and "Earnings Date" in cal:
            dates = cal["Earnings Date"]
            if isinstance(dates, list) and dates:
                return pd.Timestamp(dates[0]).to_pydatetime()
        if isinstance(cal, pd.DataFrame) and not cal.empty:
            row = cal.get("Earnings Date")
            if row is not None:
                return pd.Timestamp(row.iloc[0]).to_pydatetime()
    except Exception:
        pass
    return None


def get_options_chain(symbol: str, earnings_buffer_days: int = 10) -> tuple[pd.DataFrame | None, datetime | None, str | None]:
    ticker = yf.Ticker(symbol)
    expirations = ticker.options
    if not expirations:
        return None, None, f"No options found for {symbol}"

    earnings_date = get_earnings_date(ticker)
    chains = []

    for exp in expirations[:8]:
        try:
            exp_dt = datetime.strptime(exp, "%Y-%m-%d")
        except ValueError:
            continue

        dte = (exp_dt - datetime.now()).days
        if dte < 1:
            continue

        # Skip expiries that straddle an earnings date
        if earnings_date:
            days_to_earnings = abs((exp_dt - earnings_date).days)
            if days_to_earnings <= earnings_buffer_days:
                continue

        try:
            chain = ticker.option_chain(exp)
        except Exception:
            continue

        calls = chain.calls.copy()
        puts = chain.puts.copy()
        for df, opt_type in [(calls, "call"), (puts, "put")]:
            df["expiry"] = exp
            df["type"] = opt_type
            df["dte"] = dte
        chains.append(pd.concat([calls, puts], ignore_index=True))

    if not chains:
        return None, earnings_date, f"No valid expiries for {symbol} (all near earnings or expired)"

    return pd.concat(chains, ignore_index=True), earnings_date, None
