import pandas as pd


# Volume/OI above this ratio is considered elevated
ELEVATED_THRESHOLD = 0.3
STRONG_THRESHOLD = 1.0


def classify_flow(vol_oi_ratio: float) -> str:
    if vol_oi_ratio >= STRONG_THRESHOLD:
        return "STRONG"
    elif vol_oi_ratio >= ELEVATED_THRESHOLD:
        return "ELEVATED"
    return "NORMAL"


def enrich_flow(df: pd.DataFrame) -> pd.DataFrame:
    """Add vol_oi_ratio and flow_signal columns to an options chain DataFrame."""
    df = df.copy()
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
    df["openInterest"] = pd.to_numeric(df.get("openInterest", 0), errors="coerce").fillna(0)

    df["vol_oi_ratio"] = df.apply(
        lambda r: round(r["volume"] / r["openInterest"], 3) if r["openInterest"] > 0 else 0.0,
        axis=1,
    )
    df["flow_signal"] = df["vol_oi_ratio"].apply(classify_flow)
    return df


def directional_bias(calls_flow: str, puts_flow: str) -> str:
    """
    Given the flow signal on the call side vs put side for the same strike,
    return a rough directional hint.
    """
    call_rank = {"STRONG": 2, "ELEVATED": 1, "NORMAL": 0}
    c = call_rank.get(calls_flow, 0)
    p = call_rank.get(puts_flow, 0)
    if c > p:
        return "BULLISH FLOW"
    elif p > c:
        return "BEARISH FLOW"
    return "MIXED"
