"""
Build a self-contained HTML dashboard of Options Edge activity.

Single-page dashboard with cards, tables, and charts. Sections mirror the
`daily_review` output: account, today's snapshot, per-tier paper trades,
open positions, deployed-capital history, alerts.

Data sources (all local):
  - snapshots/                             latest morning snapshot
  - logs/paper_trades.jsonl                trade log
  - logs/morning_auto_run_YYYY-MM-DD.json  today's run summary
  - logs/error_alert_*.log                 recent alerts
  - Alpaca account via broker.alpaca       cash / positions

Embedded as JSON in the HTML — no server needed. Chart.js loads from CDN.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    import config_loader  # noqa: F401
except Exception:
    pass

DASHBOARD_PATH = REPO_ROOT / "dashboard.html"


# ── Data collection ─────────────────────────────────────────────────────────

def _safe_json_load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_snapshot_for(d: date):
    snap_dir = REPO_ROOT / "snapshots"
    candidates = []
    for f in snap_dir.glob("*.json"):
        if not f.is_file() or f.parent != snap_dir:
            continue
        mtime_d = datetime.fromtimestamp(f.stat().st_mtime).date()
        if mtime_d == d:
            candidates.append((f.stat().st_mtime, f))
    if not candidates:
        return None, {}
    candidates.sort(reverse=True)
    path = candidates[0][1]
    data = _safe_json_load(path) or {}
    return path, data


def _paper_trades(days_back: int = 30):
    path = REPO_ROOT / "logs" / "paper_trades.jsonl"
    if not path.exists():
        return []
    cutoff = (date.today() - timedelta(days=days_back)).isoformat()
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                if o.get("timestamp", "") >= cutoff:
                    out.append(o)
    except Exception:
        pass
    return out


def _recent_alerts(days_back: int = 3):
    log_dir = REPO_ROOT / "logs"
    if not log_dir.exists():
        return []
    cutoff = datetime.now() - timedelta(days=days_back)
    alerts = []
    for f in sorted(log_dir.glob("error_alert_*.log"), reverse=True):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            continue
        data = _safe_json_load(f)
        if isinstance(data, dict):
            alerts.append(data)
    return alerts


def _flow_news_today(d: date):
    fn_dir = REPO_ROOT / "snapshots" / "flow_news"
    if not fn_dir.exists():
        return {"scans": 0, "high_conviction": []}
    date_str = d.strftime("%Y%m%d")
    files = sorted(fn_dir.glob(f"flow_news_{date_str}_*.json"))
    if not files:
        return {"scans": 0, "high_conviction": []}
    high = []
    for f in files:
        data = _safe_json_load(f) or {}
        for r in data.get("results", []):
            if r.get("combined") == "HIGH_CONVICTION":
                high.append({"scan": f.stem.split("_")[-1], **r})
    return {"scans": len(files), "high_conviction": high}


def _get_broker_status():
    try:
        import broker.alpaca as b
        a = b.get_account()
        return {
            "connected": True,
            "account_number": a.account_number,
            "paper": a.is_paper,
            "equity": a.equity,
            "cash": a.cash,
            "buying_power": a.buying_power,
            "blocked": a.account_blocked,
        }
    except Exception as e:
        return {"connected": False, "error": str(e)[:200]}


def _positions():
    try:
        import broker.alpaca as b
        pos = b.get_positions()
        return [
            {
                "symbol": p.symbol, "qty": p.qty, "avg_entry": p.avg_entry,
                "mark": p.mark, "market_value": p.market_value,
                "unrealized_pl": p.unrealized_pl,
                "unrealized_pl_pct": p.unrealized_pl_pct,
            }
            for p in pos
        ]
    except Exception:
        return []


def _tier_stats(trades):
    by_tier: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        tag = t.get("tag") or "(untagged)"
        by_tier[tag].append(t)

    stats = {}
    for tier, items in by_tier.items():
        submitted = [x for x in items if x.get("status") == "submitted"]
        dry = [x for x in items if x.get("status") == "dry_run"]
        skipped = [x for x in items if x.get("status") == "skipped"]
        failed = [x for x in items if x.get("status") == "failed"]
        stats[tier] = {
            "total_attempts": len(items),
            "submitted": len(submitted),
            "dry_run": len(dry),
            "skipped": len(skipped),
            "failed": len(failed),
            "deployed": round(sum(float(x.get("total_cost") or 0) for x in submitted), 2),
            "orders": items,
        }
    return stats


def _daily_deployed_by_tier(trades):
    by_day_tier: dict[tuple[str, str], float] = defaultdict(float)
    for t in trades:
        ts = t.get("timestamp", "")
        if not ts or t.get("status") != "submitted":
            continue
        d = ts[:10]
        tag = t.get("tag") or "(untagged)"
        by_day_tier[(d, tag)] += float(t.get("total_cost") or 0)

    tiers = sorted({k[1] for k in by_day_tier})
    days = sorted({k[0] for k in by_day_tier})
    series = {tier: [by_day_tier.get((d, tier), 0.0) for d in days] for tier in tiers}
    return {"days": days, "tiers": tiers, "series": series}


def _score_distribution(snap_trades):
    """Histogram of scores for the analysis tab."""
    buckets = {"0-40": 0, "40-50": 0, "50-60": 0, "60-70": 0, "70-80": 0, "80+": 0}
    for t in snap_trades:
        s = float(t.get("score") or 0)
        if s < 40: buckets["0-40"] += 1
        elif s < 50: buckets["40-50"] += 1
        elif s < 60: buckets["50-60"] += 1
        elif s < 70: buckets["60-70"] += 1
        elif s < 80: buckets["70-80"] += 1
        else: buckets["80+"] += 1
    return buckets


def _signal_breakdown(snap_trades):
    by_sig: dict[str, int] = defaultdict(int)
    by_type: dict[str, int] = defaultdict(int)
    for t in snap_trades:
        by_sig[t.get("vol_signal", "?")] += 1
        by_type[t.get("option_type", "?")] += 1
    return {"by_signal": dict(by_sig), "by_type": dict(by_type)}


def _classify_session(ts_iso: str) -> str:
    """Classify a UTC timestamp into regular/pre/post session (ET-based)."""
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        # ET = UTC - 4 (DST) or UTC - 5 (EST). Use -4 as approximation; IEX feed
        # is DST-aware so bars are already at correct clock times.
        # 9:30 AM ET = 13:30 UTC (DST) / 14:30 UTC (EST)
        # 4:00 PM ET = 20:00 UTC (DST) / 21:00 UTC (EST)
        # 4:00 AM ET = 08:00 UTC (DST) / 09:00 UTC (EST)
        # 8:00 PM ET = 00:00 UTC (DST, next day)
        hour_utc = dt.hour + dt.minute / 60
        # Regular session 13:30-20:00 UTC (DST) is most accurate
        if 13.5 <= hour_utc < 20:
            return "regular"
        elif 8 <= hour_utc < 13.5:
            return "pre"
        else:
            return "post"
    except Exception:
        return "regular"


def _fetch_ticker_bars(symbols: list[str]) -> dict:
    """
    Fetch intraday + daily bars for each unique ticker.
    Uses Alpaca's free IEX feed, which includes extended-hours bars.

    Returns:
        {symbol: {
            "intraday": [{t, o, h, l, c, v, s}, ...],  # 5-min bars, ~5 days, s=session
            "daily":    [{t, o, h, l, c, v}, ...],     # daily bars, ~365 days
            "last":     float,                          # most recent close
            "last_ts":  str,                            # ISO timestamp of most recent bar
        }}

    Session (s) values: "regular", "pre", "post" — client-side toggles hide/show.
    """
    if not symbols:
        return {}
    try:
        from alpaca.data.historical.stock import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        from alpaca.data.enums import DataFeed
        import os
    except Exception:
        return {}

    key = os.environ.get("ALPACA_API_KEY")
    sec = os.environ.get("ALPACA_API_SECRET")
    if not key or not sec:
        return {}

    client = StockHistoricalDataClient(key, sec)
    unique = sorted(set(s.upper() for s in symbols if s))
    result: dict[str, dict] = {s: {"intraday": [], "daily": [], "last": None, "last_ts": None}
                               for s in unique}

    now = datetime.now(timezone.utc)

    # Intraday: last ~4 days of 5-min bars (enough for a 1D and 5D view)
    try:
        req = StockBarsRequest(
            symbol_or_symbols=unique,
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=now - timedelta(days=5),
            end=now,
            feed=DataFeed.IEX,
        )
        bars = client.get_stock_bars(req)
        raw = getattr(bars, "data", {}) or {}
        for sym, rows in raw.items():
            ud = sym.upper()
            result[ud]["intraday"] = [
                {
                    "t": b.timestamp.isoformat(),
                    "o": float(b.open), "h": float(b.high),
                    "l": float(b.low), "c": float(b.close),
                    "v": int(b.volume or 0),
                    "s": _classify_session(b.timestamp.isoformat()),
                }
                for b in rows
            ]
            if rows:
                result[ud]["last"] = float(rows[-1].close)
                result[ud]["last_ts"] = rows[-1].timestamp.isoformat()
    except Exception as e:
        for s in unique:
            result[s].setdefault("error", str(e)[:100])

    # Daily: last 365 days (for 1M, 3M, 1Y toggles)
    try:
        req = StockBarsRequest(
            symbol_or_symbols=unique,
            timeframe=TimeFrame.Day,
            start=now - timedelta(days=365),
            end=now,
            feed=DataFeed.IEX,
        )
        bars = client.get_stock_bars(req)
        raw = getattr(bars, "data", {}) or {}
        for sym, rows in raw.items():
            ud = sym.upper()
            result[ud]["daily"] = [
                {"t": b.timestamp.date().isoformat(), "o": float(b.open),
                 "h": float(b.high), "l": float(b.low), "c": float(b.close),
                 "v": int(b.volume or 0)}
                for b in rows
            ]
            # If we didn't get intraday, use latest daily as "last"
            if rows and result[ud]["last"] is None:
                result[ud]["last"] = float(rows[-1].close)
                result[ud]["last_ts"] = rows[-1].timestamp.isoformat()
    except Exception as e:
        for s in unique:
            result[s].setdefault("error", str(e)[:100])

    return result


def _load_user_watchlist() -> list[str]:
    """
    Load ticker watchlist from watchlist.json at repo root.

    Format: ["AAPL", "TSLA", ...]   (raw list)
           or  {"tickers": [...]}   (wrapped)

    Returns [] on any failure.
    """
    path = REPO_ROOT / "watchlist.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(x).upper() for x in data if x]
        if isinstance(data, dict):
            items = data.get("tickers") or data.get("watchlist") or []
            return [str(x).upper() for x in items if x]
    except Exception:
        pass
    return []


def _fetch_news_per_ticker(symbols: list[str], per_ticker: int = 6) -> dict:
    """
    Pull recent news for each ticker using existing data/news.py.

    Returns: {symbol: [{title, link, published, summary, source, sentiment}, ...]}
    Failures per ticker are silent — we return {} for that ticker.
    """
    if not symbols:
        return {}
    try:
        from data.news import get_news
    except Exception:
        return {sym: [] for sym in symbols}

    out: dict[str, list[dict]] = {}
    for sym in symbols:
        try:
            articles = get_news(sym, max_age_days=3, limit=per_ticker) or []
            # Normalize shape
            out[sym] = [
                {
                    "title": a.get("title", "")[:160],
                    "link": a.get("link") or a.get("url") or "",
                    "published": a.get("published") or "",
                    "summary": (a.get("summary") or "")[:300],
                    "source": a.get("source") or "",
                    "sentiment": a.get("sentiment"),
                }
                for a in articles
                if a.get("title")
            ]
        except Exception:
            out[sym] = []
    return out


# ── Main build ──────────────────────────────────────────────────────────────

def _best_recent_snapshot():
    """
    Walk back up to 7 days, preferring the snapshot with the most trades
    (avoids picking empty end-of-day scans).
    """
    today = date.today()
    best = None
    best_count = -1
    best_note = None
    # Check today first (prefer even an empty scan from today over yesterday's)
    for back in range(0, 8):
        d = today - timedelta(days=back)
        # Enumerate ALL of d's snapshots, not just the newest
        snap_dir = REPO_ROOT / "snapshots"
        for f in snap_dir.glob("*.json"):
            if not f.is_file() or f.parent != snap_dir:
                continue
            if datetime.fromtimestamp(f.stat().st_mtime).date() != d:
                continue
            data = _safe_json_load(f)
            if not isinstance(data, dict):
                continue
            n = len(data.get("trades", []) or [])
            # Prefer most trades. If the picked snap is today, keep it unless 0.
            if n > best_count:
                best = (f, data)
                best_count = n
                best_note = None if back == 0 else d.isoformat()
        # If today had any snap with trades, stop walking back
        if back == 0 and best_count > 0:
            break
    if best:
        return best[0], best[1], best_note
    return None, {}, None


def build() -> Path:
    today = date.today()
    snap_path, snap, fallback_note = _best_recent_snapshot()

    snap_trades = sorted(
        snap.get("trades", []),
        key=lambda x: -float(x.get("score", 0) or 0),
    )

    trades_log = _paper_trades(days_back=30)
    tier_stats = _tier_stats(trades_log)
    deployed_history = _daily_deployed_by_tier(trades_log)
    run_summary = _safe_json_load(REPO_ROOT / "logs" / f"morning_auto_run_{today.isoformat()}.json") or {}
    alerts = _recent_alerts(days_back=3)
    broker = _get_broker_status()
    positions = _positions()
    flow_news = _flow_news_today(today)

    score_hist = _score_distribution(snap_trades)
    sig_break = _signal_breakdown(snap_trades)

    # Gather tickers: snapshot tickers + user watchlist + popular benchmarks
    snap_tickers = sorted({
        (t.get("symbol") or "").upper()
        for t in snap_trades
        if t.get("symbol")
    })
    user_watchlist = _load_user_watchlist()
    benchmarks = ["SPY", "QQQ", "IWM", "VIX"]  # always included
    all_tickers = []
    seen = set()
    for t in snap_tickers + user_watchlist + benchmarks:
        if t and t not in seen:
            seen.add(t)
            all_tickers.append(t)
    # Cap at 30 to keep file size reasonable
    all_tickers = all_tickers[:30]

    ticker_bars = _fetch_ticker_bars(all_tickers)

    # News — only fetch for a small set (news fetch is slow); use snapshot + watchlist
    news_set = []
    news_seen = set()
    for t in snap_tickers + user_watchlist:
        if t and t not in news_seen and len(news_set) < 20:
            news_seen.add(t)
            news_set.append(t)
    ticker_news = _fetch_news_per_ticker(news_set, per_ticker=6)

    data = {
        "generated": datetime.now().isoformat(),
        "today": today.isoformat(),
        "snap_path": str(snap_path) if snap_path else None,
        "snap_fallback_date": fallback_note,
        "snap_trades": snap_trades[:50],
        "snap_total": len(snap_trades),
        "tier_stats": tier_stats,
        "deployed_history": deployed_history,
        "run_summary": run_summary,
        "alerts": alerts,
        "broker": broker,
        "positions": positions,
        "flow_news": flow_news,
        "score_histogram": score_hist,
        "signal_breakdown": sig_break,
        "trades_last_30d": len(trades_log),
        "snap_tickers": snap_tickers,           # today's highlighted tickers (buttons)
        "user_watchlist": user_watchlist,       # user's watchlist.json
        "benchmarks": benchmarks,
        "unique_tickers": all_tickers,          # superset used for charts/search
        "ticker_bars": ticker_bars,
        "ticker_news": ticker_news,
    }

    body = _HTML_TEMPLATE.replace("__DATA__", json.dumps(data, default=str))
    DASHBOARD_PATH.write_text(body, encoding="utf-8")
    return DASHBOARD_PATH


# ── HTML template ───────────────────────────────────────────────────────────

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Options Edge Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  :root {
    --bg: #0f1117;
    --bg-elev: #181b24;
    --card: #1d212d;
    --card-hover: #242936;
    --border: #2a2f3e;
    --fg: #e8ebf4;
    --mute: #8b93a7;
    --subtle: #5a617a;
    --accent: #7aa2f7;
    --accent-2: #bb9af7;
    --ok: #9ece6a;
    --warn: #e0af68;
    --err: #f7768e;
    --info: #7dcfff;
    --gradient-a: linear-gradient(135deg, rgba(122,162,247,0.15) 0%, rgba(122,162,247,0) 100%);
    --shadow: 0 2px 8px rgba(0,0,0,0.3);
  }
  * { box-sizing: border-box; }
  html, body { background: var(--bg); color: var(--fg); }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, sans-serif;
    margin: 0; padding: 0; min-height: 100vh;
    background: radial-gradient(ellipse at top, #1a1f2e 0%, #0f1117 50%);
    background-attachment: fixed;
  }
  .page { max-width: 1500px; margin: 0 auto; padding: 24px 32px 48px; }

  header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border);
  }
  header h1 {
    margin: 0; font-size: 22px; font-weight: 600;
    background: linear-gradient(90deg, var(--accent), var(--accent-2));
    -webkit-background-clip: text; background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .subtitle { color: var(--mute); font-size: 12px; margin-top: 4px; }
  .header-actions { display: flex; gap: 8px; align-items: center; }
  .btn {
    background: var(--card); color: var(--fg); border: 1px solid var(--border);
    padding: 8px 14px; border-radius: 6px; cursor: pointer; font-size: 13px;
    transition: all 0.15s;
  }
  .btn:hover { background: var(--card-hover); border-color: var(--accent); }
  .btn.primary { background: var(--accent); color: var(--bg); border-color: var(--accent); font-weight: 500; }
  .btn.primary:hover { filter: brightness(1.1); }

  .tabs {
    display: flex; gap: 4px; margin-bottom: 24px;
    background: var(--card); border: 1px solid var(--border); border-radius: 8px;
    padding: 4px;
  }
  .tab {
    padding: 8px 18px; cursor: pointer; color: var(--mute);
    font-size: 13px; border-radius: 5px; user-select: none;
    transition: all 0.15s;
  }
  .tab.active { color: var(--fg); background: var(--bg-elev); }
  .tab:hover:not(.active) { color: var(--fg); }
  .tab-content { display: none; animation: fadeIn 0.2s; }
  .tab-content.active { display: block; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: none; } }

  section { margin-bottom: 28px; }
  .section-title {
    display: flex; align-items: center; gap: 10px; margin: 0 0 14px;
    font-size: 14px; font-weight: 500; color: var(--mute);
    text-transform: uppercase; letter-spacing: 0.8px;
  }
  .section-title .dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: var(--accent); box-shadow: 0 0 6px var(--accent);
  }
  .section-title .count {
    font-size: 11px; padding: 1px 8px; border-radius: 10px;
    background: var(--card); color: var(--fg); letter-spacing: 0;
  }

  /* Cards */
  .cards-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; }
  .stat-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    padding: 18px 20px; position: relative; overflow: hidden;
    transition: border-color 0.15s;
  }
  .stat-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--accent-2));
    opacity: 0.6;
  }
  .stat-card:hover { border-color: var(--accent); }
  .stat-card .label {
    color: var(--mute); font-size: 11px; font-weight: 500;
    text-transform: uppercase; letter-spacing: 0.8px;
  }
  .stat-card .value { font-size: 24px; font-weight: 600; margin-top: 6px; letter-spacing: -0.5px; }
  .stat-card .sub { color: var(--subtle); font-size: 11px; margin-top: 4px; }

  /* Tables */
  .table-wrap {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    overflow: hidden; box-shadow: var(--shadow);
  }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--border); }
  th {
    background: var(--bg-elev); color: var(--mute); font-weight: 500;
    font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px;
    position: sticky; top: 0;
  }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .mono { font-family: "JetBrains Mono", "SF Mono", "Cascadia Mono", Consolas, monospace; font-size: 12px; }

  .pos { color: var(--ok); }
  .neg { color: var(--err); }
  .warn-txt { color: var(--warn); }

  .badge {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-size: 10px; font-weight: 600; letter-spacing: 0.5px;
    background: rgba(122,162,247,0.15); color: var(--accent);
  }
  .badge.ok  { background: rgba(158,206,106,0.15); color: var(--ok); }
  .badge.warn{ background: rgba(224,175,104,0.15); color: var(--warn); }
  .badge.err { background: rgba(247,118,142,0.15); color: var(--err); }
  .badge.info{ background: rgba(125,207,255,0.15); color: var(--info); }

  /* Tier cards */
  .tier-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 14px; }
  .tier-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    padding: 18px 20px;
  }
  .tier-card h3 { margin: 0 0 14px; font-size: 15px; color: var(--fg); display: flex; align-items: center; gap: 8px; }
  .tier-card h3 .tier-icon {
    width: 24px; height: 24px; border-radius: 6px;
    background: linear-gradient(135deg, var(--accent), var(--accent-2));
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700; color: var(--bg);
  }
  .tier-stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 12px; }
  .tier-stat { text-align: center; padding: 8px; background: var(--bg-elev); border-radius: 6px; }
  .tier-stat .n { font-size: 16px; font-weight: 600; color: var(--fg); }
  .tier-stat .l { font-size: 10px; color: var(--mute); text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }

  /* Chart wraps */
  .chart-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    padding: 20px; position: relative;
  }
  .chart-card h3 {
    margin: 0 0 12px; font-size: 14px; color: var(--fg); display: flex;
    justify-content: space-between; align-items: center;
  }
  .chart-card canvas { max-height: 260px; }
  .ticker-chart-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px; transition: border-color 0.15s;
  }
  .ticker-chart-card:hover { border-color: var(--accent); }
  .ticker-head {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 10px;
  }
  .ticker-head .sym { font-size: 17px; font-weight: 600; }
  .ticker-head .last { font-size: 16px; color: var(--fg); margin-left: 8px; }
  .ticker-head .change { font-size: 13px; margin-left: 6px; }

  .indicator-bar {
    display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px;
  }
  .ind-btn {
    padding: 3px 10px; font-size: 11px; font-weight: 500;
    background: var(--bg-elev); color: var(--mute);
    border: 1px solid var(--border); border-radius: 4px;
    cursor: pointer; transition: all 0.15s;
  }
  .ind-btn:hover { color: var(--fg); border-color: var(--accent); }
  .ind-btn.active { background: var(--accent); color: var(--bg); border-color: var(--accent); }
  .ind-btn.ext { background: var(--bg-elev); }
  .ind-btn.ext.active { background: var(--warn); color: var(--bg); border-color: var(--warn); }
  .quick-btn {
    display: inline-block; padding: 5px 12px; margin: 2px 4px 2px 0;
    font-size: 12px; font-weight: 500;
    background: var(--card); color: var(--fg);
    border: 1px solid var(--border); border-radius: 14px;
    cursor: pointer; transition: all 0.15s;
  }
  .quick-btn:hover { background: var(--card-hover); border-color: var(--accent); }
  .quick-btn.watch { border-color: var(--accent-2); }
  .quick-btn .rm {
    margin-left: 6px; color: var(--err); font-weight: 600; font-size: 14px;
    cursor: pointer;
  }
  .news-item {
    background: var(--card); border: 1px solid var(--border);
    border-left: 3px solid var(--accent); border-radius: 8px;
    padding: 12px 16px; margin-bottom: 8px;
  }
  .news-item.pos { border-left-color: var(--ok); }
  .news-item.neg { border-left-color: var(--err); }
  .news-item a { color: var(--fg); text-decoration: none; font-weight: 500; font-size: 14px; }
  .news-item a:hover { color: var(--accent); }
  .news-item .meta { color: var(--mute); font-size: 11px; margin-top: 4px; }
  .news-item .summary { color: var(--mute); font-size: 12px; margin-top: 6px; line-height: 1.5; }
  .tf-toggles { display: inline-flex; background: var(--bg-elev); border-radius: 6px; padding: 2px; }
  .tf-btn {
    padding: 4px 10px; font-size: 11px; color: var(--mute);
    background: transparent; border: none; cursor: pointer; border-radius: 4px;
    transition: all 0.15s;
  }
  .tf-btn.active { background: var(--accent); color: var(--bg); font-weight: 600; }
  .tf-btn:hover:not(.active) { color: var(--fg); }

  .ticker-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 14px; }
  .ticker-chart-card canvas { max-height: 180px; }

  .empty {
    color: var(--mute); font-size: 13px; padding: 20px; text-align: center;
    background: var(--card); border: 1px dashed var(--border); border-radius: 8px;
  }

  .alert-card {
    background: var(--card); border: 1px solid var(--border); border-left: 3px solid var(--err);
    border-radius: 8px; padding: 12px 16px; margin-bottom: 8px; font-size: 13px;
  }
  .alert-card.info { border-left-color: var(--info); }
  .alert-card.warn { border-left-color: var(--warn); }
  .alert-card .alert-meta { color: var(--mute); font-size: 11px; margin-bottom: 4px; }
  .alert-card .alert-msg { color: var(--fg); }

  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (max-width: 900px) {
    .two-col { grid-template-columns: 1fr; }
    .tier-grid, .ticker-grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<div class="page">

<header>
  <div>
    <h1>Options Edge Dashboard</h1>
    <div class="subtitle" id="subtitle"></div>
  </div>
  <div class="header-actions">
    <label style="display:flex; align-items:center; gap:6px; font-size:12px; color:var(--mute); cursor:pointer;">
      <input type="checkbox" id="auto-refresh-cb" style="cursor:pointer;"> Auto-refresh
      <select id="auto-refresh-sec" style="background:var(--bg-elev); color:var(--fg);
              border:1px solid var(--border); padding:3px 6px; border-radius:4px; font-size:11px;">
        <option value="30">30s</option>
        <option value="60" selected>1m</option>
        <option value="300">5m</option>
      </select>
    </label>
    <button class="btn" onclick="location.reload()">Refresh now</button>
  </div>
</header>

<div class="tabs">
  <div class="tab active" data-tab="overview">Overview</div>
  <div class="tab" data-tab="analysis">Analysis</div>
  <div class="tab" data-tab="lookup">Ticker Lookup</div>
  <div class="tab" data-tab="override">Manual Buy</div>
  <div class="tab" data-tab="trades">Trades</div>
</div>

<!-- OVERVIEW -->
<div class="tab-content active" id="tab-overview">
  <section>
    <div class="section-title"><span class="dot"></span> Account <span id="ov-account-sub" class="count"></span></div>
    <div class="cards-grid" id="account-cards"></div>
  </section>

  <section>
    <div class="section-title"><span class="dot"></span> Today's snapshot <span class="count" id="snap-count">0</span></div>
    <div class="cards-grid" id="snap-summary-cards"></div>
  </section>

  <section>
    <div class="section-title"><span class="dot"></span> Open positions <span class="count" id="pos-count">0</span></div>
    <div id="positions-wrap"></div>
  </section>

  <section>
    <div class="section-title"><span class="dot"></span> Paper trades by tier</div>
    <div class="tier-grid" id="tier-grid"></div>
  </section>

  <section>
    <div class="section-title"><span class="dot"></span> Recent alerts <span class="count" id="alert-count">0</span></div>
    <div id="alerts-wrap"></div>
  </section>
</div>

<!-- ANALYSIS -->
<div class="tab-content" id="tab-analysis">
  <section>
    <div class="section-title"><span class="dot"></span> Score distribution</div>
    <div class="chart-card"><canvas id="scoreChart"></canvas></div>
  </section>

  <section>
    <div class="section-title">
      <span class="dot"></span> Live price per ticker
      <span class="count" id="ticker-count">0</span>
    </div>
    <div class="subtitle" style="margin-bottom: 14px;">Data from Alpaca IEX feed. Toggle timeframe per chart.</div>
    <div class="ticker-grid" id="ticker-charts"></div>
  </section>

  <section>
    <div class="section-title"><span class="dot"></span> Top snapshot candidates</div>
    <div class="table-wrap" id="snap-table-wrap"></div>
  </section>

  <section>
    <div class="section-title"><span class="dot"></span> Deployed capital (last 30d)</div>
    <div class="chart-card"><canvas id="pnlChart"></canvas></div>
  </section>
</div>

<!-- TICKER LOOKUP -->
<div class="tab-content" id="tab-lookup">
  <section>
    <div class="section-title"><span class="dot"></span> Ticker search</div>
    <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:14px;">
      <input id="lookup-search" type="text" placeholder="Type a ticker..." autocomplete="off"
             style="background:var(--bg-elev); color:var(--fg); border:1px solid var(--border);
                    padding:10px 14px; border-radius:6px; font-size:14px; min-width:220px;">
      <button class="btn primary" id="lookup-go">Show</button>
      <span class="subtitle" id="lookup-hint">Or click a ticker below</span>
    </div>
    <div id="lookup-quick-buttons" style="margin-bottom:10px;"></div>
    <div id="lookup-watchlist-buttons" style="margin-bottom:10px;"></div>
    <div style="display:flex; gap:10px; align-items:center; margin-bottom:14px;">
      <input id="lookup-wl-add" type="text" placeholder="Add ticker to watchlist" autocomplete="off"
             style="background:var(--bg-elev); color:var(--fg); border:1px solid var(--border);
                    padding:6px 12px; border-radius:6px; font-size:13px;">
      <button class="btn" id="lookup-wl-add-btn">+ Add to watchlist</button>
      <span class="subtitle">Stored in your browser</span>
    </div>
  </section>

  <section id="lookup-chart-section" style="display:none;">
    <div class="chart-card" id="lookup-main-chart-wrap">
      <div class="ticker-head">
        <div>
          <span class="sym" id="lookup-sym">—</span>
          <span class="last" id="lookup-last"></span>
          <span class="change" id="lookup-change"></span>
        </div>
        <div class="tf-toggles" id="lookup-tf-toggles">
          <button class="tf-btn active" data-tf="1D">1D</button>
          <button class="tf-btn" data-tf="5D">5D</button>
          <button class="tf-btn" data-tf="1M">1M</button>
          <button class="tf-btn" data-tf="3M">3M</button>
          <button class="tf-btn" data-tf="1Y">1Y</button>
        </div>
      </div>
      <div class="indicator-bar" id="lookup-ind-bar"></div>
      <canvas id="lookup-chart" style="max-height:380px;"></canvas>
    </div>
  </section>

  <section id="lookup-news-section" style="display:none;">
    <div class="section-title"><span class="dot"></span> News for <span id="lookup-news-sym">—</span> <span class="count" id="lookup-news-count">0</span></div>
    <div id="lookup-news-wrap"></div>
  </section>
</div>

<!-- MANUAL OVERRIDE BUY -->
<div class="tab-content" id="tab-override">
  <section>
    <div class="section-title"><span class="dot"></span> Manual override buy</div>
    <div class="subtitle" style="margin-bottom: 14px;">
      Saw a setup the scheduled scanner missed? Drop a ticker here and the
      system will analyze it, pick the most-poised contract, and submit a
      paper order tagged <code>override-*</code>. Concentration cap (max
      2/ticker/day) still applies.
    </div>

    <div id="override-status-banner" class="empty" style="margin-bottom:12px;">
      Checking server status...
    </div>

    <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-bottom:14px;">
      <input id="override-ticker" type="text" placeholder="Ticker (e.g. GME)"
             autocomplete="off" maxlength="10"
             style="background:var(--bg-elev); color:var(--fg); border:1px solid var(--border);
                    padding:10px 14px; border-radius:6px; font-size:14px; width:140px;
                    text-transform: uppercase; font-weight: 600;">

      <select id="override-side"
              style="background:var(--bg-elev); color:var(--fg); border:1px solid var(--border);
                     padding:10px 8px; border-radius:6px; font-size:13px;">
        <option value="">Auto (best of call/put)</option>
        <option value="call">Force CALL</option>
        <option value="put">Force PUT</option>
      </select>

      <input id="override-maxcost" type="number" placeholder="Max $/contract" min="10" max="2000"
             style="background:var(--bg-elev); color:var(--fg); border:1px solid var(--border);
                    padding:10px 12px; border-radius:6px; font-size:13px; width:140px;">

      <input id="override-tag" type="text" placeholder="Tag (e.g. mine)" maxlength="10"
             style="background:var(--bg-elev); color:var(--fg); border:1px solid var(--border);
                    padding:10px 12px; border-radius:6px; font-size:13px; width:130px;">

      <label style="display:flex; gap:6px; align-items:center; font-size:13px; cursor:pointer;">
        <input type="checkbox" id="override-live" style="cursor:pointer;">
        <span class="warn-txt">Live submit</span>
      </label>

      <button class="btn primary" id="override-submit">Analyze + Buy</button>
    </div>

    <div id="override-result" style="margin-bottom:18px;"></div>

    <div class="section-title"><span class="dot"></span> Recent override attempts</div>
    <div id="override-recent" class="empty">Loading recent results…</div>
  </section>
</div>

<!-- TRADES -->
<div class="tab-content" id="tab-trades">
  <section>
    <div class="section-title"><span class="dot"></span> All paper trade attempts (30 days)</div>
    <div class="table-wrap" id="all-trades-wrap"></div>
  </section>
</div>

</div><!-- /page -->

<script>
const DATA = __DATA__;

function fmtMoney(n) {
  if (n == null) return '—';
  return '$' + Number(n).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
}
function pnlClass(n) { if (!n) return ''; return n > 0 ? 'pos' : 'neg'; }
function pctText(n) {
  if (n == null) return '';
  const sign = n > 0 ? '+' : '';
  return `${sign}${Number(n).toFixed(2)}%`;
}

// ── Subtitle
document.getElementById('subtitle').textContent =
  `Generated ${new Date(DATA.generated).toLocaleString()}  ·  ${DATA.today}  ·  Log: ${DATA.trades_last_30d} entries (30d)`;

// ── Auto-refresh (persists to localStorage)
(function initAutoRefresh() {
  const cb = document.getElementById('auto-refresh-cb');
  const sel = document.getElementById('auto-refresh-sec');
  const savedOn = localStorage.getItem('optionsEdge.autoRefresh') === '1';
  const savedSec = localStorage.getItem('optionsEdge.autoRefreshSec') || '60';
  cb.checked = savedOn;
  sel.value = savedSec;
  let timer = null;
  function apply() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (cb.checked) {
      const ms = parseInt(sel.value, 10) * 1000;
      timer = setTimeout(() => location.reload(), ms);
    }
    localStorage.setItem('optionsEdge.autoRefresh', cb.checked ? '1' : '0');
    localStorage.setItem('optionsEdge.autoRefreshSec', sel.value);
  }
  cb.addEventListener('change', apply);
  sel.addEventListener('change', apply);
  apply();
})();

// ── Tab switching
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'analysis') initAnalysis();
    if (tab.dataset.tab === 'trades') initTrades();
    if (tab.dataset.tab === 'lookup') initLookup();
    if (tab.dataset.tab === 'override') initOverride();
  });
});

// ── OVERVIEW TAB ────────────────────────────────────────────────────────────

function renderOverview() {
  // Account cards
  const acc = DATA.broker || {};
  const accEl = document.getElementById('account-cards');
  if (acc.connected) {
    document.getElementById('ov-account-sub').textContent = acc.paper ? 'PAPER' : 'LIVE';
    accEl.innerHTML = `
      <div class="stat-card"><div class="label">Account</div>
        <div class="value mono" style="font-size:15px;">${acc.account_number || '—'}</div>
        <div class="sub">${acc.blocked ? 'BLOCKED' : 'Active'}</div></div>
      <div class="stat-card"><div class="label">Equity</div>
        <div class="value">${fmtMoney(acc.equity)}</div></div>
      <div class="stat-card"><div class="label">Cash</div>
        <div class="value">${fmtMoney(acc.cash)}</div></div>
      <div class="stat-card"><div class="label">Buying power</div>
        <div class="value">${fmtMoney(acc.buying_power)}</div></div>
    `;
  } else {
    accEl.innerHTML = `<div class="stat-card"><div class="label">Broker</div><div class="value warn-txt">Disconnected</div><div class="sub">${acc.error || ''}</div></div>`;
  }

  // Snapshot summary cards
  const snap = DATA.snap_trades || [];
  const sb = DATA.signal_breakdown || {by_signal: {}, by_type: {}};
  const topScore = snap.length ? Math.max(...snap.map(t => Number(t.score || 0))) : 0;
  const avgScore = snap.length ? (snap.reduce((s, t) => s + Number(t.score || 0), 0) / snap.length) : 0;
  const fallbackNote = DATA.snap_fallback_date ? `Using ${DATA.snap_fallback_date} (no today scan)` : '';
  document.getElementById('snap-count').textContent = DATA.snap_total || snap.length;
  document.getElementById('snap-summary-cards').innerHTML = `
    <div class="stat-card"><div class="label">Candidates</div>
      <div class="value">${DATA.snap_total ?? snap.length}</div>
      ${fallbackNote ? `<div class="sub warn-txt">${fallbackNote}</div>` : ''}</div>
    <div class="stat-card"><div class="label">Top score</div>
      <div class="value">${topScore.toFixed(1)}</div></div>
    <div class="stat-card"><div class="label">Avg score</div>
      <div class="value">${avgScore.toFixed(1)}</div></div>
    <div class="stat-card"><div class="label">Calls / Puts</div>
      <div class="value">${(sb.by_type.call || 0)} / ${(sb.by_type.put || 0)}</div></div>
  `;

  // Positions
  const positions = DATA.positions || [];
  document.getElementById('pos-count').textContent = positions.length;
  const posWrap = document.getElementById('positions-wrap');
  if (!positions.length) {
    posWrap.innerHTML = '<div class="empty">No open positions.</div>';
  } else {
    let h = `<div class="table-wrap"><table><thead><tr>
      <th>Symbol</th><th>Qty</th><th>Entry</th><th>Mark</th><th>Value</th>
      <th>Unrealized</th><th>%</th></tr></thead><tbody>`;
    positions.forEach(p => {
      h += `<tr>
        <td class="mono">${p.symbol}</td>
        <td>${p.qty}</td>
        <td>${fmtMoney(p.avg_entry)}</td>
        <td>${fmtMoney(p.mark)}</td>
        <td>${fmtMoney(p.market_value)}</td>
        <td class="${pnlClass(p.unrealized_pl)}">${fmtMoney(p.unrealized_pl)}</td>
        <td class="${pnlClass(p.unrealized_pl)}">${pctText(p.unrealized_pl_pct)}</td>
      </tr>`;
    });
    h += '</tbody></table></div>';
    posWrap.innerHTML = h;
  }

  // Tier cards
  const tiers = DATA.tier_stats || {};
  const tierNames = Object.keys(tiers).sort();
  const tierEl = document.getElementById('tier-grid');
  if (!tierNames.length) {
    tierEl.innerHTML = '<div class="empty">No paper trades recorded yet. Starts tomorrow at 9:35 AM.</div>';
  } else {
    tierEl.innerHTML = tierNames.map(name => {
      const t = tiers[name];
      const iconLetter = name.replace(/\D/g, '').slice(0, 2) || name.slice(0, 2).toUpperCase();
      const rows = (t.orders || []).slice(0, 6).map(o => {
        const cls = o.status === 'submitted' ? 'ok' :
                    o.status === 'skipped' ? 'warn' :
                    o.status === 'failed' ? 'err' : '';
        return `<tr>
          <td><span class="badge ${cls}">${o.status || '?'}</span></td>
          <td>${o.symbol || ''} ${(o.option_type || '').charAt(0).toUpperCase()}</td>
          <td>$${o.strike || '—'}</td>
          <td>${fmtMoney(o.total_cost)}</td>
        </tr>`;
      }).join('');
      return `<div class="tier-card">
        <h3><span class="tier-icon">${iconLetter}</span>${name}</h3>
        <div class="tier-stats">
          <div class="tier-stat"><div class="n">${t.submitted}</div><div class="l">Submitted</div></div>
          <div class="tier-stat"><div class="n">${t.skipped}</div><div class="l">Skipped</div></div>
          <div class="tier-stat"><div class="n">${t.failed}</div><div class="l">Failed</div></div>
          <div class="tier-stat"><div class="n">${fmtMoney(t.deployed)}</div><div class="l">Deployed</div></div>
        </div>
        ${rows ? `<table style="font-size:11px;"><thead><tr><th>Status</th><th>Contract</th><th>Strike</th><th>Cost</th></tr></thead><tbody>${rows}</tbody></table>` : '<div class="empty" style="padding:10px;">No attempts</div>'}
      </div>`;
    }).join('');
  }

  // Alerts
  const alerts = DATA.alerts || [];
  document.getElementById('alert-count').textContent = alerts.length;
  const alertsWrap = document.getElementById('alerts-wrap');
  if (!alerts.length) {
    alertsWrap.innerHTML = '<div class="empty">No recent alerts.</div>';
  } else {
    alertsWrap.innerHTML = alerts.slice(0, 10).map(a => {
      const sev = (a.severity || 'ERROR').toUpperCase();
      const cls = sev === 'INFO' ? 'info' : sev === 'WARN' ? 'warn' : '';
      const ts = (a.timestamp || '').slice(0, 19).replace('T', ' ');
      return `<div class="alert-card ${cls}">
        <div class="alert-meta"><span class="badge ${cls || 'err'}">${sev}</span> ${a.source || '?'} · ${ts} ${a.symbol ? '· ' + a.symbol : ''}</div>
        <div class="alert-msg">${(a.message || '').replace(/[<>]/g, '')}</div>
      </div>`;
    }).join('');
  }
}

// ── ANALYSIS TAB ────────────────────────────────────────────────────────────

let _analysisReady = false;
function initAnalysis() {
  if (_analysisReady) return;
  _analysisReady = true;

  // Score distribution
  const hist = DATA.score_histogram || {};
  const histLabels = Object.keys(hist);
  const histValues = histLabels.map(k => hist[k]);
  const total = histValues.reduce((a, b) => a + b, 0);
  if (total > 0) {
    new Chart(document.getElementById('scoreChart').getContext('2d'), {
      type: 'bar',
      data: { labels: histLabels, datasets: [{
        label: 'Candidates', data: histValues,
        backgroundColor: 'rgba(122,162,247,0.6)',
        borderColor: 'rgba(122,162,247,1)', borderWidth: 1,
      }]},
      options: {
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#8b93a7' }, grid: { color: '#2a2f3e' } },
          y: { ticks: { color: '#8b93a7', precision: 0 }, grid: { color: '#2a2f3e' }, beginAtZero: true },
        },
      },
    });
  } else {
    document.getElementById('scoreChart').parentElement.innerHTML =
      '<div class="empty">No snapshot data to chart.</div>';
  }

  // Snapshot top table
  const snap = DATA.snap_trades || [];
  const tblWrap = document.getElementById('snap-table-wrap');
  if (!snap.length) {
    tblWrap.outerHTML = '<div class="empty">No snapshot available.</div>';
  } else {
    let h = `<table><thead><tr>
      <th>#</th><th>Score</th><th>Sym</th><th>Type</th><th>Strike</th>
      <th>Expiry</th><th>DTE</th><th>Entry</th><th>Signal</th>
    </tr></thead><tbody>`;
    snap.slice(0, 30).forEach((t, i) => {
      h += `<tr>
        <td>${i+1}</td>
        <td><strong>${Number(t.score || 0).toFixed(1)}</strong></td>
        <td><strong>${t.symbol || ''}</strong></td>
        <td>${(t.option_type || '').toUpperCase()}</td>
        <td>$${t.strike || '—'}</td>
        <td class="mono">${t.expiry || ''}</td>
        <td>${t.dte || '—'}</td>
        <td>${fmtMoney(t.entry_price)}</td>
        <td><span class="badge">${t.vol_signal || ''}</span></td>
      </tr>`;
    });
    h += '</tbody></table>';
    tblWrap.innerHTML = h;
  }

  // Deployed capital chart
  const pnl = DATA.deployed_history || {};
  if (pnl.days && pnl.days.length && pnl.tiers.length) {
    const palette = ['#7aa2f7', '#bb9af7', '#9ece6a', '#e0af68', '#f7768e'];
    new Chart(document.getElementById('pnlChart').getContext('2d'), {
      type: 'line',
      data: {
        labels: pnl.days,
        datasets: pnl.tiers.map((tier, i) => ({
          label: tier,
          data: pnl.series[tier],
          borderColor: palette[i % palette.length],
          backgroundColor: palette[i % palette.length] + '22',
          tension: 0.25, pointRadius: 3, borderWidth: 2,
        })),
      },
      options: {
        responsive: true,
        plugins: { legend: { labels: { color: '#e8ebf4' } } },
        scales: {
          x: { ticks: { color: '#8b93a7' }, grid: { color: '#2a2f3e' } },
          y: { ticks: { color: '#8b93a7', callback: v => '$' + v }, grid: { color: '#2a2f3e' }, beginAtZero: true },
        },
      },
    });
  } else {
    document.querySelectorAll('#tab-analysis .chart-card').forEach((el, idx) => {
      if (idx === 1) el.innerHTML = '<div class="empty">No trade history yet — builds as the scheduled runs log data.</div>';
    });
  }

  // Ticker live charts
  initTickerCharts();
}

// Indicator helpers: compute VWAP, SMA, Bollinger Bands, RSI
// All take an array of bars [{t, o, h, l, c, v}]; return aligned point arrays.

function calcSMA(bars, window) {
  const out = [];
  let sum = 0;
  for (let i = 0; i < bars.length; i++) {
    sum += bars[i].c;
    if (i >= window) sum -= bars[i - window].c;
    out.push(i >= window - 1 ? { x: bars[i].t, y: sum / window } : { x: bars[i].t, y: null });
  }
  return out;
}

function calcVWAP(bars) {
  // Typical price * volume, cumulative
  const out = [];
  let cumPV = 0, cumV = 0;
  let lastDay = null;
  for (const b of bars) {
    const day = (b.t || '').slice(0, 10);
    if (day !== lastDay) { cumPV = 0; cumV = 0; lastDay = day; }
    const tp = (b.h + b.l + b.c) / 3;
    cumPV += tp * (b.v || 0);
    cumV += (b.v || 0);
    out.push({ x: b.t, y: cumV > 0 ? cumPV / cumV : null });
  }
  return out;
}

function calcBollinger(bars, window, mult) {
  const out = { upper: [], mid: [], lower: [] };
  for (let i = 0; i < bars.length; i++) {
    if (i < window - 1) {
      out.upper.push({ x: bars[i].t, y: null });
      out.mid.push({   x: bars[i].t, y: null });
      out.lower.push({ x: bars[i].t, y: null });
      continue;
    }
    let sum = 0;
    for (let j = i - window + 1; j <= i; j++) sum += bars[j].c;
    const mean = sum / window;
    let sqSum = 0;
    for (let j = i - window + 1; j <= i; j++) sqSum += (bars[j].c - mean) ** 2;
    const sd = Math.sqrt(sqSum / window);
    out.upper.push({ x: bars[i].t, y: mean + mult * sd });
    out.mid.push({   x: bars[i].t, y: mean });
    out.lower.push({ x: bars[i].t, y: mean - mult * sd });
  }
  return out;
}

function calcRSI(bars, period) {
  const out = [];
  if (bars.length < period + 1) {
    return bars.map(b => ({ x: b.t, y: null }));
  }
  let gain = 0, loss = 0;
  for (let i = 1; i <= period; i++) {
    const d = bars[i].c - bars[i - 1].c;
    if (d > 0) gain += d; else loss -= d;
  }
  gain /= period; loss /= period;
  out.push(...bars.slice(0, period).map(b => ({ x: b.t, y: null })));
  out.push({ x: bars[period].t, y: loss === 0 ? 100 : 100 - (100 / (1 + gain / loss)) });
  for (let i = period + 1; i < bars.length; i++) {
    const d = bars[i].c - bars[i - 1].c;
    const g = d > 0 ? d : 0;
    const l = d < 0 ? -d : 0;
    gain = (gain * (period - 1) + g) / period;
    loss = (loss * (period - 1) + l) / period;
    out.push({ x: bars[i].t, y: loss === 0 ? 100 : 100 - (100 / (1 + gain / loss)) });
  }
  return out;
}

// Per-chart state: { sym -> { tf, indicators: Set, showExt: bool } }
const _chartState = {};
const _chartInstances = {};

function initTickerCharts() {
  const tickers = DATA.snap_tickers || [];  // Analysis shows snap tickers only
  const bars = DATA.ticker_bars || {};
  const grid = document.getElementById('ticker-charts');
  document.getElementById('ticker-count').textContent = tickers.length;

  if (!tickers.length) {
    grid.innerHTML = '<div class="empty">No tickers in today\'s snapshot.</div>';
    return;
  }

  grid.innerHTML = '';
  tickers.forEach(sym => {
    const data = bars[sym] || {};
    const card = document.createElement('div');
    card.className = 'ticker-chart-card';
    card.dataset.sym = sym;

    const intraday = data.intraday || [];
    const last = data.last;

    // Day change from today's regular-session bars
    let change = null, changePct = null;
    if (intraday.length >= 2) {
      const todayStr = intraday[intraday.length - 1].t.slice(0, 10);
      const regBars = intraday.filter(b => b.t.slice(0, 10) === todayStr && b.s === 'regular');
      if (regBars.length >= 2) {
        const openPx = regBars[0].o;
        change = regBars[regBars.length - 1].c - openPx;
        changePct = (change / openPx) * 100;
      }
    }
    const changeHtml = change != null
      ? `<span class="change ${change > 0 ? 'pos' : 'neg'}">${change > 0 ? '+' : ''}${change.toFixed(2)} (${changePct > 0 ? '+' : ''}${changePct.toFixed(2)}%)</span>`
      : '';

    _chartState[sym] = { tf: '1D', indicators: new Set(), showExt: false };

    card.innerHTML = `
      <div class="ticker-head">
        <div>
          <span class="sym">${sym}</span>
          <span class="last">${last != null ? '$' + last.toFixed(2) : '—'}</span>
          ${changeHtml}
        </div>
        <div class="tf-toggles">
          <button class="tf-btn active" data-tf="1D">1D</button>
          <button class="tf-btn" data-tf="5D">5D</button>
          <button class="tf-btn" data-tf="1M">1M</button>
          <button class="tf-btn" data-tf="3M">3M</button>
          <button class="tf-btn" data-tf="1Y">1Y</button>
        </div>
      </div>
      <div class="indicator-bar">
        <button class="ind-btn" data-ind="vwap">VWAP</button>
        <button class="ind-btn" data-ind="ma20">MA20</button>
        <button class="ind-btn" data-ind="ma50">MA50</button>
        <button class="ind-btn" data-ind="boll">BOLL</button>
        <button class="ind-btn" data-ind="rsi">RSI</button>
        <button class="ind-btn ext" data-ind="ext">Pre/After</button>
      </div>
      <canvas id="chart-${sym}"></canvas>
    `;
    grid.appendChild(card);

    // Wire timeframe buttons
    card.querySelectorAll('.tf-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        card.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        _chartState[sym].tf = btn.dataset.tf;
        renderTickerChart(sym, `chart-${sym}`, _chartState[sym]);
      });
    });
    // Wire indicator buttons
    card.querySelectorAll('.ind-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const k = btn.dataset.ind;
        btn.classList.toggle('active');
        if (k === 'ext') {
          _chartState[sym].showExt = btn.classList.contains('active');
        } else {
          if (btn.classList.contains('active')) _chartState[sym].indicators.add(k);
          else _chartState[sym].indicators.delete(k);
        }
        renderTickerChart(sym, `chart-${sym}`, _chartState[sym]);
      });
    });

    // Initial render
    renderTickerChart(sym, `chart-${sym}`, _chartState[sym]);
  });
}

function _sliceBarsForTF(sym, tf, showExt) {
  const bars = (DATA.ticker_bars || {})[sym] || {};
  const intraday = bars.intraday || [];
  const daily = bars.daily || [];

  let filteredIntra = showExt ? intraday : intraday.filter(b => b.s === 'regular');

  if (tf === '1D' && filteredIntra.length) {
    const lastDay = filteredIntra[filteredIntra.length - 1].t.slice(0, 10);
    return filteredIntra.filter(b => b.t.slice(0, 10) === lastDay);
  } else if (tf === '5D' && filteredIntra.length) {
    return filteredIntra;
  } else if (tf === '1M' && daily.length) {
    return daily.slice(-22);
  } else if (tf === '3M' && daily.length) {
    return daily.slice(-65);
  } else if (tf === '1Y' && daily.length) {
    return daily;
  }
  return [];
}

function renderTickerChart(sym, canvasId, state) {
  const bars = _sliceBarsForTF(sym, state.tf, state.showExt);
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  if (_chartInstances[canvasId]) { _chartInstances[canvasId].destroy(); }

  if (!bars.length) {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#8b93a7'; ctx.font = '12px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(`No ${state.tf} data`, canvas.width / 2, 60);
    return;
  }

  const pricePoints = bars.map(b => ({ x: b.t, y: b.c }));
  const color = pricePoints[0].y > pricePoints[pricePoints.length - 1].y ? '#f7768e' : '#9ece6a';

  const datasets = [{
    label: sym,
    data: pricePoints,
    borderColor: color,
    backgroundColor: color + '20',
    tension: 0.2, pointRadius: 0, fill: true, borderWidth: 2,
    yAxisID: 'y',
  }];

  const ind = state.indicators;
  if (ind.has('vwap')) {
    datasets.push({
      label: 'VWAP', data: calcVWAP(bars),
      borderColor: '#e0af68', borderWidth: 1.5, pointRadius: 0,
      fill: false, borderDash: [4, 3], yAxisID: 'y',
    });
  }
  if (ind.has('ma20')) {
    datasets.push({
      label: 'MA20', data: calcSMA(bars, 20),
      borderColor: '#7dcfff', borderWidth: 1.5, pointRadius: 0, fill: false, yAxisID: 'y',
    });
  }
  if (ind.has('ma50')) {
    datasets.push({
      label: 'MA50', data: calcSMA(bars, 50),
      borderColor: '#bb9af7', borderWidth: 1.5, pointRadius: 0, fill: false, yAxisID: 'y',
    });
  }
  if (ind.has('boll')) {
    const b = calcBollinger(bars, 20, 2);
    datasets.push({ label: 'BB Upper', data: b.upper, borderColor: '#9ece6a80', borderWidth: 1, pointRadius: 0, fill: false, yAxisID: 'y' });
    datasets.push({ label: 'BB Mid',   data: b.mid,   borderColor: '#9ece6a', borderWidth: 1, pointRadius: 0, fill: false, borderDash: [3, 3], yAxisID: 'y' });
    datasets.push({ label: 'BB Lower', data: b.lower, borderColor: '#9ece6a80', borderWidth: 1, pointRadius: 0, fill: false, yAxisID: 'y' });
  }

  // RSI on a separate axis (right)
  const useRSI = ind.has('rsi');
  if (useRSI) {
    datasets.push({
      label: 'RSI', data: calcRSI(bars, 14),
      borderColor: '#f7768e', borderWidth: 1.5, pointRadius: 0, fill: false, yAxisID: 'yRSI',
    });
  }

  const scales = {
    x: {
      type: 'time',
      time: { unit: state.tf === '1D' ? 'hour' : 'day' },
      ticks: { color: '#8b93a7', maxTicksLimit: 6 },
      grid: { color: '#2a2f3e' },
    },
    y: {
      ticks: { color: '#8b93a7', callback: v => '$' + Number(v).toFixed(2) },
      grid: { color: '#2a2f3e' },
    },
  };
  if (useRSI) {
    scales.yRSI = {
      position: 'right', min: 0, max: 100,
      ticks: { color: '#f7768e' }, grid: { drawOnChartArea: false },
    };
  }

  _chartInstances[canvasId] = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      animation: { duration: 200 },
      plugins: {
        legend: { labels: { color: '#e8ebf4', font: { size: 10 } }, display: ind.size > 0 },
        tooltip: { mode: 'index', intersect: false },
      },
      scales,
    },
  });
}

// ── TICKER LOOKUP TAB ───────────────────────────────────────────────────────
let _lookupReady = false;
let _lookupState = { sym: null, tf: '1D', indicators: new Set(), showExt: false };

function initLookup() {
  if (_lookupReady) return;
  _lookupReady = true;

  // Quick buttons for today's tickers
  const snap = DATA.snap_tickers || [];
  const benchmarks = DATA.benchmarks || [];
  const qb = document.getElementById('lookup-quick-buttons');
  qb.innerHTML = '<div class="subtitle" style="margin-bottom:4px;">Today\'s highlighted tickers + benchmarks:</div>' +
    [...snap, ...benchmarks].map(s =>
      `<button class="quick-btn" data-sym="${s}">${s}</button>`
    ).join('');

  qb.querySelectorAll('.quick-btn').forEach(btn => {
    btn.addEventListener('click', () => selectLookup(btn.dataset.sym));
  });

  // Watchlist (localStorage)
  renderWatchlistButtons();

  // Manual text search
  const inp = document.getElementById('lookup-search');
  const go = document.getElementById('lookup-go');
  const submit = () => { const s = (inp.value || '').trim().toUpperCase(); if (s) selectLookup(s); };
  go.addEventListener('click', submit);
  inp.addEventListener('keydown', e => { if (e.key === 'Enter') submit(); });

  // Watchlist add
  const addInp = document.getElementById('lookup-wl-add');
  const addBtn = document.getElementById('lookup-wl-add-btn');
  const doAdd = () => {
    const s = (addInp.value || '').trim().toUpperCase();
    if (!s) return;
    const wl = loadBrowserWatchlist();
    if (!wl.includes(s)) wl.push(s);
    saveBrowserWatchlist(wl);
    addInp.value = '';
    renderWatchlistButtons();
  };
  addBtn.addEventListener('click', doAdd);
  addInp.addEventListener('keydown', e => { if (e.key === 'Enter') doAdd(); });

  // Auto-select first snap ticker if any
  if (snap.length) selectLookup(snap[0]);
}

function loadBrowserWatchlist() {
  try {
    const raw = localStorage.getItem('optionsEdge.watchlist');
    return raw ? JSON.parse(raw) : [];
  } catch (_) { return []; }
}
function saveBrowserWatchlist(list) {
  try { localStorage.setItem('optionsEdge.watchlist', JSON.stringify(list)); } catch (_) {}
}
function renderWatchlistButtons() {
  const wl = loadBrowserWatchlist();
  const div = document.getElementById('lookup-watchlist-buttons');
  if (!wl.length) {
    div.innerHTML = '<div class="subtitle">Your watchlist is empty. Add tickers below.</div>';
    return;
  }
  div.innerHTML = '<div class="subtitle" style="margin-bottom:4px;">Your watchlist:</div>' +
    wl.map(s =>
      `<button class="quick-btn watch" data-sym="${s}">${s}<span class="rm" data-rm="${s}" title="Remove">×</span></button>`
    ).join('');
  div.querySelectorAll('.quick-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      if (e.target.classList.contains('rm')) {
        const sym = e.target.dataset.rm;
        saveBrowserWatchlist(loadBrowserWatchlist().filter(x => x !== sym));
        renderWatchlistButtons();
      } else {
        selectLookup(btn.dataset.sym);
      }
    });
  });
}

function selectLookup(sym) {
  _lookupState.sym = sym;
  document.getElementById('lookup-sym').textContent = sym;
  document.getElementById('lookup-chart-section').style.display = '';
  document.getElementById('lookup-news-section').style.display = '';

  const bars = (DATA.ticker_bars || {})[sym] || {};
  const last = bars.last;
  document.getElementById('lookup-last').textContent = last != null ? `$${last.toFixed(2)}` : '—';

  // Day change
  const intra = bars.intraday || [];
  let change = null, pct = null;
  if (intra.length) {
    const lastDay = intra[intra.length - 1].t.slice(0, 10);
    const todayReg = intra.filter(b => b.t.slice(0, 10) === lastDay && b.s === 'regular');
    if (todayReg.length >= 2) {
      change = todayReg[todayReg.length - 1].c - todayReg[0].o;
      pct = (change / todayReg[0].o) * 100;
    }
  }
  const ce = document.getElementById('lookup-change');
  if (change != null) {
    ce.className = `change ${change > 0 ? 'pos' : 'neg'}`;
    ce.textContent = `${change > 0 ? '+' : ''}${change.toFixed(2)} (${pct > 0 ? '+' : ''}${pct.toFixed(2)}%)`;
  } else {
    ce.textContent = '';
  }

  // Build indicator bar if not built
  const bar = document.getElementById('lookup-ind-bar');
  if (!bar.dataset.built) {
    bar.innerHTML = `
      <button class="ind-btn" data-ind="vwap">VWAP</button>
      <button class="ind-btn" data-ind="ma20">MA20</button>
      <button class="ind-btn" data-ind="ma50">MA50</button>
      <button class="ind-btn" data-ind="boll">BOLL</button>
      <button class="ind-btn" data-ind="rsi">RSI</button>
      <button class="ind-btn ext" data-ind="ext">Pre/After</button>
    `;
    bar.dataset.built = '1';
    bar.querySelectorAll('.ind-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const k = btn.dataset.ind;
        btn.classList.toggle('active');
        if (k === 'ext') {
          _lookupState.showExt = btn.classList.contains('active');
        } else {
          if (btn.classList.contains('active')) _lookupState.indicators.add(k);
          else _lookupState.indicators.delete(k);
        }
        renderLookupChart();
      });
    });
    document.querySelectorAll('#lookup-tf-toggles .tf-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('#lookup-tf-toggles .tf-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        _lookupState.tf = btn.dataset.tf;
        renderLookupChart();
      });
    });
  }

  renderLookupChart();
  renderLookupNews(sym);
}

function renderLookupChart() {
  const sym = _lookupState.sym;
  if (!sym) return;
  const bars = (DATA.ticker_bars || {})[sym];
  if (!bars) {
    document.getElementById('lookup-main-chart-wrap').insertAdjacentHTML('beforeend',
      `<div class="empty" id="no-data">No Alpaca data for ${sym}. Add it to watchlist.json and rebuild.</div>`);
    return;
  }
  renderTickerChart(sym, 'lookup-chart', _lookupState);
}

function renderLookupNews(sym) {
  document.getElementById('lookup-news-sym').textContent = sym;
  const news = (DATA.ticker_news || {})[sym] || [];
  document.getElementById('lookup-news-count').textContent = news.length;
  const wrap = document.getElementById('lookup-news-wrap');
  if (!news.length) {
    wrap.innerHTML = `<div class="empty">No recent news for ${sym}. (Only snapshot + watchlist tickers have pre-fetched news.)</div>`;
    return;
  }
  wrap.innerHTML = news.map(a => {
    let cls = '';
    if (typeof a.sentiment === 'number') {
      if (a.sentiment > 0.15) cls = 'pos';
      else if (a.sentiment < -0.15) cls = 'neg';
    }
    const sentBadge = typeof a.sentiment === 'number'
      ? `<span class="badge ${cls === 'pos' ? 'ok' : cls === 'neg' ? 'err' : 'info'}">${a.sentiment.toFixed(2)}</span>`
      : '';
    const time = a.published ? new Date(a.published).toLocaleString() : '';
    const link = a.link
      ? `<a href="${a.link}" target="_blank" rel="noopener">${a.title}</a>`
      : `<span>${a.title}</span>`;
    const summary = a.summary ? `<div class="summary">${a.summary}</div>` : '';
    return `<div class="news-item ${cls}">
      ${link}
      <div class="meta">${a.source || 'rss'} · ${time} ${sentBadge}</div>
      ${summary}
    </div>`;
  }).join('');
}

// ── TRADES TAB ──────────────────────────────────────────────────────────────

let _tradesReady = false;
function initTrades() {
  if (_tradesReady) return;
  _tradesReady = true;

  // Flatten all tier orders
  const tiers = DATA.tier_stats || {};
  const all = [];
  Object.keys(tiers).forEach(name => {
    (tiers[name].orders || []).forEach(o => all.push({ tier: name, ...o }));
  });
  all.sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));

  const wrap = document.getElementById('all-trades-wrap');
  if (!all.length) {
    wrap.outerHTML = '<div class="empty">No trade attempts logged yet.</div>';
    return;
  }
  let h = `<table><thead><tr>
    <th>Time</th><th>Tier</th><th>Status</th><th>Symbol</th><th>Type</th>
    <th>Strike</th><th>Exp</th><th>Cost</th><th>Score</th><th>Client Order ID</th>
  </tr></thead><tbody>`;
  all.forEach(o => {
    const cls = o.status === 'submitted' ? 'ok' :
                o.status === 'skipped' ? 'warn' :
                o.status === 'failed' ? 'err' : '';
    h += `<tr>
      <td class="mono">${(o.timestamp || '').slice(11, 19)}</td>
      <td><span class="badge info">${o.tier || ''}</span></td>
      <td><span class="badge ${cls}">${o.status || '?'}</span></td>
      <td><strong>${o.symbol || ''}</strong></td>
      <td>${(o.option_type || '').toUpperCase()}</td>
      <td>$${o.strike || '—'}</td>
      <td class="mono">${o.expiry || ''}</td>
      <td>${fmtMoney(o.total_cost)}</td>
      <td>${Number(o.score || 0).toFixed(0)}</td>
      <td class="mono" style="font-size:10px;">${o.client_order_id || ''}</td>
    </tr>`;
  });
  h += '</tbody></table>';
  wrap.innerHTML = h;
}

// ── MANUAL OVERRIDE BUY ─────────────────────────────────────────────────────
const OVERRIDE_BASE = 'http://127.0.0.1:8503';
let _overrideReady = false;

function initOverride() {
  if (!_overrideReady) {
    _overrideReady = true;
    document.getElementById('override-submit').addEventListener('click', submitOverride);
    document.getElementById('override-ticker').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') submitOverride();
    });
    document.getElementById('override-ticker').addEventListener('input', (e) => {
      e.target.value = (e.target.value || '').toUpperCase();
    });
  }
  pingOverrideServer();
  loadRecentOverrides();
}

async function pingOverrideServer() {
  const banner = document.getElementById('override-status-banner');
  try {
    const r = await fetch(`${OVERRIDE_BASE}/health`, { method: 'GET',
                                                       cache: 'no-store' });
    if (r.ok) {
      const d = await r.json();
      banner.innerHTML = `<span class="badge ok">ONLINE</span> Override server reachable on port ${d.port}.`;
      banner.className = '';
    } else {
      banner.innerHTML = `<span class="badge err">DOWN</span> Server returned ${r.status}.`;
      banner.className = '';
    }
  } catch (err) {
    banner.innerHTML = `<span class="badge err">OFFLINE</span> Override server not reachable. Start it: <code>tools\\override_server.bat</code> or via Task Scheduler (OptionsEdge-OverrideServer).`;
    banner.className = '';
  }
}

function _safe(v) {
  if (v === null || v === undefined) return '—';
  return String(v);
}

async function loadRecentOverrides() {
  const wrap = document.getElementById('override-recent');
  try {
    const r = await fetch(`${OVERRIDE_BASE}/recent?limit=10`, {
        method: 'GET', cache: 'no-store' });
    if (!r.ok) {
      wrap.innerHTML = `<div class="empty">Server returned ${r.status}.</div>`;
      return;
    }
    const data = await r.json();
    const results = data.results || [];
    if (!results.length) {
      wrap.innerHTML = `<div class="empty">No override attempts yet.</div>`;
      return;
    }
    let h = '<div class="table-wrap"><table><thead><tr>'
      + '<th>Time</th><th>Ticker</th><th>Status</th><th>Contract</th>'
      + '<th>Cost</th><th>Score</th><th>Reason</th></tr></thead><tbody>';
    for (const r of results) {
      const c = r.contract || {};
      const status = r.status || '?';
      const cls = status === 'submitted' ? 'ok'
                 : status === 'dry_run' ? 'info'
                 : status === 'skipped' ? 'warn' : 'err';
      const ts = (r.timestamp || '').slice(11, 19);
      const contractDesc = c.symbol
        ? `${c.symbol} ${(c.type || '').toUpperCase()} $${c.strike} ${c.expiry}`
        : '—';
      h += `<tr>
        <td class="mono">${ts}</td>
        <td><strong>${_safe(r.ticker)}</strong></td>
        <td><span class="badge ${cls}">${status}</span></td>
        <td class="mono">${contractDesc}</td>
        <td>${r.total_cost ? '$' + Number(r.total_cost).toFixed(2) : '—'}</td>
        <td>${c.score ? Number(c.score).toFixed(1) : '—'}</td>
        <td class="small">${_safe(r.reason || r.note || '').slice(0, 80)}</td>
      </tr>`;
    }
    h += '</tbody></table></div>';
    wrap.innerHTML = h;
  } catch (err) {
    wrap.innerHTML = `<div class="empty">Could not reach server: ${err}</div>`;
  }
}

async function submitOverride() {
  const tickerEl = document.getElementById('override-ticker');
  const sideEl   = document.getElementById('override-side');
  const maxEl    = document.getElementById('override-maxcost');
  const tagEl    = document.getElementById('override-tag');
  const liveEl   = document.getElementById('override-live');
  const result   = document.getElementById('override-result');
  const submitBtn = document.getElementById('override-submit');

  const ticker = (tickerEl.value || '').trim().toUpperCase();
  if (!ticker) {
    result.innerHTML = '<div class="alert-card warn">Enter a ticker first.</div>';
    return;
  }
  const body = {
    ticker: ticker,
    side: sideEl.value || null,
    tag: tagEl.value || 'manual',
    live: !!liveEl.checked,
    max_cost: maxEl.value ? Number(maxEl.value) : null,
    min_score: 40.0,
  };

  submitBtn.disabled = true;
  submitBtn.textContent = 'Analyzing…';
  result.innerHTML = `<div class="alert-card info">Running analyze_ticker(${ticker}). This takes 30-90s on first run.</div>`;

  try {
    const r = await fetch(`${OVERRIDE_BASE}/override`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    renderOverrideResult(data);
    loadRecentOverrides();
  } catch (err) {
    result.innerHTML = `<div class="alert-card err">Server call failed: ${err}</div>`;
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = 'Analyze + Buy';
  }
}

function renderOverrideResult(d) {
  const result = document.getElementById('override-result');
  const status = d.status || '?';
  const cls = status === 'submitted' ? 'ok'
             : status === 'dry_run' ? 'info'
             : status === 'skipped' ? 'warn' : 'err';
  const c = d.contract || {};

  let html = `<div class="card" style="border-left:3px solid var(--${cls === 'ok' ? 'ok' : cls === 'info' ? 'info' : cls === 'warn' ? 'warn' : 'err'});">
    <div style="display:flex; justify-content:space-between; align-items:baseline;">
      <div><strong>${d.ticker}</strong> &nbsp; <span class="badge ${cls}">${status}</span></div>
      <div class="small">${(d.timestamp || '').slice(11, 19)}</div>
    </div>`;

  if (d.reason || d.note) {
    html += `<div style="margin-top:8px; font-size:13px;">${d.reason || d.note}</div>`;
  }
  if (c.symbol) {
    html += `<div style="margin-top:10px; font-size:13px;">
      <strong>${c.symbol} ${(c.type || '').toUpperCase()} $${c.strike} exp ${c.expiry}</strong>
      <span class="small mono">(${c.dte}d, ${c.vol_signal}, score ${c.score?.toFixed(1)})</span>
    </div>`;
  }
  if (d.total_cost) {
    html += `<div style="margin-top:6px; font-size:13px;">
      Cost: <strong>$${Number(d.total_cost).toFixed(2)}</strong>
      &nbsp; Limit: $${Number(d.limit_price).toFixed(2)}
      &nbsp; Mid: $${Number(d.mid).toFixed(2)}
      &nbsp; Bid/Ask: $${Number(d.bid).toFixed(2)} / $${Number(d.ask).toFixed(2)}
    </div>`;
  }
  if (d.client_order_id) {
    html += `<div class="small mono" style="margin-top:6px;">COID: ${d.client_order_id}</div>`;
  }
  html += '</div>';
  result.innerHTML = html;
}

renderOverview();
</script>

</body>
</html>
"""



def main() -> int:
    path = build()
    print(f"Wrote: {path}")
    print(f"Open with: start \"\" \"{path}\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
