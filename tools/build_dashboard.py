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


def _fetch_ticker_bars(symbols: list[str]) -> dict:
    """
    Fetch intraday + daily bars for each unique ticker.
    Uses Alpaca's free IEX feed.

    Returns:
        {symbol: {
            "intraday": [{t, o, h, l, c, v}, ...],  # 5-min bars, last 2 trading days
            "daily":    [{t, o, h, l, c, v}, ...],  # daily bars, last 252 days
            "last":     float,                      # most recent close
            "last_ts":  str,                        # ISO timestamp of most recent bar
        }}
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
                {"t": b.timestamp.isoformat(), "o": float(b.open), "h": float(b.high),
                 "l": float(b.low), "c": float(b.close), "v": int(b.volume or 0)}
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

    # Gather unique tickers for bar fetching
    unique_tickers = sorted({
        (t.get("symbol") or "").upper()
        for t in snap_trades
        if t.get("symbol")
    })[:20]  # cap at 20 to keep file size reasonable
    ticker_bars = _fetch_ticker_bars(unique_tickers)

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
        "unique_tickers": unique_tickers,
        "ticker_bars": ticker_bars,
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
    <button class="btn" onclick="location.reload()">Refresh page</button>
  </div>
</header>

<div class="tabs">
  <div class="tab active" data-tab="overview">Overview</div>
  <div class="tab" data-tab="analysis">Analysis</div>
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

// ── Tab switching
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'analysis') initAnalysis();
    if (tab.dataset.tab === 'trades') initTrades();
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

function initTickerCharts() {
  const tickers = DATA.unique_tickers || [];
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
    const daily = data.daily || [];
    const last = data.last;
    // Compute day change
    let change = null, changePct = null;
    if (intraday.length >= 2) {
      // Use first intraday bar of today vs last intraday
      const todayStr = intraday[intraday.length - 1].t.slice(0, 10);
      const todayBars = intraday.filter(b => b.t.slice(0, 10) === todayStr);
      if (todayBars.length >= 2) {
        const openPrice = todayBars[0].o;
        change = todayBars[todayBars.length - 1].c - openPrice;
        changePct = (change / openPrice) * 100;
      }
    }

    const changeHtml = change != null
      ? `<span class="change ${change > 0 ? 'pos' : 'neg'}">${change > 0 ? '+' : ''}${change.toFixed(2)} (${changePct > 0 ? '+' : ''}${changePct.toFixed(2)}%)</span>`
      : '';

    card.innerHTML = `
      <div class="ticker-head">
        <div>
          <span class="sym">${sym}</span>
          <span class="last">${last != null ? '$' + last.toFixed(2) : '—'}</span>
          ${changeHtml}
        </div>
        <div class="tf-toggles" data-sym="${sym}">
          <button class="tf-btn active" data-tf="1D">1D</button>
          <button class="tf-btn" data-tf="5D">5D</button>
          <button class="tf-btn" data-tf="1M">1M</button>
          <button class="tf-btn" data-tf="3M">3M</button>
          <button class="tf-btn" data-tf="1Y">1Y</button>
        </div>
      </div>
      <canvas id="chart-${sym}"></canvas>
    `;
    grid.appendChild(card);

    // Initial render: 1D
    renderTickerChart(sym, '1D');

    // Wire toggles
    card.querySelectorAll('.tf-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        card.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        renderTickerChart(sym, btn.dataset.tf);
      });
    });
  });
}

const _chartInstances = {};

function renderTickerChart(sym, tf) {
  const bars = (DATA.ticker_bars || {})[sym] || {};
  const intraday = bars.intraday || [];
  const daily = bars.daily || [];

  let points = [];
  let label = tf;

  if (tf === '1D' && intraday.length) {
    const lastDay = intraday[intraday.length - 1].t.slice(0, 10);
    points = intraday.filter(b => b.t.slice(0, 10) === lastDay).map(b => ({ x: b.t, y: b.c }));
  } else if (tf === '5D' && intraday.length) {
    points = intraday.map(b => ({ x: b.t, y: b.c }));
  } else if (tf === '1M' && daily.length) {
    points = daily.slice(-22).map(b => ({ x: b.t, y: b.c }));
  } else if (tf === '3M' && daily.length) {
    points = daily.slice(-65).map(b => ({ x: b.t, y: b.c }));
  } else if (tf === '1Y' && daily.length) {
    points = daily.map(b => ({ x: b.t, y: b.c }));
  }

  const canvas = document.getElementById('chart-' + sym);
  if (!canvas) return;

  // Destroy prior instance if exists
  if (_chartInstances[sym]) _chartInstances[sym].destroy();

  if (!points.length) {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#8b93a7';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(`No ${tf} data`, canvas.width / 2, 60);
    return;
  }

  const color = points[0].y > points[points.length - 1].y ? '#f7768e' : '#9ece6a';

  _chartInstances[sym] = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      datasets: [{
        label: sym,
        data: points,
        borderColor: color,
        backgroundColor: color + '20',
        tension: 0.2, pointRadius: 0, fill: true, borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      animation: { duration: 250 },
      plugins: { legend: { display: false },
                 tooltip: { mode: 'index', intersect: false } },
      scales: {
        x: {
          type: 'time',
          time: { unit: tf === '1D' ? 'hour' : tf === '5D' ? 'day' : 'day' },
          ticks: { color: '#8b93a7', maxTicksLimit: 6 },
          grid: { color: '#2a2f3e' },
        },
        y: {
          ticks: { color: '#8b93a7', callback: v => '$' + Number(v).toFixed(2) },
          grid: { color: '#2a2f3e' },
        },
      },
    },
  });
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
