"""
Build a self-contained HTML dashboard of Options Edge activity.

Two tabs:
  1. TERMINAL — monospace, matches the `daily_review` PowerShell output
     (same sections: snapshot, flow+news, paper trades, alerts)
  2. ANALYSIS — tables + charts for deeper review of today's snapshot
     and tier performance

Data sources (all local):
  - snapshots/                             latest morning snapshot
  - logs/paper_trades.jsonl                trade log
  - logs/morning_auto_run_YYYY-MM-DD.json  today's run summary
  - logs/error_alert_*.log                 recent alerts
  - Alpaca account via broker.alpaca       cash / positions

Embedded as JSON in the HTML — no server needed. Chart.js loads from CDN.
"""

from __future__ import annotations

import html
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


# ── Terminal-style text rendering ───────────────────────────────────────────

def _render_terminal(data: dict) -> str:
    """Produce the PowerShell-style text output embedded in the terminal tab."""
    lines = []
    push = lines.append

    today = data["today"]
    now = datetime.now().strftime("%H:%M:%S")
    day_name = datetime.strptime(today, "%Y-%m-%d").strftime("%A")

    push("#" * 60)
    push(f"#  DAILY REVIEW — {day_name}, {today}")
    push(f"#  As of {now}")
    push("#" * 60)
    push("")

    # ── Broker / account ────────────────────────────────────────────────
    br = data.get("broker", {})
    push("=" * 60)
    push("  ACCOUNT")
    push("=" * 60)
    if br.get("connected"):
        push(f"  Account:       {br.get('account_number', '—')}")
        push(f"  Mode:          {'PAPER' if br.get('paper') else 'LIVE'}")
        push(f"  Equity:        ${br.get('equity', 0):,.2f}")
        push(f"  Cash:          ${br.get('cash', 0):,.2f}")
        push(f"  Buying power:  ${br.get('buying_power', 0):,.2f}")
        push(f"  Blocked:       {br.get('blocked', False)}")
    else:
        push(f"  NOT CONNECTED: {br.get('error', 'unknown error')}")
    push("")

    # ── Snapshot ───────────────────────────────────────────────────────
    push("=" * 60)
    push("  SNAPSHOT (morning recommendations)")
    push("=" * 60)
    snap = data.get("snap_trades", [])
    if not snap:
        push(f"  (no snapshot for {today})")
    else:
        if data.get("snap_path"):
            push(f"  File: {Path(data['snap_path']).name}")
        push(f"  Total flagged: {len(snap)}")

        by_sig: dict[str, int] = defaultdict(int)
        for t in snap:
            by_sig[t.get("vol_signal", "?")] += 1
        for sig, n in sorted(by_sig.items()):
            push(f"    {sig}: {n}")

        push("")
        push("  Top 10 by score:")
        for t in snap[:10]:
            sym = t.get("symbol", "")
            ot = t.get("option_type", "")
            k = t.get("strike", "")
            exp = t.get("expiry", "")
            entry = t.get("entry_price") or 0
            score = t.get("score") or 0
            push(
                f"    {sym:<6} {ot:<5} ${k:<7} exp {exp:<12} "
                f"entry ${entry:<6.2f}  score {score:<6.1f}  [{t.get('vol_signal','')}]"
            )
    push("")

    # ── Flow + News ────────────────────────────────────────────────────
    push("=" * 60)
    push("  FLOW + NEWS signals")
    push("=" * 60)
    fn = data.get("flow_news", {})
    push(f"  Scans today: {fn.get('scans', 0)}")
    high = fn.get("high_conviction", [])
    push(f"  HIGH CONVICTION signals: {len(high)}")
    for r in high[:5]:
        opt = (r.get("option_direction") or "?").upper()
        push(f"    [{r.get('scan', '')}] {r.get('ticker','?')}: "
             f"{opt} — {r.get('rationale','')}")
    push("")

    # ── Paper trades (grouped by tier) ─────────────────────────────────
    push("=" * 60)
    push("  PAPER TRADES")
    push("=" * 60)
    tiers = data.get("tier_stats", {}) or {}
    if not tiers:
        push("  (no paper trades attempted)")
    else:
        total = sum(t["total_attempts"] for t in tiers.values())
        push(f"  Attempts: {total} across {len(tiers)} tier(s)")
        for name in sorted(tiers.keys()):
            t = tiers[name]
            push("")
            push(f"  Tier: {name}  ({t['total_attempts']} attempts)")
            status_line = "  ".join(
                f"{s}={t[s]}" for s in ("submitted", "dry_run", "skipped", "failed")
            )
            push(f"    {status_line}   deployed: ${t['deployed']:.2f}")
            for o in t.get("orders", []):
                status = o.get("status", "?")
                sym = o.get("symbol", "?")
                ot = o.get("option_type", "?")
                strike = o.get("strike", "?")
                exp = o.get("expiry", "?")
                cost = o.get("total_cost")
                coid = o.get("client_order_id") or ""
                err = o.get("error", "")
                cost_s = f"${cost:.2f}" if cost else "n/a"
                line = f"    [{status:<9}] {sym} {ot} ${strike} {exp} cost={cost_s}"
                if coid: line += f" coid={coid[:40]}"
                if err:  line += f" err={err[:60]}"
                push(line)
    push("")

    # ── Open positions ─────────────────────────────────────────────────
    push("=" * 60)
    push("  OPEN POSITIONS")
    push("=" * 60)
    positions = data.get("positions", [])
    if not positions:
        push("  (no open positions)")
    else:
        for p in positions:
            push(f"  {p.get('symbol','?'):<22} qty={p.get('qty',0)}  "
                 f"entry=${p.get('avg_entry',0):.2f}  mark=${p.get('mark',0):.2f}  "
                 f"P&L=${p.get('unrealized_pl',0):+.2f} ({p.get('unrealized_pl_pct',0):+.1f}%)")
    push("")

    # ── Alerts ─────────────────────────────────────────────────────────
    push("=" * 60)
    push(f"  ALERTS (last 3 days)")
    push("=" * 60)
    alerts = data.get("alerts", [])
    if not alerts:
        push("  (no alerts)")
    else:
        push(f"  Alert files: {len(alerts)}")
        for a in alerts[:15]:
            ts = (a.get("timestamp") or "")[:19].replace("T", " ")
            sev = a.get("severity", "?")
            src = a.get("source", "?")
            sym = a.get("symbol") or "-"
            msg = (a.get("message") or "")[:90]
            push(f"    [{sev}] {ts} {src} [{sym}]: {msg}")
    push("")

    # ── Footer ─────────────────────────────────────────────────────────
    push("=" * 60)
    push("  Helpful commands:")
    push("    tools\\dashboard.bat                       # rebuild + open this page")
    push("    python -m tools.snapshot                  # new scan")
    push("    python -m tools.flow_news_monitor         # check unusual flow")
    push("    python -m tools.paper_trade               # dry-run paper trade")
    push("    python -m tools.paper_trade --live        # actually submit")
    push("=" * 60)

    return "\n".join(lines)


# ── Main build ──────────────────────────────────────────────────────────────

def build() -> Path:
    today = date.today()
    snap_path, snap = _latest_snapshot_for(today)
    # If no snapshot for today, fall back to most recent in the last 7 days
    fallback_note = None
    if not snap:
        for back in range(1, 8):
            d = today - timedelta(days=back)
            p, s = _latest_snapshot_for(d)
            if s:
                snap_path, snap = p, s
                fallback_note = d.isoformat()
                break

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
    }

    terminal_text = _render_terminal(data)

    body = _HTML_TEMPLATE.replace(
        "__TERMINAL_TEXT__", html.escape(terminal_text)
    ).replace(
        "__DATA__", json.dumps(data, default=str)
    )

    DASHBOARD_PATH.write_text(body, encoding="utf-8")
    return DASHBOARD_PATH


# ── HTML template ───────────────────────────────────────────────────────────

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Options Edge Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  :root {
    --bg: #0c0c0c;
    --card: #161616;
    --fg: #dcdcdc;
    --mute: #8a8a8a;
    --accent: #4fc3f7;
    --ok: #4caf50;
    --warn: #ffb300;
    --err: #ef5350;
    --border: #2a2a2a;
    --prompt: #50fa7b;
  }
  * { box-sizing: border-box; }
  html, body { background: var(--bg); color: var(--fg); }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0; padding: 0;
  }
  .page {
    max-width: 1400px; margin: 0 auto; padding: 16px 24px 40px;
  }
  header {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: 10px 0 14px; border-bottom: 1px solid var(--border); margin-bottom: 14px;
  }
  header h1 { margin: 0; font-size: 20px; color: var(--fg); }
  .subtitle { color: var(--mute); font-size: 12px; margin-top: 2px; }
  .tabs {
    display: flex; gap: 4px; margin-bottom: 14px; border-bottom: 1px solid var(--border);
  }
  .tab {
    padding: 10px 18px; cursor: pointer; color: var(--mute);
    font-size: 14px; border-bottom: 2px solid transparent;
    user-select: none;
  }
  .tab.active { color: var(--fg); border-bottom-color: var(--accent); }
  .tab:hover { color: var(--fg); }
  .tab-content { display: none; }
  .tab-content.active { display: block; }

  /* Terminal tab */
  .terminal {
    background: #0c0c0c; border: 1px solid var(--border); border-radius: 6px;
    padding: 16px 20px;
    font-family: "Cascadia Mono", "Consolas", "SF Mono", "Menlo", monospace;
    font-size: 13px; line-height: 1.45;
    color: #d4d4d4;
    white-space: pre;
    overflow-x: auto;
    min-height: 500px;
  }
  .terminal .prompt { color: var(--prompt); }
  .toolbar {
    display: flex; gap: 8px; margin-bottom: 8px;
  }
  .btn {
    background: #1e1e1e; color: var(--fg); border: 1px solid var(--border);
    padding: 5px 12px; border-radius: 4px; cursor: pointer; font-size: 12px;
  }
  .btn:hover { background: #2a2a2a; }

  /* Analysis tab */
  h2 { margin: 20px 0 10px; font-size: 15px;
       color: var(--mute); text-transform: uppercase; letter-spacing: 0.6px;
       border-bottom: 1px solid var(--border); padding-bottom: 4px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 16px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 12px 14px; }
  .card .label { color: var(--mute); font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; }
  .card .value { font-size: 20px; font-weight: 500; margin-top: 3px; }
  .card .sub { color: var(--mute); font-size: 11px; margin-top: 2px; }
  .pos { color: var(--ok); }
  .neg { color: var(--err); }

  table { width: 100%; border-collapse: collapse; font-size: 12px;
          background: var(--card); border-radius: 6px; overflow: hidden; margin-bottom: 14px; }
  th, td { padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--border); }
  th { background: #1a1a1a; color: var(--mute); font-weight: 500;
       font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255,255,255,0.03); }
  .mono { font-family: "Cascadia Mono", Consolas, monospace; font-size: 11px; }

  .tier-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .tier-card { background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 12px 14px; }
  .tier-card h3 { margin: 0 0 8px; font-size: 14px; color: var(--fg); }
  .stats-row { display: flex; gap: 14px; margin-bottom: 8px; font-size: 12px; }
  .stats-row .s { color: var(--mute); }
  .stats-row strong { color: var(--fg); }

  .badge { display: inline-block; padding: 1px 6px; border-radius: 10px;
           font-size: 10px; font-weight: 500; background: #253649; color: var(--accent); }
  .badge.ok { background: rgba(76,175,80,0.18); color: var(--ok); }
  .badge.warn { background: rgba(255,179,0,0.18); color: var(--warn); }
  .badge.err { background: rgba(239,83,80,0.18); color: var(--err); }

  .chart-wrap { background: var(--card); border: 1px solid var(--border); border-radius: 6px;
                padding: 14px; margin-bottom: 14px; }
  .chart-wrap h2 { margin-top: 0; border: none; padding: 0; }
  canvas { max-height: 260px; }
  .empty { color: var(--mute); font-size: 12px; padding: 14px; text-align: center; }
</style>
</head>
<body>
<div class="page">

<header>
  <div>
    <h1>Options Edge Dashboard</h1>
    <div class="subtitle" id="subtitle"></div>
  </div>
  <div class="toolbar">
    <button class="btn" onclick="location.reload()">Refresh</button>
  </div>
</header>

<div class="tabs">
  <div class="tab active" data-tab="terminal">Terminal</div>
  <div class="tab" data-tab="analysis">Analysis</div>
</div>

<!-- TERMINAL TAB -->
<div class="tab-content active" id="tab-terminal">
<pre class="terminal">__TERMINAL_TEXT__</pre>
</div>

<!-- ANALYSIS TAB -->
<div class="tab-content" id="tab-analysis">

<section>
  <h2>Account</h2>
  <div class="cards" id="account-cards"></div>
</section>

<section>
  <h2>Today's snapshot — summary</h2>
  <div class="cards" id="snap-summary-cards"></div>
  <div class="chart-wrap">
    <h2 style="margin: 0 0 8px; text-transform:none; letter-spacing:0; color: var(--fg); font-size: 14px;">Score distribution</h2>
    <canvas id="scoreChart"></canvas>
  </div>
</section>

<section>
  <h2>Top snapshot candidates</h2>
  <div id="snap-table-wrap"></div>
</section>

<section>
  <h2>Paper trades by tier</h2>
  <div class="tier-grid" id="tier-grid"></div>
</section>

<section>
  <h2>Open positions</h2>
  <div id="positions-wrap"></div>
</section>

<section>
  <h2>Deployed capital (last 30 days)</h2>
  <div class="chart-wrap"><canvas id="pnlChart"></canvas></div>
</section>

<section>
  <h2>Recent alerts (3 days)</h2>
  <div id="alerts-wrap"></div>
</section>

</div><!-- /analysis tab -->

</div><!-- /page -->

<script>
const DATA = __DATA__;

// Tab switching
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'analysis') initAnalysis();
  });
});

// Subtitle
document.getElementById('subtitle').textContent =
  `Generated ${new Date(DATA.generated).toLocaleString()}  ·  Today: ${DATA.today}  ·  Trade log entries (30d): ${DATA.trades_last_30d}`;

function fmtMoney(n) {
  if (n == null) return '—';
  return '$' + Number(n).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
}
function pnlClass(n) { if (!n) return ''; return n > 0 ? 'pos' : 'neg'; }

let _analysisInitialized = false;
function initAnalysis() {
  if (_analysisInitialized) return;
  _analysisInitialized = true;

  // Account
  const acc = DATA.broker || {};
  const accEl = document.getElementById('account-cards');
  if (acc.connected) {
    accEl.innerHTML = `
      <div class="card"><div class="label">Account</div>
        <div class="value mono" style="font-size:14px;">${acc.account_number || '—'}</div>
        <div class="sub">${acc.paper ? 'Paper' : 'Live'}${acc.blocked ? ' · BLOCKED' : ''}</div></div>
      <div class="card"><div class="label">Equity</div><div class="value">${fmtMoney(acc.equity)}</div></div>
      <div class="card"><div class="label">Cash</div><div class="value">${fmtMoney(acc.cash)}</div></div>
      <div class="card"><div class="label">Buying power</div><div class="value">${fmtMoney(acc.buying_power)}</div></div>
    `;
  } else {
    accEl.innerHTML = `<div class="card">Broker not reachable: ${acc.error || ''}</div>`;
  }

  // Snapshot summary cards
  const snap = DATA.snap_trades || [];
  const sb = DATA.signal_breakdown || {by_signal: {}, by_type: {}};
  const snapEl = document.getElementById('snap-summary-cards');
  const topScore = snap.length ? Math.max(...snap.map(t => Number(t.score || 0))) : 0;
  const avgScore = snap.length ? (snap.reduce((s, t) => s + Number(t.score || 0), 0) / snap.length) : 0;
  snapEl.innerHTML = `
    <div class="card"><div class="label">Candidates</div><div class="value">${DATA.snap_total ?? snap.length}</div>${DATA.snap_fallback_date ? `<div class="sub">Using ${DATA.snap_fallback_date} (no scan today)</div>` : ''}</div>
    <div class="card"><div class="label">Top score</div><div class="value">${topScore.toFixed(1)}</div></div>
    <div class="card"><div class="label">Avg score</div><div class="value">${avgScore.toFixed(1)}</div></div>
    <div class="card"><div class="label">Calls vs Puts</div><div class="value">${(sb.by_type.call || 0)} / ${(sb.by_type.put || 0)}</div></div>
  `;

  // Score distribution chart
  const hist = DATA.score_histogram || {};
  const histLabels = Object.keys(hist);
  const histValues = histLabels.map(k => hist[k]);
  if (histLabels.length && histValues.reduce((a,b)=>a+b,0) > 0) {
    new Chart(document.getElementById('scoreChart').getContext('2d'), {
      type: 'bar',
      data: { labels: histLabels, datasets: [{
        label: 'Candidates', data: histValues,
        backgroundColor: '#4fc3f7',
      }]},
      options: {
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#8a8a8a' }, grid: { color: '#222' } },
          y: { ticks: { color: '#8a8a8a', precision: 0 }, grid: { color: '#222' }, beginAtZero: true },
        },
      },
    });
  } else {
    document.getElementById('scoreChart').parentElement.innerHTML =
      '<div class="empty">No snapshot data to chart.</div>';
  }

  // Snapshot top table
  const tblWrap = document.getElementById('snap-table-wrap');
  if (!snap.length) {
    tblWrap.innerHTML = '<div class="empty">No snapshot available.</div>';
  } else {
    let h = `<table><thead><tr>
      <th>Rank</th><th>Score</th><th>Sym</th><th>Type</th><th>Strike</th>
      <th>Expiry</th><th>DTE</th><th>Entry</th><th>Signal</th>
    </tr></thead><tbody>`;
    snap.slice(0, 25).forEach((t, i) => {
      h += `<tr>
        <td>${i+1}</td>
        <td><strong>${Number(t.score || 0).toFixed(1)}</strong></td>
        <td>${t.symbol || ''}</td>
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

  // Tier grid
  const tiers = DATA.tier_stats || {};
  const tierEl = document.getElementById('tier-grid');
  const tierNames = Object.keys(tiers).sort();
  if (!tierNames.length) {
    tierEl.innerHTML = '<div class="empty">No paper trades recorded yet.</div>';
  } else {
    tierEl.innerHTML = tierNames.map(name => {
      const t = tiers[name];
      const rows = (t.orders || []).map(o => {
        const cls = o.status === 'submitted' ? 'ok' :
                    o.status === 'skipped' ? 'warn' :
                    o.status === 'failed' ? 'err' : '';
        return `<tr>
          <td><span class="badge ${cls}">${o.status || '?'}</span></td>
          <td>${o.symbol || ''} ${(o.option_type || '').charAt(0).toUpperCase()} $${o.strike || '—'}</td>
          <td class="mono">${o.expiry || ''}</td>
          <td>${fmtMoney(o.total_cost)}</td>
          <td>${Number(o.score || 0).toFixed(0)}</td>
          <td class="mono" style="font-size:10px;">${(o.timestamp || '').slice(11,19)}</td>
        </tr>`;
      }).join('');
      return `<div class="tier-card">
        <h3>${name}</h3>
        <div class="stats-row">
          <div><span class="s">Submitted:</span> <strong>${t.submitted}</strong></div>
          <div><span class="s">Skipped:</span> <strong>${t.skipped}</strong></div>
          <div><span class="s">Failed:</span> <strong>${t.failed}</strong></div>
          <div><span class="s">Deployed:</span> <strong>${fmtMoney(t.deployed)}</strong></div>
        </div>
        ${rows ? `<table><thead><tr><th>Status</th><th>Contract</th><th>Exp</th><th>Cost</th><th>Scr</th><th>Time</th></tr></thead><tbody>${rows}</tbody></table>` : '<div class="empty">No attempts</div>'}
      </div>`;
    }).join('');
  }

  // Positions
  const positions = DATA.positions || [];
  const posWrap = document.getElementById('positions-wrap');
  if (!positions.length) {
    posWrap.innerHTML = '<div class="empty">No open positions.</div>';
  } else {
    let h = `<table><thead><tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Mark</th><th>Value</th><th>Unrealized</th><th>%</th></tr></thead><tbody>`;
    positions.forEach(p => {
      h += `<tr>
        <td class="mono">${p.symbol}</td>
        <td>${p.qty}</td>
        <td>${fmtMoney(p.avg_entry)}</td>
        <td>${fmtMoney(p.mark)}</td>
        <td>${fmtMoney(p.market_value)}</td>
        <td class="${pnlClass(p.unrealized_pl)}">${fmtMoney(p.unrealized_pl)}</td>
        <td class="${pnlClass(p.unrealized_pl)}">${Number(p.unrealized_pl_pct || 0).toFixed(2)}%</td>
      </tr>`;
    });
    h += '</tbody></table>';
    posWrap.innerHTML = h;
  }

  // 30-day deployed chart
  const pnl = DATA.deployed_history || {};
  if (pnl.days && pnl.days.length && pnl.tiers.length) {
    const palette = ['#4fc3f7', '#ab47bc', '#66bb6a', '#ffb300', '#ef5350'];
    new Chart(document.getElementById('pnlChart').getContext('2d'), {
      type: 'line',
      data: {
        labels: pnl.days,
        datasets: pnl.tiers.map((tier, i) => ({
          label: tier,
          data: pnl.series[tier],
          borderColor: palette[i % palette.length],
          backgroundColor: palette[i % palette.length] + '22',
          tension: 0.2, pointRadius: 3,
        })),
      },
      options: {
        responsive: true,
        plugins: { legend: { labels: { color: '#dcdcdc' } } },
        scales: {
          x: { ticks: { color: '#8a8a8a' }, grid: { color: '#222' } },
          y: { ticks: { color: '#8a8a8a', callback: v => '$' + v }, grid: { color: '#222' }, beginAtZero: true },
        },
      },
    });
  } else {
    document.querySelector('#tab-analysis .chart-wrap:last-of-type').innerHTML =
      '<div class="empty">No trade history yet.</div>';
  }

  // Alerts
  const alerts = DATA.alerts || [];
  const alertsWrap = document.getElementById('alerts-wrap');
  if (!alerts.length) {
    alertsWrap.innerHTML = '<div class="empty">No alerts.</div>';
  } else {
    alertsWrap.innerHTML = alerts.map(a => {
      const sev = (a.severity || 'ERROR').toUpperCase();
      const cls = sev === 'INFO' ? 'ok' : sev === 'WARN' ? 'warn' : 'err';
      const ts = (a.timestamp || '').slice(0, 19).replace('T', ' ');
      return `<div class="card" style="margin-bottom:6px; border-left: 3px solid var(--${cls === 'ok' ? 'accent' : cls}); padding:8px 12px;">
        <div class="small"><span class="badge ${cls}">${sev}</span> ${a.source || '?'} · ${ts} ${a.symbol ? '· ' + a.symbol : ''}</div>
        <div style="margin-top:4px; font-size: 12px;">${a.message || ''}</div>
      </div>`;
    }).join('');
  }
}
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
