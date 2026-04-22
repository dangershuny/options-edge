"""
Build a self-contained HTML dashboard of today's Options Edge activity.

Reads from:
  - snapshots/  (latest morning snapshot)
  - logs/paper_trades.jsonl  (today's paper trade log)
  - logs/morning_auto_run_*.json  (today's run summary)
  - logs/error_alert_*.log  (recent alerts)
  - Alpaca account (current cash/equity if reachable)

Writes:
  - dashboard.html  (one file, double-click to open in browser)

Data is embedded as JSON inside the HTML — no server needed, works offline.
Chart.js loads from CDN for the P&L history chart; tables render regardless.
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


def _safe_json_load(path: Path) -> dict | list | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_snapshot() -> tuple[Path | None, dict]:
    snap_dir = REPO_ROOT / "snapshots"
    today = date.today()
    candidates = []
    for f in snap_dir.glob("*.json"):
        if not f.is_file() or f.parent != snap_dir:
            continue
        mtime_d = datetime.fromtimestamp(f.stat().st_mtime).date()
        if mtime_d == today:
            candidates.append((f.stat().st_mtime, f))
    if not candidates:
        return None, {}
    candidates.sort(reverse=True)
    path = candidates[0][1]
    data = _safe_json_load(path) or {}
    return path, data


def _paper_trades(days_back: int = 14) -> list[dict]:
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
                ts = o.get("timestamp", "")
                if ts and ts >= cutoff:
                    out.append(o)
    except Exception:
        pass
    return out


def _recent_alerts(days_back: int = 3) -> list[dict]:
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


def _todays_run_summary() -> dict:
    today = date.today().isoformat()
    path = REPO_ROOT / "logs" / f"morning_auto_run_{today}.json"
    return _safe_json_load(path) or {}


def _get_broker_status() -> dict:
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


def _positions() -> list[dict]:
    try:
        import broker.alpaca as b
        pos = b.get_positions()
        return [
            {
                "symbol": p.symbol,
                "qty": p.qty,
                "avg_entry": p.avg_entry,
                "mark": p.mark,
                "market_value": p.market_value,
                "unrealized_pl": p.unrealized_pl,
                "unrealized_pl_pct": p.unrealized_pl_pct,
            }
            for p in pos
        ]
    except Exception:
        return []


def _tier_performance(trades: list[dict]) -> dict:
    """Group trades by tier, compute per-tier stats."""
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
        total_deployed = sum(float(x.get("total_cost") or 0) for x in submitted)
        stats[tier] = {
            "submitted": len(submitted),
            "dry_run": len(dry),
            "skipped": len(skipped),
            "failed": len(failed),
            "total_attempts": len(items),
            "deployed": round(total_deployed, 2),
            "orders": items,
        }
    return stats


def _daily_pnl_by_tier(trades: list[dict]) -> dict:
    """Aggregate daily deployed-cost by tier (proxy for P&L until closes wire in)."""
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


def build() -> Path:
    snap_path, snap = _latest_snapshot()
    snap_trades = sorted(
        snap.get("trades", []),
        key=lambda x: -float(x.get("score", 0) or 0),
    )[:20]

    trades_log = _paper_trades(days_back=30)
    tier_stats = _tier_performance(trades_log)
    pnl_history = _daily_pnl_by_tier(trades_log)
    run_summary = _todays_run_summary()
    alerts = _recent_alerts(days_back=3)
    broker = _get_broker_status()
    positions = _positions()

    now = datetime.now()
    data = {
        "generated": now.isoformat(),
        "today": date.today().isoformat(),
        "snap_path": str(snap_path) if snap_path else None,
        "snap_trades": snap_trades,
        "tier_stats": tier_stats,
        "pnl_history": pnl_history,
        "run_summary": run_summary,
        "alerts": alerts,
        "broker": broker,
        "positions": positions,
        "trades_last_30d": len(trades_log),
    }

    html = _HTML.replace("__DATA__", json.dumps(data, default=str))
    DASHBOARD_PATH.write_text(html, encoding="utf-8")
    return DASHBOARD_PATH


# ── HTML template ───────────────────────────────────────────────────────────
_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Options Edge Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  :root {
    --bg: #0e1117;
    --card: #161b22;
    --fg: #e6edf3;
    --mute: #7d8590;
    --accent: #2f81f7;
    --ok: #3fb950;
    --warn: #d29922;
    --err: #f85149;
    --border: #30363d;
  }
  * { box-sizing: border-box; }
  body {
    background: var(--bg); color: var(--fg);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0; padding: 24px;
    max-width: 1400px; margin: 0 auto;
  }
  h1 { margin: 0 0 4px 0; font-size: 24px; }
  h2 { margin: 24px 0 12px 0; font-size: 18px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }
  h3 { margin: 16px 0 8px 0; font-size: 14px; color: var(--mute); text-transform: uppercase; letter-spacing: 0.5px; }
  .header {
    display: flex; justify-content: space-between; align-items: center;
    padding-bottom: 12px; border-bottom: 2px solid var(--border); margin-bottom: 12px;
  }
  .subtitle { color: var(--mute); font-size: 13px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .card {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px;
  }
  .card .label { color: var(--mute); font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }
  .card .value { font-size: 22px; font-weight: 600; margin-top: 4px; }
  .card .sub { color: var(--mute); font-size: 12px; margin-top: 2px; }
  .pos { color: var(--ok); }
  .neg { color: var(--err); }
  .warn { color: var(--warn); }
  table {
    width: 100%; border-collapse: collapse; font-size: 13px;
    background: var(--card); border-radius: 8px; overflow: hidden;
  }
  th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
  th {
    background: rgba(255,255,255,0.03); color: var(--mute);
    font-weight: 500; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;
  }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .tier-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin-bottom: 20px; }
  .tier-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .tier-card h3 { margin-top: 0; color: var(--fg); font-size: 15px; text-transform: none; letter-spacing: 0; }
  .badge {
    display: inline-block; padding: 2px 8px; border-radius: 12px;
    font-size: 11px; font-weight: 500; background: rgba(47,129,247,0.2); color: var(--accent);
  }
  .badge.ok { background: rgba(63,185,80,0.2); color: var(--ok); }
  .badge.warn { background: rgba(210,153,34,0.2); color: var(--warn); }
  .badge.err { background: rgba(248,81,73,0.2); color: var(--err); }
  .empty { color: var(--mute); font-size: 13px; padding: 16px; text-align: center; }
  .alert {
    padding: 12px; background: rgba(248,81,73,0.08); border-left: 3px solid var(--err);
    border-radius: 4px; margin-bottom: 8px; font-size: 13px;
  }
  .alert.info { border-left-color: var(--accent); background: rgba(47,129,247,0.08); }
  .alert.warn { border-left-color: var(--warn); background: rgba(210,153,34,0.08); }
  .mono { font-family: "SF Mono", Consolas, monospace; font-size: 12px; }
  .small { font-size: 11px; color: var(--mute); }
  .toolbar { display: flex; gap: 8px; margin-bottom: 16px; }
  .btn {
    background: var(--card); color: var(--fg); border: 1px solid var(--border);
    padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 13px;
  }
  .btn:hover { background: rgba(255,255,255,0.05); }
  .chart-wrap { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 20px; }
  #pnlChart { max-height: 240px; }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Options Edge Dashboard</h1>
    <div class="subtitle" id="subtitle">Loading…</div>
  </div>
  <div class="toolbar">
    <button class="btn" onclick="location.reload()">Refresh</button>
  </div>
</div>

<section>
  <h2>Account</h2>
  <div class="cards" id="account-cards"></div>
</section>

<section>
  <h2>Today's top signals from snapshot</h2>
  <div id="snapshot-table-wrap"></div>
</section>

<section>
  <h2>Paper trades by tier</h2>
  <div class="tier-grid" id="tier-grid"></div>
</section>

<section>
  <h2>Open positions</h2>
  <div id="positions-table-wrap"></div>
</section>

<section>
  <h2>Deployed capital history (last 30 days)</h2>
  <div class="chart-wrap"><canvas id="pnlChart"></canvas></div>
</section>

<section>
  <h2>Recent alerts (3 days)</h2>
  <div id="alerts-wrap"></div>
</section>

<script>
const DATA = __DATA__;

function fmtMoney(n) {
  if (n == null) return '—';
  const v = Number(n);
  return '$' + v.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
}
function fmtPct(n) {
  if (n == null) return '—';
  const v = Number(n);
  const cls = v > 0 ? 'pos' : v < 0 ? 'neg' : '';
  return `<span class="${cls}">${v.toFixed(2)}%</span>`;
}
function pnlClass(n) {
  if (n == null || n === 0) return '';
  return n > 0 ? 'pos' : 'neg';
}

// Header subtitle
document.getElementById('subtitle').textContent =
  `Generated ${new Date(DATA.generated).toLocaleString()}  ·  Today: ${DATA.today}  ·  Trades logged (30d): ${DATA.trades_last_30d}`;

// Account cards
const acc = DATA.broker || {};
const accEl = document.getElementById('account-cards');
if (acc.connected) {
  accEl.innerHTML = `
    <div class="card">
      <div class="label">Account</div>
      <div class="value">${acc.account_number || '—'}</div>
      <div class="sub">${acc.paper ? 'Paper' : 'Live'} · ${acc.blocked ? 'BLOCKED' : 'Active'}</div>
    </div>
    <div class="card">
      <div class="label">Equity</div>
      <div class="value">${fmtMoney(acc.equity)}</div>
    </div>
    <div class="card">
      <div class="label">Cash</div>
      <div class="value">${fmtMoney(acc.cash)}</div>
    </div>
    <div class="card">
      <div class="label">Buying power</div>
      <div class="value">${fmtMoney(acc.buying_power)}</div>
    </div>
  `;
} else {
  accEl.innerHTML = `<div class="alert">Broker not reachable: ${acc.error || 'unknown'}</div>`;
}

// Snapshot table
const snapWrap = document.getElementById('snapshot-table-wrap');
const snap = DATA.snap_trades || [];
if (snap.length === 0) {
  snapWrap.innerHTML = `<div class="empty">No snapshot for today yet.</div>`;
} else {
  let html = `<table><thead><tr>
    <th>Score</th><th>Symbol</th><th>Type</th><th>Strike</th><th>Expiry</th>
    <th>Entry</th><th>Signal</th><th>DTE</th>
  </tr></thead><tbody>`;
  for (const t of snap) {
    html += `<tr>
      <td><strong>${Number(t.score || 0).toFixed(1)}</strong></td>
      <td>${t.symbol || ''}</td>
      <td>${(t.option_type || '').toUpperCase()}</td>
      <td>$${t.strike || '—'}</td>
      <td class="mono">${t.expiry || ''}</td>
      <td>${fmtMoney(t.entry_price)}</td>
      <td><span class="badge">${t.vol_signal || ''}</span></td>
      <td>${t.dte || '—'}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  if (DATA.snap_path) {
    html += `<div class="small" style="margin-top:6px;">Source: ${DATA.snap_path}</div>`;
  }
  snapWrap.innerHTML = html;
}

// Tier grid
const tiers = DATA.tier_stats || {};
const tierEl = document.getElementById('tier-grid');
const tierNames = Object.keys(tiers).sort();
if (tierNames.length === 0) {
  tierEl.innerHTML = `<div class="empty">No paper trades recorded yet.</div>`;
} else {
  tierEl.innerHTML = tierNames.map(name => {
    const t = tiers[name];
    const rows = (t.orders || []).map(o => {
      const statusClass = o.status === 'submitted' ? 'ok' :
                          o.status === 'skipped' ? 'warn' :
                          o.status === 'failed' ? 'err' : '';
      return `<tr>
        <td><span class="badge ${statusClass}">${o.status || '?'}</span></td>
        <td>${o.symbol || ''} ${(o.option_type || '').charAt(0).toUpperCase()} $${o.strike || '—'}</td>
        <td class="mono">${o.expiry || ''}</td>
        <td>${fmtMoney(o.total_cost)}</td>
        <td>${Number(o.score || 0).toFixed(0)}</td>
        <td class="small mono">${(o.timestamp || '').slice(11, 19)}</td>
      </tr>`;
    }).join('');
    return `
      <div class="tier-card">
        <h3>${name}</h3>
        <div style="display:flex;gap:12px;margin-bottom:10px;">
          <div><div class="small">Submitted</div><strong>${t.submitted}</strong></div>
          <div><div class="small">Skipped</div><strong>${t.skipped}</strong></div>
          <div><div class="small">Failed</div><strong>${t.failed}</strong></div>
          <div><div class="small">Deployed</div><strong>${fmtMoney(t.deployed)}</strong></div>
        </div>
        ${rows ? `<table style="font-size:12px;"><thead><tr>
          <th>Status</th><th>Contract</th><th>Exp</th><th>Cost</th><th>Scr</th><th>Time</th>
        </tr></thead><tbody>${rows}</tbody></table>` : `<div class="empty">No attempts</div>`}
      </div>
    `;
  }).join('');
}

// Positions table
const posWrap = document.getElementById('positions-table-wrap');
const positions = DATA.positions || [];
if (positions.length === 0) {
  posWrap.innerHTML = `<div class="empty">No open positions.</div>`;
} else {
  let html = `<table><thead><tr>
    <th>Symbol</th><th>Qty</th><th>Entry</th><th>Mark</th><th>Market Value</th>
    <th>Unrealized P&L</th><th>%</th>
  </tr></thead><tbody>`;
  for (const p of positions) {
    html += `<tr>
      <td class="mono">${p.symbol}</td>
      <td>${p.qty}</td>
      <td>${fmtMoney(p.avg_entry)}</td>
      <td>${fmtMoney(p.mark)}</td>
      <td>${fmtMoney(p.market_value)}</td>
      <td class="${pnlClass(p.unrealized_pl)}">${fmtMoney(p.unrealized_pl)}</td>
      <td>${fmtPct(p.unrealized_pl_pct)}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  posWrap.innerHTML = html;
}

// Chart: deployed $ by tier over time
const pnl = DATA.pnl_history || {};
if (pnl.days && pnl.days.length && pnl.tiers.length) {
  const palette = ['#2f81f7', '#a371f7', '#3fb950', '#d29922', '#f85149'];
  const ctx = document.getElementById('pnlChart').getContext('2d');
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: pnl.days,
      datasets: pnl.tiers.map((t, i) => ({
        label: t,
        data: pnl.series[t],
        borderColor: palette[i % palette.length],
        backgroundColor: palette[i % palette.length] + '20',
        tension: 0.2,
        pointRadius: 3,
      })),
    },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: '#e6edf3' } } },
      scales: {
        x: { ticks: { color: '#7d8590' }, grid: { color: '#21262d' } },
        y: {
          ticks: { color: '#7d8590', callback: (v) => '$' + v },
          grid: { color: '#21262d' }, beginAtZero: true,
        },
      },
    },
  });
} else {
  document.querySelector('.chart-wrap').innerHTML = '<div class="empty">No trade history yet.</div>';
}

// Alerts
const alerts = DATA.alerts || [];
const alertWrap = document.getElementById('alerts-wrap');
if (alerts.length === 0) {
  alertWrap.innerHTML = `<div class="empty">No alerts.</div>`;
} else {
  alertWrap.innerHTML = alerts.map(a => {
    const sev = (a.severity || 'ERROR').toUpperCase();
    const cls = sev === 'INFO' ? 'info' : sev === 'WARN' ? 'warn' : '';
    const ts = (a.timestamp || '').slice(0, 19).replace('T', ' ');
    return `<div class="alert ${cls}">
      <div><strong>[${sev}]</strong> ${a.source || '?'} · ${ts} ${a.symbol ? '· ' + a.symbol : ''}</div>
      <div style="margin-top:4px;">${a.message || ''}</div>
    </div>`;
  }).join('');
}
</script>

</body>
</html>
"""


def main() -> int:
    path = build()
    print(f"Wrote: {path}")
    print(f"Open it by double-clicking, or: start {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
