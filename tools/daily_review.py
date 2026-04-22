"""
Daily review — one-command summary of today's tool activity.

Pulls from:
  - Latest snapshot (morning recommendations)
  - Flow-news monitor results (if any)
  - Paper trade log
  - Recent error alerts

Run anytime: python -m tools.daily_review
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _find_snapshot_for(d: date) -> Path | None:
    """Find the snapshot written nearest to date d."""
    snap_dir = REPO_ROOT / "snapshots"
    candidates = []
    for f in snap_dir.glob("*.json"):
        if not f.is_file() or f.parent != snap_dir:
            continue
        # Filename pattern: YYYY-MM-DD.json or YYYY-MM-DD_suffix.json
        stem = f.stem
        try:
            file_d = datetime.strptime(stem[:10], "%Y-%m-%d").date()
            if file_d == d:
                candidates.append((f.stat().st_mtime, f))
        except (ValueError, IndexError):
            continue
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _show_snapshot(d: date) -> None:
    path = _find_snapshot_for(d)
    if not path:
        print(f"  (no snapshot for {d.isoformat()})")
        return
    try:
        snap = json.loads(path.read_text())
    except Exception as e:
        print(f"  (error reading {path.name}: {e})")
        return

    trades = snap.get("trades", [])
    print(f"  File: {path.name}")
    print(f"  Total flagged: {len(trades)}")

    by_signal: dict[str, int] = {}
    for t in trades:
        sig = t.get("vol_signal", "UNKNOWN")
        by_signal[sig] = by_signal.get(sig, 0) + 1
    for sig, count in sorted(by_signal.items()):
        print(f"    {sig}: {count}")

    # Top 5 by score
    top = sorted(trades, key=lambda x: -x.get("score", 0))[:5]
    if top:
        print(f"\n  Top 5 by score:")
        for t in top:
            entry = t.get("entry_price", 0)
            print(f"    {t.get('symbol',''):<6} {t.get('option_type',''):<5} "
                  f"${t.get('strike',0):<7} exp {t.get('expiry',''):<12} "
                  f"entry ${entry:<6.2f} score {t.get('score',0):.1f} "
                  f"[{t.get('vol_signal','')}]")


def _show_flow_news(d: date) -> None:
    fn_dir = REPO_ROOT / "snapshots" / "flow_news"
    if not fn_dir.exists():
        print("  (no flow_news directory)")
        return

    date_str = d.strftime("%Y%m%d")
    files = sorted(fn_dir.glob(f"flow_news_{date_str}_*.json"))
    if not files:
        print(f"  (no flow+news scans for {d.isoformat()})")
        return

    # Show latest scan + aggregate HIGH CONVICTION across the day
    latest = files[-1]
    data = json.loads(latest.read_text())
    results = data.get("results", [])

    all_high = []
    for f in files:
        d_ = json.loads(f.read_text())
        for r in d_.get("results", []):
            if r.get("combined") == "HIGH_CONVICTION":
                all_high.append((f.stem.split("_")[-1], r))

    print(f"  Scans today: {len(files)}")
    print(f"  Latest: {latest.name}")
    print(f"  Tickers in latest: {len(results)}")
    print(f"  HIGH CONVICTION signals today: {len(all_high)}")

    if all_high:
        print(f"\n  High conviction signals (all scans today):")
        for t, r in all_high[:10]:
            print(f"    [{t}] {r['ticker']}: {r.get('option_direction','?').upper()} — {r['rationale']}")


def _show_paper_trades(d: date) -> None:
    path = REPO_ROOT / "logs" / "paper_trades.jsonl"
    if not path.exists():
        print(f"  (no paper trades log)")
        return

    today_iso = d.isoformat()
    orders = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = o.get("timestamp", "")
                if ts.startswith(today_iso):
                    orders.append(o)
    except Exception as e:
        print(f"  (error reading {path.name}: {e})")
        return

    if not orders:
        print(f"  (no paper trades attempted on {today_iso})")
        return

    # Group by tier tag
    by_tier: dict[str, list[dict]] = {}
    for o in orders:
        tag = o.get("tag") or "(untagged)"
        by_tier.setdefault(tag, []).append(o)

    print(f"  Attempts today: {len(orders)} across {len(by_tier)} tier(s)")

    for tier, tier_orders in sorted(by_tier.items()):
        print(f"\n  Tier: {tier}  ({len(tier_orders)} attempts)")
        status_counts: dict[str, int] = {}
        total_cost = 0.0
        for o in tier_orders:
            s = o.get("status", "?")
            status_counts[s] = status_counts.get(s, 0) + 1
            if o.get("status") == "submitted":
                total_cost += float(o.get("total_cost") or 0)
        status_line = " ".join(f"{s}={c}" for s, c in status_counts.items())
        print(f"    status: {status_line}   deployed: ${total_cost:.2f}")

        for o in tier_orders:
            status = o.get("status", "?")
            sym = o.get("symbol", "?")
            ot = o.get("option_type", "?")
            k = o.get("strike", "?")
            exp = o.get("expiry", "?")
            cost = o.get("total_cost")
            cost_s = f"${cost:.2f}" if cost else "n/a"
            coid = o.get("client_order_id") or ""
            err = o.get("error", "")
            line = f"    [{status:<9}] {sym} {ot} ${k} {exp} cost={cost_s}"
            if coid:
                line += f" coid={coid[:40]}"
            if err:
                line += f" err={err[:60]}"
            print(line)


def _show_recent_errors(days: int = 1) -> None:
    log_dir = REPO_ROOT / "logs"
    if not log_dir.exists():
        print("  (no logs directory)")
        return

    cutoff = datetime.now() - timedelta(days=days)
    alert_files = sorted([
        f for f in log_dir.glob("error_alert_*.log")
        if datetime.fromtimestamp(f.stat().st_mtime) >= cutoff
    ], reverse=True)

    if not alert_files:
        print(f"  (no alerts in last {days} day(s))")
        return

    print(f"  Alert files in last {days} day(s): {len(alert_files)}")
    for f in alert_files[:10]:
        try:
            data = json.loads(f.read_text())
            ts = data.get("timestamp", "")[:19]
            sev = data.get("severity", "?")
            src = data.get("source", "?")
            sym = data.get("symbol", "") or "-"
            msg = (data.get("message") or "")[:90]
            print(f"    [{sev}] {ts} {src} [{sym}]: {msg}")
        except Exception:
            print(f"    {f.name} (could not parse)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily review — one-command summary")
    parser.add_argument("--date", type=str, default=None,
                        help="Review date YYYY-MM-DD (default: today)")
    parser.add_argument("--errors-days", type=int, default=1,
                        help="How many days of errors to show (default: 1)")
    args = parser.parse_args()

    target = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()

    print(f"\n{'#' * 60}")
    print(f"#  DAILY REVIEW — {target.strftime('%A, %Y-%m-%d')}")
    print(f"#  As of {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'#' * 60}")

    _section("SNAPSHOT (morning recommendations)")
    _show_snapshot(target)

    _section("FLOW + NEWS signals")
    _show_flow_news(target)

    _section("PAPER TRADES")
    _show_paper_trades(target)

    _section(f"ALERTS (last {args.errors_days} day(s))")
    _show_recent_errors(args.errors_days)

    print("\n" + "=" * 60)
    print("  Helpful commands:")
    print("    python -m tools.snapshot                  # new scan")
    print("    python -m tools.flow_news_monitor         # check unusual flow")
    print("    python -m tools.paper_trade               # dry-run paper trade")
    print("    python -m tools.paper_trade --live        # actually submit")
    print("=" * 60 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
