"""
Health runner — orchestrates the full Tier-2 monitoring loop.

Steps each invocation:
  1. health_check.run_checks()                      detect
  2. auto_remediate.remediate_report()              fix what we know
  3. anomaly_classifier.classify()                  propose what we don't
  4. notify.send for any CRIT and any new proposal  alert
  5. Append a row to the daily summary md           audit

Schedule:
  - Every 5 min, weekdays 09:30-16:00 ET (OptionsEdge-Health)
  - Once at 16:35 ET with --summary (OptionsEdge-HealthSummary): writes
    the day's report and clears proposals-current.

Usage:
    python -m tools.health_runner            # full pass
    python -m tools.health_runner --summary  # end-of-day report only
    python -m tools.health_runner --dry-run  # detect+propose, no remediations
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _summary_path() -> Path:
    return LOG_DIR / f"health-{date.today().isoformat()}.md"


def _append_summary(line: str) -> None:
    p = _summary_path()
    if not p.exists():
        p.write_text("# Daily health log\n\n", encoding="utf-8")
    with p.open("a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def run_one_pass(dry_run: bool = False) -> dict:
    from tools.health_check import run_checks, write_outputs as write_health
    from tools.auto_remediate import remediate_report
    from tools.anomaly_classifier import classify, write_outputs as write_proposals
    from tools.notify import send

    report = run_checks()
    write_health(report)

    actions = remediate_report(report, dry_run=dry_run)
    proposals = classify(report)
    write_proposals(proposals)

    # Notify CRIT-level anomalies
    crits = [c for c in report["checks"] if c["status"] == "CRIT"]
    for c in crits:
        title = f"CRIT: {c['name']}"
        body = json.dumps({k: v for k, v in c.items() if k not in ("name", "status")},
                          default=str)[:1000]
        send("CRIT", title, body, payload=c)

    # Notify proposals (only WARNs that don't have whitelisted handlers)
    for p in proposals:
        title = f"PROPOSAL: {p['check']}"
        body = f"{p['summary']}\n\nSuggested: {p['suggested_action']}"
        send("PROPOSAL", title, body, payload=p)

    # Compact line for the daily summary
    counts = report["summary"]
    n_actions = sum(1 for a in actions if a["result"].get("ok"))
    n_skipped = sum(1 for a in actions if a["result"].get("skipped"))
    n_proposals = len(proposals)
    line = (f"- {report['ts']}  OK={counts['OK']} WARN={counts['WARN']} "
            f"CRIT={counts['CRIT']}  fixed={n_actions} skipped={n_skipped} "
            f"proposals={n_proposals}")
    _append_summary(line)

    if actions:
        for a in actions:
            r = a["result"]
            ok = "[+]" if r.get("ok") else ("[!]" if r.get("skipped") else "[X]")
            _append_summary(f"  {ok}   {a['action']:<30} {a['check']:<24} :: "
                            f"{json.dumps(r, default=str)[:200]}")

    return {
        "report": report,
        "actions": actions,
        "proposals": proposals,
    }


def write_eod_summary() -> None:
    """16:35 daily summary — counts the day's actions, P&L, and open proposals."""
    from tools.notify import send
    md = _summary_path()
    rem_log = LOG_DIR / f"remediations-{date.today().isoformat()}.jsonl"

    n_remediations = 0
    if rem_log.exists():
        n_remediations = sum(1 for _ in rem_log.read_text(encoding="utf-8").splitlines())

    proposals_path = LOG_DIR / "proposals-current.json"
    n_open_proposals = 0
    if proposals_path.exists():
        try:
            n_open_proposals = len(json.loads(proposals_path.read_text(encoding="utf-8")))
        except Exception:
            n_open_proposals = 0

    # Day's P&L: count today's submitted entries + filled exits from logs
    n_entries = 0
    n_exits = 0
    realized_pl = 0.0
    today_iso = date.today().isoformat()
    pt_log = LOG_DIR / "paper_trades.jsonl"
    if pt_log.exists():
        try:
            for line in pt_log.read_text(encoding="utf-8").splitlines():
                rec = json.loads(line)
                if not rec.get("timestamp", "").startswith(today_iso):
                    continue
                if rec.get("status") == "submitted":
                    n_entries += 1
        except Exception:
            pass
    # Realized P&L from engine_state.db closed-today rows
    try:
        import sqlite3
        with sqlite3.connect(REPO_ROOT / "engine_state.db") as c:
            for r in c.execute(
                "SELECT realized_pl FROM positions "
                "WHERE status='closed' AND exit_date=? AND realized_pl IS NOT NULL",
                (today_iso,),
            ):
                n_exits += 1
                realized_pl += float(r[0] or 0)
    except Exception:
        pass

    # Account snapshot for the EOD line
    try:
        from broker import alpaca
        acct = alpaca.get_account()
        equity = acct.equity
    except Exception:
        equity = None

    eq_str = f"${equity:,.2f}" if equity is not None else "n/a"
    summary = (
        f"\n## EOD summary {today_iso}\n"
        f"- Equity at close: {eq_str}\n"
        f"- Entries submitted: {n_entries}\n"
        f"- Exits filled: {n_exits}\n"
        f"- Realized P&L (closed today): ${realized_pl:+,.2f}\n"
        f"- Remediations fired: {n_remediations}\n"
        f"- Open proposals: {n_open_proposals}\n"
    )
    if md.exists():
        with md.open("a", encoding="utf-8") as f:
            f.write(summary)
    else:
        md.write_text("# Daily health log\n" + summary, encoding="utf-8")

    body = (
        f"equity={eq_str}\n"
        f"entries={n_entries} exits={n_exits} pl=${realized_pl:+,.0f}\n"
        f"remediations={n_remediations} open_proposals={n_open_proposals}"
    )
    send("INFO", f"EOD {today_iso}", body)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="store_true",
                    help="EOD-only mode: write daily summary and exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="run detection + classification but skip remediations")
    args = ap.parse_args()

    if args.summary:
        write_eod_summary()
        return 0

    result = run_one_pass(dry_run=args.dry_run)
    counts = result["report"]["summary"]
    n_act = len(result["actions"])
    n_prop = len(result["proposals"])
    print(f"OK={counts['OK']} WARN={counts['WARN']} CRIT={counts['CRIT']}  "
          f"actions={n_act} proposals={n_prop}")
    return 0 if counts["CRIT"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
