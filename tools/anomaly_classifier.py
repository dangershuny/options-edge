"""
Anomaly classifier — for WARN/CRIT health-check hits that DON'T match a
whitelisted remediation, generate a structured proposal so a human can
review and one-tap approve later.

In aggressive mode, certain proposal categories may be auto-executed by
auto_remediate; this module exists for the residual set: novel failures
that should not be acted on without review.

Usage:
    python -m tools.anomaly_classifier   # reads logs/health-current.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs"
CURRENT_HEALTH = LOG_DIR / "health-current.json"
PROPOSALS_PATH = LOG_DIR / "proposals-current.json"
PROPOSALS_HISTORY = LOG_DIR / "proposals-history.jsonl"


# Categories of "no auto-fix today" failures we want to surface as proposals.
# Each entry maps a check name to (severity_floor, suggested_action_text).
PROPOSAL_HANDLERS: dict[str, tuple[str, str]] = {
    "broker_connectivity": ("CRIT",
        "Verify .env keys, network, and Alpaca status page. "
        "If keys rotated: update .env and restart all OptionsEdge tasks."),
    "account_blocked": ("CRIT",
        "Account blocked at broker. Log into Alpaca dashboard immediately — "
        "common causes: PDT violation, KYC issue, manual lock."),
    "engine_state_db": ("CRIT",
        "engine_state.db corruption detected. Stop OptionsEdge-ExitMonitor, "
        "back up the db, run `sqlite3 engine_state.db .recover | sqlite3 fixed.db`, "
        "swap, restart."),
    "sentinel_freshness": ("WARN",
        "Sentinel data stale during RTH. If restart_sentinel_server cooldown "
        "exhausted: investigate news_sentinel server.py logs at "
        "C:\\Users\\dange\\OneDrive\\Documents\\Claude Projects\\news_sentinel\\."),
    "quote_staleness": ("WARN",
        "Live quote feed stale on open positions. Check Alpaca data status; "
        "monitor_tick will fall through but exits won't fire on accurate marks."),
}


def _proposal(check: dict) -> dict | None:
    name = check.get("name")
    status = check.get("status")
    if status == "OK":
        return None
    # If a known remediation hint matches, the auto_remediate pipeline owns it.
    if check.get("remediation_hint"):
        return None
    # Otherwise classify against our proposal table.
    if name not in PROPOSAL_HANDLERS:
        # Truly novel: generic proposal asking for human investigation.
        return {
            "check": name,
            "severity": status,
            "summary": f"Novel anomaly in {name}: no whitelisted handler.",
            "suggested_action": (
                "Inspect logs/health-current.json and the relevant runner log; "
                "if recurring, add a handler to tools/auto_remediate.py."
            ),
            "auto_executable": False,
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "raw_check": check,
        }
    floor, suggestion = PROPOSAL_HANDLERS[name]
    return {
        "check": name,
        "severity": status,
        "summary": f"{status} on {name}: {check.get('detail') or 'see raw_check'}",
        "suggested_action": suggestion,
        "auto_executable": False,
        "severity_floor": floor,
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "raw_check": check,
    }


def classify(report: dict) -> list[dict]:
    proposals: list[dict] = []
    for c in report.get("checks", []):
        p = _proposal(c)
        if p is not None:
            proposals.append(p)
    return proposals


def write_outputs(proposals: list[dict]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PROPOSALS_PATH.write_text(json.dumps(proposals, indent=2, default=str), encoding="utf-8")
    if proposals:
        with PROPOSALS_HISTORY.open("a", encoding="utf-8") as f:
            for p in proposals:
                f.write(json.dumps(p, default=str) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true")
    args = ap.parse_args()

    if not CURRENT_HEALTH.exists():
        print("no health-current.json — run tools.health_check first")
        return 1
    report = json.loads(CURRENT_HEALTH.read_text(encoding="utf-8"))
    proposals = classify(report)
    write_outputs(proposals)

    if args.print or proposals:
        for p in proposals:
            print(f"[{p['severity']}] {p['check']}: {p['summary']}")
            print(f"    suggested: {p['suggested_action']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
