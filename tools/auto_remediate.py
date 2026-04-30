"""
Auto-remediator — applies whitelisted fixes for known WARN/CRIT health-check
hits during trading hours, in **aggressive** mode (restart services, retry
transient errors, blacklist dead-money contracts, etc.).

Each remediation is a pure function that takes the check result dict and
returns a Remediation result. All remediations are logged to
`logs/remediations-{date}.jsonl` for audit. Restart-storm protection: each
remediation type can fire at most N times per hour (per the COOLDOWNS map).

Usage:
    python -m tools.auto_remediate                 # use logs/health-current.json
    python -m tools.auto_remediate --check stale_closing
    python -m tools.auto_remediate --dry-run       # log only
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

CURRENT_HEALTH = LOG_DIR / "health-current.json"
REMEDIATION_LOG = LOG_DIR / f"remediations-{date.today().isoformat()}.jsonl"
BLACKLIST_PATH = LOG_DIR / f"contract_blacklist_{date.today().isoformat()}.json"

# Cooldowns: max firings per hour per remediation key.
COOLDOWNS = {
    "restart_sentinel_server": 4,
    "restart_override_server": 4,
    "rerun_failed_tasks": 6,
    "vacuum_engine_state": 1,
    "run_reconcile": 12,
    "blacklist_contracts_for_today": 3,
    "cancel_and_repost_close": 6,
    "halt_new_buys_for_day": 1,
}


def _audit(action: str, result: dict) -> None:
    rec = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "action": action,
        "result": result,
    }
    with REMEDIATION_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, default=str) + "\n")


def _recent_count(action: str, hours: int = 1) -> int:
    """How many times has this action fired in the last `hours`?"""
    if not REMEDIATION_LOG.exists():
        return 0
    cutoff = datetime.now(tz=timezone.utc).timestamp() - hours * 3600
    n = 0
    try:
        for line in REMEDIATION_LOG.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("action") != action:
                continue
            try:
                ts = datetime.fromisoformat(rec["ts"].replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            if ts >= cutoff:
                n += 1
    except Exception:
        pass
    return n


def _cooldown_ok(action: str) -> bool:
    cap = COOLDOWNS.get(action)
    if cap is None:
        return True  # unbounded if no cooldown configured
    return _recent_count(action) < cap


# ── Remediations ─────────────────────────────────────────────────────────────

def restart_sentinel_server(check: dict, dry_run: bool) -> dict:
    if not _cooldown_ok("restart_sentinel_server"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    if dry_run:
        return {"ok": True, "would": "subprocess.Popen sentinel server.py"}
    try:
        from sentinel_bridge import ensure_sentinel_running
        started = ensure_sentinel_running()
        return {"ok": bool(started), "ensured": started}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def restart_override_server(check: dict, dry_run: bool) -> dict:
    if not _cooldown_ok("restart_override_server"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    if dry_run:
        return {"ok": True, "would": "schtasks /Run OptionsEdge-OverrideServer"}
    try:
        r = subprocess.run(
            ["schtasks", "/Run", "/TN", "OptionsEdge-OverrideServer"],
            capture_output=True, text=True, timeout=15,
        )
        return {"ok": r.returncode == 0, "stdout": r.stdout.strip(),
                "stderr": r.stderr.strip()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def rerun_failed_tasks(check: dict, dry_run: bool) -> dict:
    if not _cooldown_ok("rerun_failed_tasks"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    failed = check.get("failed", [])
    if not failed:
        return {"ok": True, "rerun": []}
    rerun = []
    for f in failed:
        task = f.get("task")
        if not task:
            continue
        # OverrideServer is non-trading and crashes are tolerated
        if task == "OptionsEdge-OverrideServer":
            continue
        if dry_run:
            rerun.append({"task": task, "would": "schtasks /Run"})
            continue
        try:
            r = subprocess.run(
                ["schtasks", "/Run", "/TN", task],
                capture_output=True, text=True, timeout=15,
            )
            rerun.append({"task": task, "ok": r.returncode == 0,
                          "msg": (r.stdout or r.stderr).strip()})
        except Exception as e:
            rerun.append({"task": task, "ok": False, "error": str(e)})
    return {"ok": True, "rerun": rerun}


def vacuum_engine_state(check: dict, dry_run: bool) -> dict:
    if not _cooldown_ok("vacuum_engine_state"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    if dry_run:
        return {"ok": True, "would": "PRAGMA wal_checkpoint + VACUUM"}
    try:
        import sqlite3
        with sqlite3.connect(REPO_ROOT / "engine_state.db") as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            c.execute("VACUUM")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def run_reconcile(check: dict, dry_run: bool) -> dict:
    if not _cooldown_ok("run_reconcile"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    if dry_run:
        return {"ok": True, "would": "engine.execute.reconcile_with_broker()"}
    try:
        from engine.execute import reconcile_with_broker
        reconcile_with_broker()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def run_reconcile_or_backfill(check: dict, dry_run: bool) -> dict:
    """If untracked positions exist, run backfill_positions; otherwise reconcile."""
    untracked = check.get("untracked", [])
    if untracked:
        if not _cooldown_ok("run_reconcile"):
            return {"ok": False, "skipped": "cooldown_exhausted"}
        if dry_run:
            return {"ok": True, "would": "python -m tools.backfill_positions"}
        try:
            r = subprocess.run(
                [sys.executable, "-m", "tools.backfill_positions"],
                capture_output=True, text=True, timeout=60, cwd=str(REPO_ROOT),
            )
            return {"ok": r.returncode == 0,
                    "stdout": r.stdout[-500:], "stderr": r.stderr[-500:]}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return run_reconcile(check, dry_run)


def blacklist_contracts_for_today(check: dict, dry_run: bool) -> dict:
    """Add OCCs with N+ expired BUYs to today's blacklist so paper_trade
    skips them on subsequent intraday rescans."""
    if not _cooldown_ok("blacklist_contracts_for_today"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    dead = check.get("dead", [])
    if not dead:
        return {"ok": True, "added": []}

    existing = set()
    if BLACKLIST_PATH.exists():
        try:
            existing = set(json.loads(BLACKLIST_PATH.read_text()))
        except Exception:
            existing = set()
    new = {d.get("occ") for d in dead if d.get("occ")}
    merged = sorted(existing | new)
    if dry_run:
        return {"ok": True, "would_write": merged}
    BLACKLIST_PATH.write_text(json.dumps(merged), encoding="utf-8")
    return {"ok": True, "added": sorted(new - existing), "total": len(merged)}


def cancel_and_repost_close(check: dict, dry_run: bool) -> dict:
    """For 'closing' rows whose order has been pending too long, cancel the
    stuck order and let the next monitor_tick re-fire at a fresher price."""
    if not _cooldown_ok("cancel_and_repost_close"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    stale = check.get("stale", [])
    if not stale:
        return {"ok": True, "canceled": []}

    canceled = []
    try:
        from broker import alpaca
        from engine.state import init_db, list_closing, revert_to_open
        init_db()
        rows = {r["occ_symbol"]: r for r in list_closing()}
        tc = alpaca._trading_client()
        for s in stale:
            occ = s.get("occ")
            row = rows.get(occ)
            if not row or not row.get("exit_order_id"):
                continue
            if dry_run:
                canceled.append({"occ": occ, "would": f"cancel {row['exit_order_id']}"})
                continue
            try:
                tc.cancel_order_by_id(row["exit_order_id"])
                revert_to_open(row["id"])
                canceled.append({"occ": occ, "ok": True})
            except Exception as e:
                canceled.append({"occ": occ, "ok": False, "error": str(e)})
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "canceled": canceled}


def halt_new_buys_for_day(check: dict, dry_run: bool) -> dict:
    """Daily-loss cap exceeded — write a halt flag so paper_trade skips."""
    if not _cooldown_ok("halt_new_buys_for_day"):
        return {"ok": False, "skipped": "cooldown_exhausted"}
    flag = LOG_DIR / f"halt_buys_{date.today().isoformat()}.flag"
    if dry_run:
        return {"ok": True, "would_write": str(flag)}
    flag.write_text(json.dumps({
        "reason": "daily_loss_cap_hit",
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "details": check,
    }, default=str), encoding="utf-8")
    return {"ok": True, "flag": str(flag)}


# ── Registry ─────────────────────────────────────────────────────────────────

REMEDIATIONS: dict[str, Callable[[dict, bool], dict]] = {
    "restart_sentinel_server": restart_sentinel_server,
    "restart_override_server": restart_override_server,
    "rerun_failed_tasks": rerun_failed_tasks,
    "vacuum_engine_state": vacuum_engine_state,
    "run_reconcile": run_reconcile,
    "run_reconcile_or_backfill": run_reconcile_or_backfill,
    "blacklist_contracts_for_today": blacklist_contracts_for_today,
    "cancel_and_repost_close": cancel_and_repost_close,
    "halt_new_buys_for_day": halt_new_buys_for_day,
}


def remediate_report(report: dict, dry_run: bool = False) -> list[dict]:
    """Iterate health checks; for each WARN/CRIT with a known hint, fire
    the remediation. Returns list of {check, action, result}."""
    actions: list[dict] = []
    for c in report.get("checks", []):
        if c.get("status") == "OK":
            continue
        hint = c.get("remediation_hint")
        if not hint:
            continue
        fn = REMEDIATIONS.get(hint)
        if fn is None:
            actions.append({
                "check": c["name"], "status": c["status"],
                "action": hint, "result": {"ok": False, "skipped": "no_handler"},
            })
            continue
        result = fn(c, dry_run)
        _audit(hint, {"check": c["name"], "status": c["status"], **result})
        actions.append({
            "check": c["name"], "status": c["status"],
            "action": hint, "result": result,
        })
    return actions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--check", help="run remediation only for this check name")
    args = ap.parse_args()

    if not CURRENT_HEALTH.exists():
        # Run health_check first
        from tools.health_check import run_checks, write_outputs
        report = run_checks()
        write_outputs(report)
    else:
        report = json.loads(CURRENT_HEALTH.read_text(encoding="utf-8"))

    if args.check:
        report = {**report, "checks": [c for c in report["checks"] if c["name"] == args.check]}

    actions = remediate_report(report, dry_run=args.dry_run)
    if not actions:
        print("no remediations triggered")
        return 0
    for a in actions:
        result = a["result"]
        ok = result.get("ok")
        marker = "[+]" if ok else ("[!]" if result.get("skipped") else "[X]")
        print(f"  {marker} {a['check']:<24} -> {a['action']:<30} :: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
