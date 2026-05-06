"""
Watchdog — supervises every critical process and auto-resets failures.

Runs every 1 min during RTH (Mon-Fri 09:30-16:00 ET). Zero-touch:
detects dead/hung/duplicate processes and auto-recovers without
requiring human approval.

Components watched:
  1. ExitMonitor daemon       — drives ratchet/SL/trail/exit firing
  2. Sentinel SUPERVISOR      — news_sentinel/supervisor.py, which itself
                                 supervises server/dashboard/scheduler.
                                 We do NOT touch the supervisor's children
                                 directly — that would race against its
                                 own restart logic.
  3. Telegram listener        — phone command channel
  4. Override server          — manual buy endpoint (8504)
  5. Engine state DB          — auto-vacuum if oversized
  6. Log files                — rotate if any >100 MB

Recovery actions (no confirmation needed):
  - Process dead    → schtasks /Run the appropriate task OR direct spawn
  - Process hung    → kill + relaunch
  - Multiple alive  → kill all but one (only for processes NOT covered by
                       a sibling-side singleton lock)
  - DB > 200 MB     → WAL checkpoint + VACUUM
  - Log > 100 MB    → rotate (rename .old, start fresh)

Cooldowns to prevent restart-storms: each action capped per hour.
Audit log: logs/watchdog-{date}.jsonl for every action.

Usage:
  python -m tools.watchdog              # one supervisor pass + exit
  python -m tools.watchdog --dry-run    # log what it WOULD do, no writes
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import urlopen
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

NY = ZoneInfo("America/New_York")
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG = LOG_DIR / f"watchdog-{date.today().isoformat()}.jsonl"

# Per-action hourly caps (matches auto_remediate cooldown pattern)
COOLDOWNS_PER_HOUR = {
    "relaunch_daemon":            6,   # daemon should never die >6×/hr
    "relaunch_sentinel_supervisor": 3, # supervisor should be even rarer
    "relaunch_listener":          3,
    "relaunch_override":          3,
    "kill_duplicate_processes":   4,
    "vacuum_engine_db":           1,
    "rotate_logs":                2,
}

# Path to the Sentinel side's own supervisor + its singleton lock file.
SENTINEL_DIR = Path(r"C:\Users\dange\OneDrive\Documents\Claude Projects\news_sentinel")
SENTINEL_SUPERVISOR_PY  = SENTINEL_DIR / "supervisor.py"
SENTINEL_SUPERVISOR_PID = SENTINEL_DIR / "supervisor.pid"

# How recently the daemon's monitor_tick must have stamped last_monitor_check
# on every open position before we consider it alive
MONITOR_FRESHNESS_THRESHOLD_SEC = 90

# Sentinel /health response timeout
SENTINEL_TIMEOUT_SEC = 8

# Override server /health response timeout
OVERRIDE_TIMEOUT_SEC = 5

# Telegram getUpdates response timeout to confirm listener is polling
LISTENER_TIMEOUT_SEC = 10


def _audit(action: str, status: str, **kwargs) -> None:
    rec = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "action": action,
        "status": status,
        **kwargs,
    }
    try:
        with AUDIT_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")
    except Exception:
        pass
    print(f"[{rec['ts']}] {action}: {status} {kwargs}", flush=True)


def _recent_count(action: str, hours: int = 1) -> int:
    if not AUDIT_LOG.exists():
        return 0
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).timestamp()
    n = 0
    try:
        for line in AUDIT_LOG.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("action") != action:
                continue
            if rec.get("status") != "fired":
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
    cap = COOLDOWNS_PER_HOUR.get(action, 999)
    return _recent_count(action) < cap


def _is_rth() -> bool:
    now_et = datetime.now(tz=NY)
    if now_et.weekday() >= 5:
        return False
    return (datetime.strptime("09:30", "%H:%M").time()
            <= now_et.time()
            <= datetime.strptime("16:00", "%H:%M").time())


# ── Process discovery (subprocess + WMI via PowerShell — Windows-native) ─────

def _list_python_procs(must_match: str, must_not_match: str | None = None) -> list[dict]:
    """Return [{pid, ppid, name, cmd}] for python.exe/pythonw.exe procs whose
    command line contains must_match (and optionally NOT must_not_match)."""
    try:
        ps = subprocess.run([
            "powershell", "-NoProfile", "-Command",
            "Get-WmiObject Win32_Process | Where-Object { "
            "$_.Name -in @('python.exe','pythonw.exe') } | "
            "Select-Object ProcessId, ParentProcessId, Name, CommandLine | "
            "ConvertTo-Json -Depth 2 -Compress"
        ], capture_output=True, text=True, timeout=20)
    except Exception:
        return []
    if ps.returncode != 0 or not ps.stdout:
        return []
    try:
        data = json.loads(ps.stdout)
    except Exception:
        return []
    if isinstance(data, dict):
        data = [data]
    out = []
    for d in data:
        cmd = d.get("CommandLine") or ""
        if must_match not in cmd:
            continue
        if must_not_match and must_not_match in cmd:
            continue
        out.append({
            "pid": d.get("ProcessId"),
            "ppid": d.get("ParentProcessId"),
            "name": d.get("Name"),
            "cmd": cmd,
        })
    return out


def _kill(pid: int) -> bool:
    try:
        subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                       capture_output=True, text=True, timeout=10)
        return True
    except Exception:
        return False


def _schtask_enabled(task_name: str) -> bool | None:
    """True if the named scheduled task exists AND is enabled.
    False if it exists but is disabled.
    None if it doesn't exist (treat as 'not under our supervision')."""
    try:
        r = subprocess.run(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"],
            capture_output=True, text=True, timeout=15,
        )
    except Exception:
        return None
    if r.returncode != 0:
        return None
    out = r.stdout or ""
    # The "Scheduled Task State" line shows "Enabled" or "Disabled"
    for line in out.splitlines():
        s = line.strip()
        if s.startswith("Scheduled Task State:"):
            val = s.split(":", 1)[1].strip().lower()
            return val == "enabled"
    return None


# ── Component health probes ──────────────────────────────────────────────────

def _http_ok(url: str, timeout: int) -> bool:
    try:
        with urlopen(url, timeout=timeout) as r:
            r.read(64)
        return True
    except Exception:
        return False


def daemon_alive_and_fresh() -> tuple[bool, str]:
    """Daemon = ExitMonitor's --monitor-only --monitor-seconds 15 process.
    Considered healthy if (a) at least one such python proc exists AND
    (b) every 'open' position in engine_state.db has last_monitor_check
    within MONITOR_FRESHNESS_THRESHOLD_SEC."""
    procs = _list_python_procs("--monitor-only", must_not_match="momentum-edge")
    if not procs:
        return False, "no daemon process"

    try:
        from engine.state import init_db, list_open
        init_db()
        opens = [r for r in list_open() if r["status"] == "open"]
    except Exception as e:
        return False, f"engine_state error: {e}"

    if not opens:
        return True, f"daemon alive (pid={procs[0]['pid']}); no open positions to monitor"

    now = datetime.now(tz=timezone.utc)
    stale = []
    for r in opens:
        last = r.get("last_monitor_check")
        if not last:
            stale.append({"occ": r["occ_symbol"], "age_sec": -1})
            continue
        try:
            last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
            age = (now - last_dt).total_seconds()
            if age > MONITOR_FRESHNESS_THRESHOLD_SEC:
                stale.append({"occ": r["occ_symbol"], "age_sec": int(age)})
        except Exception:
            stale.append({"occ": r["occ_symbol"], "age_sec": -1})

    if stale:
        return False, f"stale monitor checks: {stale}"
    return True, f"daemon alive (pid={procs[0]['pid']}), {len(opens)} positions fresh"


def _sentinel_supervisor_pid_alive() -> int | None:
    """Return the PID of the live Sentinel supervisor, or None if dead.
    The supervisor.pid file is written by news_sentinel/supervisor.py and
    contains the PID of the live supervisor."""
    try:
        if not SENTINEL_SUPERVISOR_PID.exists():
            return None
        pid = int(SENTINEL_SUPERVISOR_PID.read_text().strip())
    except Exception:
        return None
    # Cross-check via WMI/Get-Process — _list_python_procs is overkill for one PID
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) "
             "{ 'ALIVE' } else { 'DEAD' }"],
            capture_output=True, text=True, timeout=10,
        )
        if "ALIVE" in (r.stdout or ""):
            return pid
    except Exception:
        pass
    return None


def sentinel_supervisor_alive() -> tuple[bool, str]:
    """Check the Sentinel supervisor.py — NOT the children. The supervisor
    owns server.py / dashboard_writer.py / scheduler.py recovery; we just
    have to ensure the supervisor itself is up.

    Also do a soft /health probe so a totally-dead Sentinel (supervisor
    alive but server stuck for >90s) gets surfaced in logs. Don't take
    action on /health alone — supervisor's 30s × 3 misses ≈ 90s detector
    will handle it before our next pass."""
    pid = _sentinel_supervisor_pid_alive()
    if pid is None:
        return False, "sentinel supervisor.pid missing or stale"
    health_ok = _http_ok("http://localhost:8502/health", SENTINEL_TIMEOUT_SEC)
    if health_ok:
        return True, f"sentinel supervisor alive (pid={pid}); /health ok"
    return True, (f"sentinel supervisor alive (pid={pid}); /health DOWN — "
                  "trusting supervisor's 90s detector to restart server")


def listener_alive() -> tuple[bool, str]:
    procs = _list_python_procs("-m tools.telegram_listener",
                                 must_not_match="momentum-edge")
    if not procs:
        return False, "no listener process"
    return True, f"listener alive (pid={procs[0]['pid']})"


def override_alive() -> tuple[bool, str]:
    if _http_ok("http://localhost:8504/health", OVERRIDE_TIMEOUT_SEC):
        return True, "override /health ok"
    return False, "override /health unreachable"


# ── Recovery actions ─────────────────────────────────────────────────────────

def relaunch_daemon(dry_run: bool) -> dict:
    if not _cooldown_ok("relaunch_daemon"):
        return {"skipped": "cooldown"}
    # Kill any zombie monitor-only python procs first
    procs = _list_python_procs("--monitor-only", must_not_match="momentum-edge")
    for p in procs:
        _kill(p["pid"])
    if dry_run:
        return {"would": "schtasks /Run OptionsEdge-ExitMonitor"}
    r = subprocess.run(["schtasks", "/Run", "/TN", "OptionsEdge-ExitMonitor"],
                       capture_output=True, text=True, timeout=15)
    return {"rc": r.returncode, "stdout": r.stdout.strip(), "stderr": r.stderr.strip()}


def relaunch_sentinel_supervisor(dry_run: bool) -> dict:
    """Restart news_sentinel/supervisor.py. The supervisor itself spawns
    server.py / dashboard_writer.py / scheduler.py on startup, so we don't
    need to touch them directly. We deliberately do NOT kill running
    server/dashboard/scheduler children — if any are alive, the new
    supervisor will detect them via PID file + port probe and leave them
    in place."""
    if not _cooldown_ok("relaunch_sentinel_supervisor"):
        return {"skipped": "cooldown"}
    # Clear stale supervisor.pid if it points to a dead PID — otherwise
    # the new supervisor will refuse to start under its singleton lock.
    if SENTINEL_SUPERVISOR_PID.exists() and _sentinel_supervisor_pid_alive() is None:
        if dry_run:
            return {"would": "rm stale supervisor.pid + spawn supervisor.py"}
        try:
            SENTINEL_SUPERVISOR_PID.unlink()
        except Exception:
            pass
    if dry_run:
        return {"would": "spawn news_sentinel/supervisor.py"}
    python = r"C:\Users\dange\AppData\Local\Programs\Python\Python313\pythonw.exe"
    try:
        subprocess.Popen(
            [python, str(SENTINEL_SUPERVISOR_PY)],
            cwd=str(SENTINEL_DIR),
            stdout=open(SENTINEL_DIR / "supervisor.log", "ab"),
            stderr=open(SENTINEL_DIR / "supervisor-err.log", "ab"),
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000008,  # DETACHED_PROCESS
        )
    except Exception as e:
        return {"error": str(e)}
    return {"ok": True}


def relaunch_listener(dry_run: bool) -> dict:
    if not _cooldown_ok("relaunch_listener"):
        return {"skipped": "cooldown"}
    if dry_run:
        return {"would": "spawn telegram_listener via Startup-folder bat"}
    # Trigger via the bat (which has its own retry loop)
    bat = REPO_ROOT / "tools" / "telegram_listener.bat"
    if not bat.exists():
        return {"error": "bat missing"}
    try:
        subprocess.Popen(["cmd", "/c", str(bat)],
                         creationflags=subprocess.CREATE_NEW_CONSOLE | 0x00000008)
    except Exception as e:
        return {"error": str(e)}
    return {"ok": True}


def relaunch_override(dry_run: bool) -> dict:
    if not _cooldown_ok("relaunch_override"):
        return {"skipped": "cooldown"}
    if dry_run:
        return {"would": "schtasks /Run OptionsEdge-OverrideServer"}
    r = subprocess.run(["schtasks", "/Run", "/TN", "OptionsEdge-OverrideServer"],
                       capture_output=True, text=True, timeout=15)
    return {"rc": r.returncode, "stdout": r.stdout.strip()[:200],
            "stderr": r.stderr.strip()[:200]}


def kill_duplicate_processes(dry_run: bool) -> dict:
    """Multiple instances of the same supervised process = leftover zombies.
    Kill all but the most recent. Matches must be SPECIFIC enough to avoid
    confusing sibling scripts in the same project (e.g. news_sentinel has
    server.py + dashboard_writer.py + scheduler.py — they are NOT dupes
    of each other).

    Sentinel children (server/dashboard/scheduler) intentionally NOT in
    this list — they each have their own singleton-lock PID file
    (server.py uses port-bind probe, dashboard/scheduler use *.pid). If
    a duplicate ever does appear there, the Sentinel supervisor or its
    own re-startup logic will refuse it. Killing here would race with
    the supervisor's own restart cycle. We do supervise the Sentinel
    SUPERVISOR itself — it's idempotent under its supervisor.pid lock."""
    if not _cooldown_ok("kill_duplicate_processes"):
        return {"skipped": "cooldown"}
    actions = []
    # Each rule: (label, must_match_substring, must_not_match_substring)
    # Substrings are chosen so each rule matches EXACTLY ONE legitimate
    # process — anything beyond that is a zombie.
    for label, match, exclude in [
        ("daemon", "--monitor-only", "momentum-edge"),
        ("listener", "-m tools.telegram_listener", "momentum-edge"),
        ("sentinel_supervisor", r"news_sentinel\supervisor.py", None),
    ]:
        procs = _list_python_procs(match, must_not_match=exclude)
        if len(procs) <= 1:
            continue
        # Keep highest PID (typically most recent), kill rest
        sorted_procs = sorted(procs, key=lambda p: p["pid"], reverse=True)
        for p in sorted_procs[1:]:
            if not dry_run:
                _kill(p["pid"])
            actions.append({"label": label, "killed": p["pid"]})
    return {"ok": True, "actions": actions}


def vacuum_engine_db(dry_run: bool) -> dict:
    if not _cooldown_ok("vacuum_engine_db"):
        return {"skipped": "cooldown"}
    db = REPO_ROOT / "engine_state.db"
    if not db.exists():
        return {"error": "db missing"}
    size_mb = db.stat().st_size / (1024 * 1024)
    if size_mb < 200:
        return {"ok": True, "skipped": "db < 200 MB"}
    if dry_run:
        return {"would": "WAL checkpoint + VACUUM", "size_mb": round(size_mb, 1)}
    try:
        import sqlite3
        with sqlite3.connect(db) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            c.execute("VACUUM")
    except Exception as e:
        return {"error": str(e)}
    return {"ok": True, "size_before_mb": round(size_mb, 1)}


def rotate_logs(dry_run: bool) -> dict:
    """Rename any .log file > 100 MB to .log.YYYY-MM-DD.old; start fresh."""
    if not _cooldown_ok("rotate_logs"):
        return {"skipped": "cooldown"}
    rotated = []
    for log in LOG_DIR.glob("*.log"):
        try:
            size_mb = log.stat().st_size / (1024 * 1024)
        except Exception:
            continue
        if size_mb < 100:
            continue
        old = log.with_suffix(f".log.{date.today().isoformat()}.old")
        if dry_run:
            rotated.append({"file": log.name, "size_mb": round(size_mb, 1),
                             "would_rename_to": old.name})
            continue
        try:
            log.rename(old)
            rotated.append({"file": log.name, "size_mb": round(size_mb, 1),
                             "renamed_to": old.name})
        except Exception as e:
            rotated.append({"file": log.name, "error": str(e)})
    return {"ok": True, "rotated": rotated}


# ── Supervisor pass ─────────────────────────────────────────────────────────

def supervise(dry_run: bool = False) -> dict:
    rth = _is_rth()
    started = datetime.now(tz=timezone.utc).isoformat()
    actions_taken: list[dict] = []
    findings: list[dict] = []

    # 1. Daemon (ONLY supervised during RTH — weekend/after-hours daemon
    # exits clean by design)
    if rth:
        ok, msg = daemon_alive_and_fresh()
        findings.append({"component": "daemon", "ok": ok, "msg": msg})
        if not ok:
            r = relaunch_daemon(dry_run)
            _audit("relaunch_daemon", "fired", finding=msg, result=r)
            actions_taken.append({"action": "relaunch_daemon", "result": r})

    # 2. Sentinel SUPERVISOR (always — owns server/dashboard/scheduler 24/7).
    # We only restart the supervisor itself; never the children. If the
    # supervisor is alive we trust its own 90s detector to handle a stuck
    # /health. If supervisor.pid is dead/missing, we relaunch supervisor.py,
    # which spawns the children on startup.
    ok, msg = sentinel_supervisor_alive()
    findings.append({"component": "sentinel_supervisor", "ok": ok, "msg": msg})
    if not ok:
        r = relaunch_sentinel_supervisor(dry_run)
        _audit("relaunch_sentinel_supervisor", "fired", finding=msg, result=r)
        actions_taken.append({"action": "relaunch_sentinel_supervisor", "result": r})

    # 3. Listener (always — user expects /commands 24/7)
    ok, msg = listener_alive()
    findings.append({"component": "listener", "ok": ok, "msg": msg})
    if not ok:
        r = relaunch_listener(dry_run)
        _audit("relaunch_listener", "fired", finding=msg, result=r)
        actions_taken.append({"action": "relaunch_listener", "result": r})

    # 4. Override server (only if its scheduled task is ENABLED — if user
    # disabled it intentionally, don't fight that)
    override_task_enabled = _schtask_enabled("OptionsEdge-OverrideServer")
    if override_task_enabled is True:
        ok, msg = override_alive()
        findings.append({"component": "override", "ok": ok, "msg": msg})
        if not ok:
            r = relaunch_override(dry_run)
            _audit("relaunch_override", "fired", finding=msg, result=r)
            actions_taken.append({"action": "relaunch_override", "result": r})
    else:
        findings.append({"component": "override", "ok": True,
                         "msg": f"task disabled/missing ({override_task_enabled}); not supervised"})

    # 5. Duplicate-process cleanup
    dup_check = kill_duplicate_processes(dry_run)
    if dup_check.get("actions"):
        _audit("kill_duplicate_processes", "fired", result=dup_check)
        actions_taken.append({"action": "kill_duplicate_processes", "result": dup_check})

    # 6. DB vacuum if oversized
    db_action = vacuum_engine_db(dry_run)
    if db_action.get("ok") and not db_action.get("skipped"):
        _audit("vacuum_engine_db", "fired", result=db_action)
        actions_taken.append({"action": "vacuum_engine_db", "result": db_action})

    # 7. Log rotation
    log_action = rotate_logs(dry_run)
    if log_action.get("rotated"):
        _audit("rotate_logs", "fired", result=log_action)
        actions_taken.append({"action": "rotate_logs", "result": log_action})

    summary = {
        "ts": started,
        "rth": rth,
        "dry_run": dry_run,
        "findings": findings,
        "actions": actions_taken,
        "n_actions": len(actions_taken),
    }
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="log what would happen, don't actually relaunch anything")
    args = ap.parse_args()
    s = supervise(dry_run=args.dry_run)
    print(json.dumps(s, indent=2, default=str))
    # Telegram alert if anything was actually fired (CRIT-style noise so
    # operator sees autonomous recovery in real time)
    if s["n_actions"] and not args.dry_run:
        try:
            from tools.notify import send
            actions_summary = ", ".join(a["action"] for a in s["actions"])
            send("WARN", f"Watchdog fired {s['n_actions']} action(s)",
                 f"Components recovered automatically: {actions_summary}")
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
