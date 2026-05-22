"""
Health check — pure detection, no side effects.

Runs every 5min during RTH (and on demand). Returns a structured dict of
every category we monitor, each with a status (OK/WARN/CRIT) and a
machine-readable check_name so auto_remediate can match it to a fix.

Output:
    logs/health-current.json   — most recent run, overwrites
    logs/health-history.jsonl  — appended one row per run

Usage:
    python -m tools.health_check                # full run, prints summary
    python -m tools.health_check --json         # raw JSON to stdout
    python -m tools.health_check --check broker # run a single category
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
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

CURRENT_PATH = LOG_DIR / "health-current.json"
HISTORY_PATH = LOG_DIR / "health-history.jsonl"

OK, WARN, CRIT = "OK", "WARN", "CRIT"


def _check(name: str, status: str, **kwargs) -> dict:
    """Build a check result dict with consistent keys."""
    return {"name": name, "status": status, **kwargs}


# ── Service health ───────────────────────────────────────────────────────────

def check_sentinel_server(timeout: float = 2.0) -> dict:
    """Sentinel HTTP /health endpoint reachable on localhost:8502."""
    try:
        with urlopen("http://localhost:8502/health", timeout=timeout) as r:
            r.read()
        return _check("sentinel_server", OK, url="http://localhost:8502/health")
    except (URLError, HTTPError, ConnectionError, OSError, TimeoutError) as e:
        return _check("sentinel_server", CRIT,
                      detail=f"unreachable: {type(e).__name__}: {e}",
                      remediation_hint="restart_sentinel_server")


def check_override_server(timeout: float = 2.0) -> dict:
    """Override HTTP server on localhost:8504 — manual buy endpoint.

    Returns OK when the OptionsEdge-OverrideServer scheduled task is
    explicitly disabled (operator turned it off intentionally — don't
    fight that with a remediation loop). Today the disabled task spawned
    75 restart attempts that all hit cooldown — pure noise."""
    # Short-circuit if task is intentionally disabled
    try:
        r = subprocess.run(
            ["schtasks", "/Query", "/TN", "OptionsEdge-OverrideServer",
             "/FO", "LIST", "/V"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            for line in (r.stdout or "").splitlines():
                s = line.strip()
                if s.startswith("Scheduled Task State:"):
                    state = s.split(":", 1)[1].strip().lower()
                    if state == "disabled":
                        return _check("override_server", OK,
                                       detail="task disabled — not supervised")
                    break
    except Exception:
        pass  # if schtasks lookup fails, fall through to the live probe
    try:
        with urlopen("http://localhost:8504/health", timeout=timeout) as r:
            r.read()
        return _check("override_server", OK, url="http://localhost:8504/health")
    except (URLError, HTTPError, ConnectionError, OSError, TimeoutError) as e:
        # Override server is non-critical for autonomous trading — WARN not CRIT.
        return _check("override_server", WARN,
                      detail=f"unreachable: {type(e).__name__}",
                      remediation_hint="restart_override_server")


# ── Broker health ────────────────────────────────────────────────────────────

def check_broker_connectivity() -> dict:
    """Account state + buying power + daily-loss check."""
    try:
        from broker import alpaca
        acct = alpaca.get_account()
    except Exception as e:
        return _check("broker_connectivity", CRIT,
                      detail=f"alpaca.get_account failed: {e}",
                      remediation_hint=None)
    return _check(
        "broker_connectivity", OK,
        equity=acct.equity, cash=acct.cash, buying_power=acct.buying_power,
        is_paper=acct.is_paper, blocked=acct.account_blocked,
    )


def check_account_blocked() -> dict:
    try:
        from broker import alpaca
        acct = alpaca.get_account()
    except Exception:
        return _check("account_blocked", WARN, detail="broker unavailable")
    return _check(
        "account_blocked", CRIT if acct.account_blocked else OK,
        blocked=acct.account_blocked,
    )


def check_daily_loss_proximity() -> dict:
    """Alert when realized + unrealized P&L approaches RISK['max_daily_loss']."""
    try:
        from broker import alpaca
        from risk.config import RISK
        acct = alpaca.get_account()
        positions = alpaca.get_positions()
    except Exception as e:
        return _check("daily_loss", WARN, detail=f"unavailable: {e}")

    # Yesterday's equity reference (rough — use start-of-day from history if available)
    snapshot_path = LOG_DIR / "equity-snapshot.json"
    prev_equity = None
    if snapshot_path.exists():
        try:
            prev_equity = float(json.loads(snapshot_path.read_text()).get("equity"))
        except Exception:
            pass

    unrealized = sum(p.unrealized_pl for p in positions)
    daily_pl = (acct.equity - prev_equity) if prev_equity else unrealized
    cap = float(RISK.get("max_daily_loss") or 0)

    if cap <= 0:
        return _check("daily_loss", OK, daily_pl=daily_pl, cap=cap)

    pct_used = abs(min(0.0, daily_pl)) / cap
    if pct_used >= 1.0:
        return _check("daily_loss", CRIT, daily_pl=daily_pl, cap=cap, pct=pct_used,
                      remediation_hint="halt_new_buys_for_day")
    if pct_used >= 0.75:
        return _check("daily_loss", WARN, daily_pl=daily_pl, cap=cap, pct=pct_used)
    return _check("daily_loss", OK, daily_pl=daily_pl, cap=cap, pct=pct_used)


# ── Engine state vs broker truth ─────────────────────────────────────────────

def check_engine_state_drift() -> dict:
    """Engine state ('open' or 'closing') should agree with Alpaca's positions.
    A position in 'closing' is still managed (awaiting fill); it counts as tracked."""
    try:
        from broker import alpaca
        from engine.state import init_db, list_open
        init_db()
        live = {p.symbol for p in alpaca.get_positions()}
        engine_managed = {r["occ_symbol"] for r in list_open()
                          if r["status"] in ("open", "closing")}
    except Exception as e:
        return _check("engine_drift", WARN, detail=f"unavailable: {e}")

    only_in_engine = engine_managed - live  # phantoms (marked closing/open but no broker pos)
    only_at_broker = live - engine_managed  # untracked (broker has it, engine doesn't manage)

    if only_in_engine or only_at_broker:
        return _check(
            "engine_drift",
            WARN if (only_at_broker and not only_in_engine) else CRIT,
            phantoms=sorted(only_in_engine),
            untracked=sorted(only_at_broker),
            remediation_hint="run_reconcile_or_backfill",
        )
    return _check("engine_drift", OK, count=len(engine_managed))


def check_stale_closing_rows(stale_minutes: int = 30) -> dict:
    """A 'closing' row with an exit_order_id whose Alpaca order has been
    PENDING for >stale_minutes during RTH — likely stuck and needs
    investigation. Reconcile handles FILLED/EXPIRED/CANCELED on its own;
    after-hours orders are queued for the next session and not stale."""
    now_et = datetime.now(tz=NY)
    rth = (now_et.weekday() < 5
           and datetime.strptime("09:30", "%H:%M").time() <= now_et.time()
           <= datetime.strptime("16:00", "%H:%M").time())
    if not rth:
        return _check("stale_closing", OK, detail="outside RTH")

    try:
        from broker import alpaca
        from engine.state import init_db, list_closing
        init_db()
        closing = list_closing()
    except Exception as e:
        return _check("stale_closing", WARN, detail=str(e))

    if not closing:
        return _check("stale_closing", OK, count=0)

    stale = []
    try:
        tc = alpaca._trading_client()
    except Exception as e:
        return _check("stale_closing", WARN, detail=str(e))

    now = datetime.now(tz=timezone.utc)
    for r in closing:
        oid = r.get("exit_order_id")
        if not oid:
            continue
        try:
            o = tc.get_order_by_id(oid)
        except Exception:
            continue
        status = str(o.status)
        if any(t in status for t in ("FILLED", "EXPIRED", "CANCELED", "REJECTED")):
            continue  # reconcile handles these
        sub_at = getattr(o, "submitted_at", None)
        if sub_at is None:
            continue
        age_min = (now - sub_at).total_seconds() / 60.0
        if age_min > stale_minutes:
            stale.append({"occ": r["occ_symbol"], "status": status,
                           "age_min": round(age_min, 1)})

    if stale:
        return _check("stale_closing", WARN, stale=stale,
                      remediation_hint="cancel_and_repost_close")
    return _check("stale_closing", OK, count=len(closing))


def check_dead_money_contracts(expired_threshold: int = 3) -> dict:
    """Same OCC submitted N times with all BUYs EXPIRED → blacklist for day.
    Reads paper_trades.jsonl + queries Alpaca for matching client_order_id status."""
    log = LOG_DIR / "paper_trades.jsonl"
    if not log.exists():
        return _check("dead_money", OK, count=0)

    today_iso = date.today().isoformat()
    today_submits: dict[str, list[str]] = {}  # occ -> [order_ids]
    try:
        with log.open(encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                ts = rec.get("timestamp", "")
                if not ts.startswith(today_iso):
                    continue
                if rec.get("status") != "submitted":
                    continue
                occ = rec.get("occ")
                oid = rec.get("order_id")
                if occ and oid:
                    today_submits.setdefault(occ, []).append(oid)
    except Exception as e:
        return _check("dead_money", WARN, detail=str(e))

    # Check expiry rate via Alpaca for any OCC with N+ submissions
    dead: list[dict] = []
    if today_submits:
        try:
            from broker import alpaca
            tc = alpaca._trading_client()
            for occ, oids in today_submits.items():
                if len(oids) < expired_threshold:
                    continue
                expired = 0
                for oid in oids:
                    try:
                        o = tc.get_order_by_id(oid)
                        if "EXPIRED" in str(o.status) or "CANCELED" in str(o.status):
                            expired += 1
                    except Exception:
                        pass
                if expired >= expired_threshold:
                    dead.append({"occ": occ, "submits": len(oids), "expired": expired})
        except Exception as e:
            return _check("dead_money", WARN, detail=str(e))

    if dead:
        return _check("dead_money", WARN, dead=dead,
                      remediation_hint="blacklist_contracts_for_today")
    return _check("dead_money", OK, count=len(today_submits))


# ── Sentinel data freshness ──────────────────────────────────────────────────

def check_sentinel_data_freshness(threshold_minutes: int = 60) -> dict:
    """Sentinel DB should have writes within the threshold during RTH."""
    sentinel_db = REPO_ROOT.parent.parent / "OneDrive" / "Documents" / "Claude Projects" / "news_sentinel" / "sentinel.db"
    if not sentinel_db.exists():
        # Try direct path (env may differ)
        sentinel_db = Path(r"C:\Users\dange\OneDrive\Documents\Claude Projects\news_sentinel\sentinel.db")
        if not sentinel_db.exists():
            return _check("sentinel_freshness", WARN, detail="sentinel.db not found")

    age_min = (time.time() - sentinel_db.stat().st_mtime) / 60.0
    # Only enforce freshness during RTH; outside RTH staleness is expected
    now_et = datetime.now(tz=NY).time()
    rth = (now_et >= datetime.strptime("09:30", "%H:%M").time() and
           now_et <= datetime.strptime("16:00", "%H:%M").time())
    weekday = datetime.now(tz=NY).weekday() < 5

    if rth and weekday and age_min > threshold_minutes:
        return _check("sentinel_freshness", WARN,
                      age_minutes=round(age_min, 1), threshold=threshold_minutes,
                      remediation_hint="restart_sentinel_server")
    return _check("sentinel_freshness", OK, age_minutes=round(age_min, 1))


# ── Scheduled task health ────────────────────────────────────────────────────

def check_scheduled_tasks() -> dict:
    """Each OptionsEdge-* task: did it run on schedule with exit code 0?"""
    try:
        out = subprocess.run(
            ["schtasks", "/Query", "/FO", "CSV", "/V"],
            capture_output=True, text=True, timeout=15,
        ).stdout
    except Exception as e:
        return _check("scheduled_tasks", WARN, detail=str(e))

    import csv as _csv
    from io import StringIO

    bad = []
    for row in _csv.DictReader(StringIO(out)):
        name = row.get("TaskName", "")
        if "OptionsEdge" not in name:
            continue
        # 2026-05-22: skip disabled tasks — the operator intentionally turned
        # them off, the stale Last Result from before disable shouldn't count
        # as failure. Today's 38 spurious rerun_failed_tasks remediations all
        # traced to OptionsEdge-OverrideServer (disabled since 5/1, Last
        # Result -1073741510 from a Ctrl+C exit).
        task_state = (row.get("Scheduled Task State") or "").strip().lower()
        if task_state == "disabled":
            continue
        last_result = (row.get("Last Result") or "").strip()
        # Exit codes: 0=success, 267009=running, 267011=never run, 267014=ready
        if last_result in ("0", "267009", "267011", "267014"):
            continue
        # Any other code is a failure to investigate
        bad.append({
            "task": name.split("\\")[-1],
            "last_result": last_result,
            "last_run": (row.get("Last Run Time") or "").strip(),
        })

    if bad:
        return _check("scheduled_tasks", WARN, failed=bad,
                      remediation_hint="rerun_failed_tasks")
    return _check("scheduled_tasks", OK)


# ── Engine state DB integrity ────────────────────────────────────────────────

def check_engine_state_db() -> dict:
    db = REPO_ROOT / "engine_state.db"
    if not db.exists():
        return _check("engine_state_db", WARN, detail="db missing")
    size_mb = db.stat().st_size / (1024 * 1024)
    try:
        with sqlite3.connect(db) as c:
            c.execute("PRAGMA quick_check").fetchall()
    except sqlite3.DatabaseError as e:
        return _check("engine_state_db", CRIT, detail=f"corrupt: {e}")
    if size_mb > 200:
        return _check("engine_state_db", WARN, size_mb=round(size_mb, 1),
                      remediation_hint="vacuum_engine_state")
    return _check("engine_state_db", OK, size_mb=round(size_mb, 1))


# ── Quote staleness on open positions ────────────────────────────────────────

def check_monitor_freshness(stale_threshold_sec: int = 120) -> dict:
    """
    CRIT if any 'open' position has last_monitor_check older than
    stale_threshold_sec during RTH. This catches the 2026-05-04 failure
    mode where the daemon exited at 09:30:26 and no monitor_tick ran for
    the rest of the session — positions sat unmonitored for 6.5 hours.

    Triggers the `relaunch_exit_monitor_daemon` remediation if CRIT.
    """
    now_et = datetime.now(tz=NY)
    rth = (now_et.weekday() < 5
           and datetime.strptime("09:30", "%H:%M").time() <= now_et.time()
           <= datetime.strptime("16:00", "%H:%M").time())
    if not rth:
        return _check("monitor_freshness", OK, detail="outside RTH")

    try:
        from engine.state import init_db, list_open
        init_db()
        opens = [r for r in list_open() if r["status"] == "open"]
    except Exception as e:
        return _check("monitor_freshness", WARN, detail=str(e))

    if not opens:
        return _check("monitor_freshness", OK, count=0)

    now_utc = datetime.now(tz=timezone.utc)
    stale_positions: list[dict] = []
    for r in opens:
        last_str = r.get("last_monitor_check")
        if not last_str:
            stale_positions.append({"occ": r["occ_symbol"], "age_sec": -1,
                                     "reason": "never monitored"})
            continue
        try:
            last_dt = datetime.fromisoformat(str(last_str).replace("Z", "+00:00"))
        except Exception:
            stale_positions.append({"occ": r["occ_symbol"], "age_sec": -1,
                                     "reason": "unparseable timestamp"})
            continue
        age_sec = (now_utc - last_dt).total_seconds()
        if age_sec > stale_threshold_sec:
            stale_positions.append({
                "occ": r["occ_symbol"],
                "age_sec": int(age_sec),
                "last": last_str,
            })

    if stale_positions:
        return _check(
            "monitor_freshness", CRIT,
            stale=stale_positions,
            threshold_sec=stale_threshold_sec,
            remediation_hint="relaunch_exit_monitor_daemon",
        )
    return _check("monitor_freshness", OK, count=len(opens),
                  oldest_age_sec=max((now_utc - datetime.fromisoformat(str(r["last_monitor_check"]).replace("Z", "+00:00"))).total_seconds() for r in opens if r.get("last_monitor_check")))


def check_quote_staleness(threshold_min: float = 5.0) -> dict:
    """Any 'open' position whose last quote is >threshold_min minutes old.
    Only enforced during RTH — quote age is naturally large after-hours."""
    now_et = datetime.now(tz=NY)
    rth = (now_et.weekday() < 5
           and datetime.strptime("09:30", "%H:%M").time() <= now_et.time()
           <= datetime.strptime("16:00", "%H:%M").time())
    if not rth:
        return _check("quote_staleness", OK, detail="outside RTH")
    try:
        from broker import alpaca
        from engine.state import init_db, list_open
        init_db()
        opens = [r for r in list_open() if r["status"] == "open"]
    except Exception as e:
        return _check("quote_staleness", WARN, detail=str(e))

    stale = []
    for r in opens:
        try:
            q = alpaca.get_quote(r["occ_symbol"])
            ts = getattr(q, "timestamp", None) or getattr(q, "ts", None)
            if ts is None:
                continue
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            age_min = (datetime.now(tz=timezone.utc) - ts).total_seconds() / 60.0
            if age_min > threshold_min:
                stale.append({"occ": r["occ_symbol"], "age_min": round(age_min, 1)})
        except Exception:
            continue

    if stale:
        return _check("quote_staleness", WARN, stale=stale)
    return _check("quote_staleness", OK, count=len(opens))


# ── Driver ───────────────────────────────────────────────────────────────────

ALL_CHECKS = [
    check_broker_connectivity,
    check_account_blocked,
    check_sentinel_server,
    check_override_server,
    check_engine_state_drift,
    check_stale_closing_rows,
    check_engine_state_db,
    check_dead_money_contracts,
    check_sentinel_data_freshness,
    check_quote_staleness,
    check_scheduled_tasks,
    check_daily_loss_proximity,
    check_monitor_freshness,   # CRIT if open position unmonitored >120s during RTH
]


def run_checks(only: str | None = None) -> dict:
    started = datetime.now(tz=timezone.utc).isoformat()
    results: list[dict] = []
    for fn in ALL_CHECKS:
        if only and fn.__name__ != f"check_{only}":
            continue
        try:
            results.append(fn())
        except Exception as e:
            results.append(_check(fn.__name__, WARN,
                                   detail=f"check itself failed: {e}"))

    counts = {"OK": 0, "WARN": 0, "CRIT": 0}
    for r in results:
        counts[r.get("status", WARN)] = counts.get(r.get("status", WARN), 0) + 1

    report = {
        "ts": started,
        "summary": counts,
        "checks": results,
    }
    return report


def write_outputs(report: dict) -> None:
    CURRENT_PATH.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    with HISTORY_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(report, default=str) + "\n")


def render_text(report: dict) -> str:
    lines = [f"health @ {report['ts']}",
             f"  summary: OK={report['summary']['OK']} "
             f"WARN={report['summary']['WARN']} CRIT={report['summary']['CRIT']}",
             ""]
    for c in report["checks"]:
        st = c["status"]
        marker = {"OK": "[+]", "WARN": "[!]", "CRIT": "[X]"}.get(st, "[?]")
        extras = " ".join(f"{k}={v}" for k, v in c.items()
                          if k not in ("name", "status"))
        lines.append(f"  {marker} [{st:<4}] {c['name']:<26} {extras}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--check", help="run only the named check (without 'check_' prefix)")
    args = ap.parse_args()

    report = run_checks(only=args.check)
    write_outputs(report)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(render_text(report))

    # Exit code 1 if any CRIT (so cron / monitor caller can react)
    return 1 if report["summary"]["CRIT"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
