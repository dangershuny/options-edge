"""
Autonomous morning run — the whole pipeline in one command.

Designed to be invoked by Windows Task Scheduler at ~9:35 AM ET.

Steps (each isolated in try/except — one failure doesn't stop the others):
  1. Take a fresh snapshot (live prices, now that market is open)
  2. Run flow_news_monitor to capture unusual flow + news context
  3. Verify Alpaca paper account is live + funded
  4. Submit paper trades from the fresh snapshot (--live)
  5. Email a summary of what happened (or what went wrong)

Configurable via env / CLI:
    --bankroll  default 500
    --min-score default 55 (lower than snapshot default to catch more)
    --max-trades default 3 (conservative)
    --dry-run   if set, no live orders submitted (for testing)

All output goes to logs/morning_auto_run_YYYY-MM-DD.log too.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import traceback
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load .env before any broker code
import config_loader  # noqa: F401

from tools.error_alerting import trigger_alert

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


class _Tee:
    """Write to both stdout and a file."""
    def __init__(self, path: Path):
        self.path = path
        self._f = open(path, "a", encoding="utf-8")
        self._stdout = sys.stdout

    def write(self, s):
        self._stdout.write(s)
        self._f.write(s)
        self._f.flush()

    def flush(self):
        self._stdout.flush()
        self._f.flush()

    def close(self):
        self._f.close()


def _python_exe() -> str:
    return sys.executable


def _run_subprocess(cmd: list[str], timeout: int = 600) -> tuple[int, str, str]:
    """Run a subprocess, return (code, stdout, stderr). Never raises."""
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            cwd=REPO_ROOT,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        return proc.returncode, proc.stdout or "", proc.stderr or ""
    except subprocess.TimeoutExpired as e:
        return 124, getattr(e, "stdout", "") or "", f"TIMEOUT after {timeout}s"
    except Exception as e:
        return 99, "", f"SUBPROCESS FAILED: {e}"


def _latest_snapshot() -> Path | None:
    snap_dir = REPO_ROOT / "snapshots"
    candidates = []
    today = date.today()
    for f in snap_dir.glob("*.json"):
        if not f.is_file() or f.parent != snap_dir:
            continue
        # Only accept snapshots written today
        mtime_date = datetime.fromtimestamp(f.stat().st_mtime).date()
        if mtime_date == today:
            candidates.append((f.stat().st_mtime, f))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def step_fresh_snapshot() -> dict:
    print("\n=== STEP 1: Fresh snapshot (market open) ===")
    suffix = f"auto-{datetime.now().strftime('%H%M')}"
    code, out, err = _run_subprocess(
        [_python_exe(), "-m", "tools.snapshot", "--suffix", suffix],
        timeout=600,
    )
    if code != 0:
        print(f"[FAIL] snapshot returned {code}")
        print(f"stderr tail: {err[-500:]}")
        return {"ok": False, "code": code, "error": err[-500:]}
    print(f"[OK] snapshot complete")
    snap = _latest_snapshot()
    return {"ok": True, "snapshot_path": str(snap) if snap else None}


def step_flow_news() -> dict:
    print("\n=== STEP 2: Flow + news monitor ===")
    code, out, err = _run_subprocess(
        [_python_exe(), "-m", "tools.flow_news_monitor", "--quiet"],
        timeout=180,
    )
    if code != 0:
        print(f"[WARN] flow_news_monitor returned {code}")
        return {"ok": False, "code": code}
    print(f"[OK] flow+news complete")
    return {"ok": True}


def step_verify_broker() -> dict:
    print("\n=== STEP 3: Verify broker funded ===")
    try:
        import broker.alpaca as broker_mod
        acct = broker_mod.get_account()
    except Exception as e:
        print(f"[FAIL] broker connection: {e}")
        return {"ok": False, "error": str(e)}

    print(f"Alpaca {'PAPER' if acct.is_paper else 'LIVE'} account:")
    print(f"  Equity:        ${acct.equity:,.2f}")
    print(f"  Cash:          ${acct.cash:,.2f}")
    print(f"  Buying power:  ${acct.buying_power:,.2f}")
    print(f"  Account blocked: {acct.account_blocked}")

    if acct.buying_power < 100:
        print("[FAIL] Buying power below $100 — account needs funding.")
        return {
            "ok": False,
            "error": "paper_account_unfunded",
            "equity": acct.equity,
            "cash": acct.cash,
            "buying_power": acct.buying_power,
        }
    return {"ok": True, "equity": acct.equity, "cash": acct.cash, "buying_power": acct.buying_power}


def step_paper_trade(snapshot_path: Path, bankroll: float, min_score: float,
                     max_trades: int, dry_run: bool, tag: str = "",
                     max_per_trade: float | None = None) -> dict:
    tier_label = tag or f"bankroll-{int(bankroll)}"
    print(f"\n--- Paper trades — tier {tier_label} "
          f"({'DRY RUN' if dry_run else 'LIVE'}) ---")
    cmd = [
        _python_exe(), "-m", "tools.paper_trade",
        "--snapshot", str(snapshot_path),
        "--bankroll", str(bankroll),
        "--min-score", str(min_score),
        "--max-trades", str(max_trades),
        "--tag", tag,
    ]
    if max_per_trade is not None:
        cmd.extend(["--max-per-trade", str(max_per_trade)])
    if not dry_run:
        cmd.append("--live")

    code, out, err = _run_subprocess(cmd, timeout=300)
    print(out)
    if err and code != 0:
        print(f"stderr tail: {err[-500:]}")
    if code != 0:
        return {"ok": False, "code": code, "error": err[-500:], "tag": tag}

    # Parse the paper_trades.jsonl for today's new entries matching this tag
    log_path = REPO_ROOT / "logs" / "paper_trades.jsonl"
    today_iso = date.today().isoformat()
    orders_tagged = []
    if log_path.exists():
        try:
            with open(log_path) as f:
                for line in f:
                    try:
                        o = json.loads(line.strip())
                    except Exception:
                        continue
                    if (o.get("timestamp", "").startswith(today_iso)
                            and o.get("tag") == tag):
                        orders_tagged.append(o)
        except Exception:
            pass

    return {
        "ok": True, "tag": tag, "bankroll": bankroll,
        "orders_today": orders_tagged, "stdout_tail": out[-1200:],
    }


def step_paper_trade_all_tiers(snapshot_path: Path, dry_run: bool,
                               tiers: list[dict]) -> list[dict]:
    """Run paper_trade once per tier, each with its own tag."""
    print(f"\n=== STEP 4: Paper trades across {len(tiers)} tier(s) "
          f"({'DRY RUN' if dry_run else 'LIVE'}) ===")
    results = []
    for tier in tiers:
        r = step_paper_trade(
            snapshot_path=snapshot_path,
            bankroll=tier["bankroll"],
            min_score=tier["min_score"],
            max_trades=tier["max_trades"],
            dry_run=dry_run,
            tag=tier["tag"],
            max_per_trade=tier.get("max_per_trade"),
        )
        results.append(r)
    return results


def default_tiers(min_score: float, max_trades: int) -> list[dict]:
    """Three-tier bankroll simulation. Same signals, different risk envelopes."""
    return [
        {"tag": "sim500",  "bankroll": 500,  "min_score": min_score,
         "max_trades": max_trades, "max_per_trade": 75},
        {"tag": "sim1000", "bankroll": 1000, "min_score": min_score,
         "max_trades": max_trades + 2, "max_per_trade": 150},
        {"tag": "sim2000", "bankroll": 2000, "min_score": min_score,
         "max_trades": max_trades + 5, "max_per_trade": 300},
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-score", type=float, default=60.0,
                        help="Min signal score (default 60)")
    parser.add_argument("--max-trades", type=int, default=3,
                        help="Max trades for smallest tier; larger tiers add +2 and +5")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--single-tier", type=str, default=None,
                        help="Override: only run one tier (sim500/sim1000/sim2000)")
    args = parser.parse_args()

    today = date.today().isoformat()
    log_path = LOG_DIR / f"morning_auto_run_{today}.log"
    tee = _Tee(log_path)
    sys.stdout = tee

    tiers = default_tiers(args.min_score, args.max_trades)
    if args.single_tier:
        tiers = [t for t in tiers if t["tag"] == args.single_tier]
        if not tiers:
            print(f"Unknown tier: {args.single_tier}")
            return 1

    start_ts = datetime.now(timezone.utc)
    print(f"\n{'#' * 60}")
    print(f"#  AUTONOMOUS MORNING RUN — {start_ts.isoformat()}")
    tier_labels = [f"{t['tag']} (${t['bankroll']})" for t in tiers]
    print(f"#  Tiers: {', '.join(tier_labels)}")
    print(f"#  Min score: {args.min_score} | Max trades (small): {args.max_trades}")
    print(f"#  Mode: {'DRY RUN' if args.dry_run else 'LIVE PAPER TRADING'}")
    print(f"{'#' * 60}\n")

    summary: dict = {
        "start": start_ts.isoformat(),
        "dry_run": args.dry_run,
        "tiers": tiers,
        "min_score": args.min_score,
    }

    try:
        # Step 1: snapshot
        s1 = step_fresh_snapshot()
        summary["snapshot"] = s1
        snap_path = Path(s1.get("snapshot_path") or "")

        # Step 2: flow + news (don't block on failure)
        s2 = step_flow_news()
        summary["flow_news"] = s2

        # Step 3: verify broker
        s3 = step_verify_broker()
        summary["broker"] = s3

        # Step 4: paper trade — one pass per tier
        if s3.get("ok") or args.dry_run:
            if snap_path.exists():
                tier_results = step_paper_trade_all_tiers(
                    snap_path, args.dry_run, tiers,
                )
                summary["paper_trades_by_tier"] = tier_results
            else:
                summary["paper_trades_by_tier"] = [{"ok": False, "error": "no snapshot"}]
                print("\n[SKIP] No snapshot available for paper trading.")
        else:
            summary["paper_trades_by_tier"] = [{"ok": False, "error": "broker not ready"}]
            print(f"\n[SKIP] Skipping all tiers because broker check failed.")

    except Exception as e:
        print(f"\nFATAL: {e}")
        print(traceback.format_exc())
        summary["fatal"] = str(e)

    end_ts = datetime.now(timezone.utc)
    summary["end"] = end_ts.isoformat()
    summary["elapsed_sec"] = (end_ts - start_ts).total_seconds()

    # Write summary JSON
    summary_path = LOG_DIR / f"morning_auto_run_{today}.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str))

    print(f"\n{'=' * 60}")
    print(f"  DONE in {summary['elapsed_sec']:.1f}s")
    print(f"  Summary: {summary_path}")
    print(f"  Log:     {log_path}")
    print(f"{'=' * 60}")

    # Email alert — always, so user knows what happened
    sev = "INFO"
    status_bits = []
    if summary.get("snapshot", {}).get("ok"):
        status_bits.append("snapshot OK")
    else:
        status_bits.append("snapshot FAIL")
        sev = "ERROR"
    if summary.get("broker", {}).get("ok"):
        bp = summary["broker"].get("buying_power", 0)
        status_bits.append(f"broker OK (${bp:,.0f} buying power)")
    else:
        status_bits.append(f"broker NOT READY: {summary.get('broker', {}).get('error', 'unknown')}")
        if summary.get("broker", {}).get("error") == "paper_account_unfunded":
            sev = "ERROR"

    tier_results = summary.get("paper_trades_by_tier", [])
    tier_summary_lines = []
    tier_context = []
    for tr in tier_results:
        tag = tr.get("tag", "?")
        if not tr.get("ok"):
            tier_summary_lines.append(f"{tag}: FAILED ({str(tr.get('error', ''))[:60]})")
            tier_context.append({"tag": tag, "error": tr.get("error")})
            continue
        orders = tr.get("orders_today", [])
        sub = sum(1 for o in orders if o.get("status") == "submitted")
        dry = sum(1 for o in orders if o.get("status") == "dry_run")
        skp = sum(1 for o in orders if o.get("status") == "skipped")
        fld = sum(1 for o in orders if o.get("status") == "failed")
        bits = f"{sub}S/{dry}D/{skp}K/{fld}F"
        tier_summary_lines.append(f"{tag} (${tr.get('bankroll'):,.0f}): {bits}")
        tier_context.append({
            "tag": tag, "bankroll": tr.get("bankroll"),
            "submitted": sub, "dry_run": dry, "skipped": skp, "failed": fld,
            "orders": [
                {
                    "symbol": o.get("symbol"), "type": o.get("option_type"),
                    "strike": o.get("strike"), "expiry": o.get("expiry"),
                    "cost": o.get("total_cost"),
                    "client_order_id": o.get("client_order_id"),
                    "status": o.get("status"),
                    "error": o.get("error"),
                } for o in orders
            ],
        })
    status_bits.append("tiers: " + "; ".join(tier_summary_lines))

    msg_summary = "; ".join(status_bits)

    try:
        trigger_alert(
            source="morning_auto_run",
            error_code="morning_summary" if sev == "INFO" else "morning_failure",
            message=msg_summary,
            symbol="",
            severity=sev,
            context={
                "tiers": tier_context,
                "dry_run": args.dry_run,
                "summary_file": str(summary_path),
                "log_file": str(log_path),
                "broker": summary.get("broker", {}),
                "elapsed_sec": summary["elapsed_sec"],
            },
        )
    except Exception as e:
        print(f"(alert failed: {e})")

    sys.stdout = tee._stdout
    tee.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
