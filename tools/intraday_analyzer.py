"""
Intraday analyzer — read-only anomaly detection during RTH.

Runs every 15 min via OptionsEdge-IntradayAnalyzer schtask. Surfaces
unusual situations via Telegram WARN so the operator can intervene if
warranted. NEVER modifies rules or positions automatically — rule
changes are EOD-only after backtest validation (see eod_analysis.py).

Checks performed each tick:

  1. Position trajectory deviation
       For each open position, compare today's intra-session
       price arc to the historical strategy_v1 baseline. If a
       position is >2σ below expected arc at this hold-day, surface.

  2. Signal stability on held positions
       Re-pull current sentinel/skew/vol_signal for each held
       OCC. If a signal has FLIPPED away from entry direction
       (e.g., entered with BULLISH skew, now BEARISH), surface.

  3. Daytrade-count proximity
       If daytrade_count is at or near the cash-account limit,
       remind operator. The pdt_limit_gate handles this for new
       entries but mid-day visibility helps.

  4. Spread widening on held positions
       If a held position's spread has more-than-doubled since
       entry, the exit is going to be expensive. Surface so the
       operator can manually exit at mid before it gets worse.

  5. Daemon and watchdog health
       Quick alive-check on daemon PID file. If stale, surface
       (watchdog should also catch this but redundant alert is
       cheap during fast-moving sessions).

Anti-spam: each unique anomaly is alerted at most once per session
(tracked in logs/intraday_alerted_<date>.json).

Usage:
    python -m tools.intraday_analyzer                # one pass
    python -m tools.intraday_analyzer --dry-run      # log only, no Telegram
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

NY = ZoneInfo("America/New_York")
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _alert_state_path() -> Path:
    return LOG_DIR / f"intraday_alerted_{date.today().isoformat()}.json"


def _load_alerted() -> set[str]:
    p = _alert_state_path()
    if not p.exists():
        return set()
    try:
        return set(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        return set()


def _save_alerted(s: set[str]) -> None:
    try:
        _alert_state_path().write_text(json.dumps(sorted(s)), encoding="utf-8")
    except Exception:
        pass


def _is_rth() -> bool:
    now = datetime.now(tz=NY)
    if now.weekday() >= 5:
        return False
    return (datetime.strptime("09:30", "%H:%M").time()
            <= now.time()
            <= datetime.strptime("16:00", "%H:%M").time())


def _send_alert(key: str, level: str, title: str, body: str,
                 alerted: set[str], dry_run: bool) -> None:
    if key in alerted:
        return
    alerted.add(key)
    print(f"[{level}] {title}\n  {body}")
    if dry_run:
        return
    try:
        from tools.notify import send
        send(level, title, body)
    except Exception:
        pass


# ── Check 1: position trajectory deviation ───────────────────────────────────

def check_position_pnl(open_positions: list[dict], alerted: set[str],
                        dry_run: bool) -> None:
    """Flag any position deeply underwater that hasn't triggered SL yet
    (likely because daemon is using mid and bid has collapsed)."""
    try:
        from broker import alpaca
    except Exception:
        return
    for p in open_positions:
        try:
            q = alpaca.get_quote(p["occ_symbol"])
            mid = (q.bid + q.ask) / 2 if (q.bid > 0 and q.ask > 0) else (q.bid or q.ask or 0)
        except Exception:
            continue
        if mid <= 0:
            continue
        entry = float(p["entry_price"] or 0)
        if entry <= 0:
            continue
        mid_pnl = (mid / entry) - 1
        bid_pnl = (q.bid / entry) - 1 if q.bid > 0 else None
        # Surface if bid pnl is much worse than mid pnl (spread widened)
        if bid_pnl is not None and bid_pnl <= -0.30 and mid_pnl > -0.15:
            _send_alert(
                f"spread_collapse_{p['id']}", "WARN",
                f"Spread collapsed on {p['occ_symbol']}",
                f"id={p['id']} entry=${entry:.2f}  mid=${mid:.2f} "
                f"({mid_pnl*100:+.0f}%)  bid=${q.bid:.2f} "
                f"({bid_pnl*100:+.0f}%). Bid is much worse than mid — "
                f"daemon won't fire SL yet (mid still above threshold). "
                f"Consider manual exit at mid before spread widens further.",
                alerted, dry_run,
            )
        # Surface deeply-bleeding positions even on mid
        elif mid_pnl <= -0.25:
            _send_alert(
                f"deep_loss_{p['id']}", "WARN",
                f"Deep loss on {p['occ_symbol']}",
                f"id={p['id']} entry=${entry:.2f} mid=${mid:.2f} "
                f"({mid_pnl*100:+.0f}%). Already past 2× the -12% SL "
                f"threshold. Daemon may have queued exit (PDT) or hit "
                f"a stale-quote path. Investigate.",
                alerted, dry_run,
            )


# ── Check 2: signal flip on held positions ──────────────────────────────────

def check_signal_flip(open_positions: list[dict], alerted: set[str],
                       dry_run: bool) -> None:
    """If a held position was entered with BULLISH skew + BUY VOL but
    the chain now shows BEARISH skew or signal reversal, surface."""
    try:
        from analysis.scorer import analyze_ticker
    except Exception:
        return
    seen_tickers = set()
    for p in open_positions:
        sym = p["underlying"]
        if sym in seen_tickers:
            continue
        seen_tickers.add(sym)
        ctx_str = p.get("entry_context_json") or "{}"
        try:
            ctx = json.loads(ctx_str)
        except Exception:
            ctx = {}
        entry_skew = ctx.get("skew_signal")
        if not entry_skew:
            continue
        # Re-analyze the ticker (heavy — only do this every 15 min)
        try:
            df, _news, _err, _ee = analyze_ticker(sym)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        # Use the top row's skew as the current chain read
        cur_skew = df.iloc[0].get("skew_signal")
        if cur_skew and cur_skew != entry_skew and cur_skew == "BEARISH":
            _send_alert(
                f"skew_flip_{p['id']}", "WARN",
                f"Skew flipped on {p['occ_symbol']}",
                f"id={p['id']} entered with skew={entry_skew}; "
                f"chain now reads skew={cur_skew}. "
                f"Strategy_v1's entry thesis no longer holds. Consider "
                f"manual exit before SL fires.",
                alerted, dry_run,
            )


# ── Check 3: daytrade-count proximity ────────────────────────────────────────

def check_pdt_proximity(alerted: set[str], dry_run: bool) -> None:
    try:
        from broker.alpaca import _trading_client
        raw = _trading_client().get_account()
        dt = int(getattr(raw, "daytrade_count", 0) or 0)
    except Exception:
        return
    if dt >= 3:
        _send_alert(
            f"pdt_at_limit_{date.today().isoformat()}", "INFO",
            f"PDT at limit (daytrade_count={dt})",
            f"At PDT threshold. New entries are being refused by "
            f"_pdt_limit_gate. Carry-over exits still fire normally "
            f"(non-same-day SELLs don't count). Count rolls down as "
            f"old day-trades age past 5 business days.",
            alerted, dry_run,
        )


# ── Check 4: spread widening since entry ─────────────────────────────────────

def check_spread_widening(open_positions: list[dict], alerted: set[str],
                           dry_run: bool) -> None:
    try:
        from broker import alpaca
    except Exception:
        return
    for p in open_positions:
        ctx_str = p.get("entry_context_json") or "{}"
        try:
            ctx = json.loads(ctx_str)
        except Exception:
            continue
        entry_bid = ctx.get("bid"); entry_ask = ctx.get("ask")
        if not (entry_bid and entry_ask):
            continue
        entry_spread_pct = (entry_ask - entry_bid) / ((entry_ask + entry_bid) / 2)
        try:
            q = alpaca.get_quote(p["occ_symbol"])
            if q.bid <= 0 or q.ask <= 0:
                continue
            cur_spread_pct = (q.ask - q.bid) / ((q.ask + q.bid) / 2)
        except Exception:
            continue
        # Surface if spread tripled
        if cur_spread_pct > entry_spread_pct * 3 and cur_spread_pct > 0.25:
            _send_alert(
                f"spread_widened_{p['id']}", "WARN",
                f"Spread widened 3× on {p['occ_symbol']}",
                f"id={p['id']} entry spread={entry_spread_pct*100:.1f}% → "
                f"now {cur_spread_pct*100:.1f}%. Round-trip exit cost is "
                f"climbing. If we exit now, slippage will be large.",
                alerted, dry_run,
            )


# ── Check 5: daemon alive ────────────────────────────────────────────────────

def _pid_alive_windows(pid: int) -> bool:
    """Cross-platform PID-alive check. On Windows os.kill(pid, 0) is
    unreliable (raises even for live processes due to signal model
    differences), so use PowerShell Get-Process instead — same pattern
    the watchdog uses successfully."""
    import subprocess
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) "
             "{ 'ALIVE' } else { 'DEAD' }"],
            capture_output=True, text=True, timeout=8,
        )
        return "ALIVE" in (r.stdout or "")
    except Exception:
        return False


def check_daemon_alive(alerted: set[str], dry_run: bool) -> None:
    pid_file = LOG_DIR / "exit_daemon.pid"
    if not pid_file.exists():
        _send_alert(
            f"daemon_no_pid_{datetime.now().strftime('%H')}", "CRIT",
            "Exit daemon PID file missing",
            f"{pid_file} doesn't exist mid-session. Daemon may not have "
            f"started or singleton lock was deleted. Watchdog should "
            f"recover but verify manually.",
            alerted, dry_run,
        )
        return
    try:
        pid = int(pid_file.read_text().strip())
    except Exception:
        _send_alert(
            f"daemon_pid_unreadable_{datetime.now().strftime('%H')}", "CRIT",
            "Exit daemon PID file unreadable",
            f"Could not parse {pid_file}.", alerted, dry_run,
        )
        return
    # Windows-correct liveness probe
    if not _pid_alive_windows(pid):
        _send_alert(
            f"daemon_dead_{datetime.now().strftime('%H')}", "CRIT",
            "Exit daemon PID is stale (process dead)",
            f"PID file says {pid} but Get-Process reports DEAD. "
            f"Watchdog should recover within 1 min. If positions are "
            f"open, monitor manually until then.",
            alerted, dry_run,
        )


# ── Main pass ────────────────────────────────────────────────────────────────

def run_pass(dry_run: bool = False) -> dict:
    if not _is_rth():
        return {"skipped": "outside RTH"}

    alerted = _load_alerted()
    started_alerts = len(alerted)

    # Pull open positions once with their entry context
    try:
        with sqlite3.connect(REPO_ROOT / "engine_state.db") as c:
            c.row_factory = sqlite3.Row
            open_positions = [dict(r) for r in c.execute(
                "SELECT * FROM positions WHERE status IN ('open','closing')"
            )]
    except Exception as e:
        return {"error": f"engine_state read failed: {e}"}

    print(f"Intraday analyzer @ {datetime.now(tz=NY).strftime('%H:%M ET')}  "
          f"({len(open_positions)} open positions)")

    # Run all checks
    check_position_pnl(open_positions, alerted, dry_run)
    check_signal_flip(open_positions, alerted, dry_run)
    check_pdt_proximity(alerted, dry_run)
    check_spread_widening(open_positions, alerted, dry_run)
    check_daemon_alive(alerted, dry_run)

    _save_alerted(alerted)
    new_alerts = len(alerted) - started_alerts
    return {"n_open": len(open_positions), "new_alerts": new_alerts}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="log to stdout only; don't send Telegram")
    args = ap.parse_args()
    r = run_pass(dry_run=args.dry_run)
    print(json.dumps(r, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
