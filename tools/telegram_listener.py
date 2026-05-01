"""
Telegram listener — long-polls Telegram for incoming commands and dispatches
them to handlers that wrap our existing remediations / broker calls.

Authorization: only responds to messages from TELEGRAM_CHAT_ID. Anyone else
gets ignored (no reply at all to avoid leaking that the bot exists).

Long-poll loop with 25s timeout. Persists last update_id to
`logs/telegram_offset.txt` so a restart doesn't re-process old messages.

Failures inside a handler are caught + replied as text. Failures in the loop
itself are logged + retried after 10s. The wrapper bat keeps the process up.

Commands:
  /start /help          — help text
  /health               — runs health_check, returns the rendered summary
  /positions            — live Alpaca positions with P/L
  /halt                 — write halt_buys flag (paper_trade refuses entries)
  /resume               — clear halt
  /blacklist <OCC>      — add OCC to today's blacklist
  /unblacklist <OCC>    — remove from today's blacklist
  /cancel <OCC|all>     — cancel pending order(s) for that contract
  /close <OCC>          — submit close via engine.execute._execute_exit
  /restart sentinel     — restart news_sentinel server
  /restart override     — restart override server scheduled task
  /diagnose             — last 15 lines from each major runner log

Usage:
    python -m tools.telegram_listener
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
OFFSET_FILE = LOG_DIR / "telegram_offset.txt"

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
API = f"https://api.telegram.org/bot{TOKEN}"


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _reply(chat_id: str | int, text: str) -> None:
    """Send a plain-text reply. Truncates to Telegram's 4096-char limit."""
    text = (text or "(no output)")[:3900]
    data = urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    try:
        urlopen(Request(f"{API}/sendMessage", data=data, method="POST"), timeout=8).read()
    except (URLError, HTTPError, OSError) as e:
        _log(f"reply send failed: {e}")


# ── Command handlers ────────────────────────────────────────────────────────

def cmd_help(args: list[str]) -> str:
    return (
        "commands:\n"
        "  /health              health check summary\n"
        "  /positions           live positions\n"
        "  /halt                stop new entries today\n"
        "  /resume              clear halt\n"
        "  /blacklist <OCC>     skip this contract today\n"
        "  /unblacklist <OCC>   un-skip\n"
        "  /cancel <OCC|all>    cancel pending order(s)\n"
        "  /close <OCC>         submit close order\n"
        "  /restart sentinel    restart news server\n"
        "  /restart override    restart override server\n"
        "  /diagnose            last lines of each runner log\n"
        "  /report [YYYY-MM-DD] today's EOD analysis (or a past day)\n"
        "  /proposals [date]    proposals from EOD analysis"
    )


def cmd_health(args: list[str]) -> str:
    from tools.health_check import run_checks, render_text
    report = run_checks()
    return render_text(report)


def cmd_positions(args: list[str]) -> str:
    from broker import alpaca
    try:
        acct = alpaca.get_account()
        positions = alpaca.get_positions()
    except Exception as e:
        return f"alpaca error: {e}"
    head = f"equity ${acct.equity:,.2f} cash ${acct.cash:,.2f}"
    if not positions:
        return head + "\n(no open positions)"
    lines = [head]
    for p in positions:
        pl_pct = ((p.mark / p.avg_entry) - 1) * 100 if p.avg_entry else 0
        lines.append(
            f"  {p.symbol} qty={p.qty} entry=${p.avg_entry:.2f} "
            f"mark=${p.mark:.2f} pl=${p.unrealized_pl:+.0f} ({pl_pct:+.1f}%)"
        )
    return "\n".join(lines)


def cmd_halt(args: list[str]) -> str:
    flag = LOG_DIR / f"halt_buys_{date.today().isoformat()}.flag"
    flag.write_text(json.dumps({
        "reason": "manual via telegram",
        "ts": datetime.now(timezone.utc).isoformat(),
    }), encoding="utf-8")
    return f"halt set: {flag.name}"


def cmd_resume(args: list[str]) -> str:
    flag = LOG_DIR / f"halt_buys_{date.today().isoformat()}.flag"
    if flag.exists():
        flag.unlink()
        return "halt cleared"
    return "no halt active"


def _blacklist_path() -> Path:
    return LOG_DIR / f"contract_blacklist_{date.today().isoformat()}.json"


def cmd_blacklist(args: list[str]) -> str:
    if not args:
        return "usage: /blacklist <OCC_SYMBOL>"
    occ = args[0].upper().strip()
    path = _blacklist_path()
    existing: list[str] = []
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    if occ in existing:
        return f"already blacklisted: {occ}"
    existing.append(occ)
    path.write_text(json.dumps(sorted(set(existing))), encoding="utf-8")
    return f"blacklisted {occ} (total today: {len(set(existing))})"


def cmd_unblacklist(args: list[str]) -> str:
    if not args:
        return "usage: /unblacklist <OCC_SYMBOL>"
    occ = args[0].upper().strip()
    path = _blacklist_path()
    if not path.exists():
        return "no blacklist file today"
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        existing = []
    if occ not in existing:
        return f"not blacklisted: {occ}"
    existing = [x for x in existing if x != occ]
    path.write_text(json.dumps(sorted(set(existing))), encoding="utf-8")
    return f"removed {occ} from blacklist"


def cmd_cancel(args: list[str]) -> str:
    if not args:
        return "usage: /cancel <OCC|all>"
    target = args[0].upper().strip()
    try:
        from broker import alpaca
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        c = alpaca._trading_client()
        orders = c.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=50))
    except Exception as e:
        return f"alpaca error: {e}"

    canceled = []
    failed = []
    for o in orders:
        if target != "ALL" and o.symbol != target:
            continue
        try:
            c.cancel_order_by_id(o.id)
            canceled.append(o.symbol)
        except Exception as e:
            failed.append(f"{o.symbol}: {e}")

    msg = f"canceled {len(canceled)}: {', '.join(canceled) or '(none)'}"
    if failed:
        msg += f"\nfailed: {failed}"
    return msg


def cmd_close(args: list[str]) -> str:
    if not args:
        return "usage: /close <OCC>"
    occ = args[0].upper().strip()
    try:
        from engine.state import init_db, list_open
        from engine.execute import _execute_exit
        init_db()
        rows = [r for r in list_open()
                if r["occ_symbol"] == occ and r["status"] == "open"]
    except Exception as e:
        return f"engine error: {e}"

    if rows:
        try:
            _execute_exit(rows[0], "manual close via telegram", urgent=True)
            return f"close submitted for {occ} (engine row id={rows[0]['id']})"
        except Exception as e:
            return f"_execute_exit failed: {e}"

    # Fall back to direct broker close (untracked position)
    try:
        from broker import alpaca
        c = alpaca._trading_client()
        c.close_position(occ)
        return f"close submitted for {occ} (broker direct, not in engine state)"
    except Exception as e:
        return f"close failed: {e}"


def cmd_restart(args: list[str]) -> str:
    if not args:
        return "usage: /restart <sentinel|override>"
    target = args[0].lower().strip()
    if target == "sentinel":
        try:
            from sentinel_bridge import ensure_sentinel_running
            return f"sentinel: ensure_running={ensure_sentinel_running()}"
        except Exception as e:
            return f"sentinel restart err: {e}"
    if target == "override":
        try:
            r = subprocess.run(
                ["schtasks", "/Run", "/TN", "OptionsEdge-OverrideServer"],
                capture_output=True, text=True, timeout=15,
            )
            return f"override: rc={r.returncode} {(r.stdout or r.stderr).strip()}"
        except Exception as e:
            return f"override restart err: {e}"
    return f"unknown service: {target} (use sentinel|override)"


def cmd_report(args: list[str]) -> str:
    """Send today's EOD analysis markdown. If a date arg is provided, fetch
    that day's report instead."""
    iso = args[0] if args else date.today().isoformat()
    md = LOG_DIR / f"eod-analysis-{iso}.md"
    if not md.exists():
        return (f"no report for {iso} yet. "
                f"EOD analysis runs at 16:45 ET — "
                f"or invoke `python -m tools.eod_analysis` directly.")
    text = md.read_text(encoding="utf-8", errors="replace")
    # 4096-char telegram cap; prioritize the proposals section
    if len(text) > 3800:
        prop_idx = text.find("## Proposals for review")
        if prop_idx > 0:
            head = text[:1500]
            tail = text[prop_idx:prop_idx + 2200]
            text = head + "\n\n[...]\n\n" + tail
        else:
            text = text[:3800]
    return text


def cmd_proposals(args: list[str]) -> str:
    """List today's structured proposals, one per line."""
    iso = args[0] if args else date.today().isoformat()
    pp = LOG_DIR / f"eod-proposals-{iso}.json"
    if not pp.exists():
        return f"no proposals for {iso} (eod_analysis hasn't run yet)"
    try:
        items = json.loads(pp.read_text(encoding="utf-8"))
    except Exception as e:
        return f"could not read proposals: {e}"
    if not items:
        return f"no proposals for {iso} — quiet day"
    lines = [f"{len(items)} proposals for {iso}:"]
    for p in items:
        lines.append(f"\n[{p['risk']}] {p['title']}")
        lines.append(f"  why: {p['rationale']}")
        lines.append(f"  suggest: {p['suggested_action']}")
    return "\n".join(lines)[:3800]


def cmd_diagnose(args: list[str]) -> str:
    """Tail the most relevant runner logs."""
    today = date.today().isoformat()
    candidates = [
        f"exit-monitor-runner.log",
        f"eod-session-runner.log",
        f"health-runner.log",
        f"morning_auto_run_{today}.log",
        f"telegram-listener.log",
    ]
    out: list[str] = []
    for name in candidates:
        p = LOG_DIR / name
        if not p.exists():
            continue
        try:
            tail = p.read_text(encoding="utf-8", errors="replace").splitlines()[-12:]
            out.append(f"=== {name} (last 12) ===\n" + "\n".join(tail))
        except Exception as e:
            out.append(f"=== {name}: read err {e} ===")
    return ("\n\n".join(out) or "no runner logs found")[:3800]


HANDLERS: dict[str, callable] = {
    "/start": cmd_help,
    "/help": cmd_help,
    "/health": cmd_health,
    "/positions": cmd_positions,
    "/halt": cmd_halt,
    "/resume": cmd_resume,
    "/blacklist": cmd_blacklist,
    "/unblacklist": cmd_unblacklist,
    "/cancel": cmd_cancel,
    "/close": cmd_close,
    "/restart": cmd_restart,
    "/diagnose": cmd_diagnose,
    "/report": cmd_report,
    "/proposals": cmd_proposals,
}


# ── Long-poll loop ──────────────────────────────────────────────────────────

def _load_offset() -> int:
    if OFFSET_FILE.exists():
        try:
            return int(OFFSET_FILE.read_text(encoding="utf-8").strip() or 0)
        except Exception:
            return 0
    return 0


def _save_offset(offset: int) -> None:
    try:
        OFFSET_FILE.write_text(str(offset), encoding="utf-8")
    except Exception:
        pass


def _dispatch(text: str) -> str:
    parts = shlex.split(text)
    if not parts:
        return "(empty)"
    cmd = parts[0].lower()
    args = parts[1:]
    handler = HANDLERS.get(cmd)
    if handler is None:
        return f"unknown command: {cmd}\n\n" + cmd_help([])
    try:
        return handler(args)
    except Exception:
        return f"handler error in {cmd}:\n{traceback.format_exc()[-1500:]}"


def main() -> int:
    if not TOKEN or not CHAT_ID:
        _log("missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return 2

    _log(f"listener starting; chat_id={CHAT_ID}")
    offset = _load_offset()
    backoff = 1

    while True:
        try:
            url = f"{API}/getUpdates?offset={offset + 1}&timeout=25"
            with urlopen(url, timeout=30) as r:
                data = json.loads(r.read())
            if not data.get("ok"):
                _log(f"getUpdates not OK: {data}")
                time.sleep(min(backoff, 30))
                backoff *= 2
                continue
            backoff = 1

            for update in data.get("result", []):
                offset = max(offset, update["update_id"])
                msg = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                from_id = str(msg.get("from", {}).get("id", ""))
                text = (msg.get("text") or "").strip()
                if not text:
                    continue
                if from_id != CHAT_ID:
                    _log(f"ignoring message from unauthorized chat={from_id}")
                    continue

                _log(f"command: {text[:200]}")
                response = _dispatch(text)
                _reply(msg["chat"]["id"], response)

            _save_offset(offset)

        except (URLError, HTTPError, OSError, TimeoutError) as e:
            _log(f"network error: {e} — backoff {backoff}s")
            time.sleep(min(backoff, 30))
            backoff *= 2
        except Exception:
            _log(f"loop error:\n{traceback.format_exc()[-1500:]}")
            time.sleep(10)


if __name__ == "__main__":
    sys.exit(main())
