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
        "  /proposals [date]    proposals from EOD analysis\n"
        "  /debug <issue>       spawn Claude (plan mode) to investigate\n"
        "  /apply [debug-id]    apply the proposal from a /debug (defaults to latest)\n"
        "  /restart_listener    relaunch the listener to pick up code edits"
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


def _send_async_telegram(text: str) -> None:
    """Send a Telegram message NOT in response to a poll (used by background
    workers). Same auth as the main reply path."""
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not chat_id or not token:
        return
    text = (text or "")[:3900]
    api = f"https://api.telegram.org/bot{token}"
    try:
        from urllib.parse import urlencode as _ue
        from urllib.request import Request as _Req, urlopen as _uo
        data = _ue({"chat_id": chat_id, "text": text}).encode("utf-8")
        _uo(_Req(f"{api}/sendMessage", data=data, method="POST"), timeout=8).read()
    except Exception as e:
        _log(f"async send failed: {e}")


def _claude_invoke(prompt: str, permission_mode: str, timeout_sec: int) -> tuple[int, str, str]:
    """Run the claude CLI with stdin-fed prompt + shell=True (matches
    momentum-edge's pattern that works across scheduled-task / interactive /
    Startup-folder contexts). Returns (rc, stdout, stderr).

    Note: we do NOT pre-check os.path.exists(cli). On certain Windows
    process contexts (the listener's specific token-inheritance chain),
    direct os.path.exists on files in %APPDATA%\\Roaming\\npm returns
    False even when the file exists and other process contexts can read
    it. The subprocess invocation via shell=True routes through cmd.exe
    which resolves the path correctly regardless. If the path is truly
    invalid, subprocess will fail with FileNotFoundError or a non-zero
    rc, surfaced cleanly to the caller."""
    cli = os.environ.get("CLAUDE_CLI_PATH")
    if not cli:
        return -1, "", "CLAUDE_CLI_PATH not set in .env"
    cli_args = [cli, "--permission-mode", permission_mode,
                "--add-dir", str(REPO_ROOT), "--print"]
    try:
        r = subprocess.run(
            cli_args,
            input=prompt,
            cwd=str(REPO_ROOT),
            capture_output=True, text=True, timeout=timeout_sec,
            encoding="utf-8", errors="replace",
            shell=True,
        )
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return -2, "", f"claude timed out after {timeout_sec}s"
    except Exception as e:
        return -3, "", f"{type(e).__name__}: {e}"


def _debug_worker(debug_id: str, issue: str) -> None:
    """Background thread — runs claude (plan mode), sends result via
    async Telegram message so the main poll loop never blocks."""
    prompt = (
        "You are debugging the options-edge trading repo (project root is "
        "the directory you are in). Diagnose this issue using read-only "
        "tools (Read, Grep, Glob, Bash for queries — DO NOT edit files, "
        "do not submit broker orders, do not run destructive commands). "
        "Be concise.\n\n"
        f"Issue: {issue}\n\n"
        "Respond with: (1) what you found, (2) most likely root cause, "
        "(3) a specific proposed fix with file paths and exact change. "
        "Keep total output under 3000 characters."
    )
    rc, out, err = _claude_invoke(prompt, "plan", 300)
    log_path = LOG_DIR / f"debug-{debug_id}.log"
    log_path.write_text(
        f"prompt:\n{prompt}\n\n--- stdout ---\n{out}\n\n--- stderr ---\n{err}\n",
        encoding="utf-8",
    )
    (LOG_DIR / "debug-latest.txt").write_text(debug_id, encoding="utf-8")

    if rc != 0 and not out:
        _send_async_telegram(f"debug-{debug_id} failed (rc={rc}):\n{err[:1500]}")
        return
    if not out:
        _send_async_telegram(f"debug-{debug_id} empty output (rc={rc})")
        return

    header = (f"debug-{debug_id} done.\n"
              f"Reply /apply or /apply {debug_id} to action.\n\n")
    body = out if len(out) <= 3300 else (
        f"[truncated — full output in {log_path.name}]\n\n" + out[-3000:]
    )
    _send_async_telegram(header + body)


def cmd_debug(args: list[str]) -> str:
    """Spawn Claude (plan mode, read-only) on a background thread so the
    listener keeps polling Telegram during the 30-60s claude takes. Result
    is delivered as a follow-up message when ready."""
    if not args:
        return ("usage: /debug <describe issue>\n"
                "example: /debug why is BBAI 5-22 still in 'closing' status "
                "with no exit fill?")
    issue = " ".join(args).strip()
    cli = os.environ.get("CLAUDE_CLI_PATH")
    if not cli:
        return ("CLAUDE_CLI_PATH not set in .env. Add the path to claude.exe "
                "(forward slashes — dotenv eats backslashes).")
    # NOTE: do NOT pre-check os.path.exists(cli). The listener process can
    # have spurious False here even when the file is reachable via subprocess
    # shell=True. _claude_invoke handles real failures.

    debug_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    import threading
    threading.Thread(
        target=_debug_worker, args=(debug_id, issue),
        daemon=True, name=f"debug-{debug_id}",
    ).start()
    return (f"debug-{debug_id} started (Claude plan mode, ~30-60s).\n"
            f"You'll get a follow-up message with the diagnosis. The listener "
            f"stays responsive — other commands still work in the meantime.")


def cmd_apply(args: list[str]) -> str:
    """Apply a previous /debug proposal. Spawns Claude in --permission-mode
    acceptEdits so it can Edit/Write — but the prompt explicitly forbids
    Bash side effects (no commits, no test runs). Auth is the same
    subscription path as /debug.

    Usage:
      /apply              — apply the most recent /debug
      /apply <debug-id>   — apply a specific proposal
    """
    if args:
        debug_id = args[0]
    else:
        latest = LOG_DIR / "debug-latest.txt"
        if not latest.exists():
            return "no previous /debug to apply (run /debug <issue> first)"
        debug_id = latest.read_text(encoding="utf-8").strip()

    plan_path = LOG_DIR / f"debug-{debug_id}.log"
    if not plan_path.exists():
        return f"proposal not found: logs/debug-{debug_id}.log"

    try:
        plan_text = plan_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return f"could not read proposal: {e}"

    # Plan mode writes to ~/.claude/plans/ rather than stdout, so the
    # debug.log only contains the original ISSUE in the prompt section.
    # /apply re-uses that issue and spawns Claude in acceptEdits mode —
    # Claude reads, plans, applies in one pass.
    issue = ""
    for line in plan_text.splitlines():
        if line.startswith("Issue:"):
            issue = line[len("Issue:"):].strip()
            break
    if not issue:
        return f"could not find original issue in {plan_path.name}"

    apply_prompt = (
        "You previously planned this fix in plan mode. Now APPLY it: read "
        "the relevant files, make the necessary edits using Edit/Write. "
        "Do NOT run Bash for anything (no tests, no git commit, no shell "
        "side effects). Do NOT modify broker/, risk/exits.py, "
        "risk/config.py, or engine/execute.py without explicitly calling "
        "that out in your response. After editing, list the files you "
        "changed and a one-sentence summary of what changed in each.\n\n"
        f"Original issue (debug-id {debug_id}): {issue}"
    )
    apply_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    import threading
    threading.Thread(
        target=_apply_worker,
        args=(debug_id, apply_id, apply_prompt),
        daemon=True, name=f"apply-{apply_id}",
    ).start()
    return (f"apply-{apply_id} started (Claude acceptEdits mode, ~30-60s).\n"
            f"You'll get a follow-up message with the diff and Claude's report.")


def _apply_worker(debug_id: str, apply_id: str, apply_prompt: str) -> None:
    rc, out, err = _claude_invoke(apply_prompt, "acceptEdits", 600)
    apply_log = LOG_DIR / f"apply-{apply_id}.log"
    apply_log.write_text(
        f"prompt:\n{apply_prompt}\n\n--- stdout ---\n{out}\n\n"
        f"--- stderr ---\n{err}\n",
        encoding="utf-8",
    )
    if rc != 0 and not out:
        _send_async_telegram(f"apply-{apply_id} failed (rc={rc}):\n{err[:1500]}")
        return

    try:
        diff = subprocess.run(
            ["git", "diff", "--stat"],
            cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=15,
        )
        diff_out = (diff.stdout or "").strip()[:1000]
    except Exception:
        diff_out = "(git diff unavailable)"

    header = (
        f"apply-{apply_id} done (debug-id {debug_id})\n"
        f"git diff --stat:\n{diff_out or '(no changes)'}\n\n"
        f"Claude's report:\n"
    )
    body = out
    if len(header) + len(body) > 3500:
        body = body[-(3500 - len(header) - 50):]
        body = f"[truncated — full in {apply_log.name}]\n\n" + body
    _send_async_telegram(header + body)


def cmd_restart_listener(args: list[str]) -> str:
    """Trigger the listener to exit cleanly. The .bat retry loop relaunches
    a fresh process within ~10s, picking up any code edits to the listener
    itself."""
    import threading
    def _exit():
        import time as _time
        _time.sleep(1)
        # 0 means "clean exit" — bat retry loop will relaunch
        os._exit(0)
    threading.Thread(target=_exit, daemon=True).start()
    return "listener exiting in 1s; bat will relaunch within ~10s with fresh code"


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
    "/debug": cmd_debug,
    "/apply": cmd_apply,
    "/restart_listener": cmd_restart_listener,
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


# Aliases for phone-autocorrect mangled inputs (no slash, capitalized,
# truncated). Maps the FIRST WORD lowercased (without slash) to the
# canonical command. Today's listener log showed user typed "Restart",
# "Sentinel", "Po", "Refresh", "Fix" — all autocorrect failures.
_ALIASES = {
    "restart": "/restart",
    "halt": "/halt",
    "resume": "/resume",
    "positions": "/positions",
    "po": "/positions",
    "pos": "/positions",
    "refresh": "/health",
    "health": "/health",
    "fix": "/debug",
    "debug": "/debug",
    "apply": "/apply",
    "report": "/report",
    "proposals": "/proposals",
    "diagnose": "/diagnose",
    "help": "/help",
    "close": "/close",
    "cancel": "/cancel",
    "blacklist": "/blacklist",
    "sentinel": "/restart sentinel",
}


# Stripped-alphanumeric index of canonical commands. Lets us match
# "/restartlistener" or "Restart Listener" or "RESTARTLISTENER" against
# the canonical "/restart_listener" by collapsing both sides to letters
# and digits only.
import re as _re_mod

def _strip_alnum(s: str) -> str:
    return _re_mod.sub(r"[^a-z0-9]", "", (s or "").lower())


def _build_handlers_stripped() -> dict[str, str]:
    return {_strip_alnum(k): k for k in HANDLERS}


# Some compound commands also need single-token alias entries. Pre-computed
# at import; rebuilt by _resolve_command on first call to ensure HANDLERS
# is populated.
_HANDLERS_STRIPPED: dict[str, str] = {}


def _resolve_command(raw_first_token: str) -> str | None:
    """Try to resolve a user-typed first token to a canonical command in
    HANDLERS. Returns the canonical key (e.g., '/restart_listener') or None.

    Resolution order:
      1. Exact match on HANDLERS (with or without leading slash)
      2. Lowercased exact match
      3. Stripped-alphanumeric fuzzy match (handles autocorrect-stripped
         underscores, lost case, missing slash, double slashes, etc.)
      4. Alias map for short forms ('po' -> '/positions')
    """
    global _HANDLERS_STRIPPED
    if not _HANDLERS_STRIPPED:
        _HANDLERS_STRIPPED = _build_handlers_stripped()

    if not raw_first_token:
        return None

    t = raw_first_token.lower()

    # 1. Exact match
    if t in HANDLERS:
        return t

    # 2. Add leading slash if missing
    if not t.startswith("/"):
        if "/" + t in HANDLERS:
            return "/" + t

    # 3. Stripped-alphanumeric fuzzy match — kills underscores, slashes,
    # punctuation, case differences, hyphens. So 'restartlistener',
    # '/Restart-Listener', 'RESTART_LISTENER', 'restart listener'-without-space
    # all resolve.
    stripped = _strip_alnum(t)
    if stripped:
        canonical = _HANDLERS_STRIPPED.get(stripped)
        if canonical:
            return canonical

    # 4. Alias map (short forms)
    if t in _ALIASES:
        return _ALIASES[t]
    bare = t.lstrip("/")
    if bare in _ALIASES:
        return _ALIASES[bare]

    return None


def _normalize(text: str) -> str:
    """Map phone-mangled inputs to canonical commands. Handles every form of
    phone autocorrect mangling we've seen:
      'Restart'            -> '/restart'
      'Restart Listener'   -> '/restart_listener'  (space ate underscore)
      'restart_listener'   -> '/restart_listener'
      'restartlistener'    -> '/restart_listener'
      'RESTART LISTENER'   -> '/restart_listener'
      '/po'                -> '/positions'        (alias map)
      '/restart sentinel'  -> '/restart sentinel' (preserves valid sub-args)

    Strategy: longest-prefix match first. If 'restart sentinel' has a
    canonical, use it. Otherwise fall back to 'restart' as command +
    'sentinel' as arg. Also tries alias map for short forms.
    """
    if not text:
        return text
    text = text.strip()

    global _HANDLERS_STRIPPED
    if not _HANDLERS_STRIPPED:
        _HANDLERS_STRIPPED = _build_handlers_stripped()

    parts = text.split()
    if not parts:
        return text

    # Try matching prefixes from LONGEST to SHORTEST against the
    # stripped-alphanumeric index of canonical commands. This way:
    #   'Restart Listener'  -> tries 'RestartListener' first  -> matches
    #                                                           '/restart_listener'
    #   '/restart sentinel' -> tries 'restartsentinel' first  -> miss
    #                       -> falls back to '/restart' + 'sentinel'
    for n in range(len(parts), 0, -1):
        prefix = " ".join(parts[:n])
        ps = _strip_alnum(prefix)
        if not ps:
            continue
        canonical = _HANDLERS_STRIPPED.get(ps)
        if canonical:
            rest = " ".join(parts[n:])
            return canonical + ((" " + rest) if rest else "")

    # Alias map for short forms ('po', 'pos', etc.)
    first = parts[0].lower().lstrip("/")
    if first in _ALIASES:
        rest = " ".join(parts[1:])
        canonical = _ALIASES[first]
        return canonical + ((" " + rest) if rest else "")

    return text  # let _dispatch surface "unknown command"


def _dispatch(text: str) -> str:
    text = _normalize(text)
    try:
        parts = shlex.split(text)
    except ValueError:
        parts = text.split()
    if not parts:
        return "(empty)"

    cmd = parts[0].lower()
    args = parts[1:]
    handler = HANDLERS.get(cmd)

    # Last-ditch fuzzy retry — catches cases where _normalize didn't fire
    # (e.g., new HANDLERS added without alias updates).
    if handler is None:
        canonical = _resolve_command(cmd)
        if canonical:
            handler = HANDLERS.get(canonical)
            cmd = canonical

    if handler is None:
        return f"unknown command: {cmd}\n\n" + cmd_help([])
    try:
        return handler(args)
    except Exception:
        return f"handler error in {cmd}:\n{traceback.format_exc()[-1500:]}"


def _register_bot_commands() -> None:
    """Telegram setMyCommands — populates the / menu in the chat client so
    the user can tap commands instead of typing (avoids autocorrect mangling
    underscores/case). Idempotent; safe to call on every listener start."""
    cmds = [
        {"command": "help", "description": "list all commands"},
        {"command": "positions", "description": "open positions + P&L"},
        {"command": "health", "description": "12-check audit"},
        {"command": "halt", "description": "stop new entries today"},
        {"command": "resume", "description": "clear halt"},
        {"command": "blacklist", "description": "skip OCC today: /blacklist NKE..."},
        {"command": "cancel", "description": "cancel order: /cancel OCC|all"},
        {"command": "close", "description": "submit close: /close OCC"},
        {"command": "restart", "description": "/restart sentinel | override"},
        {"command": "diagnose", "description": "tail of runner logs"},
        {"command": "report", "description": "EOD analysis markdown"},
        {"command": "proposals", "description": "EOD proposals list"},
        {"command": "debug", "description": "Claude plan-mode investigation"},
        {"command": "apply", "description": "Claude applies the latest /debug"},
        {"command": "restart_listener", "description": "reload listener code"},
    ]
    try:
        from urllib.parse import urlencode as _ue
        from urllib.request import Request as _Req, urlopen as _uo
        data = _ue({"commands": json.dumps(cmds)}).encode("utf-8")
        _uo(_Req(f"{API}/setMyCommands", data=data, method="POST"), timeout=8).read()
        _log(f"registered {len(cmds)} bot commands with Telegram")
    except Exception as e:
        _log(f"setMyCommands failed (non-fatal): {e}")


def main() -> int:
    if not TOKEN or not CHAT_ID:
        _log("missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return 2

    _register_bot_commands()
    _ccp = os.environ.get("CLAUDE_CLI_PATH") or ""
    _log(f"listener starting; chat_id={CHAT_ID}  "
         f"claude_cli={'set (' + str(len(_ccp)) + ' chars)' if _ccp else 'UNSET'}")
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
