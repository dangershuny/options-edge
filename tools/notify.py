"""
Notification stub — write alerts to log files now, swap to Telegram once
the momentum-edge Telegram bot is live and we have the bot token.

Public API (matches what a Telegram swap will need):
    send(severity, title, body, payload=None) -> bool

Severity values: 'OK', 'INFO', 'WARN', 'CRIT', 'PROPOSAL'.

CRIT alerts also get tee'd through tools.error_alerting.trigger_alert so
the existing email channel still fires until Telegram replaces it.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

ALERTS_JSONL = LOG_DIR / "alerts-history.jsonl"
ALERTS_CURRENT = LOG_DIR / "alerts-current.txt"


def send(severity: str, title: str, body: str = "",
         payload: dict | None = None) -> bool:
    """Append to alerts log + tee CRIT to legacy error_alerting channel.
    Returns True if at least one channel succeeded."""
    ts = datetime.now(tz=timezone.utc).isoformat()
    record = {
        "ts": ts,
        "severity": severity.upper(),
        "title": title,
        "body": body,
        "payload": payload or {},
    }

    ok = False
    try:
        with ALERTS_JSONL.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
        ok = True
    except Exception:
        pass

    # Maintain a current.txt that shows the last ~20 alerts (operator quick-look)
    try:
        existing = []
        if ALERTS_CURRENT.exists():
            existing = ALERTS_CURRENT.read_text(encoding="utf-8").splitlines()
        line = f"[{record['ts']}] {record['severity']:<8} {title}"
        if body:
            line += f" :: {body}"
        existing.append(line)
        ALERTS_CURRENT.write_text("\n".join(existing[-20:]), encoding="utf-8")
    except Exception:
        pass

    # CRIT alerts also fire through legacy channel (email if configured)
    if severity.upper() == "CRIT":
        try:
            from tools.error_alerting import trigger_alert
            trigger_alert(
                source="health",
                error_code=title[:48],
                message=body,
                severity="ERROR",
                context=payload or {},
            )
        except Exception:
            pass

    # TODO: swap-in for Telegram once bot token wired into .env. The intended
    # interface is identical: a single call site here, every channel routes
    # through send(). When the momentum-edge Telegram process is finalized,
    # add the post here gated on TELEGRAM_BOT_TOKEN being set.
    if os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"):
        try:
            _send_telegram(record)
            ok = True
        except Exception:
            pass

    return ok


def _send_telegram(record: dict) -> None:
    """Placeholder — implement once we have the bot token format from the
    momentum-edge Telegram setup. Either polling or webhook is fine; the
    minimum interface is bot.send_message(chat_id, text)."""
    import urllib.request
    import urllib.parse
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat = os.environ["TELEGRAM_CHAT_ID"]
    text = f"*[{record['severity']}] {record['title']}*\n{record['body']}"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat, "text": text, "parse_mode": "Markdown",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=8) as r:
        r.read()


def main() -> int:
    """CLI: python -m tools.notify --severity CRIT --title "..." --body "..." """
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--severity", default="INFO")
    ap.add_argument("--title", required=True)
    ap.add_argument("--body", default="")
    args = ap.parse_args()
    ok = send(args.severity, args.title, args.body)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
