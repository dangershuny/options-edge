"""
Centralized error/signal alerting — email + file + console.

Reads SMTP config from env vars:
    OPTIONS_EDGE_ALERT_EMAIL          — recipient (required)
    OPTIONS_EDGE_ALERT_EMAIL_FROM     — from address (defaults to recipient)
    OPTIONS_EDGE_ALERT_EMAIL_USER     — SMTP username
    OPTIONS_EDGE_ALERT_EMAIL_PASSWORD — SMTP password / app password
    OPTIONS_EDGE_ALERT_EMAIL_HOST     — SMTP host (default smtp.gmail.com)
    OPTIONS_EDGE_ALERT_EMAIL_PORT     — SMTP port (default 587, TLS)

All operations safe on failure — never raises to caller. Designed to
notify, not to interrupt a scan.
"""

from __future__ import annotations

import json
import os
import smtplib
import socket
import sys
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


def _write_file_alert(payload: dict) -> Path | None:
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        source = payload.get("source", "unknown")
        symbol = payload.get("symbol", "") or "NA"
        path = LOG_DIR / f"error_alert_{ts}_{symbol}_{source}.log"
        path.write_text(json.dumps(payload, indent=2, default=str))
        return path
    except Exception:
        return None


def _console_alert(payload: dict) -> None:
    sev = payload.get("severity", "ERROR")
    src = payload.get("source", "unknown")
    msg = payload.get("message", "")
    sym = payload.get("symbol", "")
    ts = payload.get("timestamp", "")
    line = f"[ALERT {sev}] {src}"
    if sym:
        line += f" [{sym}]"
    line += f" {ts}: {msg}"
    print(line, file=sys.stderr)


def _send_email(payload: dict) -> bool:
    to = os.environ.get("OPTIONS_EDGE_ALERT_EMAIL", "").strip()
    user = os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_USER", "").strip()
    pw = os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_PASSWORD", "").strip()
    if not (to and user and pw):
        return False

    from_addr = os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_FROM", to).strip() or to
    host = os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_HOST", "smtp.gmail.com").strip()
    port = int(os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_PORT", "587"))

    sev = payload.get("severity", "ERROR")
    src = payload.get("source", "unknown")
    ts = payload.get("timestamp", "")
    sym = payload.get("symbol", "")
    msg = payload.get("message", "")
    code = payload.get("error_code", "")

    subject = f"🚨 Options Edge Alert ({src}) at {ts[:19]}"
    if sev == "INFO":
        subject = f"📊 Options Edge Signal ({src}) at {ts[:19]}"

    body_lines = [
        f"Time: {ts}",
        f"Source: {src}",
        f"Error Code: {code}" if code else "",
        f"Symbol: {sym}" if sym else "",
        f"Summary: {msg}",
        "",
        "-- CONTEXT --",
    ]
    ctx = payload.get("context", {})
    if isinstance(ctx, dict):
        for k, v in ctx.items():
            if isinstance(v, (list, dict)):
                body_lines.append(f"{k}:")
                body_lines.append(json.dumps(v, indent=2, default=str))
            else:
                body_lines.append(f"{k}: {v}")

    body = "\n".join(x for x in body_lines if x is not None)

    try:
        mime = MIMEText(body, "plain", "utf-8")
        mime["Subject"] = subject
        mime["From"] = from_addr
        mime["To"] = to

        with smtplib.SMTP(host, port, timeout=10) as s:
            s.ehlo()
            s.starttls()
            s.login(user, pw)
            s.sendmail(from_addr, [to], mime.as_string())
        return True
    except (smtplib.SMTPException, socket.gaierror, socket.timeout, OSError):
        return False


def trigger_alert(
    source: str,
    error_code: str = "",
    message: str = "",
    symbol: str = "",
    context: dict[str, Any] | None = None,
    severity: str = "ERROR",
) -> dict:
    """
    Fire an alert through all configured channels.

    Returns a dict summarizing which channels succeeded.
    Never raises.
    """
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": severity,
        "source": source,
        "error_code": error_code,
        "symbol": symbol or "",
        "message": message,
        "context": context or {},
    }

    result = {"console": False, "file": False, "email": False, "path": None}

    try:
        _console_alert(payload)
        result["console"] = True
    except Exception:
        pass

    path = _write_file_alert(payload)
    if path:
        result["file"] = True
        result["path"] = str(path)

    try:
        result["email"] = _send_email(payload)
    except Exception:
        pass

    return result


def email_configured() -> bool:
    """Quick check: do we have SMTP creds?"""
    return bool(
        os.environ.get("OPTIONS_EDGE_ALERT_EMAIL", "").strip()
        and os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_USER", "").strip()
        and os.environ.get("OPTIONS_EDGE_ALERT_EMAIL_PASSWORD", "").strip()
    )


if __name__ == "__main__":
    # Quick self-test
    print("Email configured:", email_configured())
    r = trigger_alert(
        source="selftest",
        error_code="test",
        message="Alerting module self-test",
        symbol="TEST",
        severity="INFO",
    )
    print("Result:", r)
