@echo off
REM Watchdog runner — fires every 1 min during RTH (Mon-Fri 09:30-16:00 ET)
REM
REM Zero-touch self-healing supervisor. Detects:
REM   - Dead/hung ExitMonitor daemon
REM   - Dead news_sentinel server
REM   - Dead telegram listener
REM   - Dead override server (only if its task is enabled)
REM   - Duplicate process zombies
REM   - Oversized engine_state.db
REM   - Oversized log files
REM
REM Each pass exits in <30s. Audit log at logs/watchdog-{date}.jsonl.
REM Telegram WARN alert fires when any recovery action is taken so the
REM operator sees the autonomous fix in real time.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\python.exe

echo === watchdog pass at %DATE% %TIME% >> logs\watchdog.log
"%PYTHON%" -m tools.watchdog >> logs\watchdog.log 2>&1
exit /b %ERRORLEVEL%
