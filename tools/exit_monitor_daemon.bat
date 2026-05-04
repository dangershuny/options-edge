@echo off
REM ExitMonitor daemon — long-running monitor at 15s cadence with HARD GUARANTEE
REM that an open position cannot sit unmonitored during RTH.
REM
REM Defense-in-depth lifecycle:
REM   - Triggered once at 09:30 ET by OptionsEdge-ExitMonitor schtask
REM   - Daemon runs in-process loop checking every 15s
REM   - If daemon exits FOR ANY REASON (clean rc=0, crash, OOM, opening-bell
REM     transient, network blip), THIS BAT relaunches it within ~10s
REM   - Bat ONLY stops trying when current ET time is >= 16:00 (true close)
REM   - rc=0 from daemon mid-session is now treated as a SOFT signal,
REM     not a "we're done for the day" command
REM
REM This protects against:
REM   - Today's bug (alpaca clock false-closed at 09:30:26 → daemon exited rc=0
REM     → bat treated as clean done → no monitor for 6.5 hours)
REM   - Daemon crash (bat retries up to MAX_RETRIES)
REM   - Network glitches that kill the daemon cleanly
REM   - Anything else that lets daemon return rc=0 mid-session

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\python.exe
set RETRIES=0
set MAX_RETRIES=200

:retry
set /a RETRIES=%RETRIES%+1
echo === exit_monitor_daemon launched at %DATE% %TIME% (run %RETRIES%/%MAX_RETRIES%) >> logs\exit-monitor-daemon.log

"%PYTHON%" -m engine.execute --monitor-only --monitor-seconds 15 >> logs\exit-monitor-daemon.log 2>&1
set EXITCODE=%ERRORLEVEL%

echo === daemon exited code=%EXITCODE% at %DATE% %TIME% >> logs\exit-monitor-daemon.log

REM ── HARD GUARANTEE: relaunch if we're still in RTH (Mon-Fri before 16:00 ET) ──
REM Use Python to evaluate: ET hour < 16 AND weekday Mon-Fri  → exit code 1 = relaunch
"%PYTHON%" -c "from datetime import datetime; from zoneinfo import ZoneInfo; et = datetime.now(tz=ZoneInfo('America/New_York')); raise SystemExit(0 if (et.hour >= 16 or et.weekday() >= 5) else 1)"
set IS_PAST_CLOSE=%ERRORLEVEL%

if %IS_PAST_CLOSE% EQU 0 (
    echo === past 16:00 ET (or weekend), session complete, exiting bat clean >> logs\exit-monitor-daemon.log
    exit /b 0
)

REM Still in RTH — relaunch unconditionally
echo === still in RTH, relaunching daemon in 10s (rc was %EXITCODE%) >> logs\exit-monitor-daemon.log

if %RETRIES% GEQ %MAX_RETRIES% (
    echo === MAX RETRIES reached during RTH — giving up, ALERT NEEDED >> logs\exit-monitor-daemon.log
    exit /b 1
)

timeout /t 10 /nobreak > nul
goto retry
