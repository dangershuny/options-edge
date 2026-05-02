@echo off
REM ExitMonitor daemon — long-running monitor process at 15s cadence.
REM
REM Replaces the 1-min schtask cadence that was capped by Windows scheduler's
REM minimum interval. Runs `python -m engine.execute --monitor-only --monitor-seconds 15`,
REM which loops in-process (no subprocess startup per tick) until alpaca.get_clock()
REM reports the market closed, then exits cleanly with rc=0.
REM
REM Triggered by OptionsEdge-ExitMonitor schtask at 09:30 ET weekdays. The
REM daemon self-terminates ~16:00 ET when market closes; bat exits clean.
REM On crash (non-zero rc), bat retries up to MAX_RETRIES times with a 10s
REM backoff so a transient broker hiccup doesn't kill exit coverage for the day.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\pythonw.exe
set RETRIES=0
set MAX_RETRIES=50

:retry
set /a RETRIES=%RETRIES%+1
echo === exit_monitor_daemon launched at %DATE% %TIME% (run %RETRIES%/%MAX_RETRIES%) >> logs\exit-monitor-daemon.log

"%PYTHON%" -m engine.execute --monitor-only --monitor-seconds 15 >> logs\exit-monitor-daemon.log 2>&1
set EXITCODE=%ERRORLEVEL%

echo === daemon exited code=%EXITCODE% at %DATE% %TIME% >> logs\exit-monitor-daemon.log

REM Clean exit (market closed) -> we're done for the session
if %EXITCODE% EQU 0 (
    echo === clean exit at end of session, bat done >> logs\exit-monitor-daemon.log
    exit /b 0
)

REM Crash: bounded retry with 10s backoff
if %RETRIES% GEQ %MAX_RETRIES% (
    echo === MAX RETRIES, giving up >> logs\exit-monitor-daemon.log
    exit /b 1
)

timeout /t 10 /nobreak > nul
goto retry
