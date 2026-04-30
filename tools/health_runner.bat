@echo off
REM Health runner — detect, remediate, classify, notify, log.
REM Default: every 5 min weekdays 09:30-16:00 ET.
REM Pass --summary for the 16:35 EOD summary task.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

if not exist logs mkdir logs

if /I "%~1"=="--summary" (
    python -m tools.health_runner --summary >> logs\health-runner.log 2>&1
) else (
    python -m tools.health_runner >> logs\health-runner.log 2>&1
)

exit /b %ERRORLEVEL%
