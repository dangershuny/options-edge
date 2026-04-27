@echo off
REM Build & open the Options Edge HTML dashboard in your default browser.
REM Also ensures the local API server is running so the manual ticker
REM lookup works for ANY ticker (not just the prefetched universe).

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

REM ── Ensure the dashboard API server is up on port 8503 ────────────────
REM netstat is faster than spinning up Python just to check a port.
netstat -an -p tcp | findstr ":8503" | findstr LISTENING >nul
if errorlevel 1 (
    echo Starting dashboard API server in background...
    start "" /B pythonw -m tools.dashboard_server
    REM tiny pause so the API is reachable by the time the page loads
    ping -n 3 127.0.0.1 >nul
) else (
    echo Dashboard API server already running on port 8503
)

echo Building dashboard...
python -m tools.build_dashboard
if errorlevel 1 (
    echo Build failed.
    pause
    exit /b 1
)

REM Try multiple ways to open — Windows 11 sometimes ignores bare "start"
set DASH=%CD%\dashboard.html
echo Opening %DASH%
explorer "%DASH%"
if errorlevel 1 (
    start "Options Edge Dashboard" "%DASH%"
)

exit /b 0
