@echo off
REM Background dashboard API server. Kept alive so the manual ticker
REM lookup in dashboard.html works for any ticker. Auto-launched at
REM logon by OptionsEdge-DashboardAPI scheduled task.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

REM Skip if already running on 8503
netstat -an -p tcp | findstr ":8503" | findstr LISTENING >nul
if not errorlevel 1 (
    echo Already running.
    exit /b 0
)

pythonw -m tools.dashboard_server
exit /b %ERRORLEVEL%
