@echo off
REM Regenerate dashboard.html — called every 5 min during market hours by
REM OptionsEdge-DashboardRefresh so graphs step forward as the market moves.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

python -m tools.build_dashboard >nul 2>&1
exit /b %ERRORLEVEL%
