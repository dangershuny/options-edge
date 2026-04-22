@echo off
REM Build & open the Options Edge HTML dashboard in your default browser.
REM Double-click this file anytime to get a fresh view.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

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
