@echo off
REM Build & open the Options Edge HTML dashboard in your default browser.
REM Double-click this file anytime to get a fresh view.

set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

python -m tools.build_dashboard
if %ERRORLEVEL% EQU 0 start "" "dashboard.html"

exit /b %ERRORLEVEL%
