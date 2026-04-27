@echo off
REM Launch the override HTTP server (port 8503) for dashboard "manual buy"
REM button. Designed to run continuously alongside the existing scheduled
REM tasks. Safe to start/stop at any time — does not modify any other
REM process or shared file beyond its own logs/override_results dir.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

python -m tools.override_server
exit /b %ERRORLEVEL%
