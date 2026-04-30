@echo off
REM Health EOD summary — runs at 16:35 ET to write the day's roll-up.
setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs
python -m tools.health_runner --summary >> logs\health-runner.log 2>&1
exit /b %ERRORLEVEL%
