@echo off
REM Autonomous morning run — invoked by Windows Task Scheduler.
REM Runs fresh snapshot, flow+news scan, verifies broker, submits paper trades.

set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

REM --live means actually submit orders to Alpaca paper account.
python -m tools.morning_auto_run --bankroll 500 --min-score 55 --max-trades 3

exit /b %ERRORLEVEL%
