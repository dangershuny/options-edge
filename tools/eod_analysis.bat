@echo off
REM End-of-day analysis + proposals. Runs at 16:45 ET, after EODSession (15:45),
REM SurfaceSnapshot (16:30), and HealthSummary (16:35) have all written data.
REM Produces logs/eod-analysis-{date}.md + logs/eod-proposals-{date}.json,
REM sends a Telegram top-line summary with proposal count.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

python -m tools.eod_analysis >> logs\eod-analysis-runner.log 2>&1
exit /b %ERRORLEVEL%
