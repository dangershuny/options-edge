@echo off
REM Autonomous morning run — invoked by Windows Task Scheduler.
REM Runs one fresh snapshot, then paper trades across 3 bankroll tiers
REM (sim500, sim1000, sim2000) each tagged with its own Alpaca client_order_id.

set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

REM --min-score 60 filters the lowest-tier signals
REM --max-trades 3 = sim500 cap; sim1000 gets 5, sim2000 gets 8 (auto-scaled)
python -m tools.morning_auto_run --min-score 60 --max-trades 3

exit /b %ERRORLEVEL%
