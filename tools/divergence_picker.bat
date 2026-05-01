@echo off
REM Divergence-driven entry picker — runs at 09:34 ET weekdays, just before
REM MorningAutoRun. Pulls active divergence_events from news_sentinel.db,
REM picks one option contract per ticker aligned with the divergence
REM direction, submits paper orders. Engine.execute monitor_tick takes over
REM exit management (record_open is wired into paper_trade._execute_trade).

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

python -m tools.divergence_picker --live --max-picks 3 --qty 1 ^
    >> logs\divergence-picker-runner.log 2>&1
exit /b %ERRORLEVEL%
