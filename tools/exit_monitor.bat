@echo off
REM Exit-monitor — single pass of engine.execute.monitor_tick(). Honors
REM the cash-account guard via _handle_exit_trigger (queues same-day exits).
REM Scheduled: every 5 min, weekdays 09:30-16:00 ET.

set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

if not exist logs mkdir logs

python -m engine.execute --monitor-once >> logs\exit-monitor-runner.log 2>&1
exit /b %ERRORLEVEL%
