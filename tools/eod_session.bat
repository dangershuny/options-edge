@echo off
REM EOD session — fires queued exits whose entry_date is before today
REM (cash-account T+1 settlement compliance). Scheduled: 15:45 ET weekdays.

set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

if not exist logs mkdir logs

python -m engine.execute --eod >> logs\eod-session-runner.log 2>&1
exit /b %ERRORLEVEL%
