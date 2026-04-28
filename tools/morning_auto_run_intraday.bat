@echo off
REM Intraday rescan — invoked by Windows Task Scheduler at 11:00, 12:30, 14:00.
REM Uses --only-if-empty so it skips when the morning run already produced
REM trades. Use --label so its log file doesn't overwrite the morning's.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

REM First arg from the scheduled task is the time-label (e.g. "intraday-1100").
REM Defaults to "intraday" if not passed.
set LABEL=%~1
if "%LABEL%"=="" set LABEL=intraday

python -m tools.morning_auto_run --only-if-empty --label "%LABEL%" ^
    --min-score 60 --max-trades 3

exit /b %ERRORLEVEL%
