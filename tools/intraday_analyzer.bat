@echo off
REM Intraday analyzer — read-only anomaly detector. Runs every 15 min
REM during RTH via OptionsEdge-IntradayAnalyzer schtask. Never modifies
REM rules or positions; surfaces unusual situations via Telegram WARN
REM so the operator can intervene if warranted.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\python.exe
echo === intraday_analyzer at %DATE% %TIME% >> logs\intraday-analyzer.log
"%PYTHON%" -m tools.intraday_analyzer >> logs\intraday-analyzer.log 2>&1
exit /b %ERRORLEVEL%
