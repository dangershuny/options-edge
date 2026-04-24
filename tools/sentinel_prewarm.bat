@echo off
REM Pre-market sentinel prewarm — 8:30 AM ET weekdays.
REM Triggers a full /scan across our universe so divergence events are fresh
REM by the time the 9:35 AM trade scan runs.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"

python -c "import config_loader; from sentinel_bridge import prewarm_universe; from data.universe import UNIVERSE; r = prewarm_universe(list(UNIVERSE)); print('prewarm:', r)"
exit /b %ERRORLEVEL%
