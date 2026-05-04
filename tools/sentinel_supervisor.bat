@echo off
REM Launch news_sentinel/supervisor.py at user logon.
REM Supervisor.py owns server.py / dashboard_writer.py / scheduler.py
REM recovery on the Sentinel side. Has its own singleton lock via
REM supervisor.pid, so launching twice is safe (the second instance
REM detects the live PID and exits clean).
REM
REM This bat exists so a scheduled task can fire it at OnLogon. The
REM watchdog (tools/watchdog.py) ALSO restarts supervisor.py if it
REM dies mid-session — this just makes startup faster than waiting
REM for the watchdog's first tick.

setlocal
set PYTHONIOENCODING=utf-8
set SENTINEL_DIR=C:\Users\dange\OneDrive\Documents\Claude Projects\news_sentinel
set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\pythonw.exe

cd /d "%SENTINEL_DIR%"
start "" /B "%PYTHON%" supervisor.py
exit /b 0
