@echo off
REM Telegram listener watchdog — invoked every 5 min by scheduler.
REM If a listener is already running (PID file + tasklist check), exit
REM cleanly. Otherwise launch a new background instance.

setlocal enabledelayedexpansion
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\pythonw.exe
set PIDFILE=logs\telegram_listener.pid

REM Check if previous instance is still alive
if exist "%PIDFILE%" (
    set /p OLDPID=<"%PIDFILE%"
    tasklist /FI "PID eq !OLDPID!" 2>nul | find /I "python" >nul
    if not errorlevel 1 (
        echo Already running PID=!OLDPID!, exiting watchdog cleanly. >> logs\telegram-listener.log
        exit /b 0
    )
    REM stale PID file — clear it
    del "%PIDFILE%" 2>nul
)

REM Launch in the background and capture PID
echo === watchdog launching listener at %DATE% %TIME% >> logs\telegram-listener.log
start "" /B "%PYTHON%" -m tools.telegram_listener >> logs\telegram-listener.log 2>&1

REM Get the new pythonw PID — match by command line (best-effort)
for /f "tokens=2" %%P in ('tasklist /FI "IMAGENAME eq pythonw.exe" /FO TABLE /NH 2^>nul') do (
    echo %%P > "%PIDFILE%"
)

exit /b 0
