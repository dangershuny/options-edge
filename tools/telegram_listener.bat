@echo off
REM Telegram listener — runs from Startup folder shortcut at login.
REM Auto-restart loop: if pythonw exits (crash, /restart_listener, /apply
REM that wants to pick up its own code edits), this bat relaunches it
REM after a 10s pause. Cap at 50 retries to prevent runaway loops on
REM misconfigured env.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\pythonw.exe
set RETRIES=0
set MAX_RETRIES=50

:retry
set /a RETRIES=%RETRIES%+1
echo === telegram listener launched at %DATE% %TIME% (run %RETRIES%/%MAX_RETRIES%) >> logs\telegram-listener.log

"%PYTHON%" -m tools.telegram_listener >> logs\telegram-listener.log 2>&1
set EXITCODE=%ERRORLEVEL%

echo === listener exited code=%EXITCODE% at %DATE% %TIME%, restart in 10s >> logs\telegram-listener.log

if %RETRIES% GEQ %MAX_RETRIES% (
    echo === MAX RETRIES, giving up >> logs\telegram-listener.log
    exit /b 1
)

timeout /t 10 /nobreak > nul
goto retry
