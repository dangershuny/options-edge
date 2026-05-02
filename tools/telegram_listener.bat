@echo off
REM Telegram listener — runs from Startup folder shortcut at login.
REM Auto-restart loop: if python exits, wait 10s and relaunch.
REM
REM IMPORTANT: uses python.exe (NOT pythonw.exe). pythonw.exe spawns
REM subprocesses without a console attached, which makes shell=True
REM subprocess invocations of claude.exe (under %APPDATA%\Roaming\npm)
REM fail with "the system cannot find the path specified" — even though
REM the same path is reachable via cmd, bash, and python.exe directly.
REM This matches momentum-edge's working configuration. The cosmetic
REM tradeoff is that python.exe attaches a console to the bat process;
REM the Startup folder shortcut launches with WindowStyle=Hidden so no
REM console is visible to the user.

setlocal
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\dange\Personal_Projects\options-edge-new"
if not exist logs mkdir logs

set PYTHON=C:\Users\dange\AppData\Local\Programs\Python\Python313\python.exe
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
