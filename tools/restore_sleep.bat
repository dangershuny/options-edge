@echo off
REM Restore normal sleep behavior (run this when you no longer need 24/7 uptime).
REM Values mirror Windows defaults: sleep after 2h on AC, hibernate after 4h.

echo Restoring default sleep/hibernate timeouts...
powercfg /change standby-timeout-ac 120
powercfg /change hibernate-timeout-ac 240
powercfg /change disk-timeout-ac 20
echo Done.
pause
