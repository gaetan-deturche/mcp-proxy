@echo off
REM Register a logon task that starts the proxy hidden, then start it now.
REM %~dp0 is this script's own folder (with trailing backslash), so no path is hardcoded.
set "VBS=%~dp0start-proxy-hidden.vbs"
schtasks /Create /TN "mcp-proxy" /TR "wscript.exe \"%VBS%\"" /SC ONLOGON /RL LIMITED /F
echo.
echo Registered. Starting proxy now...
wscript.exe "%VBS%"
