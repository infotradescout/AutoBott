@echo off
setlocal

cd /d "%~dp0"

if "%DATA_DIR%"=="" set "DATA_DIR=%USERPROFILE%\AutoBottData"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"

set "AUTO_RESUME_TRADING_ON_BOOT=true"
if "%PORT%"=="" set "PORT=5050"

echo.
echo AutoBott local run
echo DATA_DIR=%DATA_DIR%
echo Dashboard: http://localhost:%PORT%/
echo.

cd /d "%~dp0autotrader"
py render_service_dashboard_v2.py

endlocal
