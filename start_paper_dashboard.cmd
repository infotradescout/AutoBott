@echo off
setlocal

set "ROOT=%~dp0"
if not exist "%ROOT%\.venv\Scripts\python.exe" (
  echo Missing virtual environment at "%ROOT%\.venv"
  pause
  exit /b 1
)

for /f %%I in ('powershell -NoProfile -Command "(Get-CimInstance Win32_Process | Where-Object { $_.Name -in @(''python.exe'',''pythonw.exe'') -and $_.CommandLine -and $_.CommandLine.Contains(''autobott_v2.launch_dashboard'') } | Measure-Object).Count"') do set "RUNNING_COUNT=%%I"
if not "%RUNNING_COUNT%"=="0" exit /b 0

set "PYTHONPATH=%ROOT%src;%PYTHONPATH%"
set "AUTOBOTT_ENV_FILE=C:\Users\flavo\Downloads\AutoBott.env"

echo Starting AutoBott paper dashboard...
echo Env file: %AUTOBOTT_ENV_FILE%
echo URL: http://127.0.0.1:8000
echo Protected API access requires AUTOBOTT_DASHBOARD_AUTH_TOKEN in the configured env file.
echo.

"%ROOT%\.venv\Scripts\python.exe" -m autobott_v2.launch_dashboard
