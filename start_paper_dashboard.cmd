@echo off
setlocal

set "ROOT=%~dp0"
if not exist "%ROOT%\.venv\Scripts\python.exe" (
  echo Missing virtual environment at "%ROOT%\.venv"
  pause
  exit /b 1
)

set "PYTHONPATH=%ROOT%src;%PYTHONPATH%"
set "AUTOBOTT_ENV_FILE=C:\Users\flavo\Downloads\AutoBott.env"

echo Starting AutoBott paper dashboard...
echo Env file: %AUTOBOTT_ENV_FILE%
echo URL: http://127.0.0.1:8000
echo Dashboard token: autobott-local
echo.

"%ROOT%\.venv\Scripts\python.exe" -m autobott_v2.launch_dashboard
