@echo off
setlocal
cd /d "%~dp0.."
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" -m scripts.serve_reports %*
) else (
  python -m scripts.serve_reports %*
)
exit /b %errorlevel%
