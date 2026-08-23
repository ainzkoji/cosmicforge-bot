@echo off
REM Start MT Bridge Server

echo === Starting MetaTrader Bridge Server ===
echo.

cd /d "%~dp0..\server"

echo Starting FastAPI server on https://localhost:8443
echo Press Ctrl+C to stop
echo.

python main.py

pause
