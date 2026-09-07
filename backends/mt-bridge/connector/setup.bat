@echo off
REM MT Bridge Connector Setup
REM Installs Python dependencies for the pairing tool

echo ========================================
echo  MT Bridge Connector Setup
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://www.python.org/
    pause
    exit /b 1
)

echo [1/2] Installing Python dependencies...
python -m pip install --upgrade pip
pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo [2/2] Checking for cloudflared...
where cloudflared >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: cloudflared not found in PATH
    echo.
    echo Please download cloudflared for secure tunneling:
    echo   1. Visit: https://github.com/cloudflare/cloudflared/releases
    echo   2. Download cloudflared-windows-amd64.exe
    echo   3. Rename to cloudflared.exe
    echo   4. Place in this folder OR add to system PATH
    echo.
    echo You can skip this and use --skip-tunnel flag later.
) else (
    echo OK: cloudflared found!
)

echo.
echo ========================================
echo Setup Complete!
echo ========================================
echo.
echo To pair your MT Bridge, run:
echo     python pair.py
echo.
pause
