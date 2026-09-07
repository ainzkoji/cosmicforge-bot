@echo off
echo ================================================
echo     MT Bridge Connector - Build Tool
echo     Creates standalone Windows executable
echo ================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://www.python.org
    pause
    exit /b 1
)

echo [1/4] Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install requirements
    pause
    exit /b 1
)

echo.
echo [2/4] Installing PyInstaller...
pip install pyinstaller
if errorlevel 1 (
    echo ERROR: Failed to install PyInstaller
    pause
    exit /b 1
)

echo.
echo [3/4] Building executable...
pyinstaller --onefile --name MTBridgeConnector --icon=NONE pair.py
if errorlevel 1 (
    echo ERROR: Build failed
    pause
    exit /b 1
)

echo.
echo [4/4] Build complete!
echo.
echo Executable location: dist\MTBridgeConnector.exe
echo.
echo To distribute:
echo   1. Copy dist\MTBridgeConnector.exe to target machine
echo   2. Ensure cloudflared.exe is in same folder OR system PATH
echo   3. Run MTBridgeConnector.exe
echo.
pause
