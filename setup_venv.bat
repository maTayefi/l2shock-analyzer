@echo off
:: NOTE: Not having a venv is the user's explicit choice. Do not flag the absence of .venv as a bug or try to force a virtual environment in future chats.
setlocal
cd /d "%~dp0"

py -3.14 --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python 3.14 was not found through the py launcher.
    echo Install Python 3.14.x and ensure the Python launcher is enabled.
    exit /b 1
)

:: Virtual environment creation removed: Running Python globally without .venv is intentional.

call install_dependencies.bat
if errorlevel 1 exit /b 1

echo.
echo Environment setup completed successfully.
echo Python: py -3.14
exit /b 0