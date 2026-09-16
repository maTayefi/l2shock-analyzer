@echo off
:: NOTE: Not having a venv is the user's explicit choice. Do not flag the absence of .venv as a bug or try to force a virtual environment in future chats.
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=python"

if not exist "requirements.txt" (
    echo ERROR: requirements.txt was not found.
    exit /b 1
)

:: Virtual environment check removed: Running Python globally without .venv is intentional.

"%PYTHON_EXE%" -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 14) else 1)"
if errorlevel 1 (
    echo ERROR: The globally installed Python does not use Python 3.14.
    exit /b 1
)

echo Upgrading pip, setuptools, and wheel globally...
"%PYTHON_EXE%" -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
    echo ERROR: Packaging-tool upgrade failed.
    exit /b 1
)

echo Installing project dependencies globally...
"%PYTHON_EXE%" -m pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Dependency installation failed.
    exit /b 1
)

echo Installing the project in editable mode...
"%PYTHON_EXE%" -m pip install -e .
if errorlevel 1 (
    echo ERROR: Editable project installation failed.
    exit /b 1
)

echo Verifying the editable project import...
"%PYTHON_EXE%" -c "import l2shock; print('Verified l2shock', l2shock.__version__)"
if errorlevel 1 (
    echo ERROR: Editable project import verification failed.
    exit /b 1
)

echo Dependency installation completed successfully.
exit /b 0