@echo off
setlocal
cd /d "%~dp0"

echo Creating or upgrading the l2shock_local database...
echo.

py -3.14 -m l2shock.db.bootstrap

if errorlevel 1 (
    echo.
    echo ============================================================
    echo ERROR: Database bootstrap failed.
    echo ============================================================
    echo.
    echo Check the log files in the logs\ directory for the traceback.
    echo.
    exit /b 1
)

echo.
echo ============================================================
echo Database bootstrap completed successfully.
echo ============================================================
exit /b 0