@echo off
setlocal
cd /d "%~dp0"


echo Upgrading and verifying database through the guarded bootstrap path...
echo.

py -3.14 -m l2shock.db.bootstrap

if errorlevel 1 (
    echo.
    echo ERROR: Guarded database upgrade failed.
    echo Check logs\l2shock.log for details.
    exit /b 1
)

echo.
echo Database upgrade and verification completed successfully.
exit /b 0