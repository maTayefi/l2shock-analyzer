@echo off
setlocal
cd /d "%~dp0"


py -3.14 -m l2shock.diagnostics
exit /b %errorlevel%