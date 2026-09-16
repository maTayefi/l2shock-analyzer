@echo off
setlocal
cd /d "%~dp0"


py -3.14 -m pytest -v
exit /b %errorlevel%