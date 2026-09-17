```bat
@echo off

cd /d C:\l2_liquidity_shock_analyzer

echo.
echo ===== 1. git status =====
git status

echo.
echo ===== 2. git add . =====
git add .

echo.
echo ===== 3. git status after staging =====
git status

echo.
echo ===== 4. git commit =====
git commit -m "next batch"

if errorlevel 1 (
    echo.
    echo ===== COMMIT FAILED - PUSH NOT EXECUTED =====
    pause
    exit /b 1
)

echo.
echo ===== 5. git push =====
git push

echo.
echo ===== Finished =====
pause
```