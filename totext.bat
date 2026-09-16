@echo off
:: Ensure we are operating in the directory where the .bat file is located
cd /d "%~dp0"

set "OUTPUT_FILE=l2_liquidity_shock_analyzer-project_contents.txt"
set "PS_SCRIPT=%temp%\Combine-AiContext.ps1"

echo Scanning directory for source code and config files...
echo Skipping binaries, media, .pyc files, and ignored folders (.git, node_modules, .VSCodeCounter, etc.)
echo.

:: Generate the PowerShell script dynamically
> "%PS_SCRIPT%" (
    echo $ErrorActionPreference = 'SilentlyContinue'
    echo $OutputFile = '%OUTPUT_FILE%'
    echo $ScriptName = '%~nx0'
    echo $StartPath = (Get-Item -Path '.\'^).FullName
    echo $PrefixLen = $StartPath.Length + 1
    echo if ($StartPath.EndsWith('\'^)^) { $PrefixLen = $StartPath.Length }
    echo.
    echo if (Test-Path $OutputFile^) { Remove-Item $OutputFile }
    echo.
    echo # Exclude common heavy/binary/generated/secret directories to save time and avoid leaking local secrets
    echo $ExcludeDirs = @('.git', 'node_modules', 'venv', '.venv', '.idea', '.vs', '.vscode', '__pycache__', 'data', 'backups', '.VSCodeCounter', 'logs', 'exports', 'backups', 'ai_chunks', 'dist', 'build', '.pytest_cache', '.mypy_cache'^)
    echo $ExcludeExtensions = @('.pyc', '.pyo', '.pyd', '.db', '.sqlite', '.sqlite3', '.dump', '.bak', '.pem', '.key', '.p12', '.pfx', '.crt', '.cer'^)
    echo $ExcludeFileNames = @('.env', '.env.local', '.env.production', 'config.yaml', 'config.yml', 'secrets.yaml', 'secrets.yml', 'project_contents.txt'^)
    echo.
    echo $Files = Get-ChildItem -Path '.' -File -Recurse ^| Where-Object { 
    echo     $file = $_
    echo     $skip = $false
    echo     $nameLower = $file.Name.ToLowerInvariant(^)
    echo     $extLower = $file.Extension.ToLowerInvariant(^)
    echo     if ($file.Name -eq $OutputFile -or $file.Name -eq $ScriptName^) { $skip = $true }
    echo     if ($ExcludeFileNames -contains $nameLower^) { $skip = $true }
    echo     if ($ExcludeExtensions -contains $extLower^) { $skip = $true }
    echo     if ($file.Name -like 'project_contents*.txt' -or $file.Name -like 'project_segment_*.txt'^) { $skip = $true }
    echo     if ($file.Name -like '*secret*' -or $file.Name -like '*token*' -or $file.Name -like '*credential*'^) { $skip = $true }
    echo     foreach ($dir in $ExcludeDirs^) {
    echo         if ($file.FullName -like "*\$dir\*"^) { $skip = $true; break }
    echo     }
    echo     -not $skip
    echo }
    echo.
    echo foreach ($File in $Files^) {
    echo     $isText = $true
    echo     if ($File.Length -gt 0 -and $File.Length -lt 2MB^) {
    echo         # Read first 1024 bytes and check for null bytes 0x00 to filter out binaries
    echo         $bytes = Get-Content $File.FullName -Encoding Byte -TotalCount 1024
    echo         if ($null -ne $bytes -and $bytes -contains 0^) { $isText = $false }
    echo     } elseif ($File.Length -ge 2MB^) {
    echo         # Skip files larger than 2MB to prevent blowing up the AI context limits
    echo         $isText = $false
    echo     }
    echo.
    echo     if ($isText^) {
    echo         # Get relative path and change Windows backslashes to forward slashes
    echo         $RelPath = $File.FullName.Substring($PrefixLen^).Replace('\', '/'^)
    echo         Write-Host "Adding: $RelPath"
    echo         
    echo         Add-Content -Path $OutputFile -Value $RelPath -Encoding UTF8
    echo         $content = Get-Content $File.FullName -Raw -Encoding UTF8
    echo         if ($null -ne $content^) { Add-Content -Path $OutputFile -Value $content -Encoding UTF8 }
    echo         Add-Content -Path $OutputFile -Value '#####################' -Encoding UTF8
    echo     }
    echo }
)

:: Execute the generated PowerShell script
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"

:: Clean up the temporary script
del "%PS_SCRIPT%"

echo.
echo =======================================================
echo Done! Combined files are safely saved to: %OUTPUT_FILE%
echo =======================================================
pause