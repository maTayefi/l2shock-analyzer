$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = (Get-Location).Path
$CoresFile = Join-Path $ProjectRoot "project_cores.md"

if (-not (Test-Path $CoresFile)) {
    Write-Error "project_cores.md not found at $CoresFile"
    exit 1
}

$coresContent = Get-Content $CoresFile -Raw

# Collect all Python files in l2shock/ and tests/
$productionFiles = Get-ChildItem -Path (Join-Path $ProjectRoot "l2shock") -Recurse -Filter "*.py" -File |
    Where-Object { $_.FullName -notmatch "__pycache__" } |
    ForEach-Object { $_.FullName.Substring($ProjectRoot.Length + 1).Replace("\", "/") }

$testFiles = Get-ChildItem -Path (Join-Path $ProjectRoot "tests") -Recurse -Filter "*.py" -File |
    Where-Object { $_.FullName -notmatch "__pycache__" } |
    ForEach-Object { $_.FullName.Substring($ProjectRoot.Length + 1).Replace("\", "/") }

$toolFiles = @()
if (Test-Path (Join-Path $ProjectRoot "tools")) {
    $toolFiles = Get-ChildItem -Path (Join-Path $ProjectRoot "tools") -Recurse -Filter "*.py" -File |
        ForEach-Object { $_.FullName.Substring($ProjectRoot.Length + 1).Replace("\", "/") }
}

$rootScripts = Get-ChildItem -Path $ProjectRoot -Filter "*.py" -File |
    ForEach-Object { $_.Name }

$allFiles = @($productionFiles) + @($testFiles) + @($toolFiles) + @($rootScripts)

# Check which files appear in project_cores.md
$missing = @()
$found = @()

foreach ($file in $allFiles) {
    # Normalize: try both forward-slash and backslash
    $normalized = $file.Replace("\", "/")
    $backslash = $file.Replace("/", "\")
    
    if ($coresContent -match [regex]::Escape($normalized) -or $coresContent -match [regex]::Escape($backslash)) {
        $found += $file
    } else {
        $missing += $file
    }
}

Write-Host ""
Write-Host "=" * 70
Write-Host " CORES COVERAGE REPORT"
Write-Host "=" * 70
Write-Host ""
Write-Host "Total files scanned:    $($allFiles.Count)"
Write-Host "Found in cores:         $($found.Count)"
Write-Host "MISSING from cores:     $($missing.Count)"
Write-Host ""

if ($missing.Count -gt 0) {
    Write-Host "FILES NOT IN ANY CORE:" -ForegroundColor Red
    Write-Host "-" * 70
    
    # Group by directory
    $grouped = $missing | Group-Object { Split-Path $_ -Parent }
    
    foreach ($group in ($grouped | Sort-Object Name)) {
        Write-Host ""
        Write-Host "  [$($group.Name)]" -ForegroundColor Yellow
        foreach ($file in ($group.Group | Sort-Object)) {
            Write-Host "    - $($file | Split-Path -Leaf)" -ForegroundColor Red
        }
    }
    
    Write-Host ""
    Write-Host "-" * 70
	Read-Host "`nPress Enter to close"
    Write-Host "FLAT LIST (for copy-paste):" -ForegroundColor Cyan
    Write-Host ""
    $missing | Sort-Object | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Host "All files are covered by project_cores.md!" -ForegroundColor Green
}

Write-Host ""
Write-Host "=" * 70