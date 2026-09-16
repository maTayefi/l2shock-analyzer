@echo off
setlocal

cd /d "%~dp0"

echo Scanning for recently modified .py files...
echo (Up to 200 newest; sorted newest-to-oldest)
echo (Date headers are shown only for the latest 3 distinct modification dates)
echo --------------------------------------------------------

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$root = (Get-Location).Path.TrimEnd('\'); ^
$top = Get-ChildItem -Path . -Recurse -Filter '*.py' -File -ErrorAction SilentlyContinue ^| ^
    Sort-Object -Property LastWriteTime -Descending ^| ^
    Select-Object -First 200; ^
if (-not $top) { ^
    Write-Output 'No .py files found.'; ^
    exit ^
}; ^
$latestDates = @($top ^| ^
    ForEach-Object { $_.LastWriteTime.Date } ^| ^
    Sort-Object -Descending -Unique ^| ^
    Select-Object -First 3); ^
$currentDate = [datetime]::MinValue; ^
$olderSectionPrinted = $false; ^
foreach ($f in $top) { ^
    $date = $f.LastWriteTime.Date; ^
    if ($date -ne $currentDate) { ^
        $currentDate = $date; ^
        $dateIsShown = $latestDates -contains $date; ^
        if ($dateIsShown) { ^
            Write-Output ''; ^
            Write-Output ('[' + $date.ToString('yyyy-MM-dd') + ']') ^
        } elseif (-not $olderSectionPrinted) { ^
            Write-Output ''; ^
            Write-Output 'Older files (date headers omitted):'; ^
            $olderSectionPrinted = $true ^
        } ^
    }; ^
    $full = $f.FullName; ^
    $sizeKB = [math]::Round($f.Length / 1KB, 1); ^
    $sizeSuffix = if ($sizeKB -gt 25) { '  [' + $sizeKB + ' KB]' } else { '' }; ^
    if ($full.StartsWith($root, [System.StringComparison]::OrdinalIgnoreCase)) { ^
        Write-Output (($full.Substring($root.Length).TrimStart('\')) + $sizeSuffix) ^
    } else { ^
        Write-Output ($full + $sizeSuffix) ^
    } ^
}"

echo.
echo --------------------------------------------------------
pause