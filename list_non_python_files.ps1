$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$ExcludedDirectories = @(
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv"
)

Get-ChildItem -Path $Root -File -Recurse |
    Where-Object {
        $_.Extension -notin @(".py", ".pyc") -and
        -not ($_.FullName -split '[\\/]+' | Where-Object {
            $_ -in $ExcludedDirectories
        })
    } |
    ForEach-Object {
        $_.FullName.Substring($Root.Length + 1)
    } |
    Sort-Object

Read-Host "`nPress Enter to exit"