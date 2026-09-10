<#
.SYNOPSIS
    Put the current shell into the canonical runtime environment.

.DESCRIPTION
    Dot-source this to work by hand in the same environment the supervised
    runtime uses:

        . .\scripts\activate_manual_runtime.ps1

    It activates the project venv, moves to the backend directory and reports
    what the runtime situation currently is. It does NOT start anything, and it
    does not change how the runtime decides ownership: a manual

        python -m uvicorn app.main:app --host 0.0.0.0 --port 9000

    started from here is subject to exactly the same preflight as a supervised
    one, and will refuse -- before writing any evidence -- if a runtime is
    already active. That refusal is the feature; do not work around it.

    Running the file without dot-sourcing still works, but the venv activation
    disappears with the child scope, which is almost never what you wanted.
#>
[CmdletBinding()]
param(
    [int]$Port = 9000
)

$RepoRoot   = Split-Path -Parent $PSScriptRoot
$BackendDir = Join-Path $RepoRoot 'backends\bot-backend'
$Activate   = Join-Path $RepoRoot 'backends\venv\Scripts\Activate.ps1'
$Python     = Join-Path $RepoRoot 'backends\venv\Scripts\python.exe'

if ($MyInvocation.InvocationName -ne '.') {
    Write-Host "[activate] NOTE: not dot-sourced -- the venv will not survive this script." -ForegroundColor Yellow
    Write-Host "[activate]       use:  . .\scripts\activate_manual_runtime.ps1"
}

if (-not (Test-Path $Activate)) {
    Write-Host "[activate] FATAL: venv not found at $Activate" -ForegroundColor Red
    return
}

& $Activate
Set-Location $BackendDir
$env:PYTHONUTF8 = '1'
$env:PYTHONUNBUFFERED = '1'

Write-Host ""
Write-Host "[activate] interpreter : $Python"
Write-Host "[activate] working dir : $BackendDir"
Write-Host ""

& $Python (Join-Path $PSScriptRoot 'runtime_status.py') --port $Port |
    Where-Object { $_ -and $_.TrimStart().StartsWith('{') } |
    Select-Object -Last 1 |
    ForEach-Object {
        $s = $_ | ConvertFrom-Json
        if ($s.running) {
            Write-Host "[activate] a runtime is ALREADY ACTIVE" -ForegroundColor Yellow
            Write-Host "[activate]   pid=$($s.port_pid) session=$($s.lease_session_id)"
            Write-Host "[activate]   starting another uvicorn here will be refused by preflight."
            Write-Host "[activate]   stop it first:  .\scripts\trading_runtime.ps1 stop"
        } else {
            Write-Host "[activate] no runtime is active; port $Port is free" -ForegroundColor Green
            Write-Host "[activate] start one with:"
            Write-Host "[activate]   python -m uvicorn app.main:app --host 0.0.0.0 --port $Port"
        }
    }
Write-Host ""
