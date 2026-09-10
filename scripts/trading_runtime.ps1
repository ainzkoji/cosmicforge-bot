<#
.SYNOPSIS
    One operator surface for the canonical CosmicForge trading runtime.

.DESCRIPTION
    status   what is running, who owns the lease, on what revision
    start    start the runtime, refusing rather than duplicating
    stop     graceful stop: quiesce, release the lease, close the session
    restart  stop then start, verifying the port is actually free between

    Force termination is not the normal path. `stop` asks the runtime to shut
    itself down and waits; it only escalates after -TimeoutSeconds, and when it
    does it says so.

.EXAMPLE
    .\scripts\trading_runtime.ps1 status
    .\scripts\trading_runtime.ps1 stop
    .\scripts\trading_runtime.ps1 start -Mode manual
    .\scripts\trading_runtime.ps1 restart
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('status', 'start', 'stop', 'restart')]
    [string]$Command = 'status',

    [ValidateSet('manual', 'supervised')]
    [string]$Mode = 'supervised',

    [int]$Port = 9000,
    [int]$TimeoutSeconds = 45
)

$ErrorActionPreference = 'Stop'

$RepoRoot   = Split-Path -Parent $PSScriptRoot
$BackendDir = Join-Path $RepoRoot 'backends\bot-backend'
$Python     = Join-Path $RepoRoot 'backends\venv\Scripts\python.exe'
$StopFile   = Join-Path $BackendDir 'logs\runtime\STOP'
$StoppedMarker = Join-Path $BackendDir 'logs\runtime\STOPPED_BY_OPERATOR'
$LogDir     = Join-Path $BackendDir 'logs\runtime'

function Write-Line($msg) { Write-Host "[trading_runtime] $msg" }
function Fail($msg) { Write-Host "[trading_runtime] FATAL: $msg" -ForegroundColor Red; exit 1 }

if (-not (Test-Path $Python)) { Fail "canonical interpreter not found: $Python" }

function Get-RuntimeStatus {
    Push-Location $BackendDir
    try {
        $raw = & $Python (Join-Path $PSScriptRoot 'runtime_status.py') --port $Port 2>$null
        $line = @($raw | Where-Object { $_ -and $_.TrimStart().StartsWith('{') }) | Select-Object -Last 1
        if (-not $line) { return $null }
        return $line | ConvertFrom-Json
    } finally { Pop-Location }
}

function Test-PortFree {
    $c = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
    return (-not $c)
}

function Show-Status($s) {
    if (-not $s) { Write-Line "status unavailable"; return }
    Write-Host ""
    Write-Host "  running              : $($s.running)"
    Write-Host "  preflight            : $($s.status)"
    Write-Host "  message              : $($s.message)"
    Write-Host "  port                 : $($s.port)"
    Write-Host "  port pid             : $($s.port_pid)"
    Write-Host "  lease owner pid      : $($s.lease_pid)"
    Write-Host "  lease heartbeat age  : $($s.lease_heartbeat_age_seconds) s"
    Write-Host "  lease stale          : $($s.lease_stale)"
    Write-Host "  runtime_session_id   : $($s.lease_session_id)"
    Write-Host "  code revision        : $($s.code_revision)"
    Write-Host "  database             : $($s.database_path)"
    Write-Host "  database role        : $($s.database_role)"
    if ($s.process) {
        Write-Host "  process parent pid   : $($s.process.parent_pid) ($($s.process.parent_name))"
    }
    Write-Host "  sessions RUNNING     : $($s.sessions_marked_running)"
    foreach ($b in $s.bots) {
        Write-Host "  bot $($b.id)  $($b.bot_health_status) / $($b.bot_health_reason_code)  last_run=$($b.last_run_at)"
    }
    Write-Host ""
}

function Invoke-GracefulStop {
    $before = Get-RuntimeStatus
    if (-not $before -or -not $before.running) {
        Write-Line "runtime is not running; nothing to stop"
        if (-not (Test-PortFree)) {
            Write-Line "WARNING: port $Port is still held by pid $((Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess)"
        }
        return $true
    }

    $ownerPid = $before.port_pid
    if (-not $ownerPid) { $ownerPid = $before.lease_pid }
    Write-Line "requesting graceful shutdown of pid $ownerPid (session $($before.lease_session_id))"

    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    # A marker left by a previous stop must not be mistaken for this one.
    Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue
    New-Item -ItemType File -Path $StopFile -Force | Out-Null

    # The runtime polls for the stop file, quiesces, releases the lease, closes
    # its session and then raises SIGINT so uvicorn runs its own shutdown.
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 2
        $now = Get-RuntimeStatus
        $portFree = Test-PortFree
        $leaseGone = (-not $now) -or (-not $now.lease_pid) -or ($now.status -eq 'READY')
        if ($portFree -and $leaseGone) {
            Write-Line "graceful stop complete (lease released, port $Port free)"
            Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
            Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue
            return $true
        }
    }

    # Escalation. Only here, only after the timeout, and it is recorded.
    Write-Line "graceful stop did not complete within ${TimeoutSeconds}s"
    Write-Host "[trading_runtime] FORCED_RUNTIME_TERMINATION pid=$ownerPid" -ForegroundColor Yellow
    Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
    if ($ownerPid) {
        # Kill the listener and its launcher stub: the venv python.exe is a
        # redirector, so stopping only the tracked pid orphans the real server.
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$ownerPid" -ErrorAction SilentlyContinue
        if ($proc -and $proc.ParentProcessId) {
            Stop-Process -Id $proc.ParentProcessId -Force -ErrorAction SilentlyContinue
        }
        Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 3
    if (Test-PortFree) {
        Write-Line "port $Port released after forced termination"
        Write-Line "the stale lease will be recovered by PID-liveness takeover on next start"
        return $true
    }
    Fail "port $Port still held after forced termination"
}

function Invoke-Start {
    $s = Get-RuntimeStatus
    if ($s -and $s.running) {
        Write-Line "refusing to start: a canonical runtime is already active"
        Show-Status $s
        exit 1
    }
    if (-not (Test-PortFree)) {
        Fail "port $Port is occupied but no canonical runtime was detected. Inspect it before starting."
    }

    # A stop request from the last shutdown is not a request to stop this one.
    Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
    Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue

    if ($Mode -eq 'manual') {
        Write-Line "starting manual uvicorn on port $Port (Ctrl+C to stop)"
        Push-Location $BackendDir
        try {
            $env:PYTHONUTF8 = '1'
            $env:PYTHONUNBUFFERED = '1'
            & $Python -u -X utf8 -m uvicorn app.main:app --host 0.0.0.0 --port $Port --log-level info
        } finally { Pop-Location }
        return
    }

    $launcher = Join-Path $PSScriptRoot 'start_trading_runtime.ps1'
    if (-not (Test-Path $launcher)) { Fail "supervised launcher not found: $launcher" }
    Write-Line "starting supervised runtime"
    Start-Process -FilePath 'powershell.exe' `
        -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $launcher, '-Port', "$Port") `
        -WorkingDirectory $RepoRoot -WindowStyle Hidden | Out-Null

    # The lease is taken during application startup; uvicorn binds the port
    # only afterwards. Reporting "up" at the first of those shows the operator
    # the lease-held-but-port-free wording, which reads like a conflict. Wait
    # for both before saying the runtime is up.
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $partial = $null
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 2
        $s = Get-RuntimeStatus
        if ($s -and $s.running) {
            $partial = $s
            if ($s.port_pid) {
                Write-Line "runtime is up"
                Show-Status $s
                return
            }
        }
    }
    if ($partial) {
        Write-Line "WARNING: the lease is held by pid $($partial.lease_pid) but port $Port is not bound yet"
        Show-Status $partial
        return
    }
    Fail "runtime did not come up within ${TimeoutSeconds}s"
}

switch ($Command) {
    'status'  { Show-Status (Get-RuntimeStatus) }
    'stop'    { if (Invoke-GracefulStop) { Show-Status (Get-RuntimeStatus) } }
    'start'   { Invoke-Start }
    'restart' {
        if (-not (Invoke-GracefulStop)) { Fail "stop failed; not restarting" }
        Start-Sleep -Seconds 2
        Invoke-Start
    }
}
