<#
.SYNOPSIS
    Canonical always-on launcher for the CosmicForge trading backend.

.DESCRIPTION
    Starts exactly one trading backend and keeps it alive with bounded restart
    backoff. Refuses to start if another process already holds the runtime
    trading lease for the same database -- the lease is held against the
    DATABASE, not the port, because two backends on different ports pointed at
    the same DB would both drive the same bots.

    Operator shutdown (Ctrl+C, or the stop file) is distinguished from a crash:
    only crashes trigger a restart.

.PARAMETER Once
    Start once and exit when the process exits. No restart supervision.

.PARAMETER Force
    Start even if a live lease is detected. Use only when you know the holder
    is gone and the lease is stale.

.EXAMPLE
    .\scripts\start_trading_runtime.ps1
#>
[CmdletBinding()]
param(
    [switch]$Once,
    [switch]$Force,
    [int]$Port = 9000,
    # How long a graceful stop may take before force is used. A cycle is ~10s;
    # this allows for one in flight plus lease release and session close.
    [int]$GracefulStopSeconds = 45
)

$ErrorActionPreference = 'Stop'

# ── 1-3. Resolve canonical paths ────────────────────────────────────────────
$RepoRoot   = Split-Path -Parent $PSScriptRoot
$BackendDir = Join-Path $RepoRoot 'backends\bot-backend'
$Python     = Join-Path $RepoRoot 'backends\venv\Scripts\python.exe'
$LogDir     = Join-Path $BackendDir 'logs\runtime'
$StopFile   = Join-Path $BackendDir 'logs\runtime\STOP'
# Written by the runtime itself when it stops on request. Read after the child
# exits, so the supervisor never has to win a polling race to learn why.
$StoppedMarker = Join-Path $BackendDir 'logs\runtime\STOPPED_BY_OPERATOR'

function Write-Step($msg) { Write-Host "[start_trading_runtime] $msg" }
function Fail($msg) { Write-Host "[start_trading_runtime] FATAL: $msg" -ForegroundColor Red; exit 1 }

Write-Step "repo         : $RepoRoot"
if (-not (Test-Path $BackendDir)) { Fail "backend directory not found: $BackendDir" }
if (-not (Test-Path $Python))     { Fail "canonical interpreter not found: $Python" }
Write-Step "interpreter  : $Python"
Write-Step "backend      : $BackendDir"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# ── 4. Verify the database path resolves ────────────────────────────────────
Push-Location $BackendDir
try {
    $Probe  = Join-Path $PSScriptRoot 'runtime_probe.py'
    $dbInfo = & $Python $Probe database 2>$null | Select-Object -Last 1

    $db = $dbInfo | ConvertFrom-Json
    if ($db.error) { Fail "could not resolve database: $($db.error)" }
    if (-not $db.exists) { Fail "database does not exist: $($db.path)" }
    Write-Step "database     : $($db.path) [role=$($db.role), $([math]::Round($db.size/1GB,2)) GB]"

    # ── 5. Port check ───────────────────────────────────────────────────────
    $portOwner = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
                 Select-Object -First 1
    if ($portOwner) {
        $p = Get-Process -Id $portOwner.OwningProcess -ErrorAction SilentlyContinue
        if (-not $Force) {
            Fail "port $Port already in use by PID $($portOwner.OwningProcess) ($($p.ProcessName)). Stop it first, or pass -Force."
        }
        Write-Step "WARNING: port $Port in use by PID $($portOwner.OwningProcess); -Force given"
    }

    # ── 6-7. Runtime ownership lease ────────────────────────────────────────
    $leaseInfo = & $Python $Probe lease 2>$null | Select-Object -Last 1

    $lease = $leaseInfo | ConvertFrom-Json
    if ($lease.error) {
        Write-Step "WARNING: lease check failed ($($lease.error)); the backend will fail closed if a live lease exists"
    } elseif ($lease.held) {
        if ((-not $lease.stale) -and $lease.pid_alive) {
            if (-not $Force) {
                Fail "runtime trading lease is held by a live process (pid=$($lease.pid) host=$($lease.hostname), heartbeat=$($lease.heartbeat_at)). Refusing to start a duplicate canonical runner."
            }
            Write-Step "WARNING: live lease held by pid $($lease.pid); -Force given"
        } else {
            Write-Step "lease        : stale/abandoned (pid=$($lease.pid)) - will be taken over"
        }
    } else {
        Write-Step "lease        : free"
    }
}
finally { Pop-Location }

# ── 8-12. Supervised launch ─────────────────────────────────────────────────
if (Test-Path $StopFile) { Remove-Item $StopFile -Force }

$backoffs = @(5, 10, 30, 60)   # bounded; never a rapid restart loop
$attempt  = 0

Write-Step "starting supervised backend on port $Port (Ctrl+C or create $StopFile to stop)"

while ($true) {
    $stamp   = Get-Date -Format 'yyyyMMdd-HHmmss'
    $outLog  = Join-Path $LogDir "runtime-$stamp.log"
    $pidFile = Join-Path $LogDir 'runtime.pid'

    Write-Step "launch #$($attempt + 1) -> $outLog"

    # Clear any stop request left over from a previous run BEFORE launching.
    # A marker still on disk from the last shutdown was read as a request to
    # stop the child that had only just started, which then failed to exit
    # (nobody had asked it to) and was force-killed mid-cycle at the grace
    # deadline. Anything found after this point belongs to this child.
    Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
    Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue

    Push-Location $BackendDir
    try {
        $env:PYTHONUTF8 = '1'          # 9. force UTF-8
        $env:PYTHONUNBUFFERED = '1'

        # 8. No --reload: it restarts on DB/log writes and kills the runner loop.
        $proc = Start-Process -FilePath $Python `
            -ArgumentList @('-u', '-X', 'utf8', '-m', 'uvicorn', 'app.main:app',
                            '--host', '0.0.0.0', '--port', "$Port", '--log-level', 'info') `
            -RedirectStandardOutput $outLog `
            -RedirectStandardError  "$outLog.err" `
            -NoNewWindow -PassThru

        # The venv python.exe is a redirector that re-execs the real
        # interpreter, so $proc.Id is the stub and the server is its child.
        # Recording the stub meant the pid file never named the process that
        # actually held the port.
        Start-Sleep -Milliseconds 800
        $listenerPid = $proc.Id
        $child = Get-CimInstance Win32_Process -Filter "ParentProcessId=$($proc.Id)" -ErrorAction SilentlyContinue |
                 Where-Object { $_.Name -eq 'python.exe' } | Select-Object -First 1
        if ($child) { $listenerPid = $child.ProcessId }
        Set-Content -Path $pidFile -Value $listenerPid -Encoding utf8   # 11. record PID
        Write-Step "pid          : $listenerPid (supervisor child $($proc.Id))"

        if ($Once) { Wait-Process -Id $proc.Id; Write-Step "process exited (-Once)"; break }

        # Poll so an operator stop file is noticed promptly.
        #
        # The stop file is NOT consumed here. The runtime itself watches for it,
        # quiesces -- stops new entries, stops the scheduler, drains the current
        # cycle, releases the ownership lease, closes its runtime session -- and
        # then raises SIGINT so uvicorn runs its own shutdown. Force-killing here
        # is what used to skip all of that and leave a lease with released_at
        # NULL against a dead PID.
        while (-not $proc.HasExited) {
            if ((Test-Path $StopFile) -or (Test-Path $StoppedMarker)) {
                Write-Step "stop file detected - waiting for graceful shutdown"
                $graceDeadline = (Get-Date).AddSeconds($GracefulStopSeconds)
                while ((-not $proc.HasExited) -and ((Get-Date) -lt $graceDeadline)) {
                    Start-Sleep -Seconds 1
                }

                if ($proc.HasExited) {
                    Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
                    Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue
                    Write-Step "graceful shutdown complete; not restarting"
                } else {
                    # Escalation, and it is recorded rather than silent. The
                    # stale lease is recovered by PID-liveness takeover at the
                    # next start.
                    Write-Step "FORCED_RUNTIME_TERMINATION: no graceful exit within ${GracefulStopSeconds}s"
                    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                    Wait-Process -Id $proc.Id -ErrorAction SilentlyContinue
                    Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
                    Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue
                    Write-Step "stopped by force; lease will be recovered on next start"
                }

                $stillListening = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
                if ($stillListening) {
                    Write-Step "WARNING: port $Port still held by pid $($stillListening.OwningProcess)"
                } else {
                    Write-Step "port $Port released"
                }
                return
            }
            Start-Sleep -Seconds 2
        }

        # The child is gone. Ask why before deciding whether to restart: an
        # operator stop that the poll above never saw would otherwise read as a
        # crash, and the runtime the operator just stopped would come straight
        # back up. That is exactly what happened before this check existed.
        if (Test-Path $StoppedMarker) {
            $reason = (Get-Content $StoppedMarker -Raw -ErrorAction SilentlyContinue)
            Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue
            Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
            Write-Step "operator shutdown ($($reason.Trim())) - not restarting"
            break
        }

        $code = $proc.ExitCode
        if ($code -eq 0) {
            # 12. Clean exit is an operator shutdown, not a crash.
            Write-Step "process exited cleanly (code 0) - treating as operator shutdown"
            break
        }

        $wait = $backoffs[[Math]::Min($attempt, $backoffs.Count - 1)]
        $attempt++
        Write-Step "CRASH: exit code $code - restarting in ${wait}s (attempt $attempt)"
        Start-Sleep -Seconds $wait
    }
    finally { Pop-Location }
}

Write-Step "supervisor finished"
