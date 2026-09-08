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
    [int]$Port = 9000
)

$ErrorActionPreference = 'Stop'

# ── 1-3. Resolve canonical paths ────────────────────────────────────────────
$RepoRoot   = Split-Path -Parent $PSScriptRoot
$BackendDir = Join-Path $RepoRoot 'backends\bot-backend'
$Python     = Join-Path $RepoRoot 'backends\venv\Scripts\python.exe'
$LogDir     = Join-Path $BackendDir 'logs\runtime'
$StopFile   = Join-Path $BackendDir 'logs\runtime\STOP'

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
    $dbInfo = & $Python -c @'
import json, os, sys
sys.path.insert(0, os.getcwd())
try:
    from shared_lib.persistence.db import DB
    from app.core.config import settings
    p = DB().path
    print(json.dumps({
        "path": p,
        "exists": os.path.exists(p),
        "size": os.path.getsize(p) if os.path.exists(p) else 0,
        "role": getattr(settings, "DATABASE_ROLE", "development"),
    }))
except Exception as exc:
    print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
'@ 2>$null | Select-Object -Last 1

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
    $leaseInfo = & $Python -c @'
import json, os, sys
sys.path.insert(0, os.getcwd())
try:
    from shared_lib.persistence.db import DB
    from app.ops.runtime_ownership import RuntimeOwnership, current_owner
    db = DB()
    owner = current_owner(db, db.path)
    if not owner:
        print(json.dumps({"held": False}))
    else:
        probe = RuntimeOwnership(db, database_path=db.path)
        stale = probe._is_stale(owner.get("heartbeat_at"), __import__("datetime").datetime.now(__import__("datetime").timezone.utc))
        alive = RuntimeOwnership._pid_alive(int(owner.get("pid", -1)))
        print(json.dumps({
            "held": True, "pid": owner.get("pid"), "hostname": owner.get("hostname"),
            "heartbeat_at": owner.get("heartbeat_at"), "stale": bool(stale), "pid_alive": bool(alive),
        }))
except Exception as exc:
    print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
'@ 2>$null | Select-Object -Last 1

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

        Set-Content -Path $pidFile -Value $proc.Id -Encoding utf8   # 11. record PID
        Write-Step "pid          : $($proc.Id)"

        if ($Once) { Wait-Process -Id $proc.Id; Write-Step "process exited (-Once)"; break }

        # Poll so an operator stop file is noticed promptly.
        while (-not $proc.HasExited) {
            if (Test-Path $StopFile) {
                Write-Step "stop file detected - operator shutdown"
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                Wait-Process -Id $proc.Id -ErrorAction SilentlyContinue
                Remove-Item $StopFile -Force -ErrorAction SilentlyContinue
                Write-Step "stopped by operator; not restarting"
                return
            }
            Start-Sleep -Seconds 2
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
