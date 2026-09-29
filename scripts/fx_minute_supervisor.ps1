<#
.SYNOPSIS
  Unattended, single-writer supervisor for the FX 1m Dukascopy acquisition.

.DESCRIPTION
  `acquire_fx_reference_dataset.py minute` is resumable (FETCHED / EMPTY / NO_FILE periods are never
  re-downloaded; FAILED periods are retried) and stops itself through its circuit breaker when the
  provider keeps refusing (HTTP 429 / 5xx / timeouts). This wrapper only re-runs it:

    * refuses to start while another `minute` writer is running against the research DB;
    * after each run, asks `minute --plan` (read-only) how many pair/day periods remain;
    * exits when none remain; otherwise waits with exponential backoff (10 -> 60 min), reset
      whenever the previous run made progress.

  It never changes the pace, the manifest, or anything the acquisition writes.

.EXAMPLE
  Start-Process powershell -WindowStyle Hidden -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass',
      '-File','scripts\fx_minute_supervisor.ps1'
#>
[CmdletBinding()]
param(
    [double]$Pace = 2.5,
    [int]$InitialBackoffSeconds = 600,
    [int]$MaxBackoffSeconds = 3600,
    [string]$Log = "data\research\logs\fx_minute_supervisor.log"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$python = Join-Path $repo "backends\venv\Scripts\python.exe"
$db = "data\research\fx_reference_dukascopy.db"
$manifest = "docs\research\cati_fx_universe_dukascopy_v1.json"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Log) | Out-Null

function Write-Log($msg) {
    "$([DateTime]::UtcNow.ToString('s'))Z $msg" | Out-File -FilePath $Log -Append -Encoding utf8
}

function Get-Remaining {
    $out = (& $python scripts\acquire_fx_reference_dataset.py --db $db minute --manifest $manifest --plan 2>$null) -join "`n"
    $start = $out.IndexOf("{")
    if ($start -lt 0) { return $null }
    return [int](($out.Substring($start) | ConvertFrom-Json).remaining_periods)
}

$backoff = $InitialBackoffSeconds
$previous = Get-Remaining
Write-Log "supervisor start pid=$PID remaining=$previous"
while ($true) {
    $writers = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -like "*acquire_fx_reference_dataset.py*minute*" -and $_.CommandLine -notlike "*--plan*"
    }
    if ($writers) {
        Write-Log "another minute writer is active (pid $($writers.ProcessId -join ',')); exiting"
        exit 3
    }
    Write-Log "run start"
    & $python scripts\acquire_fx_reference_dataset.py --db $db --pace $Pace minute --manifest $manifest *>> $Log
    Write-Log "run exit code=$LASTEXITCODE"
    $remaining = Get-Remaining
    Write-Log "remaining=$remaining"
    if ($remaining -eq 0) { Write-Log "1m acquisition complete"; exit 0 }
    if ($null -ne $remaining -and $null -ne $previous -and $remaining -lt $previous) {
        $backoff = $InitialBackoffSeconds   # progress was made: the provider is not refusing us persistently
    }
    $previous = $remaining
    Write-Log "backoff ${backoff}s"
    Start-Sleep -Seconds $backoff
    $backoff = [Math]::Min($backoff * 2, $MaxBackoffSeconds)
}
