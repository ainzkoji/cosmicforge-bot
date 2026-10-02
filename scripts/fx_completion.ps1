<# Wait read-only for the existing single writer, then finalize sequentially. #>
[CmdletBinding()]
param([int]$PollSeconds = 600)
$ErrorActionPreference = 'Stop'
$taskRepo = Split-Path -Parent $PSScriptRoot
Set-Location $taskRepo
$taskPython = Join-Path $taskRepo 'backends\venv\Scripts\python.exe'
$taskOutput = Join-Path $taskRepo 'data\research\fx_finalization'
New-Item -ItemType Directory -Force -Path $taskOutput | Out-Null
while ($true) {
    $taskText = (& $taskPython scripts\acquire_fx_reference_dataset.py --db data\research\fx_reference_dukascopy.db minute --manifest docs\research\cati_fx_universe_dukascopy_v1.json --plan) -join "`n"
    $taskPlan = $taskText | ConvertFrom-Json
    $taskWriters = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -like '*acquire_fx_reference_dataset.py*minute*' -and $_.CommandLine -notlike '*--plan*'
    }
    @{status='WAITING_ACQUISITION';remaining_periods=$taskPlan.remaining_periods;updated_at=[DateTime]::UtcNow.ToString('o')} |
        ConvertTo-Json | Set-Content (Join-Path $taskOutput 'completion_status.json') -Encoding utf8
    if ($taskPlan.remaining_periods -eq 0 -and !$taskWriters) { break }
    Start-Sleep -Seconds $PollSeconds
}
& $taskPython scripts\finalize_fx_reference.py --db data\research\fx_reference_dukascopy.db --manifest docs\research\cati_fx_universe_dukascopy_v1.json --output $taskOutput *>> (Join-Path $taskOutput 'finalization.log')
exit $LASTEXITCODE
