<#
.SYNOPSIS
  CATI Section 22 local certification -- one command, real data only.

.DESCRIPTION
  Thin wrapper around the canonical Section 22 CLI
  (python -m app.trading_intelligence.research.certification). It adds no
  certification logic of its own:

    1. locates and validates the canonical candle DB (read-only)
    2. runs `plan` and prints coverage / chronology / feasibility
    3. runs the supported stages with the frozen RESEARCH_DEFAULT_V1 policy
       (FULL / MEDIUM report BLOCKED_DATA themselves when coverage is short)
    4. prints dataset / policy / freeze / report hashes and every stage status
    5. NEVER activates CATI: the CATI execution / exit-routing / ML flags are
       cleared for this process, and the pipeline never touches them
    6. fails closed (exit 2) when real data is absent

  The HOLDOUT is opened only with -OpenHoldout, and the pipeline itself still
  refuses unless the source tree is committed/clean and the preceding stage
  PASSED. A holdout opens exactly once.

.EXAMPLE
  cd backends\bot-backend
  .\scripts\cati\run_final_local_certification.ps1
  .\scripts\cati\run_final_local_certification.ps1 -Symbols BTCUSDT,ETHUSDT -Timeframe 15m
#>
[CmdletBinding()]
param(
    [string]$Db = "..\shared\shared_lib\persistence\cosmicforge.db",
    [string[]]$Symbols = @("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"),
    [string]$Timeframe = "15m",
    [string]$ResearchDb = "..\..\data\research\certification.db",
    [string]$Artifacts = "..\..\data\research\certification",
    [string]$RuntimeDb = "",
    [switch]$OpenHoldout
)

$ErrorActionPreference = "Stop"
$backend = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $backend

foreach ($flag in "CATI_ACTIVE_EXECUTION_ENABLED", "CATI_EXIT_INTENT_ROUTING_ENABLED", "CATI_ML_ENABLED") {
    Remove-Item "Env:$flag" -ErrorAction SilentlyContinue   # never activate CATI from here
}

$python = @(".\venv\Scripts\python.exe", "..\..\venv\Scripts\python.exe", "python") |
    Where-Object { $_ -eq "python" -or (Test-Path $_) } | Select-Object -First 1

if (-not (Test-Path $Db)) {
    Write-Host "BLOCKED_DATA: canonical candle DB not found at $Db" -ForegroundColor Red
    Write-Host "Backfill (bounded): $python scripts\ml\backfill_historical_candles.py --db-path $Db --symbols $($Symbols -join ',') --intervals $Timeframe --days 730"
    exit 2
}

$symbolArg = $Symbols -join ","
Write-Host "== PLAN ==" -ForegroundColor Cyan
& $python -m app.trading_intelligence.research.certification plan --db $Db --symbols $symbolArg `
    --timeframe $Timeframe --artifacts $Artifacts
if ($LASTEXITCODE -ne 0) {
    Write-Host "BLOCKED_DATA: plan could not find real candles for the scope (exit $LASTEXITCODE)" -ForegroundColor Red
    exit 2
}

$runArgs = @("-m", "app.trading_intelligence.research.certification", "run", "--stage", "ALL", "--db", $Db,
             "--symbols", $symbolArg, "--timeframe", $Timeframe, "--research-db", $ResearchDb,
             "--artifacts", $Artifacts)
if ($RuntimeDb) { $runArgs += @("--runtime-db", $RuntimeDb) }
if ($OpenHoldout) { $runArgs += "--open-holdout" }

Write-Host "== RUN (frozen RESEARCH_DEFAULT_V1) ==" -ForegroundColor Cyan
& $python @runArgs
if ($LASTEXITCODE -ne 0) { Write-Host "certification run exited $LASTEXITCODE" -ForegroundColor Red; exit $LASTEXITCODE }

Write-Host "== STATUS ==" -ForegroundColor Cyan
& $python -m app.trading_intelligence.research.certification status --research-db $ResearchDb
Write-Host "CATI active execution remains OFF. Promotion is governed by Section 25 only." -ForegroundColor Yellow
