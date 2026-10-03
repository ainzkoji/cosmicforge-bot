# CATI FX recovery evidence

Observed UTC: 2026-10-03T08:30:29.566390+00:00

No FX supervisor, acquisition writer or completion process existed before recovery. SQLite uses WAL; no dedicated FX lease tables or lock files were found. The existing supervisor was resumed hidden (PID 12076). Its one worker tree is supervisor 12076 -> venv launcher 29932 -> Python worker 35696. These two Python processes represent one logical acquisition worker. The existing completion process was resumed hidden (PID 19436) and waits read-only for acquisition to finish. No duplicate writer or alternative pipeline was created.

Remaining pair/day periods: initial 3,103; observed 3,103. Progress proven: False. The worker scanned completed pairs before retrying GBPAUD gaps. A separate bounded read-only diagnostic for the outstanding GBPAUD 2026-05-19 BID file returned Dukascopy HTTP 503 in 0.4 seconds (107 bytes, no Retry-After). This proves a current provider failure on that needed file; it does not claim every outstanding file is unavailable. The prior supervisor stopped after six consecutive provider failures and entered 600-second backoff. The resumed supervisor retains its existing 600-to-3600-second exponential backoff and circuit breaker.

Derivation, full QA, scale validation and strict gap classification remain queued until acquisition completes. FX is not frozen. UNKNOWN_GAP rules and freeze checks were unchanged. The finalization regression proving an open-session UNKNOWN_GAP blocks freeze passed (1 test).

Source evidence: `data/research/logs/fx_minute_supervisor.log`, `data/research/fx_finalization/completion_status.json`, read-only `fx_reference_ingest_log` and process inventory copied into the companion JSON. No credentials printed; no holdout price queries.


## Subsequent parent observation

At 2026-10-03T08:59:35Z the same single-writer pipeline had recovered from the initial 503: remaining periods decreased from 3,103 to 2,903. Acquisition is progressing. Derivation, QA, strict gap classification and freeze remain queued behind acquisition; no validation has been weakened. Final delivery reports the newer bounded snapshot.
