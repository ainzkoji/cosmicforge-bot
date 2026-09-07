# Section F-3 — Daily Close End-to-End Paper Validation (Procedure)

This document describes the **paper/testnet** daily-close validation procedure required by Section F.

## Safety Preconditions

- Run in `paper` / `testnet` mode only (never `live`).
- Use a disposable DB (or a dedicated paper DB) to keep evidence clean.

## Recommended Validation Steps (Manual / Runtime)

1. Start the bot in paper/testnet mode.
2. Ensure at least one paper position is open (manual or bot-opened).
3. Temporarily set:
   - `DAILY_CLOSE_WINDOW_START` to a few minutes ahead of current UTC time
   - `DAILY_CLOSE_WINDOW_END` to a reasonable window after start (e.g. +30 minutes)
4. Wait for the close window (or trigger a scheduler/check cycle).
5. Confirm the position is closed (paper executor / exchange state).
6. Confirm audit evidence exists:
   - `DAILY_PROFIT_CLOSE_TRIGGERED`
   - `DAILY_CLOSE_POSITION_CLOSE_SUCCESS` (or failure diagnostics)
7. Confirm a CLOSE fill is recorded with:
   - `exit_reason = DAILY_CLOSE` (aka `ExitReason.EXIT_DAILY_CLOSE`)
   - `net_pnl`, `total_fees`, timestamps populated
8. Confirm daily state resets for the next session.
9. Restore original `DAILY_CLOSE_WINDOW_START` / `DAILY_CLOSE_WINDOW_END`.

## Admin/Test Hook (Paper/Testnet Only)

Backend endpoint:

- `POST /api/admin/bots/{bot_instance_id}/test/daily-close`

Safety properties:

- Admin-only
- Rejects `mode=live`
- Writes audit events:
  - `DAILY_CLOSE_VALIDATION_STARTED`
  - `DAILY_PROFIT_CLOSE_TRIGGERED`
  - `DAILY_CLOSE_POSITION_CLOSED`
  - `DAILY_CLOSE_FILL_RECORDED`
  - `DAILY_CLOSE_STATE_RESET`
  - `DAILY_CLOSE_VALIDATION_COMPLETED` / `DAILY_CLOSE_VALIDATION_FAILED`
- Persists a report row in `daily_close_validation_reports`

## Validation Evidence (DB)

Primary artifacts:

- `events` rows for the audit events above
- `trade_fills` row for the synthetic CLOSE fill (`exit_reason=DAILY_CLOSE`)
- `daily_close_validation_reports` row for the end-to-end validation report

