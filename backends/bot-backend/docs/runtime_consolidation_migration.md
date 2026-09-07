# Runtime consolidation database changes

The runtime consolidation uses additive SQLite changes. Existing fills,
decision history, position lifecycle rows, and readiness evidence are retained.

Added columns on `trade_fills`:

- `fill_type`
- `remaining_qty`

Added tables:

- `bot_candle_evaluations`
- `canonical_trade_decisions`
- `cycle_decision_summaries`
- `readiness_approvals`
- `deployment_confirmations`

Before applying these changes to a user or production database, stop the bot and
create a SQLite backup against the exact path printed by the startup
`[DATABASE_SOURCE]` diagnostic:

```powershell
sqlite3.exe "C:\path\to\cosmicforge.db" ".backup 'C:\path\to\cosmicforge.pre-runtime-consolidation.db'"
```

Keep the backup beside the deployment record and verify it with
`PRAGMA integrity_check;`. Startup applies only `CREATE TABLE IF NOT EXISTS` and
idempotent `ALTER TABLE ADD COLUMN` operations for this phase. It never chooses,
deletes, replaces, or copies a database file.
