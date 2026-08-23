# IOFS Paper Validation Status

- start_date: 2026-06-15
- start_timestamp_utc: 2026-06-15T04:46:39.294507Z
- post_repair_restart_time: 2026-06-15T04:46:39.294507Z
- earliest_four_week_completion_date: 2026-07-13
- earliest_four_week_completion_timestamp_utc: 2026-07-13T04:46:39.294507Z
- end_date: Pending
- number_of_closed_paper_trades: 0
- win_rate: Pending
- TP1 count: 0
- TP2 count: 0
- SL count: 0
- daily close count: 0
- runner/trailing exit count: 0
- TP1:TP2 ratio: Pending
- number reviewed within 24h: 0
- missed A+ setups: Pending
- crash loops: 1
- circuit breaker trips: 0
- status: In Progress

## Active Monitoring

- bot_instance_id: bot_e5fe913972a9
- bot_instance_mode: paper
- bot_instance_symbols: BTCUSDT,ETHUSDT
- nightly_dataset_monitor: cosmicforge-iofs-dataset-monitor
- accepted_trade_review_monitor: cosmicforge-iofs-trade-review-monitor

## Strong Trend Paper-Only Experiment

- Strong trend paper-only experiment started: yes
- experiment_start_time: 2026-06-15T17:21:40.112591Z
- experiment_scope: paper only
- ML enabled: no
- live capital: no
- separate strong_trend metrics: yes
- STRONG_TREND_ALLOWED_ONLY_IN_PAPER: true
- clean_runtime_confirmation_time: 2026-06-15T17:24:58.472961Z
- clean_cycles_observed: 10
- REGIME_BLOCKED_STRONG_TREND_in_clean_sample: 0
- strong_trend_paper_orders_created: 0
- auto_stop_rules_configured: yes

This controlled experiment is part of Section 4 paper validation. It collects
forward evidence after the regime classifier repair and does not approve live
trading or capital deployment.

## Post-Restart In-Session Confirmation

- Post-restart in-session runtime confirmation completed: yes
- confirmation_time: 2026-06-15T15:45:30.035848Z
- cycles_observed: 5 complete cycles
- BTCUSDT observations: 5
- ETHUSDT observations: 5
- component_diagnostics_active: yes
- paper_executor_reached: no natural signal reached executor
- paper_orders_created: 0
- strict_restart_acceptance_closed: yes

Runtime component diagnostics were persisted during an allowed WEAK_TREND
BTCUSDT evaluation. Seven component records included indicator snapshots and
exact failed conditions. No trade was forced.

## Post-Repair Validation Note

Paper validation before the controlled restart may not represent the repaired
strategy because regime hysteresis was pinned. Post-restart validation should
be treated as the clean forward-validation period for the repaired strategy.

The superseded pre-repair validation period began at
`2026-06-13T18:20:04Z`. Its activity is retained for audit history but does not
count toward the clean post-repair four-week period.

## Clean Post-Repair Start Proof

- verified_at_utc: 2026-06-15T04:55:28.0632786Z
- process_started_at_utc: 2026-06-15T04:46:39.294507Z
- runner_running: true
- execution_mode: paper
- exchange_environment: testnet
- ML_enabled: false
- IOFS_mode: shadow
- IOFS_events_since_start: Pending monitoring
- IOFS_symbols_since_start: BTCUSDT,ETHUSDT configured
- IOFS_blocked_trade_events_since_start: 0
- trade_fills_since_start: 0
- active_positions_at_start: 0
- crash_events_since_start: 0
- circuit_events_since_start: 0

Section 4 is not passed. Validation requires at least four calendar weeks and
20 complete closed paper trades, plus all acceptance criteria.

The clean validation start excludes earlier runtime activity because the
repaired `RegimeClassifier.classify_stable()` implementation only became the
controlled paper runtime at the post-repair restart time above.

## Trade Review Monitor Audit

- reviewed_at_utc: 2026-08-22T20:00:42.1886741Z
- audit_window_start_utc: 2026-06-13T18:20:04Z
- events_table_first_event_in_window_utc: 2026-06-13T18:20:21.001598+00:00
- events_table_event_count_in_window: 38455
- IOFS_gate_evaluations_in_window: 38275
- latest_decision_trace_utc: 2026-07-18T10:28:49.318350+00:00
- latest_events_table_utc: 2026-07-18T10:30:29.148225+00:00
- live_audit_jsonl_events_in_window: 38453
- latest_live_audit_jsonl_event_utc: 2026-07-18T10:30:29.182259+00:00
- accepted_trade_events_in_window: 2
- closed_trade_events_in_window: 0
- paper_submit_attempts_without_fills: 69
- review_blocks_appended_this_run: 2
- reviews_older_than_24h: 2
- monitor_snapshot_was_stale_at_run_start: true
- symbol_violations_found: 0
- mode_violations_found: 0
- live_execution_evidence_found: 0
- ML_enabled_evidence: false
- execution_mode_evidence: paper-only DB traces and preserved audit stream
- exchange_environment_evidence: no live-environment evidence found
- IOFS_mode_evidence: shadow
- circuit_breaker_trip_evidence_found: 0
- crash_loop_evidence_found: 1
- local_health_endpoint_responding: not checked in this run

Persisted Section 4 runtime evidence still stops on `2026-07-18`. The
preserved live audit stream in
[`backends/bot-backend/logs/live_audit.jsonl`](C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/logs/live_audit.jsonl)
still contains 38453 in-window events beginning at
`2026-06-13T18:20:21.046587+00:00` and ending at
`2026-07-18T10:30:29.182259+00:00`, so this `2026-08-22` refresh again starts
from a stale monitoring snapshot more than 24 hours after the prior automation
run on `2026-08-21T19:56:49.661Z`.

The July runtime window is re-queryable in
[`backends/shared/shared_lib/persistence/cosmicforge.db`](C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/shared/shared_lib/persistence/cosmicforge.db),
which preserves 38169 in-window `decision_traces`, 38455 in-window `events`,
2 in-window `trade_fills`, and 122 in-window `runs`. The lighter local SQLite
artifacts at `data/bot.db`, `backends/bot-backend/data/bot.db`, and
`backends/bot-backend/data/cosmicforge.db` still return zero clean-window
trade or decision rows, so the shared persistence DB is the authoritative
source for the accepted-trade reconstruction in this workspace.

Within the clean-start review window, shared-DB `events` preserve 38275
`IOFS_GATE` evaluations and the live-audit JSONL preserves 38273; both sources
remain limited to BTCUSDT and ETHUSDT only, and every preserved IOFS payload
shows `mode=shadow`, `risk_profile=balanced`, `blocked_trade=false`, and no
`passed=true` evidence. No symbol violation, mode violation, `ml_enabled=true`,
or live-execution environment evidence appears in either preserved source.

Accepted-trade evidence is now limited to two July 15 paper smoke opens in the
shared DB, both for BTCUSDT under `bot_instance_id=paper_smoke` and
`run_id=paper_execution_smoke`. No `CLOSE` fill appears in-window, so complete
closed paper trades remain zero and Section 4 totals stay unchanged. The
accepted fills were previously untracked in the review ledger, so two overdue
review blocks were appended to
[`backends/bot-backend/models/reports/iofs_paper_trade_reviews.md`](C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/models/reports/iofs_paper_trade_reviews.md)
for `paper_position_fbb75c1ad8a64a179b951f306fa88bf8` and
`paper_position_cb08561bd5554e3ca61de93edd97bb85`. Both entries were first
accepted on `2026-07-15`, so `reviews_older_than_24h` is now 2 and
`number reviewed within 24h` remains 0.

The preserved live audit still contains 28 malformed lines aligned with 28
`ERROR` events carrying `action=CYCLE_STEP_ERROR` on `2026-07-16`. No
circuit-breaker trip evidence appears in the preserved audit stream or shared
DB event history. Crash-loop risk remains flagged because the shared DB now
shows 122 clean-window `runs`, 120 `RECONCILE_POSITIONS_STARTUP` events, and
tight restart bursts on `2026-06-13` (12 runs), `2026-06-14` (52 runs),
`2026-06-15` (43 runs), and `2026-07-15` (10 runs). Section 4 remains in
progress and is not passed.

## Paper Executor Repair Note

- paper_executor_repair_time: 2026-07-15T21:13:27.089449+00:00
- historical_paper_only_skipped_attempts: 68
- old_skipped_attempts_count_toward_section4: no
- post_paper_executor_repair_validation_start: 2026-07-15T21:13:27.089449+00:00

Section 4 remains in progress and is not passed by this repair smoke validation.

## Latest Daily Monitor Snapshot

- generated_at_utc: 2026-07-17T23:30:00.012139+00:00
- bot_health: healthy
- paper_orders_today: 0
- closed_paper_trades_since_clean_start: 0
- strong_trend_experiment_status: ACTIVE
- section5_retry_ready: false
- active_alerts: ["NO_TRADES_AFTER_24H", "NO_TRADES_AFTER_72H", "NO_TRADES_AFTER_7D", "REGIME_BLOCKING_DOMINATES", "STRONG_TREND_STOP_RECOMMENDED"]
- section4_status: In Progress

Section 4 remains in progress. This snapshot does not approve live trading or capital deployment.
