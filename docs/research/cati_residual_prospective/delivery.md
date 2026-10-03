# Residual Momentum prospective collection: delivery

The frozen Mandate 003 residual family is continuously collecting in CATI OBSERVE.
No rule, threshold, universe, geometry, cost, timeframe or ranking change was made.
No orders, forecast training, new alpha family, Mandate 004 or holdout access.

START_COMMIT = bf23a957924fb3d3a6e7d3009f0fd9cc55039d6b.
Runtime source commit = ddccfa1c2565c7e263cd96adc152f0c93afac7c8.
Final receipt commit and clean/remote verification are supplied in final delivery.
Registry SHA-256 = f3cac76976590fb59ae9f83252092dc484cf89e59b3880945288bc92458ce36f (unchanged).

The canonical runtime was gracefully restarted to load the hook. PID 1316,
one running session, CATI / OBSERVE / BLOCKED / M0. Runtime source remains exactly
on the tested and committed revision; the final additional changes are receipts.
The existing microstructure observer remains active (2108
source-specific feature records at the snapshot). FX has one logical writer and
its original supervisor/completion watcher; 150
periods remain at 2026-10-03T18:02:09.893200+00:00. No duplicate writer and no FX freeze.
Existing finalization stays queued for derivation, QA, scale/gap checks and freeze
only on pass.

Enrollment UTC = 2026-10-03T17:54:22.519000+00:00.
The first decision boundary was the next future 18:00 UTC hour, not a historical
replay. Warm-up loaded public closed trailing inputs for all 136 source symbols,
with all 134 registered tradable assets complete and no input errors. No old
historical outcomes or reserved dataset tables were read.

First live decision:

| Field | Observation |
| --- | --- |
| Decision timestamp | 2026-10-03T17:59:59.999000+00:00 |
| Recorded timestamp | 2026-10-03T18:01:48.199000+00:00 |
| Symbol / side | STRKUSDT / LONG |
| Score | 2.053653569 |
| Eligible universe | 134 frozen tradable symbols; BTC/ETH are the two additional factors |
| Decision-close entry reference | 0.05189 |
| Next-native-open reference | 0.05188 |
| Stop | 0.040265892857142854 |
| Target | 0.08095026785714285 |
| Fixed structural risk | 0.011624107142857143 |
| Modeled costs | Frozen rates and entry/exit-notional policy persisted in live_evidence.json; full48h funding reserve |
| Risk state | OBSERVE, entry authority BLOCKED, portfolio selected, no overlap, no orders |
| Lifecycle | OPEN observation, outcome not yet available |

Decision recording latency was 108200ms.
The first 15m outcome bar had not closed when this decision was committed.
The entry reference was requested only after commit; partial H/L/C were never
used. These are reference observations, not claimed real fills. No outcome has
been fabricated to finish the task. TARGET/STOP/TIMEOUT and gross/cost/net R will
settle automatically from subsequent closed native bars, with restart-safe cursor,
stop priority, gap checks and frozen costs.

One portfolio observation may be pending/open. Subsequent overlapping hourly top1
observations retain prospective counterfactual outcomes, with portfolio_selected=0
and explicit rejection; they are never counted as portfolio trades. Missing/no-signal
or missed-hour decisions retain explicit skip reasons. No threshold is relaxed to
create observations.

Tests: 16 focused synthetic tests passed. Full CATI/runtime suite: 1,442 passed,
8 existing LightGBM feature-name warnings, 464.72 seconds. Source hashes and frozen
registry/evaluator integrity verified again after deployment. Read-only live validation
verified all required decision fields, enrollment timing, post-commit entry reference,
one runtime/FX writer, active OBSERVE/BLOCKED/M0 and zero latest-cycle attempts/fills.
Outcome completion is covered by synthetic gap/stop/target/192-bar timeout tests;
the genuine first observation remains open at this snapshot.

[Tracker design and status command](README.md), [source/test receipt](implementation_receipt.json),
[live decision and authority proof](live_evidence.json), [collector status](prospective_status.json),
[runtime and FX proof](operations.json).
