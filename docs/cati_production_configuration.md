CATI production configuration
=============================

The production profile is saved in the backend settings and `.env.example`
templates. Local `.env` files use the same profile and remain ignored by Git.
Changing these files does not deploy code, rebuild the served UI, restart a
process, migrate accounts, or change a running process's loaded settings.

| Configuration | Resolved value |
|---|---|
| App / runtime / API / UI / database role | PRODUCTION |
| CATI mode / market data / broker environment | LIVE |
| Engine | CATI only |
| Strategy | RESIDUAL_MOMENTUM_PORTFOLIO_TOP1, Mandate 003 unchanged |
| Trading / discovery / balance, position and order sync / reconciliation | ACTIVE |
| Risk / FX pipeline / prospective forward collection | ACTIVE |
| Legacy V2 / fallback / training / simulation runtime | DISABLED |
| LIVE_ORDER_SUBMISSION_ENABLED | false |
| Permanent daily hard-loss ceiling | 2.5%, tighter controls preserved |

`python scripts/cati_production_config.py` resolves all three backends in
separate interpreters without starting an app or constructing a database. A
conflicting production setting fails validation. Explicit TEST and DEVELOPMENT
profiles retain historical adapters and fixtures; production cannot select
them or use a DEMO account as a LIVE substitute.

At the next authorized startup, the canonical lease holder starts the frozen
prospective collector, public forward observations, existing FX supervision,
and LIVE account reads. The ownership loop never instantiates the legacy
runner. Broker-authoritative balance, position and open-order snapshots are
persisted separately from the historical simulation ledger; existing canonical
position reconciliation is reused. Account-owner ambiguity, failed reads and
stale snapshots are reported explicitly. No virtual capital or fills are
created in this production path. Order submission remains blocked; enabling
the switch alone does not confer CATI risk or governance authority.

The independent submission switch guards the HTTP/SDK mutation boundaries,
including entry, close, protection, cancellation, leverage and transfers.
LIVE GET reads remain available. Real execution must continue to satisfy all
existing ownership, account, risk and governance controls. Strategy thresholds,
universe, timeframe, geometry, cost assumptions and registry hash are unchanged.

`GET /api/v1/cati/runtime/status` requires authentication and `bot:read`; it
returns only the requesting user's LIVE accounts and production snapshots.
Production health exposes the loaded profile and the production task state.
The UI source uses the CATI backend URL separately from the authentication API.
Historical simulation status remains excluded from production reporting.

An authenticated LIVE sync requires a validated LIVE broker account and its
credentials. Existing DEMO accounts keep their original identity and records.
The configuration audit is not evidence that a running service has adopted
the profile or that authenticated broker reads succeeded. This change was
prepared without deployment, restart, broker orders or holdout access.

Validation
----------

The selected bot-backend suite passed 348 tests. Subsequent production status
and read-only reporting changes passed the final 121-test subset (including
36 production configuration/transport/reporting tests). User-backend broker
and configuration tests passed 14 tests; frontend tests passed 14 tests.
TypeScript checking and CATI component lint passed. BrokerConnection's hook
issues were corrected; its 23 existing typing/unused-variable lint errors
remain outside this configuration change.

The local audit confirmed all backend environments and templates agree,
the public LIVE venue GET returns HTTP 200, runtime PID 19188 and its parent
22508 remain unchanged, and FX watcher PID 25328 remains active. No production
state table was created in the canonical database. The only configured broker
account remains DEMO, so authenticated LIVE synchronization is pending a valid
LIVE account and credentials. Existing running mode and historical records
were not relabeled as production execution.
