CATI production execution configuration
=======================================

The production application keeps CATI LIVE, live market data, the sole frozen
RESIDUAL_MOMENTUM_PORTFOLIO_TOP1 entry authority, risk, forward observation,
FX and reconciliation. Broker execution is ACCOUNT_SCOPED. Production does not
imply a real-money account, and DEMO execution is actual broker execution.
There is no PaperRunner, simulated fill, legacy entry or alternate strategy.

Account routing and gates
-------------------------

Connected/active accounts are discovered through execution_accounts(). The
canonical BrokerEnvironment normalizes demo/testnet/sandbox/paper/test to DEMO
and live/mainnet/production to LIVE. Authentication, account ownership, active
credential version, environment and endpoint come from the existing resolver
and shared client factory. A mismatched account/credential environment or
noncanonical endpoint fails closed. Account-scoped plans, catalogs, risk and
reconciliation preserve that same identity and environment.

Local operational configuration and backend templates stage:

    BROKER_ENVIRONMENT=ACCOUNT_SCOPED
    DEMO_ORDER_SUBMISSION_ENABLED=true
    LIVE_ORDER_SUBMISSION_ENABLED=false

The default DEMO flag in code is false; it must be explicitly enabled by the
operational configuration. Loaded process settings select the gate from the
resolved account environment. Changing files does not alter an existing
process. No service was deployed or restarted during this task.

Each mutation transport validates its own account environment and canonical
endpoint before selecting its gate. The DEMO flag cannot authorize LIVE and the
LIVE flag cannot authorize DEMO. Gates cover create, cancel, replace/amend,
protection, leverage and wallet mutations. GET reads remain possible. Entry
CREATE additionally requires the current CATI boundary permit and matching
durable account/user/risk intent; a contextless legacy client cannot enter.

Execution and certification
---------------------------

| Broker | Production DEMO execution contract | Real-money LIVE certification |
| --- | --- | --- |
| Binance USD-M | DEMO_VALIDATED by isolated contract/safety tests | CONTRACT_VALIDATED; LIVE entry remains blocked |
| Bybit | DEMO_CAPABILITY_UNAVAILABLE | UNVALIDATED |
| BingX | DEMO_CAPABILITY_UNAVAILABLE | UNVALIDATED |
| Other brokers | DEMO_ADAPTER_UNVALIDATED; no crypto fallback | No new certification |

Binance uses its existing market-order, durable client-ID query, fill and
income history, metadata and close-position stop/target APIs. DEMO_VALIDATED
means this environment's execution and recovery contract passes the tests; it
is not evidence of an actual testnet trade or real-money promotion. No manual,
synthetic or natural broker order was submitted during implementation.

Bybit and BingX may sync balances, positions, orders and instruments, but the
current production contract lacks complete daily account income history and
durable protective-outcome read-back for them. It refuses entry with the
missing capabilities instead of routing them through Binance-specific risk.
Other brokers retain their code and report unavailable execution capabilities.
No wallet-transfer endpoint is invented. Binance shared futures collateral
uses the existing logical allocation, reservation and capital ledger; an
unrelated unavailable DEMO wallet API does not authorize or prevent transfer
requirements from being enforced where a transfer is actually necessary.

The same hard risk applies to DEMO and LIVE: permanent 2.5% daily loss cap with
a durable latch, adaptive daily budget, weekly/monthly controls, kill switch,
loss streak/cooldown, minimum sizing, margin and all account-wide exposure.
Multiple mapped bots cannot each claim the whole account; ambiguous execution
ownership fails closed. Frozen geometry is rejected if invalid, never clamped.
Native close-position stop AND target support is required before entry.

Risk precedes a durable intent and deterministic client ID. Ambiguous CREATE
or interruption remains owned and is read back before any further action;
replays and restarts cannot issue a duplicate entry. Protective legs have
separate durable IDs, close-only semantics, geometry read-back and no blind
recreate after an unknown result. Broker orders, fills and positions determine
execution and PnL; observation outcomes never become fills.

Runtime/API/UI truth
--------------------

One existing canonical lease and runtime loop runs residual scheduling,
observation, FX, account discovery/sync, CATI eligibility, risk, gated execution
and reconciliation. Status exposes BROKER_EXECUTION_SCOPE=ACCOUNT_SCOPED and
both loaded gates, then each account's broker, environment, canonical URL,
gate, connection, reconciliation, execution permission and exact reasons.
Authentication limits status to the requesting user's accounts. DEMO balances,
positions and orders are identified as DEMO; they are never called real-money
LIVE execution. Stale successful snapshots block entry. Read failures preserve
the failure reason. The UI displays both gates and per-account state.

Local account verification (2026-10-05)
--------------------------------------

The canonical database contains one connected Binance DEMO account with one
active mapped bot. The updated discovery includes it and a production state
row was persisted. A GET-only sync attempt could not resolve its encrypted
credential: broker_decrypt_failed. Its state truthfully reports READ_FAILED /
BLOCKED_ACCOUNT with no balances, positions or orders asserted. Matching
credential-decryption configuration is an external prerequisite; credentials
were neither changed nor printed. After an operator restart, the new runtime
will discover this account and retry sync; it cannot execute until decryption
and all existing governance, permission, persisted risk and capital checks pass.

Validation
----------

The broad backend regression suite passed 1,464 tests. The final affected execution
subset passed 166 tests after native target-capability and status checks. The user/shared broker
suite passed 40 tests with isolated test settings, in-memory schema and mocked
permission probes. The frontend TypeScript/Vite build passed. Broker lifecycle
tests use isolated persistence and mocked transports, never real broker orders.
