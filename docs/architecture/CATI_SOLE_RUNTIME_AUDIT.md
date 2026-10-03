# CATI sole runtime authority audit — 2026-10-03

The production intelligence path is CATI only. M0 runs observation with new
entries BLOCKED. All phase/scope/health combinations resolve to CATI or NONE;
none resolve to a legacy engine. CATI denial closes the entry gate.

## Semantic reachability review

| Entry point / edge inspected | Previous classification | Final disposition |
|---|---|---|
| `governance/runtime_authority.py`, `phases.py`, `promotion.py` | RUNTIME_ACTIVE | Removed every V2 authority/fallback return, including M0, demo, unpromoted live scopes, unhealthy CATI and unknown environment. Historical helper names return false. |
| `strategy/loader.py`, package import registration, both registries | RUNTIME_REACHABLE | Loader always binds CATI metadata, first registry exposes only CATI, second registry cannot retrieve/list historical implementations and no longer auto-instantiates them. |
| `core/trading_orchestrator.py` dependency injection and scalar opportunity processing | RUNTIME_REACHABLE | Injected historical strategies cannot be adapted into runtime authority. Scalar interface returns BLOCKED before analysis or intent construction. CATI `process_trade_plan` retains the existing hard-risk stack. |
| `runner/runner.py` main cycle, contextless path and orchestrated entry path | RUNTIME_ACTIVE | Physically removed scalar strategy analysis, confidence/ML admission, intent creation, legacy dispatch and contextless SMA branch. Position reconciliation, protection, lifecycle management and CLOSE remain. Fresh snapshots go to the existing CATI cycle controller. |
| `runner/runner.py` ML and dynamic/legacy shadow construction | RUNTIME_ACTIVE | No legacy scorer load or strategy construction; removed legacy shadow capture hook. CATI cycle shadow is the sole observation pipeline. |
| `main.py` scheduler registration | RUNTIME_ACTIVE | Removed old signal generation, organic legacy dataset and monthly legacy model training. Only signal expiry and read-only daily validation maintenance jobs remain, behind runtime ownership. Calendar/news workers remain. |
| `multi_runner.py`, TradingView and external signal processor | RUNTIME_REACHABLE | Legacy processors disabled unconditionally. Webhook stores advisory information only; external scalar candidate processing always denies entry. |
| `signals/crypto_signal_engine.py` class and convenience wrapper | RUNTIME_REACHABLE | Both deny before scanning, network work or publication. Historical indicator/geometry helpers remain reproducible. |
| `execution/executor.py` public and internal wrapper entry | RUNTIME_REACHABLE | Non-CLOSE calls require a call-scoped CATI boundary permit matching plan identity, symbol, direction and notional. Forged identity strings/flags cannot grant it. Reset occurs even for unknown broker results. |
| `execution/boundary.py` and `integration/cati_dispatch.py` | Retained CATI production chain | Governance → immutable TradePlan validation → existing hard risk → permit-scoped venue submission → protection/reconciliation. Missing or ineligible artifact and unsupported economic adapter fail closed. No alternate alpha path. |
| User/backend/frontend controls and backtest creation | RUNTIME_REACHABLE | CATI-only creation/catalog/status, owned-account scoped authority, legacy simulation creation blocked before writes. Historical views remain. See the separate surface audit. |
| Low-level broker adapter order methods | Retained execution capability | No API directly submits new entries. CATI boundary is the runtime caller. CLOSE, protection and recovery remain executable; these are position safety, not a second intelligence authority. Explicit demo-validation tooling remains authorization gated. |

The audit searched authority owners, loaders, registries, dependency injection,
schedulers, scalar signals, intent construction, broker callers, configuration,
APIs and UI, rather than treating the word “V2” as a sufficient test.

## Remaining references

`CATI_SOLE_RUNTIME_REFERENCE_INVENTORY.json` records every matching tracked-source
reference in the audited backend, frontend, tests, scripts and documentation,
with path/line/classification. It is a lexical inventory supporting the semantic
edge review above, not an automatic proof of reachability. New CATI alpha V2 and
unrelated version-2 contracts are distinct from the retired legacy engine.

- RUNTIME_ACTIVE legacy intelligence: none after cutover.
- RUNTIME_REACHABLE legacy intelligence: none through production entry points.
- TEST_ONLY: historical strategy mechanics/replay tests and compatibility labels.
- MIGRATION_ONLY: immutable database schema/data compatibility.
- HISTORICAL_DATA_ONLY: old strategy IDs, run/order/fill labels, immutable research
  reports and isolated offline replay/certification helpers. They grant no entry.
- DEAD_CODE: importable historical strategy implementations, legacy ML/scalar
  helpers and disabled debug/backtest code. Runtime does not construct them;
  registries and execution entry gate prevent their admission.
- DOCUMENTATION_ONLY: prior architecture and incident history. Superseded runtime
  authority statements in old documents are historical, not active policy.

No frozen V1 artifact or Section 22 requirement was changed. Research rejection
does not disable market data or position management. Model training, library
creation, certification, runtime pin, holdout and governed demo/live are blocked
by the failed predeclared alpha economics, not bypassed by this cutover.

## Risk and operational defects

The retained configuration container still had a 10% daily-loss ceiling. It now
clamps at 2.5%, including injected custom limits. The runtime adaptive budget
also clamps the environment setting at 2.5%; tighter settings remain allowed.
Existing broker/account exposure, sizing, stops, margin, kill switches,
reservations, idempotency and unknown-result handling remain in the hard-risk
and execution stack.

Runtime session reaping now proves PID reuse from process creation time versus
the recorded session start. It marks the old row ABANDONED without inventing a
stop timestamp. The FX completion process had reused an old runtime PID; this
caused the misleading second RUNNING session, not a second runtime owner.

CATI INFO telemetry is enabled by default, including per-symbol causal analysis
and rejection logs. Observation explicitly records execution_attempted=False.
Neither research replay nor these observations count as trades or eligible
ranking epochs. Auto Trading and Auto Capital Routing remain independent.

## Verification

- Full existing CATI suite: 1,213 passed (8 existing sklearn feature-name warnings).
- Existing runtime/risk/broker/protection/reconciliation regressions plus sole
  runtime invariants: 443 passed.
- Final modified CATI cycle/boundary/surface/sole-runtime checks: 85 passed.
- Product surface regressions: 86 passed; user/security regressions: 16 passed.
- Frontend TypeScript build passed; React review found no introduced hook-order,
  dependent fetch waterfall, ownership, or accessibility regression.
- Compressed alpha evidence: all 40,492 candidate/label identities verified
  offline; zero holdout queries and zero future HTF timestamps.

Actual process/component and FX progress evidence is recorded separately in
`docs/research/cati_final_operational_evidence.json`. External authenticated demo
order validation was not performed: Bybit/BingX user demo connections are absent
and the economic/model/governance prerequisites have not passed.
