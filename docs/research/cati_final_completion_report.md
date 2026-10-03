# CATI final completion and activation map

All automatically reachable code, bounded research and operational recovery work is complete. The economic gate blocks model/promotion work; runtime and FX continue. Work was performed directly on main.

```text
15m execution structure
fully closed 1h context
fully closed 4h context
trend quality
pullback depth
reclaim quality
volatility-adjusted structure

D. FLOW_CONFIRMED_DIRECTIONAL

ONLY where actual historical causal data exists.

Potential:
volume acceleration
open interest
funding
basis
trade-flow evidence

Unavailable data must be omitted.

Never fabricate neutral observations.

============================================================
SECTION 12 — TIMEFRAMES
============================================================

Use only actually supported historical resolutions.

Do not fabricate 5m from 15m.

If actual 5m is unavailable:
mark unavailable.

15m may be native.

1h/4h may be causally derived from complete lower-resolution groups where
valid.

Any HTF signal must use only fully closed HTF bars.

Never use partially formed bars.

============================================================
SECTION 13 — ECONOMICALLY SANE GEOMETRY
============================================================

The prior audits found major geometry/cost problems.

For new candidates use causal geometry incorporating:

structural invalidation
volatility floor
minimum executable distance
target distance supported by structure
realistic fee/spread/slippage/funding estimate
cost relative to structural R

Reject candidates whose geometry makes ordinary trading friction dominate R.

Do not merely lower modeled transaction costs.

============================================================
SECTION 14 — COST SEMANTICS
============================================================

Audit the Alpha/V5 research cost assumptions against what can actually be
supported historically.

Where evidence exists, version a more realistic cost model using:

actual venue fee schedule assumptions
historical spread evidence
historical slippage proxy
historical funding timestamps
actual event duration where labels support it

Do not mutate historical V1–V5 artifacts.

New cost semantics require a new version/hash.

If actual evidence is unavailable:
retain a disclosed conservative assumption.

Never choose cheaper costs because they improve results.

============================================================
SECTION 15 — ECONOMIC GATE BEFORE MODEL TRAINING
============================================================

For every new generator hypothesis compute BEFORE fitting another CATI model:

candidate count
calendar coverage
gross R
cost R
net R
TARGET rate
STOP rate
TIMEOUT rate
profitable rate
2x cost stress
chronological fold results

No ML/model admission yet.

A generator must demonstrate genuinely viable opportunity economics first.

Do not confuse ranking skill with economic alpha.

============================================================
SECTION 16 — ALPHA ADMISSION
============================================================

Use a predeclared economic viability policy.

At minimum preserve the existing strong principles:

adequate samples
adequate calendar coverage
positive gross economics
credible positive net economics
temporal robustness
cost stress

Do not invent easier thresholds after looking at results.

Report separately:

RESEARCH_PROMISING
ECONOMIC_VIABILITY_PASS

If every bounded hypothesis FAILS:

STOP model training.

Continue runtime/FX/operational work.

Return the economic blocker.

Do NOT automatically create Alpha V3/V4/V5 in an endless adaptive loop.

============================================================
SECTION 17 — ONLY IF ALPHA ECONOMIC GATE PASSES
============================================================

If and ONLY if a new generator passes its registered economic gate:

build the next CATI forecast candidate.

Preserve V5's coherent principles:

single coherent joint outcome distribution
p(net profitable) derived from joint states
terminal probabilities from same distribution
expected net R from same distribution
conditional payoff
MFE / MAE
event timing

No inconsistent independent marginals.

============================================================
SECTION 18 — MODEL DEVELOPMENT
============================================================

Use causal chronological nested development.

All:

transforms
feature selection
hyperparameters
calibration
regularization

must be learned inside training prefixes.

The existing development range has been repeatedly inspected.

Record:

DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED = YES

Do not describe it as untouched confirmation.

============================================================
SECTION 19 — CALIBRATION GATES
============================================================

Do not alter the frozen requirements.

At minimum preserve:

Brier skill >= 0.02
ECE <= 0.05
minimum required sample count

and the complete frozen Section 22 policy.

Report:

Brier
causal baseline Brier
Brier skill
ECE
ROC-AUC
PR-AUC
log loss
ranking
expected-R calibration
fold stability

Do not promote merely from pooled score if temporal robustness fails.

============================================================
SECTION 20 — SECTION 22 GOVERNANCE
============================================================

Frozen governance requirements stay unchanged:

P(expectancy > 0) >= 0.95

2x cost-stress floor >= -0.10 R

maximum holdout drawdown <= 10 R

maximum positive concentration <= 0.50

minimum accepted evidence >= 100

PBO <= 0.25

neighbor positive share >= 0.75

forward demo:
>= 30 executed CATI trades
AND
>= 30 calendar days

No threshold may be retuned after holdout.

============================================================
SECTION 21 — LIBRARY / CERTIFICATION
============================================================

If the candidate legitimately passes development/calibration:

build the candidate library through the existing bounded-memory pipeline.

Verify:

content hash
dataset hash
universe
policy
code revision
holdout cutoff
clean provenance
row identity
deterministic reload
bounded memory

Then run the existing certification pipeline.

Do not invent another library architecture.

============================================================
SECTION 22 — RUNTIME PIN
============================================================

Only after a legitimately eligible candidate passes all pre-runtime
requirements:

pin the exact eligible CATI library/artifact.

No "latest" alias.

Pin exact identity/hash.

Runtime must fail closed on:

missing library
wrong hash
wrong dataset
wrong policy
dirty provenance
RESEARCH_ONLY
failed calibration
unknown version

============================================================
SECTION 23 — RUNTIME RANKING / EPOCH PROOF
============================================================

If an eligible artifact is pinned:

prove CATI runtime ranking against current markets.

Then complete the required full runtime epoch evidence.

Do not count research replay as runtime epochs.

CATI remains blocked from external orders until its governance phase allows
them.

============================================================
SECTION 24 — FX FINALIZATION IN PARALLEL
============================================================

If FX acquisition completes while other CATI work runs:

automatically continue:

derive 5m
derive 15m
derive 4h
QA
scale validation
gap classification
freeze

If any governed validation fails:
do not freeze.

Report exact failure.

============================================================
SECTION 25 — BROKER VALIDATION
============================================================

When pre-holdout/runtime prerequisites permit, inspect user-connected demo
accounts through the normal account architecture.

Target supported demo venues include:

Bybit Demo:
https://api-demo.bybit.com

BingX VST:
https://open-api-vst.bingx.com

Do NOT:

use centrally owned credentials
hard-code broker keys
require withdrawal permission

Validate where an authenticated user account is available:

authentication
permissions
account mode
balances
positions
instrument capabilities
order create
order lookup
cancel
protection
reconciliation
unknown-outcome read-back

If no authenticated user demo account exists:

record:

WAITING_FOR_USER_BROKER_CONNECTION

Do not fabricate validation.

Continue any other work that does not depend on it.

============================================================
SECTION 26 — PRE-HOLDOUT CHECKPOINT
============================================================

Only when ALL required pre-holdout technical/data/model/runtime/external
conditions are satisfied:

produce PRE_HOLDOUT_READY.

THEN STOP.

Do not open the reserved holdout automatically.

The user must explicitly authorize holdout access.

============================================================
SECTION 27 — HOLDOUT
============================================================

DO NOT execute this Section without explicit user authorization.

When/if authorized later:

open the registered holdout once
evaluate the frozen candidate once
do not retune afterward

If fail:
candidate rejected.

If pass:
continue according to frozen governance.

============================================================
SECTION 28 — DEMO
============================================================

After the required holdout/governance phase and explicit demo authorization:

CATI only
no legacy engine

Run CATI on authenticated user demo broker accounts.

Requirement remains:

>=30 executed CATI trades
AND
>=30 calendar days

Collect:

orders
fills
positions
protection
reconciliation
slippage
fees
funding
PnL
risk events

No production inference before completion.

============================================================
SECTION 29 — LIVE
============================================================

DO NOT activate production merely because previous Sections pass.

Production requires explicit production approval.

When eventually approved:

CATI is the ONLY intelligence authority.

Required production chain:

CATI
→ hard risk
→ user live broker
→ execution
→ protection
→ reconciliation

No V2.
No fallback.

============================================================
SECTION 30 — RISK
============================================================

Preserve permanently:

2.5% DAILY HARD LOSS CAP

CATI can never override hard risk.

Preserve:

per-trade risk
account exposure
cross-bot/account aggregation
symbol exposure
margin validation
position limits
stop validation
kill switch
duplicate-order protection
unknown-outcome reconciliation

============================================================
SECTION 31 — AUTO TRADING VS CAPITAL ROUTING
============================================================

Keep separate:

AUTO TRADING
AUTO CAPITAL ROUTING

Enabling one must never automatically enable the other.

============================================================
SECTION 32 — FRONTEND/API
============================================================

Audit and fix wiring so the product presents CATI as the sole trading engine.

Expose:

CATI status
CATI generator
CATI model/library
governance phase
observe/demo/live state
broker accounts
market state
positions
orders
trade history
PnL
risk state
capital-routing state

Remove active user-facing legacy engine selection.

Historical records may continue displaying historical engine metadata.

============================================================
SECTION 33 — TEST MATRIX
============================================================

Run the full existing CATI suite plus regression coverage for all changes.

Must prove:

legacy runtime V2 unreachable
legacy V2 cannot receive authority
legacy V2 cannot create orders
no CATI→V2 fallback
CATI runtime works while authority blocked
CATI blocked means no entry
CATI shadow cannot submit order
hard risk remains superior
2.5% cap unchanged
broker-neutral behavior
user-account ownership
reconciliation
unknown-outcome handling
FX single-writer
holdout exclusion
dataset identity
candidate identity
causal timestamps
deterministic replay
runtime artifact rejection
dirty provenance rejection

Do not only run new tests.

============================================================
SECTION 34 — GIT
============================================================

Work directly on main.

NO NEW BRANCH.

Commit coherent completed work.

Push main.

Do not create empty commits.

Do not leave important completed work only locally.

============================================================
FINAL RETURN FORMAT
============================================================

Return ONE consolidated report only after all automatically reachable work is
complete.

Do not return after every Section.

Use:

MAIN_START_COMMIT = b8724a54aea91c4ac429cf9d95fc8534751f7378
MAIN_END_COMMIT = delivery commit; exact hash in final response (git log -1 --format=%H -- docs/research/cati_final_completion_report.md)
REMOTE_MAIN_PUSHED = final delivery verifies origin/main equality
WORKTREE_CLEAN = final delivery verifies YES after canonical restart

====================
ARCHITECTURE
====================

CATI_SOLE_TRADING_ENGINE = YES
LEGACY_V2_RUNTIME_REACHABLE = NO
LEGACY_V2_ENTRY_AUTHORITY = NONE
LEGACY_V2_FALLBACK = NO
LEGACY_V2_HISTORICAL_REFERENCES_REMAINING = YES; classified inventory and semantic runtime audit

====================
RUNTIME
====================

RUNTIME_STATUS = RUNNING; final restart PID/lease verified in delivery
RUNTIME_PID = 31824 at evidence snapshot; final PID in delivery
RUNTIME_SINGLE_OWNER = YES; one session, one lease, venv shim is not another owner
CATI_RUNTIME_ACTIVE = YES
CATI_OBSERVE_MODE = YES
CATI_ENTRY_AUTHORITY = BLOCKED

MARKET_DATA = ACTIVE; fresh closed 15m snapshots, aligned closed HTF
SCHEDULER = ACTIVE; 2 maintenance jobs, no legacy generation/training
CALENDAR = ACTIVE; sync and event-ingestion workers; provider freshness remains separately governed
RISK_ENGINE = ACTIVE; permanent daily cap <=2.5%; existing hard-risk/sizing stack retained
BROKER_DISCOVERY = ACTIVE; 741 Binance instruments discovered
PORTFOLIO_RECONCILIATION = ACTIVE safety path; zero open managed positions observed; existing exposure/reconciliation regressions pass

====================
FX
====================

FX_STATUS = ACQUISITION_PROGRESSING; recovered after initial Dukascopy HTTP503
FX_WRITER_COUNT = 1 logical writer (venv shim + Python child)
FX_REMAINING_PERIODS = 2871 at 2026-10-03T09:03:31.524859+00:00
FX_PROGRESSING = YES; 3103 -> 2871
FX_DERIVATION = QUEUED after acquisition; existing completion process
FX_QA = QUEUED
FX_GAP_CLASSIFICATION = QUEUED; UNKNOWN_GAP remains strict
FX_FROZEN = NO

====================
ALPHA
====================

ALPHA_V1_STATUS = REJECTED; immutable; gross -0.040923573 R / net -0.138137635 R REJECTED
NEXT_ALPHA_GENERATOR_ID = CATI_ALPHA_CAUSAL_DIVERSIFIED_V2
ALPHA_RESEARCH_BUDGET = 4 mechanisms; 1 fixed run; no retuning or next-alpha loop
ALPHA_UNIVERSE_SIZE = 136/136 native-15m source-supported assets; unavailable 5m/OI/funding/basis/signed flow/spread/slippage explicitly omitted

ALPHA_CANDIDATES = A=20407; B=12; C=3523; D=16550
ALPHA_GROSS_R = A=-0.032447307; B=-0.346722256; C=0.050511595; D=-0.009168694
ALPHA_COST_R = A=0.082631546; B=0.049053819; C=0.102384193; D=0.076995863
ALPHA_NET_R = A=-0.115078853; B=-0.395776075; C=-0.051872598; D=-0.086164556

ALPHA_FOLD_RESULTS = 5 chronological folds per mechanism; full counts/days/rates/clustered intervals/2x stress in cati_alpha_v2_fixed_report.md and immutable JSON

RESEARCH_PROMISING = NO for A/B/C/D
ECONOMIC_VIABILITY_PASS = NO for A/B/C/D

====================
CATI MODEL
====================

MODEL_TRAINING_PERFORMED = NO; economic gate failed
CATI_CANDIDATE_ID = NONE
MODEL_ARCHITECTURE = N/A; no fitted candidate

BRIER = N/A
CAUSAL_BASELINE_BRIER = N/A N/A
BRIER_SKILL = N/A
ECE = N/A
ROC_AUC = N/A
PR_AUC = N/A
LOG_LOSS = N/A

PAYOFF_VALIDATION = FAILED upstream opportunity economics
TEMPORAL_ROBUSTNESS = FAILED opportunity gate; model robustness N/A
MODEL_READY = NO

====================
CERTIFICATION
====================

LIBRARY_BUILT = NO
LIBRARY_ID = NONE
LIBRARY_HASH = NONE
CERTIFICATION = NOT_RUN; upstream gate failed
RUNTIME_PINNED = NO
RUNTIME_RANKING_PROVED = NO; missing eligible library
FULL_EPOCHS_COMPLETED = 0 eligible validation epochs; incomplete observation epoch explicitly rejected

====================
BROKERS
====================

BYBIT_DEMO = WAITING_FOR_USER_BROKER_CONNECTION
BINGX_VST = WAITING_FOR_USER_BROKER_CONNECTION
AUTHENTICATED_DEMO_VALIDATION = NOT_RUN
BROKER_VALIDATION_BLOCKER = No Bybit/BingX user connections; economic/model/governance prerequisites failed. Existing Binance demo metadata is connected; no new orders validated.

====================
GOVERNANCE
====================

CURRENT_GOVERNANCE_PHASE = M0
CATI_EXECUTION = BLOCKED
HOLDOUT_OPENED = NO
HOLDOUT_INSPECTED = NO
HOLDOUT_QUERY_COUNT = 0

PRE_HOLDOUT_READY = NO
WAITING_FOR_EXPLICIT_HOLDOUT_AUTHORIZATION = NO; prerequisites have not passed; eventual holdout still requires explicit authorization

FORWARD_DEMO_ELIGIBLE = NO
FORWARD_DEMO_TRADES = 0
FORWARD_DEMO_DAYS = 0

PRODUCTION_ELIGIBLE = NO
CATI_LIVE = NO

====================
TESTS
====================

TESTS = 1213 full CATI; 443 broader runtime/risk/broker/regressions; 85 final cutover; 60 final observation/reason regressions; 86 product surface; 16 user/security; frontend production build PASS; offline 40492 labels PASS. Groups overlap.
PEAK_RAM = 254.1 MB sampled alpha evaluator; runtime observation epoch sampled 263.3 MB; not system-wide peaks
OPERATIONAL_DEFECTS_FIXED = Legacy authority/fallback/selection/injection/intents/schedulers; executor bypass; API/UI controls and allocation proxy; 10% legacy loss ceiling; env daily-cap relaxation; PID reuse session accounting; missing CATI logs/canonical observation reason

====================
FINAL BLOCKER
====================

FIRST_BLOCKER = All four fixed alpha mechanisms fail economic viability: every pooled net R and 2x cost return is negative
REMAINING_BLOCKERS = FX acquisition/strict finalization incomplete; no eligible forecast/library/certification/pin/ranking epochs; missing Bybit/BingX user demo connections; eventual frozen holdout/demo/production authorization
NEXT_REQUIRED_USER_ACTION = None for running observation or existing FX pipeline. Advancing research needs a separately authorized bounded new hypothesis/data budget; broker connections and explicit holdout/demo/live approvals are required only when their prerequisites pass.
```

A/B/C/D are relative strength, volatility transition, multi-timeframe pullback and volume-confirmed directional flow. No union or profitable subset was promoted. Development was repeatedly inspected (`DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED=YES`). All labels are hypothetical overlapping opportunities, not portfolio PnL or broker fills.

Costs retain the conservative inherited rates under a new version/hash. Actual historical spread, slippage, fee-tier and funding observations are unavailable, so they were not fabricated or made cheaper. Native 5m was unavailable; 1h/4h use only complete closed 15m groups. V1–V5 artifacts and frozen Section 22 thresholds remain unchanged. The evaluator ran from a dirty research tree, recorded exact source hashes, and remains RESEARCH_ONLY even though its code/evidence is now committed.

Evidence: [runtime authority audit](../architecture/CATI_SOLE_RUNTIME_AUDIT.md), [product surface audit](../architecture/CATI_SOLE_AUTHORITY_SURFACE_AUDIT.md), [reference inventory](../architecture/CATI_SOLE_RUNTIME_REFERENCE_INVENTORY.json), [alpha report](cati_alpha_v2_fixed_report.md), [complete economics](artifacts/cati_alpha_v2_fixed/report.json), [offline label verification](artifacts/cati_alpha_v2_fixed/offline_verification.json), [operational snapshot](cati_final_operational_evidence.json), [FX recovery](cati_fx_recovery_result.md).

The operational snapshot predates the final documentation commit/restart and records that provenance honestly. The final delivery confirms the final clean revision, runtime PID/lease heartbeat and origin/main. No holdout, authenticated demo order validation, governed forward-demo trades, or live activation was performed.
