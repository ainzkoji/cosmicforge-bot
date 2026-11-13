# Strategy, Research & Certification Audit (Sections 6 and 28)

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · **Date:** 2026-11-13
**Baseline:** `origin/main` = `649080b` · engineering branch `origin/ccr-b5c520cf-80g8x2` = `732e0e2` (7 commits not on main, inspected by diff/worktree only)
**Master report:** [TRADINGBOT_MASTER_END_TO_END_AUDIT.md](TRADINGBOT_MASTER_END_TO_END_AUDIT.md) · **Gap register:** [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv)

Evidence levels used throughout: L1 observed authenticated behaviour · L2 real venue evidence · L3 integration test with real boundaries ·
L4 contract/unit test with mocks · L5 static source inspection · L6 documentation/historical claim · L7 inference.
No production database, VPS or authenticated venue session was available to this audit; nothing below is L1.
Finding IDs are stable and map to the gap register (column "Master ID" consolidates duplicates across areas).

## Lead auditor reconciliation
- **Correction to the audit brief and to the branch certification draft:** not every family is FAILED. RESIDUAL_MOMENTUM_PORTFOLIO_TOP1 (H-015) is FAILED; DAILY_TREND (H-018, Mandate 004) is REGISTERED / NOT_CERTIFIED / PENDING_HOLDOUT (holdout never opened); the four 15m setup specialists are NOT_CERTIFIED (no hypothesis). All six are at governance stage M0. `TRADINGBOT_FINAL_SYSTEM_CERTIFICATION.md` (branch, l.32/l.235) is inaccurate on this point (RES-06).
- The register hash chain was verified by the auditor's own run (27 records, 18 hypotheses, anchor checked); 75 targeted research/governance tests passed (L4).
- Autonomous strategy discovery is **not implemented** beyond callable libraries and CLIs; see the 18-capability table below and Decision 7 in the master report.

---
## RES — Strategies, research, certification and governance (audit sections 6 and 28)

Baseline: origin/main 649080b. Branch ccr-b5c520cf-80g8x2 touches no research/governance code (only adds
docs/implementation/TRADINGBOT_FINAL_SYSTEM_CERTIFICATION.md, which makes research claims checked below).
Paths below are relative to backends/bot-backend/app unless they start with docs/, research/, scripts/ or deploy/.

### 1. Scope and method

- Static reading of the family registry, family governance, experimental DEMO, the research register, the
  Mandate 004 evaluator, Section 22 certification, the legacy app/strategy tree, the production runtime entry
  points (main.py, integration/production_runtime.py, integration/platform_runtime.py) and deploy/systemd.
- Authoritative records read directly: docs/research/registry/research_register.jsonl (27 records, hash
  chained), docs/research/registry/historical_hypotheses_v1.json,
  docs/research/mandate_004/certification_result.json and runs/run_0b88d389f8da50dc48de5e51/.
- Executed (L4/L3, my own runs):
  - `python -m app.trading_intelligence.research.governance verify`: chain intact, 27 records, 18 hypotheses,
    head d35c4a31…, anchor checked.
  - FamilyRegistry built on a fresh SQLite DB with ensure_cati_platform_schema + register_all, reading the
    committed register. Output reproduced in section 4.
  - pytest test_experimental_demo.py, test_step2_research_governance.py, test_step2_mandate_004.py,
    test_step3_family_registry.py: **75 passed**.
- Not inspected: the owner's runtime database. That covers the live family governance log, the
  experimental-DEMO ledger and the L-08 ledger. Their state is inferred from code and documents (L6/L7).

### 2. Strategy inventory

The platform has **two unrelated strategy inventories**:

- **The governed CATI family registry.** pipeline/family_registry.py:181-192 `definitions()` builds it from
  code: DAILY_TREND, RESIDUAL_MOMENTUM_PORTFOLIO_TOP1, and the four setup specialists from
  setups/registry.py SPECIALIST_REGISTRY. It holds 6 families in total.
- **The legacy "V2" app/strategy experts.** No production code can reach them.
  - strategy/registry.py:47-58: `list_strategies()` returns only "cati", and `get_strategy_class` returns
    CatiRuntimeBinding for "cati" and None for anything else.
  - strategy/loader.py:24-31: `build_strategy` always returns CatiRuntimeBinding ("Stored strategy names are
    historical metadata, never engine selection").
  - governance/destination_map.py:107-121 lists them as LEGACY_AFTER_PROMOTION.

| Family / strategy | Implementation | Hypothesis | Markets / TF | Entry / exit | Inputs | Registry status (live computation, L4) | Research verdict (register) | Gov. stage | Execution integration | Production use | Trading eligibility evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **RESIDUAL_MOMENTUM_PORTFOLIO_TOP1** (Residual Momentum, H-015, Mandate 003) | integration/residual_prospective.py (FAMILY l.23, frozen registry hash); integration/production_execution.py:445-475 builds TradePlans | 24h BTC/ETH-residual momentum continues | Binance USD-M USDT perps (~136 frozen universe) / 1h | Largest \|z\|≥2, long or short, one position; fixed stop, 2.5R target, 48h timeout | Hourly (15m native) bars of the universe plus BTC and ETH | FAILED; modes (RESEARCH_SIMULATION) only; blockers FAMILY_FAILED_RESEARCH, NO_PLATFORM_PIPELINE_ADAPTER, FAMILY_STAGE_M0_NO_ORDER_AUTHORITY | **FAILED**: ECONOMIC_GATE FOLD_1_GROSS_NOT_POSITIVE; FOLD_n day-clustered LCB not positive for folds 1-5; 326 trades gross +0.0878R, net +0.0728R, 2x cost +0.0577R (register rec. 15) | M0 | Yes: production_runtime.py:895 schedules hourly decisions → production_execution plan → boundary → FamilyExecutionGate refuses unless an experimental-DEMO grant applies (pipeline/provenance.py:281-293) | Running hourly in the production runtime. Decisions are recorded; orders are refused | None, except experimental grant xdemo_a2e102e911db4144b29b (72h from 2026-11-01, expired by its terms). No strategy order was ever placed (activation report §6) |
| **DAILY_TREND** (H-018, Mandate 004) | families/daily_trend/{spec,rules,targets}.py; pipeline/daily_trend.py (adapter); research/evaluator/* | Long-or-flat breakout ensemble captures crypto trend | 20 most liquid Binance USD-M USDT perps / 1d | 7 breakout lookbacks (10-150d) give a strength 0-1; next-open entry; 3×ATR(20) ratcheting stop (5-40%); exit on signal 0, stop, data gap or drawdown brake; no target | Daily OHLC, quote volume, funding; 150 bars of history | **NOT_CERTIFIED** (research_status REGISTERED); modes RESEARCH_SIMULATION + SHADOW; mandate_verified True, valid_run True, admission NOT_ELIGIBLE | **Not FAILED. PENDING_HOLDOUT** (rec. 26; certification_result.json verdict PENDING_HOLDOUT, statistical_gate BLOCKED_PENDING_APPROVAL) | M0 | Shadow only: platform_runtime.ShadowScheduler (production_runtime.py:822-824). The portfolio execution tail runs only if DEMO is in allowed_modes (portfolio_runtime.py:237-245), which needs CERTIFIED + M6 | Shadow books plus model accounts, daily | None |
| TREND_PULLBACK_V2 | setups/trend_pullback.py | none registered | Binance perps / 15m | SetupCandidate → forecast → TradePlan | 15m snapshot, market state, regime | NOT_CERTIFIED (NO_REGISTERED_HYPOTHESIS), no adapter | Never given its own verdict. Diagnostic net −0.4849R. The forecast models built on the specialists (H-001..H-004) were REJECTED_PRE_HOLDOUT | M0 | None (no adapter) | None | None |
| BREAKOUT_VOL_EXPANSION_V2 | setups/breakout_expansion.py | none | same | same | same | NOT_CERTIFIED | diagnostic −0.0858R | M0 | None | None | None |
| RANGE_MEAN_REVERSION_V2 | setups/range_mean_reversion.py | none | same | same | same | NOT_CERTIFIED | diagnostic −0.2212R | M0 | None | None | None |
| MOMENTUM_CONTINUATION_V1 | setups/momentum_continuation.py | none | same | same | same | NOT_CERTIFIED | diagnostic −0.0567R | M0 | None | None | None |
| H-005..H-010 Confirmed structure (breakout acceptance and trend reclaim at 5m/15m/1h) | scripts/evaluate_cati_alpha.py (research script only) | see register | Binance perps, 8-symbol panel | see register | 5m/15m/1h bars | not in family registry | FAILED (H-005/006 DATA_UNAVAILABLE; H-007..010 net −0.063 to −0.151R) | M0 | none | none | none |
| H-011..H-014 Causal diversified (cross-sectional RS, volatility transition, MTF trend pullback, flow-confirmed) | scripts/evaluate_cati_alpha_v2.py | — | 136 perps / 15m | — | — | not in registry | FAILED (net −0.052 to −0.396R) | M0 | none | none | none |
| H-016 dispersion-break RV; H-017 funding/basis carry | scripts/evaluate_cati_edge003.py | — | perps / 1h | — | — | not in registry | FAILED (H-016 net −0.1195R; H-017 net −0.0421R; runtime_capability_blocked) | M0 | none | none | none |
| Master Ensemble, Robust Ensemble, Trend Pullback (V2), Donchian Breakout, Bollinger Reversion, VWAP Reversion, Supertrend, Squeeze Breakout, SMA Cross, Hold Breakdown | strategy/*.py | none | CRYPTO/FOREX (decorator metadata) | Indicator rules (per file) | OHLCV | Not in the governed registry; only callers are scripts/validation/* and legacy runner metadata | No hypothesis, no verdict. scripts/build_master_ensemble_baseline.py only measures historical decision funnels | n/a | **None.** Loader and registry always return CatiRuntimeBinding | None (dead code kept until M9) | None |
| IOFS gate / components | strategy/iofs_gate.py, strategy/iofs_components/ | none | — | gate | MTF candles | not registered | none | n/a | No production caller. Only the settings value is echoed (main.py:2993, policy/effective_policy.py:366; config.py:417 default "shadow") | None | None |
| trend_v2_tradfi | research/trend_v2_tradfi/SPEC.md | DRAFT, not frozen, not registered | TradFi / 1d | draft | none | not in registry | none | — | none | none | none |

Correction to the brief's premise: **not every family is FAILED/M0.**
- Every family is **M0**.
- Only RESIDUAL is FAILED.
- DAILY_TREND is **REGISTERED / NOT_CERTIFIED / PENDING_HOLDOUT**. The development run did not fail the frozen
  pass rule: criterion 2 PASS, criteria 1, 3 and 4 PENDING_HOLDOUT.
- The four specialists are NOT_CERTIFIED. They have no hypothesis.

The branch document TRADINGBOT_FINAL_SYSTEM_CERTIFICATION.md (l.32 and l.235: "Every family is research
FAILED/M0") is inaccurate. docs/implementation/STEP2_RESEARCH_CERTIFICATION_STATUS.md ("not certified, not
failed") is accurate.

### 3. Autonomous research capabilities

There is no systemd unit, timer or in-process scheduler for any research stage. deploy/systemd contains only
trading, user, admin, backups, healthcheck, ops-tick and deadman units, and a grep finds nothing research- or
certification-related. The production runtime (production_runtime.py:809-898) schedules residual collection,
forward public observations, the ShadowScheduler and the VenueRuntime, nothing else.

| # | Capability | Classification | Evidence | Started by a scheduler? |
|---|---|---|---|---|
| 1 | Detect opportunities | **Absent** | No module searches for new edges. Residual scanning is a frozen strategy, not discovery | no |
| 2 | Generate hypothesis | **Absent** | Hypotheses are written by humans or agents (research/trend_v1/SPEC.md, research/trend_v2_tradfi DRAFT) | no |
| 3 | Define testable strategy | **Manual code** | families/daily_trend/spec.py + rules.py; each family is hand-coded and added to family_registry.definitions() (l.181-192) | no |
| 4 | Register experiment | **Callable library** (per-family module) | research/governance/mandates.py:54 `register_hypothesis_and_mandate`, l.82 `amend_mandate`; families/daily_trend/registration.py. The governance CLI (`research/governance/__main__.py`) has verify/status/import-history/record-decision/authorize-holdout but **no register-hypothesis command** | no |
| 5 | Select / freeze data | **CLI tool** | `python -m app.market_data.daily_dataset build`; `research/evaluator freeze-dataset` (Mandate 004 only); research/certification/freeze.py; scripts/freeze_research_dataset.py | no |
| 6 | Cost evaluation | **CLI tool** | research/evaluator `register-costs`; `research/costs collect\|report` (costs/__main__.py:48,161). Calibration INSUFFICIENT_DATA | no |
| 7 | Backtest | **CLI tool, family-specific** | research/evaluator/__main__.py:134 `run` (imports families.daily_trend.spec, l.25: Mandate 004 only); research/certification/cli.py:144 `run` (Section 22 forecast line); scripts/evaluate_cati_*.py (one bespoke script per mandate). app/backtest/worker.py runs API-submitted jobs, but build_strategy always yields CatiRuntimeBinding: not a research evaluator | BacktestWorker loop starts in main.py:906, but only for user-submitted jobs |
| 8 | Walk-forward | **Callable library / CLI** | research/certification/splits.py; per-mandate fold logic in scripts/evaluate_cati_edge003.py | no |
| 9 | OOS / holdout | **Callable library + owner CLI** | research/governance/holdout.py (reserve, record_owner_holdout_authorization, open_holdout_once, burn); evaluator/official.py:107-110 HoldoutEmbargo | no (by design, owner-only) |
| 10 | Multiple-testing correction | **Callable library** | research/governance/statistics.py (Bonferroni over register count, block bootstrap, power). Thresholds `None` (l.54-58), so the result is always BLOCKED_PENDING_APPROVAL | no |
| 11 | Reject | **Manual job** | HYPOTHESIS_STATUS / RUN_COMPLETED records appended by the evaluator run or by hand (register.py:44,51) | no |
| 12 | Record success | **Manual job** | same; `evaluator record-verdict` (l.151) | no |
| 13 | Reproducible evidence | **CLI tool** | scripts/reproduce_mandate_004.py, scripts/verify_mandate_004_run.py; hash-chained register; run artifact hashes | no |
| 14 | Schedule shadow testing | **Scheduled automated job (for already-coded families with an adapter)** | platform_runtime.ShadowScheduler: register_all + PlatformCycleDriver over FamilyRegistry.evaluable() (family_registry.py:341); started from production_runtime.run (l.822-824). Only DAILY_TREND qualifies; a new family needs code plus an adapter | yes, inside cosmicforge-trading.service (production profile, runtime owner) |
| 15 | Schedule forward DEMO | **Manual** | Family promotion to M6 needs an admin OperatorAuthorization (api/family_governance.py:208-231) and CERTIFIED; or a manual experimental grant (POST /experimental-demo, l.347) | no |
| 16 | Ongoing performance evaluation | **Partially scheduled** | DAILY_TREND shadow vs model comparison (driver.py:266-269, compare_models=True); residual forward observation (production_runtime.py:895-896). Gate H tracker (research/certification/forward_demo.py) is only called from the certification CLI pipeline (pipeline.py:350-355). pipeline/performance.py is a load harness, not strategy evaluation | partially |
| 17 | Propose improvements | **Absent** | — | no |
| 18 | Retire deteriorating | **Manual only** | Kill switch, revoke and disable only through admin API (api/family_governance.py:239-269) or emergency flatten (pipeline/coordination.py:349). ml/monitoring.py `drift_report`/`psi` has **no caller** | no |

**Answer to Q3.** No. CATI cannot take new market data, propose a new strategy, evaluate it and produce a
governed candidate without a developer at each stage.

Missing pieces:
1. An opportunity and hypothesis generator.
2. A generic, family-agnostic evaluator. Today's evaluator is hard-wired to Mandate 004, and Section 22
   certification is tied to the forecast line.
3. A strategy specification DSL or plugin contract, so a new family does not need new Python code, a new
   definitions() entry and a new pipeline adapter.
4. A register-hypothesis CLI or API.
5. An orchestrator or scheduler chaining freeze → costs → run → statistics → verdict → register.
6. Approved statistical thresholds.
7. Automatic promotion-request creation. Approval must stay human.
8. Drift monitoring wired to family kill switch or revoke proposals.

Existing tools that can be connected:
- mandates.register_hypothesis_and_mandate
- daily_dataset build / certification.freeze
- evaluator register-costs / costs collect
- evaluator.official.run, generalized over FamilyDefinition
- certification.splits and certification.stats
- governance.statistics
- holdout.reserve_holdout
- FamilyGovernance.request_promotion (anyone may request; this changes nothing)
- ShadowScheduler, which picks up any registered family with an adapter automatically
- ml.monitoring.drift_report

Holdout authorization and promotion approval are intentionally human and should stay so.

### 4. Research certification

**Pipeline elements and where they live (all exist and are unit-tested; L4/L5):**
- Hypothesis registration and freezing: register.py, mandates.py. MANDATE_REGISTERED pins the spec sha
  ea02ace4…, rule artifact 7795ad79…, and the research code commit.
- Datasets: DATASET_FROZEN rec. 22, binance_usdm_daily v1, 911 perps, 2020-01-01..2026-09-30, 663,145 bars,
  manifest 38d940f1…. Raw files are local only (≈280 MB, not committed; reproducible from the Binance public
  archive).
- Cost models: COST_MODEL_REGISTERED rec. 23. "ASSUMED_BY_THE_FROZEN_MANDATE (0.05% taker + 0.05% slippage)",
  not measured.
- Statistical policy: statistics.py; thresholds unset.
- Holdout protection: HOLDOUT_RESERVED rec. 24, 2025-01-01..2026-09-30, never authorized or opened. The
  evaluator refuses held-back rows without a HoldoutAccess token (official.py:107-110).
- Evaluation: RUN_STARTED/RUN_COMPLETED rec. 25-26, status VALID, verdict PENDING_HOLDOUT.
- Family registration: family_registry.register_all, M0 only.
- Governance promotion: M0..M6 operable (family_governance.py:42-46). M6 needs CERTIFIED and admission
  ELIGIBLE (l.302-312); M7+ is refused in this build.
- DEMO testing: the M6 portfolio tail or an experimental grant.
- Revocation: revoke_promotion and revoke_certification (l.353-379).
- Drift detection: library only, not wired.

**Verified status per family** (my registry run against the committed register; L4):

```
BREAKOUT_VOL_EXPANSION_V2 15m NOT_CERTIFIED M0 (RESEARCH_SIMULATION)
DAILY_TREND 1d NOT_CERTIFIED (REGISTERED) M0 (RESEARCH_SIMULATION, SHADOW)   prereq: mandate_verified=True valid_run=True admission=NOT_ELIGIBLE
MOMENTUM_CONTINUATION_V1 15m NOT_CERTIFIED M0
RANGE_MEAN_REVERSION_V2 15m NOT_CERTIFIED M0
RESIDUAL_MOMENTUM_PORTFOLIO_TOP1 1h FAILED (FAILED) M0   prereq: mandate_verified=False (RegisterError: Mandate 003 not in the register)
TREND_PULLBACK_V2 15m NOT_CERTIFIED M0
```

**Why each hypothesis failed.** Source: register records 1-18; failure_reasons are verbatim from the register.

| H | Family | Verdict | Cause class | Recorded metrics |
|---|---|---|---|---|
| H-001 | Forecast outcome library V1/V2 | REJECTED_PRE_HOLDOUT | V1: model defect (CALIBRATION_DEFECT=YES). V2: weak evidence | Brier skill 0.00048 / 0.00105 vs 0.02 required |
| H-002 | V3 info conditioning | REJECTED_PRE_HOLDOUT | weak evidence (one aborted run disclosed) | Brier skill 0.01853 < 0.02 |
| H-003 | V4 edge and payoff | REJECTED_PRE_HOLDOUT | weak evidence | Brier 0.01929; top quintile −0.078R |
| H-004 | V5 joint | REJECTED_PRE_HOLDOUT | weak evidence (temporal guard fails folds 3 and 5) | Brier 0.02009 passes, but every fold's top bucket is negative |
| H-005, H-006 | breakout acceptance / trend reclaim, 5m | FAILED | **missing data** | DATA_UNAVAILABLE, no 5m data, zero labels |
| H-007..H-010 | same, 15m/1h | FAILED | weak evidence | net −0.063 / −0.151 / −0.127 / −0.122 R |
| H-011..H-014 | causal diversified | FAILED | weak evidence (gross below cost) | net −0.115 / −0.396 / −0.052 / −0.086 R |
| H-015 | Residual momentum | FAILED | weak evidence (fold instability) | Fold-1 gross ≤0; LCB ≤0 in all folds; pooled net +0.073R on 326 trades |
| H-016 | dispersion RV | FAILED | weak evidence + runtime capability blocked | net −0.1195R on 103 trades |
| H-017 | funding carry | FAILED | weak evidence + runtime capability blocked | net −0.0421R on 518 trades |
| H-018 | Daily Trend | **REGISTERED, PENDING_HOLDOUT** | blocked on governance, not failed | Dev 2020-2024: net +33.68%, Sharpe 0.99, maxDD 11.54%, 1,192 trades; p=0.0147 unadjusted, 0.264 Bonferroni over 18 hypotheses; executable policy (0.40%/15% stop) balanced +12.59%, Sharpe 0.95; 86% of mandate entries have a stop >15% |

None of these is recorded as a software-defect failure, except H-001 V1's calibration defect, which V2
re-tested. The Section 22 `cati_experiment_registry` held zero rows (historical_hypotheses_v1.json
known_gaps). That trial-count defect was fixed by counting from the register.

**Can development testing continue without opening protected holdouts?** Yes.
- The evaluator's DEVELOPMENT run type only reads ≤2024-12-31 (official.py:107-110), and reruns are allowed
  as new runs with reason_for_rerun.
- New, independent data accrues from 2026-10-01 onward, after the Mandate 004 holdout end. The production
  daily feed stores it (market_data/daily_feed.py, sealed days).
- Section 22's reserved window starts 2026-07-12 and must not be touched.
- A rule change to Daily Trend is a **new hypothesis** (H-019+). It increases the multiple-testing count.

**Can new independent hypotheses be evaluated?** Mechanically yes. They are registered through mandates.py,
with a frozen dataset, a cost record and a reserved holdout. In practice each needs:
- new family code;
- an evaluator (the current one is Mandate-004-specific);
- owner-approved statistical thresholds, otherwise the gate always reports BLOCKED_PENDING_APPROVAL.

At 5% familywise / 80% power with 18+ hypotheses, the 1.75-year holdout can only detect Sharpe ≈2.7
(STEP2 doc open decision 2). Certifying any realistic strategy therefore needs an owner decision on the
standard.

**Genuinely required owner and research-governance actions** (none overrides a failed outcome):
1. Ratify or reject the 21 Mandate 004 interpretation points (INTERPRETATION_001/002). They were fixed by
   the agent before data was loaded and are not owner-approved.
2. Set the statistical gate (familywise alpha, power, target Sharpe) with approved_by and reference.
3. Decide the rule-based admission route (Section L). Without it DAILY_TREND cannot reach M6
   (admission NOT_ELIGIBLE).
4. Authorize, or decline, the Mandate 004 holdout (`governance authorize-holdout`), acknowledging that the
   window overlaps periods inspected by H-001..H-017.
5. Confirm the enumeration of the 17 historical hypotheses, which sets the multiple-testing N.
6. If Daily Trend passes, decide whether certification applies to the mandate policy or to the engine's
   executable policy (0.40% risk, ≤15% stop), because the two differ materially.
7. Optional: authorize new hypotheses (e.g., trend_v2_tradfi as H-019) on post-2026-10-01 data.

### 5. Section 28: unattended VPS operation

| Capability | State | Evidence |
|---|---|---|
| Collect data | **Automated** (production profile, runtime owner) | production_runtime.py:893-898 residual collect (hourly) + forward public observations (5 min); ShadowScheduler daily feed (platform_runtime.py:38-56) |
| Store histories | **Partial** | Daily bars are stored and sealed (daily_feed). Hourly residual inputs are pruned to ~28 days (residual_prospective.py:22 WINDOW=672h; l.415 DELETE). Research datasets come from CLI downloads, raw files not committed |
| Detect gaps | **Partial** | daily_feed tracks missing/failed/pending symbols (l.746-749). Residual missed boundaries are recorded as explicit skips (l.384-388). No alerting of research-data gaps found |
| Run research evaluations | **Absent** (CLI only) | §3 rows 5-13 |
| Generate fresh candidates | **Absent** | §3 rows 1-3 |
| Immutable evidence | **Present** | hash-chained register (verified); cati_family_events; cati_experimental_demo_events with UPDATE/DELETE triggers (experimental_demo.py:85-98) |
| Shadow trading | **Automated for DAILY_TREND only** | ShadowScheduler + PlatformCycleDriver (handoff=None) |
| Experimental DEMO | **Manual grant, bounded, currently lapsed** | experimental_demo.py:42-43 (≤336h, ≤1000 USDT); grant xdemo_a2e102e911db4144b29b 72h from 2026-11-01 |
| Monitor candidates | **Partial** | Decisions persisted in cati_production_decisions; dashboard. No automated candidate-quality monitor |
| Forward evidence | **Partial** | Residual forward observations accumulate. Gate H tracker is only run from the certification CLI |
| Drift | **Absent in runtime** | ml/monitoring.py has no caller; forecast/ood.py belongs to the forecast pipeline |
| Re-evaluation | **Absent** | no scheduled re-run |
| Promotion requests | **Manual** | API request/approve, operator token |

What prevents unattended operation:
- Every research stage needs a human. There is no scheduler or orchestrator, and the evaluator is
  family-specific.
- No family has order authority. RESIDUAL is FAILED and DAILY_TREND is uncertified, so unattended DEMO
  trading depends on owner-renewed experimental grants and an L-08 policy. Both have lapsed by their recorded
  terms (L6/L7; DB not inspected).
- The bot's fixed 120 USDT margin is incompatible with the 0.40% risk limit and the 100 USDT grant cap
  (activation report §6).
- No drift-triggered retirement exists.
- Hourly history is not retained for later research.

The data-collection and shadow parts can run unattended. The research and certification loop cannot.

### 6. Experimental DEMO authorization

What it permits (experimental_demo.py docstring l.1-29; provenance.py:281-293, 340-360; L5, tests L4 20/20 in
the 75 passed):
- It waives exactly two family-gate checks: research certification ≠ CERTIFIED, and stage < M6.
- The waiver covers one (account, user, family) on a verified DEMO account only. Stored env, resolved
  credentials and base URL are each re-checked on every use.
- **So yes: it lets a FAILED/M0 family trade on DEMO.** The operator must type the current research status
  (`acknowledged_research_status`, l.322-324).
- It refuses families that are unregistered, have a hash mismatch, are disabled or are kill-switched. It also
  refuses families that already have DEMO authority.
- Every other gate still applies: venue certification, consent, DEMO order switch, risk, L-08, account fence
  and protection.
- It never writes the governance log, the register or the holdout, and never grants LIVE.

Who grants it:
- An `OperatorAuthorization` from an authenticated administrator: bot API POST
  /api/v1/admin/governance/experimental-demo, proxied by user-backend with a 60-second operator token.
- An approval_ref and a reason are required.

Expiry and limits:
- 1-336 h, lapsing on its own; the notional cap is ≤1000 USDT per entry.
- Revocation is effective at the next check.
- A broken ledger chain authorizes nothing.

Integration limit (L5/L7, not covered by a test I found): only RESIDUAL can actually use a grant.
- Residual plans go through `_process_account` (production_execution.py:461) and the gate's waiver.
- DAILY_TREND's portfolio tail is selected only when DEMO ∈ allowed_modes (portfolio_runtime.py:237-245),
  which a grant never adds. A grant for DAILY_TREND would be accepted by `authorize()` but no order path would
  use it.

The grant in TRADINGBOT_DEMO_ACTIVATION_REPORT.md (l.13, 62-82, 132-140):
- xdemo_a2e102e911db4144b29b, owner-approved per the report, 72 hours, ≤100 USDT notional per entry, account
  brk_c729454e6c98 (Binance DEMO), family RESIDUAL (acknowledged FAILED).
- It was granted 2026-11-01, so it expired around 2026-11-04 by its terms. The DEMO L-08 policy 81df3fbf…
  expired 2026-11-08.
- No strategy order was placed under it. The first signal was refused on notional (480 > 100).
- The ledger itself was not inspected (owner DB).

### 7. Gap list

| ID | Component | File:lines | Problem | Root cause | Evidence (+level) | Customer impact | Trading impact | Sev | Classification | Existing implementation | Required correction | Dependency | Owner | Validation test | State | Effort | Phase |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| RES-01 | Mandate 004 / DAILY_TREND certification | docs/research/mandate_004/certification_result.json; research_register.jsonl rec.18-26 | The only promising family is blocked at PENDING_HOLDOUT: interpretation points unratified, statistical thresholds unset, admission route undecided, holdout not authorized | verified | register + registry run (L4); STEP2 doc (L6) | No certified strategy to offer | No family can trade under certification | P1 | OWNER_POLICY_DECISION | Full evaluator, holdout guard, admission and promotion code | Owner records the decisions via `governance record-decision` / `authorize-holdout`; then run the HOLDOUT evaluation; accept the verdict whichever way it goes | RES-05 | Owner/Research | register shows HOLDOUT_AUTHORIZED→OPENED→BURNED and a verdict | OPEN | M | C |
| RES-02 | Family portfolio | pipeline/family_registry.py:181-192 | Zero CERTIFIED families. The only family wired to orders (RESIDUAL) is FAILED | verified | registry run (L4) | Autonomous strategy trading unavailable | Only experimental DEMO possible | P1 | RESEARCH_GAP | — | Certify a family through governed research; do not override failures | RES-01 | Research | FamilyRegistry.executable() non-empty | OPEN | XL | C |
| RES-03 | Autonomous research | (absent) | No opportunity detection, hypothesis generation, orchestration or scheduler for research | verified | grep of deploy/systemd and production_runtime (L5) | "Autonomous research" not deliverable | none directly | P2 | MISSING_FEATURE | Libraries/CLIs in §3 | Build an orchestrator chaining the existing CLIs, with human gates at holdout and promotion | RES-04 | Engineering/Research | end-to-end synthetic hypothesis → governed candidate test | OPEN | XL | D |
| RES-04 | Evaluator | research/evaluator/__main__.py:25, official.py:31 | The official evaluator is hard-wired to families.daily_trend; each prior mandate used a bespoke script | verified | imports (L5) | — | Every new family needs new evaluator code | P2 | ARCHITECTURE_GAP | simulator/metrics/official for Daily Trend; Section 22 pipeline | Generalize the evaluator over FamilyDefinition plus a rules plugin contract | — | Engineering | evaluate a second synthetic family with no evaluator edits | OPEN | L | D |
| RES-05 | Statistical gate | research/governance/statistics.py:50-75 | Thresholds None, so the gate is always BLOCKED_PENDING_APPROVAL; at the plausible standard, MDS ≈ Sharpe 2.7 | verified | code (L5); STEP2 (L6) | — | No hypothesis can pass | P1 | OWNER_POLICY_DECISION | Policy dataclass with approval fields | Owner sets alpha/power/target with reference | — | Owner | statistical gate returns PASS/FAIL, not BLOCKED | OPEN | S | C |
| RES-06 | Reports | TRADINGBOT_FINAL_SYSTEM_CERTIFICATION.md (branch) l.32, l.235 | Claims every family is FAILED/M0; DAILY_TREND is NOT_CERTIFIED/PENDING_HOLDOUT | verified | register + registry run (L4) | Misinforms owner decisions | Could cause Daily Trend to be abandoned wrongly | P2 | OPERATIONS_GAP | STEP2 doc is correct | Correct the document wording | — | Engineering | doc review | OPEN (the wrong claim exists only in the branch doc) | S | A |
| RES-07 | Experimental DEMO ↔ portfolio tail | portfolio_runtime.py:237-245; experimental_demo.py:313-327 | A grant for DAILY_TREND (or any adapter family) is accepted but no execution path honors it; only the residual legacy path does | unverified (no test found) | static (L5/L7) | Admin may grant something with no effect | none (fails closed) | P3 | INTEGRATION_GAP | waiver in FamilyExecutionGate | Either refuse grants for families without a path, or route waived families to the portfolio tail | — | Engineering | test: grant DAILY_TREND → authorize refused or plan reaches boundary | OPEN | S | C |
| RES-08 | DEMO execution authority | experimental_demo.py:42-43; activation report l.13, l.98 | Grant (72h from 2026-11-01) and L-08 policy (to 2026-11-08) have lapsed: no strategy order possible on DEMO | unverified (DB not read) | docs (L6) + code expiry (L5) | DEMO bot never trades | none | P1 | OWNER_POLICY_DECISION | grant/revoke UI and API | Owner decides whether to renew (knowing RESIDUAL is FAILED) and fixes bot sizing | — | Owner | ledger shows ACTIVE grant; decision → order | OPEN | S | C |
| RES-09 | Drift / retirement | ml/monitoring.py:16-33 | drift_report has no caller; no automatic deterioration detection or retirement proposal | verified | grep (L5) | — | A deteriorating family keeps running until a human acts | P2 | MISSING_FEATURE | kill switch, revoke, PSI library | Schedule drift and forward-performance checks that raise kill-switch/revoke **requests** | RES-02 | Engineering | synthetic drift → alert + request recorded | OPEN | M | C |
| RES-10 | Forward evidence / Gate H | research/certification/forward_demo.py; pipeline.py:350-355 | Forward-DEMO tracker runs only from the CLI certification pipeline; no periodic re-evaluation | verified | grep (L5) | — | M6→M7 evidence not accumulated automatically | P2 | INTEGRATION_GAP | tracker class | Schedule tracker snapshots per M6 family | RES-02 | Engineering | timer writes tracker evidence | OPEN | M | C |
| RES-11 | Research data retention | residual_prospective.py:22, 415 | Hourly inputs pruned to ~28 days; research datasets are local CLI downloads (raw not committed) | verified | code (L5); STEP2 2.3 (L6) | — | Fresh hourly history not available for future research | P2 | DATA_GAP | daily_feed sealed days; acquire_* scripts | Archive hourly bars or schedule dataset acquisition with manifests | — | Engineering/Operations | dataset coverage report grows daily | OPEN | M | C |
| RES-12 | Measured costs | research/costs/__main__.py:48; mandate_004/market_costs | Calibration INSUFFICIENT_DATA (one 2-minute window, 2 fills); collector not scheduled | verified | docs (L6) + no caller (L5) | — | Cost model assumed, not measured | P2 | DATA_GAP | collector + store | Schedule `costs collect`; record DEMO fills | — | Engineering | ≥N observations across regimes | OPEN | S | C |
| RES-13 | Holdout independence | rec.24; STEP2 open decision 1 | Mandate 004 holdout window 2025-01..2026-09 overlaps data inspected by H-001..H-017 and contains the Section 22 reserved window | verified (documented) | register (L4) | — | Weaker OOS claim | P2 | RESEARCH_GAP | acknowledgements field in authorize-holdout | Owner acknowledges explicitly; consider post-2026-10-01 forward data as additional OOS | RES-01 | Owner/Research | acknowledgement recorded in HOLDOUT_AUTHORIZED | OPEN | S | C |
| RES-14 | Mandate vs executable policy | certification_result.json by_risk_level; STEP2 "55-of-74" | 86% of Daily Trend (58% of residual) entries have a stop >15%; the engine refuses them, so certified results would not represent executable behaviour | verified | artifacts (L4) | — | Live results would diverge from certified results | P2 | RESEARCH_GAP | executable scenario reported separately | Owner decides which policy certification binds to, before the holdout | RES-01 | Owner/Research | decision recorded | OPEN | S | C |
| RES-15 | Multiple-testing N | historical_hypotheses_v1.json counting_rule/known_gaps | The 17 historical hypotheses are reconstructed (alternative readings give 17 or 18); owner confirmation outstanding | verified | register rec.0 (L4) | — | Bonferroni N uncertain | P3 | OWNER_POLICY_DECISION | import record | Owner confirms the enumeration | — | Owner | GOVERNANCE_DECISION record | OPEN | S | C |
| RES-16 | Legacy V2 strategies / IOFS | strategy/*.py; config.py:417; main.py:2993 | Dead code with no production caller; IOFS_GATE_MODE echoed in /health and effective policy though the module is unused | verified | grep (L5) | Misleading config/health | none | P3 | ARCHITECTURE_GAP | destination_map LEGACY_AFTER_PROMOTION | Remove at M9, or label as inactive in health/policy | — | Engineering | health no longer reports an unused IOFS mode | OPEN | S | A |
| RES-17 | Residual as production strategy | register rec.15 notes; production_execution.py:461 | A FAILED hypothesis is the only family wired into the production order path (kept by owner choice; conflict recorded) | verified | register (L4), code (L5) | DEMO users see a FAILED strategy's signals | Experimental DEMO only | P2 | OWNER_POLICY_DECISION | family gate refuses without grant | Keep the gate. The owner should decide whether to keep running it or to replace it with a certified family | RES-02 | Owner | — | OPEN (WORKING_AS_DESIGNED for safety) | S | C |

### 8. Verdicts

- **Autonomous strategy research: BLOCKED (not implemented).** Individual stages exist as libraries and CLIs.
  These are registration, frozen datasets, cost models, an evaluator, holdout governance, statistics and a
  reproduction harness. Hypothesis generation, opportunity detection, a generic evaluator, orchestration,
  scheduling, drift and retirement are absent. Only shadow evaluation of already-coded families is scheduled.
- **Strategy certification: PARTIAL.** The tooling is real, tamper-evident and tested (75/75 targeted tests;
  register chain verified). The outcome is that no family is certified:
  - RESIDUAL is FAILED.
  - DAILY_TREND is NOT_CERTIFIED and blocked at PENDING_HOLDOUT on owner decisions. It has not failed.
  - Four specialists have no hypothesis; 16 historical hypotheses are FAILED or REJECTED.
  - Certification of anything is blocked until the owner sets the statistical standard and admission route.
- **Historical research data: PARTIAL.**
  - binance_usdm_daily v1 is frozen with manifest, coverage and quality report (2020-01-01..2026-09-30,
    911 perps).
  - The crypto_broad 15m set exists via manifests. Raw files are local only but reproducible from the public
    archive.
  - Missing: 5m data (it caused H-005/H-006), measured cost calibration, retained hourly runtime history, and
    any FX/TradFi strategy dataset.

