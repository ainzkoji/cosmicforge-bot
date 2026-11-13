# Implementation Roadmap (Section 34)

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · 2026-11-13 · baseline `origin/main` `649080b`, branch `732e0e2`
Work is ordered by dependency. IDs: M-xx = consolidated findings in [TRADINGBOT_MASTER_END_TO_END_AUDIT.md](TRADINGBOT_MASTER_END_TO_END_AUDIT.md);
area IDs = [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv). Payment work is excluded. Effort: S ≤ 2 days, M ≤ 1 week,
L ≤ 3 weeks, XL > 3 weeks (one engineer, relative).

Guiding rules for every phase: reuse existing components (named per task); never weaken a fail-closed gate to get a green path; never change
research verdicts, strategy parameters, saved customer settings or LIVE locks; never open protected holdouts; every closure carries a test
named in the gap register's "Validation" column.

---

## Phase A — Critical trading correctness

**Goal:** no configuration of the platform can leave an open position without its protection, and `main` contains every verified fix.

| Task | Closes | Reuse | Effort |
|---|---|---|---|
| A1. Separate risk-reducing authority from entry authority: one `risk_reducing_permitted()` decision used by `production_execution._reconcile_attempt` (post-fill stop, fail-safe close, horizon close), `execution/boundary.reconcile_submit_unknown` (recovered fill), `execution/perpetual_protection.place_protection` (Bybit/BingX). Classify a DELETE as risk-reducing only when it does not remove the last protective order of an open position. | M-01 (EXEC-01/02, CATI-05, OPS-20, EXEC-10 part) | Branch `334f2ff` transport classification, `protection_state`, `test_risk_reduction_gate.py` | M |
| A2. Merge the engineering branch to `main` via PR after A1; correct RES-06 wording in the branch certification draft. | M-02 (LEAD-01), RES-06 | Branch commits 334f2ff, a746a58, f8401d5, 65372fe | S |
| A3. Disconnect/delete: delete `broker_credentials_v2`; refuse while positions/orders exist unless the customer confirms a flatten through `production_runtime.flatten`. | M-11 (EXEC-03, CUST-08) | `broker_service.py`, emergency flatten path | S |
| A4. Remove or gate root scripts that sign broker requests (`close_btc_raw.py`, `close_btc.py`, etc.); put legacy `/binance/cancel-all`, `/binance/leverage`, `/trade/protection` behind `_cycle_lock` + CATI account authority, or delete them. | M-25 (EXEC-09, ARCH-08) | `_LEGACY_ADMIN_ONLY`, `production_runtime` cycle lock | S |

**Dependencies:** none (start immediately). **Blockers:** none.
**Acceptance tests:** with `DEMO_ORDER_SUBMISSION_ENABLED=false`: in-flight fill gets a native stop; recovered unknown-ack fill gets a stop;
Bybit/BingX protection placed; entries and increases refused; cancelling the only stop refused; LIVE fully refused (`BUILD_PERMITS_LIVE=False`).
Disconnect leaves no credential row; flatten-on-delete reconciles flat. No script in the repo can sign a broker request outside the gates (grep test).
**Deliverables:** merged `main` with CI green; updated `test_emergency_controls`.
**Completion criteria:** M-01, M-02, M-11, M-25 marked VERIFIED_FIXED with commit SHAs.

## Phase B — Core integration

**Goal:** one self-consistent deployable system whose newer layers are actually active, with gate parity on both execution tails.

| Task | Closes | Reuse | Effort |
|---|---|---|---|
| B1. Staging deployment strictly from the merged VPS guide: PostgreSQL 16, `PLATFORM_DATABASE_URL`, `shared_lib.persistence.sql migrate/status`; `/health` must report the platform layer enforced. | M-03 (ARCH-01, CATI-12, OPS-01) | a746a58 guide §2.4, `deploy/staging/*` | M |
| B2. Enforce the config-profile validator at production startup (fail closed); give `env.production.example` a profile name; admin-backend refuses non-SQLite URLs (or is migrated to the platform DB). Decide and document **one** production DB model. | M-30 (ARCH-05, ARCH-06), OPS validator | `shared_lib/core/config_profiles.py`, `scripts/validate_config_profile.py` | M |
| B3. Path-B gate parity: go-live gate, platform order budget/spread (L-08), depth freshness, account fence in `PortfolioExecutionBoundary`. | M-05 (ARCH-02, CATI-04) | `CATIExecutionBoundary._reserve_spread`, `_go_live_reason`, `account_fence.entry_refusal` | M |
| B4. Certification portability: export/import of venue-certification records with hash-chain verification; call `record_adapter_version` at startup; refuse reuse across adapter versions; refresh `docs/exchanges/EXCHANGE_CERTIFICATION.md`. | M-06 (EXEC-06, CATI-11) | `platform_adapter/certification.py`, `cati_adapter_cert_events` | M |
| B5. One certified execution tail for strategy orders: certify the legacy `BinanceExecutor` path via the existing harness, or route path A entries through the certified platform adapter; remove hard-coded `DEMO_VALIDATED`; pass `adapter_version` to `adapter_certified`. | M-07 (EXEC-05, ARCH-03) | `certification_harness.py`, `platform_adapter/factory.py` | L |
| B6. Operations wiring: install ops-tick, ops-deadman, platform-db-backup timers in the guide; offsite upload of platform dumps; revocation anchor off the data disk. | M-13 (OPS-02, ARCH-04) | `deploy/systemd/*`, `scripts/offsite_backup.py` | S |
| B7. Shutdown safety: keep the lease until the broker cycle is idle; fix `force_exit` (captured loop; no `os._exit` while `_cycle_lock` held). Lease-gate the remaining background loops; reclaim stale `processing` job claims. | M-27 (ARCH-09), ARCH-11, ARCH-13 | `runtime_shutdown._quiesce_inner`, `main.py:770-788` | S |
| B8. Fix user-backend `POST /api/v1/bot-instances` proxy target; remove duplicate `/api/v1/mt/connector/claim`; delete dead client functions. | LEAD-02, CUST-14 | — | S |

**Dependencies:** Phase A merged. **Blockers:** a staging host (owner/operations).
**Acceptance tests:** staging stack built only from the guide passes `/health` (platform layer enforced, lease held, collectors fresh);
`test_step5_spread_boundary`-equivalent suite passes on path B; certificate imported on a fresh DB authorizes only its adapter version;
SIGTERM during a cycle with a pending fill leaves a protected position; ops-tick alert delivered to a test channel.
**Deliverables:** staging evidence bundle (commands, outputs, timestamps) committed under `evidence/staging-*`.
**Completion criteria:** M-03, M-05, M-06, M-07, M-13, M-27 VERIFIED_FIXED; staging evidence recorded.

## Phase C — Data and research autonomy

**Goal:** reliable 24/7 data with honest health, and a research loop that produces governed candidates without a developer starting each stage.

| Task | Closes | Reuse | Effort |
|---|---|---|---|
| C1. Route the 15m collector through the shared HTTP budget and record its 429/418; key the budget by host/environment so a DEMO-host ban cannot block LIVE protection; reserve budget for protection/flatten. | M-19 (DATA-02, DATA-04) | `exchange/platform_adapter/http_budget.py` | M |
| C2. Market-data operations: prune rule for forward observations (or store aggregates); `/health` reports DAILY_TREND feed freshness and per-symbol failure ratios; backoff for missing archive files; parallelize residual collection within the budget. | M-28 (DATA-03, -06, -08), CATI-09 | `forward_observe.py`, `runtime_supervisor`, `daily_feed.py` | M |
| C3. Research data custody: offsite, versioned copy of frozen datasets; licence record for exchange and Dukascopy data; retain hourly history beyond ~28 days. | M-33 (DATA-10, DATA-14, RES-11) | dataset manifests, `scripts/offsite_backup.py` | S |
| C4. Research orchestration on existing tools: generic evaluator + strategy plugin contract (generalize the Mandate 004 evaluator); governance CLI `register-hypothesis`; an orchestrator job chaining register → freeze → cost → walk-forward → multiple-testing → verdict → shadow scheduling; scheduled drift (`ml/monitoring.drift_report`) and forward-evidence (Gate H) checks that raise promotion/retirement *requests* for humans; scheduled cost-calibration collector. | M-24 (RES-03, -04, -09, -10, -12) | `research/*` CLIs, research register, family registry, ShadowScheduler | XL |
| C5. Hypothesis generation (bounded): a template-driven candidate generator over approved feature/rule families, every candidate counted in the multiple-testing ledger. Holdout authorization and promotion stay human. | M-24 (generation) | research register, cost model | L |

**Dependencies:** Phase B (platform DB for durable jobs). **Owner decisions (blocking C4 outcomes, not the engineering):** statistical standard
(RES-05), Mandate 004 interpretation points (RES-01), admission route, holdout authorization, mandate vs executable policy (RES-14), multiple-testing
count (RES-15).
**Acceptance tests:** fault-injected 429 on the 15m collector reduces collection, never protection; forward-observation table bounded; an
end-to-end orchestrator run on development data registers, evaluates and records a verdict for a synthetic hypothesis with no manual step and
without reading holdout data (assert by data-window guard).
**Completion criteria:** M-19, M-28, M-33 fixed; orchestrator produces a governed (REGISTERED → verdict) candidate unattended in staging.

## Phase D — Trading execution

**Goal:** authorized strategies can trade correctly sized, on every offered venue, through one risk model.

| Task | Closes | Reuse | Effort |
|---|---|---|---|
| D1. Sizing compatibility: Edit gains the risk-based option; a compatibility preview (fixed/percent vs 0.40 % ceiling, stop distance, leverage, grant cap) before deploy/edit; the legacy deploy body goes through preview/consent/idempotency/one-bot rule or is removed; enforce the per-trade ceiling in hard risk for fixed/percent bots. | M-09 (CUST-04/05/06/16, CATI-06) | deployment preview service, `spread_gate.py:604-621` | M |
| D2. Path B honours experimental DEMO grants (or the admin console refuses to issue grants for families that cannot use them). | M-31 (CATI-03, RES-07) | `experimental_demo.plan_scope` | S |
| D3. Consolidate risk engines into one evaluated at the final boundary for both tails; add a correlation/cluster exposure limit on path B; cross-account risk budget per user. | M-26 (EXEC-12), OPS-12 | `risk_sizing`, `AccountLimits`, `ReservationLedger` | L |
| D4. Concurrency/capacity: per-account parallel broker cycle with per-tenant fairness; multi-egress plan (IP pools) with per-IP budget accounting; capacity metrics and thresholds. | M-17 (OPS-06, DATA-07, CATI-10, OPS-10) | `http_budget`, platform worker assignments | L |
| D5. Bybit/BingX completion: emergency flatten, adapter status entries, verified rate limits, venue-native data or measured divergence, then genuine DEMO certification runs. | M-20 (EXEC-07, DATA-12) | certification harness | L (+ external) |
| D6. Withdrawal-permission check fails closed (unknown → not activated), periodic re-probe, BingX/DEMO handling explicit. | M-21 (EXEC-04) | activation probe | S |
| D7. Credential validation HTTP outside the DB write lock. | EXEC-11 | `broker_service` | S |

**Dependencies:** Phase B (B3, B5). **Acceptance tests:** `bot_a8117dc719fc`-shaped configuration is blocked at preview with the explanation;
grant on DAILY_TREND either executes on DEMO staging or is refused at issue; one risk engine passes both tails' existing risk suites; 50-account
synthetic run with real network latency budgets meets the 15-minute entry window.
**Completion criteria:** M-09, M-17 (initial capacity documented), M-20 (if venues offered), M-21, M-26, M-31 closed.

## Phase E — Customer and admin experience

**Goal:** every screen tells the truth; operators can run the platform from the console. Preserve the approved layout.

| Task | Closes | Reuse | Effort |
|---|---|---|---|
| E1. Portfolio truth: environment labels everywhere; analytics/reports separated by environment and currency; funding included; per-account errors and data age displayed; signed open P&L. | M-10 (CUST-01/02/03), CUST-15 | 65372fe `portfolio_service` | M |
| E2. Admin console: real System Health (from runtime supervisor/ops tick) or remove; wire or remove Platform Settings; retarget Bot Monitor to CATI tables; proxy engine admin CATI/evidence/operations/readiness endpoints; resolve the duplicate admin API. | M-12 (CUST-07/07b/09/10), LEAD-03 | bot-backend admin routers, user-backend proxy utils | L |
| E3. Customer controls: close-position action through the gated flatten path; correct the stop-dialog wording; attach BotDetails delete; suspension stops the user's bots and audits the acting admin. | M-34 (CUST-11/12), CUST unwired button | `production_runtime.flatten` | M |
| E4. Admin roles: at least operator / risk approver / viewer with two-person rule for risk-increasing actions. | M-29 (CUST-13) | `require_permission` | M |
| E5. Tests: DOM/E2E journey tests (Playwright) for the 27 customer steps and key admin flows; systematic cross-tenant ownership test over every object-id route; route smoke test over all 709 routes. | M-32 (OPS-15, LEAD-05, CUST-17) | existing pytest fixtures | L |

**Dependencies:** Phase A (65372fe merged); E2 depends on B1. **Completion criteria:** E2E suite green in CI; no admin page renders static data.

## Phase F — Production infrastructure

**Goal:** the VPS runs the release with evidence, security hygiene and humans on call.

| Task | Closes | Owner | Effort |
|---|---|---|---|
| F1. Install on the production VPS from the guide (read-only inspection first for conflicts with other hosted apps): units, nginx/TLS, UFW, PostgreSQL, backups (local + offsite, both DBs), alert delivery; perform a restore drill; record evidence. | M-14 (OPS-03) | Operations | M |
| F2. Rotate every administrator credential named in history; re-enrol MFA; record date/operator (EXT-SEC-02). | M-15 (OPS-04) | Operations | S |
| F3. Key-rotation tooling for `BROKER_SECRET_KEY`, `CREDENTIAL_KEY`, KYC key (PostgreSQL-capable); `AUTH_BEARER_COMPAT=false`; admin IP allow-list enabled. | M-29 (OPS-07/08/09) | Engineering | M |
| F4. External penetration test and remediation. | M-16 (OPS-05) | External | M (+ external) |
| F5. On-call roster, alert channels, runbooks exercised; kill-switch and flatten drill on Binance DEMO in the authorized environment. | M-18 (OPS-21) | Owner + Operations | S |

**Dependencies:** Phases A–B (F1 needs B1 proven on staging). **Completion criteria:** host evidence bundle; rotation record; pen-test report with
no open high findings; drill record.

## Phase G — Real-world DEMO trading tests

**Goal:** the complete system trades on Binance DEMO under valid authorization and accumulates forward evidence.

| Task | Closes | Owner | Effort |
|---|---|---|---|
| G1. Choose the authorization route: (a) owner-approved, time-boxed experimental grant for RESIDUAL as a clearly labelled diagnostic of a FAILED family, with a compatible risk-based bot; and/or (b) DAILY_TREND after owner decisions, holdout evaluation and governance promotion to a DEMO stage (requires B3/D2). | M-04 | Owner + Research | — |
| G2. Operator-approved, time-boxed L-08 market-entry policy for the DEMO account(s). | M-06 | Owner/Operations | S |
| G3. Re-run Binance DEMO venue certification on the production host for the tail strategies use. | M-07, M-06 | Operations | S |
| G4. Operate continuously; collect forward evidence (fills, protection, exits, reconciliation, refusals); weekly evidence review; drift checks from C4. | M-08 | Operations + Research | elapsed time |

**Completion criteria:** first CATI strategy order with fill, native stop, exit and final reconciliation recorded as L2 evidence; agreed forward
window completed without unprotected exposure or unreconciled state.

## Phase H — Controlled LIVE certification

**Goal:** genuinely certified, controlled real-money beta. Nothing here bypasses the seven LIVE locks; each lock is satisfied by its intended evidence.

| Task | Closes | Owner |
|---|---|---|
| H1. Family promotion to the LIVE stage through governance with forward DEMO evidence. | M-23 | Research + Owner |
| H2. Binance LIVE venue certification run (requires a live-capable build). | M-23 | Operations + External |
| H3. Live-capable signed release: owner + two reviewers populate `APPROVED_LIVE_RELEASES`, build via `live-release.Dockerfile`, release manifest recorded. | M-23 | Owner |
| H4. Policy keys: `LIVE_CONSENT_VERSION`, `APPROVAL_MAX_VALIDITY_DAYS`, `STAGE_A_MAX_NOTIONAL`, LIVE L-08 policy, liquidity calibration (EXT-LIQ-01). | M-23 | Owner |
| H5. Legal counsel opinion; country eligibility enforced in code before trading enablement. | M-22 (OPS-17) | External + Engineering |
| H6. M-19 budget isolation verified; M-21 withdrawal check fail-closed verified on LIVE keys. | M-19, M-21 | Engineering |
| H7. Live beta enrollment (Stage A, 8 weeks) with two-operator switch and revocation ledger anchored off-host. | M-23 | Owner + Operations |

**Completion criteria:** every go-live gate condition satisfied with recorded evidence; first LIVE beta account trades under caps with daily
reconciliation.

---

## Dependency summary

```text
A (protection, merge) ──► B (deployable, gate parity, certification) ──► C (data safety, research loop)
        │                         │                                         │
        │                         ├──► D (sizing, risk consolidation, venues, capacity)
        │                         ├──► E (truthful UI, admin ops, E2E)
        │                         └──► F (VPS, rotation, pen test, on-call)
        └──────────────────────────────────────────────────────────────► G (DEMO trading)  needs A,B,D1,F + owner authorization
                                                                         └──► H (LIVE)    needs G evidence + C1 + D6 + legal
```
