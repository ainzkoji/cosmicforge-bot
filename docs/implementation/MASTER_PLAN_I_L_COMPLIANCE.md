# Master plan Sections I–L: compliance, consolidation and implementation record

> **Superseded in part (31 October 2026, later pass):** the quarantine is now empty, D2/D3/D8 are fixed, D4 is partly fixed, F6 and L-07 are done and L-08 has its guarded mechanism. See [TRADINGBOT_FINAL_ENGINEERING_CLOSEOUT.md](TRADINGBOT_FINAL_ENGINEERING_CLOSEOUT.md).

Assessed and implemented on 31 October 2026 on `main`, starting from `ef22dba`. It covers:

- Section I (removal and consolidation);
- Section J (security, operations, legal and the launch checklist);
- Section K (K1 risk sizing, K2 database, K3 API, K4 configuration);
- Section L (decisions);
- the Section M progress reconciliation.

The text of Sections I–M is not in the repository. Requirements are taken from the assignment that quoted them.

**Status words.**

| Word | Meaning |
|---|---|
| **PASS** | Implemented, integrated in the real runtime, tested, CI green on the final commit. |
| **PARTIAL** | Some of the requirement is met; the rest is named. |
| **BLOCKED** | Cannot proceed without the named owner decision, external party, infrastructure or elapsed time. |
| **DEFERRED** | Deliberately not done now; the reason is given. |

Engineering CI never counts as strategy certification, venue certification, counsel approval, a penetration test or
elapsed live operation.

**Unchanged trading safety.**
- SHADOW_ONLY. Every real family is at M0; no family is certified; DAILY_TREND/H-018 is uncertified.
- The Mandate 004 holdout is unopened.
- 0.40 % per-trade ceiling; 15 % maximum entry stop distance.
- `BUILD_PERMITS_LIVE=False`, `LIVE_ORDER_SUBMISSION_ENABLED=false`, `BILLING_ENFORCED=false`.
- No exchange order, Stripe charge or production database access was made. Research files are unchanged
  (section 13).

Related records, updated in this pass:
- [SECTION_I_REMOVAL_INVENTORY.md](SECTION_I_REMOVAL_INVENTORY.md): batches, dependency evidence, deletion plans;
- [QUARANTINE_REGISTER.md](QUARANTINE_REGISTER.md);
- [STEP5_STEP8_EXTERNAL_GATES.md](STEP5_STEP8_EXTERNAL_GATES.md): the Section J launch checklist and every external
  gate;
- [STEP5_STEP8_ENGINEERING_CLOSURE.md](STEP5_STEP8_ENGINEERING_CLOSURE.md): the previous pass.

---

## 1. Summary

| Section | Result |
|---|---|
| I — removal | Batches 1 and 5 verified done. **Batch 7 progressed**: the source-reading capability probes are replaced by behavioural ones, and four unreferenced legacy strategy modules (1,441 lines) are removed. Batches 2, 3, 4, 6, 9 and 10 await the owner; their deletion plans are ready. Batch 8 is consolidation only (the three execution boundaries differ in their gates). |
| J — security and operations | 21 earlier fixes re-verified. **8 new fixes**: an IBKR gateway SSRF in the portal, Stripe signature rejections recorded and alerted, API-outage and governance-violation alert sources, admin MFA not switchable off in production and enforced by the admin backend, SMTP certificate verification, weak broker and KYC keys refused. F6: `cryptography` 50, `pillow` 13 and `pyasn1` 0.6.5 upgraded and regression-tested; 25 advisories remain in 4 packages (starlette 10, pip 12, ecdsa 2, python-jose 1), against 53 in the bot environment before. F8 cut-over, penetration test and on-call stay external. |
| J — launch checklist | Written from the master plan's J-01…J-09 (section 5.4). None is fully accepted; each is split into engineering, CI, staging, external and owner parts. |
| K1 — sizing | **Bug fixed**: a `percent_balance` budget was sized as if the percentage were USDT. The formula, cap order and 40 %/15 % separation are verified with file and line evidence. 4 deviations need the owner (section 6). |
| K2 — database | Every required entity is mapped to its physical table. The gaps are listed. No table is dropped: every legacy candidate still has active readers. |
| K3 — API | **Bug fixed**: the bot summary ignored every trade past the newest 200. Added: decimal-string money with an asset, cursor pagination, a status blocker list, percent-budget money view, case-insensitive `risk_level`, SSE event ids, Last-Event-ID replay, cross-process delivery, the full event envelope and the portal subscribing to the eight bot events. |
| K4 — configuration | Each variable is mapped to its real name. **Startup now refuses**: the live submission flag in a non-live build, a certification older than 90 days, invalid lease settings, and a production database fallback. |
| L — decisions | Approved decisions verified; L-01…L-06 recorded as open. The Academy page is now an honest placeholder; keeping or removing it is open. |
| M — progress | Chronology reconciled with commit evidence (section 11). |

## 2. Baseline and commits

- **Baseline:** `ef22dba`, the last Steps 5–8 closure commit. Its CI was run [41134191747](https://github.com/ainzkoji/cosmicforge-bot/actions/runs/41134191747), all 8 jobs green.
- **Final:** the commit that adds this record (the last of this pass). Its CI result is in the hand-over: a commit cannot cite its own run.
- **Branches.** Work is committed directly to `main`. The owner's standing instruction is "no new branches", so each
  removal batch is its own commit series with its own CI run instead of its own branch.

| Commit | Content |
|---|---|
| `cffa9ed` | K1: percentage budget resolved from the broker equity |
| `88c05d4` | Shared request budget for the shadow-comparison feed and the pipeline CLI |
| `8713b63` | L-05: Academy page made an honest placeholder |
| `c9ea50c` | K3: summary over every trade; decimal money with asset; cursor; blocker list; percent money view; `risk_level` case |
| `c7b9297` | K3: SSE ids, Last-Event-ID replay, durable tail across processes, envelope; portal subscribes |
| `62f2ee8` | J: IBKR gateway URL validated before the server requests it |
| `eca30d2` | J: Stripe webhook rejections counted and alerted |
| `9d19a81` | J: API outage and governance-violation alert sources |
| `14b6cb2` | J: admin MFA mandatory in production; enforced by the admin backend |
| `cd7f99c` | J: SMTP certificate verification; weak broker and KYC keys refused in production |
| `37ddcb3` | K4: unsafe configuration combinations refused at startup |
| `570984e` | I/7: runtime authority switch checked by behaviour, not source text |
| `129353a` | I/7: four unreferenced legacy strategy modules removed |
| `1ab866d` | Quarantine: 39 repaired, unknown-broker defect fixed (62 → 23) |
| `73aa5bc` | F6: cryptography 50.0.2, pillow 13.0.0, pyasn1 0.6.5, black 26.10.1 |
| `6f94cf0` | Live-flag test fixtures built past the new startup refusal; registry text for the new budget codes |
| the commit adding this record | Documentation: this record, the inventory, the quarantine register, the gates register, alerting, the security report |

**Working tree.** Three files still carry someone else's uncommitted change (section 14): `daily_feed.py`,
`platform_runtime.py` and `test_step3_platform_runtime.py`. They are neither committed nor modified.

---

## 3. Section I — removal and consolidation

The full evidence per batch is in [SECTION_I_REMOVAL_INVENTORY.md](SECTION_I_REMOVAL_INVENTORY.md).

| Batch | Status | What happened in this pass |
|---|---|---|
| 1 Unreferenced modules, scripts, dumps | **PASS** | `9fb120f` (197 files, 20,798 deletions) is in history. Not repeated. |
| 2 Retired backtest engine | **BLOCKED — owner (REM-01)** | Deletion plan ready (inventory). `app/backtest` has 6 files and 2,195 lines; it is still started by `main.py`, its list/detail/export routes are proxied, and the portal links them. It is not used by the CATI evaluator, Step 2 or research. |
| 3 Old shadow-trade subsystem | **BLOCKED — owner (REM-01)** | Deletion plan ready. `app/shadow` has 6 files and 1,763 lines; its importers are `shadow_routes.py`, a guarded hook in `multi_runner.py` and auth-hardening tests. The CATI `ShadowScheduler` does not import it; `trading_intelligence/config.py` only names it in a docstring. |
| 4 Callerless routers | **BLOCKED — owner (REM-01)** | Nine routers with no repository caller (inventory); external or operator callers cannot be ruled out from the code. |
| 5 TradingView signal path | **PASS** | Verified: no route, script or page remains, and `app/queue` holds only an untracked `__pycache__`. The `TRADINGVIEW_*` flags stay as forced-off safety guards; historical tables are kept. No quarantine entry refers to it. |
| 6 Legacy V2 ML scorer | **BLOCKED — conflicting governance rule** | Conflict report in the inventory: the governance map keeps `ml/scorer.py` "until M9", and the M9 rule is enforced by a test. Only the owner can lift it. |
| 7 Legacy runner | **PARTIAL — progressed** | Probes replaced by behaviour (`570984e`). `strategy/router.py`, `mode.py`, `timeframe.py` and `activation.py` removed (`129353a`). Remaining blockers are listed below. |
| 8 Duplicate execution glue | **DEFERRED — not duplicates** | The three `boundary_for` factories differ in authority, adapter and checks; merging any two would change a gate. The residual `build_plan` path is the maintenance cycle of every account without a manageable family, which is all of them at M0. Only a shared prelude could be extracted (inventory). |
| 9 Traditional broker adapters | **BLOCKED — owner (venue choice, H-019 D1/D3)** | Decision matrix in the inventory. |
| 10 News, events, Signal Center | **PARTIAL KEEP; rest owner** | Classification in the inventory. Calendars and blackouts that CATI reads are kept. |

**Batch 7: what still blocks the runner.**
1. `runner.py` hosts the non-production CATI cycle (`cycle_shadow`, `cati_dispatch`) and the legacy admin
   endpoints of `main.py` (about 18 `get_runner()` callers). 29 test files import it.
2. `multi_runner.py` holds the runtime-ownership lease in production. A `RuntimeOwnerLoop` migration is designed but
   not done (medium risk; it needs Linux CI and staging).
3. `effective_policy.py` and `market_snapshot.py` are CATI dependencies that live under `runner/`. They need a move
   with re-export shims.
4. `replay/cost_model.py` and `identity.py` are inside the research evaluation fingerprint and must not move.
5. Governance lists the V2 modules until M9 (Batch 6).

Two dead modules are left for the next Batch 7 commit:
- `decision/external_signal_gate.py`, a TradingView leftover;
- `strategy/activity_targets.py`.

Their tests were repaired in this same pass, so removing them now would have reworked those tests again.

---

## 4. Test quarantine

**62 → 23** (`1ab866d`). All 62 were re-run in a pristine export first; every one still failed.

| Disposition | Entries |
|---|---|
| Real defect fixed (an unknown broker now refuses with `BrokerResolverError` instead of a plain `ValueError`) | 1 |
| Retargeted to the real plan path (`process_trade_plan`), keeping every reason code | 8 |
| Updated for intentional interface changes (CATI sole engine, retired V2 schedulers, frozen strategy name) | 13 |
| Deleted as duplicates of `test_cati_sole_runtime` (which was extended to cover them) | 10 |
| Time-bomb fixtures dated relative to now | 8 |
| **Left: legacy-runner tests (Batch 7)** | **23** |

The master plan requires an empty list before Step 6. **It is not empty**, so J-07 stays open.
- The 23 entries assert legacy-runner source text, or a replay that installs a legacy strategy.
- They retire with the next Batch 7 steps.
- No test was skipped, broadened or added to the list.

Details: [QUARANTINE_REGISTER.md](QUARANTINE_REGISTER.md).

---

## 5. Section J — security, operations and legal

### 5.1 Previously completed fixes, re-verified

| Fix | State | Evidence and gap |
|---|---|---|
| Durable login rate limits | VERIFIED | `user-backend/app/api/auth.py:159-230`; admin `admin_auth.py`. Gap: per-IP limits are skipped for loopback addresses, which relies on uvicorn `--forwarded-allow-ips 127.0.0.1`. |
| Generic authentication errors | PARTIAL | Login, forgot and reset are generic. `/register` still answers 400 for an active address, and `/verify-email` says "already verified". Closing this changes the signup flow: product decision **L-07**. |
| Verification and reset e-mail | VERIFIED (+ fixed) | Certificate now verified (`cd7f99c`). Sending is still fire-and-forget, without retry. |
| Two-factor login | VERIFIED | TOTP with replay protection. |
| Mandatory keys | VERIFIED (+ fixed) | Short or placeholder `BROKER_SECRET_KEY` and short `KYC_ENCRYPTION_KEY` are now refused in production (`cd7f99c`). |
| Secure KYC files | VERIFIED | Fernet at rest; owner-only `no-store` downloads. Legacy plaintext files are still served. |
| Authenticated bot APIs | VERIFIED | A route walk refuses anything not explicitly public. |
| Validated outbound URLs | **FIXED** | The portal's IBKR test sent requests to a customer URL unchecked with `verify=False` (`62f2ee8`). |
| Per-user event filtering | VERIFIED | Also on the new replay and durable-tail paths (section 8). |
| Masked debug configuration | VERIFIED | `/debug/*` stays admin-only in production. |
| Production API documentation | VERIFIED | Off unless enabled. |
| Signed, idempotent Stripe webhooks | VERIFIED (+ alerting) | Rejections are now counted and alerted (`eca30d2`). |
| Safe stop and close retries | VERIFIED | Reduce-only, deterministic ids, bounded. |
| Kill switch; emergency flatten | VERIFIED (code and synthetic drills) | Not drilled in an authorized live environment (J-06). |
| Recovery before entry | VERIFIED (code) | Active when a platform database is configured. |
| Failure alerts | VERIFIED (+ 2 sources) | 24 evaluators. No delivery channel or on-call owner is configured (EXT-OPS-01). |
| Health-check timers | PARTIAL | The systemd health check probes the bot backend only; the operations tick now probes all three APIs (`9d19a81`). |
| Off-site backup scripts | VERIFIED (SQLite) | No scheduled `pg_dump` for the platform PostgreSQL database (follow-up). |
| systemd services | VERIFIED | Hardened units. `ProtectSystem=strict` and `CapabilityBoundingSet` are not set (follow-up). |
| nginx headers and CSP | PARTIAL | Headers present. The admin-host IP allow-list is commented out (operator); `style-src 'unsafe-inline'`; HSTS without `includeSubDomains`. There is no static test (follow-up). |

### 5.2 Findings F1–F9 and the new findings

| Id | State | Detail |
|---|---|---|
| F1–F5, F7, F9 | PASS | Unchanged since `6087e19`. |
| F6 dependencies | PARTIAL — upgraded | **Upgraded** in `deploy/constraints.txt` and `backends/requirements.txt`, after a full regression in a separate venv (user 881, shared 256, admin 88, and the 1,697 bot tests that touch encryption, credentials, PDFs and backups; all pass):<br>- `cryptography` 46.0.7 → 50.0.2;<br>- `pillow` 12.3.0 → 13.0.0 (used only through `reportlab`);<br>- `pyasn1` 0.6.4 → 0.6.5;<br>- `black` 25.12.0 → 26.10.1, dev only, with `pathspec` 1.1.1 and `pytokens` 0.4.1.<br>`pip-audit` of that environment after the upgrades: **25 advisories in 4 packages** (53 in the bot environment before):<br>- `starlette` 0.50.0 (10);<br>- `pip` 24.0 (12, venv tooling, not deployed code);<br>- `ecdsa` (2) and `python-jose` (1): no fix exists, accepted.<br>starlette 1.x needs FastAPI to move too, and the 12 `@app.on_event` handlers in the bot backend's `main.py` to become a `lifespan`. That is a planned migration, not a pin change. mt-bridge pins wait for the Batch 9 decision. |
| F8 authentication compatibility window | BLOCKED — operational cut-over | `AUTH_BEARER_COMPAT` defaults to true (user backend, admin backend, `env.production.example`). **No in-repository client needs it**: both frontends use cookies only; the bot backend receives bearer tokens from the user backend and has no cookie middleware; the MT connector uses pairing codes. Only the stale `tests/integration` (INT-01) send bearers. Out-of-repository API clients cannot be ruled out from the code, so the switch-off is an operator step after an observation window. |
| N1 IBKR gateway SSRF (portal) | **FIXED** `62f2ee8` | `url_guard`, public destinations only, https, no redirect, TLS verified in production. |
| N2 SMTP without certificate check | **FIXED** `cd7f99c` | |
| N3 Admin MFA switchable off in production; not enforced by the admin backend | **FIXED** `14b6cb2` | |
| N4 Weak broker and KYC keys accepted | **FIXED** `cd7f99c` | |
| N5 Account enumeration on `/register` and `/verify-email` | OPEN — product (L-07) | |

### 5.3 Historical administrator credentials

Administrator e-mail and password literals were committed to the public history in `db4580b` (2026-01-16),
`cc13da0` (2026-08-23) and `b35c198` (2026-09-07). The scripts were deleted in `c2eb3a5` (2026-10-07). No tracked
non-test file holds an administrator credential today. Values are not reproduced here.

**Deleting them from the current tree does not remove them from history: treat every account named there as
compromised until rotated.** This pass could not rotate anything: there is no access to a deployed database.

The operator procedure:
1. On each environment, run `python backends/user-backend/scripts/create_admin.py <email> --reset-password` for every
   administrator. The password comes from a hidden prompt or an environment variable, never an argument; the script
   revokes sessions and bumps `token_version`.
2. Run `python scripts/security_maintenance.py reset-admin-mfa <email> --issue-bootstrap`, then re-enrol MFA with the
   one-time code.
3. Delete any account that exists only because of those scripts.
4. Record the date and operator in EXT-SEC-02.

A history rewrite is not proposed: it would not revoke copies already fetched.

### 5.4 Section J launch checklist (J-01 … J-09)

Recorded permanently in [STEP5_STEP8_EXTERNAL_GATES.md](STEP5_STEP8_EXTERNAL_GATES.md), where it replaces the
provisional list. Summary:

| Item | Engineering | CI | Staging | External / elapsed | Owner | Accepted |
|---|---|---|---|---|---|---|
| J-01 Strategy certification (research test, demo run, 8 weeks live) | gates built | yes | — | no family passes; no demo run; no live weeks | promotion | **No** |
| J-02 All three exchanges certified; every beta position protected | harness, register, protection gate | yes | — | no venue certification run | permits | **No** |
| J-03 Daily shadow decisions match the certified rules | shadow comparisons recorded | yes | — | nothing is certified to compare against | — | **No** |
| J-04 Onboarding without operator help; positions, trades, fees, results, reasons, notifications shown | yes (section 12) | yes | not run | real demo journey not repeated | — | **No** |
| J-05 Stop, pause and risk changes take effect as documented | yes (section 12) | yes | not run | — | — | **Engineering only** |
| J-06 Independent assessment; admin MFA; protected sessions; kill switch and flatten drilled | MFA and sessions done | yes | drills not on staging | penetration test | authorized drill | **No** |
| J-07 Quarantine empty | not empty (section 4) | — | — | — | Batch 7 owner items | **No** |
| J-08 1,000-account load, PostgreSQL restore, off-site backups, staging, monitoring, on-call, runbooks | synthetic load and drills, runbooks | yes | no host | demo-account load | on-call person | **No** |
| J-09 Terms, privacy and risk disclosures approved; licensing per country; billing verified; live track record; support and status page | drafts, pages, help desk, status page | yes | — | counsel, licensing advice, live track record | billing activation | **No** |

---

## 6. Section K1 — risk sizing

**Path.** DAILY_TREND runs on the portfolio tail:
1. `production_execution.process_account` hands the account to `portfolio_runtime.dispatch_account`, and from there
   to `ConnectedPortfolioRuntime.process`.
2. `DailyTrendPipeline.broker_account` builds the targets with `targets.decide_targets`.
3. `PortfolioExecutionBoundary` authorizes them through `portfolio.authorize`, which applies the caps and the
   exchange filters, then calls `_increase` (executable re-checks, `set_leverage`, the order).

The legacy `risk_sizing.size_risk_based` serves only the frozen residual path.

| Formula element | Where | Result |
|---|---|---|
| budget = fixed, or % × equity | `portfolio_runtime.resolve_capital_budget` (new) → `daily_trend.py:470-473` | **Was wrong**: a percentage was read as USDT. Now resolved against the broker-confirmed equity with the preview's own `effective_budget` rule (`cffa9ed`). |
| budget ≤ free capital | margin factor, `portfolio.py:809` | Partial: orders are scaled to usable margin; the budget itself is not compared with free margin (deviation D3). |
| risk = risk_per_trade × budget | `targets.py:98`, frozen level risk under the 0.40 % clamp | Matches. |
| stop = 3·ATR20 / close, floor 5 %, research cap 40 % | `rules.py:84-89`, `spec.py:42` | Matches. |
| notional = strength × risk / stop | `targets.py:98` | Matches (strength overlay 1.0). |

**Cap order applied.**
1. Per-trade clamp.
2. Customer maximum: min(budget × leverage, `max_position_usdt`, `MAX_NOTIONAL_PER_SYMBOL`, fixed amount).
3. Position slots.
4. Open risk, proportional.
5. Leverage.
6. Margin.
7. Round down to the step, then the maximum quantity.
8. Refuse below the minimum quantity or notional; never raised to it.

The shared factors combine as a minimum, so the plan's order 1–7 holds for the result. Merging the system per-symbol
cap into step 2 only changes which cap is reported as binding.

**40 % and 15 %.** Separate concepts.
- The research cap (40 %) is part of the frozen rule.
- The execution ceiling (15 %) refuses new entries and adds: in `targets.py:99-103`, at the portfolio gate
  (`portfolio.py:661-663`) and at the live mark (`execution.py:948`).
- An entry is never clipped to fit.

**Protective stops.**
- An exchange-side stop is required after each fill, or the position gets a fail-safe close and the account is
  blocked.
- The ratchet only tightens.
- Replacements keep coverage.
- A partially filled order is cut when its stop cannot grow.
- Re-entry starts a new owned lifecycle.

**Test matrix.** Covered: small accounts, low free capital, existing exposure, multiple positions, minimums,
rounding, maximum position, open risk, leverage, large ATR, the 15 % ceiling, partial fills, slippage, rebalance,
stop and re-entry.
- Fixed and percent budgets on the portfolio path: **added** (`test_k1_budget_resolution.py`, 8 tests).
- Liquidation distance (D2), budget-based caps (D3), the single 15 % / 0.40 % value (D8), invalid strength
  (`DECISION_STRENGTH_INVALID`, 0 ≤ s ≤ 1 on the broker stage) and the missing decision bar / mark price
  refusals (`BROKER_POSITION_DECISION_BAR_MISSING`, `POSITION_PRICE_UNKNOWN`, now in the reason registry):
  **added** (`test_k1_sizing_gates.py`).

| Deviation | Severity | Needs | Detail |
|---|---|---|---|
| D1 percent budget read as USDT | HIGH | — | **Fixed** (`cffa9ed`). Under-sized only; never over-risked. |
| D2 no liquidation-distance or margin-mode gate on the portfolio path | MEDIUM | engineering (refusal only) | **Fixed**: `portfolio._gate` refuses an increase with `LIQUIDATION_TOO_CLOSE` when its leverage exceeds `1 / (2 d + LIQUIDATION_DEDUCTION)`; the leverage is chosen by margin mode (ISOLATED: the position's; CROSSED: the account maximum; not reported: the larger). A hedge-mode position is not increased (`POSITION_MODE_UNSUPPORTED`). Cannot fire at the frozen numbers (3.27 ≥ 3). |
| D3 account caps measured on equity, not the budget | MEDIUM | engineering (tightens) | **Fixed**: `AccountLimits.capital_budget`; the per-trade clamp, open risk and gross leverage are measured on `min(budget, equity)` and every exposure of the account counts. Margin stays the venue's available margin. |
| D4 trailing ratchet inert on Binance and BingX, and skipped on rebalance days | MEDIUM | venue certification | **Binance**: create-confirm-retire declared from the venue documentation (reduce-only stop beside the `closePosition` one, then the position-bound form back); trustworthy only after a certification run. **BingX**: still `REPLACEMENT_NOT_POSSIBLE_SAFELY` (no documented overlap; amend changes quantity only; cancel-and-replace can leave no stop). **Rebalance up**: carries the ratchet, sized at the wider resting stop. **Rebalance down**: the ratchet waits for the next close (needs a `pipeline.execution` change). |
| D5 legacy presets below the family risk are refused, not resized | LOW | **owner** | |
| D6 no `max_slippage_bps` on DAILY_TREND market entries | LOW | **owner** (frozen cost model) | |
| D7 initial stop anchored to the decision close, not the fill | LOW | document | Bounded by `_executable_risk_refusal`. |
| D8 the 15 % value has three sources | LOW | engineering | **Fixed (test)**: `test_k1_sizing_gates.py` holds `spec.py`, `SystemLimits.max_stop_loss_pct`, `risk_sizing.MAX_STOP_DISTANCE`, `AccountLimits` and the family's executable levels at one 15 % (and the 0.40 % ceiling at one value). `spec.py` is not edited. |
| D9 `ATR_RISK_CAP_EXCEEDED` cannot fire on the plan path | LOW | document / owner | The hard-risk chain hands PolicyEngine a fixed margin, which it reports (`REPORTED_FIXED_MARGIN_AUTHORITATIVE`) but does not cap. Portfolio sizing is risk-based and has its own caps (0.40 %, 15 %, open risk, leverage). The legacy ATR risk-cap gate is redundant there, not missing. The test keeps it covered. |

No ceiling was changed.

## 7. Section K2 — database

Three schema mechanisms exist:
- **legacy:** an unversioned, idempotent `migrate()`;
- **CATI:** `cati_*_schema` modules, `CREATE IF NOT EXISTS`;
- **platform:** versioned migrations v1–v69 in `shared_lib/persistence/sql`.

| Entity | Physical table | Gaps |
|---|---|---|
| Bot instances | `bot_instances` | Money columns are REAL. The `deploy_request_id` index is not unique (idempotency is in the deploy transaction, tested). `strategy_version` is the literal `"1.0.0"`. |
| Consent | `deployment_consents`; live consent in `golive_events` | `request_id` not unique; no foreign keys. |
| Plan grants and subscriptions | `subscriptions`, `billing_operator_grants` | Two active grants per user are not prevented by the schema (the code supersedes). The grant POST has no idempotency key. |
| Equity snapshots | `account_equity_snapshots`, `account_equity_daily` | REAL money. The API now returns decimal strings (section 8). |
| User events / notification outbox | `user_events`, `notification_outbox` | No pruning of delivered outbox rows. |
| Strategy targets | none: stored in `cati_family_decisions.document` | The decision record is hash-chained and append-only; a separate table is an owner choice. |
| Shadow comparisons | `cati_shadow_comparisons` | Idempotency by deterministic id; REAL difference column. |
| Account brakes | `cati_brake_state`, `cati_brake_events`, `cati_brake_restarts` | REAL equity columns. |
| Exchange certifications | `cati_adapter_cert_events/_checks/_actions` | Hash chain. The 90-day cap is now enforced (section 9). |
| Account leases | `cati_account_leases`; `platform_account_assignments` (v1) | |
| Durable jobs | `platform_jobs`, `platform_job_attempts`, `platform_order_intents` (v2) | Exact money (NUMERIC(38,18)). Jobs are kept forever. |
| Live approvals | `golive_events` (v10) | Hash chain, append-only. |

Exact money types exist only in the platform migrations. Moving the CATI and legacy REAL money columns to exact
decimals means rebuilding tables, which is an owner decision. The legacy runtime on PostgreSQL already stores money as
NUMERIC through the adapter (Step 5.1).

**Additive unique indexes** (deploy request, consent request, one active grant, job attempt) are prepared, not
applied. A production copy must first be checked for duplicates, which needs EXT-DB-01.

**Legacy tables.** All of them still have active readers or writers: trade fills, positions, equity, TradingView, ML,
news and signals (the K2 audit is in the inventory). **None is dropped.** A destructive migration needs the code removed
first, an archive or export of financial evidence, and the owner's approval.

## 8. Section K3 — API contracts

| Contract item | Implementation | State |
|---|---|---|
| `POST /api/v1/auto-pilot/preview`, `/deploy` | user backend proxy → bot `/api/v1/auto-pilot/preview`, `/deployments` | PASS. All six blockers emitted. `risk_level` is now case-insensitive. Open for the owner: the legacy deploy body (used by `pages/AutoPilot.tsx`) bypasses the six blockers; live entries are still refused at execution. Also open: whether `BUDGET_EXCEEDS_BALANCE` should compare with equity or available balance, and that preview lists `RISK_NOT_ACKNOWLEDGED` as a requirement, not a blocker. |
| Preview money fields | `deployment_service.evaluate` | PASS for the fields; decimal strings. The asset is named on `money` only, not on `account.*` (follow-up). |
| Bot payload | `deployment_service.bot_payload` | PASS. Budget and maximum position are decimal strings with an asset. A percent budget now has a money view, with `money_basis` from the engine's equity reading. |
| `GET /cati/bots/{id}/status` | `cati_read_model.bot_status` | PASS. `eligibility.blocker_reasons` lists every blocking reason, explained. |
| `/positions` | `cati_read_model.positions` | PASS, with different names (`quantity`, `opened_at`, `protection.state`). Values are the exchange's own strings. |
| `/trades` | `cati_read_model.trades` | PASS. Cursor (`cursor` / `next_cursor`) next to `page`; decimal strings with asset; fees, funding, exit reason, R multiple. |
| `/summary` | `cati_read_model.summary` | **Bug fixed**: it was built from the newest 200 trades only. Now every closed trade; windows today / 7 d / 30 d / all; never adds different assets together. |
| `/equity` | `cati_read_model.equity` | PASS. Decimal strings with asset. Not yet shown by the portal. |
| Runtime status proxy | `cati_proxy.runtime_status` | PASS. |
| Ownership | `_owned_bot` (404 for someone else's bot), deploy ownership on both sides | PASS. |
| SSE events | `api/events.py`, `observability/user_events.py` | PASS. All eight types, each with `id`, `event_id`, `event_type`, `timestamp`, `user_id`, `bot_id`, `broker_account_id`, `environment` and `payload`. Last-Event-ID replay is owner-scoped. A durable tail delivers trading-process events to API-process subscribers. The portal subscribes and keeps reconnecting. |
| Operator: plan grants and revocation | user backend `admin_billing` | PASS (API). No admin UI. |
| Operator: emergency status, kill switch, flatten | user backend `admin_emergency` → bot `emergency` (minted short-lived token) | PASS. |
| Operator: exchange certification readback and initiation | the register and runner exist; no HTTP route | OPEN — owner: initiation needs an OperatorPermit by design. |
| Operator: live switch and per-account eligibility | bot `api/golive.py` | Present in the bot backend; **not reachable from the portal** (the forwarder is pinned to `/api/v1/admin/emergency`). The gate always refuses in this build. Exposing it is an owner decision. |

## 9. Section K4 — configuration

| Plan name | Real name and treatment | Result |
|---|---|---|
| `BILLING_ENFORCED` | same; false by default and in every deploy file | PASS. Turning it on is the business owner's step (EXT-BILL-01); it is deliberately not locked in code. |
| `DEMO_ORDER_SUBMISSION_ENABLED` | same; demo orders also need a fresh adapter certification at submission | PASS. |
| `LIVE_ORDER_SUBMISSION_ENABLED` | same; false everywhere | **Now refused at startup** in a build with `BUILD_PERMITS_LIVE=False` (`37ddcb3`); one build constant for gate and profile. |
| `EXCHANGES_ENABLED` | none; the supported set is the `SUPPORTED_BROKERS` constant, and certification is checked at submission | PARTIAL. A deploy-time "venue not certified" blocker is the follow-up. |
| `CERTIFICATION_MAX_AGE_DAYS` | `CERTIFICATION_VALIDITY_DAYS = 90` (code constant) | **Now enforced against the stored record too** (`37ddcb3`). |
| `STRATEGY_VERSION` | none; `STRATEGY_NAME == FROZEN_STRATEGY` in production; family versions are code-defined and registered | PARTIAL. `bot_instances.strategy_version` is an unvalidated literal; replacing the name check with a family-version check is an owner choice. |
| `SIGNAL_INTERVAL` | exists, **read by nothing** | Documented as unread (`37ddcb3`). The cadence is each family's decision schedule. Retention runs inside `ShadowScheduler.tick` after `run_due`, in a protected file (section 14). |
| `ACCOUNT_RECORDER_PRUNE_DAYS` | `PruneRule("account_equity_snapshots")`: 90 days, minimum 30, rolled up into daily rows | PASS (constant). |
| `DATABASE_URL` | `DATABASE_URL` + `PLATFORM_DATABASE_URL` | **The production fallback to `data/bot.db` is now refused** (`37ddcb3`). Services do not share incompatible databases; the topology choice is EXT-DB-01. |
| `WORKER_LEASE_SECONDS` | `BROKER_ASSIGNMENT_TTL_MS` (60 s) and `BROKER_ASSIGNMENT_FENCE_MARGIN_MS` (15 s) | **Now validated** (`37ddcb3`): positive TTL, margin below the TTL, pool size 1–64, each with its own refusal code. |

## 10. Section L — decisions

**Previously approved decisions, verified in the code.**
- The customer chooses the account, budget and risk level; the per-position amount is an optional cap (the
  deployment contract).
- No custody: customers trade on their own exchange accounts with their own keys.
- The target scale is 1,000 accounts (Step 5 load evidence).
- CATI is the only engine (the runtime authority switch, now checked by behaviour), and new strategies are
  registered families.
- CATI's gates stay enforced.
- Billing stays off.

**Open decisions.** None was decided here.

| Id | Decision | Current rule kept | What the owner needs to decide it |
|---|---|---|---|
| L-01 | Raise the per-trade risk from 0.40 % to 0.75 % | 0.40 % (code ceiling, `system_limits.py`, `activation.py:71`, the spec's executable scenario) | The 0.75 % evidence: drawdown and ruin under the frozen DAILY_TREND rules, per risk level. A new mandate, since the executable scenario is frozen. |
| L-02 | A second admission route for slow rule-based families | The CATI research policy (Mandate 004 standard) | A written policy change, registered through the governance log. H-018 stays uncertified either way. |
| L-03 | Let the failed residual-momentum family trade on demo as test traffic | Refused: a FAILED family has no order authority | Use the operator-scoped certification harness for infrastructure testing (it carries no strategy). |
| L-04 | Removal authority for Batches 2–4, 6, 9 and 10 | Nothing deleted | Per batch: approve the deletion plan in the inventory (REM-01; the M9 rule for Batch 6; the venue choice for 9; product surfaces for 10). |
| L-05 | Academy and DeveloperSettings pages | Both are now honest "not available yet" placeholders (`8713b63`) | Keep as placeholders or remove from the navigation. |
| L-06 | Traditional markets | H-019 decisions D1–D16 are open (`research/trend_v2_tradfi/SPEC.md`); nothing registered or frozen | D1–D16; then freeze and register H-019. |
| L-07 (new) | Account enumeration on signup | `/register` and `/verify-email` reveal an existing address | A non-revealing signup (same answer, e-mail to the existing owner) changes the flow and needs mail sending. |
| L-08 (new) | Order-spreading market-entry policy | Every production entry is refused (EXT-LIQ-01) | Approve a single-account policy permitting market entries under the conservative cap. |

## 11. Section M — progress reconciliation

The Section M text reported in early October 2026 that Steps 1 and 2 "had not started". That was true when written.
The repository shows:

| Date | Commits | What happened |
|---|---|---|
| 2026-08-23 | `cc13da0` | Codebase prepared for a full audit |
| 2026-09-07 → 09-12 | `7f2bca6` … `5f297f2` | Post-audit correctness fixes; threshold-policy audits |
| 2026-09-24 → 10-03 | `7bb89f9` … `bf23a95` | CATI Sections 5–27; multi-asset phases; Mandate 003 frozen, run once, failed its economic gates |
| 2026-10-05 → 10-06 | `afbd41e` … `9e0506e` | Demo certification harness |
| 2026-10-08 → 10-09 | `b86c076` … `8dcf1c1` | **Step 1** (portal to engine): conditional closure, first remote CI |
| 2026-10-09 | `e897f88` … `75435cd` | **Step 2**: research registry; Mandate 004 frozen; dataset; evaluator; official run; the holdout is reserved and unopened |
| 2026-10-15 → 10-16 | `cda273a`, `6372b83`, `78d0c41` | **Steps 3/4**: CATI pipeline, gates, broker portfolios; Section H validation |
| 2026-10-17 | `221b2bb`, `d16ebe1`, `78a14e4` | Steps 5–8 status and Section I inventory; **Batch 5 removed** |
| 2026-10-30 | `8811546` … `ef22dba` | **Steps 5–8 engineering closure** (PostgreSQL runtime, worker assignments, jobs, order budget, security, operations, deployment) |
| 2026-10-31 | `cffa9ed` … this record | **This pass**: Sections I–L (17 commits) |

**Certification gates still open.**
- No family is certified (H-018 uncertified; the residual family failed Mandate 003; the Mandate 004 holdout is
  unopened).
- No venue certification.
- No live trading.

The open gates are all in [STEP5_STEP8_EXTERNAL_GATES.md](STEP5_STEP8_EXTERNAL_GATES.md).

## 12. Customer journey (Section 32)

Each step runs through the real code with fake exchanges and throwaway databases. No exchange order was sent.

| Step | Covered by |
|---|---|
| Register, authenticate, 2FA | `user-backend/tests/test_auth_hardening.py` (login, lockout, TOTP, reset), `test_section_h84_*` |
| Consent | `test_onboarding_step1.py`, `test_legal_documents.py`; deploy consent in `test_deployment_contract_step1.py` |
| Connect an exchange account | `test_broker_reconnect.py`, `test_account_http_flow.py`; the IBKR destination in `test_ibkr_gateway_ssrf.py` |
| Preview, budget, risk | `test_deployment_contract_step1.py` (all blockers, percent budget, case-insensitive level) |
| Deploy a bot | `test_step1_closing_synthetic.py::test_d_*` (409 on a second bot) |
| Runtime state, target portfolio, shadow decisions, account status | `test_step1_closing_synthetic.py::test_e_f_*`, `test_cati_read_model*.py`, `test_step3_connected_portfolio.py` |
| Events | `test_user_events.py`; `test_sse_replay_and_durable_tail.py` (replay, other process, isolation) |
| Pause, resume, stop, restart | `test_step1_closing_synthetic.py::test_h_*` |
| Change risk where allowed | `PATCH /bot-instances/{id}`, validated through `resolve_effective_bot_policy` before saving |
| Emergency control | `test_emergency_controls.py` (admin-only, kill switch before entry, flatten preempts) |
| Recovery after restart | `test_step1_closing_synthetic.py::test_h_*`, `test_worker_takeover.py`, `test_runtime_jobs.py` |
| Another customer sees nothing | `test_step1_closing_synthetic.py::test_f_*`, `test_api_auth_hardening.py` (SSE), the replay isolation tests |

Not run: a browser journey (no browser automation exists in the repository), and the Binance demo journey of Step 1
(it needs the operator's demo account and certification).

## 13. Research integrity

No file under `research/` or `docs/research/` changed in this pass:
`git diff --stat ef22dba..HEAD -- research docs/research` is empty. The evaluation fingerprint
`research/governance/mandates.source_fingerprint()` is unchanged at
`59a3b6689f63bd8e61c7db5f9251bc58162d0bdb5d37a9c866478f76dc50a7aa`. Recomputed on a pristine export of `73aa5bc`: identical.

## 14. The uncommitted change

The three files with someone else's uncommitted change are neither committed nor modified.
- As it stands it breaks `ConnectedPortfolioRuntime._market`: it removes `budgeted_archive_get`, which three callers
  import.
- Its intent, sharing the request budget, is now met for the pipeline CLI and the shadow-comparison job (`88c05d4`),
  with a guard test.
- The only call site left unbudgeted is the `daily_feed` operator CLI, in one of the protected files. The isolated
  repair is to pass `http_get=budgeted_archive_get(db)` at `daily_feed.py:1056`.
- The owner of the change should discard it, or rework it into that one line.
- Also waiting on that file: running retention independently of `run_due` in `ShadowScheduler.tick` (K4 maintenance).

## 15. Regression and CI

**How the regression was run.**
- Full regression of `129353a` (every code commit up to the Batch 7 removal) in a pristine `git archive` export, with
  the CI job environment and the quarantine plugin.
- PostgreSQL 18 on the workstation for the both-dialect tests and the legacy-runtime job.
- Baseline: CI run 41134191747 on `ef22dba`: bot 7,705 passed and 0 failed; legacy runtime on PostgreSQL 1,186
  passed.

| Suite | `129353a` | Cause | After `6f94cf0` |
|---|---|---|---|
| Bot backend | 7,693 passed, **18 failed, 7 errors**, 36 skipped, 54 xfailed (57 min) | 23: the production-execution, demo-execution and emergency fixtures built a production profile with the live flag on, which the new K4 startup refusal rejects. 1: the reason registry lacked the new budget codes. 1: a lease-timing drill missed while another test run shared the machine (passes alone, and on its own in the original CI) | the 24 pass (180 tests in those files on SQLite; 68 on legacy PostgreSQL) |
| Legacy runtime on PostgreSQL | 1,167 passed, **12 failed, 7 errors**, 11 deselected | the same fixture cause | pass |
| User backend | exit 0 | — | 881 passed (also on the upgraded dependencies) |
| Admin backend | 88 passed, 6 xfailed | — | the 6 quarantined repaired in `1ab866d` |
| Shared library | 256 passed | — | 256 passed |
| Root unit tests | 5 passed, 2 xfailed | — | the 2 repaired in `1ab866d` |
| User frontend | — | — | eslint 0 errors (210 warnings, unchanged); 91 tests; production build OK; `npm audit` 0 |

**Later commits were verified separately.**
- `1ab866d` (quarantine): every repaired test passes without the plugin in a pristine tree (494 bot tests in the
  touched files, admin 66, root 6).
- `73aa5bc` (F6): the separate-venv regression of section 5.2.
- `6f94cf0`: above.

**Exact-commit CI.** The GitHub Actions run on the final commit (all 8 jobs, Linux) is recorded in the hand-over.
