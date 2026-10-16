# Step 3/4 completion audit — 15 October 2026

## A. Executive outcome

Implementation commits: `2bc06fb5775c692fabfd01f1233fe55aabbf3a1a`,
`cda273aab6dfb0db5ecefbe08c358da57f5075e6`, `49b81ae`, `6e4d725`, `6372b83`, `20527f3`, `61d3199` and `32b0278`.
The runtime now schedules independent shadow cycles and account streams. No
strategy is certified, no family was promoted, and no trading was enabled.
The remaining runtime handoff, shared backend budget and concrete certification
harness have been implemented and locally verified. Full operational closure
remains BLOCKED by genuine strategy/venue certification and elapsed shadow evidence.
Overall engineering acceptance also requires exact-final-commit Linux CI.

This report supersedes older component-document statements about missing runtime
scheduling, concrete stream transport, rules-refresh integration and replay evidence.

## B. Step 3 requirements

The [implementation-status register](STEP3_STEP4_IMPLEMENTATION_STATUS.md) lists all 15 requirements and their
component tests. The canonical broker cycle now routes authorized families through
ConnectedPortfolioRuntime and PortfolioExecutionBoundary. It reconstructs holdings
from fresh broker truth, records a separate executable decision, and resumes that
same decision on retry. The independent shadow scheduler retains no order capability.
New runtime tests cover ownership, cancellation, credential
rotation, account removal and daily evaluator comparison. Cycle writes are fenced
against ownership takeover. A configured executable callback is rejected instead
of falsely recording a dispatch that never happened.

## C. Step 4 requirements

Rows 4.1–4.6, evidence and commits are recorded in the same requirement register.
The neutral contract, three adapters, certification register, rules cache, stream
manager and shared budget have synthetic coverage. AccountSession composes them;
VenueRuntime schedules read-only reconciliation independently of strategy work.
Adapter versions are 1.1.0. The physical HTTP seam now charges runtime client
construction, retries, maintenance, stream keys and public observations. Unknown
Binance endpoints fail closed. Its 100-unit reservation per registered endpoint is
a conservative local policy; it is not a verified endpoint weight or throughput claim.
The contract and budget now live in shared_lib/broker, with compatible bot imports.
Resolved auth binds the database before client construction in either backend.
Direct production validation clients also use the shared allowance. Production
factory calls without a budget database fail closed. Private-stream handshakes
have a separate shared five-minute window; the Bybit configured usable allowance
is below its documented 500 connections. Other venues retain a conservative local
connection envelope pending authenticated certification. Production execution checks persisted version-specific
certification directly, rather than trusting an injected callback.

## D. Architecture

The shadow scheduler registers identities at M0, processes due cycles, uses bounded
account workers with durable leases and emergency checks, and runs bounded archival
retention. Shutdown joins in-flight work. Daily model comparisons are written before
marking the cycle complete, making interrupted comparisons retryable.

The separate venue runtime uses four workers, rotates sessions on credential-version
changes and closes them on account removal or loss of runtime ownership. Instrument
rules refresh on startup and when due. CATI_EGRESS_SCOPE identifies shared egress;
the default is one conservative shared scope, never a budget per customer.
The executable lane uses measured broker equity within 60 seconds after consecutive
UTC closes and a complete income ledger for transfers. It preserves high-water
marks and refuses missing closes. It also carries forward intraday loss latches,
weekly/monthly drawdown, loss-streak cooldown, transfer/margin ambiguity, capital
allocation, allowed symbols, and daily-entry caps. Manual holdings still count
toward account risk but are not adopted into a family book.
Owned position attribution starts at the latest filled opening, so a native stop
followed by reentry does not inherit the previous position's fills. Before an
increase, the boundary checks executable price-to-stop risk against the approved
reservation, including earlier partial fills before a market fallback. Limits use
their worst permitted price; markets use a fresh mark. The 15% entry stop limit is
also checked at that executable price, including when quantity rounding leaves risk
headroom. This check does not guarantee
the eventual market fill price or eliminate gap/slippage risk.

The raw CREATE gate checks a call-scoped permit against a SUBMITTING intent and
its durable reservation. Account, user, environment, quantity, order type, limit
price and client ID must match. Certification orders use a separate scoped permit
against the pending action of an explicit, unexpired DEMO run. Neither permit is
an environment flag, and both reset on failure or process interruption.

WebSocketTransport uses TLS, bounded receive queues and messages, nonblocking reads,
ping/pong handling and sanitized errors. Private frame logging is disabled. Tests
include an actual local TLS/WebSocket server. Binance uses the current private
route and renews listen keys; Bybit authenticates before subscribing. BingX LIVE
has documented listen-key setup; its unverified VST stream host remains refused.

## E. Governance

Real families remain M0 and uncertified. DAILY_TREND/H-018 remains development-valid
with holdout unopened. H-015 remains economically failed. Admin APIs and UI enforce
authorization and audit promotion/revocation/emergency actions. No test-only gate
override was added to production. Legacy execution tests retain their explicitly
scoped test fixtures; production refusal tests exercise the real gates.

## F. Risk and safety

The 0.40% engine per-trade ceiling, 15% maximum entry stop, frozen rules and four
specialist invariant are unchanged. Reservation, multi-position, stop-replacement,
brake, unknown-outcome and restart tests use temporary databases and synthetic venues.
The production portfolio boundary requires reconciled stream health for increases;
protective/reducing recovery does not depend on that entry-health check.

The migration fixture is generated by the actual baseline `75435cd` migration
module, not today's migration with a call disabled. Its SQL hash and source hash
are recorded. Tests preserve legacy reservations and recover interrupted upgrades.

## G. Research reconciliation

`evidence/step3/development_shadow_reconciliation.json` reports 1,827 development
days, 2020-01-01 through 2024-12-31. Every model account matched the recomputed
frozen evaluator, and the primary account matched the committed development
reference. Largest relative difference: 0.0. Holdout rows read: 0. Orders sent: 0.
This is replay evidence, not elapsed operational shadow time or strategy approval.

## H. Exchange certification

| Venue | Synthetic contract/transport | Real execution certification |
|---|---|---|
| Binance | Passing | BLOCKED: scoped real certification not performed |
| Bybit | Passing | BLOCKED: scoped real certification not performed |
| BingX | Passing; VST stream refused | BLOCKED: scoped real certification not performed |

Public BingX metadata reads returned HTTP 200/code 0 from both hosts: 1,262 LIVE
contracts and 1,209 VST contracts. Coverage differs and cannot be assumed identical.
Evidence with timestamps and response hashes is in `evidence/step4/bingx_public_metadata.json`.
No credentials were used for those reads.

Primary sources checked:

- [Binance private routing](https://www.binance.com/en-NG/support/announcement/detail/ebf9b0aa9eca4ff3804eef6fb09ba32a)
- [Binance listen-key lifecycle](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/user-data-streams)
- [Bybit authentication](https://bybit-exchange.github.io/docs/v5/ws/connect) and [demo service](https://bybit-exchange.github.io/docs/v5/demo)
- [BingX private-stream reference](https://github.com/bingx-api/api-ai-skills/blob/main/skills/swap-ws-account/api-reference.md)
- [Bybit rate and connection limits](https://bybit-exchange.github.io/docs/v5/rate-limit)
- [BingX published rate-limit update](https://bingx.com/en/support/articles/31103871611289)
- [Websockets 15 transport API](https://websockets.readthedocs.io/en/15.0.1/reference/sync/client.html)

## I. Test results

- Focused original Step 3/4 suite: 1,443 passed, 3 skipped.
- Final exchange suite after stream integration: 589 passed.
- Regression fixes: 204 passed; runtime lifecycle/parity: 25 passed;
  daily-audit/session tests: 8 passed; actual-baseline migration: 2 passed.
- Admin backend: 88 passed, 6 existing xfailed. Shared: 96 passed.
  Root: 4 passed, 3 existing xfailed. User backend completed without test failures.
- Both frontend lint, unit tests (admin 21, user 41) and production builds passed.
- Ruff correctness rules passed. No quarantine entries were added.
- Initial broad Windows bot run: 6,896 passed, 16 failed, 5 skipped,
  106 xfailed, 2 xpassed. Implementation regressions were repaired and retested.
  Three failures inspect the workstation's existing `.env` and assume paper mode;
  that configuration was preserved. Linux CI is the authoritative clean-environment
  full-suite result; its exact status is recorded separately, not inferred here.

Additional follow-up: 320 tail/security/performance cases passed with two existing
skips; 42 targeted handoff/harness/rate-budget cases passed; 20 latest
stream/session/portfolio cases passed; shared suite 96 passed plus three new
shared-budget cases; user backend completed successfully. The 641-pass exchange
run exposed two old requests.get mocks after transport injection; both were moved
to Session.request and all 35 production-configuration tests then passed.

Follow-up validation: 53 HTTP/feed/scheduler tests passed (including raw-client
constructor admission and no adapter double charge); 48 account/factory/emergency
tests passed after fixing injected-factory handling. The local real-HTTP suite
also covers redirects, cross-account 429 closure, lost acknowledgements,
pre-send interruptions and signed clock skew.

Representative actual commands (repository root, `backends/venv` Python,
isolated TEST database and paper execution; no production database):

```powershell
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/exchange backends/bot-backend/tests/trading_intelligence/test_step3_connected_portfolio.py backends/bot-backend/tests/test_cati_production_config.py -q --disable-warnings --maxfail=2
# 641 passed; 2 old transport mocks failed. They were corrected at Session.request.
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/test_cati_production_config.py -q --disable-warnings --maxfail=2
# 35 passed after that correction.
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/trading_intelligence/test_step3_connected_portfolio.py backends/bot-backend/tests/exchange/test_certification_harness.py backends/bot-backend/tests/trading_intelligence/test_step3_execution_tail.py backends/bot-backend/tests/test_cati_section25_security.py backends/bot-backend/tests/trading_intelligence/test_step3_performance.py -q --disable-warnings --maxfail=3
# 320 passed, 2 existing skips.
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/exchange/test_stream_transport.py backends/bot-backend/tests/exchange/test_account_session.py backends/bot-backend/tests/trading_intelligence/test_step3_connected_portfolio.py -q --disable-warnings --maxfail=2
# 20 passed.
.\backends\venv\Scripts\python.exe -m ruff check backends/bot-backend/app backends/user-backend/app backends/shared/shared_lib --select E9,F63,F7,F82
# Passed.
```

Component-directory commands: `../venv/Scripts/python.exe -m pytest tests -q
--disable-warnings --maxfail=3` completed successfully in `backends/user-backend`
and passed 96 tests in `backends/shared`. The subsequently added shared factory/
IP/UID cases passed with `python -m pytest tests/test_shared_request_budget.py -q`:
3 passed. CI runs the full component suites with the existing quarantine plugin;
its totals and exact final SHA are reported with the final Actions result.

Counts overlap and must not be summed into a unique-test total.

Final risk/lifecycle correction `32b0278` was checked with:

```powershell
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/trading_intelligence/test_step3_multi_position_execution.py backends/bot-backend/tests/trading_intelligence/test_step3_connected_portfolio.py backends/bot-backend/tests/trading_intelligence/test_reason_registry.py -q --disable-warnings --maxfail=2
# 91 passed in 266.50s; collected before the additional fallback test was added.
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/trading_intelligence/test_step3_multi_position_execution.py -k 'fallback_accounts' -q --disable-warnings --maxfail=1
# 1 passed, 64 deselected in 3.71s; partial fill plus fallback risk accounting.
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/trading_intelligence/test_step3_connected_portfolio.py -q --disable-warnings --maxfail=2
# 16 passed in 7.95s, including native stop then reentry lifecycle attribution.
```

A final follow-up also enforces the 15% stop-width limit at executable price.
The combined price, fallback and stop-width selection passed 7 tests, 59 deselected
in 8.06s with `python -m pytest backends/bot-backend/tests/trading_intelligence/test_step3_multi_position_execution.py -k 'quote or limit_price or fallback_accounts or fifteen_percent or falls_back' -q --disable-warnings --maxfail=1`.

The canceled `dc95cfa` run's partial log exposed nine account-scoped test failures
before cancellation. Reproducing the affected file showed legacy fixtures omitted
the now-required production budget database and mocked the pre-budget HTTP seam.
Those fixtures now bind a temporary budget database and mock Session.request;
three new cases prove that unbound production construction is refused before
any builder is called. No production gate or quarantine was changed.

```powershell
.\backends\venv\Scripts\python.exe -m pytest backends/bot-backend/tests/test_cati_account_scoped_execution.py backends/bot-backend/tests/test_cati_production_config.py backends/bot-backend/tests/exchange/test_account_session.py -q --disable-warnings --maxfail=2
# 54 passed in 12.32s after fixture correction, including all three venue profiles.
```

The same isolated TEST/paper environment was used. Correctness lint, whitespace,
added-line secret-pattern checks and unchanged research/deployment/quarantine paths
were verified before pushing. The final Actions run validates the combined suite.

## J. Performance

Synthetic Windows measurements, two cycles each, warmed-up cycle:

| Accounts | End-to-end ms | Account-stage ms | Statements | Connections |
|---:|---:|---:|---:|---:|
| 1 | 101.3 | 2.0 | 45 | 1 |
| 10 | 77.1 | 16.7 | 288 | 1 |
| 100 | 445.7 | 320.9 | 2,718 | 1 |
| 1,000 | 4,831.4 | 4,674.1 | 27,018 | 1 |

The JSON also records memory, queue depth, feed requests and recovery. These are
driver measurements, not authenticated venue throughput or Linux scale certification.
`coordination_comparison.json` compares sequential and 1/2/4/8 workers, with and
without durable leases, on the same synthetic accounts. No production latency SLA
is claimed from this workstation.

## K. Git and CI

Baseline `75435cd` equalled origin/main and had green Actions run 37960746421.
Implementation `2bc06fb` was pushed normally to main. Actions run 38683547780
tested that exact implementation commit: 6,909 bot tests passed and one table-growth
assertion failed because inserts fitted in existing SQLite pages. The corrected
test distinguishes page allocation from payload size. Run 38788716818 tested `cda273a`: 6,916 bot tests passed and three failed. Two
signing fixtures had bypassed constructor HTTP injection; the timing assertion
was stricter than independent four-decimal rounding. The corrected fixtures and
rounding bound passed in the 320-test regression. Subsequent commits require their
own exact-commit CI result.
The final response reports the final pushed commit and observed Actions status. No force-push, deployment, runtime restart or production migration
was performed.

## L. Remaining gates

Engineering corrections completed locally:

1. Canonical broker-backed portfolio handoff, including restart, reservations,
   protection, actual-close brakes and raw transport authorization.
2. Shared backend HTTP budgets, factory and direct-client construction coverage,
   public observation, UID-specific rate refusal and per-IP stream connection budgets.
3. Concrete CertificationHarness wired to HTTP faults and an independent, bounded,
   one-shot external reduction. No real certification run was performed.

Final reporting correction `61d3199`: portfolio refusals, deferrals and recovery
blocks now have registered customer reasons; a refused batch is not reported as
healthy processing. The expanded reason-registry and handoff tests passed 27 cases.
Correction `32b0278` adds executable-price reservation checks and restarts owned
position attribution after a genuinely filled reentry; the above regression evidence
covers both corrections.

The final remaining engineering gate is green exact-final-commit Linux CI. Local
component evidence does not substitute for it. Existing known failures remain
explicitly quarantined by the unchanged repository list; no new quarantine was added.

External gates:

- Research certification and governance promotion; no bypass or owner request to
  trade can substitute for the frozen research gate.
- Owner-scoped account/environment/size authorization for real venue certification.
- Verify BingX VST private-stream routing and capture authenticated venue frames.
- One-week/four-week continuous shadow evidence on a suitable host: DEFERRED
  INFRASTRUCTURE, not simulated by replay.

## M. Step 5 readiness

The reusable scale harness and evidence are available. Full Step 5 deployment
readiness is not claimed until the engineering gates above, clean exact-commit CI,
and operational shadow requirements are satisfied. Frozen research artifacts and
the unopened holdout remain protected.

## N. Exact next action

Verify exact-final-commit Linux CI, then use the carry-forward register and
Step 5 handoff for an authorized continuous shadow host and owner-scoped venue
certification. Gather genuine elapsed evidence. Keep all strategy order paths closed until
research certification, governance and venue certification independently permit them.

Operational gates and ownership: [carry-forward register](STEP3_STEP4_CARRY_FORWARD.md). Scale preparation: [Step 5 handoff](STEP5_HANDOFF.md).
