# Step 3.11: operating family governance from the admin console

Status on 2026-10-09: implemented and tested on Windows. Not run on Linux. Exercised in a browser only against
local stand-ins of the two backends over a throwaway database; never against a deployed backend and never with
a real admin sign-in. No family was registered, promoted or changed in any real database. Trading stays shadow
only: no flag, limit or phase was changed.

| Part | File |
|---|---|
| Governance API (engine) | `backends/bot-backend/app/api/family_governance.py` |
| Proxy (main backend) | `backends/user-backend/app/api/admin_governance.py` |
| Proxy tests | `backends/user-backend/tests/test_admin_governance_proxy.py` |
| Admin pages | `frontends/admin-frontend/src/pages/admin/FamilyGovernance.tsx`, `FamilyGovernanceDetail.tsx` |
| Confirmation dialog | `frontends/admin-frontend/src/components/admin/governance/GovernanceActionDialog.tsx` |
| Page logic (tested) | `frontends/admin-frontend/src/utils/familyGovernance.ts` |
| Page logic tests | `frontends/admin-frontend/src/tests/family_governance.test.ts` |

Run the tests:

```
cd backends/user-backend && ../venv/Scripts/python.exe -m pytest tests/test_admin_governance_proxy.py tests/test_admin_emergency_proxy.py -q
cd frontends/admin-frontend && npm test
```

## 1. How a request travels

The admin console never talks to the engine directly. Every call goes to the main backend at
`/api/admin/governance/...`, which does two things before anything reaches the engine.

1. It verifies the caller's admin-portal token against the `admins` table (`require_admin`). An ordinary user
   token, and the token of an end-user account whose role is `admin`, are refused with 401 and nothing is sent.
2. It mints a service token for that one call: `act=admin-emergency`, the administrator's id as `sub`, valid
   for 60 seconds. The engine accepts no other credential on `/api/v1/admin/governance/...`, and it refuses a
   token that lives longer than 120 seconds.

The administrator id in that token is the reviewer the engine writes on every decision. Nothing in a request
body can name a different person.

The proxy returns the engine's answer unchanged, status code and body. It adds only these answers of its own:

| Status | `reason` | Meaning |
|---|---|---|
| 400 | `IDENTIFIER_INVALID` | A family id, request id or account id was not a plain identifier. Nothing was sent. |
| 502 | `UPSTREAM_UNREACHABLE` | The engine could not be reached. Nothing was sent and nothing changed. |
| 502 | `UPSTREAM_AUTH_REJECTED` | The engine refused the service token (the two services do not share a signing key). Nothing changed. |
| 504 | `UPSTREAM_TIMEOUT`, `UPSTREAM_OUTCOME_UNKNOWN` | The request was sent and no answer came back. The outcome is unknown: reload before doing anything else. |

A plain identifier is 1 to 128 characters of letters, digits, `_` and `-`, starting with a letter or a digit.
A value with a `.`, a `%` or a space is refused, and a value with a `/` matches no proxy route at all, so a path
parameter cannot select another engine route.

## 2. Seeing the registry

Open **Family Governance** in the sidebar (`/admin/governance`).

- The banner at the top states who may submit orders. It is computed from the engine's answer, not written into
  the page. With the research register as committed on 2026-10-09 the engine lists no family as executable, so
  it reads "No family has order authority". If the registry cannot be read it says the order authority is
  unknown; it never falls back to "none".
- The table lists every family the code defines: certification status and the research register's reason,
  governance stage, allowed modes, kill-switch state, order authority, the blockers, and what the next stage is
  still missing. Blockers and missing items are the engine's own codes.
- "Governance log integrity" is the engine's check of the hash chain of the governance log. `BROKEN` means a
  record was changed, removed or reordered; the states shown must not be relied on until that is investigated.

Click a family id to open its page (`/admin/governance/<family id>`). It shows the family's state, its recorded
evidence, its pending promotion request, its definition, and its whole history: every record with its number,
time, event type, stage change, actor, reason and record id, oldest first.

## 3. Every action is confirmed the same way

Each action opens a dialog that shows:

- what the action does and what it does not change;
- every record it will write;
- the request exactly as it will be sent (method, path and JSON body);
- a reason field. The reason is required, 3 to 500 characters, and the confirm button stays disabled without it.

If the engine refuses, the dialog stays open and shows the HTTP status, the engine's `reason` code and its
`detail` text, unchanged. If no definite answer comes back, the dialog says the outcome is unknown. The page
reports an action as recorded only when the engine returned the record it wrote.

Two calls have no reason field in the engine: registration and evidence. For those the reason is kept in the
main backend's audit row, and the dialog says so.

## 4. Registering families

On the Family Governance page, **Register code-defined families** records every family the code defines. A
family already on record with the same definition gets no new record. Registration gives stage M0 and nothing
else. A family whose rules or version changed since its last registration is recorded again and returns to M0.

Nothing can be recorded for a family before it is registered; the engine answers `FAMILY_NOT_REGISTERED`.

## 5. Requesting and deciding a promotion

All of this is on the family's page.

1. **Record evidence.** Enter the requirement key (the page suggests the keys the next stage is missing), a
   reference to where the evidence is (a file, a run id, a report), and the value `true`. Recording `false`
   withdraws the key. Evidence moves no stage.
2. **Request promotion to the next stage.** Only the next stage can be requested, one request at a time. A
   request changes nothing.
3. **Approve or reject the pending request.** The page sends a reason and nothing else. The engine checks the
   prerequisites itself at that moment (section 6). If any is missing it answers `PREREQUISITES_NOT_MET` with the
   list, records nothing in the governance log, and the family stays where it is. A rejection closes the request
   and leaves the stage unchanged.

Common refusals: `STAGE_SKIPPED` (not the next stage), `STAGE_NOT_OPERABLE_HERE` (above M6),
`PROMOTION_ALREADY_PENDING`, `FAMILY_DISABLED`, `REQUEST_NOT_PENDING`, `STAGE_CHANGED_SINCE_REQUEST`,
`FAMILY_RULE_HASH_MISMATCH` (the code no longer matches the registration), `REQUEST_NOT_FOUND`.

## 6. What each stage requires

The stages are the phases of `governance/phases.py`, unchanged. The evidence keys are that file's
`entry_requirements`. A key counts when evidence with a true value is recorded under it. (The API also counts
evidence attached to the promotion request itself; the page does not send any.)

| Stage | Title | Evidence keys | Verified by the engine, not by evidence |
|---|---|---|---|
| M0 | CATI OBSERVE / ENTRY BLOCKED | none | The stage a family has after registration. |
| M1 | SECTION 9 SHADOW | `v2_benchmark_tag`, `section9_shadow_verified` | |
| M2 | SECTIONS 10-12 SHADOW | `sections_10_12_shadow_verified` | |
| M3 | SECTIONS 13-17 SHADOW | `sections_13_17_shadow_verified` | |
| M4 | SECTION 19 SHADOW | `section19_shadow_verified` | |
| M5 | REPLAY CERTIFICATION | `certification_run_id`, `policy_freeze_hash`, `certification_policy_frozen` | The family's mandate verifies against the research register, and the register holds a completed, valid evaluation run of it. |
| M6 | DEMO CATI ACTIVE | `certification_run_id`, `policy_freeze_hash`, `replay_gates_passed` | Everything of M5, and the research register says the hypothesis passed, and a rule-based family's admission route answers eligible. |

Every approval also requires that the certification was not revoked, that the family is not disabled, and that
the code's rule hash equals the registered one.

Stages M1 to M5 grant no order authority. The registry gives a family the DEMO mode (order authority on demo
accounts) only when it is registered, enabled, certified by the research register, at M6, and its kill switch is
off. The order-submission flags and the other gates of the execution boundary still apply after that.

On 2026-10-09 the research register does not certify any family: the daily trend family is registered but not
passed, the residual momentum family failed, and the four setup specialists have no registered hypothesis.
Recording evidence cannot change that. No family can therefore reach order authority through this interface
until the research process records a pass.

## 7. Revoking

On the family's page:

- **Revoke promotion** moves the family back to any stage up to its current one (M0 by default). It needs no
  evidence and closes a pending request. Recorded evidence stays on record.
- **Revoke certification** withdraws the platform's acceptance of the family's research certification. The
  family becomes `REVOKED` and returns to M0 until it is registered again with a changed rule set or version.
  The research register itself is not changed.

Both are always possible for a registered family.

## 8. The three kill switches

| Switch | Where | Effect |
|---|---|---|
| Platform | **Bot Monitor** page, Emergency controls | No new entries for any family on any account. Emergency flatten is on the same page. |
| Family | The family's page, **Stop and disable** | No new entry and no increase from this family on any account. The registry then never lists the family as executable and shows the blocker `FAMILY_KILL_SWITCH`. Positions it holds follow the family's registered policy (`MANAGE_TO_EXIT` for every family defined today). |
| Account | Family Governance page, **Account kill switch** | No new entry for that one account, from any family. Its open positions keep their protection and their exits. |

Notes for the operator:

- Turning a family or account switch off lifts only that switch. The platform switch and the other switches
  still apply.
- The account id must be an existing broker account; otherwise the engine answers `ACCOUNT_UNKNOWN`. The value
  `GLOBAL` is refused (`SCOPE_INVALID`): the platform switch is changed only on the Bot Monitor page.
- The governance API has no read for account kill switches. The page cannot show which accounts are stopped; the
  answer to each change states the state the engine recorded.
- **Disable family** (same section) stops the family from being evaluated at all, shadow included, and blocks
  promotion until it is enabled again.

## 9. Where each action is recorded

| Record | Where | Written for |
|---|---|---|
| Governance log | `cati_family_events` (append-only, hash-chained) | Registration, evidence, requests, approvals, rejections, revocations, family kill switch, disable and enable. |
| Kill-switch control log | `cati_governance_controls` (append-only) | Account kill switch changes. |
| Engine audit events | the engine's audit store, type `GOVERNANCE` | Every write: requested, then recorded, refused or failed. |
| Main backend audit trail | `auth_audit_log`, event `admin_governance_<action>` | Every write and every read that did not succeed: administrator id and email, action, identifiers, reason, outcome status and reason. |

A successful read writes a log line only.

## 10. What the interface cannot do

- It cannot promote a family above M6. The API refuses it and the page never builds such a request.
- It cannot certify a family. Certification is the research register's record and only the research process
  writes it.
- It cannot enable order submission. The submission flags are configuration; they are neither shown nor changed.
- It cannot move the global CATI phase.
- It cannot put a prerequisite into an approval. Extra fields in a request are ignored by the engine.
- It cannot list account kill switches (no such read exists).
- It does not show the platform cycles or the shadow divergences. The proxy forwards those two reads
  (`/cycles`, `/shadow/divergences`), but no page uses them yet.

## 11. What was not verified

- No run on Linux.
- No browser session against the deployed or locally started real backends, and no real admin sign-in. The pages
  were opened in a browser against the real proxy code and the real engine router running locally over a
  throwaway database, with the sign-in guard bypassed; the token minted by the proxy was accepted by the
  engine's real token check there.
- No component tests: the admin frontend has no component test framework. The page's decisions (what is
  offered, what is sent, how answers and refusals are read) are in `familyGovernance.ts`, which the unit tests
  run as it ships.
