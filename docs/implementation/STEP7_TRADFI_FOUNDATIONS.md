# Section H Step 7: traditional-market contract foundations

Status (2026-10-17): foundations built and tested. **Nothing traditional-market is enabled.** No hypothesis
was registered, no research document under `docs/research/` was touched, the research register was not
written, no family definition changed, live trading stays disabled and the platform risk caps (0.40 % per
trade, entry stop at most 15 %) are unchanged.

## 7.1 Research draft

- `research/trend_v2_tradfi/SPEC.md` — **DRAFT, NOT FROZEN, NOT REGISTERED.** Adapts the structure of
  `research/trend_v1/SPEC.md` to traditional-market contracts: closed sessions, contract calendars,
  price/quantity rules, expiry and rolls (back-adjustment method left open), carry/financing in place of
  crypto funding, dividends and corporate actions, available history, venue-specific settlement. States
  that Mandate 004 certification does not transfer. Every unsourced parameter is listed in the owner-decision
  table (D1–D16). The registration procedure: H-019 only after owner approval through the governed register,
  then frozen under the v1 rules.
- `research/trend_v2_tradfi/README.md` — status note.

## 7.2 Market calendar and contract economics (`backends/bot-backend/app/markets/`)

Not under `families/` and not under any evaluation-fingerprint root: the Mandate 004 source fingerprint is
unchanged (`59a3b668…a7aa` before and after).

| Module | What it does |
|---|---|
| `calendar.py` | Venue-local weekly sessions via `zoneinfo` (overnight / multi-day sessions, trading date = local closing date), holidays, early closes, maintenance windows, coverage period (a date outside it is an error, never "no holiday"), DST gaps rejected as definition errors. `is_open`, `session_for`, `next_open`, `next_close`, `open_intervals`, `expected_bars` (never a bar for a closed period), `classify_bars` (flags forward-filled closure bars as `unexpected`). `CONTINUOUS` calendars are always open with UTC-day daily bars. |
| `contracts.py` | `ContractSpec` (symbol, venue, asset class, contract type, settlement currency, multiplier, tick size, derived tick value, quantity step, minimum, margin rule initial/maintenance, listing / first notice / last trade / expiry, carry model, corporate actions). Decimal only; floats refused. |
| `roll.py` | Deterministic roll schedule (`SESSIONS_BEFORE_LAST_TRADE`, `SESSIONS_BEFORE_FIRST_NOTICE`, `AT_LAST_TRADE`) counted in calendar sessions; `active_contract`; `tradability_refusal` (`CONTRACT_NOT_LISTED`, `CONTRACT_EXPIRED`). |
| `economics.py` | Notional, PnL in settlement currency, explicit conversion, tick money, funding accrual (perpetuals), overnight financing with an optional triple weekday, futures carry = 0 outside the roll, dividend cash, split and back-adjustment of prices. |
| `guard.py` | `MarketAvailabilityGuard.refuse_reason(market, ts, contract, last_bar_open_ms, check_data)` → `CALENDAR_UNVERIFIED` / `CONTRACT_NOT_LISTED` / `CONTRACT_EXPIRED` / `MARKET_CLOSED` / `DATA_STALE` (closed time not counted) / `MARKET_UNKNOWN`, or `None`. Refuses only; authorizes nothing. Not yet wired into the execution path (see open items). |
| `registry.py` + `data/*.json` | Versioned definitions (`schema_version` 1, `data_version` 2026-10-17.1). `CRYPTO_24_7` (status `PLATFORM_CONVENTION`, preserving "crypto is always open") and three crypto markets (binance, bybit, bingx); three `TRADFI_EXAMPLE_*` calendars, markets and synthetic contracts, all `UNVERIFIED` with placeholder venues. Validation rejects missing source/status, unknown calendars, duplicates, `PLATFORM_CONVENTION` outside continuous crypto, and a `VERIFIED` session calendar without a coverage period. |

`app/trading_intelligence/pipeline/family_markets.py` declares supported market types, markets and calendars
per registry family **outside** `FamilyDefinition` (DAILY_TREND: `CRYPTO_BINANCE_USDT_PERPETUAL` on
`CRYPTO_24_7`). No family declares a traditional market. DAILY_TREND's `definition_hash`
(`09d33c15…1527`) is pinned in a test and unchanged.

## 7.3 Portal market selection

- `shared_lib/deployment/contract.py`: optional `market_scope` (`CRYPTO_ONLY` default | `CRYPTO_AND_TRADFI`)
  and blocker `TRADFI_NOT_AVAILABLE` (registered in the reason registry). **Schema version unchanged**
  (`2026-10-08.v1`): the field is optional and additive, every older payload validates unchanged, and the
  default scope is excluded from the request fingerprint so idempotent replays of pre-7.3 requests still
  match.
- `app/core/deployment_service.py`: a `CRYPTO_AND_TRADFI` preview/deploy always gets `TRADFI_NOT_AVAILABLE`
  with the availability reasons (the deploy creates nothing); `market_scope` is echoed in the preview; bot
  payloads carry `market_type` and `market_scope` (`CRYPTO_ONLY`, or `LEGACY_<TYPE>` for a non-crypto legacy
  bot). The crypto path is otherwise unchanged.
- `app/markets/availability.py` + `GET /api/v1/markets/availability` (`app/api/markets.py`, registered in
  bot-backend `main.py`), proxied by user-backend `app/api/markets_proxy.py` (one registration in
  `main.py`). Per market: adapter support (factory has only binance/bybit/bingx), declaring and certified
  families (research register), calendar verification, activation stage, connected accounts →
  `{available, reasons[]}`; plus the two scopes. Crypto keeps its behaviour (a missing account is reported,
  not blocking).
- Frontend: `src/lib/markets.ts` (pure; traditional markets never selectable in this build:
  `TRADFI_SELECTABLE_IN_THIS_BUILD = false`), a "4. Markets" control in `CatiDeployment.tsx` showing the
  traditional option disabled with the backend's reasons, a "Markets" row in the preview, the scope on the
  bot row (`BotInstanceRow.tsx`), `api.getMarketAvailability` in `client.ts`, `deployment.ts` optional
  `marketScope` (the body is byte-identical when it is crypto only). The legacy forex form in `AutoPilot.tsx`
  is kept and now says it is the legacy form, not CATI.
- Onboarding: not changed. The step sequence is driven by the user-backend onboarding service; a market
  preference step would change that state machine for a choice that has only one available answer. Left for
  the batch that decides the legacy forex form.

## 7.4 Staged activation (`app/markets/activation.py`)

Append-only, hash-chained `market_activation_events` (`shared_lib/persistence/markets_schema.py`,
`ensure_markets_schema` hooked at the end of `migrations.migrate()`; UPDATE/DELETE abort). Per
(market, family): `NONE → PAPER → DEMO → LIVE`, `REVOKED` from any active stage, `REVOKED → PAPER` only.

- Every transition needs an `OperatorAuthorization` (the `pipeline.family_governance` class) and a reason;
  nothing promotes automatically. Approved, refused and policy-blocked decisions are all recorded with
  operator, evidence references, blockers, the policy, and (when approved) risk restrictions and exit
  criteria.
- Entry criteria: known market; traditional only (`MARKET_GOVERNED_BY_FAMILY_GOVERNANCE` for crypto, so
  there is one authority per market); the family declares the market; required evidence per stage.
- DEMO: `CERTIFIED` from the research register via `FamilyRegistry.research_certification` (no family is
  certified for any traditional market → refused), `VERIFIED` calendar, an execution adapter for the venue.
- Durations come from `ActivationPolicy` (`paper_min_days`, `demo_min_days`), `None` by default →
  `BLOCKED_PENDING_POLICY`.
- LIVE: always `TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD`; no account environment may be eligible for LIVE.
- Risk restrictions per stage are validated against 0.40 % / 15 %; account eligibility by stage
  (PAPER: none, DEMO: demo accounts on the market's venue).

## Tests (run 2026-10-17)

| Suite | Result |
|---|---|
| `tests/markets/test_step7_market_calendar.py` | 25 passed |
| `tests/markets/test_step7_contracts_economics.py` | 22 passed |
| `tests/markets/test_step7_market_guard_registry.py` | 19 passed |
| `tests/markets/test_step7_market_activation.py` | 19 passed |
| `tests/markets/test_step7_market_scope.py` | 10 passed |
| bot-backend regression set (above + deployment contract, reason registry, Step 1 security / closing, Step 3 family registry, Step 2 research governance) | 193 passed |
| user-backend `tests/test_step7_markets_proxy.py` + `tests/test_step1_proxies.py` | 30 passed (2 new) |
| bot-backend architecture/invariant set (section 25/27 security and acceptance, sections 7-9, section B, threshold invariants, sections 14/22/23-26, Step 3 multi-position execution and retention) | 254 passed, 6 failed: 2 `test_section_b_code_fixes` runner checks (code not touched by Step 7) and 4 Step 3 retention checks failing only on tables added concurrently by other work (`admin_mfa_recovery_codes`, `billing_*`, `legal_*`); `market_activation_events` was classified (IMMUTABLE_GOVERNANCE) in `pipeline/retention.py` |
| frontend `npm test` | 47 passed (6 new in `src/tests/markets.test.ts`) |
| frontend `npx tsc --noEmit` (root and `tsconfig.app.json`) | clean |
| frontend `npx eslint` on the changed files | 0 errors (warnings pre-existing) |

## Open owner decisions

1. Every row D1–D16 of `research/trend_v2_tradfi/SPEC.md` (universe, venues, data source and licence,
   development / held-back periods, roll rule and back-adjustment, cost model, pass thresholds, risk levels
   within the platform caps, calendars and contract specifications with cited sources, activation durations).
2. Whether to register the draft as H-019 (only after 1).
3. Data-staleness expectations per market (`freshness` in `markets.json` is a proposal: one missing daily bar).
4. Whether `PLATFORM_CONVENTION` is an acceptable status for the 24/7 crypto calendar or should be replaced by
   a cited venue statement.
5. The fate of the legacy forex form and whether onboarding should ask for a market preference.

## Not done in this step

- The guard is not wired into the order path (crypto behaviour would be unchanged by it; wiring it is a
  separate, reviewed change to the execution boundary).
- No traditional-market execution adapter, data source, verified calendar or real contract specification
  exists; none was invented.
- No administrative API for the activation ledger (it is a library with tests; an admin route should follow
  the family governance router pattern when an owner policy exists).
