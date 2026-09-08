# Batch 3 — Real Runtime Validation of Canonical Evidence

Live validation of the canonical evidence wiring against the actual running
backend. Every ID below is real and queryable in
`backends/shared/shared_lib/persistence/cosmicforge.db`.

**Correction applied:** `bot_e5fe913972a9` was an *intentional operator
deletion*, not an unexplained replacement. It was not mutated, reactivated or
used. The active bot is `bot_a8117dc719fc`.

---

## Environment

| Item | Value |
| --- | --- |
| Canonical backend | port 9000, `uvicorn app.main:app` (venv `backends/venv`) |
| Code revision running | `51771f2487851c1a75ab35f5b6e51250c1e84f26` → later `f3187922` |
| Active bot | `bot_a8117dc719fc` — active, paper/demo, master_ensemble, BTCUSDT+ETHUSDT, 15m |
| Policy hash | `a59cec7cbe2fbe93941da037bf92689f85800aae5b8d746b3b5a3df3e4b7c864` |
| Suite | **2,054 passed, 0 failed, 0 skipped** |

A second instance was found on port 8000 (`--reload`) that does not serve
`/runner/status`. Only port 9000 drives the runner. Noted, not changed.

---

## Two defects found — both only visible in live runtime

### 1. Heartbeat rows destroyed real candle evaluations (data loss)

At the 04:30 close, cycle `e00b3a42` recorded two genuine evaluations
(`ENTRY_CONFIDENCE_BELOW_THRESHOLD`, `REGIME_LOW_VOL_CHOP`). Moments later
`trading_decisions` held **only two `NO_NEW_CANDLE` rows carrying the same
candle timestamp**. The regime, confidence and threshold evidence was gone.

Cause: `NO_NEW_CANDLE` rows carry the current candle's close time (useful for
diagnostics), which put them inside `uq_trading_decisions_candle`. The
10-second heartbeat's `INSERT OR REPLACE` then overwrote the real evaluation.

Fix (`1dfe26bf`): the constraint is about one *entry* decision per candle. A
heartbeat is a management tick, so it is excluded from both the index predicate
and `find_duplicate_candle_decisions()`. Three regression tests added.

**No unit test caught this** — none mixed a real evaluation and a heartbeat on
the same candle.

### 2. `market_snapshot_id` pointed at no row

`decisions/{id}` returned `market_snapshot: null`. `record_market_snapshot()`
existed but nothing called it.

Fix (`f3187922`): the runner persists the snapshot when `claim_candle()`
succeeds. Snapshots are built every heartbeat (that is how the gate works), so
persisting unconditionally would write ~17k identical rows/day; one row per
symbol per candle instead.

---

## Item-by-item proof

### 2 — One canonical runtime session

```
runtime_session_id : rts_8faae826a0b24f5d85ff
started_at         : 2026-09-08T04:41 UTC
pid                : (recorded)   python 3.12.2
code_revision      : matches HEAD
database_role      : development
process_execution_mode : paper
status             : RUNNING
```

Startup also emitted, as designed:

```
[DATABASE_REGISTRY_WARNING] active=…/cosmicforge.db role=development
  other_candidates=[('…cosmicforge-LAPTOP-5B3QOQDJ.db','FORENSIC'),
                    ('…pre_capital_recovery….db','BACKUP')]
  (none will be selected automatically)
```

### 3 — `bot_run` tied to session and policy hash

```
run_id             : 13263c73d72143cf816280380657d7a7
bot_instance_id    : bot_a8117dc719fc
runtime_session_id : rts_8faae826a0b24f5d85ff
policy_hash        : a59cec7cbe2fbe93941da037bf92689f85800aae5b8d746b3b5a3df3e4b7c864
provenance         : PAPER_FORWARD
execution_mode/env : paper / demo
```

### 5 — Heartbeats persist lightweight evidence without rerunning the ensemble

39 cycles at ~10s intervals in one earlier run; 58 `NO_NEW_CANDLE` rows in the
final run. A sampled heartbeat row:

```
symbol=BTCUSDT reason=NO_NEW_CANDLE complete=1 candle_t=1788841799999
component_signals_json     : (empty)
component_metadata_json    : (empty)
supporting_strategies_json : (empty)
raw_confidence             : (empty)
regime                     : (empty)
```

No component payload, no regime, no confidence — the ensemble did not run.

### 6/7 — Canonical decisions for a newly closed 15m candle

Candle `1788841800000 → 1788842699999` (04:30:00 → 04:44:59.999 UTC),
cycle `761f48d1-9cfb-4739-85b6-2101b16fd6f9`:

| Field | BTCUSDT | ETHUSDT |
| --- | --- | --- |
| `decision_id` | `dec_1e381f069d714626bc70` | `dec_2e5cc9a426554c538b9d` |
| `bot_instance_id` | `bot_a8117dc719fc` | `bot_a8117dc719fc` |
| `runtime_session_id` | `rts_8faae826a0b24f5d85ff` | same |
| `run_id` | `13263c73d72143cf816280380657d7a7` | same |
| `cycle_id` | `761f48d1-…` | same |
| `market_snapshot_id` | `ms_483c27e06c1f48e1` | `ms_8fd24b0ae655449c` |
| `timeframe` | 15m | 15m |
| `closed_candle_close_time` | 1788842699999 | 1788842699999 |
| `regime` / confidence | WEAK_TREND / 1.0 | WEAK_TREND / 0.468 |
| `active_strategies` | donchian_breakout, sma_cross, supertrend, trend_pullback | same |
| `raw_confidence` | 0.0 | 0.0 |
| `effective_entry_threshold` | 0.0 | 0.0 |
| `quality_result` | FAIL | FAIL |
| `final_action` | HOLD | HOLD |
| `primary_reason` | **NO_OPPORTUNITY** | **REGIME_LOW_VOL_CHOP** |
| `policy_hash` | a59cec7c… | a59cec7c… |
| `provenance` | PAPER_FORWARD | PAPER_FORWARD |
| `evidence_quality` | COMPLETE | COMPLETE |
| `execution_mode` / env | paper / demo | paper / demo |
| `complete` / `finalized_at` | 1 / 04:45:10.115 | 1 / 04:45:11.834 |

Snapshot rows persisted for both:

```
ms_483c27e06c1f48e1 BTCUSDT 15m close_t=1788842699999 htf=4h aligned=1 candles=249 hash=5168f480c166
ms_8fd24b0ae655449c ETHUSDT 15m close_t=1788842699999 htf=4h aligned=1 candles=250 hash=47bc3036ca3d
```

`higher_timeframe_aligned=1` — the 4h HTF candle closed at or before the 15m
decision candle. No look-ahead.

### 8 — Diagnostics API explains the absence of a trade

`GET /api/v1/admin/trading/evidence/bots/bot_a8117dc719fc/diagnostics?organic_only=true`

```json
{
  "evaluations": 63,
  "new_candle_evaluations": 2,
  "no_new_candle": 61,
  "reason_counts": {"NO_NEW_CANDLE": 61, "REGIME_LOW_VOL_CHOP": 1, "NO_OPPORTUNITY": 1},
  "stage_rejections": {"quality": 2, "risk": 0, "execution": 0, "entry_protection": 0},
  "approved": 0,
  "execution_attempts": 0,
  "current_policy_hash": "a59cec7cbe2fbe93941da037bf92689f85800aae5b8d746b3b5a3df3e4b7c864"
}
```

`GET .../decisions/dec_1e381f069d714626bc70` returns the whole causal chain:

```
decision   : dec_1e381f069d714626bc70 BTCUSDT 15m
candle     : 1788841800000 -> 1788842699999
market seen: ms_483c27e06c1f48e1 candles=249 htf=4h aligned=1
regime     : WEAK_TREND 1.0
strategies : ['donchian_breakout','sma_cross','supertrend','trend_pullback']
quality    : FAIL NO_OPPORTUNITY
risk       : None      (never reached — quality stopped it)
execution  : None
final      : HOLD / NO_OPPORTUNITY
attempts   : 0 | position: None
```

**OLD-R4 confirmed closed against live data**, not fixtures.

### 9 — Integrity checks on the live run

| Check | Result |
| --- | --- |
| Abandoned decisions (complete=0, >120s) | 0 — PASS |
| Decisions with no primary reason | 0 — PASS |
| Duplicate entry decision per bot/symbol/timeframe/candle | 0 — PASS |
| Cycle counts reconcile to decision rows | 39/39 cycles, 0 mismatches — PASS |
| Lineage session → run → cycle → decision | 60/60 joinable — PASS |
| Provenance | 100% `PAPER_FORWARD` — PASS |
| Non-organic contamination in this bot's evidence | 0 — PASS |

### 10 — No trade was forced

Both outcomes are natural: `NO_OPPORTUNITY` and `REGIME_LOW_VOL_CHOP`. No
threshold was lowered, no opportunity injected into the live path.

### 11/12 — Phase 10–12 acceptance

Live Phase 10 endpoint against the real bot:

```json
{"state":"NOT_READY","policy_hash":"a59cec7c…","policy_stale":false,
 "missing_sections":["A","B","C","D","E"],"reason":"no_state_recorded"}
```

Fail-closed as designed — no state row means NOT_READY.

Phase 12 remains on its own fresh identity `bot_phase12_validation`
(`PAPER_FORWARD_VALIDATION` provenance, outside `ORGANIC_PROVENANCE`), 26 tests
covering: controlled approved opportunity → real `TradingDecisionEngine` at an
unlowered threshold → `PaperExecutor` → OPEN → TP1 → BE/trailing on the
remainder → restart restoring 0.5 not 1.0 → final close of the remainder →
FLAT; plus daily close (once across 10 heartbeats, restart-safe, next day is a
different window), kill switch (blocks entries; persists close evidence when
flattening) and duplicate-entry protection.

---

## Observations

1. **Heartbeat row volume.** ~12 decision rows/minute ≈ **17,000/day** for two
   symbols. Rows are tiny (no payloads) and §5 requires every evaluation to
   finalize, but this will need a retention policy before a long readiness run.
   `trading_cycles` already carries per-cycle counts, so per-symbol heartbeat
   rows could later be summarised rather than kept indefinitely.
2. **Pre-opportunity filters carry thinner evidence.** `REGIME_LOW_VOL_CHOP`
   returns before the ensemble builds an opportunity, so
   `component_metadata_json` is null and supporting/opposing sets are empty.
   Architecturally correct — it is not a quality verdict — but the row is less
   informative than a full evaluation. Carried from Batch 2 §I.2.
3. **Two backend instances.** Port 8000 (`--reload`) coexists with port 9000.
   Only 9000 drives the runner. The README warns `--reload` can kill the loop.
   Not changed; flagged for the operator.
4. **`bot_runs` aggregate counters** (`cycles`, `decisions`, `attempts`) are
   still not incremented; per-cycle counts live in `trading_cycles`.

---

## Verdict

```
BACKEND_RESTARTED_WITH_NEW_CODE:      PASS
CANONICAL_RUNTIME_SESSION:            PASS   rts_8faae826a0b24f5d85ff
CANONICAL_BOT_RUN_LINKED:             PASS   13263c73d72143cf816280380657d7a7
HEARTBEAT_LIGHTWEIGHT_EVIDENCE:       PASS
NEW_CANDLE_CAPTURED_BTC_AND_ETH:      PASS   candle 1788842699999
CANONICAL_DECISION_FIELDS_COMPLETE:   PASS
MARKET_SNAPSHOT_LINEAGE:              PASS
DIAGNOSTICS_API_EXPLAINS_NO_TRADE:    PASS
INTEGRITY_NO_ABANDONED_DECISIONS:     PASS
INTEGRITY_NO_DUPLICATE_CANDLE:        PASS
INTEGRITY_CYCLE_RECONCILIATION:       PASS
INTEGRITY_LINEAGE_INTACT:             PASS   60/60
PROVENANCE_CORRECT:                   PASS   100% PAPER_FORWARD
NO_TEST_OR_REPLAY_CONTAMINATION:      PASS
OLD_R4_AGAINST_LIVE_DATA:             CLOSED
PHASE_10_READINESS_LIVE:              PASS   NOT_READY, fail-closed
PHASE_12_PAPER_SMOKE:                 PASS   fresh validation bot
FULL_TEST_SUITE:                      PASS   2054 passed, 0 failed

ORGANIC_CANONICAL_EVIDENCE_PROVEN:    YES
SAFE_TO_BEGIN_LONG_PAPER_READINESS:   YES
SAFE_TO_BEGIN_AI_DATASET_WORK:        NO
```

**`SAFE_TO_BEGIN_LONG_PAPER_READINESS` is now YES.** Organic canonical
evidence is proven end to end against the real running bot, with clean
integrity and correct provenance.

**`SAFE_TO_BEGIN_AI_DATASET_WORK` stays NO.** The pipeline is proven over
minutes, not weeks. Two things should land first: a retention decision for
heartbeat rows (observation 1), and enough accumulated organic evidence that
the integrity checks have been exercised against real volume rather than a
five-minute window. Nothing here authorises mainnet.
