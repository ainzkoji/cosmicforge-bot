# Session Gate Consistency — Forensic Report

At the 01:30 UTC cycle on 2026-09-10, BTCUSDT recorded `SESSION_BLOCKED` while
ETHUSDT reached `AdaptiveEntryThresholdEngine` and produced a valid EVALUATED
threshold decision. Same bot, same runtime, same cycle, both `market_type=CRYPTO`.

**The two symbols had identical effective session policy. The divergence was a
bug, and the bug was mine** — introduced by the threshold rebuild in `61b4ef43`,
then silently converted into a false session verdict by two pre-existing
exception handlers.

No threshold parameter, session hour, expert weight or regime rule was changed.

---

## 1. Session policy resolution

Both symbols belong to `bot_a8117dc719fc`. Every session-relevant value is
resolved **per bot**, not per symbol — there is no symbol override anywhere in
the resolution path.

| Value | BTCUSDT | ETHUSDT | Source / precedence |
| --- | --- | --- | --- |
| bot_instance_id | `bot_a8117dc719fc` | `bot_a8117dc719fc` | `BotInstance` |
| run_id | `9c329a9e…87428` | `9c329a9e…87428` | same run |
| cycle_id | `81bcddb2…abf6` | `81bcddb2…abf6` | **same cycle** |
| runtime_session_id | `rts_a28204303a1a46f2a253` | same | same process |
| policy_hash | `cbdf8436…aa557` | `cbdf8436…aa557` | `EffectiveBotPolicy` |
| market_type | `CRYPTO` | `CRYPTO` | `BotRunContext.market_type` (per bot) |
| venue | binance | binance | `broker_account_id = brk_c729454e6c98` |
| strategy | `master_ensemble` | `master_ensemble` | `BotInstance.strategy_id` |
| timeframe | `15m` | `15m` | per bot |
| session_filter_enabled | `True` | `True` | `ENSEMBLE_SESSION_FILTER_ENABLED`, global setting |
| session windows (UTC) | `06:00-19:00` | `06:00-19:00` | `ENSEMBLE_SESSION_WINDOWS_UTC`, global |
| `enforce_session` | `None` | `None` | `strategy_params`, per bot |
| `crypto_session_enabled` | `False` | `False` | `strategy_params`, per bot |
| symbol override | **none exists** | **none exists** | no per-symbol session field in any model |
| closed candle | `1789003799999` | `1789003799999` | same candle |
| regime | `WEAK_TREND` | `WEAK_TREND` | classifier |

**POLICIES_IDENTICAL: YES.** Every input is either global or per-bot. The
resolution path contains no per-symbol branch, so a per-symbol divergence is
not expressible through policy.

---

## 2. Session gate inputs, and what actually differed

The recorded gate evidence (`decision_traces.gate_details_json`):

```
BTCUSDT  01:30:00.782Z   session_allowed = false   session_status = SESSION_BLOCKED
ETHUSDT  01:30:03.449Z   session_allowed = true    session_status = CRYPTO_SESSION_24_7_BYPASS
```

The gate is `master_ensemble.get_signal` Step 3.6:

```python
_market_type = str(kwargs.get("market_type") or "UNKNOWN").upper()
_explicit_session = kwargs.get("enforce_session")
if _explicit_session is None:
    _explicit_session = bool(kwargs.get("crypto_session_enabled", False))
_crypto_bypass = _market_type == "CRYPTO" and not bool(_explicit_session)
```

With this bot's kwargs — `market_type="CRYPTO"`, `enforce_session=None`,
`crypto_session_enabled=False` — `_crypto_bypass` is **always True**. The fixed
session window is never even consulted.

That inverts the question. `SESSION_BLOCKED` cannot occur for this bot while the
kwargs are intact, so **every `SESSION_BLOCKED` row is a row where the kwargs
were lost before the gate ran.**

The clock is not implicated: both evaluations used the same wall-clock UTC hour
via `_check_session_gate`, both carried the same `closed_candle_close_time`, and
the gate never reads candle time. The two evaluations are 2.1 seconds apart,
inside the same minute and the same session window.

---

## 3. Code path — the exact condition

```
runner.step_symbol
  -> orchestrator.process_trading_opportunity(market_type="CRYPTO", ...)
     -> self.strategy.analyze(symbol=…, klines=…, current_price=…, **kwargs)
        -> LegacyStrategyAdapter.analyze
           try:    self.legacy.get_signal(symbol, **kwargs)      # market_type present
           except TypeError:
                   self.legacy.get_signal(symbol)                # ALL kwargs discarded
```

**The trigger.** `master_ensemble.py:1165` passed
`threshold_floor=float(effective_threshold)` into `classify_hold_reason`. The
threshold rebuild made `effective_entry_threshold` `float | None`, and it is
`None` on every candle that produced no directional candidate. `float(None)`
raises `TypeError`.

**The amplifier.** That `except TypeError` exists for a legacy strategy whose
signature cannot accept kwargs. It also caught a `TypeError` raised from *inside*
a strategy that had accepted them, and re-ran the entire evaluation with no
kwargs at all. Without `market_type`, `_market_type` becomes `"UNKNOWN"`,
`_crypto_bypass` becomes `False`, the fixed window applies, 01:30 UTC is outside
`06:00-19:00`, and the candle is recorded as `SESSION_BLOCKED`.

The identical handler exists a second time, in `runner.py`.

**Why the two symbols differed.** The retry returns cleanly *because* the session
gate stops it early — before the code that raised. So:

* **BTC** produced no directional candidate → `effective_threshold` is `None` →
  TypeError → retry without kwargs → blocked at the session gate → returns
  `SESSION_BLOCKED`.
* **ETH** produced a directional candidate → a threshold was evaluated (0.810263)
  → no TypeError → first call completed → `CRYPTO_SESSION_24_7_BYPASS`.

The divergence is therefore **data-dependent, not policy-dependent**: whichever
symbol happens to have no candidate on a given candle gets mislabelled.

### Reproduced

Against live public market data, running the real ensemble with the runner's
kwargs and then with none (`scripts` probe, read-only, test-mode state store):

```
BEFORE   BTCUSDT  with kwargs -> TypeError: float() argument must be … not 'NoneType'
         ETHUSDT  with kwargs -> CRYPTO_SESSION_24_7_BYPASS

AFTER    BTCUSDT  with kwargs -> CRYPTO_SESSION_24_7_BYPASS   (reason NO_OPPORTUNITY)
         ETHUSDT  with kwargs -> CRYPTO_SESSION_24_7_BYPASS   (reason NO_OPPORTUNITY)
```

### Ruled out

| Hypothesis | Verdict |
| --- | --- |
| One symbol uses a legacy session path | **No** — both went through the orchestrated path; both traces carry `orchestrator_reason` |
| Different `market_type` source | **No** — one `BotRunContext`, one value, recorded identically on both rows |
| Stale mutable state leaking across symbols | **No** — the bypass decision reads only kwargs; asserted by test |
| Venue/symbol normalisation differs | **No** — normalisation is `.upper()` on `market_type` only; the gate never reads the symbol |
| Ordering or caching | **Contributing, not causal** — order determines *which* symbol lacks a candidate first, but the gate itself is order-independent |
| One used candle time, the other wall-clock | **No** — both used wall-clock UTC; the gate never reads candle time |
| Intentional symbol override | **No** — no per-symbol session field exists in any model |

---

## 4. Blast radius

`SESSION_BLOCKED` did not exist in this database until the threshold rebuild ran
live:

| Window | SESSION_BLOCKED | NO_OPPORTUNITY |
| --- | --- | --- |
| 2026-09-08 (before rebuild) | 0 | 44 |
| 2026-09-09 (mostly before) | 6 | 89 |
| after the rebuild went live (21:35Z) | **35** (23 BTC, 12 ETH) | **0** |

The bug converted `NO_OPPORTUNITY` into `SESSION_BLOCKED`. **35 decision rows
carry a reason for a thing that did not happen.**

They are historical evidence and have **not** been rewritten. Anything reading
`primary_reason` in the window 2026-09-09T21:35Z → 2026-09-10T04:00Z should treat
`SESSION_BLOCKED` as `NO_OPPORTUNITY`.

A second consequence, now also fixed: the retry re-executed all seven experts, so
those candles ran the strategy twice.

---

## 5. Fix

* `hold_breakdown.classify_hold_reason` takes `threshold_floor: float | None`.
  `None` means no threshold was evaluated, and must not fire
  `CONFIDENCE_BELOW_FLOOR` — a candle with no candidate never had a floor.
* `master_ensemble` passes `None` through instead of coercing it.
* `_is_signature_rejection(exc, func)` distinguishes Python rejecting a call
  signature from a `TypeError` raised inside a strategy body. It inspects the
  signature rather than pattern-matching the message, and **fails closed** when
  it cannot tell. Both call sites now re-raise a body failure. A crash is
  reported as a crash.

The legacy fallback still works for a genuine kwargs-less signature; a test
covers that so the fix does not break the case the handler was written for.

---

## 6. Multi-market

Nothing is hard-coded to BTC or ETH. The invariant test parametrises symbol
pairs across crypto, metals and FX precisely so the gate cannot acquire
per-ticker behaviour.

Instruments that legitimately need different sessions — exchange trading hours,
futures calendars, holidays, maintenance windows — must get them from **resolved
policy**. The test that the bypass decision reads only kwargs and no instance
state is what keeps that door open: a future per-symbol session policy would
arrive as an explicit resolved input, and would still satisfy the invariant,
because the policies would genuinely differ.

---

## 7. Live validation — not performed, and why

**I did not restart the runtime.** Another session is mid-flight on the runtime
lifecycle: `app/main.py` and `app/ops/runtime_ownership.py` are modified, and
`app/ops/runtime_preflight.py`, `app/ops/runtime_shutdown.py`,
`scripts/runtime_status.py` and `scripts/trading_runtime.ps1` are new — all
uncommitted. Restarting would deploy another session's work in progress to the
live paper runtime. That is not a call I should make.

The running process is therefore still on `4fb218d5` and still has the bug; it
will keep mislabelling `NO_OPPORTUNITY` as `SESSION_BLOCKED` until it is
restarted on `8622fc73` or later.

What has been established without a restart: the divergence is reproduced
deterministically against live market data, the fix is verified the same way,
and the invariant is covered by 22 regression tests.

**To validate live once the runtime work has landed**, restart and then, at any
15m boundary, confirm both symbols agree:

```bash
sqlite3 backends/shared/shared_lib/persistence/cosmicforge.db "SELECT symbol, primary_reason, threshold_status FROM trading_decisions WHERE primary_reason <> 'NO_NEW_CANDLE' ORDER BY evaluated_at DESC LIMIT 4;"
```

Expected: no `SESSION_BLOCKED` outside 06:00–19:00 UTC for a CRYPTO bot, and
`NO_OPPORTUNITY` where there was no directional candidate.

### An unrelated observation

The supervisor logged `CRASH: exit code` and self-recovered onto launch #2. The
crashed process stopped mid-cycle at iteration 1250 with no traceback, which
indicates external termination rather than a Python fault — consistent with the
other session restarting it. The watchdog behaved correctly. Noted, not chased.

---

## 8. Verdict

```
BTC_EFFECTIVE_SESSION_POLICY:
  market_type=CRYPTO, session_filter_enabled=True,
  windows_utc=06:00-19:00, enforce_session=None,
  crypto_session_enabled=False, symbol_override=none
  -> crypto_24_7_bypass = TRUE  (window never consulted)
  policy_hash cbdf8436686f7c0dbb503dc09caf7972a5ff0ce857413b0a0e173d4b209aa557

ETH_EFFECTIVE_SESSION_POLICY:
  market_type=CRYPTO, session_filter_enabled=True,
  windows_utc=06:00-19:00, enforce_session=None,
  crypto_session_enabled=False, symbol_override=none
  -> crypto_24_7_bypass = TRUE  (window never consulted)
  policy_hash cbdf8436686f7c0dbb503dc09caf7972a5ff0ce857413b0a0e173d4b209aa557

POLICIES_IDENTICAL:              YES

SESSION_VERDICTS_SHOULD_MATCH:   YES

ROOT_CAUSE:
  master_ensemble.py passed threshold_floor=float(effective_threshold) into
  classify_hold_reason. The threshold rebuild (61b4ef43) made that value None
  whenever no threshold was evaluated, so float(None) raised TypeError on every
  candle with no directional candidate. Two call sites -- LegacyStrategyAdapter
  and the runner -- caught that TypeError and silently re-ran get_signal with no
  kwargs, discarding market_type. Without market_type the CRYPTO 24/7 bypass
  does not apply, the fixed 06:00-19:00 window does, and a NO_OPPORTUNITY candle
  at 01:30 UTC was recorded as SESSION_BLOCKED. BTC had no candidate that cycle;
  ETH did, so ETH never raised and kept its kwargs.

BUG_FOUND:                       YES  (two: the float(None), and the handler
                                 that converted it into a different evaluation)

CROSS_SYMBOL_STATE_LEAK:         NO   (the bypass decision reads only kwargs;
                                 asserted by test)

TIME_SOURCE_CONSISTENT:          YES  (both wall-clock UTC, same minute, same
                                 window; the gate never reads candle time)

CRYPTO_24_7_BYPASS_CONSISTENT:   NO before the fix / YES after

FIX_REQUIRED:                    YES  (applied, 8622fc73)

FULL_TEST_SUITE:                 2414 passed, 0 failed, 0 skipped,
                                 4 subtests passed, 27 warnings
                                 (+22 new session-gate regression tests)

LIVE_SAME-CANDLE_VALIDATION:     NOT_REQUIRED for diagnosis -- the divergence is
                                 reproduced and the fix verified deterministically
                                 against live market data. NOT PERFORMED as a
                                 runtime check: restarting would deploy another
                                 session's uncommitted runtime-lifecycle work.
                                 The live process still carries the bug until it
                                 is restarted on 8622fc73.
```
