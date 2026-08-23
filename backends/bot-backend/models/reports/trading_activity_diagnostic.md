# Trading Activity Diagnostic

Generated: `2026-07-15T21:17:01.582842+00:00`

## Conclusion

Exact root cause: during the configured 06:00-19:00 UTC runtime window, the master ensemble generated only HOLD/no-setup decisions for BTCUSDT and ETHUSDT, so execution was never eligible. After 19:00 UTC, the intentional ensemble session gate blocks cycles. IOFS shadow, disabled ML, daily limits, and circuit breakers are not the cause.

Fix applied: Added this read-only diagnostic and explicit block-reason evidence. No trading thresholds or safety configuration were loosened.

## Runtime Answers

1. Runner loop alive: **True**.
2. Market data loading: **False**.
3. Strategy decisions created: **True**.
4. Paper executor reached in latest sample: **True**.
5. Paper orders attempted in latest sample: **2**.
6. Top blocking reason: **strategy_no_signal**.
7. IOFS shadow blocking bug: **False**.
8. ML disabled blocking bug: **False**.
9. Session filter blocking all cycles: **False** (latest sample all session-blocked: `False`).
10. Circuit breaker/daily limit stuck: **False**.

## Configuration

| Field | .env | Runtime loaded | Active run snapshot | Match |
|---|---|---|---|---|
| `EXECUTION_MODE` | `paper` | `paper` | `paper` | `True` |
| `ML_ENABLED` | `False` | `False` | `False` | `True` |
| `ML_SHADOW_MODE` | `None` | `False` | `False` | `None` |
| `IOFS_GATE_ENABLED` | `True` | `True` | `True` | `True` |
| `IOFS_GATE_MODE` | `shadow` | `shadow` | `shadow` | `True` |
| `TRADE_SYMBOLS` | `BTCUSDT,ETHUSDT` | `BTCUSDT,ETHUSDT` | `BTCUSDT,ETHUSDT` | `True` |
| `MAX_TRADES_DAILY` | `3` | `3` | `3` | `True` |
| `ENSEMBLE_BLOCKED_REGIMES` | `` | `` | `` | `True` |
| `ENSEMBLE_MIN_THRESHOLD_FLOOR` | `0.55` | `0.55` | `0.55` | `True` |
| `ENSEMBLE_SESSION_FILTER_ENABLED` | `True` | `True` | `True` | `True` |
| `ENSEMBLE_SESSION_WINDOWS_UTC` | `06:00-19:00` | `06:00-19:00` | `06:00-19:00` | `True` |
| `IOFS_SESSION_FILTER_ENABLED` | `True` | `True` | `True` | `True` |
| `IOFS_SESSION_WINDOWS_UTC` | `07:00-10:00,13:00-16:00` | `07:00-10:00,13:00-16:00` | `07:00-10:00,13:00-16:00` | `True` |
| `KILL_SWITCH_CLOSE_POSITIONS` | `true` | `True` | `True` | `True` |
| `DAILY_MAX_LOSS_USDT` | `50` | `50.0` | `50.0` | `True` |
| `MAX_OPEN_POSITIONS` | `3` | `3` | `3` | `True` |
| `KILL_SWITCH` | `None` | `runtime state; see daily_state.kill` | `None` | `None` |
| `CIRCUIT_BREAKER` | `None` | `runtime registry; see circuit_breaker section` | `None` | `None` |
| `DAILY_LOSS_LIMIT` | `50` | `50.0` | `50.0` | `True` |

## Session State

- Current UTC time: `2026-07-15T21:17:01.582842+00:00`
- Ensemble runtime window `06:00-19:00` allowed now: `False`
- IOFS shadow window `07:00-10:00,13:00-16:00` allowed now: `False`
- Replay/IOFS windows are separate from the ensemble runtime window.

## Evidence

- Latest trace: `2026-07-15T21:16:46.173812+00:00`
- Active bot: `None` / `None`
- Latest 100 trace blockers: `{'session_blocked': 32, 'symbol_blocked': 0, 'regime_blocked': 12, 'threshold_blocked': 0, 'risk_budget_blocked': 0, 'max_daily_trades_blocked': 0, 'max_open_positions_blocked': 0, 'circuit_breaker_blocked': 0, 'kill_switch_blocked': 0, 'spread_blocked': 0, 'volume_blocked': 0, 'market_data_failed': 0, 'strategy_no_signal': 56, 'iofs_blocked': 0, 'ml_blocked': 0, 'executor_error': 0}`
- Wider in-session decisions: `{'HOLD': 763}`
- IOFS recent modes: `{'shadow': 100}`
- IOFS `blocked_trade=true` count: `0`
- Current daily state: `{'day': '2026-07-15', 'realized_pnl': 0.0, 'kill': 0, 'trade_count': 0, 'last_updated_at': '2026-07-15T17:10:44.333617+00:00', 'consecutive_losses': 0, 'consec_loss_cooldown_until_ms': 0}`
- Circuit states visible to diagnostic process: `{}`

## Market Data

The public probe and the running bot are reported separately. A blocked diagnostic probe does not imply the runner feed failed.

- `BTCUSDT` runtime feed healthy: `True`; public probe healthy: `True`
  - Runtime evidence: `{'fresh_15m_strategy_trace': True, 'latest_trace_at': '2026-07-15T21:16:46.173812+00:00', 'latest_trace_price': 100000.0, 'complete_4h_1h_15m_iofs_fetch_today': False, 'iofs_evidence': {'timestamp_utc': '2026-06-20T08:32:37.646444+00:00', 'reason': 'STRUCTURE_NOT_ACTIVE', 'trend_adx': 25.454712908317635, 'proves_complete_4h_1h_15m_fetch': True}}`
  - `15m`: `OK`, candles `120`, latest closed `2026-07-15T21:14:59.999000+00:00`
  - `1h`: `OK`, candles `50`, latest closed `2026-07-15T20:59:59.999000+00:00`
  - `4h`: `OK`, candles `220`, latest closed `2026-07-15T19:59:59.999000+00:00`
- `ETHUSDT` runtime feed healthy: `False`; public probe healthy: `True`
  - Runtime evidence: `{'fresh_15m_strategy_trace': False, 'latest_trace_at': '2026-07-15T20:56:26.569847+00:00', 'latest_trace_price': 1923.2, 'complete_4h_1h_15m_iofs_fetch_today': False, 'iofs_evidence': {'timestamp_utc': '2026-06-20T08:32:11.729006+00:00', 'reason': 'STRUCTURE_NOT_ACTIVE', 'trend_adx': 26.28629061850131, 'proves_complete_4h_1h_15m_fetch': True}}`
  - `15m`: `OK`, candles `120`, latest closed `2026-07-15T21:14:59.999000+00:00`
  - `1h`: `OK`, candles `50`, latest closed `2026-07-15T20:59:59.999000+00:00`
  - `4h`: `OK`, candles `220`, latest closed `2026-07-15T19:59:59.999000+00:00`

## Safety Status

- Execution mode remains `paper`.
- ML remains `False`.
- IOFS remains `shadow`.
- No configuration was changed and no order path was called.
