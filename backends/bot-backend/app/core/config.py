from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Binance API
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    BINANCE_FAPI_BASE_URL: str = "https://testnet.binancefuture.com"
    BINANCE_RECV_WINDOW: int = 5000
    BINANCE_ENV: str = "testnet"

    # Bybit API (Added)
    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""
    BYBIT_ENV: str = "testnet"  # mainnet | testnet
    BYBIT_BASE_URL: str = ""    # Optional override
    BYBIT_RECV_WINDOW: int = 5000
    BYBIT_TIMEOUT_SECONDS: int = 10
    BYBIT_ACCOUNT_TYPE: str = "" # e.g. "UNIFIED"
    BYBIT_CATEGORY_DEFAULT: str = "linear"

    # BingX API (Future Placeholder)
    BINGX_API_KEY: str = ""
    BINGX_API_SECRET: str = ""
    BINGX_ENV: str = "testnet"
    BINGX_BASE_URL: str = ""
    BINGX_TIMEOUT_SECONDS: int = 10
    
    # Engine Authentication
    ENGINE_API_KEY: str = "default-engine-key"

    # --- Security / Auth (Added for Monolith) ---
    SECRET_KEY: str = "changeme_in_production_secret_key"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 14
    CREDENTIAL_KEY: str = "changeme_in_production_credential_key_32bytes!!!!"

    # --- Email Configuration ---
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM_EMAIL: str = ""
    SMTP_FROM_NAME: str = "CosmicForge"
    EMAIL_ENABLED: bool = False

    # --- Billing / Stripe ---
    STRIPE_SECRET_KEY: str = ""
    STRIPE_WEBHOOK_SECRET: str = ""
    
    # Trading Configuration
    EXECUTION_MODE: str = "paper"
    TRADE_SYMBOLS: str = "BTCUSDT,ETHUSDT"
    MAX_SYMBOLS: int = 2
    DEFAULT_LEVERAGE: int = 3
    MIN_LEVERAGE: int = 1
    # SAFETY: minimum 50 USDT to avoid exchange min-notional failures at 3x leverage.
    TRADE_USDT_PER_ORDER: float = 50.0
    DAILY_MAX_LOSS_USDT: float = 50.0
    MIN_NOTIONAL_USDT: float = 5.0
    DEFAULT_INTERVAL: str = "15m"
    SIGNAL_INTERVAL: str = "15m"
    HIGHER_TIMEFRAME_INTERVAL: str = "1h"
    EXECUTION_MONITOR_INTERVAL: str = "1m"
    EXECUTION_STALE_DATA_BUFFER_MS: int = 180000
    EXECUTION_STALE_DATA_VERIFY_ATTEMPTS: int = 2
    PAPER_SLIPPAGE_BPS: float = 2.0
    PAPER_FEE_BPS: float = 4.0
    STOP_LOSS_PCT: float = 0.02
    TAKE_PROFIT_PCT: float = 0.036  # 0.036/0.02 = 1.8 R:R matching MIN_RISK_REWARD
    TRADE_MODE: str = "normal"
    STRATEGY_NAME: str = "master_ensemble"
    STRATEGY_PARAMS_JSON: str = ""

    
    # Runner Configuration
    RUN_INTERVAL_SECONDS: int = 10
    RUN_MAX_SYMBOLS: int = 10
    
    # Symbol Leverage Mapping
    SYMBOL_LEVERAGE_MAP: str = ""
    SYMBOL_USDT_MAP: str = ""
    
    # Live Trading
    LIVE_SYMBOLS: str = ""
    MAX_LIVE_TRADES_PER_CYCLE: int = 1

    # TradingView Phase 6 -- Live-Mode Limited TradingView Candidate Mode.
    # Safe defaults: disabled, BUY/SELL only, restricted symbols, small caps,
    # mandatory SL/TP, and auto-disable on critical invariant failure.
    TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED: bool = False
    TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED: bool = False
    TRADINGVIEW_ALLOWED_ACTIONS: str = "BUY,SELL"
    TRADINGVIEW_ALLOW_CLOSE: bool = False
    TRADINGVIEW_ALLOW_REVERSE: bool = False
    TRADINGVIEW_ALLOW_REDUCE: bool = False
    TRADINGVIEW_ALLOW_CANCEL: bool = False
    TRADINGVIEW_ALLOW_EXTERNAL_SLTP: bool = False
    TRADINGVIEW_ALLOW_EXTERNAL_SIZE: bool = False
    TRADINGVIEW_ALLOW_RISK_OVERRIDE: bool = False
    TRADINGVIEW_MAX_TRADE_USDT_CAP: float = 150.0
    TRADINGVIEW_MAX_SIGNALS_PER_HOUR: int = 5
    TRADINGVIEW_MAX_SIGNALS_PER_DAY: int = 20
    TRADINGVIEW_MAX_EXECUTIONS_PER_DAY: int = 3
    TRADINGVIEW_MAX_QUEUE_PER_CYCLE: int = 1
    TRADINGVIEW_ALLOWED_SYMBOLS: str = "BTCUSDT,ETHUSDT,BNBUSDT"
    TRADINGVIEW_REQUIRE_SLTP_PROTECTION: bool = True
    TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL: bool = True
    TRADINGVIEW_SAFETY_LOCKOUT_ENABLED: bool = False
    TRADINGVIEW_SAFETY_LOCKOUT_REASON: str = ""
    # Legacy/pre-Phase-6 processor flags retained for backward-compatible
    # environment gating; Phase 6 limited mode still requires its own opt-in.
    TRADINGVIEW_TESTNET_ONLY: bool = True
    TRADINGVIEW_QUEUE_MAX_PER_CYCLE: int = 3
    TRADINGVIEW_LIVE_MODE_PROOF_ENABLED: bool = False
    TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED: bool = False
    TRADINGVIEW_ALLOW_PAPER_LIVE_MODE: bool = False
    PAPER_TRADING_MODE: bool = False

    # Dynamic symbol universe diagnostics (shadow-only by default).
    # These settings must never be used as executor allowlists unless a later
    # hybrid-live phase explicitly wires that path.
    DYNAMIC_UNIVERSE_SHADOW_ENABLED: bool = False
    DYNAMIC_UNIVERSE_BASE_URL: str = "https://fapi.binance.com"
    DYNAMIC_UNIVERSE_SHADOW_EVAL_TOP_N: int = 30
    DYNAMIC_UNIVERSE_SHADOW_INTERVAL_SECONDS: int = 300
    DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME_USDT: float = 50_000_000.0
    DYNAMIC_UNIVERSE_MAX_SPREAD_BPS: float = 10.0
    DYNAMIC_UNIVERSE_HTTP_TIMEOUT_SECONDS: int = 10
    DYNAMIC_UNIVERSE_REQUIRE_KLINES: bool = True
    DYNAMIC_UNIVERSE_KLINE_INTERVAL: str = "1m"
    DYNAMIC_UNIVERSE_KLINE_LIMIT: int = 2
    DYNAMIC_UNIVERSE_KLINE_CACHE_SECONDS: int = 3600
    SYMBOL_UNIVERSE_MODE: str = "static"  # static | dynamic_shadow | auto_top_n
    AUTO_SYMBOL_SELECTION_ENABLED: bool = False
    AUTO_SYMBOL_TOP_N: int = 20
    AUTO_SYMBOL_MAX_PER_CYCLE: int = 20
    AUTO_SYMBOL_REBALANCE_INTERVAL_HOURS: int = 24
    AUTO_SYMBOL_DENYLIST: str = ""
    AUTO_SYMBOL_ALLOW_MANUAL_REVIEW: bool = False
    AUTO_SYMBOL_PROMOTION_ENABLED: bool = False
    AUTO_SYMBOL_PROMOTION_REQUIRE_ADMIN_APPROVAL: bool = False
    AUTO_SYMBOL_AUTO_DEMOTION_ENABLED: bool = True
    AUTO_SYMBOL_SCORING_MODEL_CHANGED_AT: str = ""
    AUTO_SYMBOL_PROMOTION_MIN_HOURS: int = 72
    AUTO_SYMBOL_PROMOTION_MIN_RANKING_RUNS: int = 100
    AUTO_SYMBOL_PROMOTION_MIN_TRADE_SYMBOLS: int = 10
    AUTO_SYMBOL_PROMOTION_TRADE_RUN_RATIO: float = 0.60
    AUTO_SYMBOL_PROMOTION_TOPN_STABILITY: float = 0.70
    AUTO_SYMBOL_PROMOTION_STABILITY_HOURS: int = 24
    AUTO_SYMBOL_PROMOTION_MIN_WOULD_PASS: int = 3
    AUTO_SYMBOL_PROMOTION_MIN_CONFIDENCE_SAMPLES: int = 20
    AUTO_SYMBOL_PROMOTION_MIN_AVG_CONFIDENCE: float = 0.25
    AUTO_SYMBOL_PROMOTION_MIN_QUOTE_VOLUME: float = 50_000_000.0
    AUTO_SYMBOL_PROMOTION_MAX_SPREAD_BPS: float = 6.0
    AUTO_SYMBOL_PROMOTION_REQUIRE_CANDLE_SUFFICIENCY: bool = True
    AUTO_SYMBOL_PROMOTION_REQUIRE_FUNDING_STABILITY: bool = False
    AUTO_SYMBOL_PROMOTION_SUBMIT_UNKNOWN_TTL_SECONDS: int = 900
    AUTO_SYMBOL_DEMOTION_FALLBACK_MODE: str = "dynamic_shadow"
    AUTO_SYMBOL_DEMOTION_MAX_ORDER_FAILURE_RATE: float = 0.10
    AUTO_SYMBOL_DEMOTION_MAX_SLIPPAGE_BPS: float = 20.0
    AUTO_SYMBOL_DEMOTION_MAX_DRAWDOWN_PCT: float = 5.0
    AUTO_SYMBOL_DEMOTION_NO_TRADE_CANDIDATE_HOURS: int = 6
    AUTO_SYMBOL_DEMOTION_STABILITY_MIN: float = 0.50
    
    # Forex Trading (Fallback - forex_config.allowlist is primary)
    FOREX_SYMBOLS: str = ""
    
    # Additional Trading Parameters
    COOLDOWN_SECONDS: int = 120
    MIN_HOLD_TIME_SECONDS: int = 0
    # SAFETY: 0 = position adding disabled (martingale protection).
    # Do not increase without a dedicated, validated add-position system.
    MAX_ADDS_PER_POSITION: int = 0
    SL_COOLDOWN_SECONDS: int = 600
    
    # Risk Configuration
    MAX_TRADES_DAILY: int = 6
    MAX_OPEN_POSITIONS: int = 3
    MAX_WEEKLY_DRAWDOWN_PCT: float = 5.0
    MAX_MONTHLY_DRAWDOWN_PCT: float = 10.0
    MAX_CONSECUTIVE_LOSSES: int = 3  # Legacy — use soft/hard below

    # D-1: Consecutive loss pause (soft + hard)
    MAX_CONSECUTIVE_LOSSES_SOFT: int = 3    # Losses before 2-hour pause
    CONSECUTIVE_LOSS_COOLDOWN_MINUTES: int = 120
    MAX_CONSECUTIVE_LOSSES_HARD: int = 5    # Losses before rest-of-day pause

    # SAFETY: true = close all open positions when daily loss kill switch fires.
    KILL_SWITCH_CLOSE_POSITIONS: bool = True

    # Trade Quality Gate
    MIN_CONFIDENCE_THRESHOLD: float = 0.70
    MIN_RISK_REWARD: float = 1.8

    # D-3: ATR fixed sizing protection
    MIN_STOP_ATR_MULTIPLIER: float = 0.5   # Stop distance must be >= 0.5 × ATR
    MAX_RISK_PER_TRADE_PCT: float = 1.0    # Max % of equity risked per trade

    # ── FIX-D: Break-even buffer fraction ────────────────────────────────────
    # After TP1, BE stop = entry + max(BREAK_EVEN_BUFFER_FRACTION * SL_distance, fee_buffer).
    # Default 0.20 = 20% of original SL distance.  Do NOT set to 0.0 — that
    # would revert to exact-entry stops which are immediately noise-stopped.
    BREAK_EVEN_BUFFER_FRACTION: float = 0.20

    # ── FIX-E: Ensemble Execution Gates ─────────────────────────────────────
    # Analysis (2026-06-10) found STRONG_TREND (24% WR) is the primary loss
    # driver, not RANGE (52.9% WR — actually the best regime).
    #
    # ENSEMBLE_MIN_THRESHOLD_FLOOR: dynamic threshold can never go below this.
    #   Default 0.50 — slightly tighter than the historic min floor of 0.40.
    #   Raising to 0.55+ further reduces trade count but improves quality.
    #
    # ENSEMBLE_BLOCKED_REGIMES: comma-separated regime names to suppress.
    #   Empty string (default) = backward-compatible, no regimes blocked.
    #   Recommended value = "STRONG_TREND" (proven by backtest on 314 trades).
    #   NOTE: RANGE is NOT a loss driver — do not add RANGE here without new data.
    #
    # ENSEMBLE_SESSION_FILTER_ENABLED: when True, only trade in UTC windows below.
    #   Default False = backward-compatible.
    #
    # ENSEMBLE_SESSION_WINDOWS_UTC: comma-separated HH:MM-HH:MM windows (UTC only).
    #   Example: "06:00-19:00" or "08:00-11:00,13:00-16:00"
    ENSEMBLE_MIN_THRESHOLD_FLOOR: float = 0.50
    ENSEMBLE_BLOCKED_REGIMES: str = ""          # e.g. "STRONG_TREND" to block
    ENSEMBLE_SESSION_FILTER_ENABLED: bool = False
    ENSEMBLE_SESSION_WINDOWS_UTC: str = "06:00-19:00"

    # IOFS Gate 0 pre-ensemble filter. Disabled/shadow by default; enforce is
    # permitted only for paper/testnet execution in PaperRunner.
    IOFS_GATE_ENABLED: bool = False
    IOFS_GATE_MODE: str = "shadow"  # disabled | shadow | enforce
    IOFS_RISK_PROFILE: str = "balanced"
    IOFS_ALLOWED_SYMBOLS: str = "BTCUSDT,ETHUSDT"
    IOFS_SESSION_FILTER_ENABLED: bool = True
    IOFS_SESSION_WINDOWS_UTC: str = "07:00-10:00,13:00-16:00"

    # Organic ML dataset refresh. Disabled by default and explicitly enabled
    # only in environments prepared for the nightly validation build.
    ORGANIC_DATASET_NIGHTLY_ENABLED: bool = False
    ORGANIC_DATASET_NIGHTLY_TIME_UTC: str = "02:00"
    ORGANIC_DATASET_OUTPUT_PATH: str = "models/datasets/training_v2_organic.parquet"
    ML_RETRY_WATCHER_ENABLED: bool = True
    ML_RETRY_MIN_ORGANIC_ROWS: int = 500
    ML_RETRY_MIN_IOFS_ORGANIC_ROWS: int = 300
    ML_RETRY_MIN_CLOSED_IOFS_TRADES: int = 20

    # Daily Profit Close (Session Management)
    DAILY_CLOSE_ENABLED: bool = True
    DAILY_CLOSE_TIMEZONE: str = "Europe/Rome"
    DAILY_CLOSE_WINDOW_START: str = "23:55"  # HH:MM
    DAILY_CLOSE_WINDOW_END: str = "00:35"    # HH:MM
    DAILY_CLOSE_MIN_PROFIT_USDT: float = 0.0
    DAILY_CLOSE_MIN_PROFIT_PCT: float = 0.1  # 0.1% profit required to triggering close

    # Firebase / Push Notifications
    FIREBASE_SERVER_KEY: str = ""  # Legacy FCM (optional, deprecated)
    FIREBASE_SERVICE_ACCOUNT_PATH: str = ""  # Firebase Admin SDK service account JSON path

    # ── ML Inference (Step 5D-2) ──────────────────────────────────────────────
    ML_ENABLED: bool = False
    ML_SHADOW_MODE: bool = False
    ML_MODEL_PATH: str = ""
    ML_ENCODERS_PATH: str = ""
    ML_METADATA_PATH: str = ""
    ML_SCORE_THRESHOLD: float = 0.30
    ML_LOG_DIR: str = "models/logs"
    # Phase 1 controlled activation floor.  0.0 = disabled (pure shadow).
    # Set to 0.10 to hard-block extreme low-quality signals even in shadow mode.
    # Do NOT set ML_SHADOW_MODE=False until shadow evaluation is validated.
    ML_HARD_BLOCK_FLOOR: float = 0.0

    # ── ML Dataset Feature Policy (Phase E — 2026-04-26) ─────────────────────
    # Controls which feature groups are included when building training datasets.
    # These flags do NOT affect the live runtime scorer — they only gate dataset
    # columns during offline build_dataset.py runs.
    ML_EVENT_FEATURES_ENABLED: bool = True
    ML_MARKET_REACTION_FEATURES_ENABLED: bool = True
    # News validation and raw news features are excluded until leakage safety is
    # proven at scale.  Flip to True only when explicitly approved.
    ML_NEWS_VALIDATION_FEATURES_ENABLED: bool = False
    ML_RAW_NEWS_FEATURES_ENABLED: bool = False

    # ── Shadow Trading System (Step 5E-1) ─────────────────────────────────────
    SHADOW_ENABLED: bool = False
    SHADOW_CAPTURE_ML_BLOCKED: bool = True
    SHADOW_CAPTURE_THRESHOLD_BLOCKED: bool = True
    SHADOW_CAPTURE_CORRELATION_BLOCKED: bool = True
    SHADOW_CAPTURE_ALREADY_OPEN: bool = True
    SHADOW_CAPTURE_EXECUTOR_REJECTED: bool = True
    SHADOW_CAPTURE_SAFETY_BLOCKED: bool = False
    SHADOW_CAPTURE_NEAR_MISS: bool = True
    SHADOW_NEAR_MISS_FACTOR: float = 0.90
    SHADOW_EXPIRY_BARS: int = 48
    SHADOW_MAX_EVAL_PER_PASS: int = 20
    SHADOW_ASSUMED_FEE_RATE: float = 0.001
    SHADOW_CYCLE_SUMMARY: bool = True

    # ── Event Awareness (Phase 1) ─────────────────────────────────────────────
    # Master switch — all event-blackout logic is disabled when False.
    EVENT_FILTER_ENABLED: bool = True
    # When True, block new entries during high-impact event windows.
    # When False, high-impact events only log — no execution block.
    NEWS_TRADING_ENABLED: bool = False
    # Minutes before scheduled event start to begin global block (high impact)
    HIGH_IMPACT_BLACKOUT_MINUTES_BEFORE: int = 30
    # Minutes after scheduled event start to lift global block (high impact)
    HIGH_IMPACT_BLACKOUT_MINUTES_AFTER: int = 15
    # Minutes before/after for medium-impact events (log only, no hard block)
    MEDIUM_IMPACT_BLACKOUT_MINUTES_BEFORE: int = 5
    MEDIUM_IMPACT_BLACKOUT_MINUTES_AFTER: int = 5
    # Whether to allow any new entries during an active news window
    ALLOW_TRADING_DURING_NEWS: bool = False
    # Calendar data source: "manual" (DB seed only) or future provider key
    EVENT_CALENDAR_PROVIDER: str = "manual"
    # When True and feed is stale, block all new entries as a safety measure
    EVENT_FILTER_FAILSAFE_ENABLED: bool = True
    # Hours after which the event calendar is considered stale
    EVENT_FEED_STALE_HOURS: int = 24
    # In Binance testnet/demo validation, stale manually-seeded feeds warn only;
    # production remains fail-closed unless this is explicitly changed.
    EVENT_FILTER_STALE_FAILSAFE_TESTNET_WARN_ONLY: bool = True

    # ── Event Ingestion (auto-fetch from external sources) ───────────────────
    # Master switch — enables background worker that fetches events from APIs.
    # When False, events must be seeded manually via admin API.
    EVENT_INGESTION_ENABLED: bool = False
    # How often the ingestion worker fetches new events (hours).
    EVENT_INGESTION_INTERVAL_HOURS: int = 6
    # Macro events: configurable JSON endpoint (ForexFactory-compatible format).
    # Leave empty to disable macro auto-fetch. User must supply their own feed URL.
    EVENT_MACRO_JSON_URL: str = ""
    # CoinMarketCal API key (https://developers.coinmarketcal.com/).
    # Empty = CoinMarketCal disabled.
    COINMARKETCAL_API_KEY: str = ""
    # When True, fetch crypto events from CoinMarketCal.
    COINMARKETCAL_ENABLED: bool = False
    # Minimum CoinMarketCal event importance score (1–5) to ingest.
    COINMARKETCAL_MIN_IMPORTANCE: int = 3

    # ── Market Reaction Layer (Phase 2) ──────────────────────────────────────
    # Master switch — disabling this stops all reaction tracking with zero overhead.
    MARKET_REACTION_LAYER_ENABLED: bool = False
    # When True, metrics are collected and logged but never influence trading.
    MARKET_REACTION_SHADOW_MODE: bool = True
    REACTION_TRACKING_ENABLED: bool = False
    # How far before the event to start collecting snapshots (minutes).
    REACTION_PRE_EVENT_MINUTES: int = 60
    # How far after the event to keep collecting snapshots (minutes).
    # Phase 2 contract ends at +60m: POST_5 / POST_15 / POST_30 / POST_60.
    REACTION_POST_EVENT_MINUTES: int = 60
    # How often the snapshot collector fires within a window (seconds).
    REACTION_SNAPSHOT_INTERVAL_SECONDS: int = 60
    # When True AND REACTION_ALLOW_RISK_INFLUENCE=True, extend blackout if
    # volatility expansion exceeds the threshold after an event.
    REACTION_EXTEND_BLACKOUT_ON_VOL_SPIKE: bool = True
    # ATR expansion ratio that triggers a VOLATILITY_SPIKE or WHIPSAW classification.
    REACTION_VOL_SPIKE_THRESHOLD: float = 2.5
    # Volume spike ratio considered abnormal (event_volume / avg_pre_event_volume).
    REACTION_VOLUME_SPIKE_THRESHOLD: float = 3.0
    # Spread widening ratio that triggers LIQUIDITY_SHOCK classification.
    REACTION_SPREAD_WIDENING_THRESHOLD: float = 2.0
    # Safety gate: when False (default), reaction layer NEVER blocks or modifies trades.
    # Set True only after shadow-mode data quality has been validated.
    REACTION_ALLOW_RISK_INFLUENCE: bool = False

    # ── News Intelligence Layer (Phase 3) ────────────────────────────────────
    # Master switch — all news intelligence is disabled when False.
    NEWS_INTELLIGENCE_ENABLED: bool = False
    # When True (default), system observes and stores only — never affects trading.
    NEWS_INTELLIGENCE_SHADOW_MODE: bool = True
    # Guard: news signals CANNOT open or close trades in Phase 3.
    NEWS_TRADING_ENABLED: bool = False
    NEWS_SIGNAL_CAN_BLOCK_TRADES: bool = False
    NEWS_SIGNAL_CAN_OPEN_TRADES: bool = False
    NEWS_SIGNAL_CAN_CLOSE_TRADES: bool = False
    # Comma-separated active providers: cryptopanic, benzinga
    NEWS_PROVIDERS: str = "cryptopanic"
    # API keys for providers (empty = provider disabled)
    CRYPTOPANIC_API_KEY: str = ""
    BENZINGA_API_KEY: str = ""
    # How often the ingestion worker polls providers (seconds).
    NEWS_POLL_INTERVAL_SECONDS: int = 60
    # Jaccard similarity threshold for deduplication clustering.
    NEWS_DEDUP_SIMILARITY_THRESHOLD: float = 0.82
    # Rolling window for dedup comparison (hours).
    NEWS_DEDUP_WINDOW_HOURS: int = 6
    # Minimum source reliability score to include in a signal.
    NEWS_MIN_SOURCE_RELIABILITY_FOR_SIGNAL: float = 0.70
    # Minimum sentiment confidence to emit a signal.
    NEWS_MIN_CONFIDENCE_FOR_SIGNAL: float = 0.75
    # Social media ingestion — all off by default.
    SOCIAL_MEDIA_INGESTION_ENABLED: bool = False
    TWITTER_INGESTION_ENABLED: bool = False
    REDDIT_INGESTION_ENABLED: bool = False
    TELEGRAM_INGESTION_ENABLED: bool = False
    # Manipulation detection thresholds.
    NEWS_DOMAIN_FLOOD_THRESHOLD: int = 5   # items from same domain within window
    NEWS_DOMAIN_FLOOD_WINDOW_MINUTES: int = 10
    # How many hours of raw items to retain before purging.
    NEWS_RETENTION_HOURS: int = 168        # 7 days

    # ── Real-Time News Ingestion Hardening ───────────────────────────────
    # Master switch — all real-time providers disabled when False
    REAL_TIME_NEWS_ENABLED: bool = False
    REAL_TIME_NEWS_SHADOW_MODE: bool = True       # hard-locked; never set False
    # Per-provider enable flags (all off until API keys provided)
    CRYPTOPANIC_ENABLED: bool = False
    BENZINGA_ENABLED: bool = False
    REUTERS_ENABLED: bool = False
    GENERIC_NEWS_API_ENABLED: bool = False
    # Provider API keys (empty = provider auto-disabled)
    REUTERS_API_KEY: str = ""
    GENERIC_NEWS_API_KEY: str = ""
    GENERIC_NEWS_API_URL: str = ""
    # Real-time operational parameters
    REAL_TIME_NEWS_POLL_INTERVAL_SECONDS: int = 30
    REAL_TIME_NEWS_MAX_ITEMS_PER_FETCH: int = 100
    REAL_TIME_NEWS_PROVIDER_TIMEOUT_SECONDS: int = 10
    REAL_TIME_NEWS_MIN_RELIABILITY_SCORE: float = 0.70
    REAL_TIME_NEWS_MIN_CLUSTER_CONFIDENCE: float = 0.75
    # Hard safety invariants — must remain False in Phase 3
    REAL_TIME_NEWS_CAN_OPEN_TRADES: bool = False
    REAL_TIME_NEWS_CAN_CLOSE_TRADES: bool = False
    REAL_TIME_NEWS_CAN_BLOCK_TRADES: bool = False

    # ---------------------------------------------------------------------
    # Section F — Product Safety Gates
    # ---------------------------------------------------------------------
    # Manual confirmation that Sections A–E are implemented + deployed in the
    # running environment. User-capital activation must not be possible until
    # this is explicitly set true by operators.
    SECTIONS_A_TO_E_CONFIRMED: bool = False

    # ── Real News Source Connection (Phase 3 — RSS layer) ────────────────
    # Master switch — enables live RSS ingestion. Off by default.
    NEWS_REAL_SOURCE_INGESTION_ENABLED: bool = False
    NEWS_RSS_INGESTION_ENABLED: bool = True
    NEWS_API_INGESTION_ENABLED: bool = False
    NEWS_MANUAL_IMPORT_ENABLED: bool = True
    NEWS_RSS_TIMEOUT_SECONDS: int = 15
    NEWS_RSS_MAX_ITEMS_PER_SOURCE: int = 50
    NEWS_SOURCE_STALE_MINUTES: int = 60
    NEWS_REQUIRE_API_KEYS: bool = False
    NEWS_SHADOW_ONLY: bool = True   # hard-enforced in signal service

    # Event/News automatic runtime mode controller (Prompt 2).
    # This controller may only promote SHADOW -> ADVISORY in this phase.
    # Both modes are ANNOTATE_ONLY and cannot affect execution.
    EVENT_NEWS_MODE_CONTROLLER_ENABLED: bool = True
    EVENT_NEWS_AUTO_PROMOTION_ENABLED: bool = True
    EVENT_NEWS_AUTO_DEMOTION_ENABLED: bool = True
    EVENT_NEWS_ADVISORY_MIN_RUNTIME_HOURS: float = 24.0
    EVENT_NEWS_ADVISORY_LOOKBACK_HOURS: int = 24
    EVENT_NEWS_ADVISORY_MIN_RAW_NEWS_ITEMS: int = 1
    EVENT_NEWS_ADVISORY_MIN_CLUSTERS: int = 1
    EVENT_NEWS_ADVISORY_MIN_PROVIDER_HEALTH_ROWS: int = 1
    EVENT_NEWS_ADVISORY_MAX_FAILED_PROVIDERS: int = 999999
    EVENT_NEWS_ADVISORY_MAX_UNSAFE_SIGNALS: int = 0
    EVENT_NEWS_RISK_LITE_MIN_ADVISORY_DAYS: float = 7.0
    EVENT_NEWS_RISK_LITE_MIN_VALIDATED_CLUSTERS: int = 200
    EVENT_NEWS_RISK_LITE_MAX_FALSE_SIGNAL_RATIO: float = 0.20
    EVENT_NEWS_RISK_LITE_MAX_PROVIDER_FAILURE_RATE: float = 0.20
    EVENT_NEWS_ALLOW_DEV_RISK_LITE_PROMOTION: bool = False
    EVENT_NEWS_DEV_MIN_ADVISORY_MINUTES: int = 30
    EVENT_NEWS_DEV_MIN_CLUSTERS: int = 100
    EVENT_NEWS_DEV_MIN_SIGNALS: int = 100
    EVENT_NEWS_RISK_LITE_MAX_SIZE_REDUCTION_PCT: float = 0.25
    EVENT_NEWS_RISK_LITE_MAX_CONFIDENCE_PENALTY: float = 0.15
    EVENT_NEWS_RISK_LITE_MAX_DELAY_SECONDS: int = 300
    EVENT_NEWS_OVERINFLUENCE_WINDOW_HOURS: int = 6
    EVENT_NEWS_OVERINFLUENCE_MAX_SIZE_REDUCTION_RATE: float = 0.20
    EVENT_NEWS_OVERINFLUENCE_MAX_DELAY_RATE: float = 0.10
    EVENT_NEWS_OVERINFLUENCE_MAX_AVG_SIZE_REDUCTION: float = 0.15

    # ── TradingView External Signal Runner (Phase 4) ──────────────────────────
    # Master switch — set True to enable Phase 4 queue processing.
    # Phase 3 webhook ingestion works regardless of this flag.
    TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED: bool = False
    # Safety guard — when True, Phase 4 only runs on non-live/sandbox-style environments.
    # Must remain True until live-trading validation is complete.
    TRADINGVIEW_TESTNET_ONLY: bool = True
    # Max queue rows processed per bot per cycle (prevents cycle budget overrun).
    TRADINGVIEW_QUEUE_MAX_PER_CYCLE: int = 3
    # Live-mode TradingView proof unlock (explicit opt-in safety):
    #   Factor 1: TRADINGVIEW_TESTNET_ONLY=false
    #   Factor 2: TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=true
    #   Factor 3: TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=true
    # ALL THREE must be set to allow Phase 4 in a live/mainnet environment,
    # unless using the legacy flags below during migration.
    # Default: false — live/mainnet environments are blocked by default.
    TRADINGVIEW_LIVE_MODE_PROOF_ENABLED: bool = False
    TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED: bool = False
    # Legacy explicit live-mode unlock flags retained for compatibility with
    # existing deployments. Prefer the TRADINGVIEW_LIVE_MODE_* flags above.
    TRADINGVIEW_ALLOW_PAPER_LIVE_MODE: bool = False
    PAPER_TRADING_MODE: bool = False

    # Database
    DATABASE_URL: str = "sqlite:///../bot.db"
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Prevent startup crash when extra vars exist in .env
    
    def validate_runtime(self):
        """Validate configuration at runtime"""
        warnings = []

        if not self.BINANCE_API_KEY:
            warnings.append("BINANCE_API_KEY is not set")
        if not self.BINANCE_API_SECRET:
            warnings.append("BINANCE_API_SECRET is not set")
        if self.ENGINE_API_KEY == "default-engine-key":
            warnings.append("ENGINE_API_KEY is using default value - change in production!")

        return warnings

    def validate_safety(self) -> tuple[bool, list[str], list[str]]:
        """
        Safety configuration validation.
        Returns (all_pass, failures, warnings).
        Failures are critical — bot must not trade if any exist in live/user-capital mode.
        """
        failures = []
        warnings_list = []

        if self.DEFAULT_INTERVAL == "1m":
            failures.append(
                f"DEFAULT_INTERVAL=1m rejected as signal interval. "
                f"Use 15m or higher. 1m is for execution monitoring only."
            )

        if self.MAX_ADDS_PER_POSITION != 0:
            failures.append(
                f"MAX_ADDS_PER_POSITION={self.MAX_ADDS_PER_POSITION} — must be 0. "
                f"Position adding creates martingale exposure."
            )

        if self.MAX_WEEKLY_DRAWDOWN_PCT <= 0:
            failures.append(
                f"MAX_WEEKLY_DRAWDOWN_PCT={self.MAX_WEEKLY_DRAWDOWN_PCT} — must be > 0. "
                f"Weekly drawdown protection is disabled."
            )

        if self.MAX_MONTHLY_DRAWDOWN_PCT <= 0:
            failures.append(
                f"MAX_MONTHLY_DRAWDOWN_PCT={self.MAX_MONTHLY_DRAWDOWN_PCT} — must be > 0. "
                f"Monthly drawdown protection is disabled."
            )

        if self.DAILY_MAX_LOSS_USDT <= 0:
            failures.append(
                f"DAILY_MAX_LOSS_USDT={self.DAILY_MAX_LOSS_USDT} — must be > 0. "
                f"Daily loss protection is disabled."
            )

        if self.MAX_TRADES_DAILY > 10:
            warnings_list.append(
                f"MAX_TRADES_DAILY={self.MAX_TRADES_DAILY} is high — recommend <= 5 during validation."
            )

        if self.MAX_OPEN_POSITIONS > 3:
            warnings_list.append(
                f"MAX_OPEN_POSITIONS={self.MAX_OPEN_POSITIONS} is high — recommend <= 2 during validation."
            )

        if self.MIN_CONFIDENCE_THRESHOLD < 0.70:
            warnings_list.append(
                f"MIN_CONFIDENCE_THRESHOLD={self.MIN_CONFIDENCE_THRESHOLD} is below recommended 0.70."
            )

        if not self.KILL_SWITCH_CLOSE_POSITIONS:
            failures.append(
                f"KILL_SWITCH_CLOSE_POSITIONS=false — open positions will keep losing after kill switch. Set to true."
            )

        if not self.EVENT_FILTER_ENABLED:
            failures.append(
                f"EVENT_FILTER_ENABLED=false — bot can trade through high-impact news events."
            )

        if self.TRADE_USDT_PER_ORDER < 50.0:
            failures.append(
                f"TRADE_USDT_PER_ORDER={self.TRADE_USDT_PER_ORDER} — minimum 50 USDT required to avoid exchange notional failures."
            )

        all_pass = len(failures) == 0
        return all_pass, failures, warnings_list


settings = Settings()
