"""Money in the legacy schema: which REAL columns hold money, and how a stored value becomes an exact Decimal.

The legacy runtime (``shared_lib.persistence.db.DB`` and the ``*_schema`` modules) stores every number with a
fractional part as SQLite ``REAL`` -- an IEEE-754 binary64 float. Moving that schema to PostgreSQL (EXT-DB-01)
needs two decisions that this module makes once, explicitly and reviewably:

1. **Classification.** Every ``REAL`` column of the legacy schema is either MONETARY -- money or an exchange
   quantity/price: balances, equity, wallet, notional, fees, funding amounts, margin, PnL, reservations, prices,
   quantities, stop/target prices, exchange precision (tick/step/min notional), volumes, amounts -- and becomes
   unconstrained ``NUMERIC`` (see "Why not NUMERIC(38,18)" below); or NON_MONETARY -- ratios, percentages, basis points, scores, probabilities, confidences,
   thresholds, multipliers, leverage, funding *rates*, indicators (ATR, ADX, slopes), derived statistics
   (averages, expectancies, drawdown fractions), latencies, durations, API rate weights -- and stays
   ``DOUBLE PRECISION`` (binary64 is exactly what SQLite stored; no conversion is needed).
   Both maps below are literal and keyed by table. ``classify_real_column`` raises for a column in neither, and
   ``tests`` build the full legacy schema and fail when a ``REAL`` column appears unclassified.

2. **Conversion.** A legacy value becomes a Decimal with ``to_decimal``:

   * ``float`` -> ``Decimal(repr(x))``: Python's ``repr`` is the SHORTEST decimal string that round-trips to the
     same binary64 (``float(repr(x)) == x``), so ``0.1`` stays ``0.1`` and ``0.1 + 0.2`` stays
     ``0.30000000000000004`` -- nothing is invented and nothing is rounded. ``Decimal(x)`` (the exact binary
     expansion, ``0.1000000000000000055511151231257827...``) and PostgreSQL's own ``float8 -> numeric`` cast
     (which rounds to 15 significant digits) are both wrong for money: the first invents 50 digits nobody
     entered, the second loses digits that were stored (``0.30000000000000004`` -> ``0.3``).
   * ``int`` -> exact; ``Decimal`` -> itself when finite; numeric text -> validated Decimal; ``None`` -> ``None``.
   * NaN / +-inf (float, Decimal or text) -> ``NonFiniteMoney``; anything else (non-numeric text, bytes, bool)
     -> ``MoneyTypeMismatch``. Nothing is coerced to 0 or dropped: a migration reports the row instead.

   ``numeric_misfit`` reports a Decimal that ``NUMERIC(38,18)`` (or any declared precision/scale) cannot hold
   exactly (more than 18 significant fractional digits, or more than 20 integer digits) instead of letting the
   server round it.

**Why not NUMERIC(38,18).** The legacy runtime computes money in binary floats, and a float result carries up to 17
significant digits wherever its magnitude lies: a fee of ``notional * 0.0004`` such as ``0.0062978382397085135`` has
19 fractional digits (about 1.5 % of such fees, measured). ``NUMERIC(38,18)`` would round those at the 18th place,
so the move would not be exact. Unconstrained ``NUMERIC`` holds the shortest decimal of EVERY finite binary64
exactly (subnormals included), so legacy money columns use it (``PG_MONEY_TYPE``). The platform layer
(``shared_lib.persistence.sql``), whose money arrives as exact decimal strings, keeps ``NUMERIC(38,18)``.

Nothing here changes runtime arithmetic; it is the contract the PostgreSQL compatibility layer and the legacy
migration drill bind and verify against.
"""
from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Mapping, Optional

__all__ = [
    "LegacyMoneyError", "NonFiniteMoney", "MoneyTypeMismatch", "MoneyPrecisionLoss", "UnclassifiedRealColumn",
    "MONETARY", "NON_MONETARY", "MONETARY_COLUMNS", "NON_MONETARY_REAL_COLUMNS", "PG_MONEY_TYPE",
    "PG_NON_MONEY_TYPE", "PLATFORM_MONEY_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", "is_real_type", "classify_real_column",
    "is_monetary", "postgres_real_type", "to_decimal", "numeric_misfit", "to_numeric_exact", "decimal_text",
]

MONETARY, NON_MONETARY = "MONETARY", "NON_MONETARY"
NUMERIC_PRECISION, NUMERIC_SCALE = 38, 18
#: the PostgreSQL type of a legacy monetary REAL column: unconstrained, so every stored float survives exactly
PG_MONEY_TYPE = "NUMERIC"
#: the platform layer's money type (exact decimal strings in, never floats); kept for reference and misfit checks
PLATFORM_MONEY_TYPE = f"NUMERIC({NUMERIC_PRECISION},{NUMERIC_SCALE})"
PG_NON_MONEY_TYPE = "DOUBLE PRECISION"


class LegacyMoneyError(ValueError):
    """A legacy value that cannot become an exact Decimal. ``code`` is stable for reports."""
    code = "LEGACY_MONEY_ERROR"


class NonFiniteMoney(LegacyMoneyError):
    code = "NON_FINITE_MONEY"


class MoneyTypeMismatch(LegacyMoneyError, TypeError):
    code = "MONEY_TYPE_MISMATCH"


class MoneyPrecisionLoss(LegacyMoneyError):
    code = "MONEY_PRECISION_LOSS"


class UnclassifiedRealColumn(KeyError):
    code = "UNCLASSIFIED_REAL_COLUMN"


def _f(*names: str) -> frozenset:
    return frozenset(names)


# ═════════════════════════════════════════════════════════════════════════════════════ MONETARY (-> NUMERIC)
_MONETARY = {
    "account_equity_daily": _f("open", "high", "low", "close"),
    "account_equity_snapshots": _f("equity", "wallet", "available", "unrealized"),
    "admin_ml_linked_trade_snapshot": _f("realized_pnl"),
    "admin_profitability_daily_summary": _f("total_realized_pnl"),
    "admin_profitability_sizing_events": _f("base_notional", "configured_margin", "final_margin", "final_notional"),
    "admin_profitability_symbol_summary": _f("total_realized_pnl"),
    "affiliate_settings": _f("minimum_payout_amount"),
    "backtest_equity_curve": _f("equity", "balance", "unrealized_pnl", "realized_pnl", "drawdown_usdt", "peak_equity"),
    "backtest_fills": _f("quantity", "entry_price", "fill_price", "fee_usdt", "slippage_usdt", "pnl"),
    "backtest_performance": _f("gross_pnl", "net_pnl", "total_fees", "largest_win", "largest_loss"),
    "backtest_runs": _f("initial_capital", "net_pnl", "gross_pnl", "total_fees"),
    "benchmark_prices": _f("close_price"),
    "bot_daily_state": _f("realized_pnl"),
    "bot_instances": _f("allocation_value", "capital_allocation", "max_position_usdt"),
    "bot_symbol_state": _f("entry_price", "entry_qty", "lifecycle_current_stop", "lifecycle_original_stop"),
    "broker_transfers_cache": _f("amount", "fee"),
    "cati_account_income": _f("income"),
    "cati_account_period_risk": _f("peak_equity", "transfers"),
    "cati_brake_state": _f("opening_equity", "closing_equity", "highest_closing_equity", "daily_loss"),
    "cati_execution_intents": _f("quantity", "limit_price", "reference_price", "stop_price", "filled_quantity",
                                 "average_price"),
    "cati_exit_decisions": _f("suggested_protection_price"),
    "cati_feed_daily_bars": _f("open", "high", "low", "close", "volume", "quote_volume"),
    "cati_instrument_rules": _f("price_tick", "quantity_step", "min_quantity", "max_quantity", "market_max_quantity",
                                "min_notional", "contract_size"),
    "cati_production_daily_risk": _f("opening_wallet"),
    "cati_protection_replacements": _f("requested_stop_price", "old_stop_price", "new_stop_price", "new_quantity"),
    "cati_protection_state": _f("intended_stop_price", "resting_stop_price", "covered_quantity", "position_quantity"),
    "cati_reservation_ledger": _f("quantity", "reference_price", "stop_price", "risk_amount", "notional", "margin",
                                  "filled_quantity", "filled_price"),
    "cati_residual_decisions": _f("entry_reference", "stop", "target", "risk", "entry_price"),
    "cati_residual_inputs": _f("open", "high", "low", "close", "volume"),
    "cati_shadow_accounts": _f("initial_equity", "equity", "peak_equity"),
    "cati_shadow_days": _f("equity", "gross_notional"),
    "cati_shadow_fills": _f("quantity", "price"),
    "cati_shadow_positions": _f("quantity", "stop_price", "mark_price", "entry_price"),
    "cati_user_stream_fills": _f("quantity", "price", "fee", "realized_pnl"),
    "commission_config": _f("cap_amount"),
    "commission_ledger": _f("trade_pnl", "commission_amount"),
    "commission_tiers": _f("min_volume", "max_volume"),
    "daily_close_validation_reports": _f("entry_price", "close_price", "gross_pnl", "fees", "slippage", "net_pnl"),
    "daily_state": _f("realized_pnl"),
    "decision_traces": _f("last_price", "mark_price", "equity", "margin_used", "portfolio_risk_budget",
                          "portfolio_risk_used", "sl_plan", "tp_plan", "fill_price", "fill_qty", "base_size",
                          "final_size", "final_qty", "min_qty", "min_notional"),
    "dynamic_universe_shadow_diagnostics": _f("quote_volume_24h"),
    "entry_protection_events": _f("intended_notional", "sized_notional", "submitted_notional", "filled_notional",
                                  "reference_price", "protected_exposure", "max_exposure_limit"),
    "equity_snapshots": _f("wallet_balance", "equity", "available_balance", "unrealized_pnl", "margin_used"),
    "event_market_snapshots": _f("price", "volume", "candle_open", "candle_high", "candle_low", "candle_close",
                                 "spread", "bid_depth", "ask_depth"),
    "execution_attempts": _f("requested_qty", "requested_notional", "executed_qty", "avg_fill_price",
                             "initial_response_executed_qty", "resolved_executed_qty", "fees"),
    "global_confidence_buckets": _f("net_pnl"),
    "global_regime_performance": _f("net_pnl"),
    "global_strategy_performance": _f("gross_loss", "gross_profit", "net_pnl", "total_fees"),
    "fx_reference_quotes": _f("bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low",
                              "ask_close", "mid_close", "spread_close", "volume"),
    "historical_candles": _f("open", "high", "low", "close", "volume", "quote_volume"),
    "invoices": _f("amount"),
    "market_candles": _f("open", "high", "low", "close", "volume", "quote_volume"),
    "market_event_reactions": _f("price_before_event", "price_at_event", "price_after_event", "price_after_5m",
                                 "price_after_15m", "price_after_30m", "price_after_60m", "event_volume"),
    "market_snapshots": _f("reference_price"),
    "monthly_snapshots": _f("start_equity", "peak_equity", "low_equity"),
    "pending_entries": _f("intended_notional", "sized_notional", "sized_qty", "submitted_notional", "submitted_qty",
                          "filled_notional", "filled_qty", "reference_price"),
    "position_events": _f("quantity", "remaining_qty", "price", "fee", "realized_pnl", "stop_price", "target_price"),
    "position_lifecycle_state": _f("original_stop", "current_stop", "original_tp1", "original_tp2",
                                   "highest_since_entry", "lowest_since_entry", "entry_qty_remaining"),
    "positions": _f("original_qty", "remaining_qty", "realized_qty", "entry_price", "realized_pnl", "fees",
                    "committed_margin", "requested_qty", "broker_executed_qty"),
    "protection_state": _f("daily_loss_today"),
    "revenue_snapshots": _f("subscription_revenue", "commission_revenue", "total_revenue"),
    "run_summary": _f("realized_pnl"),
    "shadow_trade_outcomes": _f("exit_price", "pnl_abs", "pnl_net"),
    "shadow_trades": _f("entry_price", "stop_loss", "take_profit"),
    "signal_candidates": _f("entry_price", "entry_zone_low", "entry_zone_high", "stop_loss", "take_profit_1",
                            "take_profit_2", "take_profit_3"),
    "signal_outcomes": _f("pnl"),
    "signal_pair_metrics": _f("quote_volume_24h", "bid_price", "ask_price"),
    "signal_performance": _f("max_favorable_move", "max_adverse_move"),
    "strategy_performance": _f("net_pnl", "gross_pnl", "fees"),
    "symbol_state": _f("entry_price", "entry_qty", "lifecycle_current_stop", "lifecycle_original_stop", "lifecycle_tp1",
                       "lifecycle_tp2", "lifecycle_highest", "lifecycle_lowest", "lifecycle_qty_remaining"),
    "symbol_universe_rankings": _f("quote_volume_24h", "open_interest"),
    "trade_fills": _f("qty", "price", "fee", "realized_pnl", "entry_price_expected", "stop_loss_price",
                      "remaining_qty", "sl_at_exit", "tp_at_exit", "market_price_used", "gross_pnl", "total_fees",
                      "funding_fees", "net_pnl", "entry_fee", "exit_fee"),
    "trading_decisions": _f("requested_margin", "approved_margin", "requested_notional", "approved_notional",
                            "quantity", "risk_amount", "stop_price", "target_price", "stop_distance",
                            "reward_distance"),
    "trading_signals": _f("entry_price", "entry_zone_low", "entry_zone_high", "stop_loss", "take_profit_1",
                          "take_profit_2", "take_profit_3"),
    "trades": _f("entry_price", "entry_qty", "exit_price", "fees", "initial_stop", "realized_pnl"),
    "tradingview_alerts": _f("price"),
    "universe_members": _f("quote_volume_24h", "last_price"),
    "user_strategy_configs": _f("allocation_value"),
    "users": _f("total_commission"),
    "weekly_snapshots": _f("start_equity", "peak_equity", "low_equity"),
}

# ═════════════════════════════════════════════════════════════════════ NON_MONETARY (-> DOUBLE PRECISION)
_NON_MONETARY = {
    "adaptive_threshold_state": _f("previous_threshold"),
    "admin_ml_drift_snapshot": _f("avg_pnl", "avg_r_multiple", "win_rate"),
    "admin_ml_feature_completeness_snapshot": _f("completeness_pct", "recent_completeness_pct"),
    "admin_ml_linked_trade_snapshot": _f("confidence", "ml_score", "r_multiple", "threshold"),
    "admin_ml_validation_history_snapshot": _f("test_auc", "train_auc"),
    "admin_profitability_daily_summary": _f("avg_pnl", "avg_r_multiple", "profit_factor", "win_rate"),
    "admin_profitability_sizing_events": _f("atr_stop_distance_pct", "leverage", "risk_cap_pct"),
    "admin_profitability_symbol_summary": _f("avg_pnl", "avg_r_multiple", "win_rate"),
    "affiliate_settings": _f("referral_commission_rate"),
    "backtest_equity_curve": _f("drawdown_pct"),
    "backtest_fills": _f("confidence"),
    "backtest_jobs": _f("progress_pct"),
    "backtest_performance": _f("win_rate", "avg_win", "avg_loss", "max_drawdown", "avg_trade_duration_minutes",
                               "sharpe_ratio", "profit_factor", "expectancy"),
    "backtest_runs": _f("slippage_bps", "fee_bps", "progress_pct", "win_rate", "max_drawdown", "sharpe_ratio",
                        "sortino_ratio", "calmar_ratio", "profit_factor"),
    "bot_instances": _f("daily_loss_limit_pct"),
    "canonical_trade_decisions": _f("confidence", "effective_entry_threshold"),
    "cati_brake_state": _f("daily_pause", "halve_drawdown", "stop_drawdown", "daily_return", "drawdown"),
    "cati_execution_intents": _f("max_slippage_bps"),
    "cati_exit_decisions": _f("requested_fraction"),
    "cati_feed_funding": _f("rate"),
    "cati_instrument_rules": _f("max_leverage"),
    "cati_platform_cycle_metrics": _f("duration_ms"),
    "cati_position_forecasts": _f("conservative_remaining_edge_r"),
    "cati_rate_budgets": _f("capacity", "observed_used", "observed_limit"),
    "cati_rate_reservations": _f("weight"),
    "cati_reservation_ledger": _f("leverage"),
    "cati_residual_decisions": _f("score", "gross_R", "cost_R", "net_R"),
    "cati_shadow_comparisons": _f("largest_difference"),
    "cati_shadow_days": _f("daily_return", "drawdown"),
    "cati_shadow_positions": _f("entry_stop_distance"),
    "commission_config": _f("percentage"),
    "commission_tiers": _f("rate"),
    "decision_traces": _f("margin_level", "drawdown_pct", "regime_confidence", "confidence", "adx", "atr_pct",
                          "ma_slope", "compression_ratio", "breakout_pressure", "buy_score", "sell_score", "threshold",
                          "aggressiveness_score", "confidence_gate_modifier", "size_multiplier", "rolling_win_rate",
                          "rolling_expectancy", "ml_score", "ml_threshold"),
    "dynamic_universe_shadow_diagnostics": _f("confidence", "threshold", "spread_bps"),
    "economic_events": _f("actual_val", "forecast_val", "previous_val"),
    "event_market_snapshots": _f("atr"),
    "event_news_influence_decisions": _f("confidence", "reliability_score", "fake_news_risk_score", "size_multiplier",
                                         "confidence_penalty"),
    "event_news_mode_decisions": _f("readiness_score", "safety_score"),
    "event_news_runtime_mode": _f("readiness_score"),
    "expert_evaluations": _f("confidence", "raw_score", "weight", "weighted_contribution"),
    "external_signal_queue": _f("confidence"),
    "global_confidence_buckets": _f("avg_pnl", "bucket_high", "bucket_low", "win_rate"),
    "global_regime_performance": _f("avg_pnl", "win_rate"),
    "global_strategy_performance": _f("avg_duration_minutes", "avg_loss", "avg_win", "expectancy", "profit_factor"),
    "kyc_selfie_checks": _f("confidence_score"),
    "market_event_reactions": _f("max_move_pct", "min_move_pct", "net_move_pct", "atr_before", "atr_after",
                                 "volatility_expansion_ratio", "candle_range_before", "candle_range_during",
                                 "realized_vol_before", "realized_vol_after", "average_volume_before",
                                 "volume_spike_ratio", "abnormal_volume_score", "spread_before", "spread_during",
                                 "spread_after", "spread_widening_ratio", "slippage_estimate",
                                 "order_book_depth_change", "confidence_score"),
    "market_feature_observations": _f("value"),
    "ml_runtime_status": _f("threshold", "hard_block_floor"),
    "ml_validation_history": _f("train_auc", "test_auc"),
    "narrative_effectiveness_scores": _f("avg_impact_score", "avg_price_move_pct", "correct_sentiment_ratio",
                                         "false_signal_ratio", "avg_effectiveness_score"),
    "news_asset_mappings": _f("mapping_confidence"),
    "news_cluster_items": _f("similarity_score"),
    "news_clusters": _f("highest_reliability_score", "cluster_confidence", "spam_score", "latency_score",
                        "fake_news_risk_score"),
    "news_intelligence_signals": _f("sentiment_score", "reliability_score", "confidence_score", "spam_score",
                                    "latency_score", "provider_latency_score", "fake_news_risk_score"),
    "news_market_reactions": _f("sentiment_score", "sentiment_accuracy_score", "impact_score", "max_price_move_pct",
                                "volatility_expansion", "volume_spike", "reaction_latency_minutes",
                                "signal_effectiveness_score", "data_quality_score", "reliability_score"),
    "news_narratives": _f("narrative_confidence"),
    "news_sentiment_scores": _f("sentiment_score", "confidence_score", "compound_raw", "pos_raw", "neg_raw", "neu_raw"),
    "news_sources": _f("base_reliability_score", "dynamic_reliability_score"),
    "positions": _f("leverage"),
    "raw_news_items": _f("latency_seconds"),
    "real_time_news_provider_status": _f("latency_avg_seconds", "duplicate_rate"),
    "risk_events": _f("observed_value", "limit_value"),
    "risk_parameters": _f("portfolio_risk_pct", "per_trade_risk_pct", "max_margin_usage_pct", "max_drawdown_pct",
                          "daily_loss_limit_pct", "stop_loss_multiplier", "take_profit_multiplier"),
    "shadow_trade_outcomes": _f("pnl_pct", "mfe", "mae", "minutes_elapsed"),
    "shadow_trades": _f("confidence", "threshold", "confidence_gap", "ml_score"),
    "signal_candidates": _f("risk_reward", "confidence_score"),
    "signal_outcomes": _f("confidence", "r_multiple"),
    "signal_pair_metrics": _f("spread_percent", "atr_percent", "volatility_score", "liquidity_score", "spread_score",
                              "reliability_score"),
    "signal_scan_runs": _f("duration_seconds"),
    "slippage_monitoring": _f("avg_slippage_pct", "max_slippage_pct"),
    "strategy_performance": _f("avg_slippage", "avg_r", "max_drawdown"),
    "symbol_universe_rankings": _f("score", "spread_bps", "volatility_quality", "funding_stability", "signal_frequency",
                                   "average_confidence", "recent_performance_score"),
    "threshold_decisions": _f("opportunity_confidence", "base_threshold", "regime_adjustment", "volatility_score",
                              "volatility_adjustment", "expert_agreement_score", "agreement_adjustment",
                              "htf_alignment_score", "htf_adjustment", "market_quality_score",
                              "market_quality_adjustment", "performance_score", "performance_adjustment",
                              "distribution_percentile", "distribution_adjustment", "market_threshold",
                              "calibration_adjustment", "raw_unclamped_threshold", "smoothed_threshold",
                              "rate_limited_threshold", "final_threshold", "min_threshold", "max_threshold",
                              "previous_threshold"),
    "trade_fills": _f("slippage_pct", "r_multiple", "confidence", "mfe_pct", "mae_pct", "exit_regime_confidence",
                      "time_in_trade_sec", "net_pnl_percent"),
    "trading_decisions": _f("regime_confidence", "buy_score", "sell_score", "consensus_observed", "consensus_required",
                            "raw_confidence", "threshold_base", "threshold_dynamic", "threshold_adaptive_modifier",
                            "threshold_regime_modifier", "effective_entry_threshold", "leverage", "resolved_rr"),
    "trades": _f("entry_confidence", "r_multiple"),
    "trading_signals": _f("risk_reward", "confidence_score"),
    "universe_members": _f("spread_bps"),
    "universe_snapshots": _f("metadata_age_seconds", "stats_age_seconds"),
    "user_signal_preferences": _f("minimum_confidence"),
}

#: table -> REAL columns holding money or exchange quantities/prices (``NUMERIC`` on PostgreSQL).
MONETARY_COLUMNS: Mapping[str, frozenset] = MappingProxyType(_MONETARY)
#: table -> REAL columns that are ratios, scores, indicators, statistics or durations (``DOUBLE PRECISION``).
NON_MONETARY_REAL_COLUMNS: Mapping[str, frozenset] = MappingProxyType(_NON_MONETARY)

_REAL_AFFINITY = ("REAL", "FLOA", "DOUB")


def is_real_type(declared_type: Optional[str]) -> bool:
    """SQLite REAL affinity (``REAL``, ``FLOAT``, ``DOUBLE``, ``DOUBLE PRECISION``). ``NUMERIC``/``DECIMAL``
    columns are numeric affinity, not REAL, and are handled by the caller's own rule."""
    t = (declared_type or "").upper()
    return "INT" not in t and any(k in t for k in _REAL_AFFINITY)


def classify_real_column(table: str, column: str) -> str:
    """MONETARY or NON_MONETARY. Raises ``UnclassifiedRealColumn`` for a column in neither map: a new REAL
    column must be classified here before it can be migrated."""
    money = column in MONETARY_COLUMNS.get(table, ())
    other = column in NON_MONETARY_REAL_COLUMNS.get(table, ())
    if money and other:
        raise UnclassifiedRealColumn(f"{table}.{column} is classified twice")
    if money:
        return MONETARY
    if other:
        return NON_MONETARY
    raise UnclassifiedRealColumn(f"{table}.{column}")


def is_monetary(table: str, column: str) -> bool:
    return classify_real_column(table, column) == MONETARY


def postgres_real_type(table: str, column: str) -> str:
    """The PostgreSQL type of a legacy REAL column."""
    return PG_MONEY_TYPE if is_monetary(table, column) else PG_NON_MONEY_TYPE


# ════════════════════════════════════════════════════════════════════════════════════════ conversion
_NUMERIC_TEXT = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_NON_FINITE_TEXT = re.compile(r"^[+-]?(?:nan|snan|inf|infinity)$", re.IGNORECASE)


def to_decimal(value: Any) -> Optional[Decimal]:
    """The exact Decimal a legacy stored value stands for (see the module note). Deterministic: the same input
    always gives the same Decimal (same digits, same exponent)."""
    if value is None:
        return None
    if isinstance(value, bool):                       # bool is an int subclass; never money
        raise MoneyTypeMismatch(f"bool {value!r}")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NonFiniteMoney(repr(value))
        return Decimal(repr(value))
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise NonFiniteMoney(str(value))
        return value
    if isinstance(value, str):
        text = value.strip()
        if _NON_FINITE_TEXT.match(text):
            raise NonFiniteMoney(repr(value))
        if not _NUMERIC_TEXT.match(text):
            raise MoneyTypeMismatch(f"text {value[:40]!r}")
        try:
            return Decimal(text)
        except InvalidOperation as exc:              # pragma: no cover - the pattern admits only valid literals
            raise MoneyTypeMismatch(f"text {value[:40]!r}") from exc
    raise MoneyTypeMismatch(f"{type(value).__name__}")


def numeric_misfit(value: Optional[Decimal], precision: int = NUMERIC_PRECISION,
                   scale: int = NUMERIC_SCALE) -> Optional[str]:
    """Why ``value`` cannot be stored in ``NUMERIC(precision, scale)`` without rounding, or None when it can.
    Trailing fractional zeros are not significant (``1.50000000000000000000`` fits)."""
    if value is None:
        return None
    if not value.is_finite():
        return "NON_FINITE"
    if value.is_zero():
        return None
    sign, digits, exponent = value.as_tuple()       # not normalize(): it rounds to the context precision (28)
    digits = list(digits)
    while len(digits) > 1 and digits[-1] == 0:      # trailing zeros are not significant
        digits.pop()
        exponent += 1
    fractional = max(0, -exponent)
    integer = max(0, len(digits) + exponent)
    if fractional > scale:
        return f"FRACTIONAL_DIGITS:{fractional}>{scale}"
    if integer > precision - scale:
        return f"INTEGER_DIGITS:{integer}>{precision - scale}"
    return None


def to_numeric_exact(value: Any, precision: int = NUMERIC_PRECISION, scale: int = NUMERIC_SCALE) -> Optional[Decimal]:
    """``to_decimal`` plus the guarantee that ``NUMERIC(precision, scale)`` holds it exactly; raises
    ``MoneyPrecisionLoss`` otherwise (never rounds)."""
    d = to_decimal(value)
    problem = numeric_misfit(d, precision, scale)
    if problem:
        raise MoneyPrecisionLoss(f"{d}: {problem}")
    return d


def decimal_text(value: Optional[Decimal]) -> Optional[str]:
    """Plain (non-scientific) decimal text for binding to ``NUMERIC``: ``1E-7`` -> ``0.0000001``."""
    if value is None:
        return None
    return format(value, "f")
