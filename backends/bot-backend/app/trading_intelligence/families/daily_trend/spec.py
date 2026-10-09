"""Mandate 004 / hypothesis 18 -- the daily trend family: frozen parameters as data.

Authority: ``research/trend_v1/SPEC.md`` (frozen 2026-10-07, commit f85a6cf0), pinned by SHA-256 in the research
register. Every number under ``SPECIFICATION`` is copied from that text; nothing there may be changed.

``INTERPRETATION`` holds the choices the text leaves open (how ATR is averaged, which price sizes an order, what
happens to a coin that leaves the universe ...). They were fixed in ``research/trend_v1/INTERPRETATION_001.md``
BEFORE any price series was loaded, are registered as amendment 1 (it changes no rule) and are hashed together
with the specification values into ``RULE_ARTIFACT``. Changing either dictionary moves ``rule_artifact_hash``,
and the register then refuses every evaluation.
"""
from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping

FAMILY_ID = "DAILY_TREND"
MANDATE_ID = "MANDATE_004"
HYPOTHESIS_ID = "H-018"
HYPOTHESIS_NUMBER = 18
SPECIFICATION_PATH = "research/trend_v1/SPEC.md"
SPECIFICATION_VERSION = "trend-ensemble-v1"
SPECIFICATION_SHA256 = "ea02ace43ecc75ea409dd2bb3592640e0d9b77412dc4774ddf58492ccf7e4edb"
INTERPRETATION_PATH = "research/trend_v1/INTERPRETATION_001.md"
RULES_VERSION = "daily-trend-rules-1"

DEVELOPMENT_START, DEVELOPMENT_END = "2020-01-01", "2024-12-31"
HOLDOUT_START, HOLDOUT_END = "2025-01-01", "2026-09-30"

#: copied from SPEC.md -- section names in the comments
SPECIFICATION: Mapping[str, Any] = MappingProxyType({
    # Data
    "venue": "BINANCE_USDM", "margin_asset": "USDT", "bar": "1d", "bar_close_utc": "00:00",
    "data_source": "data.binance.vision", "development": [DEVELOPMENT_START, DEVELOPMENT_END],
    "holdout": [HOLDOUT_START, HOLDOUT_END],
    # Universe (point in time)
    "universe_size": 20, "universe_volume_days": 30, "universe_min_history_days": 120,
    "universe_excludes": ["STABLECOIN_AGAINST_STABLECOIN", "NON_CRYPTO"],
    # Signal
    "lookbacks": [10, 20, 30, 45, 65, 100, 150], "exit_lookback": "L // 2, minimum 5", "side": "LONG_OR_FLAT",
    # Sizing
    "atr_days": 20, "stop_atr_multiple": 3.0, "stop_floor": 0.05, "stop_cap": 0.40,
    "target_notional": "s * risk_per_trade * equity / d",
    "risk_levels": {
        "conservative": {"risk_per_trade": 0.0025, "max_open_risk": 0.01, "max_positions": 4, "leverage": 1.0,
                         "daily_pause": 0.01, "halve_drawdown": 0.05, "stop_drawdown": 0.10},
        "balanced": {"risk_per_trade": 0.0050, "max_open_risk": 0.02, "max_positions": 6, "leverage": 2.0,
                     "daily_pause": 0.02, "halve_drawdown": 0.08, "stop_drawdown": 0.15},
        "aggressive": {"risk_per_trade": 0.0075, "max_open_risk": 0.03, "max_positions": 8, "leverage": 3.0,
                       "daily_pause": 0.03, "halve_drawdown": 0.12, "stop_drawdown": 0.25}},
    # Execution and costs
    "decision": "close of day t", "fill": "open of day t+1", "rebalance_band": 0.25,
    "taker_fee": 0.0005, "slippage": 0.0005, "stress_cost_multiple": 2.0, "funding": "ACTUAL_RATES_ON_NOTIONAL_HELD",
    "stop_fill": "stop price, or the open when the day opens below it, less slippage",
    # Pass rule (all at Balanced)
    "pass_rule": {"level": "balanced", "holdout_net_return_positive_at": ["base", "stress"],
                  "positive_calendar_years_min": 4, "calendar_years": [2020, 2021, 2022, 2023, 2024, 2025],
                  "max_drawdown_without_brake": 0.15, "holdout_net_sharpe_min": 0.3},
    # Secondary test (cannot rescue a fail)
    "funding_overlay": {"trailing_days": 3, "halve_above_annualised": 0.30, "zero_above_annualised": 0.60},
})

#: fixed in INTERPRETATION_001.md before any data was loaded -- item numbers in the comments
INTERPRETATION: Mapping[str, Any] = MappingProxyType({
    "contract_identity": "EACH_ARCHIVE_SYMBOL_IS_ONE_CONTRACT",                               # I1
    "usdt_perpetual_pattern": r"^[A-Z0-9]+USDT(SETTLED)*$",                                   # I1
    "overlapping_relisting": "FEWER_SETTLED_SUFFIXES_WINS",                                   # I1
    "stablecoin_bases": ["AEUR", "BFUSD", "BUSD", "DAI", "EURI", "FDUSD", "PYUSD", "RLUSD", "TUSD", "USD0",
                         "USD1", "USDC", "USDD", "USDE", "USDP", "USDS", "UST", "XUSD"],     # I2
    "non_crypto_rule": "CONTRACT_TYPE_TRADIFI_PERPETUAL_IN_METADATA_SNAPSHOT",                # I2
    "unlisted_in_metadata": "TREATED_AS_CRYPTO",                                              # I2
    "history_rule": "120_CONSECUTIVE_DAILY_BARS_ENDING_ON_THE_DECISION_DAY",                  # I3
    "volume_window": "THE_30_DAYS_BEFORE_THE_DECISION_DAY",                                   # I3
    "universe_tie_break": "SYMBOL_ASCENDING",                                                 # I3
    "subsignal_without_history": "OFF", "subsignal_after_missing_bar": "RESET_TO_OFF",        # I4
    "atr": "SIMPLE_MEAN_OF_20_TRUE_RANGES_ENDING_ON_THE_DECISION_DAY",                        # I5
    "initial_equity_usdt": 10000.0, "sizing_equity": "MARKED_TO_MARKET_AT_DECISION_CLOSE",    # I6
    "order_quantity_price": "DECISION_CLOSE",                                                 # I6
    "selection_order": ["STRENGTH_DESC", "VOLUME_DESC", "SYMBOL_ASC"],                        # I7
    "holding_priority": "NONE", "leaves_universe": "TARGET_ZERO",                             # I7
    "scaling_order": ["HALVE_BRAKE", "OPEN_RISK_PROPORTIONAL", "LEVERAGE_PROPORTIONAL"],      # I8
    "open_risk_distance": "CURRENT_STOP_DISTANCE_D",                                          # I8
    "halve_brake": "WHILE_DRAWDOWN_AT_OR_BEYOND_THRESHOLD", "stop_brake": "PERMANENT",        # I9
    "daily_pause": "NO_NEW_POSITIONS_FROM_THAT_CLOSE",                                        # I9
    "without_the_brake": "DRAWDOWN_BRAKES_OFF_DAILY_PAUSE_ON",                                # I9
    "stop_initial": "FILL_PRICE_TIMES_ONE_MINUS_D_AT_DECISION",                               # I10
    "stop_ratchet": "MAX_OF_PREVIOUS_AND_CLOSE_TIMES_ONE_MINUS_D",                            # I10
    "stop_same_day_as_entry": True, "reentry_after_stop": "ALLOWED_BY_NEXT_DECISION",         # I10
    "gap_through_stop_with_pending_add": "ADD_FILLS_THEN_WHOLE_POSITION_STOPS_AT_OPEN",       # I10
    "exchange_filters": "CURRENT_SNAPSHOT_NOT_POINT_IN_TIME",                                 # I11
    "min_notional_without_metadata_usdt": 5.0, "quantity_rounding": "DOWN_TO_STEP",           # I11
    "funding_sign": "LONG_PAYS_POSITIVE_RATE",                                                # I12
    "funding_midnight_event": "ON_QUANTITY_HELD_BEFORE_THAT_DAYS_FILLS",                      # I12
    "funding_later_events": "ON_QUANTITY_HELD_AFTER_THAT_DAYS_FILLS",                         # I12
    "funding_notional_price": "DAY_OPEN", "funding_on_stop_day": "COSTS_CHARGED_INCOME_NOT_CREDITED",   # I12
    "missing_funding_rate_per_8h": 0.0003, "missing_funding_max_share": 0.02,                 # I13
    "missing_bar_of_held_coin": "CLOSE_AT_LAST_CLOSE_WITH_STRESS_COST",                       # I14
    "full_period_run": "ONE_CONTINUOUS_ACCOUNT_FROM_DEVELOPMENT_START",                       # I15
    "holdout_run": "FRESH_ACCOUNT_AT_HOLDOUT_START_INDICATORS_WARMED_ON_EARLIER_DATA",        # I15
    "pass_rule_brakes": {"criteria_1_2_4": "AS_SPECIFIED_BRAKES_ON", "criterion_3": "DRAWDOWN_BRAKES_OFF"},  # I15
    "calendar_year_return": "YEAR_END_EQUITY_OVER_PREVIOUS_YEAR_END", "drawdown_inside": "LESS_OR_EQUAL",    # I15
    "sharpe": "MEAN_OVER_SAMPLE_STD_OF_DAILY_NET_RETURNS_TIMES_SQRT_365_ZERO_RISK_FREE",      # I16
    "benchmarks": {"btc": "BTCUSDT_PERPETUAL_HELD_LONG_AT_1X_WITH_FUNDING",
                   "equal_weight": "DAILY_EQUAL_WEIGHT_OF_YESTERDAYS_UNIVERSE_WITH_FUNDING_AND_BASE_COST",
                   "volatility_scaling": "EX_POST_TO_THE_STRATEGYS_REALISED_VOLATILITY"},      # I17
    "funding_overlay_rate": "MEAN_EVENT_RATE_OVER_3_DAYS_ENDING_ON_DECISION_DAY_ANNUALISED_BY_EVENTS_PER_YEAR",  # I18
    "funding_overlay_applies": "TO_THE_RAW_TARGET_BEFORE_PORTFOLIO_CAPS",                     # I18
    "executable_policy_scenario": {"risk_per_trade_ceiling": 0.004, "max_entry_stop_distance": 0.15,
                                   "role": "REPORTED_SEPARATELY_NEVER_PART_OF_THE_PASS_RULE"},  # I19
    "statistical_gate": "PORTFOLIO_DAILY_NET_RETURN_THRESHOLDS_PENDING_OWNER_APPROVAL",        # I20
})


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


#: the registered rule artifact: its ``stable_hash`` is ``rule_artifact_hash`` in the register
RULE_ARTIFACT: Mapping[str, Any] = MappingProxyType({
    "family_id": FAMILY_ID, "mandate_id": MANDATE_ID, "rules_version": RULES_VERSION,
    "specification_sha256": SPECIFICATION_SHA256, "specification": _plain(SPECIFICATION),
    "interpretation": _plain(INTERPRETATION)})

LOOKBACKS = tuple(SPECIFICATION["lookbacks"])
RISK_LEVELS = tuple(SPECIFICATION["risk_levels"])

__all__ = ["FAMILY_ID", "MANDATE_ID", "HYPOTHESIS_ID", "HYPOTHESIS_NUMBER", "SPECIFICATION_PATH",
           "SPECIFICATION_VERSION", "SPECIFICATION_SHA256", "INTERPRETATION_PATH", "RULES_VERSION", "SPECIFICATION",
           "INTERPRETATION", "RULE_ARTIFACT", "LOOKBACKS", "RISK_LEVELS", "DEVELOPMENT_START", "DEVELOPMENT_END",
           "HOLDOUT_START", "HOLDOUT_END"]
