"""The ONE broker execution contract (Phase 2B / 3).

``EXECUTOR_REQUIRED`` is what the existing executor, fill resolution,
position reconciliation and protection code actually call on a client
(derived from those call sites -- see test_phase3_execution_parity). A
client missing any of them cannot run a bot: the capability profile for its
broker must not declare ORDERS / ORDER_LOOKUP / FILLS / PROTECTION_ORDERS
usable.

``CANONICAL`` is the broker-neutral surface every venue client exposes on
top (names from the multi-asset specification). ``INTERNAL_TRANSFER`` is
only required where the broker declares the capability, and there is no
withdrawal method in any list.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Tuple

EXECUTOR_REQUIRED: Tuple[str, ...] = (
    # orders
    "place_order", "get_order", "get_order_by_client_order_id", "open_orders", "get_algo_orders",
    "cancel_order", "cancel_all_orders", "place_market_order", "close_position_market",
    # fills
    "user_trades",
    # positions / account
    "position_risk", "position_risk_all", "get_position_info", "get_position_amt", "get_positions", "account",
    # protection
    "place_protection", "update_protection", "place_stop_market", "place_take_profit_market",
    # market data / metadata
    "last_price", "get_prices", "klines", "get_symbol_filters", "exchange_info", "exchange_info_cached",
    "list_instruments", "set_leverage", "server_time", "ping",
)

CANONICAL: Tuple[str, ...] = (
    "place_order", "get_order", "get_open_orders", "cancel_order", "cancel_all", "get_positions", "get_balance",
    "place_protection", "list_instruments", "get_instrument", "get_ticker", "get_orderbook", "get_funding",
    "get_account_permissions", "get_account_capabilities",
)

INTERNAL_TRANSFER_METHODS = {
    "binance": ("universal_transfer", "universal_transfer_history", "api_restrictions", "funding_assets",
                "spot_account"),
    "bybit": ("inter_transfer", "query_inter_transfers", "account_coin_balance", "account_info", "query_api_key"),
    "bingx": ("asset_transfer", "asset_transfer_history"),
}

FORBIDDEN_SUBSTRINGS = ("withdraw",)


def missing(client: Any, names: Iterable[str]) -> List[str]:
    return [n for n in names if not callable(getattr(client, n, None))]


def contract_report(client: Any, broker: str) -> dict:
    return {
        "broker": broker,
        "executor_missing": missing(client, EXECUTOR_REQUIRED),
        "canonical_missing": missing(client, CANONICAL),
        "transfer_missing": missing(client, INTERNAL_TRANSFER_METHODS.get(broker, ())),
        "forbidden_present": [n for n in dir(client) if any(f in n.lower() for f in FORBIDDEN_SUBSTRINGS)],
    }


__all__ = ["CANONICAL", "EXECUTOR_REQUIRED", "FORBIDDEN_SUBSTRINGS", "INTERNAL_TRANSFER_METHODS", "contract_report",
           "missing"]
