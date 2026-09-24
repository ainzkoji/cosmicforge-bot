"""Canonical, point-in-time CATI feature schemas per estimator role.

Market-alpha roles (REGIME / OUTCOME / RANKING / OOD) are TENANT-NEUTRAL:
no account, bot, user, capital, slot or margin state may be a feature.
Features come from the canonical decision-time evidence (Section 22 replay
records / Section 21 export) -- never from V2 ``final_confidence``, buy/sell
scores, thresholds or adaptive-engine values.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence

from .contracts import FeatureSchema, ModelRole

#: never features of a shared market model
TENANT_FIELDS = frozenset({
    "user_id", "broker_account_id", "bot_instance_id", "account_equity", "equity", "margin_used",
    "margin_available", "open_positions", "open_positions_count", "capital", "fixed_amount", "slots",
    "reservation_id", "portfolio_reservation_id",
})
#: legacy V2 semantics that are NOT CATI features
LEGACY_V2_FIELDS = frozenset({
    "final_confidence", "confidence_normed", "buy_score", "sell_score", "threshold", "threshold_gap",
    "consensus_gap", "chosen_strategy", "active_strategy_count", "active_count_normed", "adaptive_state",
    "rolling_win_rate", "rolling_expectancy",
})
#: realized-outcome fields: labels only, never inputs
OUTCOME_FIELDS = frozenset({"gross_R", "net_R", "terminal_outcome", "mfe_R", "mae_R", "stress", "neighbors",
                            "label_model_cost_R", "same_bar_conservative", "rank"})

_MARKET_CATEGORICAL = ("setup_family", "side", "regime", "session", "volume_liquidity_proxy", "symbol_group")

FEATURE_SCHEMAS: Mapping[str, FeatureSchema] = {
    ModelRole.OUTCOME.value: FeatureSchema(
        "cati_outcome_features", ModelRole.OUTCOME.value,
        _MARKET_CATEGORICAL + ("forecast_p", "cost_R"), categorical=_MARKET_CATEGORICAL),
    ModelRole.RANKING.value: FeatureSchema(
        "cati_ranking_features", ModelRole.RANKING.value,
        _MARKET_CATEGORICAL + ("forecast_p", "cost_R"), categorical=_MARKET_CATEGORICAL),
    ModelRole.REGIME.value: FeatureSchema(
        "cati_regime_features", ModelRole.REGIME.value, ("session", "volume_liquidity_proxy", "symbol_group", "regime"),
        categorical=("session", "volume_liquidity_proxy", "symbol_group", "regime")),
    ModelRole.OOD.value: FeatureSchema(
        "cati_ood_features", ModelRole.OOD.value, ("forecast_p", "cost_R"), source="point_in_time_numeric_state"),
    ModelRole.SLIPPAGE.value: FeatureSchema(
        "cati_slippage_features", ModelRole.SLIPPAGE.value,
        ("venue", "instrument", "notional", "spread_bps", "volatility", "session", "order_type"),
        categorical=("venue", "instrument", "session", "order_type"), source="execution_decision_time_features"),
    ModelRole.EXIT.value: FeatureSchema(
        "cati_exit_features", ModelRole.EXIT.value,
        ("unrealized_R", "mfe_R_so_far", "mae_R_so_far", "bars_held", "remaining_edge_R_deterministic"),
        source="position_path_point_in_time"),
}


class FeatureContractViolation(ValueError):
    pass


def validate_schema(schema: FeatureSchema, *, account_context_allowed: bool = False) -> None:
    cols = set(schema.columns)
    legacy = cols & LEGACY_V2_FIELDS
    if legacy:
        raise FeatureContractViolation(f"legacy V2 fields are not CATI features: {sorted(legacy)}")
    leaked = cols & OUTCOME_FIELDS
    if leaked:
        raise FeatureContractViolation(f"realized-outcome fields cannot be features: {sorted(leaked)}")
    tenant = cols & TENANT_FIELDS
    if tenant and not account_context_allowed:
        raise FeatureContractViolation(f"tenant/account fields in a shared market model: {sorted(tenant)}")


def extract(schema: FeatureSchema, record: Mapping[str, Any]) -> Dict[str, Any]:
    """Only the schema's columns; a missing value stays ``None`` (never 0)."""
    return {c: record.get(c) for c in schema.columns}


def matrix(schema: FeatureSchema, records: Sequence[Mapping[str, Any]]):
    """Numeric design matrix: categoricals one-hot over the sorted training vocabulary."""
    vocab = {c: sorted({str(r.get(c)) for r in records}) for c in schema.categorical}
    names = []
    for c in schema.columns:
        names.extend([f"{c}={v}" for v in vocab[c]] if c in vocab else [c])
    rows = []
    for r in records:
        row = []
        for c in schema.columns:
            if c in vocab:
                row.extend(1.0 if str(r.get(c)) == v else 0.0 for v in vocab[c])
            else:
                v = r.get(c)
                row.append(float("nan") if v is None else float(v))
        rows.append(row)
    return rows, names, vocab


__all__ = ["FEATURE_SCHEMAS", "TENANT_FIELDS", "LEGACY_V2_FIELDS", "OUTCOME_FIELDS", "FeatureContractViolation",
           "validate_schema", "extract", "matrix"]
