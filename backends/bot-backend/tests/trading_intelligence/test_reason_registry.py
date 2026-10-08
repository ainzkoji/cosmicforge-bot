"""Step 1.6 -- every reason code the production path can surface has a
customer-facing mapping, and an unknown code is never described as fine."""
import re
from pathlib import Path

import pytest

from app.trading_intelligence.integration import reason_registry as registry

APP = Path(__file__).resolve().parents[2] / "app"
SHARED = Path(__file__).resolve().parents[3] / "shared" / "shared_lib"

#: The modules whose codes reach the customer through the account state.
SOURCES = [
    APP / "trading_intelligence" / "integration" / "production_execution.py",
    APP / "trading_intelligence" / "integration" / "production_runtime.py",
    APP / "trading_intelligence" / "integration" / "production_portfolio.py",
    APP / "trading_intelligence" / "integration" / "income_ledger.py",
    APP / "execution" / "protection_state.py",
    APP / "core" / "deployment_service.py",
    APP / "trading_intelligence" / "execution" / "risk_sizing.py",
    SHARED / "broker" / "auto_trading.py",
    SHARED / "core" / "production.py",
    SHARED / "deployment" / "contract.py",
]

#: Codes raised by these modules that are NOT shown to customers as a status
#: reason (internal evidence labels, detail prefixes, SQL states). Each entry
#: is a deliberate decision; adding to it is a review item.
INTERNAL_ONLY = {
    "CLOSE_ORDER_ACKNOWLEDGED", "CLOSE_FILLED_AND_FLAT_CONFIRMED", "BROKER_REPORTS_FLAT", "BROKER_CONFIRMED_FLAT_AND_EXIT_FILL",
    "BROKER_CONFIRMED_ENTRY_AND_CLOSE", "BROKER_CASH_LEDGER_AND_DURABLE_EQUITY", "PROTECTION_CLEANUP_PENDING",
    "PROTECTION_ENTRY_LINEAGE_REQUIRED", "PROTECTION_PRECISION_UNKNOWN", "FROZEN_PROTECTION_GEOMETRY_REQUIRED",
    "PRODUCTION_PROTECTION_ACCOUNT_SCOPE_REQUIRED", "PROTECTION_CANCEL_REQUIRES_FLAT_POSITION",
    "PROTECTION_CANCEL_OUTCOME_UNKNOWN", "PROTECTION_CANCEL_READ_BACK_UNCONFIRMED",
    "DECISION_ACCEPTED", "BOUNDARY_RESULT", "EVALUATION_FAILED_STAGE", "NOT_REQUIRED_FLAT", "UNCONFIRMED",
    "DEMO_CERTIFICATION", "NATURAL_CATI", "FROZEN_RESIDUAL_TOP1", "FORECAST_NOT_APPLICABLE", "NOT_APPLICABLE_FROZEN_RESIDUAL",
    "FROZEN_RESIDUAL", "ALLOW_PARTIAL_FILL", "PER_BOT_EFFECTIVE_POLICY", "ACCOUNT_SCOPED", "BROKER_SYNC", "HEALTHY", "LOGICAL",
    "ONE_WAY", "OPEN_CONFIRMED", "OPEN_FAILED", "PENDING_SUBMIT", "SUBMIT_UNKNOWN", "PENDING_ENTRY", "POSITION_CLOSED",
    "CONSUMED", "RELEASED", "RESOLUTION_PENDING", "CLOSED", "PENDING", "FULL", "SYNCED", "STALE", "RATE_LIMIT_BACKOFF",
    "COLLECTING", "ACCOUNT_LEVEL", "COMMISSION", "FUNDING_FEE", "REALIZED_PNL", "INSURANCE_CLEAR", "COMMISSION_REBATE",
    "DELIVERED_SETTELMENT", "DELIVERED_SETTLEMENT", "POSITION_LIMIT_INCREASE_FEE", "FEE_RETURN", "API_REBATE",
    "PARTIALLY_FILLED", "FILLED", "NEW", "CANCELED", "EXPIRED", "REJECTED", "CRITICAL", "CONFIRMED", "ABSENT", "UNKNOWN",
    "RISK_DERIVED", "OPEN_RISK_CAP", "LEVERAGE_CEILING", "USER_MAX_POSITION", "AVAILABLE_MARGIN", "SYSTEM_MAX_NOTIONAL",
    "EXCHANGE_MAX_QTY", "ENGINE_SNAPSHOT", "EXCHANGE_READ", "SYSTEM_MAXIMUM_STOP", "FROZEN_STRATEGY_DECISIONS",
    "PREVIOUS_ROW", "FREE_PLAN", "SUPERSEDED", "DEPLOYMENT_REFUSED", "STOP_DISTANCE_ASSUMPTION_REQUIRED",
    "EXECUTION_ACCOUNT_SCOPE_MISMATCH", "CAPITAL_RESERVED", "WARMING_UP", "NEXT_NATIVE15M_OPEN_REFERENCE_NOT_A_FILL",
    "USDT", "DEMO", "LIVE", "LONG", "SHORT", "FLAT", "BUY", "SELL", "GTC", "MARKET", "NORMAL", "PRIMARY", "CONTRACT_PRICE",
    "STOP_MARKET", "TAKE_PROFIT_MARKET", "CONDITIONAL", "BINANCE_USDM", "SKIPPED_IDLE_ACCOUNT", "ACCOUNT_WIDE",
    "BLOCKED", "BLOCKED_", "PERSISTED_", "ADAPTIVE_DAILY_RISK_", "MAX_NOTIONAL_PER_SYMBOL", "LIVE_ORDER_SUBMISSION_ENABLED",
    "DEMO_ORDER_SUBMISSION_ENABLED", "BILLING_ENFORCED", "COSMICFORGE_TEST_MODE", "PRODUCTION", "TEST", "DEVELOPMENT",
    "R_BUDGET", "MIN_HISTORY_TRADES", "LOOKBACK_TRADES", "LOOKBACK_DAYS", "CAUTION_PCT", "DEFENSIVE_PCT",
    "PERFORMANCE_FACTOR_MIN", "PERFORMANCE_FACTOR_MAX", "VOLATILITY_FACTOR_MIN", "DRAWDOWN_FACTOR_MIN",
    "COMPLETE_ACCOUNT_INCOME_HISTORY", "DURABLE_PROTECTION_READ_BACK", "CATI_PRODUCTION_PROFILE", "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1",
    "SELECTED_TOP1", "ACCOUNT_ID", "USER_ID", "CRYPTO", "FOREX", "BROKER", "ALLOWLIST", "CATI", "PROTECTION_STATE",
    "HOURLY", "FILL", "DEPLOY", "ENGINE_CYCLE", "DEPLOYMENT", "EXECUTED", "RISK_REJECTED", "PREFLIGHT_BLOCKED",
    "STILL_UNKNOWN", "RECONCILED", "DUPLICATE_PLAN", "WRONG_ACCOUNT", "ADAPTER_UNVALIDATED", "GOVERNANCE_NOT_AUTHORIZED",
    "EXECUTION_REJECTED", "SUBMIT_UNKNOWN_PENDING_RECONCILIATION", "ORDER_INTENT_PERSISTED",
}

#: What the production path surfaces as a reason: a ValueError code, a status
#: reason assignment, an eligibility return, or a listed classification code.
PATTERNS = [
    re.compile(r'ValueError\(\s*["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'EngineUnavailable\(\s*["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'GrantError\(\s*["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'["\']reason["\']\s*[:=]\s*["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'reason\s*=\s*["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'result\[["\']reason["\']\]\s*=\s*["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'return\s+["\']([A-Z][A-Z0-9_]{5,})["\']'),
    re.compile(r'^\s*["\']([A-Z][A-Z0-9_]{5,})["\']\s*,?\s*(?:#.*)?$', re.MULTILINE),   # members of a code list
    re.compile(r'^([A-Z][A-Z0-9_]{5,})\s*=\s*["\']\1["\']', re.MULTILINE),               # CODE = "CODE" constants
]


def produced_codes() -> set:
    codes = set()
    for path in SOURCES:
        text = path.read_text(encoding="utf-8")
        for pattern in PATTERNS:
            codes.update(pattern.findall(text))
    return {c for c in codes if c not in INTERNAL_ONLY}


def test_every_engine_reason_code_has_a_customer_mapping():
    missing = sorted(code for code in produced_codes() if not registry.known(code))
    assert not missing, f"engine reason codes without a registry entry: {missing}"


def test_the_registry_entries_are_complete_and_well_formed():
    for code in registry.all_codes():
        entry = registry.describe(code)
        assert entry["code"] == code and entry["severity"] in (registry.INFO, registry.WAITING, registry.BLOCKED,
                                                                registry.ATTENTION, registry.CRITICAL)
        assert entry["message"] and entry["message"][0].isupper() and len(entry["message"]) > 12


def test_an_unknown_code_is_never_described_as_fine():
    entry = registry.describe("SOMETHING_NEW_AND_SERIOUS")
    assert entry["severity"] == registry.UNKNOWN and "Unrecognised" in entry["message"] and "review" in entry["action"].lower()
    assert registry.describe(None)["severity"] == registry.UNKNOWN


@pytest.mark.parametrize("code,severity", [
    ("WAITING_SIGNAL", registry.WAITING), ("USER_DAILY_LOSS_LIMIT_REACHED", registry.BLOCKED),
    ("PROTECTION_STATE_UNKNOWN", registry.ATTENTION), ("PROTECTION_CONFIRMED_ABSENT", registry.CRITICAL),
    ("BROKER_SNAPSHOT_STALE", registry.ATTENTION), ("LIVE_ORDER_SUBMISSION_DISABLED", registry.BLOCKED),
    ("CATI_NEW_ENTRY_KILL_SWITCH", registry.BLOCKED), ("BROKER_ACCOUNT_OWNERSHIP_MISMATCH", registry.CRITICAL),
])
def test_severities_of_the_codes_customers_see_most(code, severity):
    assert registry.describe(code)["severity"] == severity
