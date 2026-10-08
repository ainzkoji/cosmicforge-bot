"""Central reason-code registry (Step 1.6): every status / blocker code the
production engine can surface to a customer, mapped to a stable code, a
plain-language message, a recommended action and a severity.

The registry is the ONLY place such text lives. ``describe(code)`` never
invents a reassuring default: an unknown code is returned as
``severity="unknown"`` with the code itself as the message, and the test suite
fails when a code the engine produces is missing here
(``tests/trading_intelligence/test_reason_registry.py``).

Severity: ``info`` (normal operation), ``waiting`` (nothing wrong, no entry
right now), ``blocked`` (a policy or gate stops entries), ``attention`` (the
customer or operator should look), ``critical`` (a safety condition).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

INFO, WAITING, BLOCKED, ATTENTION, CRITICAL, UNKNOWN = "info", "waiting", "blocked", "attention", "critical", "unknown"

_R: Dict[str, Dict[str, str]] = {}


def _add(code: str, severity: str, message: str, action: str = "") -> None:
    _R[code] = {"code": code, "severity": severity, "message": message, "action": action}


# ── execution permission states ──────────────────────────────────────────────
_add("WAITING_SIGNAL", WAITING, "The engine is running and waiting for a qualifying signal.", "No action needed.")
_add("ORDER_ACTIVE", INFO, "An order from this bot is active at the exchange.", "No action needed.")
_add("BLOCKED_ACCOUNT", BLOCKED, "Trading is blocked for this account; see the reason.", "Read the reason and its action.")
_add("BLOCKED_RISK", BLOCKED, "A risk rule is blocking new entries right now.", "Read the reason; the block lifts when the rule allows.")
_add("BLOCKED_DEMO_ORDER_GATE", BLOCKED, "Demo order submission is switched off on this server.", "The operator must enable the demo order switch after the safety tests pass.")
_add("BLOCKED_LIVE_ORDER_GATE", BLOCKED, "Live order submission is disabled on this server (Step 1 keeps it off).", "No live orders are possible yet.")
_add("ACCOUNT_SCOPED", INFO, "Execution is evaluated per connected exchange account.", "")

# ── eligibility / signal ─────────────────────────────────────────────────────
_add("AWAITING_NATURAL_CATI_DECISION", WAITING, "No qualifying CATI decision is open right now.", "No action needed; decisions are evaluated hourly.")
_add("PROSPECTIVE_ENTRY_WINDOW_EXPIRED", WAITING, "The latest decision's entry window has passed without an entry.", "No action needed.")
_add("NEXT_NATIVE_OPEN_REFERENCE_REQUIRED", WAITING, "The engine is waiting for the next candle open to reference the entry.", "No action needed.")
_add("NEXT_NATIVE_OPEN_PROVENANCE_REQUIRED", WAITING, "The entry reference could not be proven from exchange data yet.", "No action needed.")
_add("NO_ELIGIBLE_TOP1", WAITING, "No symbol qualified in the latest decision.", "No action needed.")
_add("NO_SCORE_AT_LEAST_2", WAITING, "No symbol reached the required score in the latest decision.", "No action needed.")
_add("MISSED_PROSPECTIVE_BOUNDARY", WAITING, "A decision boundary was missed and recorded as skipped (never evaluated after the fact).", "No action needed.")
_add("FROZEN_RESIDUAL_PROVENANCE_REQUIRED", BLOCKED, "The decision lacks the provenance the frozen strategy requires.", "No customer action; the operator reviews the decision record.")
_add("RESIDUAL_DECISION_ID_MISMATCH", BLOCKED, "The decision record does not match the frozen strategy registry.", "No customer action; operator review.")
_add("RESIDUAL_DECISION_CONTENT_MISMATCH", BLOCKED, "The decision content does not match its recorded hash.", "No customer action; operator review.")
_add("RESIDUAL_DECISION_NOT_QUALIFIED", WAITING, "The latest decision did not qualify for entry.", "No action needed.")
_add("RESIDUAL_SYMBOL_OUTSIDE_FROZEN_UNIVERSE", BLOCKED, "The decision's symbol is outside the frozen trading universe.", "No customer action.")
_add("INVALID_CATI_GEOMETRY", BLOCKED, "The decision's price geometry (entry, stop, target) is not valid for an order.", "No customer action.")
_add("NON_EXECUTABLE_GAP", WAITING, "The market gapped past the entry reference; the entry was skipped.", "No action needed.")
_add("PROSPECTIVE_ENTRY_NOT_OPEN", WAITING, "The decision is not open for entry at this moment.", "No action needed.")
_add("CURRENT_CATI_DECISION_CHANGED", WAITING, "The decision changed while the entry was being prepared; nothing was sent.", "No action needed.")
_add("CATI_DECISION_ALREADY_ATTEMPTED", WAITING, "This decision was already attempted; it is never attempted twice.", "No action needed.")
_add("CATI_NOT_ELIGIBLE", WAITING, "The current decision is not eligible for entry.", "No action needed.")

# ── account / authorisation ──────────────────────────────────────────────────
_add("AUTO_TRADING_DISABLED", BLOCKED, "No running bot is authorised on this account.", "Deploy a bot, or resume the paused bot.")
_add("USER_AUTHORIZATION_REQUIRED", BLOCKED, "Your authorisation to trade this account is missing.", "Resume the bot to authorise trading on this account.")
_add("ACCOUNT_EXECUTION_OWNER_AMBIGUOUS", BLOCKED, "More than one bot claims this exchange account.", "Keep one bot per exchange account; stop the others.")
_add("BROKER_ACCOUNT_OWNERSHIP_MISMATCH", CRITICAL, "The bot and the exchange account belong to different users.", "Contact support; the bot does not trade.")
_add("ACCOUNT_OWNER_MAPPING_REQUIRED", BLOCKED, "The exchange account is not linked to a bot.", "Deploy a bot on this account.")
_add("BROKER_ACCOUNT_REQUIRED", BLOCKED, "No exchange account is connected.", "Connect an exchange account.")
_add("BROKER_ACCOUNT_NOT_FOUND", BLOCKED, "The exchange account could not be found.", "Reconnect the exchange account.")
_add("BROKER_ACCOUNT_ACCESS_DENIED", CRITICAL, "The exchange account is not yours.", "Contact support.")
_add("BROKER_EXECUTION_CAPABILITY_INCOMPLETE", BLOCKED, "The exchange API key lacks a required permission.", "Reconnect with futures trading enabled and withdrawals disabled.")
_add("DEMO_CAPABILITY_UNAVAILABLE", BLOCKED, "This exchange's demo environment is not supported for execution.", "Use a Binance demo account.")
_add("DEMO_ADAPTER_UNVALIDATED", BLOCKED, "This exchange's demo adapter is not certified for execution.", "Use a Binance demo account.")
_add("EXECUTION_ADAPTER_UNVALIDATED", BLOCKED, "This exchange is not certified for live execution.", "Live execution on this exchange is not available yet.")
_add("PRODUCTION_REQUIRES_CATI_BOT", BLOCKED, "Only CATI bots are executed by the production engine.", "Deploy a CATI bot.")
_add("PRODUCTION_REJECTS_PAPER_BOT", BLOCKED, "A paper-mode bot is not executed against the exchange.", "Deploy a broker-executed bot on a demo account.")
_add("CAPABILITY_UNAVAILABLE", BLOCKED, "This exchange is not supported by the engine.", "Use a supported exchange account.")
_add("ENVIRONMENT_CHANGED", CRITICAL, "The exchange account's environment (demo/live) changed since the last cycle.", "Reconnect the account; the bot does not trade until this is resolved.")
_add("BROKER_ENVIRONMENT_MISMATCH", CRITICAL, "The stored credentials and the account record disagree on demo versus live.", "Reconnect the exchange account.")
_add("EMERGENCY_FLATTEN_UNSUPPORTED_BROKER", BLOCKED, "Emergency flatten is not supported on this exchange.", "Operator action at the exchange.")
_add("EXECUTION_ACCOUNT_NOT_FOUND", BLOCKED, "The exchange account is not an execution account.", "Connect and validate the account.")
_add("CANONICAL_RUNTIME_LEASE_REQUIRED", ATTENTION, "This engine process does not hold the trading lease right now.", "The operator checks the runtime; trading resumes when the lease is held.")
_add("CATI_PRODUCTION_PROFILE_REQUIRED", CRITICAL, "The server is not running the production profile.", "Operator configuration required.")
_add("RUNTIME_SHUTDOWN_IN_PROGRESS", ATTENTION, "The engine is shutting down and opens no new position.", "Trading resumes when the engine restarts.")
_add("MAINTENANCE_BOUNDARY_UNAVAILABLE", CRITICAL, "The engine could not build its execution boundary for an open position.", "Operator review required.")

# ── risk gates ───────────────────────────────────────────────────────────────
_add("USER_DAILY_LOSS_LIMIT_REACHED", BLOCKED, "Today's loss limit was reached; no new entries until tomorrow.", "No action needed; existing positions stay protected.")
_add("ACCOUNT_DAILY_LOSS_POLICY_UNAVAILABLE", BLOCKED, "The bot's daily loss policy could not be resolved.", "Check the bot configuration.")
_add("ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE", WAITING, "A position or order is already open on this account; one position at a time.", "No action needed.")
_add("ACCOUNT_WIDE_ORPHAN_ORDER_ACTIVE", ATTENTION, "An open order on the account is not managed by the bot.", "Cancel manual orders on the exchange account.")
_add("NATIVE_PROTECTION_REQUIRES_ONE_WAY_ACCOUNT", BLOCKED, "The exchange account is in hedge mode; the engine needs one-way mode.", "Switch the futures account to one-way position mode.")
_add("CATI_NEW_ENTRY_KILL_SWITCH", BLOCKED, "An operator kill switch blocks new entries.", "Existing positions stay protected; the operator lifts the switch.")
_add("CONSECUTIVE_LOSS_DAY_PAUSED", BLOCKED, "Trading is paused for the day after consecutive losses.", "No action needed.")
_add("CONSECUTIVE_LOSS_COOLDOWN", BLOCKED, "A cooldown after consecutive losses is active.", "No action needed.")
_add("WEEKLY_DRAWDOWN_LIMIT_REACHED", BLOCKED, "The weekly drawdown limit was reached.", "Trading resumes next week.")
_add("MONTHLY_DRAWDOWN_LIMIT_REACHED", BLOCKED, "The monthly drawdown limit was reached.", "Trading resumes next month.")
_add("ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED", ATTENTION, "An earlier order's outcome is still being confirmed with the exchange.", "No action needed; nothing is sent until it is resolved.")
_add("EXECUTION_PORTFOLIO_OVERLAP", WAITING, "The engine's portfolio already holds this exposure.", "No action needed.")
_add("CAPITAL_ALREADY_RESERVED", WAITING, "Capital for this decision is already reserved.", "No action needed.")
_add("CAPITAL_BASIS_UNKNOWN", BLOCKED, "The capital basis for sizing is unknown.", "Check the account balance.")
_add("FAMILY_BUDGET_EXCEEDED", BLOCKED, "The strategy family's budget is fully used.", "No action needed.")
_add("INSTRUMENT_UNKNOWN", BLOCKED, "The instrument is not in the exchange catalog.", "No customer action.")

# ── broker reads / data ──────────────────────────────────────────────────────
for _code, _msg in (("BROKER_ACCOUNT_RISK_UNAVAILABLE", "The account balance could not be read."),
                    ("BROKER_INCOME_HISTORY_UNAVAILABLE", "The exchange income history could not be read."),
                    ("BROKER_INCOME_HISTORY_INVALID", "The exchange income history was malformed."),
                    ("BROKER_INCOME_HISTORY_INCOMPLETE", "The exchange income history could not be read completely."),
                    ("BROKER_INCOME_HISTORY_AMBIGUOUS", "The exchange income history is ambiguous."),
                    ("BROKER_FILL_HISTORY_UNAVAILABLE", "The exchange fill history could not be read."),
                    ("BROKER_FILL_ID_UNAVAILABLE", "An exchange fill carried no identity."),
                    ("BROKER_PERIOD_RISK_BASIS_UNAVAILABLE", "The weekly/monthly risk basis could not be computed."),
                    ("BROKER_POSITION_EXPOSURE_UNAVAILABLE", "The exchange position report was malformed."),
                    ("BROKER_READ_SHAPE_INVALID", "The exchange answered with an unexpected shape."),
                    ("DAILY_RISK_BASIS_UNAVAILABLE", "Today's risk basis could not be computed from the account."),
                    ("BROKER_SNAPSHOT_STALE", "The engine's last exchange read is older than two minutes."),
                    ("READ_FAILED", "The exchange could not be read this cycle."),
                    ("AWAITING_FIRST_BROKER_SYNC", "The engine has not read this account yet."),
                    ("ACCOUNT_RISK_PENDING", "Account risk has not been evaluated yet."),
                    ("BROKER_SYNC_PENDING", "The first exchange read is pending.")):
    _add(_code, ATTENTION, _msg + " The engine fails closed and retries next cycle.", "Retry shortly; if it persists, re-validate the exchange connection.")
_add("PERSISTED_RISK_STATE_REQUIRED", BLOCKED, "The bot's persisted risk state is missing.", "Operator review.")
_add("PERSISTED_WEEKLY_RISK_BASIS_REQUIRED", BLOCKED, "The weekly risk basis is missing.", "Operator review.")
_add("PERSISTED_MONTHLY_RISK_BASIS_REQUIRED", BLOCKED, "The monthly risk basis is missing.", "Operator review.")
_add("CLOSE_ACCOUNT_OWNERSHIP_UNCONFIRMED", CRITICAL, "A pending close could not be matched to this account.", "Operator review.")

# ── protection (Step 1.0a) ───────────────────────────────────────────────────
_add("PROTECTION_STATE_UNKNOWN", ATTENTION, "The exchange did not answer when the stop-loss protection was verified; the position is kept and re-verified every cycle.", "No action needed unless it persists; the operator is alerted after four cycles.")
_add("PROTECTION_READ_UNAVAILABLE", ATTENTION, "The protective orders could not be read from the exchange.", "Retried next cycle.")
_add("PROTECTION_READ_BACK_UNAVAILABLE", ATTENTION, "The protective orders could not be read back after placement.", "Retried next cycle.")
_add("PROTECTION_READ_BACK_UNCONFIRMED", ATTENTION, "The protective orders were acknowledged but not yet visible at the exchange.", "Retried next cycle.")
_add("PROTECTION_SUBMIT_OUTCOME_UNKNOWN", ATTENTION, "A protective order's placement outcome is still unknown.", "Retried next cycle.")
_add("NATIVE_PROTECTION_READ_UNAVAILABLE", ATTENTION, "The account's protective orders could not be read.", "Retried next cycle.")
_add("ALGO_ORDERS_RESPONSE_MALFORMED", ATTENTION, "The exchange answered the protection read with a malformed body.", "Retried next cycle.")
_add("PROTECTION_CONFIRMED_ABSENT", CRITICAL, "The exchange confirmed the protective stop is gone; the position was closed by the fail-safe.", "Review the trade; the position is flat.")
_add("PROTECTION_LEG_ATTEMPTS_EXHAUSTED", CRITICAL, "Protective orders could not be placed after bounded retries; the fail-safe close applies.", "Operator review.")
_add("PROTECTION_READ_BACK_GEOMETRY_MISMATCH", CRITICAL, "A protective order at the exchange does not match the plan.", "Operator review.")
_add("PROTECTION_VENUE_REFUSED", CRITICAL, "The exchange refused the protective order.", "Operator review.")
_add("NAKED_POSITION_OPERATOR_REQUIRED", CRITICAL, "A position is open without confirmed protection and automatic handling is exhausted.", "Operator action required.")
_add("PROTECTION_STATE_UNKNOWN_OPERATOR_REVIEW", CRITICAL, "Protection could not be verified for several cycles.", "Operator review.")

# ── order submission gates ───────────────────────────────────────────────────
_add("DEMO_ORDER_SUBMISSION_DISABLED", BLOCKED, "Demo order submission is switched off on this server.", "Operator enables it after the safety tests pass.")
_add("LIVE_ORDER_SUBMISSION_DISABLED", BLOCKED, "Live order submission is disabled (Step 1).", "Not available yet.")
_add("ORDER_SUBMISSION_DISABLED", BLOCKED, "Order submission is disabled on this server.", "Operator configuration.")
_add("CATI_ENTRY_AUTHORITY_REQUIRED", BLOCKED, "An order was attempted outside the engine's entry authority and refused.", "No customer action; operator review.")
_add("PRODUCTION_REQUIRES_LIVE_BROKER_ACCOUNT", BLOCKED, "A live order requires a connected live exchange account.", "Not available in Step 1.")
_add("PRODUCTION_REQUIRES_CANONICAL_LIVE_ENDPOINT", CRITICAL, "A live order was routed to a non-canonical exchange endpoint and refused.", "Operator review.")

# ── runtime / evaluation stages ──────────────────────────────────────────────
_add("EVALUATION_FAILED", ATTENTION, "The account evaluation failed this cycle; nothing was sent.", "Retried next cycle.")
_add("IDLE_NO_TRADABLE_BOT", INFO, "No running bot on this account; nothing to evaluate.", "Deploy or resume a bot.")
_add("SYNCED", INFO, "The engine read this account on its last cycle.", "")
_add("STALE", ATTENTION, "The engine has not reported for this account recently.", "Check that the engine is running.")
_add("EXECUTION_ACCOUNTS_PRESENT", INFO, "Execution accounts are connected.", "")
_add("BROKER_CYCLE_BUSY", ATTENTION, "The engine's broker cycle is busy; the operation waited and timed out.", "Retry.")

# ── deployment contract blockers (Step 1.2) ──────────────────────────────────
for _code, _sev in (("BUDGET_TOO_SMALL_FOR_LEVEL", BLOCKED), ("BUDGET_EXCEEDS_BALANCE", BLOCKED), ("ACCOUNT_ALREADY_HAS_BOT", BLOCKED),
                    ("ACCOUNT_NOT_CONNECTED", BLOCKED), ("RISK_NOT_ACKNOWLEDGED", BLOCKED), ("LIVE_NOT_AVAILABLE", BLOCKED),
                    ("RISK_SIZE_BELOW_EXCHANGE_MINIMUM", BLOCKED), ("BROKER_NOT_SUPPORTED", BLOCKED),
                    ("ACCOUNT_BALANCE_UNAVAILABLE", ATTENTION), ("BROKER_CAPABILITY_INCOMPLETE", BLOCKED),
                    ("REQUEST_ID_REUSED", BLOCKED), ("ACCOUNT_STATE_CHANGED", BLOCKED), ("INVALID_ADVANCED_SETTINGS", BLOCKED)):
    from shared_lib.deployment.contract import BLOCKER_MESSAGES as _BM
    _add(_code, _sev, _BM[_code]["message"], _BM[_code]["action"])

# ── sizing blockers (Step 1.4) ───────────────────────────────────────────────
_add("RISK_SIZE_INVALID_INPUT", BLOCKED, "The sizing inputs were invalid; no order was sized.", "No customer action.")
_add("RISK_SIZE_STOP_DISTANCE_INVALID", BLOCKED, "The decision's stop distance is outside the permitted range; no order was sized.", "No action needed.")
_add("RISK_SIZE_OPEN_RISK_CAP_REACHED", BLOCKED, "The combined open risk of the profile is fully used.", "No action needed.")
_add("RISK_SIZE_NO_LEGAL_LEVERAGE", BLOCKED, "No leverage within the profile ceiling makes this position safe.", "No action needed.")
_add("RISK_SIZE_INSUFFICIENT_MARGIN", BLOCKED, "The account has no free margin for the position.", "Reduce exposure or fund the account.")
_add("RISK_SIZE_ZERO", BLOCKED, "The sized position is zero.", "No action needed.")


def describe(code: Optional[str]) -> Dict[str, Any]:
    """The registry entry for ``code``; an unknown code is reported as such, never as fine."""
    if not code:
        return {"code": None, "severity": UNKNOWN, "message": "No reason was recorded.", "action": ""}
    entry = _R.get(str(code))
    if entry is None:
        text = str(code)
        if text.isidentifier() and not text.isupper() and text.endswith(("Error", "Exception", "Timeout", "Failure")):
            # The engine recorded the class of an unexpected failure (never its
            # text, which could carry request details). It is a fault, said plainly.
            return {"code": text, "severity": ATTENTION,
                    "message": f"The engine could not complete this account's evaluation ({text}). No new order was sent.",
                    "action": "It is retried on the next cycle. If it persists, contact support with this code."}
        return {"code": text, "severity": UNKNOWN, "message": f"Unrecognised engine code {code}.",
                "action": "Operator review: this code has no customer-facing mapping yet."}
    return dict(entry)


def known(code: str) -> bool:
    return code in _R


def all_codes() -> frozenset:
    return frozenset(_R)


__all__ = ["INFO", "WAITING", "BLOCKED", "ATTENTION", "CRITICAL", "UNKNOWN", "describe", "known", "all_codes"]
