"""Which accounts a platform cycle serves (Section H, Step 3.1).

Two kinds, never mixed up:

* **Model accounts** are the platform's reference books: one per risk level and sizing policy, with the
  evaluator's initial equity. They have no user, no credentials and no exchange. They are what the daily
  shadow comparison holds against the evaluator.
* **Connected accounts** belong to customers. The pipeline reads which of them have an active bot and at
  which risk level, nothing else: no credential is opened and no exchange is called to build this list. A
  connected account's shadow book starts from the capital the customer allocated to the bot, and is sized
  with the EXECUTABLE policy, because that is what the engine would be allowed to do for it.

An account source keeps no state between calls.
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence, Tuple

from app.trading_intelligence.families.daily_trend import targets as T

from .driver import AccountContext
from .family_registry import RegistryEntry

MODEL_PREFIX = "MODEL"
#: a model account is on no exchange; its decisions are recorded under this label and can never be an order
MODEL_ENVIRONMENT = "MODEL"
RISK_LEVELS: Tuple[str, ...] = ("conservative", "balanced", "aggressive")


def model_account_id(policy: str, risk_level: str) -> str:
    return f"{MODEL_PREFIX}:{policy}:{risk_level}"


class ModelAccounts:
    def __init__(self, *, initial_equity: float, levels: Sequence[str] = RISK_LEVELS,
                 policies: Sequence[str] = (T.MANDATE, T.EXECUTABLE), venue: str = "binance") -> None:
        self._equity, self._levels, self._policies, self._venue = float(initial_equity), tuple(levels), tuple(policies), venue

    def accounts(self, entry: RegistryEntry) -> List[AccountContext]:
        return [AccountContext(account_id=model_account_id(p, level), user_id=None, venue=self._venue,
                               environment=MODEL_ENVIRONMENT, risk_level=level, policy=p, initial_equity=self._equity)
                for p in self._policies for level in self._levels]


class ConnectedAccounts:
    """Customer accounts with exactly one active bot. An account with several active bots is ambiguous about
    its risk level and is left out (the execution authority refuses it for the same reason)."""

    def __init__(self, db: Any, *, equity_of: Optional[Callable[[Any, str], Optional[float]]] = None) -> None:
        self._db = db
        self._equity_of = equity_of or allocated_capital

    def accounts(self, entry: RegistryEntry) -> List[AccountContext]:
        venues = {venue for venue, _ in entry.definition.supported_markets}
        out: List[AccountContext] = []
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(
                "SELECT a.id AS account_id, a.user_id, a.broker_id, a.environment, b.risk_level, COUNT(*) AS bots "
                "FROM broker_accounts a JOIN bot_instances b ON b.broker_account_id=a.id AND b.status='active' "
                "WHERE LOWER(a.status) IN ('connected','active') GROUP BY a.id ORDER BY a.id")]
            for r in rows:
                level = str(r["risk_level"] or "").lower()
                venue = str(r["broker_id"] or "").lower()
                if r["bots"] != 1 or level not in RISK_LEVELS or venue not in venues:
                    continue
                equity = self._equity_of(conn, r["account_id"])
                if equity is None or equity <= 0:
                    continue                    # no stated capital: nothing is assumed
                environment = "LIVE" if str(r["environment"] or "").lower() in ("live", "production", "mainnet") else "DEMO"
                out.append(AccountContext(account_id=r["account_id"], user_id=r["user_id"], venue=venue,
                                          environment=environment, risk_level=level, policy=T.EXECUTABLE,
                                          initial_equity=float(equity)))
        return out


def allocated_capital(conn: Any, account_id: str) -> Optional[float]:
    """The capital the customer allocated to the account's active bot, when it is stated as an amount."""
    row = conn.execute(
        "SELECT capital_allocation, capital_allocation_type, allocation_value, allocation_type FROM bot_instances "
        "WHERE broker_account_id=? AND status='active' ORDER BY created_at DESC LIMIT 1", (account_id,)).fetchone()
    if row is None:
        return None
    for value, kind in ((row[0], row[1]), (row[2], row[3])):
        try:
            amount = float(value) if value is not None else None
        except (TypeError, ValueError):
            amount = None
        # only an amount of money is a capital basis; a percentage or a per-trade risk figure is not
        if amount and amount > 0 and str(kind or "").lower() in ("fixed_amount", "fixed_usdt"):
            return amount
    return None


class CombinedAccounts:
    def __init__(self, *sources: Any) -> None:
        self._sources = sources

    def accounts(self, entry: RegistryEntry) -> List[AccountContext]:
        seen, out = set(), []
        for source in self._sources:
            for account in source.accounts(entry):
                if account.account_id not in seen:
                    seen.add(account.account_id)
                    out.append(account)
        return out


__all__ = ["MODEL_PREFIX", "MODEL_ENVIRONMENT", "RISK_LEVELS", "model_account_id", "ModelAccounts", "ConnectedAccounts",
           "CombinedAccounts", "allocated_capital"]
