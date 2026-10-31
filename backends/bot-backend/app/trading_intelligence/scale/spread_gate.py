"""The platform order budget at the execution boundary (Section H, Step 5.5; external gate EXT-LIQ-01).

``CATIExecutionBoundary`` asks ``PlatformSpreadGate.reserve`` for every production ENTRY after hard risk, the
quantity preflight and the final family re-check, and before it claims the durable attempt. Exits, reductions
and protective orders never come here: a risk-reducing order is never blocked by a liquidity budget.

What ``reserve`` requires, in order (the first failure is the refusal code; nothing is held after a refusal):

1. a platform database with the order-coordination tables (``PLATFORM_ORDER_BUDGET_UNAVAILABLE``);
2. a known tenant for the plan (``PLATFORM_ACCOUNT_TENANT_UNKNOWN``);
3. FRESH depth, read from the account's own venue client now (``get_orderbook``): unreadable
   (``DEPTH_UNAVAILABLE``), unstamped (``DEPTH_TIMESTAMP_MISSING``), older than the policy allows against the
   DATABASE clock (``DEPTH_STALE``), crossed (``DEPTH_CROSSED``) or too thin (``DEPTH_INSUFFICIENT_LEVELS``);
4. a MARKET entry -- every entry adapter of this repository sends MARKET orders -- must pass
   ``market_fallback_permitted`` on that depth: with the default policy (``allow_market_fallback=False``) it is
   refused ``MARKET_FALLBACK_DISABLED_BY_POLICY``; otherwise walking the current book for the risk-approved
   quantity must stay inside the worst permitted price (``MARKET_WALK_EXCEEDS_WORST_PRICE`` /
   ``DEPTH_INSUFFICIENT_FOR_QUANTITY``);

   4a. (L-08) when the constructor policy refuses market entries -- the production default does -- a MARKET entry
   may still proceed under a governed single-account policy record (``market_entry_policy``), and then only when
   ALL of these hold, in this order, each refusing with its own code: the record resolves for this venue / symbol /
   environment / account (``MARKET_FALLBACK_DISABLED_BY_POLICY`` when none was ever recorded -- the default, as
   before -- ``MARKET_POLICY_UNAVAILABLE``, ``MARKET_POLICY_LEDGER_INTEGRITY_FAILED``, ``MARKET_POLICY_REVOKED``,
   ``MARKET_POLICY_EXPIRED``, ``MARKET_POLICY_ENVIRONMENT_NOT_PERMITTED`` (LIVE in this build),
   ``MARKET_POLICY_ACCOUNT_SCOPE_MISMATCH``, ``MARKET_POLICY_TERMS_INVALID``); the same book passes the record's own
   (equal or tighter) depth terms (``DEPTH_STALE`` / ``DEPTH_CROSSED`` / ``DEPTH_INSUFFICIENT_LEVELS``); the
   quoted spread is inside the record's bound (``MARKET_SPREAD_UNKNOWN`` / ``MARKET_SPREAD_EXCESSIVE``); the account
   is an ACTIVE registered platform account of this tenant on this venue and environment
   (``PLATFORM_ACCOUNT_NOT_REGISTERED`` / ``PLATFORM_ACCOUNT_TENANT_MISMATCH`` /
   ``MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE``); the market walk (as 4); the predicted slippage of the walk is inside
   the record's band and the plan's own bound (``MARKET_PREDICTED_SLIPPAGE_EXCEEDS_BOUND``); at the WORST walked
   price the stop is on the protective side, at most 15 % away, and the risk-approved quantity risks at most
   0.40 % of equity (``EXECUTABLE_PRICE_RISK_UNVERIFIABLE`` / ``EXECUTABLE_STOP_GEOMETRY_INVALID`` /
   ``EXECUTABLE_STOP_DISTANCE_EXCEEDED`` / ``EXECUTABLE_PRICE_RISK_EXCEEDED`` -- refused, never resized); and a
   joined budget was opened under this same record (``MARKET_POLICY_BUDGET_MISMATCH``). The record is never
   calibrated, so 5 still refuses a second account. Everything the boundary checks before this gate (family
   certification and promotion, kill switches, adapter certification, environment and go-live gate, instrument
   filters, capital, hard risk, the account fence, native reduce-only protection) and after it (the claim, the
   raw submission permit) is unchanged;
5. the shared budget of (venue, venue symbol, shared signal, side) -- opened or joined -- and a RESERVED slot
   for the FULL risk-approved notional of this account, in one transaction (``PlatformOrderBudget.reserve_for``):
   ``PLATFORM_ACCOUNT_NOT_REGISTERED``, ``PLATFORM_ACCOUNT_TENANT_MISMATCH``, ``BUDGET_EXPIRED``,
   ``ORDER_SPREADING_CALIBRATION_REQUIRED`` (EXT-LIQ-01: another account already holds an active slot on this
   venue/symbol/side while the policy is uncalibrated), ``PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT``,
   ``PLATFORM_SYMBOL_BUDGET_EXHAUSTED``, ``PLATFORM_ORDER_SLOT_ALREADY_ACTIVE``, ``PLATFORM_ORDER_SLOT_NOT_DUE``.
   The quantity is never resized to fit.

Calibration status: the default policy is ``CONSERVATIVE_DEFAULT`` (``calibrated=False``, no impact
coefficients). Nothing here sets ``calibrated``; a calibrated policy needs an owner ``approval_ref`` (enforced by
``SpreadPolicy`` and by the schema) that no code creates. Until then a single account may proceed under the
conservative cap and a second account on the same venue/symbol/side is refused. A governed market-entry record
(4a) is recorded only by an operator through ``/api/v1/admin/order-spreading`` with an owner approval reference;
none exists in this repository, so every production MARKET entry is still refused.

Slot lifecycle, tied to the durable attempt (``platform_spread_decisions`` links slot <-> attempt)::

    reserve()                 -> RESERVED             (refused: nothing held, REFUSED row with the code)
    attempt claimed           -> SUBMITTED            (claim lost / duplicate / reservation lost -> RELEASED)
    settle: position exists   -> FILLED  (granted_notional := ACTUAL filled notional; partial fills shrink it)
    settle: no entry          -> RELEASED
    settle: SUBMIT_UNKNOWN    -> stays SUBMITTED until reconcile_submit_unknown -> FILLED | RELEASED
    budget TTL elapsed        -> no longer counted; unlinked unused slots -> EXPIRED (sweep)
    crash recovery (sweep)    -> every open link is settled from its attempt's latest durable state

Calibration evidence (no venue order of its own): per entry attempt, ``record_entry_observation`` stores the
depth the decision used and what the venue did (``platform_spread_observations``). A recorder failure is
swallowed: it never affects the order path.
"""
from __future__ import annotations

import logging
import os
import threading
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

from shared_lib.persistence.sql import Database, decimal_in, decimal_out

from .order_spreading import (BUY, CONSERVATIVE_DEFAULT, SELL, BudgetRefused, CapDecision, DepthRefused,
                              DepthSnapshot, PlatformOrderBudget, SpreadPolicy, aggregate_cap,
                              market_fallback_permitted, walk_book)

logger = logging.getLogger(__name__)

GATE_VERSION = "1.0.0"
EXTERNAL_GATE = "EXT-LIQ-01"
DEPTH_LIMIT = 50
#: a RESERVED slot whose attempt was never claimed is released by the sweep only after this long (database
#: clock), so a sweep cannot race the boundary between ``reserve`` and the claim
CLAIM_GRACE_MS = 120_000

GATE_NOT_AUTHORITATIVE = "ORDER_SPREADING_GATE_NOT_AUTHORITATIVE"
BUDGET_UNAVAILABLE = "PLATFORM_ORDER_BUDGET_UNAVAILABLE"
TENANT_UNKNOWN = "PLATFORM_ACCOUNT_TENANT_UNKNOWN"
DEPTH_UNAVAILABLE = "DEPTH_UNAVAILABLE"
DEPTH_TIMESTAMP_MISSING = "DEPTH_TIMESTAMP_MISSING"
SLOT_TRANSITION_FAILED = "PLATFORM_ORDER_SLOT_TRANSITION_FAILED"
IMPACT_MODEL = "VISIBLE_BOOK_WALK_UNCALIBRATED"
MARKET = "MARKET"

#: the platform migrations the gate needs: order coordination (3) and the boundary links (60)
REQUIRED_MIGRATIONS = (3, 60)

_FILLED_ATTEMPT = frozenset({"FILLED", "PARTIALLY_FILLED", "RECONCILED_POSITION_EXISTS", "POSITION_CLOSED"})
_OPEN_ATTEMPT = frozenset({"PENDING_SUBMIT", "SUBMIT_UNKNOWN"})


class SpreadRefused(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class SpreadGrant:
    decision_id: str
    budget_id: str
    slot_id: str
    account_id: str
    tenant_id: str
    signal_id: str
    venue: str
    symbol: str
    side: str
    quantity: Decimal
    requested_notional: Decimal
    calibrated: bool
    policy_version: str
    order_style: str
    depth: DepthSnapshot
    cap: CapDecision
    depth_age_ms: int
    decided_at: int


def _dec(value: Any) -> Optional[Decimal]:
    """An exact decimal from a venue/float field (``Decimal(str(x))``), ``None`` when absent or not finite."""
    if value is None or isinstance(value, bool):
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return d if d.is_finite() else None


def signal_id_of(plan: Any) -> str:
    """The signal several accounts answer together: family, family version and decision time of the immutable
    decision. The decision id itself is per account on the pipeline path (``DecisionStore.decision_id(cycle,
    account)``) and shared on the residual path; family + version + decision time is the same for every account
    answering one signal on both."""
    from app.trading_intelligence.pipeline import contracts as PC
    p = PC.provenance_of(plan)
    return "sig:" + "|".join(str(p.get(k) or "") for k in ("family_id", "family_version", "decision_time_ms"))


def entry_side(plan: Any) -> str:
    return BUY if str(plan.side).upper() == "LONG" else SELL


def entry_order_style(adapter: Any, plan: Any) -> str:
    """How the entry will ACTUALLY be sent. Every entry adapter of this repository sends a MARKET order (the
    executor's entry is MARKET; ``BinanceExecutionAdapter.translate`` reports ``actual_order_type='MARKET'``),
    whatever the plan prefers. Only an adapter that declares ``entry_order_type == 'LIMIT'`` -- none does today
    -- and a plan with no MARKET preference or fallback would be a limit entry."""
    prefs = getattr(plan, "execution_preferences", None)
    styles = {str(getattr(prefs, "preferred_order_style", "") or "").upper(),
              *(str(s).upper() for s in (getattr(prefs, "fallback_order_styles", ()) or ()))}
    if getattr(adapter, "entry_order_type", None) == "LIMIT" and MARKET not in styles:
        return "LIMIT"
    return MARKET


def read_depth(client: Any, venue: str, symbol: str, *, limit: int = DEPTH_LIMIT) -> DepthSnapshot:
    """Read the CURRENT order book through the account's own venue client (``get_orderbook``: Binance USD-M
    ``/fapi/v1/depth`` time ``T``, Bybit V5 ``ts``, BingX ``T``). Anything but a well-formed, venue-stamped book
    is refused; a missing stamp is never replaced by the local clock."""
    reader = getattr(client, "get_orderbook", None)
    if client is None or not callable(reader):
        raise DepthRefused(DEPTH_UNAVAILABLE, "no order-book read on the account client")
    try:
        raw = reader(symbol, limit=limit)
    except Exception as exc:  # an unread book is not a thin book: refuse, never assume
        raise DepthRefused(DEPTH_UNAVAILABLE, type(exc).__name__)
    if not isinstance(raw, Mapping) or not isinstance(raw.get("bids"), (list, tuple)) \
            or not isinstance(raw.get("asks"), (list, tuple)):
        raise DepthRefused(DEPTH_UNAVAILABLE, "malformed order book")
    stamp = raw.get("time")
    try:
        observed = int(stamp) if stamp is not None and not isinstance(stamp, bool) else 0
    except (TypeError, ValueError):
        observed = 0
    if observed <= 0:
        raise DepthRefused(DEPTH_TIMESTAMP_MISSING)

    def levels(side):
        out = []
        for level in side:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                raise DepthRefused(DEPTH_UNAVAILABLE, "malformed level")
            p, q = _dec(level[0]), _dec(level[1])
            if p is None or q is None or p <= 0 or q <= 0:
                raise DepthRefused(DEPTH_UNAVAILABLE, "malformed level")
            out.append((p, q))
        return out

    bids, asks = levels(raw["bids"]), levels(raw["asks"])
    if any(bids[i][0] <= bids[i + 1][0] for i in range(len(bids) - 1)) or \
            any(asks[i][0] >= asks[i + 1][0] for i in range(len(asks) - 1)):
        raise DepthRefused(DEPTH_UNAVAILABLE, "levels out of order")
    return DepthSnapshot(venue, symbol, tuple(bids), tuple(asks), observed)


def _bps(numerator: Decimal, reference: Decimal) -> Decimal:
    return (numerator / reference * Decimal(10_000)).quantize(Decimal("0.00000001"))


_databases: Dict[str, Database] = {}
_databases_lock = threading.Lock()


def platform_database() -> Database:
    """The platform database of this process, named EXPLICITLY by ``PLATFORM_DATABASE_URL`` (as the go-live gate
    requires). Unlike the API processes' ``shared_lib.operations.platform.platform_db`` there is no fallback to
    ``DATABASE_URL``: the execution path never guesses which database holds the shared order budget. Absent ->
    ``SpreadRefused(PLATFORM_ORDER_BUDGET_UNAVAILABLE)``, i.e. every production entry is refused."""
    url = os.environ.get("PLATFORM_DATABASE_URL")
    if not url:
        raise SpreadRefused(BUDGET_UNAVAILABLE, "PLATFORM_DATABASE_URL_NOT_CONFIGURED")
    with _databases_lock:
        db = _databases.get(url)
        if db is None:
            db = _databases[url] = Database(url, pool_size=4)
        return db


class PlatformSpreadGate:
    """The boundary's view of ``PlatformOrderBudget``. ``db`` absent -> ``platform_database()`` resolved at each
    use; unavailable or unmigrated -> every entry is refused. ``policy`` is a constructor argument, never a
    setting."""

    def __init__(self, db: Optional[Database] = None, *, policy: SpreadPolicy = CONSERVATIVE_DEFAULT,
                 depth_limit: int = DEPTH_LIMIT, claim_grace_ms: int = CLAIM_GRACE_MS) -> None:
        self._db = db
        self.policy = policy
        self.depth_limit = int(depth_limit)
        self.claim_grace_ms = int(claim_grace_ms)

    # ── the platform database ───────────────────────────────────────────────────────────────────────────
    def database(self) -> Database:
        return self._db if self._db is not None else platform_database()

    @staticmethod
    def _schema_ready(conn) -> bool:
        marks = ",".join("?" * len(REQUIRED_MIGRATIONS))
        rows = conn.all(f"SELECT version FROM platform_schema_migrations WHERE version IN ({marks})",
                        REQUIRED_MIGRATIONS)
        return {int(r[0]) for r in rows} >= set(REQUIRED_MIGRATIONS)

    def _ready_db(self) -> Database:
        try:
            db = self.database()
            with db.transaction(write=False) as conn:
                ready = self._schema_ready(conn)
        except SpreadRefused:
            raise
        except Exception as exc:
            raise SpreadRefused(BUDGET_UNAVAILABLE, type(exc).__name__)
        if not ready:
            raise SpreadRefused(BUDGET_UNAVAILABLE, "ORDER_SPREADING_SCHEMA_MISSING")
        return db

    # ── reservation ─────────────────────────────────────────────────────────────────────────────────────
    def reserve(self, plan: Any, *, client: Any, adapter: Any, quantity: Any, notional: Any, attempt_id: str,
                client_order_id: Optional[str] = None, tick: Any = None, stop_price: Any = None, equity: Any = None,
                max_slippage_bps: Any = None) -> SpreadGrant:
        """A RESERVED slot covering the full ``notional`` of this ENTRY, or ``SpreadRefused`` (holding nothing).
        ``quantity`` / ``notional`` are the risk-approved size; they are never changed here. ``stop_price``,
        ``equity`` and the plan's ``max_slippage_bps`` are what a governed market entry (L-08) is validated
        against at the executable price; without them such an entry is refused."""
        side = entry_side(plan)
        venue, symbol = str(plan.venue), str(plan.instrument_key.venue_symbol).upper()
        account, tenant = str(plan.broker_account_id), (str(plan.user_id) if plan.user_id else None)
        signal = signal_id_of(plan)
        style = entry_order_style(adapter, plan)
        qty, want = _dec(quantity), _dec(notional)
        ctx = dict(venue=venue, symbol=symbol, side=side, signal_id=signal, account_id=account, tenant_id=tenant,
                   environment=str(plan.environment), execution_attempt_id=attempt_id,
                   trade_plan_id=plan.trade_plan_id, client_order_id=client_order_id, requested_notional=want,
                   order_style=style)
        db = None
        try:
            db = self._ready_db()
            if not tenant:
                raise SpreadRefused(TENANT_UNKNOWN)
            if qty is None or want is None or qty <= 0 or want <= 0:
                raise SpreadRefused("REQUESTED_NOTIONAL_INVALID")
            try:
                depth = read_depth(client, venue, symbol, limit=self.depth_limit)
                with db.transaction(write=False) as conn:
                    now = conn.now_ms()
                cap = aggregate_cap(depth, side, self.policy, now_ms=now)
            except DepthRefused as exc:
                ctx["depth_hash"] = None
                raise SpreadRefused(exc.code, str(exc))
            ctx["depth_hash"] = depth.digest
            policy, governed = self.policy, None
            if style == MARKET and not self.policy.allow_market_fallback:
                # L-08: the constructor policy refuses market entries. Only an approved, recorded, active
                # single-account policy for this venue / symbol / environment can permit one; none recorded is the
                # same refusal as before (MARKET_FALLBACK_DISABLED_BY_POLICY). The record is never calibrated.
                governed = self._governed_policy(db, venue, symbol, ctx["environment"], account)
                policy = governed.spread_policy()
                ctx["policy_version"] = policy.policy_version
                try:   # the record's own (equal or tighter) depth terms, on the same book and database time
                    cap = aggregate_cap(depth, side, policy, now_ms=now)
                except DepthRefused as exc:
                    raise SpreadRefused(exc.code, str(exc))

            def market_walk(worst):
                ok, why = market_fallback_permitted(depth, side, qty, worst, policy=policy, now_ms=now)
                if not ok:
                    raise SpreadRefused(why)

            if style == MARKET:  # refused before any budget is opened
                if governed is not None:
                    _spread_within(depth, governed.terms.max_spread_bps,
                                   getattr(getattr(plan, "execution_preferences", None), "max_spread_bps", None))
                    self._account_eligible(db, account, tenant, venue, ctx["environment"])
                market_walk(cap.worst_price)
                if governed is not None:
                    _executable_market_entry(depth, side, qty, cap, policy, stop_price=stop_price, equity=equity,
                                             plan_slippage_bps=max_slippage_bps)
            budget = PlatformOrderBudget(db, policy)
            try:
                budget_id = budget.open_budget(venue=venue, symbol=symbol, signal_id=signal, side=side, depth=depth)
            except (DepthRefused, BudgetRefused) as exc:
                raise SpreadRefused(exc.code, str(exc))
            with db.transaction(write=False) as conn:
                stored = conn.one("SELECT worst_price, calibrated, policy_version FROM platform_symbol_budgets "
                                  "WHERE budget_id=?", (budget_id,))
            stored_worst, calibrated = decimal_out(stored[0]), bool(int(stored[1])) and policy.calibrated
            if governed is not None and stored[2] != policy.policy_version:
                # a budget opened under another (revoked, expired or superseded) policy is never joined
                raise SpreadRefused(MARKET_POLICY_BUDGET_MISMATCH, str(stored[2]))
            tighter = stored_worst < cap.worst_price if side == BUY else stored_worst > cap.worst_price
            if style == MARKET and tighter:
                market_walk(stored_worst)  # a joined budget's committed band is tighter than the book read now
            decision_id = uuid.uuid4().hex

            def link(conn, slot, at):
                conn.execute(
                    "INSERT INTO platform_spread_decisions (decision_id, venue, symbol, side, signal_id, account_id, "
                    "tenant_id, environment, execution_attempt_id, trade_plan_id, client_order_id, outcome, code, "
                    "budget_id, slot_id, requested_notional, granted_notional, calibrated, policy_version, "
                    "order_style, depth_hash, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'RESERVED',NULL,?,?,?,?,?,?,?,?,?)",
                    (decision_id, venue, symbol, side, signal, account, tenant, ctx["environment"], attempt_id,
                     plan.trade_plan_id, client_order_id, budget_id, slot.slot_id, decimal_in(want),
                     decimal_in(slot.granted), 1 if calibrated else 0, policy.policy_version, style, depth.digest,
                     at))

            try:
                slot = budget.reserve_for(budget_id, account_id=account, tenant_id=tenant, notional=want,
                                          tick=_dec(tick), link=link)
            except BudgetRefused as exc:
                raise SpreadRefused(exc.code)
            if slot.granted != want:  # defence in depth: reserve_for never grants less than asked
                self._settle_decision(db, decision_id, "RELEASED", reason=PARTIAL_GRANT_CODE)
                raise SpreadRefused(PARTIAL_GRANT_CODE)
            _metric("RESERVED", "RESERVED")
            return SpreadGrant(decision_id, budget_id, slot.slot_id, account, tenant, signal, venue, symbol, side, qty,
                               want, calibrated, policy.policy_version, style, depth, cap,
                               now - depth.observed_at_ms, now)
        except SpreadRefused as exc:
            _metric("REFUSED", exc.code)
            self._record_refusal(db, ctx, exc.code)
            raise
        except Exception as exc:  # an undecidable budget is a refusal, never a pass
            _metric("REFUSED", BUDGET_UNAVAILABLE)
            self._record_refusal(db, ctx, BUDGET_UNAVAILABLE)
            raise SpreadRefused(BUDGET_UNAVAILABLE, type(exc).__name__)

    def _record_refusal(self, db: Optional[Database], ctx: Mapping[str, Any], code: str) -> None:
        """Best effort: the refusal itself never depends on being recorded."""
        if db is None:
            return
        try:
            with db.transaction() as conn:
                conn.execute(
                    "INSERT INTO platform_spread_decisions (decision_id, venue, symbol, side, signal_id, account_id, "
                    "tenant_id, environment, execution_attempt_id, trade_plan_id, client_order_id, outcome, code, "
                    "requested_notional, calibrated, policy_version, order_style, depth_hash, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,'REFUSED',?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, ctx["venue"], ctx["symbol"], ctx["side"], ctx["signal_id"], ctx["account_id"],
                     ctx["tenant_id"], ctx["environment"], ctx["execution_attempt_id"], ctx["trade_plan_id"],
                     ctx["client_order_id"], code, decimal_in(ctx["requested_notional"]),
                     1 if self.policy.calibrated else 0, ctx.get("policy_version") or self.policy.policy_version,
                     ctx["order_style"], ctx.get("depth_hash"), conn.now_ms()))
        except Exception as exc:
            logger.warning("order spreading refusal not recorded: %s", type(exc).__name__)

    # ── the governed single-account market-entry policy (L-08) ─────────────────────────────────────────────
    @staticmethod
    def _governed_policy(db: Database, venue: str, symbol: str, environment: str, account: str):
        """The active approved record for this scope (``market_entry_policy.MarketEntryPolicyLedger.resolve``) or
        ``SpreadRefused`` with its code. Read from the same platform database as the budget, at each reservation:
        a revocation applies to the very next entry."""
        from .market_entry_policy import MarketEntryPolicyLedger, MarketPolicyError
        try:
            record, _ = MarketEntryPolicyLedger(db).resolve(venue=venue, symbol=symbol, environment=environment,
                                                            account_id=account)
        except MarketPolicyError as exc:
            raise SpreadRefused(exc.code, exc.detail)
        return record

    @staticmethod
    def _account_eligible(db: Database, account: str, tenant: Optional[str], venue: str, environment: str) -> None:
        """A governed market entry is for an ACTIVE registered platform account of this tenant, on this venue and
        environment. (``reserve_for`` checks registration and tenant again inside its own transaction.)"""
        with db.transaction(write=False) as conn:
            row = conn.one("SELECT tenant_id, venue, environment, state FROM platform_accounts WHERE account_id=?",
                           (account,))
        if row is None:
            raise SpreadRefused(ACCOUNT_NOT_REGISTERED_CODE)
        if row[0] != tenant:
            raise SpreadRefused(ACCOUNT_TENANT_MISMATCH_CODE)
        if str(row[3]) != "ACTIVE" or str(row[1]).upper() != venue.upper() or \
                str(row[2]).upper() != environment.upper():
            raise SpreadRefused(MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE,
                                f"state={row[3]} venue={row[1]} environment={row[2]}")

    # ── lifecycle ───────────────────────────────────────────────────────────────────────────────────────
    def mark_submitted(self, grant: SpreadGrant) -> None:
        """RESERVED -> SUBMITTED once the attempt is durably claimed. Raises when the slot did not move."""
        with self.database().transaction() as conn:
            if not PlatformOrderBudget.transition_on(conn, grant.slot_id, "SUBMITTED", tenant_id=grant.tenant_id):
                raise SpreadRefused(SLOT_TRANSITION_FAILED)

    def release(self, grant: SpreadGrant, reason: str) -> None:
        """A reserved slot whose order is not (or provably was not) sent."""
        with self.database().transaction() as conn:
            PlatformOrderBudget.transition_on(conn, grant.slot_id, "RELEASED", tenant_id=grant.tenant_id,
                                              reason=reason[:120])
            self._close_link(conn, grant.decision_id, "RELEASED", None, reason)

    def settle_attempt(self, *, account_id: str, tenant_id: str, attempt_id: str, outcome: str,
                       filled_notional: Optional[Decimal] = None, reason: str = "") -> int:
        """Apply an attempt's outcome to every open slot linked to it. ``outcome``: ``FILLED`` (granted notional
        becomes the ACTUAL filled notional), ``RELEASED``, or ``KEEP`` (unknown: the slot stays SUBMITTED).
        Raises ``SpreadRefused(PLATFORM_ORDER_BUDGET_UNAVAILABLE)`` when no migrated platform database exists (then
        no slot can have been reserved either)."""
        db = self._ready_db()
        with db.transaction() as conn:
            rows = conn.all("SELECT decision_id, slot_id FROM platform_spread_decisions WHERE account_id=? AND "
                            "tenant_id=? AND execution_attempt_id=? AND outcome='RESERVED' AND settled_at IS NULL",
                            (account_id, tenant_id, attempt_id))
            for decision_id, slot_id in rows:
                self._apply(conn, decision_id, slot_id, tenant_id, outcome, filled_notional, reason)
        return len(rows)

    def _apply(self, conn, decision_id: str, slot_id: str, tenant_id: str, outcome: str,
               filled_notional: Optional[Decimal], reason: str) -> None:
        move = PlatformOrderBudget.transition_on
        if outcome == "KEEP":
            move(conn, slot_id, "SUBMITTED", tenant_id=tenant_id)       # a crash before the mark: the order may exist
            return
        if outcome == "FILLED":
            move(conn, slot_id, "SUBMITTED", tenant_id=tenant_id)
            move(conn, slot_id, "FILLED", tenant_id=tenant_id, filled_notional=filled_notional)
            self._close_link(conn, decision_id, "FILLED", filled_notional, reason)
            return
        move(conn, slot_id, "RELEASED", tenant_id=tenant_id, reason=(reason or "ATTEMPT_RESOLVED_NO_ENTRY")[:120])
        self._close_link(conn, decision_id, "RELEASED", None, reason)

    @staticmethod
    def _close_link(conn, decision_id: str, status: str, notional: Optional[Decimal], reason: str) -> None:
        conn.execute("UPDATE platform_spread_decisions SET settled_status=?, settled_at=?, settled_notional=?, "
                     "settle_reason=? WHERE decision_id=? AND settled_at IS NULL",
                     (status, conn.now_ms(), decimal_in(notional), (reason or None) and reason[:200], decision_id))

    def _settle_decision(self, db: Database, decision_id: str, status: str, *, reason: str) -> None:
        with db.transaction() as conn:
            row = conn.one("SELECT slot_id, tenant_id FROM platform_spread_decisions WHERE decision_id=?",
                           (decision_id,))
            if row is not None:
                self._apply(conn, decision_id, row[0], row[1], status, None, reason)

    def sweep(self, *, account_id: str, tenant_id: str,
              attempt_state: Callable[[str], Optional[Tuple[str, Mapping[str, Any]]]]) -> Dict[str, int]:
        """Crash recovery for ONE account: every open slot link is settled from the latest DURABLE state of its
        attempt (``attempt_state(attempt_id) -> (status, payload) | None``), so no slot stays active for a
        resolved attempt; then this account's unused, unlinked slots on expired budgets become EXPIRED. Raises
        ``SpreadRefused(PLATFORM_ORDER_BUDGET_UNAVAILABLE)`` when there is no migrated platform database."""
        db = self._ready_db()
        counts = {"filled": 0, "released": 0, "kept": 0, "expired": 0}
        with db.transaction(write=False) as conn:
            now = conn.now_ms()
            rows = conn.all("SELECT decision_id, slot_id, execution_attempt_id, created_at FROM platform_spread_decisions "
                            "WHERE account_id=? AND tenant_id=? AND outcome='RESERVED' AND settled_at IS NULL",
                            (account_id, tenant_id))
        for decision_id, slot_id, attempt_id, created_at in rows:
            state = attempt_state(attempt_id) if attempt_id else None
            if state is None:
                if now - int(created_at) < self.claim_grace_ms:
                    continue
                outcome, notional, reason = "RELEASED", None, "ATTEMPT_NEVER_CLAIMED"
            else:
                status, payload = state
                if status in _OPEN_ATTEMPT:
                    outcome, notional, reason = "KEEP", None, ""
                elif status in _FILLED_ATTEMPT:
                    q, p = _dec(payload.get("filled_quantity")), _dec(payload.get("filled_price"))
                    outcome, notional, reason = "FILLED", (q * p if q is not None and p is not None else None), \
                        f"ATTEMPT_{status}"
                else:
                    outcome, notional, reason = "RELEASED", None, f"ATTEMPT_{status}"
            with db.transaction() as conn:
                self._apply(conn, decision_id, slot_id, tenant_id, outcome, notional, reason)
            counts[{"KEEP": "kept", "FILLED": "filled"}.get(outcome, "released")] += 1
        counts["expired"] = expire_unused(db, account_id=account_id, tenant_id=tenant_id)
        return counts

    # ── calibration evidence (D4) ───────────────────────────────────────────────────────────────────────
    def record_entry_observation(self, grant: SpreadGrant, *, attempt_id: str, entry: Any) -> bool:
        """One row per entry attempt: the depth the decision used and what the venue did. Never raises."""
        try:
            return record_observation(
                self.database(), source="ENTRY_ATTEMPT", reference_id=attempt_id, depth=grant.depth, side=grant.side,
                quantity=grant.quantity, notional=grant.requested_notional, cap=grant.cap,
                depth_age_ms=grant.depth_age_ms, account_id=grant.account_id, tenant_id=grant.tenant_id,
                budget_id=grant.budget_id, slot_id=grant.slot_id, fill_price=getattr(entry, "avg_fill_price", None),
                filled_quantity=getattr(entry, "filled_qty", None), requested_quantity=getattr(entry, "requested_qty", None),
                entry_status=getattr(entry, "status", None))
        except Exception as exc:
            logger.warning("order spreading observation not recorded: %s", type(exc).__name__)
            return False


PARTIAL_GRANT_CODE = "PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT"
ACCOUNT_NOT_REGISTERED_CODE = "PLATFORM_ACCOUNT_NOT_REGISTERED"
ACCOUNT_TENANT_MISMATCH_CODE = "PLATFORM_ACCOUNT_TENANT_MISMATCH"

#: refusal codes of a governed market entry (L-08), after the record itself resolved
MARKET_SPREAD_UNKNOWN = "MARKET_SPREAD_UNKNOWN"
MARKET_SPREAD_EXCESSIVE = "MARKET_SPREAD_EXCESSIVE"
MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE = "MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE"
MARKET_PREDICTED_SLIPPAGE_EXCEEDS_BOUND = "MARKET_PREDICTED_SLIPPAGE_EXCEEDS_BOUND"
EXECUTABLE_PRICE_RISK_UNVERIFIABLE = "EXECUTABLE_PRICE_RISK_UNVERIFIABLE"
EXECUTABLE_STOP_GEOMETRY_INVALID = "EXECUTABLE_STOP_GEOMETRY_INVALID"
EXECUTABLE_STOP_DISTANCE_EXCEEDED = "EXECUTABLE_STOP_DISTANCE_EXCEEDED"
EXECUTABLE_PRICE_RISK_EXCEEDED = "EXECUTABLE_PRICE_RISK_EXCEEDED"
MARKET_POLICY_BUDGET_MISMATCH = "MARKET_POLICY_BUDGET_MISMATCH"


def _spread_within(depth: DepthSnapshot, max_spread_bps: Decimal, plan_spread_bps: Any = None) -> None:
    """The quoted spread (best ask - best bid over the mid) must be inside the record's bound and the plan's own."""
    if not depth.bids or not depth.asks:
        raise SpreadRefused(MARKET_SPREAD_UNKNOWN)
    bound, own = max_spread_bps, _dec(plan_spread_bps)
    if own is not None and own > 0:
        bound = min(bound, own)
    bid, ask = depth.bids[0][0], depth.asks[0][0]
    spread = _bps(ask - bid, (ask + bid) / 2)
    if spread > bound:
        raise SpreadRefused(MARKET_SPREAD_EXCESSIVE, f"{spread} bps > {bound} bps")


def worst_walked_price(depth: DepthSnapshot, side: str, quantity: Decimal) -> Optional[Decimal]:
    """The price of the LAST visible level a market order for ``quantity`` would reach, or ``None`` when the
    visible book does not hold that much."""
    left = Decimal(quantity)
    for p, q in (depth.asks if side == BUY else depth.bids):
        left -= q
        if left <= 0:
            return p
    return None


def _executable_market_entry(depth: DepthSnapshot, side: str, qty: Decimal, cap: CapDecision, policy: SpreadPolicy,
                             *, stop_price: Any, equity: Any, plan_slippage_bps: Any) -> None:
    """A governed market entry at the price the book says it would ACTUALLY get. Refused -- never resized --
    when the predicted slippage of the walk exceeds the record's band or the plan's own bound, or when, at the
    WORST walked price, the stop is on the wrong side, further than the 15 % entry-stop ceiling
    (``risk_sizing.MAX_STOP_DISTANCE``) or the risk of the risk-approved quantity exceeds the 0.40 % per-trade
    ceiling of equity (``risk_levels.SYSTEM_PER_TRADE_RISK_CEILING_PCT``). Visible-book walk only: hidden liquidity
    and the book's reaction are unknown to it."""
    from app.trading_intelligence.execution.risk_sizing import MAX_STOP_DISTANCE
    from shared_lib.risk_levels import SYSTEM_PER_TRADE_RISK_CEILING_PCT

    vwap, worst = walk_book(depth, side, qty), worst_walked_price(depth, side, qty)
    if vwap is None or worst is None:  # market_fallback_permitted already refused this; kept fail-closed
        raise SpreadRefused("DEPTH_INSUFFICIENT_FOR_QUANTITY")
    sign = Decimal(1) if side == BUY else Decimal(-1)
    predicted = _bps(sign * (vwap - cap.reference_price), cap.reference_price)
    bound = policy.max_slippage_bps
    plan_bound = _dec(plan_slippage_bps)
    if plan_bound is not None and plan_bound > 0:
        bound = min(bound, plan_bound)
    if predicted > bound:
        raise SpreadRefused(MARKET_PREDICTED_SLIPPAGE_EXCEEDS_BOUND, f"{predicted} bps > {bound} bps")
    stop, eq = _dec(stop_price), _dec(equity)
    if stop is None or stop <= 0 or eq is None or eq <= 0:
        raise SpreadRefused(EXECUTABLE_PRICE_RISK_UNVERIFIABLE, "stop price and equity are required")
    if (side == BUY and stop >= worst) or (side == SELL and stop <= worst):
        raise SpreadRefused(EXECUTABLE_STOP_GEOMETRY_INVALID, f"stop {stop} vs worst walked price {worst}")
    distance = abs(worst - stop) / worst
    if distance > MAX_STOP_DISTANCE:
        raise SpreadRefused(EXECUTABLE_STOP_DISTANCE_EXCEEDED, f"{distance} > {MAX_STOP_DISTANCE}")
    risk = qty * abs(worst - stop) / eq
    if risk > SYSTEM_PER_TRADE_RISK_CEILING_PCT / Decimal(100):
        raise SpreadRefused(EXECUTABLE_PRICE_RISK_EXCEEDED,
                            f"{risk} of equity > {SYSTEM_PER_TRADE_RISK_CEILING_PCT} %")


def _metric(outcome: str, code: str) -> None:
    try:
        from app.trading_intelligence.observability.metrics import METRICS
        METRICS.inc("cati_order_spreading_total", outcome=outcome, code=code)
    except Exception:
        pass


def expire_unused(db: Database, *, account_id: Optional[str] = None, tenant_id: Optional[str] = None) -> int:
    """Unused slots (REQUESTED / RESERVED / DEFERRED) on budgets past their TTL become EXPIRED -- except a slot
    still linked to an open attempt, which only its attempt's outcome settles. Scoped to one account (and its
    tenant) when given. An expired budget's slots already stopped counting against the cap; this records it."""
    scope, params = "", []
    if account_id is not None:
        scope, params = " AND account_id=? AND tenant_id=?", [account_id, tenant_id]
    with db.transaction() as conn:
        now = conn.now_ms()
        cur = conn.execute(
            "UPDATE platform_order_slots SET status='EXPIRED', reason='BUDGET_TTL_ELAPSED', updated_at=? WHERE status IN "
            "('REQUESTED','RESERVED','DEFERRED') AND budget_id IN (SELECT budget_id FROM platform_symbol_budgets WHERE "
            "expires_at <= ?) AND slot_id NOT IN (SELECT slot_id FROM platform_spread_decisions WHERE "
            "outcome='RESERVED' AND settled_at IS NULL AND slot_id IS NOT NULL)" + scope, (now, now, *params))
        return max(int(cur.rowcount or 0), 0)


def record_observation(db: Database, *, source: str, reference_id: str, depth: DepthSnapshot, side: str,
                       quantity: Any, notional: Any = None, cap: Optional[CapDecision] = None,
                       depth_age_ms: Optional[int] = None, account_id: Optional[str] = None,
                       tenant_id: Optional[str] = None, budget_id: Optional[str] = None, slot_id: Optional[str] = None,
                       fill_price: Any = None, filled_quantity: Any = None, requested_quantity: Any = None,
                       entry_status: Optional[str] = None) -> bool:
    """Write one calibration observation (idempotent per ``(source, reference_id)``). Every number is an exact
    decimal. ``impact_estimate_bps`` is the visible-book walk for ``quantity`` (``impact_model`` says so) and the
    row is stored ``impact_calibrated = 0``. ``source='SHADOW'`` takes a shadow measurement's own book and
    simulated fill; the shadow book of this repository does not record depth today, so nothing calls it yet."""
    qty, want = _dec(quantity), _dec(notional)
    book = depth.asks if side == BUY else depth.bids
    best_bid = depth.bids[0][0] if depth.bids else None
    best_ask = depth.asks[0][0] if depth.asks else None
    spread_bps = None
    if best_bid is not None and best_ask is not None:
        spread_bps = _bps(best_ask - best_bid, (best_ask + best_bid) / 2)
    reference = cap.reference_price if cap is not None else (book[0][0] if book else None)
    sign = Decimal(1) if side == BUY else Decimal(-1)
    vwap = walk_book(depth, side, qty) if qty is not None and qty > 0 else None
    impact = _bps(sign * (vwap - reference), reference) if vwap is not None and reference else None
    fill, filled = _dec(fill_price), _dec(filled_quantity)
    if fill is not None and fill <= 0:
        fill = None
    slippage = _bps(sign * (fill - reference), reference) if fill is not None and reference else None
    asked = _dec(requested_quantity) or qty
    partial = None
    if filled is not None:
        partial = 1 if (str(entry_status or "") == "PARTIALLY_FILLED" or (asked is not None and 0 < filled < asked)) else 0
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO platform_spread_observations (observation_id, source, venue, symbol, side, account_id, "
            "tenant_id, reference_id, budget_id, slot_id, intended_quantity, intended_notional, depth_hash, "
            "depth_observed_at, depth_age_ms, depth_levels, best_bid, best_ask, spread_bps, band_notional, worst_price, "
            "reference_price, fill_price, filled_quantity, partial_fill, realized_slippage_bps, impact_estimate_bps, "
            "impact_model, impact_calibrated, entry_status, recorded_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?) ON CONFLICT (source, reference_id) DO NOTHING",
            (uuid.uuid4().hex, source, depth.venue, depth.symbol, side, account_id, tenant_id, reference_id, budget_id,
             slot_id, decimal_in(qty), decimal_in(want), depth.digest, depth.observed_at_ms, depth_age_ms, len(book),
             decimal_in(best_bid), decimal_in(best_ask), decimal_in(spread_bps),
             decimal_in(cap.band_notional) if cap is not None else None,
             decimal_in(cap.worst_price) if cap is not None else None, decimal_in(reference), decimal_in(fill),
             decimal_in(filled), partial, decimal_in(slippage), decimal_in(impact), IMPACT_MODEL,
             entry_status, conn.now_ms()))
        return cur.rowcount == 1


__all__ = ["PlatformSpreadGate", "SpreadGrant", "SpreadRefused", "read_depth", "signal_id_of", "entry_side",
           "entry_order_style", "expire_unused", "record_observation", "GATE_NOT_AUTHORITATIVE", "BUDGET_UNAVAILABLE",
           "TENANT_UNKNOWN", "DEPTH_UNAVAILABLE", "DEPTH_TIMESTAMP_MISSING", "SLOT_TRANSITION_FAILED", "EXTERNAL_GATE",
           "IMPACT_MODEL", "CLAIM_GRACE_MS", "GATE_VERSION"]
