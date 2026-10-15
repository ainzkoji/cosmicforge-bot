"""Per-account risk brakes of the CATI platform pipeline (Section H, Step 3.10).

Three brakes, evaluated once per risk day at the decision close, with the thresholds of the account's risk
profile (``shared_lib.risk_levels``; no number is defined here):

``paused``  the day's return is at or below minus the daily loss pause: no NEW position at this decision; what is
            held stays manageable. It holds for that decision only.
``halve``   the drawdown from the highest closing equity is at or below minus the reduce threshold: targets are
            halved for as long as that holds.
``halted``  the drawdown is at or below minus the stop threshold (or equity is exhausted): every position is
            closed and nothing is opened. Terminal: later closes do not lift it, whatever the equity does.

The arithmetic is the frozen daily-trend evaluator's, statement for statement (``research/evaluator/simulator``,
"mark to the close; equity and brakes"), so a shadow account driven through ``evaluate`` reports the same flags
as the research run it is compared with::

    daily_return = closing / opening - 1            (opening = the previous closing equity)
    highest      = max(highest, closing)
    drawdown     = closing / highest - 1
    paused       = daily_return <= -daily_pause
    halve        = drawdown     <= -halve_drawdown
    halted       = halted or drawdown <= -stop_drawdown or closing <= 0

Comparisons are ``<=``: a brake fires AT its threshold. There is no switch that turns a brake off; the research
comparison that runs without the drawdown brakes has no counterpart in this module.

**State** is one row per (account, scope) in ``cati_brake_state``; a scope is the bot or family whose equity the
caller feeds (``ACCOUNT_SCOPE`` for the whole account). Every evaluation and every change of a brake appends to
``cati_brake_events``. Each call is one ``BEGIN IMMEDIATE`` transaction, so accounts evaluated concurrently, or
one account evaluated twice, cannot interleave: the second evaluation of a risk day returns the first one's
result and writes nothing.

**The risk day** is an explicit argument, never derived from the wall clock here. Two definitions are in use
and both are supported (``risk_day_closed_at`` / ``risk_day_containing``): the daily trend family decides at the
UTC daily close (``UTC_DAY``); the existing production path counts its day in ``ADAPTIVE_DAILY_RISK_TIMEZONE``.
A state keeps the definition it was opened with; mixing two would corrupt the daily return.

**External capital flows** are not trading results. ``net_transfers`` (deposits minus withdrawals since the
previous evaluated close) shifts the opening equity and the highest closing equity by the same amount, as the
existing account-period rule does (``production_execution.account_periods``), so a deposit is not a gain and a
withdrawal is not a loss. With no flows the arithmetic is exactly the evaluator's.

**A terminal stop** is a persisted column: it survives a restart of the process, and the database refuses to
clear it without a restart record made for that stop. ``authorize_restart`` writes that record with the
identity of the user or operator. The approved policy does not say what a restart does to the highest closing
equity, so the stricter reading is implemented and the other is left to the owner: the high-water mark is KEPT,
and the authorization only lets the next decision close test the stop threshold again. If the drawdown is then
still at or beyond the threshold the account stays stopped (the authorization is used up); otherwise the stop
is lifted and the halve brake applies as the preserved drawdown dictates. The stop is never lifted between
closes, so no decision can run on a half-cleared state.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from app.trading_intelligence.hashing import short_id
from shared_lib.persistence.cati_portfolio_schema import BRAKE_EVENT_TABLE, BRAKE_RESTART_TABLE, BRAKE_STATE_TABLE
from shared_lib.risk_levels import get_profile

NONE, DAILY_LOSS_PAUSE, DRAWDOWN_HALVE, DRAWDOWN_STOP, EQUITY_EXHAUSTED = (
    "NONE", "DAILY_LOSS_PAUSE", "DRAWDOWN_HALVE", "DRAWDOWN_STOP", "EQUITY_EXHAUSTED")
ACTIVE_BRAKES: Tuple[str, ...] = (NONE, DAILY_LOSS_PAUSE, DRAWDOWN_HALVE, DRAWDOWN_STOP, EQUITY_EXHAUSTED)

#: the UTC daily close (daily trend). Any other day definition is an IANA zone name (the production path's
#: ``ADAPTIVE_DAILY_RISK_TIMEZONE``).
UTC_DAY = "UTC"
#: the scope of a brake that covers the whole account rather than one bot or family
ACCOUNT_SCOPE = "account"
USER, OPERATOR = "USER", "OPERATOR"
RESTART_ROLES: Tuple[str, ...] = (USER, OPERATOR)
#: what a restart does to the highest closing equity (see the module note); the only handling implemented
HIGH_WATER_MARK_PRESERVED = "PRESERVED"

EVENT_OPENED, EVENT_EVALUATED = "STATE_OPENED", "EVALUATED"
EVENT_PAUSE, EVENT_HALVE_ON, EVENT_HALVE_OFF = "DAILY_LOSS_PAUSE", "DRAWDOWN_HALVE_ON", "DRAWDOWN_HALVE_OFF"
EVENT_STOP, EVENT_EXHAUSTED = "DRAWDOWN_STOP_FIRED", "ACCOUNT_EQUITY_EXHAUSTED"
EVENT_RESTART_AUTHORIZED, EVENT_RESTART_APPLIED, EVENT_RESTART_NOT_APPLIED = (
    "RESTART_AUTHORIZED", "RESTART_APPLIED", "RESTART_NOT_APPLIED")

_STATE_COLUMNS: Tuple[str, ...] = (
    "account_id", "scope", "risk_level", "profile_version", "daily_pause", "halve_drawdown", "stop_drawdown",
    "day_definition", "first_day", "risk_day", "opening_equity", "closing_equity", "highest_closing_equity",
    "daily_return", "daily_loss", "drawdown", "paused", "halve", "halted", "active_brake", "reason", "triggered_at",
    "triggered_day", "stop_seq", "restart_id", "restart_pending", "restart_by", "restart_role", "restart_reference",
    "restart_at", "restart_applied_day", "evaluations", "updated_at")


class BrakeError(ValueError):
    """The brake state cannot be evaluated or changed as asked. ``code`` is stable and machine-readable."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


@dataclass(frozen=True)
class BrakeThresholds:
    """The three thresholds of one risk profile, as positive fractions of equity (0.02 = 2 %)."""
    risk_level: str
    profile_version: str
    daily_pause: float
    halve_drawdown: float
    stop_drawdown: float

    def __post_init__(self) -> None:
        for name in ("daily_pause", "halve_drawdown", "stop_drawdown"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise BrakeError("INVALID_THRESHOLDS", f"{name} must be a positive fraction: {value!r}")
        if not self.halve_drawdown < self.stop_drawdown:
            raise BrakeError("INVALID_THRESHOLDS", "the halve threshold lies below the stop threshold")

    @classmethod
    def from_profile(cls, level: Any, version: Optional[str] = None) -> "BrakeThresholds":
        """The thresholds of the shared risk-profile authority; the only source of these numbers."""
        profile = get_profile(level, version)
        return cls(risk_level=profile.level, profile_version=profile.version,
                   daily_pause=float(profile.fraction("daily_loss_pause_pct")),
                   halve_drawdown=float(profile.fraction("drawdown_reduce_pct")),
                   stop_drawdown=float(profile.fraction("drawdown_stop_pct")))


@dataclass(frozen=True)
class BrakeFlags:
    """What a decision may do. The family halves its own targets when ``halve`` is set (it knows them)."""
    paused: bool
    halve: bool
    halted: bool

    @property
    def new_entries_allowed(self) -> bool:
        return not (self.paused or self.halted)

    @property
    def close_all_positions(self) -> bool:
        return self.halted


#: no brake holds (a fresh account before its first close)
NO_BRAKE = BrakeFlags(paused=False, halve=False, halted=False)


def close_measures(*, opening_equity: float, closing_equity: float,
                   highest_closing_equity: float) -> Tuple[float, float, float]:
    """``(daily_return, highest_closing_equity, drawdown)`` at a decision close, as the evaluator computes them."""
    daily_return = closing_equity / opening_equity - 1.0 if opening_equity > 0 else 0.0
    highest = max(highest_closing_equity, closing_equity)
    drawdown = closing_equity / highest - 1.0 if highest > 0 else 0.0
    return daily_return, highest, drawdown


def brake_flags(*, daily_return: float, drawdown: float, closing_equity: float, thresholds: BrakeThresholds,
                halted_before: bool) -> BrakeFlags:
    """The three flags from the measures of one close. ``<=``: a brake fires at its threshold."""
    return BrakeFlags(paused=daily_return <= -thresholds.daily_pause,
                      halve=drawdown <= -thresholds.halve_drawdown,
                      halted=bool(halted_before) or drawdown <= -thresholds.stop_drawdown or closing_equity <= 0)


# --------------------------------------------------------------------------------------------------- risk day
def _zone(day_definition: str) -> Any:
    if not isinstance(day_definition, str) or not day_definition.strip():
        raise BrakeError("DAY_DEFINITION_REQUIRED", "name the day definition: UTC or an IANA zone")
    if day_definition == UTC_DAY:
        return timezone.utc                  # no time-zone database needed for the UTC daily close
    try:
        return ZoneInfo(day_definition)
    except Exception:
        raise BrakeError("DAY_DEFINITION_UNKNOWN", f"{day_definition!r} is not UTC or a known zone") from None


def risk_day_containing(time_ms: int, day_definition: str) -> str:
    """The risk day (ISO date) an instant falls in. The production path's day: ``risk_day_containing(now,
    settings.ADAPTIVE_DAILY_RISK_TIMEZONE)``."""
    moment = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=int(time_ms))
    return moment.astimezone(_zone(day_definition)).date().isoformat()


def risk_day_closed_at(close_ms: int, day_definition: str) -> str:
    """The risk day that ENDS at this instant. The daily trend's day: the UTC daily bar whose close is
    ``close_ms`` (midnight belongs to the day it opens, so the day that just closed is one millisecond earlier)."""
    return risk_day_containing(int(close_ms) - 1, day_definition)


# ------------------------------------------------------------------------------------------------------ state
@dataclass(frozen=True)
class BrakeState:
    """The persisted row of one (account, scope)."""
    account_id: str
    scope: str
    risk_level: str
    profile_version: str
    daily_pause: float
    halve_drawdown: float
    stop_drawdown: float
    day_definition: str
    first_day: str
    risk_day: str
    opening_equity: float
    closing_equity: float
    highest_closing_equity: float
    daily_return: float
    daily_loss: float                   # quote units lost over the risk day, never negative
    drawdown: float
    paused: bool
    halve: bool
    halted: bool
    active_brake: str                   # the most severe brake in force
    reason: Optional[str]
    triggered_at_ms: Optional[int]
    triggered_day: Optional[str]
    stop_seq: Optional[int]             # the event that latched the terminal stop
    restart_id: Optional[str]
    restart_pending: bool
    restart_by: Optional[str]
    restart_role: Optional[str]
    restart_reference: Optional[str]
    restart_at_ms: Optional[int]
    restart_applied_day: Optional[str]
    evaluations: int
    updated_at_ms: int

    @property
    def flags(self) -> BrakeFlags:
        return BrakeFlags(paused=self.paused, halve=self.halve, halted=self.halted)


@dataclass(frozen=True)
class BrakeDecision:
    """The result of one evaluation. ``replayed``: this risk day had been evaluated; nothing was written."""
    state: BrakeState
    stop_fired: bool                    # the terminal stop latched at THIS close
    restart_applied: Optional[bool]     # None: no restart was pending; False: it was, and the stop still holds
    replayed: bool

    @property
    def flags(self) -> BrakeFlags:
        return self.state.flags

    @property
    def paused(self) -> bool:
        return self.state.paused

    @property
    def halve(self) -> bool:
        return self.state.halve

    @property
    def halted(self) -> bool:
        return self.state.halted

    @property
    def close_all_positions(self) -> bool:
        return self.state.halted

    @property
    def new_entries_allowed(self) -> bool:
        return self.flags.new_entries_allowed


@dataclass(frozen=True)
class RestartRecord:
    restart_id: str
    account_id: str
    scope: str
    stop_seq: int
    authorized_by: str
    role: str
    reference: str
    authorized_at_ms: int
    high_water_mark: str                # HIGH_WATER_MARK_PRESERVED
    replayed: bool


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BrakeError("INVALID_ARGUMENT", f"{name} is required")
    return value


def _number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise BrakeError("INVALID_ARGUMENT", f"{name} is not a finite number: {value!r}")
    return float(value)


def _day(value: Any) -> str:
    try:
        return date.fromisoformat(_text("risk_day", value)).isoformat()
    except ValueError as exc:
        if isinstance(exc, BrakeError):
            raise
        raise BrakeError("INVALID_ARGUMENT", f"risk_day {value!r} is not an ISO date") from None


def _state(row: Sequence[Any]) -> BrakeState:
    v = dict(zip(_STATE_COLUMNS, row))
    return BrakeState(
        account_id=v["account_id"], scope=v["scope"], risk_level=v["risk_level"], profile_version=v["profile_version"],
        daily_pause=float(v["daily_pause"]), halve_drawdown=float(v["halve_drawdown"]),
        stop_drawdown=float(v["stop_drawdown"]), day_definition=v["day_definition"], first_day=v["first_day"],
        risk_day=v["risk_day"], opening_equity=float(v["opening_equity"]), closing_equity=float(v["closing_equity"]),
        highest_closing_equity=float(v["highest_closing_equity"]), daily_return=float(v["daily_return"]),
        daily_loss=float(v["daily_loss"]), drawdown=float(v["drawdown"]), paused=bool(v["paused"]),
        halve=bool(v["halve"]), halted=bool(v["halted"]), active_brake=v["active_brake"], reason=v["reason"],
        triggered_at_ms=v["triggered_at"], triggered_day=v["triggered_day"], stop_seq=v["stop_seq"],
        restart_id=v["restart_id"], restart_pending=bool(v["restart_pending"]), restart_by=v["restart_by"],
        restart_role=v["restart_role"], restart_reference=v["restart_reference"], restart_at_ms=v["restart_at"],
        restart_applied_day=v["restart_applied_day"], evaluations=int(v["evaluations"]), updated_at_ms=int(v["updated_at"]))


def _load(conn: Any, account_id: str, scope: str) -> Optional[BrakeState]:
    row = conn.execute(f"SELECT {', '.join(_STATE_COLUMNS)} FROM {BRAKE_STATE_TABLE} WHERE account_id=? AND scope=?",
                       (account_id, scope)).fetchone()
    return _state(tuple(row)) if row is not None else None


def _event(conn: Any, account_id: str, scope: str, event_type: str, risk_day: Optional[str], now_ms: int, actor: str,
           payload: Dict[str, Any]) -> int:
    cursor = conn.execute(
        f"INSERT INTO {BRAKE_EVENT_TABLE} (account_id, scope, event_type, risk_day, recorded_at, actor, payload)"
        " VALUES (?,?,?,?,?,?,?)",
        (account_id, scope, event_type, risk_day, int(now_ms), actor, json.dumps(payload, sort_keys=True)))
    return int(cursor.lastrowid)


def _reason(code: str, measure: str, value: float, threshold: float) -> str:
    return f"{code}|{measure}={value!r}|threshold={-threshold!r}"


# --------------------------------------------------------------------------------------------------- evaluate
def evaluate(db: Any, *, account_id: str, scope: str, risk_day: str, day_definition: str, closing_equity: float,
             thresholds: BrakeThresholds, now_ms: int, initial_equity: Optional[float] = None,
             net_transfers: float = 0.0, actor: str = "pipeline") -> BrakeDecision:
    """Evaluate the brakes of one (account, scope) at the decision close of ``risk_day`` and persist the result.

    ``closing_equity`` is the equity of the scope marked to market at that close (wallet balance plus unrealised
    result). ``initial_equity`` is required the first time only: the equity the first risk day opened with,
    which is also the first high-water mark. ``net_transfers``: see the module note.

    Raises ``BrakeError`` for a first evaluation without a basis (``BRAKE_BASIS_REQUIRED``), a day earlier than
    the last one (``RISK_DAY_OUT_OF_ORDER``), a second evaluation of a day with a different equity
    (``RISK_DAY_ALREADY_EVALUATED``) and a changed day definition (``DAY_DEFINITION_CHANGED``).
    """
    account_id, scope, actor = _text("account_id", account_id), _text("scope", scope), _text("actor", actor)
    day = _day(risk_day)
    _zone(day_definition)
    closing = _number("closing_equity", closing_equity)
    flows = _number("net_transfers", net_transfers)
    if not isinstance(thresholds, BrakeThresholds):
        raise BrakeError("INVALID_ARGUMENT", "thresholds come from BrakeThresholds.from_profile")
    now_ms = int(now_ms)
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        before = _load(conn, account_id, scope)
        if before is None:
            if initial_equity is None:
                raise BrakeError("BRAKE_BASIS_REQUIRED", "the first evaluation states the equity the day opened with")
            basis = _number("initial_equity", initial_equity)
            if basis <= 0:
                raise BrakeError("BRAKE_BASIS_REQUIRED", "the opening equity of an account is positive")
            opening = highest_before = basis + flows
            halted_before = halve_before = pending = False
        else:
            if before.day_definition != day_definition:
                raise BrakeError("DAY_DEFINITION_CHANGED", f"this state counts its day in {before.day_definition}")
            if day == before.risk_day:
                if closing != before.closing_equity:
                    raise BrakeError("RISK_DAY_ALREADY_EVALUATED",
                                     f"{day} closed at {before.closing_equity!r}; a close is evaluated once")
                return BrakeDecision(state=before, stop_fired=False, restart_applied=None, replayed=True)
            if day < before.risk_day:
                raise BrakeError("RISK_DAY_OUT_OF_ORDER", f"{day} precedes the last evaluated day {before.risk_day}")
            opening = before.closing_equity + flows
            highest_before = before.highest_closing_equity + flows
            halted_before, halve_before, pending = before.halted, before.halve, before.restart_pending

        daily_return, highest, drawdown = close_measures(opening_equity=opening, closing_equity=closing,
                                                         highest_closing_equity=highest_before)
        # a pending restart authorization lets this close test the stop again; nothing else can lift it
        flags = brake_flags(daily_return=daily_return, drawdown=drawdown, closing_equity=closing,
                            thresholds=thresholds, halted_before=halted_before and not pending)
        measures = {"risk_day": day, "opening_equity": opening, "closing_equity": closing,
                    "highest_closing_equity": highest, "daily_return": daily_return, "drawdown": drawdown,
                    "net_transfers": flows, "paused": flags.paused, "halve": flags.halve, "halted": flags.halted,
                    "daily_pause": thresholds.daily_pause, "halve_drawdown": thresholds.halve_drawdown,
                    "stop_drawdown": thresholds.stop_drawdown, "risk_level": thresholds.risk_level,
                    "profile_version": thresholds.profile_version}
        if before is None:
            _event(conn, account_id, scope, EVENT_OPENED, day, now_ms, actor,
                   {"initial_equity": opening, "day_definition": day_definition})
        _event(conn, account_id, scope, EVENT_EVALUATED, day, now_ms, actor, measures)
        if flags.paused:
            _event(conn, account_id, scope, EVENT_PAUSE, day, now_ms, actor, measures)
        if flags.halve != halve_before:
            _event(conn, account_id, scope, EVENT_HALVE_ON if flags.halve else EVENT_HALVE_OFF, day, now_ms, actor, measures)

        stop_fired, restart_applied = False, None
        stop_seq = before.stop_seq if before else None
        if flags.halted and not halted_before:
            stop_fired = True
            by_drawdown = drawdown <= -thresholds.stop_drawdown
            active = DRAWDOWN_STOP if by_drawdown else EQUITY_EXHAUSTED
            reason = (_reason(active, "drawdown", drawdown, thresholds.stop_drawdown) if by_drawdown
                      else f"{active}|closing_equity={closing!r}")
            stop_seq = _event(conn, account_id, scope, EVENT_STOP if by_drawdown else EVENT_EXHAUSTED, day, now_ms,
                              actor, measures)
            triggered_at, triggered_day = now_ms, day
        elif flags.halted:
            active, reason = before.active_brake, before.reason
            triggered_at, triggered_day = before.triggered_at_ms, before.triggered_day
            if pending:
                restart_applied = False
                _event(conn, account_id, scope, EVENT_RESTART_NOT_APPLIED, day, now_ms, actor,
                       dict(measures, restart_id=before.restart_id, why="DRAWDOWN_STILL_AT_OR_BEYOND_STOP"))
        else:
            if pending:
                restart_applied = True
                _event(conn, account_id, scope, EVENT_RESTART_APPLIED, day, now_ms, actor,
                       dict(measures, restart_id=before.restart_id, high_water_mark=HIGH_WATER_MARK_PRESERVED))
            active = DRAWDOWN_HALVE if flags.halve else DAILY_LOSS_PAUSE if flags.paused else NONE
            held_since = (before is not None and not halted_before and active == DRAWDOWN_HALVE
                          and before.active_brake == DRAWDOWN_HALVE)
            if active == NONE:
                reason = triggered_at = triggered_day = None
            elif held_since:
                reason, triggered_at, triggered_day = before.reason, before.triggered_at_ms, before.triggered_day
            else:
                reason = (_reason(active, "drawdown", drawdown, thresholds.halve_drawdown) if active == DRAWDOWN_HALVE
                          else _reason(active, "daily_return", daily_return, thresholds.daily_pause))
                triggered_at, triggered_day = now_ms, day

        applied_day = day if restart_applied else (before.restart_applied_day if before else None)
        values = {
            "risk_level": thresholds.risk_level, "profile_version": thresholds.profile_version,
            "daily_pause": thresholds.daily_pause, "halve_drawdown": thresholds.halve_drawdown,
            "stop_drawdown": thresholds.stop_drawdown, "risk_day": day, "opening_equity": opening,
            "closing_equity": closing, "highest_closing_equity": highest, "daily_return": daily_return,
            "daily_loss": max(0.0, opening - closing), "drawdown": drawdown, "paused": int(flags.paused),
            "halve": int(flags.halve), "halted": int(flags.halted), "active_brake": active, "reason": reason,
            "triggered_at": triggered_at, "triggered_day": triggered_day, "stop_seq": stop_seq, "restart_pending": 0,
            "restart_applied_day": applied_day, "evaluations": (before.evaluations if before else 0) + 1,
            "updated_at": now_ms}
        if before is None:
            values.update(account_id=account_id, scope=scope, day_definition=day_definition, first_day=day)
            conn.execute(f"INSERT INTO {BRAKE_STATE_TABLE} ({', '.join(values)}) VALUES ({', '.join('?' * len(values))})",
                         tuple(values.values()))
        else:
            conn.execute(f"UPDATE {BRAKE_STATE_TABLE} SET {', '.join(f'{k}=?' for k in values)} WHERE account_id=? AND scope=?",
                         tuple(values.values()) + (account_id, scope))
        after = _load(conn, account_id, scope)
    return BrakeDecision(state=after, stop_fired=stop_fired, restart_applied=restart_applied, replayed=False)


# ---------------------------------------------------------------------------------------------------- restart
def authorize_restart(db: Any, *, account_id: str, scope: str, authorized_by: str, role: str, reference: str,
                      now_ms: int) -> RestartRecord:
    """Record the explicit restart of a stopped (account, scope). The only way out of a terminal stop.

    ``authorized_by`` is the identity of the user or operator, ``role`` says which, ``reference`` is the request,
    ticket or approval the restart can be traced to. The stop is NOT lifted here: the next decision close tests
    the stop threshold again against the preserved high-water mark (see the module note). Repeating the same
    authorization while it is pending returns the first record.

    Raises ``BrakeError``: ``BRAKE_STATE_NOT_FOUND``, ``NOT_STOPPED``, ``RESTART_ALREADY_PENDING`` (a different
    authorization is waiting), ``INVALID_ARGUMENT`` (no identity, no reference, unknown role).
    """
    account_id, scope = _text("account_id", account_id), _text("scope", scope)
    authorized_by, reference = _text("authorized_by", authorized_by).strip(), _text("reference", reference).strip()
    if role not in RESTART_ROLES:
        raise BrakeError("INVALID_ARGUMENT", f"role {role!r} is not one of {RESTART_ROLES}")
    now_ms = int(now_ms)
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        state = _load(conn, account_id, scope)
        if state is None:
            raise BrakeError("BRAKE_STATE_NOT_FOUND", f"no brake state for {account_id} / {scope}")
        if not state.halted:
            raise BrakeError("NOT_STOPPED", "only a terminal stop takes a restart")
        if state.restart_pending:
            if (state.restart_by, state.restart_role, state.restart_reference) == (authorized_by, role, reference):
                return RestartRecord(state.restart_id, account_id, scope, int(state.stop_seq), authorized_by, role,
                                     reference, int(state.restart_at_ms), HIGH_WATER_MARK_PRESERVED, replayed=True)
            raise BrakeError("RESTART_ALREADY_PENDING", f"restart {state.restart_id} waits for the next decision close")
        stop_seq = int(state.stop_seq)
        restart_id = short_id("rst", {"account": account_id, "scope": scope, "stop": stop_seq, "by": authorized_by,
                                      "role": role, "reference": reference, "at": now_ms})
        document = {"stopped_day": state.triggered_day, "stop_reason": state.reason, "closing_equity": state.closing_equity,
                    "highest_closing_equity": state.highest_closing_equity, "drawdown": state.drawdown,
                    "stop_drawdown": state.stop_drawdown, "high_water_mark": HIGH_WATER_MARK_PRESERVED}
        conn.execute(
            f"INSERT INTO {BRAKE_RESTART_TABLE} (restart_id, account_id, scope, stop_seq, authorized_by, authorized_role,"
            " reference, authorized_at, document) VALUES (?,?,?,?,?,?,?,?,?)",
            (restart_id, account_id, scope, stop_seq, authorized_by, role, reference, now_ms,
             json.dumps(document, sort_keys=True)))
        conn.execute(
            f"UPDATE {BRAKE_STATE_TABLE} SET restart_id=?, restart_pending=1, restart_by=?, restart_role=?,"
            " restart_reference=?, restart_at=?, updated_at=? WHERE account_id=? AND scope=? AND halted=1",
            (restart_id, authorized_by, role, reference, now_ms, now_ms, account_id, scope))
        _event(conn, account_id, scope, EVENT_RESTART_AUTHORIZED, state.risk_day, now_ms, authorized_by,
               dict(document, restart_id=restart_id, role=role, reference=reference))
    return RestartRecord(restart_id, account_id, scope, stop_seq, authorized_by, role, reference, now_ms,
                         HIGH_WATER_MARK_PRESERVED, replayed=False)


# ------------------------------------------------------------------------------------------------------ reads
def current(db: Any, account_id: str, scope: str) -> Optional[BrakeState]:
    """The persisted state, or None before the first evaluation. This is what a restarted process reads."""
    with db.connect() as conn:
        return _load(conn, account_id, scope)


def stopped_scopes(conn: Any, account_id: str, scopes: Optional[Sequence[str]] = None) -> Tuple[str, ...]:
    """The scopes of an account whose terminal stop is latched, read on the caller's connection so a
    reservation can check it inside its own transaction. ``scopes`` limits the answer to those named."""
    rows = conn.execute(f"SELECT scope FROM {BRAKE_STATE_TABLE} WHERE account_id=? AND halted=1 ORDER BY scope",
                        (account_id,)).fetchall()
    found = tuple(r[0] for r in rows)
    return found if scopes is None else tuple(s for s in found if s in set(scopes))


def stopped(db: Any) -> List[Tuple[str, str]]:
    """Every stopped (account, scope): the list a restarted process must keep flat."""
    with db.connect() as conn:
        rows = conn.execute(f"SELECT account_id, scope FROM {BRAKE_STATE_TABLE} WHERE halted=1"
                            " ORDER BY account_id, scope").fetchall()
    return [(r[0], r[1]) for r in rows]


def events(db: Any, account_id: str, scope: Optional[str] = None) -> List[Dict[str, Any]]:
    """The append-only brake log of an account, oldest first."""
    sql = f"SELECT seq, scope, event_type, risk_day, recorded_at, actor, payload FROM {BRAKE_EVENT_TABLE} WHERE account_id=?"
    params: List[Any] = [account_id]
    if scope is not None:
        sql += " AND scope=?"
        params.append(scope)
    with db.connect() as conn:
        rows = conn.execute(sql + " ORDER BY seq", params).fetchall()
    return [{"seq": r[0], "scope": r[1], "event_type": r[2], "risk_day": r[3], "recorded_at": r[4], "actor": r[5],
             "payload": json.loads(r[6])} for r in rows]


__all__ = ["NONE", "DAILY_LOSS_PAUSE", "DRAWDOWN_HALVE", "DRAWDOWN_STOP", "EQUITY_EXHAUSTED", "ACTIVE_BRAKES",
           "UTC_DAY", "ACCOUNT_SCOPE", "USER", "OPERATOR", "RESTART_ROLES", "HIGH_WATER_MARK_PRESERVED", "NO_BRAKE",
           "EVENT_OPENED", "EVENT_EVALUATED", "EVENT_PAUSE", "EVENT_HALVE_ON", "EVENT_HALVE_OFF", "EVENT_STOP",
           "EVENT_EXHAUSTED", "EVENT_RESTART_AUTHORIZED", "EVENT_RESTART_APPLIED", "EVENT_RESTART_NOT_APPLIED",
           "BrakeError", "BrakeThresholds", "BrakeFlags", "BrakeState", "BrakeDecision", "RestartRecord",
           "close_measures", "brake_flags", "risk_day_containing", "risk_day_closed_at", "evaluate",
           "authorize_restart", "current", "stopped_scopes", "stopped", "events"]
