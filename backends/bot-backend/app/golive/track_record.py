"""Publishable track record: daily recorder, return methodology, export and publication (Section H, 6.4 / 8.6).

What exists today
-----------------
No live trading has happened and no live record exists. Every strategy family is uncertified (M0). This
module records what will happen and refuses to invent anything: a ``LIVE`` row needs a go-live authorization
reference and a configured first-authorized-live date (``TRACK_RECORD_FIRST_AUTHORIZED_LIVE_DATE``); with none
configured every ``LIVE`` row is refused with ``LIVE_NOT_AUTHORIZED_YET``. Nothing here is audited.

Recorder (``TrackRecordRecorder.record_day``)
---------------------------------------------
One row per account, venue, family and trading date, in ``track_record_daily`` (schema:
``shared_lib.persistence.sql.track_record_schema``). Rows are **append-only**: a correction is a new row
whose ``supersedes_record_id`` names the row it replaces (same series, same date, an amendment note
required); there is no update or delete path in code (``update_record``/``delete_record`` raise) and the
database refuses ``UPDATE``/``DELETE`` with a trigger.

The account is identified only by ``account_ref`` = keyed HMAC-SHA256 of the account id with the secret in
``TRACK_RECORD_PSEUDONYM_KEY``; the recorder refuses to record without that key. The account id itself is
never stored.

A *series* is ``source_kind + family_id + venue + account_ref``. Every row carries ``prev_hash`` (the previous
row's ``row_hash`` in the same series, or 64 zeros) and ``row_hash`` = SHA-256 of the canonical JSON of all
of the row's stored fields including ``prev_hash``; ``verify_chain`` recomputes the chain from the database.

Methodology (``daily_return``, ``compute_metrics``) -- version ``TR-METHOD-1.0``
-----------------------------------------------------------------------------
See ``daily_return`` for the formula and the flow convention, and ``docs/launch/TRACK_RECORD_METHODOLOGY.md``.

Export and publication
----------------------
``export_series`` builds the public JSON document for one ``source_kind`` (kinds are never mixed), one family
and optionally one venue/version, from publishable, non-superseded rows only. It contains returns, ratios,
counts and minutes -- never an equity amount, an account reference, a user id or a credential -- and groups
of fewer than ``min_accounts`` accounts are suppressed (``SUPPRESSED_MIN_COUNT``). ``publish_series`` stores
the document in ``track_record_publications``, which is the only table the public API reads.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from shared_lib.persistence.sql import POSTGRESQL, Connection, Database, decimal_in, decimal_out
from shared_lib.persistence.sql.track_record_schema import (ACCOUNT_CLASSES, RECONCILIATION_STATUSES,
                                                            SIMULATED_SOURCE_KINDS, SOURCE_KINDS)

METHODOLOGY_VERSION = "TR-METHOD-1.0"
SIMULATED_LABEL = "Simulated — not achieved trading results"
LIVE_LABEL = "Live trading results — unaudited"
LIVE_NOT_AVAILABLE_REASON = "No authorized live trading has occurred"
LIVE_NOT_PUBLISHED_REASON = "Live results are recorded but none has been published yet"
SUPPRESSED_MIN_COUNT = "SUPPRESSED_MIN_COUNT"
DEFAULT_MIN_ACCOUNTS = 5
#: Customer and invited-beta groups are never published below this many accounts, whatever is configured.
PROTECTED_CLASS_MIN_ACCOUNTS = 5
PROTECTED_CLASSES = ("CUSTOMER", "BETA_INVITED")

PSEUDONYM_KEY_ENV = "TRACK_RECORD_PSEUDONYM_KEY"
FIRST_LIVE_DATE_ENV = "TRACK_RECORD_FIRST_AUTHORIZED_LIVE_DATE"
MIN_PSEUDONYM_KEY_LENGTH = 32
GENESIS_HASH = "0" * 64
MONEY_MAX_DECIMALS = 18          # NUMERIC(38,18): more places would not survive the round trip
_OUT_PLACES = Decimal("1e-10")   # published ratios are rounded to 10 decimal places

FLOW_CONVENTION = "START_OF_DAY"
RETURN_FORMULA = "r_t = (E_end - E_start - (D - W)) / (E_start + (D - W))"

LIMITATIONS = (
    "Results are not audited or independently verified.",
    "Past performance does not predict future results; losses can exceed those shown.",
    "Returns are time-weighted and chain-linked from daily figures; deposits and withdrawals are treated as "
    "occurring at the start of the trading day, which can misstate a day's return when a large flow arrived "
    "later in that day.",
    "Aggregates pool the capital of the accounts in a group (asset-weighted); an individual account's result "
    "can differ materially from the aggregate.",
    "Groups and days with fewer accounts than the published minimum are suppressed, so aggregates may omit "
    "some activity.",
    "Fees and funding are as reported by the venue and are already reflected in ending equity; slippage is "
    "shown only for days on which it could be measured.",
    "Platform outages and strategy suspensions are reported, not adjusted for.",
)
NON_LIVE_LIMITATION = ("This series is not live trading. Simulated, paper and demo results do not reflect real "
                       "execution, liquidity, fees or the psychological effects of trading real money.")

#: Keys that must never appear in a public document (checked recursively, case-insensitive).
FORBIDDEN_PUBLIC_KEYS = frozenset({
    "account_ref", "account_id", "accountid", "user_id", "userid", "tenant_id", "email", "api_key", "apikey",
    "api_secret", "secret", "token", "access_token", "password", "credential", "credentials", "series_key",
    "evidence_ref", "authorization_ref", "record_id", "supersedes_record_id", "starting_equity",
    "ending_equity", "deposits", "withdrawals",
})
_ACCOUNT_REF_RE = re.compile(r"acct_[0-9a-f]{16,}")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class TrackRecordError(ValueError):
    """A refused recorder, export or publication request. ``code`` is the stable reason."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


class AppendOnlyViolation(TrackRecordError):
    def __init__(self, detail: str = "corrections are new rows that supersede; rows are never changed") -> None:
        super().__init__("TRACK_RECORD_APPEND_ONLY", detail)


# ── values ──────────────────────────────────────────────────────────────────────────────────────────────────

def _money(name: str, value: Any, *, nullable: bool = False, non_negative: bool = False) -> Optional[Decimal]:
    if value is None:
        if nullable:
            return None
        raise TrackRecordError("FIELD_REQUIRED", name)
    if isinstance(value, bool):
        raise TrackRecordError("MONEY_INVALID", name)
    try:
        text = decimal_in(value)
    except TypeError:
        raise TrackRecordError("MONEY_FLOAT_REJECTED", name) from None
    except Exception:
        raise TrackRecordError("MONEY_INVALID", name) from None
    d = Decimal(text)
    if d.as_tuple().exponent < -MONEY_MAX_DECIMALS:
        raise TrackRecordError("MONEY_PRECISION_EXCEEDED", name)
    if abs(d) >= Decimal(10) ** 20:
        raise TrackRecordError("MONEY_OUT_OF_RANGE", name)
    if non_negative and d < 0:
        raise TrackRecordError("MONEY_NEGATIVE", name)
    return d


def _canon_decimal(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _iso_date(value: Any, name: str = "trading_date") -> str:
    if isinstance(value, datetime):
        raise TrackRecordError("DATE_INVALID", name)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and _DATE_RE.match(value):
        try:
            return date.fromisoformat(value).isoformat()
        except ValueError:
            pass
    raise TrackRecordError("DATE_INVALID", name)


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TrackRecordError("FIELD_REQUIRED", name)
    return value.strip()


def _out(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    with localcontext() as ctx:
        ctx.prec = 50
        q = value.quantize(_OUT_PLACES)
    return "0.0000000000" if q == 0 else format(q, "f")


def _iso_ms(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


# ── pseudonymous account reference ──────────────────────────────────────────────────────────────────────────

def resolve_pseudonym_key(key: Optional[str] = None, env: Optional[Mapping[str, str]] = None) -> str:
    env = os.environ if env is None else env
    key = key if key is not None else env.get(PSEUDONYM_KEY_ENV, "")
    if not key:
        raise TrackRecordError("TRACK_RECORD_PSEUDONYM_KEY_MISSING", f"set {PSEUDONYM_KEY_ENV}")
    if len(key) < MIN_PSEUDONYM_KEY_LENGTH:
        raise TrackRecordError("TRACK_RECORD_PSEUDONYM_KEY_WEAK", f"at least {MIN_PSEUDONYM_KEY_LENGTH} characters")
    return key


def pseudonymize(account_id: str, key: str) -> str:
    """``acct_`` + the first 32 hex digits of HMAC-SHA256(key, account id). Not reversible without the key;
    stable for one key, so one account's days form one chain."""
    account_id = _text("account_id", account_id)
    digest = hmac.new(key.encode("utf-8"), f"cf-track-record-v1:{account_id}".encode("utf-8"), hashlib.sha256)
    return "acct_" + digest.hexdigest()[:32]


# ── the daily row ───────────────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class DailyObservation:
    """One account's trading day as measured. Money is ``Decimal``/``int``/decimal text; floats are refused.

    ``starting_equity`` is equity at the start of the trading day *before* that day's deposits and
    withdrawals; ``ending_equity`` is equity at the end of the day after all flows, fees and funding.
    ``funding`` is signed (positive = received). ``drawdown`` is the account's own drawdown from its peak as
    a fraction in [0, 1]. ``slippage`` is a signed cost in account currency, or ``None`` when not measurable.
    """
    trading_date: Any
    venue: str
    family_id: str
    family_version: str
    account_class: str
    risk_level: str
    source_kind: str
    starting_equity: Any
    ending_equity: Any
    evidence_ref: str
    deposits: Any = 0
    withdrawals: Any = 0
    fees: Any = 0
    funding: Any = 0
    slippage: Any = None
    realized_pnl: Any = 0
    unrealized_pnl: Any = 0
    drawdown: Any = 0
    gross_exposure: Any = 0
    reconciliation_status: str = "PENDING"
    outage_minutes: int = 0
    strategy_suspended: bool = False
    amendment_note: Optional[str] = None


_MONEY_FIELDS = ("starting_equity", "ending_equity", "deposits", "withdrawals", "fees", "funding", "slippage",
                 "realized_pnl", "unrealized_pnl", "drawdown", "gross_exposure")
_NON_NEGATIVE = {"starting_equity", "ending_equity", "deposits", "withdrawals", "fees", "gross_exposure", "drawdown"}
_COLUMNS = ("record_id", "series_key", "seq", "trading_date", "venue", "family_id", "family_version",
            "account_class", "risk_level", "source_kind") + _MONEY_FIELDS + (
            "reconciliation_status", "outage_minutes", "strategy_suspended", "amendment_note", "account_ref",
            "evidence_ref", "authorization_ref", "publishable", "supersedes_record_id", "prev_hash", "row_hash",
            "recorded_at")


def series_key(source_kind: str, family_id: str, venue: str, account_ref: str) -> str:
    return f"{source_kind}|{family_id}|{venue}|{account_ref}"


def row_hash(row: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical JSON of every stored field except ``row_hash`` (``prev_hash`` included)."""
    body = {}
    for c in _COLUMNS:
        if c == "row_hash":
            continue
        v = row.get(c)
        if c in _MONEY_FIELDS:
            v = _canon_decimal(decimal_out(v))
        elif c in ("seq", "outage_minutes", "strategy_suspended", "publishable", "recorded_at"):
            v = int(v)
        body[c] = v
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RecordedRow:
    record_id: str
    series_key: str
    seq: int
    row_hash: str
    prev_hash: str
    publishable: bool
    supersedes_record_id: Optional[str]


def _lock_series(conn: Connection, key: str) -> None:
    if conn.dialect == POSTGRESQL:
        n = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big", signed=True)
        conn.execute("SELECT pg_advisory_xact_lock(?)", (n,))
    # SQLite: the write transaction (BEGIN IMMEDIATE) already excludes every other writer.


_ACTIVE = ("NOT EXISTS (SELECT 1 FROM track_record_daily c WHERE c.supersedes_record_id = d.record_id)")


class TrackRecordRecorder:
    """Appends daily rows. There is no update or delete operation; see the module docstring."""

    def __init__(self, db: Database, *, pseudonym_key: Optional[str] = None,
                 first_authorized_live_date: Any = None, env: Optional[Mapping[str, str]] = None) -> None:
        self.db = db
        self._env = os.environ if env is None else env
        self._key = pseudonym_key
        raw = first_authorized_live_date if first_authorized_live_date is not None else (
            self._env.get(FIRST_LIVE_DATE_ENV) or None)
        self.first_authorized_live_date = _iso_date(raw, "first_authorized_live_date") if raw else None

    def account_ref(self, account_id: str) -> str:
        return pseudonymize(account_id, resolve_pseudonym_key(self._key, self._env))

    # The two methods below exist so that a caller looking for an edit path finds the reason instead.
    def update_record(self, *_a, **_kw):
        raise AppendOnlyViolation()

    def delete_record(self, *_a, **_kw):
        raise AppendOnlyViolation()

    def correct_day(self, supersedes_record_id: str, obs: DailyObservation, *, account_id: str,
                    authorization_ref: Optional[str] = None, publishable: bool = False) -> RecordedRow:
        """Record a correction: a new row superseding ``supersedes_record_id`` (``obs.amendment_note`` required)."""
        return self.record_day(obs, account_id=account_id, authorization_ref=authorization_ref,
                               publishable=publishable, supersedes_record_id=supersedes_record_id)

    def record_day(self, obs: DailyObservation, *, account_id: str, authorization_ref: Optional[str] = None,
                   publishable: bool = False, supersedes_record_id: Optional[str] = None) -> RecordedRow:
        key = resolve_pseudonym_key(self._key, self._env)       # refuse before anything else
        ref = pseudonymize(account_id, key)
        row = self._validate(obs, authorization_ref=authorization_ref, publishable=publishable,
                             supersedes_record_id=supersedes_record_id)
        row["account_ref"] = ref
        row["series_key"] = series_key(row["source_kind"], row["family_id"], row["venue"], ref)
        with self.db.transaction() as conn:
            now = conn.now_ms()
            today = datetime.fromtimestamp(now / 1000, tz=timezone.utc).date().isoformat()
            if row["trading_date"] > today:
                raise TrackRecordError("TRADING_DATE_IN_FUTURE", row["trading_date"])
            _lock_series(conn, row["series_key"])
            active = conn.all(f"SELECT record_id FROM track_record_daily d WHERE series_key=? AND trading_date=? "
                              f"AND {_ACTIVE}", (row["series_key"], row["trading_date"]))
            if supersedes_record_id is None:
                if active:
                    raise TrackRecordError("DUPLICATE_TRADING_DATE",
                                           "a row exists for this series and date; record a correction instead")
            else:
                target = conn.one("SELECT series_key, trading_date FROM track_record_daily WHERE record_id=?",
                                  (supersedes_record_id,))
                if target is None:
                    raise TrackRecordError("CORRECTION_TARGET_NOT_FOUND", supersedes_record_id)
                if target[0] != row["series_key"] or target[1] != row["trading_date"]:
                    raise TrackRecordError("CORRECTION_SERIES_MISMATCH",
                                           "a correction replaces a row of the same series and trading date")
                if supersedes_record_id not in {a[0] for a in active}:
                    raise TrackRecordError("CORRECTION_TARGET_ALREADY_SUPERSEDED", supersedes_record_id)
            head = conn.one("SELECT seq, row_hash FROM track_record_daily WHERE series_key=? ORDER BY seq DESC LIMIT 1",
                            (row["series_key"],))
            row["seq"] = (head[0] + 1) if head else 1
            row["prev_hash"] = head[1] if head else GENESIS_HASH
            row["record_id"] = f"trd_{uuid.uuid4().hex}"
            row["recorded_at"] = now
            row["row_hash"] = row_hash(row)
            params = [decimal_in(row[c]) if c in _MONEY_FIELDS else row[c] for c in _COLUMNS]
            conn.execute(f"INSERT INTO track_record_daily ({', '.join(_COLUMNS)}) VALUES "
                         f"({', '.join('?' for _ in _COLUMNS)})", params)
        return RecordedRow(row["record_id"], row["series_key"], row["seq"], row["row_hash"], row["prev_hash"],
                           bool(row["publishable"]), supersedes_record_id)

    def _validate(self, obs: DailyObservation, *, authorization_ref, publishable, supersedes_record_id) -> Dict[str, Any]:
        if not isinstance(obs, DailyObservation):
            raise TrackRecordError("OBSERVATION_INVALID")
        kind = obs.source_kind
        if kind not in SOURCE_KINDS:
            raise TrackRecordError("SOURCE_KIND_INVALID", str(kind))
        if obs.account_class not in ACCOUNT_CLASSES:
            raise TrackRecordError("ACCOUNT_CLASS_INVALID", str(obs.account_class))
        if (kind in SIMULATED_SOURCE_KINDS) != (obs.account_class == "SIMULATION"):
            raise TrackRecordError("ACCOUNT_CLASS_KIND_MISMATCH",
                                   "BACKTEST and RESEARCH_HOLDOUT rows (and only they) use account class SIMULATION")
        if obs.reconciliation_status not in RECONCILIATION_STATUSES:
            raise TrackRecordError("RECONCILIATION_STATUS_INVALID", str(obs.reconciliation_status))
        row: Dict[str, Any] = {
            "trading_date": _iso_date(obs.trading_date),
            "venue": _text("venue", obs.venue), "family_id": _text("family_id", obs.family_id),
            "family_version": _text("family_version", obs.family_version), "account_class": obs.account_class,
            "risk_level": _text("risk_level", obs.risk_level), "source_kind": kind,
            "reconciliation_status": obs.reconciliation_status,
            "evidence_ref": _text("evidence_ref", obs.evidence_ref),
        }
        for f in _MONEY_FIELDS:
            row[f] = _money(f, getattr(obs, f), nullable=(f == "slippage"), non_negative=(f in _NON_NEGATIVE))
        if row["drawdown"] > 1:
            raise TrackRecordError("DRAWDOWN_OUT_OF_RANGE", "a fraction in [0, 1]")
        if isinstance(obs.outage_minutes, bool) or not isinstance(obs.outage_minutes, int) \
                or not 0 <= obs.outage_minutes <= 1440:
            raise TrackRecordError("OUTAGE_MINUTES_INVALID")
        row["outage_minutes"] = obs.outage_minutes
        if not isinstance(obs.strategy_suspended, bool):
            raise TrackRecordError("STRATEGY_SUSPENDED_INVALID")
        row["strategy_suspended"] = int(obs.strategy_suspended)
        note = obs.amendment_note.strip() if isinstance(obs.amendment_note, str) and obs.amendment_note.strip() else None
        if supersedes_record_id is not None and note is None:
            raise TrackRecordError("CORRECTION_REQUIRES_AMENDMENT_NOTE")
        row["amendment_note"] = note
        row["supersedes_record_id"] = supersedes_record_id
        if not isinstance(publishable, bool):
            raise TrackRecordError("PUBLISHABLE_INVALID")
        auth = authorization_ref.strip() if isinstance(authorization_ref, str) and authorization_ref.strip() else None
        if kind == "LIVE":
            if self.first_authorized_live_date is None:
                raise TrackRecordError("LIVE_NOT_AUTHORIZED_YET",
                                       f"no first authorized live date is configured ({FIRST_LIVE_DATE_ENV})")
            if row["trading_date"] < self.first_authorized_live_date:
                raise TrackRecordError("LIVE_BEFORE_FIRST_AUTHORIZED_DATE", row["trading_date"])
            if auth is None:
                raise TrackRecordError("LIVE_AUTHORIZATION_REF_REQUIRED", "the go-live approval id")
            if publishable and obs.reconciliation_status != "RECONCILED":
                raise TrackRecordError("LIVE_PUBLISH_REQUIRES_RECONCILED", obs.reconciliation_status)
        if publishable and obs.reconciliation_status == "MISMATCH":
            raise TrackRecordError("PUBLISH_REFUSED_MISMATCH")
        row["authorization_ref"] = auth
        row["publishable"] = int(publishable)
        return row


def verify_chain(db: Database, series: Optional[str] = None) -> Dict[str, Any]:
    """Recompute every row hash and link. ``{ok, chains, rows, failures: [{series_key, seq, reason}]}``."""
    sql = f"SELECT {', '.join(_COLUMNS)} FROM track_record_daily"
    params: Tuple[Any, ...] = ()
    if series is not None:
        sql += " WHERE series_key=?"
        params = (series,)
    with db.transaction(write=False) as conn:
        rows = [dict(zip(_COLUMNS, r)) for r in conn.all(sql + " ORDER BY series_key, seq", params)]
    failures: List[Dict[str, Any]] = []
    prev: Dict[str, Tuple[int, str]] = {}
    for r in rows:
        k = r["series_key"]
        exp_seq, exp_prev = (prev[k][0] + 1, prev[k][1]) if k in prev else (1, GENESIS_HASH)
        if r["seq"] != exp_seq:
            failures.append({"series_key": k, "seq": r["seq"], "reason": "SEQUENCE_GAP"})
        if r["prev_hash"] != exp_prev:
            failures.append({"series_key": k, "seq": r["seq"], "reason": "PREV_HASH_MISMATCH"})
        if row_hash(r) != r["row_hash"]:
            failures.append({"series_key": k, "seq": r["seq"], "reason": "ROW_HASH_MISMATCH"})
        prev[k] = (r["seq"], r["row_hash"])
    return {"ok": not failures, "chains": len(prev), "rows": len(rows), "failures": failures}


# ── methodology ─────────────────────────────────────────────────────────────────────────────────────────────

def daily_return(starting_equity: Decimal, ending_equity: Decimal, deposits: Decimal,
                 withdrawals: Decimal) -> Optional[Decimal]:
    """The day's time-weighted return, net of fees and funding:

        r_t = (E_end - E_start - (D - W)) / (E_start + D_weight),   D_weight = (D - W) * w,  w = 1

    **Flow convention: start of day (w = 1).** Deposits ``D`` and withdrawals ``W`` are treated as arriving
    before the day's trading. Justification:

    * the engine sizes from the equity it sees at the start of its cycle, so money deposited during a day is
      in practice available to (and at risk in) that day's trading, and money withdrawn is not;
    * the venue's daily statements do not reliably time-stamp flows, so a time-weighted ``w`` (modified
      Dietz) would rest on timestamps we cannot verify; a fixed, documented convention is reproducible;
    * it is conservative for deposits: the denominator includes the deposit, so a deposit can never raise
      the measured return, and a gain on a deposit day is spread over the larger capital.

    The flow itself is removed from the numerator, so a deposit or withdrawal alone is a 0 % return.
    ``None`` when the day's capital ``E_start + D - W`` is not positive (no capital at risk); such a day is
    excluded from the index and counted.
    """
    with localcontext() as ctx:
        ctx.prec = 50
        net = deposits - withdrawals
        capital = starting_equity + net
        if capital <= 0:
            return None
        return (ending_equity - starting_equity - net) / capital


@dataclass(frozen=True)
class DayFlows:
    trading_date: str
    starting_equity: Decimal
    ending_equity: Decimal
    deposits: Decimal = Decimal(0)
    withdrawals: Decimal = Decimal(0)
    fees: Decimal = Decimal(0)
    funding: Decimal = Decimal(0)
    gross_exposure: Decimal = Decimal(0)
    slippage: Optional[Decimal] = None
    outage_minutes: int = 0
    suspended: bool = False

    @property
    def capital(self) -> Decimal:
        return self.starting_equity + self.deposits - self.withdrawals


@dataclass
class Metrics:
    index: List[Dict[str, Any]] = field(default_factory=list)   # [{date, return, index}]
    twr: Decimal = Decimal(0)
    max_drawdown: Decimal = Decimal(0)
    fee_drag: Decimal = Decimal(0)
    funding_drag: Decimal = Decimal(0)
    slippage_drag: Optional[Decimal] = None
    slippage_measured_days: int = 0
    avg_gross_exposure: Optional[Decimal] = None
    max_gross_exposure: Optional[Decimal] = None
    outage_minutes: int = 0
    outage_days: int = 0
    suspended_days: int = 0
    excluded_days: int = 0
    trading_days: int = 0


def compute_metrics(days: Sequence[DayFlows]) -> Metrics:
    """Chain-link the daily returns into a TWR index starting at 1.

    * ``twr`` = prod(1 + r_t) - 1.
    * ``max_drawdown`` = max over t of 1 - I_t / max(1, I_1..I_t) on the TWR index (not on equity, so flows do
      not create or hide drawdowns).
    * ``fee_drag`` = prod(1 + r_t + F_t / C_t) - prod(1 + r_t): the return the period would have had without
      fees minus the return it had. ``funding_drag`` likewise with ``-funding_t / C_t`` (funding is signed,
      positive received, so a negative drag means funding added to return). ``C_t = E_start + D - W``.
    * ``slippage_drag`` = sum(slippage) / sum(C_t) over the days on which slippage was measured, else ``None``.
    * exposure: mean and max of gross_exposure_t / C_t.
    * outages and suspensions are counted, never adjusted for.
    """
    m = Metrics()
    with localcontext() as ctx:
        ctx.prec = 50
        idx = Decimal(1)
        peak = Decimal(1)
        gross_fees = Decimal(1)
        gross_funding = Decimal(1)
        exposures: List[Decimal] = []
        slip_sum, slip_cap = Decimal(0), Decimal(0)
        for d in sorted(days, key=lambda x: x.trading_date):
            m.trading_days += 1
            m.outage_minutes += int(d.outage_minutes)
            m.outage_days += 1 if d.outage_minutes > 0 else 0
            m.suspended_days += 1 if d.suspended else 0
            r = daily_return(d.starting_equity, d.ending_equity, d.deposits, d.withdrawals)
            if r is None:
                m.excluded_days += 1
                continue
            c = d.capital
            idx *= 1 + r
            gross_fees *= 1 + r + d.fees / c
            gross_funding *= 1 + r - d.funding / c
            peak = max(peak, idx)
            m.max_drawdown = max(m.max_drawdown, 1 - idx / peak)
            exposures.append(d.gross_exposure / c)
            if d.slippage is not None:
                m.slippage_measured_days += 1
                slip_sum += d.slippage
                slip_cap += c
            m.index.append({"date": d.trading_date, "return": r, "index": idx})
        m.twr = idx - 1
        m.fee_drag = gross_fees - idx
        m.funding_drag = gross_funding - idx
        m.slippage_drag = (slip_sum / slip_cap) if slip_cap > 0 else None
        if exposures:
            m.avg_gross_exposure = sum(exposures) / len(exposures)
            m.max_gross_exposure = max(exposures)
    return m


def pool_day(trading_date: str, days: Sequence[DayFlows]) -> DayFlows:
    """Several accounts' same day as one pooled (asset-weighted) portfolio: money summed, outage = the longest
    any account saw, suspended if any account was suspended; slippage only if measured for every account."""
    slips = [d.slippage for d in days]
    return DayFlows(trading_date,
                    sum((d.starting_equity for d in days), Decimal(0)), sum((d.ending_equity for d in days), Decimal(0)),
                    sum((d.deposits for d in days), Decimal(0)), sum((d.withdrawals for d in days), Decimal(0)),
                    sum((d.fees for d in days), Decimal(0)), sum((d.funding for d in days), Decimal(0)),
                    sum((d.gross_exposure for d in days), Decimal(0)),
                    None if any(s is None for s in slips) else sum(slips, Decimal(0)),
                    max((d.outage_minutes for d in days), default=0), any(d.suspended for d in days))


# ── export ──────────────────────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SeriesFilter:
    source_kind: str
    family_id: str
    venue: Optional[str] = None
    family_version: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @classmethod
    def of(cls, value: Any) -> "SeriesFilter":
        if isinstance(value, SeriesFilter):
            f = value
        elif isinstance(value, Mapping):
            unknown = set(value) - set(cls.__dataclass_fields__)
            if unknown:
                raise TrackRecordError("FILTER_UNKNOWN_FIELD", ",".join(sorted(unknown)))
            f = cls(**value)
        else:
            raise TrackRecordError("FILTER_INVALID")
        if f.source_kind not in SOURCE_KINDS:
            raise TrackRecordError("SOURCE_KIND_REQUIRED", "one source kind per series; kinds are never mixed")
        _text("family_id", f.family_id)
        for d in (f.date_from, f.date_to):
            if d is not None:
                _iso_date(d, "date_range")
        return f

    def series_id(self) -> str:
        parts = [self.source_kind.lower(), self.family_id, self.venue or "all-venues"]
        if self.family_version:
            parts.append(f"v{self.family_version}")
        return re.sub(r"[^a-z0-9._-]+", "-", "-".join(parts).lower()).strip("-")


_EXPORT_COLS = ("series_key", "trading_date", "family_version", "account_class", "risk_level", "account_ref",
                "starting_equity", "ending_equity", "deposits", "withdrawals", "fees", "funding", "slippage",
                "gross_exposure", "outage_minutes", "strategy_suspended", "recorded_at")


def _metrics_doc(m: Metrics) -> Dict[str, Any]:
    return {
        "time_weighted_return": _out(m.twr), "max_drawdown": _out(m.max_drawdown),
        "fee_drag": _out(m.fee_drag), "funding_drag": _out(m.funding_drag),
        "slippage_drag": _out(m.slippage_drag), "slippage_measured_days": m.slippage_measured_days,
        "average_gross_exposure_ratio": _out(m.avg_gross_exposure),
        "max_gross_exposure_ratio": _out(m.max_gross_exposure),
        "outage_minutes": m.outage_minutes, "outage_days": m.outage_days,
        "strategy_suspended_days": m.suspended_days, "excluded_zero_capital_days": m.excluded_days,
        "trading_days": m.trading_days,
    }


def _index_doc(m: Metrics) -> List[Dict[str, Any]]:
    return [{"date": p["date"], "return": _out(p["return"]), "index": _out(p["index"])} for p in m.index]


def _group_minimum(classes: Iterable[str], min_accounts: int) -> int:
    floor = PROTECTED_CLASS_MIN_ACCOUNTS if any(c in PROTECTED_CLASSES for c in classes) else 1
    return max(min_accounts, floor)


def _aggregate(rows: Sequence[Dict[str, Any]], min_accounts: int) -> Dict[str, Any]:
    """Pool the rows by date; days on which fewer than the minimum accounts contributed are left out."""
    minimum = _group_minimum({r["account_class"] for r in rows}, min_accounts)
    accounts = {r["account_ref"] for r in rows}
    if len(accounts) < minimum:
        return {"status": SUPPRESSED_MIN_COUNT, "minimum_accounts": minimum}
    by_date: Dict[str, List[DayFlows]] = {}
    for r in rows:
        by_date.setdefault(r["trading_date"], []).append(r["_flows"])
    pooled, suppressed_days = [], 0
    for d, flows in sorted(by_date.items()):
        if len(flows) < minimum:
            suppressed_days += 1
            continue
        pooled.append(pool_day(d, flows))
    m = compute_metrics(pooled)
    return {"status": "PUBLISHED", "account_count": len(accounts), "minimum_accounts": minimum,
            "days_suppressed_min_count": suppressed_days, "metrics": _metrics_doc(m), "daily_index": _index_doc(m)}


def export_series(db: Database, filters: Any, *, min_accounts: int = DEFAULT_MIN_ACCOUNTS) -> Dict[str, Any]:
    """The public document for one series (see the module docstring). Only publishable, non-superseded rows."""
    f = SeriesFilter.of(filters)
    if isinstance(min_accounts, bool) or not isinstance(min_accounts, int) or min_accounts < 1:
        raise TrackRecordError("MIN_ACCOUNTS_INVALID")
    where = ["d.source_kind=?", "d.family_id=?", "d.publishable=1", _ACTIVE]
    params: List[Any] = [f.source_kind, f.family_id]
    for col, val in (("venue", f.venue), ("family_version", f.family_version)):
        if val is not None:
            where.append(f"d.{col}=?")
            params.append(val)
    if f.date_from:
        where.append("d.trading_date>=?")
        params.append(_iso_date(f.date_from))
    if f.date_to:
        where.append("d.trading_date<=?")
        params.append(_iso_date(f.date_to))
    chain_where = ["source_kind=?", "family_id=?"]
    chain_params: List[Any] = [f.source_kind, f.family_id]
    if f.venue is not None:
        chain_where.append("venue=?")
        chain_params.append(f.venue)
    with db.transaction(write=False) as conn:
        generated_at = conn.now_ms()
        raw = conn.all(f"SELECT {', '.join('d.' + c for c in _EXPORT_COLS)} FROM track_record_daily d "
                       f"WHERE {' AND '.join(where)} ORDER BY d.trading_date", params)
        heads = conn.all(f"SELECT series_key, MAX(seq) FROM track_record_daily WHERE {' AND '.join(chain_where)} "
                         "GROUP BY series_key", chain_params)
        head_hashes, chain_rows = [], 0
        for key, seq in heads:
            head_hashes.append(conn.scalar("SELECT row_hash FROM track_record_daily WHERE series_key=? AND seq=?",
                                           (key, seq)))
            chain_rows += int(seq)
        any_live = bool(conn.scalar("SELECT COUNT(*) FROM track_record_daily WHERE source_kind='LIVE'")) \
            if f.source_kind == "LIVE" else False

    rows: List[Dict[str, Any]] = []
    for r in raw:
        row = dict(zip(_EXPORT_COLS, r))
        m = {k: decimal_out(row[k]) for k in ("starting_equity", "ending_equity", "deposits", "withdrawals", "fees",
                                             "funding", "slippage", "gross_exposure")}
        row["_flows"] = DayFlows(row["trading_date"], m["starting_equity"], m["ending_equity"], m["deposits"],
                                 m["withdrawals"], m["fees"], m["funding"], m["gross_exposure"], m["slippage"],
                                 int(row["outage_minutes"]), bool(row["strategy_suspended"]))
        rows.append(row)

    is_live = f.source_kind == "LIVE"
    doc: Dict[str, Any] = {
        "document_type": "cosmicforge.track_record.series",
        "methodology_version": METHODOLOGY_VERSION,
        "series_id": f.series_id(),
        "source_kind": f.source_kind,
        "is_live": is_live,
        "label": LIVE_LABEL if is_live else SIMULATED_LABEL,
        "audited": False,
        "family_id": f.family_id,
        "venue": f.venue or "ALL",
        "return_methodology": {
            "method": "TIME_WEIGHTED_CHAIN_LINKED_DAILY",
            "formula": RETURN_FORMULA,
            "flow_convention": FLOW_CONVENTION,
            "aggregation": "ASSET_WEIGHTED_POOLED_BY_DAY",
            "returns_are": "NET_OF_FEES_AND_FUNDING",
        },
        "fees_and_funding_treatment": (
            "Fees and funding are already reflected in ending equity, so returns are net of both. fee_drag and "
            "funding_drag show how much they changed the period return (funding_drag is negative when funding "
            "was received)."),
        "limitations": ([] if is_live else [NON_LIVE_LIMITATION]) + list(LIMITATIONS),
    }
    if not rows:
        doc.update({
            "state": "NOT_YET_AVAILABLE" if is_live else "NO_PUBLISHABLE_DATA",
            "reason": (LIVE_NOT_PUBLISHED_REASON if any_live else LIVE_NOT_AVAILABLE_REASON) if is_live
            else "No publishable rows match this series",
            "strategy_versions": [], "observation_period": None, "daily_index": [],
            "composite": None, "aggregates": [],
        })
    else:
        versions: Dict[str, List[str]] = {}
        for r in rows:
            versions.setdefault(r["family_version"], []).append(r["trading_date"])
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for r in rows:
            groups.setdefault((r["account_class"], r["risk_level"]), []).append(r)
        composite = _aggregate(rows, min_accounts)
        doc.update({
            "state": "PUBLISHED_DATA",
            "strategy_versions": [{"family_version": v, "first_date": min(ds), "last_date": max(ds)}
                                  for v, ds in sorted(versions.items(), key=lambda kv: min(kv[1]))],
            "observation_period": {"start": rows[0]["trading_date"], "end": rows[-1]["trading_date"],
                                   "calendar_days_with_data": len({r["trading_date"] for r in rows})},
            "composite": {k: v for k, v in composite.items() if k != "daily_index"},
            "daily_index": composite.get("daily_index", []),
            "aggregates": [dict({"account_class": c, "risk_level": rl}, **_aggregate(g, min_accounts))
                           for (c, rl), g in sorted(groups.items())],
        })
        if len(versions) > 1:
            doc["limitations"].append("The strategy version changed during the observation period; see "
                                      "strategy_versions for the dates of each version.")
    doc["provenance"] = {
        "chain_head_hash": hashlib.sha256("\n".join(sorted(head_hashes)).encode()).hexdigest() if head_hashes else None,
        "chain_count": len(head_hashes),
        "chain_row_count": chain_rows,
        "rows_used": len(rows),
        "data_freshness": {
            "latest_trading_date": rows[-1]["trading_date"] if rows else None,
            "last_recorded_at": _iso_ms(max(int(r["recorded_at"]) for r in rows)) if rows else None,
            "generated_at": _iso_ms(generated_at),
        },
        "verification": "Recompute each chain's row hashes (verify_chain) and the SHA-256 of the sorted chain "
                        "head hashes joined by newlines; it must equal chain_head_hash.",
    }
    assert_public_safe(doc, {r["account_ref"] for r in rows})
    return doc


def assert_public_safe(doc: Any, account_refs: Iterable[str] = ()) -> None:
    """Refuse a document carrying an identifying or financial key, or anything that looks like an account ref."""
    refs = set(account_refs)

    def walk(node: Any, path: str) -> None:
        if isinstance(node, Mapping):
            for k, v in node.items():
                if str(k).lower() in FORBIDDEN_PUBLIC_KEYS:
                    raise TrackRecordError("PUBLIC_DOCUMENT_FORBIDDEN_FIELD", f"{path}.{k}")
                walk(v, f"{path}.{k}")
        elif isinstance(node, (list, tuple)):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")
        elif isinstance(node, str):
            if node in refs or _ACCOUNT_REF_RE.search(node) or "@" in node:
                raise TrackRecordError("PUBLIC_DOCUMENT_IDENTIFYING_VALUE", path)

    walk(doc, "$")


# ── publication and the public read model ───────────────────────────────────────────────────────────────────

def publish_series(db: Database, filters: Any, *, min_accounts: int = DEFAULT_MIN_ACCOUNTS) -> Dict[str, Any]:
    """Export and store the document; the public API serves the newest publication per series id."""
    doc = export_series(db, filters, min_accounts=min_accounts)
    if doc["state"] != "PUBLISHED_DATA":
        raise TrackRecordError("LIVE_NOT_AVAILABLE" if doc["is_live"] else "NOTHING_TO_PUBLISH", doc.get("reason", ""))
    body = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    publication_id = f"trp_{uuid.uuid4().hex}"
    with db.transaction() as conn:
        conn.execute("INSERT INTO track_record_publications (publication_id, series_id, source_kind, family_id, "
                     "methodology_version, document, document_hash, chain_head_hash, row_count, published_at) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (publication_id, doc["series_id"], doc["source_kind"], doc["family_id"], METHODOLOGY_VERSION,
                      body, hashlib.sha256(body.encode()).hexdigest(), doc["provenance"]["chain_head_hash"],
                      doc["provenance"]["rows_used"], conn.now_ms()))
    return {"publication_id": publication_id, "series_id": doc["series_id"], "document": doc}


_TAB_ORDER = SOURCE_KINDS


def _latest_publications(conn: Connection) -> List[Tuple[str, str, str, int]]:
    latest: Dict[str, Tuple[str, str, str, int]] = {}
    for sid, kind, body, at in conn.all("SELECT series_id, source_kind, document, published_at "
                                        "FROM track_record_publications ORDER BY series_id, published_at"):
        latest[sid] = (sid, kind, body, int(at))   # rows ascend by time: the last one per series wins
    return [latest[k] for k in sorted(latest)]


def public_index(db: Optional[Database]) -> Dict[str, Any]:
    """Every kind's published series (summaries). Live has an explicit NOT_YET_AVAILABLE state."""
    pubs: List[Tuple[str, str, str, int]] = []
    any_live = False
    store = "AVAILABLE"
    if db is None:
        store = "NOT_CONFIGURED"
    else:
        try:
            with db.transaction(write=False) as conn:
                pubs = _latest_publications(conn)
                any_live = bool(conn.scalar("SELECT COUNT(*) FROM track_record_daily WHERE source_kind='LIVE'"))
        except Exception:
            store = "UNAVAILABLE"
            pubs = []
    kinds: Dict[str, Any] = {}
    for kind in _TAB_ORDER:
        series = []
        for sid, k, body, published_at in pubs:
            if k != kind:
                continue
            doc = json.loads(body)
            series.append({
                "series_id": sid, "source_kind": doc["source_kind"], "family_id": doc["family_id"],
                "venue": doc["venue"], "label": doc["label"],
                "is_live": doc["is_live"], "strategy_versions": doc["strategy_versions"],
                "observation_period": doc["observation_period"], "methodology_version": doc["methodology_version"],
                "composite": doc["composite"], "published_at": _iso_ms(published_at),
                "data_freshness": doc["provenance"]["data_freshness"],
            })
        entry: Dict[str, Any] = {"label": LIVE_LABEL if kind == "LIVE" else SIMULATED_LABEL, "series": series,
                                 "state": "AVAILABLE" if series else "NO_PUBLISHED_SERIES"}
        if kind == "LIVE" and not series:
            entry["state"] = "NOT_YET_AVAILABLE"
            entry["reason"] = LIVE_NOT_PUBLISHED_REASON if any_live else LIVE_NOT_AVAILABLE_REASON
        kinds[kind] = entry
    out = {"methodology_version": METHODOLOGY_VERSION, "audited": False, "store": store, "kinds": kinds,
           "live": {k: v for k, v in kinds["LIVE"].items() if k in ("state", "reason")}}
    assert_public_safe(out)
    return out


def public_series(db: Optional[Database], series_id: str) -> Optional[Dict[str, Any]]:
    if db is None or not isinstance(series_id, str) or not re.fullmatch(r"[a-z0-9._-]{1,200}", series_id):
        return None
    try:
        with db.transaction(write=False) as conn:
            row = conn.one("SELECT document, published_at FROM track_record_publications WHERE series_id=? "
                           "ORDER BY published_at DESC, publication_id DESC LIMIT 1", (series_id,))
    except Exception:
        return None
    if row is None:
        return None
    doc = json.loads(row[0])
    doc["published_at"] = _iso_ms(row[1])
    assert_public_safe(doc)
    return doc
