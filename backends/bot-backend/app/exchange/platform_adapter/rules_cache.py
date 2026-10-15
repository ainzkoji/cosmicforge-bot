"""The persistent market-rule cache of the venue adapters (Section H, Step 4.3).

One row per (venue, environment, canonical symbol) holding the ``InstrumentRules`` an adapter last read from
the venue, with the time they were read, a hash of their content, the adapter version and an expiry.

The single rule of this module: **a rule is either what the venue said, recently enough, or it is a refusal.**
There is no default tick, step, minimum or leverage anywhere below. Each way of not knowing is its own reason:

* ``RULES_UNKNOWN_INSTRUMENT``      nothing was ever read for the symbol
* ``RULES_STALE``                   the last read is older than the maximum age
* ``RULES_DATED_IN_THE_FUTURE``     the read is stamped after ``now`` beyond a clock tolerance
* ``RULES_INCOMPLETE``              the venue did not report a value an order needs
* ``RULES_INSTRUMENT_NOT_TRADING``  the venue lists the instrument but takes no orders for it
* ``RULES_DELISTED``                the instrument disappeared from the venue's listing
* ``RULES_CONFLICT``                the last answer for the symbol contradicted itself or its own key
* ``RULES_CHANGED_SINCE_PLAN``      the rules are not the ones the plan was sized with
* ``RULES_PLAN_REFERENCE_MISSING``  an entry was checked without saying which rules its plan used

Three questions can be asked, because the three callers need different answers:

* ``rules_for_planning``  fresh, complete, trading, consistent: the rules and the hash a plan records;
* ``rules_for_entry``     the same, and the rules must still be the ones of the plan;
* ``rules_for_reduce``    for an order that can only reduce a position (a protective stop, a close, a
  flatten). Being unable to quantise a close is worse than quantising it with the venue's last known step, so
  stale, delisted, not-trading or conflicting rules are returned WITH the tolerated reasons in ``degraded``.
  The values are still the venue's own last answer; an instrument that was never read, or whose quantity step
  the venue never reported, is refused here too.

Refresh never holds a transaction across the venue call, never treats an empty or implausibly short listing as
a mass delisting, keeps an older answer from overwriting a newer one, and backs off (bounded) after a failure
while the rows it could not refresh simply age into ``RULES_STALE``.

Relation to the existing catalogue. ``app.exchange.instruments.InstrumentCatalog`` (table
``venue_instruments``) is the discovery catalogue of every instrument a venue publishes, keyed by the legacy
venue keys and read by ``SubmissionPreflight`` with its own one-hour limit; it answers "what is this product
and may this account trade it". This cache answers "with which filters is an order for it built", in the
vocabulary of the broker-neutral contract, for the adapters of ``platform_adapter``. Neither replaces the
other and neither is edited by the other.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.exchange.instruments import MASS_DELIST_MAX_FRACTION
from app.exchange.platform_adapter import contract as C
from app.trading_intelligence.hashing import short_id, stable_hash

from shared_lib.persistence.cati_exchange_schema import RULES_CHANGE_TABLE, RULES_REFRESH_TABLE, RULES_TABLE

RULES_CACHE_VERSION = "1.0.0"

#: A venue is asked at least daily; a row older than this is never used for an entry. Configuration.
DEFAULT_MAX_AGE_MS = 24 * 3_600_000
DEFAULT_REFRESH_INTERVAL_MS = 6 * 3_600_000
#: an adapter stamps a read with its own clock, which may run slightly ahead of the caller's ``now``
CLOCK_AHEAD_TOLERANCE_MS = 120_000
REFRESH_BACKOFF_BASE_MS = 5_000
REFRESH_BACKOFF_MAX_MS = 900_000
#: how long one worker's refresh keeps the others from calling the venue for the same listing
REFRESH_CLAIM_MS = 60_000
#: below this many known instruments a short listing is not judged (same idea as ``sync_instruments``)
MASS_DELIST_MIN_KNOWN = 20

PLANNING, ENTRY, REDUCE = "PLANNING", "ENTRY", "REDUCE"

RULES_UNKNOWN_INSTRUMENT = "RULES_UNKNOWN_INSTRUMENT"
RULES_STALE = "RULES_STALE"
RULES_DATED_IN_THE_FUTURE = "RULES_DATED_IN_THE_FUTURE"
RULES_INCOMPLETE = "RULES_INCOMPLETE"
RULES_INSTRUMENT_NOT_TRADING = "RULES_INSTRUMENT_NOT_TRADING"
RULES_DELISTED = "RULES_DELISTED"
RULES_CONFLICT = "RULES_CONFLICT"
RULES_CHANGED_SINCE_PLAN = "RULES_CHANGED_SINCE_PLAN"
RULES_PLAN_REFERENCE_MISSING = "RULES_PLAN_REFERENCE_MISSING"
RULES_LEVERAGE_LIMIT_UNKNOWN = "RULES_LEVERAGE_LIMIT_UNKNOWN"
LEVERAGE_ABOVE_VENUE_MAXIMUM = "LEVERAGE_ABOVE_VENUE_MAXIMUM"

RULES_REFRESH_BACKOFF = "RULES_REFRESH_BACKOFF"
RULES_REFRESH_IN_PROGRESS = "RULES_REFRESH_IN_PROGRESS"
RULES_REFRESH_FAILED = "RULES_REFRESH_FAILED"
RULES_REFRESH_EMPTY = "RULES_REFRESH_EMPTY"
RULES_REFRESH_SUSPECT_PARTIAL = "RULES_REFRESH_SUSPECT_PARTIAL"

FIRST_SEEN, RULES_CHANGED, STATUS_CHANGED, DELISTED, RELISTED, CONFLICT, CONFLICT_CLEARED = (
    "FIRST_SEEN", "RULES_CHANGED", "STATUS_CHANGED", "DELISTED", "RELISTED", "CONFLICT", "CONFLICT_CLEARED")

# the same reason codes ``SubmissionPreflight.quantity`` uses for the same facts
EXECUTABLE_QUANTITY_UNKNOWN = "EXECUTABLE_QUANTITY_UNKNOWN"
QUANTITY_BELOW_MINIMUM = "QUANTITY_BELOW_MINIMUM"
QUANTITY_ABOVE_MAXIMUM = "QUANTITY_ABOVE_MAXIMUM"
MIN_NOTIONAL_NOT_MET = "MIN_NOTIONAL_NOT_MET"

#: the content of a rule record; ``fetched_at_ms`` and ``raw_hash`` say when and from what, not what
RULE_FIELDS: Tuple[str, ...] = (
    "venue", "environment", "symbol", "venue_symbol", "status", "venue_status", "base_asset", "quote_asset",
    "margin_asset", "price_tick", "quantity_step", "min_quantity", "max_quantity", "market_max_quantity",
    "min_notional", "max_leverage", "contract_size")


class RulesRefused(Exception):
    """The cache has no rules that may be used for this purpose."""

    def __init__(self, reasons: Sequence[str], detail: Optional[Mapping[str, Any]] = None) -> None:
        self.reasons: Tuple[str, ...] = tuple(reasons)
        self.reason = self.reasons[0] if self.reasons else ""
        self.detail: Mapping[str, Any] = dict(detail or {})
        super().__init__(",".join(self.reasons))


@dataclass(frozen=True)
class RulesDecision:
    ok: bool
    purpose: str
    reasons: Tuple[str, ...]                    # why the rules may not be used; empty when ``ok``
    rules: Optional[C.InstrumentRules]          # present only when ``ok``
    rules_hash: Optional[str]
    degraded: Tuple[str, ...] = ()              # REDUCE only: what was tolerated to keep a close possible
    fetched_at_ms: Optional[int] = None
    expires_at_ms: Optional[int] = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    @property
    def reason(self) -> str:
        return self.reasons[0] if self.reasons else ""

    def require(self) -> C.InstrumentRules:
        if not self.ok or self.rules is None:
            raise RulesRefused(self.reasons, self.detail)
        return self.rules


@dataclass(frozen=True)
class RefreshResult:
    ok: bool
    venue: str
    environment: str
    reason: str = ""
    called_venue: bool = False
    partial: bool = False
    stored: int = 0
    unchanged: int = 0
    changed: int = 0
    first_seen: int = 0
    relisted: int = 0
    delisted: int = 0
    conflicts: int = 0
    incomplete: int = 0
    skipped_older: int = 0
    next_attempt_ms: Optional[int] = None


@dataclass(frozen=True)
class QuantityDecision:
    ok: bool
    quantity: Optional[float]
    reasons: Tuple[str, ...] = ()


def rules_hash(rules: C.InstrumentRules) -> str:
    """Identity of the CONTENT of a rule record. Two reads of unchanged rules hash the same."""
    return stable_hash({name: getattr(rules, name) for name in RULE_FIELDS})


def refresh_backoff_ms(consecutive_failures: int) -> int:
    """Wait after the n-th consecutive failed refresh: doubling from the base, never above the maximum."""
    if consecutive_failures <= 0:
        return 0
    return int(min(REFRESH_BACKOFF_BASE_MS * (2 ** min(consecutive_failures - 1, 30)), REFRESH_BACKOFF_MAX_MS))


def _inconsistency(record: Any, key_symbol: str, venue: str, environment: str, now_ms: int) -> Optional[str]:
    """Why a returned record cannot be the rules of ``key_symbol`` (``None`` when it can)."""
    if not isinstance(record, C.InstrumentRules):
        return "NOT_INSTRUMENT_RULES"
    if record.symbol != key_symbol:
        return "KEY_SYMBOL_MISMATCH"
    if record.venue != venue:
        return "VENUE_MISMATCH"
    if record.environment != environment:
        return "ENVIRONMENT_MISMATCH"
    if int(record.fetched_at_ms) > int(now_ms) + CLOCK_AHEAD_TOLERANCE_MS:
        return "FETCHED_IN_THE_FUTURE"
    low, high, step = record.min_quantity, record.max_quantity, record.quantity_step
    if low is not None and high is not None and low > high:
        return "MIN_QUANTITY_ABOVE_MAX_QUANTITY"
    if low is not None and record.market_max_quantity is not None and low > record.market_max_quantity:
        return "MIN_QUANTITY_ABOVE_MARKET_MAX_QUANTITY"
    if step is not None and high is not None and step > high:
        return "QUANTITY_STEP_ABOVE_MAX_QUANTITY"
    return None


class RulesCache:
    def __init__(self, db: Any, *, max_age_ms: int = DEFAULT_MAX_AGE_MS,
                 refresh_interval_ms: int = DEFAULT_REFRESH_INTERVAL_MS, worker: str = "",
                 mass_delist_min_known: int = MASS_DELIST_MIN_KNOWN) -> None:
        if int(max_age_ms) <= 0 or int(refresh_interval_ms) <= 0:
            raise ValueError("the maximum age and the refresh interval are positive")
        self.db = db
        self.max_age_ms, self.refresh_interval_ms = int(max_age_ms), int(refresh_interval_ms)
        self.worker = str(worker)
        self.mass_delist_min_known = int(mass_delist_min_known)

    # ---------------------------------------------------------------------------------------------- refresh
    def refresh_due(self, venue: str, environment: str, now_ms: int) -> bool:
        """Never refreshed, the refresh interval has passed, or a failed refresh may be tried again."""
        state = self.refresh_state(venue, environment)
        if state is None or state["last_success_ms"] is None:
            return state is None or int(state["next_attempt_ms"]) <= int(now_ms)
        if int(state["consecutive_failures"]) > 0:
            return int(state["next_attempt_ms"]) <= int(now_ms)
        return int(now_ms) - int(state["last_success_ms"]) >= self.refresh_interval_ms

    def refresh_state(self, venue: str, environment: str) -> Optional[Dict[str, Any]]:
        with self.db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {RULES_REFRESH_TABLE} WHERE venue=? AND environment=?",
                               (venue, environment)).fetchone()
        return None if row is None else dict(row)

    def _claim(self, venue: str, environment: str, now_ms: int, force: bool) -> Optional[RefreshResult]:
        """Take the right to call the venue, or say why it is not called."""
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f"INSERT OR IGNORE INTO {RULES_REFRESH_TABLE} (venue, environment, updated_at_ms) "
                         "VALUES (?,?,?)", (venue, environment, now_ms))
            row = conn.execute(f"SELECT * FROM {RULES_REFRESH_TABLE} WHERE venue=? AND environment=?",
                               (venue, environment)).fetchone()
            if int(row["claim_until_ms"]) > now_ms and row["claim_owner"] != self.worker:
                return RefreshResult(False, venue, environment, RULES_REFRESH_IN_PROGRESS,
                                     next_attempt_ms=int(row["claim_until_ms"]))
            if not force and int(row["next_attempt_ms"]) > now_ms:
                return RefreshResult(False, venue, environment, RULES_REFRESH_BACKOFF,
                                     next_attempt_ms=int(row["next_attempt_ms"]))
            conn.execute(f"UPDATE {RULES_REFRESH_TABLE} SET claim_owner=?, claim_until_ms=?, last_attempt_ms=?, "
                         "updated_at_ms=? WHERE venue=? AND environment=?",
                         (self.worker, now_ms + REFRESH_CLAIM_MS, now_ms, now_ms, venue, environment))
        return None

    def _failed(self, venue: str, environment: str, now_ms: int, reason: str,
                error: Optional[C.AdapterError] = None) -> RefreshResult:
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            next_attempt_ms = self._note_failure(conn, venue, environment, now_ms, reason, error)
        return RefreshResult(False, venue, environment, reason, called_venue=True, next_attempt_ms=next_attempt_ms)

    @staticmethod
    def _note_failure(conn: Any, venue: str, environment: str, now_ms: int, reason: str,
                      error: Optional[C.AdapterError] = None) -> int:
        row = conn.execute(f"SELECT consecutive_failures FROM {RULES_REFRESH_TABLE} WHERE venue=? AND environment=?",
                           (venue, environment)).fetchone()
        failures = int(row[0]) + 1
        next_attempt_ms = now_ms + refresh_backoff_ms(failures)
        if error is not None:  # the venue's own instruction to wait always wins over a shorter backoff
            if error.retry_after_s is not None:
                next_attempt_ms = max(next_attempt_ms, now_ms + int(float(error.retry_after_s) * 1000) + 1)
            if error.banned_until_ms is not None:
                next_attempt_ms = max(next_attempt_ms, int(error.banned_until_ms))
        conn.execute(
            f"UPDATE {RULES_REFRESH_TABLE} SET consecutive_failures=?, next_attempt_ms=?, last_error=?, claim_owner=NULL, "
            "claim_until_ms=0, updated_at_ms=? WHERE venue=? AND environment=?",
            (failures, next_attempt_ms, reason, now_ms, venue, environment))
        return next_attempt_ms

    def refresh(self, adapter: Any, now_ms: int, *, symbols: Optional[Sequence[str]] = None,
                force: bool = False) -> RefreshResult:
        """Read rules from the venue through ``adapter`` and store them.

        ``symbols=None`` reads the whole listing: a known instrument that is absent from it is marked
        delisted, unless the listing is empty or implausibly short, in which case nothing is delisted and
        the refresh counts as failed. With ``symbols`` only those are read; one the venue does not return is
        marked delisted. ``force`` skips the backoff after a failure, never a refresh another worker is making.
        """
        venue, environment, now_ms = adapter.venue, adapter.environment, int(now_ms)
        wanted = None if symbols is None else tuple(sorted({C.canonical_symbol(s) for s in symbols}))
        if wanted is not None and not wanted:
            raise ValueError("a targeted refresh names at least one symbol")
        refused = self._claim(venue, environment, now_ms, force)
        if refused is not None:
            return refused
        try:  # no transaction is open while the venue is asked
            capabilities = asdict(adapter.capabilities())
            fetched = adapter.instrument_rules(wanted)
        except C.AdapterError as error:
            if not (error.code == C.INSTRUMENT_UNKNOWN and wanted is not None and len(wanted) == 1):
                return self._failed(venue, environment, now_ms, f"{RULES_REFRESH_FAILED}:{error.code}", error)
            fetched = {}
        if not isinstance(fetched, Mapping):
            return self._failed(venue, environment, now_ms, f"{RULES_REFRESH_FAILED}:{C.MALFORMED_RESPONSE}")
        if wanted is None and not fetched:
            # an empty listing is a venue or transport failure, never "everything was delisted"
            return self._failed(venue, environment, now_ms, RULES_REFRESH_EMPTY)
        return self._store(venue, environment, adapter.adapter_version, capabilities, fetched, wanted, now_ms)

    def _store(self, venue: str, environment: str, adapter_version: str, capabilities: Mapping[str, Any],
               fetched: Mapping[Any, Any], wanted: Optional[Tuple[str, ...]], now_ms: int) -> RefreshResult:
        counts = dict(stored=0, unchanged=0, changed=0, first_seen=0, relisted=0, delisted=0, conflicts=0,
                      incomplete=0, skipped_older=0)
        capabilities_json = json.dumps(capabilities, sort_keys=True, default=str)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            known = {row["symbol"]: row for row in conn.execute(
                f"SELECT * FROM {RULES_TABLE} WHERE venue=? AND environment=?", (venue, environment)).fetchall()}
            present = set()
            for key in sorted(fetched, key=str):
                record = fetched[key]
                try:
                    symbol = C.canonical_symbol(key)
                except C.ContractViolation:
                    counts["conflicts"] += 1
                    continue
                present.add(symbol)
                previous = known.get(symbol)
                problem = _inconsistency(record, symbol, venue, environment, now_ms)
                if problem is not None:
                    counts["conflicts"] += 1
                    self._mark_conflict(conn, venue, environment, symbol, previous, problem, record, adapter_version,
                                        now_ms)
                    continue
                if previous is not None and int(previous["fetched_at_ms"]) > int(record.fetched_at_ms):
                    counts["skipped_older"] += 1  # a late answer never replaces a newer one
                    continue
                self._store_one(conn, record, previous, adapter_version, capabilities_json, now_ms, counts)
            live = {symbol for symbol, row in known.items() if row["delisted_at_ms"] is None}
            suspect = (wanted is None and len(live) >= self.mass_delist_min_known
                       and len(present) < len(live) * (1.0 - MASS_DELIST_MAX_FRACTION))
            missing = set() if suspect else ((live if wanted is None else live & set(wanted)) - present)
            for symbol in sorted(missing):
                conn.execute(f"UPDATE {RULES_TABLE} SET delisted_at_ms=?, changed_at_ms=? WHERE venue=? AND "
                             "environment=? AND symbol=?", (now_ms, now_ms, venue, environment, symbol))
                self._change(conn, venue, environment, symbol, DELISTED, known[symbol]["rules_hash"], None, [],
                             {"absent_from": "FULL_LISTING" if wanted is None else "TARGETED_READ"}, adapter_version,
                             None, now_ms)
                counts["delisted"] += 1
            if suspect:
                next_attempt_ms: Optional[int] = self._note_failure(conn, venue, environment, now_ms,
                                                                    RULES_REFRESH_SUSPECT_PARTIAL)
            else:
                next_attempt_ms = None
                conn.execute(
                    f"UPDATE {RULES_REFRESH_TABLE} SET consecutive_failures=0, next_attempt_ms=0, last_error=NULL, "
                    "last_success_ms=?, claim_owner=NULL, claim_until_ms=0, symbols_seen=?, updated_at_ms=? "
                    "WHERE venue=? AND environment=?", (now_ms, len(present), now_ms, venue, environment))
        return RefreshResult(not suspect, venue, environment, RULES_REFRESH_SUSPECT_PARTIAL if suspect else "",
                             called_venue=True, partial=suspect, next_attempt_ms=next_attempt_ms, **counts)

    def _store_one(self, conn: Any, record: C.InstrumentRules, previous: Any, adapter_version: str,
                   capabilities_json: str, now_ms: int, counts: Dict[str, int]) -> None:
        new_hash = rules_hash(record)
        key = (record.venue, record.environment, record.symbol)
        document = {name: getattr(record, name) for name in RULE_FIELDS}
        changes: List[Tuple[str, List[str]]] = []
        if previous is None:
            changes.append((FIRST_SEEN, []))
            counts["first_seen"] += 1
        else:
            if previous["delisted_at_ms"] is not None:
                changes.append((RELISTED, []))
                counts["relisted"] += 1
            if previous["conflict_at_ms"] is not None:
                changes.append((CONFLICT_CLEARED, []))
            if previous["rules_hash"] != new_hash:
                fields = [name for name in RULE_FIELDS if previous[name] != getattr(record, name)]
                changes.append((STATUS_CHANGED if set(fields) <= {"status", "venue_status"} else RULES_CHANGED, fields))
                counts["changed"] += 1
            elif previous["delisted_at_ms"] is None:
                counts["unchanged"] += 1
        content_changed = previous is not None and (previous["rules_hash"] != new_hash
                                                    or previous["delisted_at_ms"] is not None)
        changed_at_ms = now_ms if content_changed else (None if previous is None else previous["changed_at_ms"])
        first_seen_ms = now_ms if previous is None else int(previous["first_seen_ms"])
        conn.execute(
            f"""INSERT INTO {RULES_TABLE} (venue, environment, symbol, venue_symbol, status, venue_status, base_asset,
                quote_asset, margin_asset, price_tick, quantity_step, min_quantity, max_quantity, market_max_quantity,
                min_notional, max_leverage, contract_size, complete, rules_hash, raw_hash, adapter_version,
                capabilities, fetched_at_ms, stored_at_ms, expires_at_ms, first_seen_ms, changed_at_ms,
                delisted_at_ms, conflict_at_ms, conflict_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)
                ON CONFLICT(venue, environment, symbol) DO UPDATE SET venue_symbol=excluded.venue_symbol,
                status=excluded.status, venue_status=excluded.venue_status, base_asset=excluded.base_asset,
                quote_asset=excluded.quote_asset, margin_asset=excluded.margin_asset, price_tick=excluded.price_tick,
                quantity_step=excluded.quantity_step, min_quantity=excluded.min_quantity,
                max_quantity=excluded.max_quantity, market_max_quantity=excluded.market_max_quantity,
                min_notional=excluded.min_notional, max_leverage=excluded.max_leverage,
                contract_size=excluded.contract_size, complete=excluded.complete, rules_hash=excluded.rules_hash,
                raw_hash=excluded.raw_hash, adapter_version=excluded.adapter_version,
                capabilities=excluded.capabilities, fetched_at_ms=excluded.fetched_at_ms,
                stored_at_ms=excluded.stored_at_ms, expires_at_ms=excluded.expires_at_ms,
                changed_at_ms=excluded.changed_at_ms, delisted_at_ms=NULL, conflict_at_ms=NULL, conflict_reason=NULL""",
            (*key, record.venue_symbol, record.status, record.venue_status, record.base_asset, record.quote_asset,
             record.margin_asset, record.price_tick, record.quantity_step, record.min_quantity, record.max_quantity,
             record.market_max_quantity, record.min_notional, record.max_leverage, float(record.contract_size),
             int(record.complete), new_hash, record.raw_hash, adapter_version, capabilities_json,
             int(record.fetched_at_ms), now_ms, int(record.fetched_at_ms) + self.max_age_ms, first_seen_ms,
             changed_at_ms))
        counts["stored"] += 1
        counts["incomplete"] += int(not record.complete)
        for kind, fields in changes:
            self._change(conn, *key, kind, None if previous is None else previous["rules_hash"], new_hash, fields,
                         document, adapter_version, int(record.fetched_at_ms), now_ms)

    def _mark_conflict(self, conn: Any, venue: str, environment: str, symbol: str, previous: Any, problem: str,
                       record: Any, adapter_version: str, now_ms: int) -> None:
        if previous is not None and previous["conflict_reason"] == problem:
            return  # the same contradiction again: already marked and already in the history
        if previous is not None:
            conn.execute(f"UPDATE {RULES_TABLE} SET conflict_at_ms=?, conflict_reason=? WHERE venue=? AND "
                         "environment=? AND symbol=?", (now_ms, problem, venue, environment, symbol))
        returned = ({name: getattr(record, name) for name in RULE_FIELDS} if isinstance(record, C.InstrumentRules)
                    else {"type": type(record).__name__})
        self._change(conn, venue, environment, symbol, CONFLICT, None if previous is None else previous["rules_hash"],
                     None, [], {"problem": problem, "returned": returned}, adapter_version,
                     getattr(record, "fetched_at_ms", None), now_ms)

    @staticmethod
    def _change(conn: Any, venue: str, environment: str, symbol: str, kind: str, previous_hash: Optional[str],
                new_hash: Optional[str], fields: Sequence[str], document: Mapping[str, Any], adapter_version: str,
                fetched_at_ms: Optional[int], now_ms: int) -> None:
        change_id = short_id("rulechg", [venue, environment, symbol, kind, previous_hash, new_hash, fetched_at_ms,
                                         now_ms])
        conn.execute(
            f"INSERT OR IGNORE INTO {RULES_CHANGE_TABLE} (change_id, venue, environment, symbol, change_type, "
            "previous_hash, rules_hash, changed_fields, document, adapter_version, fetched_at_ms, recorded_at_ms) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (change_id, venue, environment, symbol, kind, previous_hash, new_hash, json.dumps(list(fields)),
             json.dumps(document, sort_keys=True, default=str), adapter_version, fetched_at_ms, now_ms))

    # ----------------------------------------------------------------------------------------------- lookup
    def rules_for_planning(self, venue: str, environment: str, symbol: str, now_ms: int) -> RulesDecision:
        """The rules a new plan may be sized with, and the hash the plan records."""
        return self._decide(venue, environment, symbol, now_ms, PLANNING, None, None)

    def rules_for_entry(self, venue: str, environment: str, symbol: str, now_ms: int, *,
                        plan_rules_hash: Optional[str] = None, plan_time_ms: Optional[int] = None) -> RulesDecision:
        """The rules an entry may be submitted with. The plan says which rules it was sized with
        (``plan_rules_hash``) or when it was made (``plan_time_ms``); without either the entry is refused."""
        return self._decide(venue, environment, symbol, now_ms, ENTRY, plan_rules_hash, plan_time_ms)

    def rules_for_reduce(self, venue: str, environment: str, symbol: str, now_ms: int) -> RulesDecision:
        """The venue's last known rules for an order that can only reduce or protect a position."""
        return self._decide(venue, environment, symbol, now_ms, REDUCE, None, None)

    def _decide(self, venue: str, environment: str, symbol: str, now_ms: int, purpose: str,
                plan_rules_hash: Optional[str], plan_time_ms: Optional[int]) -> RulesDecision:
        symbol, now_ms = C.canonical_symbol(symbol), int(now_ms)
        with self.db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {RULES_TABLE} WHERE venue=? AND environment=? AND symbol=?",
                               (venue, environment, symbol)).fetchone()
        key = {"venue": venue, "environment": environment, "symbol": symbol}
        if row is None:
            return RulesDecision(False, purpose, (RULES_UNKNOWN_INSTRUMENT,), None, None, detail=key)
        fetched_at_ms = int(row["fetched_at_ms"])
        expires_at_ms = min(int(row["expires_at_ms"]), fetched_at_ms + self.max_age_ms)
        reasons: List[str] = []
        if row["conflict_at_ms"] is not None:
            reasons.append(RULES_CONFLICT)
        if row["delisted_at_ms"] is not None:
            reasons.append(RULES_DELISTED)
        if fetched_at_ms > now_ms + CLOCK_AHEAD_TOLERANCE_MS:
            reasons.append(RULES_DATED_IN_THE_FUTURE)
        elif now_ms > expires_at_ms:
            reasons.append(RULES_STALE)
        if not row["complete"]:
            reasons.append(RULES_INCOMPLETE)
        if row["status"] != C.TRADING:
            reasons.append(RULES_INSTRUMENT_NOT_TRADING)
        if purpose == ENTRY:
            if plan_rules_hash is None and plan_time_ms is None:
                reasons.append(RULES_PLAN_REFERENCE_MISSING)
            elif ((plan_rules_hash is not None and plan_rules_hash != row["rules_hash"])
                  or (plan_time_ms is not None and row["changed_at_ms"] is not None
                      and int(row["changed_at_ms"]) > int(plan_time_ms))):
                reasons.append(RULES_CHANGED_SINCE_PLAN)
        detail = {**key, "age_ms": now_ms - fetched_at_ms, "adapter_version": row["adapter_version"],
                  "changed_at_ms": row["changed_at_ms"], "conflict_reason": row["conflict_reason"]}
        common = dict(rules_hash=row["rules_hash"], fetched_at_ms=fetched_at_ms, expires_at_ms=expires_at_ms,
                      detail=detail)
        if purpose == REDUCE:
            if row["quantity_step"] is None:  # nothing to quantise a close with, and no step is ever invented
                return RulesDecision(False, purpose, (RULES_INCOMPLETE,), None, **common)
            return RulesDecision(True, purpose, (), self._rules_of(row), degraded=tuple(reasons), **common)
        if reasons:
            return RulesDecision(False, purpose, tuple(reasons), None, **common)
        return RulesDecision(True, purpose, (), self._rules_of(row), **common)

    @staticmethod
    def _rules_of(row: Any) -> C.InstrumentRules:
        values = {name: row[name] for name in RULE_FIELDS}
        return C.InstrumentRules(fetched_at_ms=int(row["fetched_at_ms"]), raw_hash=row["raw_hash"], **values)

    def capabilities(self, venue: str, environment: str, symbol: str) -> Optional[Dict[str, Any]]:
        """The adapter capabilities recorded with the rules: position modes, order types, protection."""
        with self.db.connect() as conn:
            row = conn.execute(f"SELECT capabilities FROM {RULES_TABLE} WHERE venue=? AND environment=? AND symbol=?",
                               (venue, environment, C.canonical_symbol(symbol))).fetchone()
        return None if row is None else json.loads(row[0])

    def history(self, venue: str, environment: str, symbol: str) -> Tuple[Dict[str, Any], ...]:
        """Every recorded change of the symbol's rules, oldest first."""
        with self.db.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {RULES_CHANGE_TABLE} WHERE venue=? AND environment=? AND symbol=? "
                "ORDER BY recorded_at_ms, rowid", (venue, environment, C.canonical_symbol(symbol))).fetchall()
        return tuple({**dict(row), "changed_fields": json.loads(row["changed_fields"]),
                      "document": json.loads(row["document"])} for row in rows)

    def ensure_loaded(self, adapter: Any, symbols: Sequence[str], now_ms: int) -> Dict[str, RulesDecision]:
        """Start-up: make sure the rules of the required symbols are usable, asking the venue once when any
        is not, and return the decision for each. A symbol that is still refused stays refused."""
        wanted = sorted({C.canonical_symbol(s) for s in symbols})

        def decide() -> Dict[str, RulesDecision]:
            return {s: self.rules_for_planning(adapter.venue, adapter.environment, s, now_ms) for s in wanted}

        decisions = decide()
        if any(not d.ok for d in decisions.values()) or self.refresh_due(adapter.venue, adapter.environment, now_ms):
            if self.refresh(adapter, now_ms, symbols=wanted).called_venue:
                decisions = decide()
        return decisions


# ------------------------------------------------------------------------------------------- quantisation
# Every rounding below moves an order toward LESS risk or it refuses. Nothing rounds a quantity up.
def _decimal(value: Any, name: str) -> Decimal:
    try:
        number = Decimal(repr(float(value)))
    except (TypeError, ValueError):
        raise ValueError(f"{name} is not a number: {value!r}") from None
    if not number.is_finite():
        raise ValueError(f"{name} is not finite: {value!r}")
    return number


def _required(value: Optional[float], name: str) -> Decimal:
    if value is None:
        raise RulesRefused((RULES_INCOMPLETE,), {"missing": name})
    return _decimal(value, name)


def _to_multiple(value: Decimal, step: Decimal, rounding: str) -> float:
    return float((value / step).to_integral_value(rounding=rounding) * step)


def quantize_quantity(rules: C.InstrumentRules, quantity: float) -> float:
    """Largest multiple of the quantity step not above ``quantity``."""
    amount = _decimal(quantity, "quantity")
    if amount < 0:
        raise ValueError("a quantity is not negative")
    return _to_multiple(amount, _required(rules.quantity_step, "quantity_step"), ROUND_FLOOR)


def quantize_stop_price(rules: C.InstrumentRules, position_side: str, stop_price: float) -> float:
    """A protective stop on the tick grid, moved toward the position's entry when it is between two ticks:
    up for a LONG, down for a SHORT. The loss the stop allows never grows by rounding."""
    if position_side not in (C.LONG, C.SHORT):
        raise ValueError(f"a stop protects a LONG or a SHORT position, not {position_side!r}")
    return _to_multiple(_decimal(stop_price, "stop_price"), _required(rules.price_tick, "price_tick"),
                        ROUND_CEILING if position_side == C.LONG else ROUND_FLOOR)


def quantize_limit_price(rules: C.InstrumentRules, order_side: str, price: float) -> float:
    """A limit price on the tick grid that never pays more (BUY: down) and never sells for less (SELL: up)."""
    if order_side not in (C.BUY, C.SELL):
        raise ValueError(f"an order is a BUY or a SELL, not {order_side!r}")
    return _to_multiple(_decimal(price, "price"), _required(rules.price_tick, "price_tick"),
                        ROUND_FLOOR if order_side == C.BUY else ROUND_CEILING)


def executable_quantity(rules: C.InstrumentRules, quantity: float, price: float, *,
                        market: bool = False) -> QuantityDecision:
    """``quantity`` rounded down to the step and checked against the venue's minimum, maximum and minimum
    notional at ``price``. A quantity outside the limits is refused, never moved into them."""
    if not rules.complete:
        return QuantityDecision(False, None, (RULES_INCOMPLETE,))
    try:
        amount, reference = _decimal(quantity, "quantity"), _decimal(price, "price")
    except ValueError:
        return QuantityDecision(False, None, (EXECUTABLE_QUANTITY_UNKNOWN,))
    if amount <= 0 or reference <= 0:
        return QuantityDecision(False, None, (EXECUTABLE_QUANTITY_UNKNOWN,))
    rounded = Decimal(repr(quantize_quantity(rules, quantity)))
    ceilings = [limit for limit in (rules.max_quantity, rules.market_max_quantity if market else None)
                if limit is not None]
    reasons = []
    if rounded <= 0 or rounded < _decimal(rules.min_quantity, "min_quantity"):
        reasons.append(QUANTITY_BELOW_MINIMUM)
    if ceilings and rounded > _decimal(min(ceilings), "max_quantity"):
        reasons.append(QUANTITY_ABOVE_MAXIMUM)
    if rounded * reference < _decimal(rules.min_notional, "min_notional"):
        reasons.append(MIN_NOTIONAL_NOT_MET)
    return QuantityDecision(not reasons, float(rounded), tuple(reasons))


def minimum_order_quantity(rules: C.InstrumentRules, price: float) -> float:
    """The smallest quantity the venue accepts at ``price``: its minimum quantity, or the first step at which
    the minimum notional is met, whichever is larger. This is the venue's floor, not a rounding of a
    risk-sized quantity; it exists for bounded infrastructure tests. Refuses when the rules cannot say."""
    if not rules.complete:
        raise RulesRefused((RULES_INCOMPLETE,))
    reference = _decimal(price, "price")
    if reference <= 0:
        raise ValueError("a reference price is positive")
    step = _decimal(rules.quantity_step, "quantity_step")
    by_notional = (_decimal(rules.min_notional, "min_notional") / reference / step).to_integral_value(
        rounding=ROUND_CEILING) * step
    quantity = max(_decimal(rules.min_quantity, "min_quantity"), by_notional)
    decision = executable_quantity(rules, float(quantity), price)
    if not decision.ok or decision.quantity is None:
        raise RulesRefused(decision.reasons or (EXECUTABLE_QUANTITY_UNKNOWN,))
    return decision.quantity


def leverage_allowed(rules: C.InstrumentRules, leverage: float) -> Tuple[bool, str]:
    """Is ``leverage`` within the venue's limit? An unreported limit is a refusal, never an assumed one."""
    if not float(leverage) > 0:
        raise ValueError("a leverage is positive")
    if rules.max_leverage is None:
        return False, RULES_LEVERAGE_LIMIT_UNKNOWN
    if float(leverage) > float(rules.max_leverage):
        return False, LEVERAGE_ABOVE_VENUE_MAXIMUM
    return True, ""


__all__ = [
    "RULES_CACHE_VERSION", "DEFAULT_MAX_AGE_MS", "DEFAULT_REFRESH_INTERVAL_MS", "CLOCK_AHEAD_TOLERANCE_MS",
    "REFRESH_BACKOFF_BASE_MS", "REFRESH_BACKOFF_MAX_MS", "REFRESH_CLAIM_MS", "MASS_DELIST_MIN_KNOWN", "PLANNING",
    "ENTRY", "REDUCE", "RULES_UNKNOWN_INSTRUMENT", "RULES_STALE", "RULES_DATED_IN_THE_FUTURE", "RULES_INCOMPLETE",
    "RULES_INSTRUMENT_NOT_TRADING", "RULES_DELISTED", "RULES_CONFLICT", "RULES_CHANGED_SINCE_PLAN",
    "RULES_PLAN_REFERENCE_MISSING", "RULES_LEVERAGE_LIMIT_UNKNOWN", "LEVERAGE_ABOVE_VENUE_MAXIMUM",
    "RULES_REFRESH_BACKOFF", "RULES_REFRESH_IN_PROGRESS", "RULES_REFRESH_FAILED", "RULES_REFRESH_EMPTY",
    "RULES_REFRESH_SUSPECT_PARTIAL", "FIRST_SEEN", "RULES_CHANGED", "STATUS_CHANGED", "DELISTED", "RELISTED",
    "CONFLICT", "CONFLICT_CLEARED", "EXECUTABLE_QUANTITY_UNKNOWN", "QUANTITY_BELOW_MINIMUM", "QUANTITY_ABOVE_MAXIMUM",
    "MIN_NOTIONAL_NOT_MET", "RULE_FIELDS", "RulesRefused", "RulesDecision", "RefreshResult", "QuantityDecision",
    "RulesCache", "rules_hash", "refresh_backoff_ms", "quantize_quantity", "quantize_stop_price",
    "quantize_limit_price", "executable_quantity", "minimum_order_quantity", "leverage_allowed"]
