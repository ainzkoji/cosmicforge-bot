"""The CATI platform cycle driver (Section H, Step 3.1).

One driver owns the decision cycles of every registered family. It does not use, import or depend on the
legacy strategy runner (``runner.multi_runner``) or on the residual production loop: it reads the family
registry, asks each family's pipeline adapter which cycle is due, and runs it.

A cycle is identified by ``(family, cycle key, mode)``; its id is a function of those three, so starting the
same cycle twice finds the first one. Its life is durable:

    STARTED -> MARKET_DECIDED -> COMPLETED
            -> WAITING_FOR_DATA      (the decision bars are not there yet: nothing is guessed, it is retried)
            -> PARTIAL               (some account failed: the others are done, the failed ones are retried)
            -> FAILED                (the family could not be evaluated at all: e.g. its mandate no longer verifies)

and so is the life of each account inside it (``PENDING -> DONE | FAILED | SKIPPED``). A process that dies
anywhere is recovered by running the cycle again: the market stage is recomputed from the same inputs, every
account that is DONE is left alone, and an account whose transaction did not commit is processed as if for
the first time. Decisions are immutable and keyed by (cycle, account); a replay can only reproduce the stored
decision, and a different one is a recorded conflict, never an overwrite. That is what makes "no duplicate
signals from replay" a property of the data model rather than of careful calling.

The **market stage runs once per cycle** and serves every account. The **account stage is isolated**: one
account, one transaction, one consistent snapshot of its book, no state shared between accounts in this
process (the driver keeps nothing between calls; everything it knows is in the database or in the arguments).

**Shadow only.** In this build no family has order authority, and the driver has no way to place an order:
it holds no adapter, no client and no execution boundary. For each account it records which mode the registry
allows and, when that is not an executing mode, why (the registry's blockers). The hand-off of an authorized
decision to the execution boundary is ``pipeline.execution``; it is reached only through ``handoff``, which
is ``None`` here and in production configuration until a family is certified and promoted.
"""
from __future__ import annotations

import json
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from shared_lib.persistence.cati_platform_schema import (
    CYCLE_ACCOUNT_TABLE,
    CYCLE_METRIC_TABLE,
    CYCLE_TABLE,
)

from . import contracts as C
from .family_registry import FamilyRegistry, RegistryEntry
from .provenance import DecisionConflict, DecisionStore

DRIVER_VERSION = "1.0.0"
STARTED, MARKET_DECIDED, COMPLETED, PARTIAL, WAITING_FOR_DATA, FAILED = (
    "STARTED", "MARKET_DECIDED", "COMPLETED", "PARTIAL", "WAITING_FOR_DATA", "FAILED")
TERMINAL: Tuple[str, ...] = (COMPLETED,)
PENDING, DONE, ACCOUNT_FAILED, SKIPPED = "PENDING", "DONE", "FAILED", "SKIPPED"
#: a cycle whose owner has not written for this long is taken over
HEARTBEAT_STALE_MS = 10 * 60_000
#: how many missed cycles of one family a single ``run_due`` will make up
MAX_CATCH_UP = 30
PLATFORM_KILL, ACCOUNT_KILL, FAMILY_KILL = "PLATFORM_KILL_SWITCH", "ACCOUNT_KILL_SWITCH", "FAMILY_KILL_SWITCH"


@dataclass(frozen=True)
class AccountContext:
    """An account as the pipeline sees it. A model account (``user_id`` None) is the platform's own reference
    book of a risk level; it has no credentials and no exchange."""
    account_id: str
    user_id: Optional[str]
    venue: str
    environment: str
    risk_level: str
    policy: str
    initial_equity: float

    @property
    def model(self) -> bool:
        return self.user_id is None


class AccountSource(Protocol):
    def accounts(self, entry: RegistryEntry) -> Sequence[AccountContext]: ...


class FamilyPipeline(Protocol):
    """What a family's pipeline adapter module exposes as ``PIPELINE``."""
    family_id: str
    timeframe: str

    def due_key(self, now_ms: int) -> str: ...
    def key_after(self, key: str) -> str: ...
    def decision_time_ms(self, key: str) -> int: ...
    def verify(self, research_register: Any) -> Mapping[str, Any]: ...
    def market(self, data: Any, key: str) -> Any: ...
    def account(self, conn: Any, market: Any, account: AccountContext, *, cycle_id: str, now_ms: int,
                entries_blocked: bool, blocked_reason: Optional[str]) -> Mapping[str, Any]: ...


@dataclass
class CycleResult:
    cycle_id: str
    family_id: str
    cycle_key: str
    mode: str
    status: str
    accounts_total: int = 0
    accounts_done: int = 0
    accounts_failed: int = 0
    accounts_skipped: int = 0
    reason: Optional[str] = None
    replayed: bool = False
    timings_ms: Dict[str, float] = field(default_factory=dict)
    account_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def cycle_id_of(family_id: str, cycle_key: str, mode: str) -> str:
    return short_id("cyc", {"family": family_id, "key": cycle_key, "mode": mode})


class PlatformCycleDriver:
    def __init__(self, db: Any, *, accounts: AccountSource, data: Mapping[str, Any],
                 registry: Optional[FamilyRegistry] = None, research_register: Any = None,
                 clock: Optional[Callable[[], int]] = None, owner: str = "driver",
                 account_runner: Optional[Callable[[Sequence[Callable[[], Any]]], List[Any]]] = None,
                 handoff: Any = None, compare_models: bool = False) -> None:
        """``data``: timeframe -> the market-data provider the families of that timeframe read.
        ``account_runner``: runs the per-account callables (bounded parallelism, Step 3.13); sequential by
        default. ``handoff``: the execution hand-off; ``None`` means this driver cannot place an order."""
        if handoff is not None:
            raise ValueError("SHADOW_DRIVER_EXECUTION_HANDOFF_NOT_SUPPORTED")
        self._db = db
        self._accounts = accounts
        self._data = dict(data)
        self._registry = registry if registry is not None else FamilyRegistry(db, research_register=research_register)
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._owner = owner
        self._run_accounts = account_runner or (lambda calls: [c() for c in calls])
        self._handoff = handoff
        self._compare_models = compare_models

    @property
    def registry(self) -> FamilyRegistry:
        return self._registry

    def _scope(self) -> Any:
        """One database connection for everything this thread does inside the block (``DB.scope``), instead of
        one per ``connect()``. Nothing about a transaction changes: every ``connect()`` block still commits when
        it ends and rolls back when it raises, so an account is still exactly one transaction and a failure
        still leaves nothing of it behind. What is saved is opening and closing a connection twice per account,
        which on a WAL database is a checkpoint each time (measured: 96 % of the account stage).

        The one rule this adds: code called inside an open transaction must use the connection it was given.
        A ``connect()`` block nested inside another one would commit the outer transaction when it ends."""
        scope = getattr(self._db, "scope", None)
        return scope() if callable(scope) else nullcontext()

    # ------------------------------------------------------------------------------------------ schedule
    @staticmethod
    def pipeline_of(entry: RegistryEntry) -> FamilyPipeline:
        return import_module(entry.definition.pipeline_adapter).PIPELINE

    def due(self, now_ms: Optional[int] = None) -> List[Tuple[str, str]]:
        """``(family, cycle key)`` to run now, oldest first: unfinished cycles, then missed ones, then the
        one that just became due. Families are whatever the registry says is evaluable; none is named here."""
        now = int(now_ms if now_ms is not None else self._clock())
        out: List[Tuple[str, str]] = []
        for entry in self._registry.evaluable():
            pipeline, family_id = self.pipeline_of(entry), entry.definition.family_id
            with self._db.connect() as conn:
                rows = [dict(r) for r in conn.execute(
                    f"SELECT cycle_key, status FROM {CYCLE_TABLE} WHERE family_id=? AND mode=? ORDER BY cycle_key",
                    (family_id, C.SHADOW))]
            due_key = pipeline.due_key(now)
            completed = {r["cycle_key"] for r in rows if r["status"] in TERMINAL}
            keys: List[str] = []
            if rows:
                # every key from the family's first cycle to now that is not completed, oldest first: an
                # unfinished cycle, a day nobody ran, the one that just became due
                key = min(r["cycle_key"] for r in rows)
                while key <= due_key and len(keys) < MAX_CATCH_UP:
                    if key not in completed:
                        keys.append(key)
                    key = pipeline.key_after(key)
            else:
                keys.append(due_key)            # a family seen for the first time starts now, it does not backfill
            out += [(family_id, k) for k in keys]
        return out

    def run_due(self, now_ms: Optional[int] = None) -> List[CycleResult]:
        """Run everything that is due. A family whose cycle did not complete is not taken further in this
        pass (its next day cannot be accounted before this one); the other families are unaffected."""
        now = int(now_ms if now_ms is not None else self._clock())
        results: List[CycleResult] = []
        held_back = set()
        with self._scope():
            for family_id, key in self.due(now):
                if family_id in held_back:
                    continue
                result = self.run_cycle(family_id, key, now_ms=now)
                results.append(result)
                if result.status != COMPLETED:
                    held_back.add(family_id)
        return results

    # --------------------------------------------------------------------------------------------- cycle
    def run_cycle(self, family_id: str, cycle_key: str, *, now_ms: Optional[int] = None) -> CycleResult:
        with self._scope():
            return self._run_cycle(family_id, cycle_key, now_ms=now_ms)

    def _run_cycle(self, family_id: str, cycle_key: str, *, now_ms: Optional[int] = None) -> CycleResult:
        now = int(now_ms if now_ms is not None else self._clock())
        mode = C.SHADOW
        cycle_id = cycle_id_of(family_id, cycle_key, mode)
        result = CycleResult(cycle_id=cycle_id, family_id=family_id, cycle_key=cycle_key, mode=mode, status=STARTED)
        entry = self._registry.entry(family_id)
        if entry is None or C.SHADOW not in entry.allowed_modes:
            result.status, result.reason = FAILED, "FAMILY_NOT_EVALUABLE:" + ",".join(entry.blockers if entry else ("FAMILY_UNKNOWN",))
            return result                       # nothing is recorded for a family the registry does not offer
        pipeline = self.pipeline_of(entry)
        begun = self._begin(cycle_id, entry, cycle_key, mode, now)
        if begun == "ALREADY_COMPLETED":
            return self._stored(result, replayed=True)
        if begun == "OWNED_ELSEWHERE":
            result.status, result.reason = STARTED, "CYCLE_RUNNING_ELSEWHERE"
            return result
        timer = _Timer()
        try:
            with timer("VERIFY_MANDATE"):
                pinned = pipeline.verify(self._registry._research())
            if pinned.get("rule_artifact_hash", entry.definition.rule_hash) != entry.definition.rule_hash:
                raise _CycleFailure("FAMILY_RULE_HASH_MISMATCH")
        except _CycleFailure as exc:
            return self._finish(result, FAILED, str(exc), timer, now)
        except Exception as exc:  # noqa: BLE001 -- a family whose mandate cannot be verified decides nothing
            return self._finish(result, FAILED, f"MANDATE_VERIFICATION_FAILED:{type(exc).__name__}", timer, now)
        try:
            with timer("MARKET_STAGE"):
                market = pipeline.market(self._data.get(entry.definition.timeframe), cycle_key)
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "code", None)
            if code:
                return self._finish(result, WAITING_FOR_DATA, code, timer, now)
            return self._finish(result, FAILED, f"MARKET_STAGE_FAILED:{type(exc).__name__}", timer, now)
        summary = market.summary()
        self._update(cycle_id, status=MARKET_DECIDED, data_cutoff=cycle_key, market_hash=stable_hash(summary),
                     rule_hash=entry.definition.rule_hash, heartbeat_at=self._clock(),
                     detail=json.dumps({"market": {k: summary[k] for k in ("data_snapshot_id", "freshness", "stages")},
                                        "universe": len(summary["universe"]), "driver_version": DRIVER_VERSION},
                                       sort_keys=True, default=str))
        with timer("ACCOUNT_LIST"):
            accounts = list(self._accounts.accounts(entry))
            self._enrol(cycle_id, accounts)
        controls = self._controls(entry)

        def one(account: AccountContext) -> Callable[[], Tuple[str, Dict[str, Any]]]:
            return lambda: (account.account_id, self._account(cycle_id, entry, pipeline, market, account, controls, now))

        with timer("ACCOUNT_STAGE"):
            outcomes = self._run_accounts([one(a) for a in accounts])
        for account_id, outcome in outcomes:
            result.account_results[account_id] = outcome
        counts = self._counts(cycle_id)
        result.accounts_total, result.accounts_done = counts["total"], counts[DONE]
        result.accounts_failed, result.accounts_skipped = counts[ACCOUNT_FAILED], counts[SKIPPED]
        status = COMPLETED if counts[ACCOUNT_FAILED] == 0 and counts[PENDING] == 0 else PARTIAL
        if status == COMPLETED and self._compare_models and callable(getattr(pipeline, "compare_models", None)):
            try:
                with timer("SHADOW_COMPARISON"):
                    pipeline.compare_models(self._db, market, [a for a in accounts if a.model], now_ms=now)
            except Exception as exc:
                return self._finish(result, FAILED, f"SHADOW_COMPARISON_FAILED:{type(exc).__name__}", timer, now)
        return self._finish(result, status, None if status == COMPLETED else "ACCOUNTS_FAILED", timer, now)

    # ------------------------------------------------------------------------------------------ accounts
    def _controls(self, entry: RegistryEntry) -> Dict[str, Any]:
        from app.trading_intelligence.governance.promotion import PromotionGovernance

        gov = PromotionGovernance(self._db)
        return {"platform": gov.kill_switch_on(), "family": entry.kill_switch_state == "ON", "governance": gov}

    def _account(self, cycle_id: str, entry: RegistryEntry, pipeline: FamilyPipeline, market: Any,
                 account: AccountContext, controls: Mapping[str, Any], now: int) -> Dict[str, Any]:
        """One account, one transaction. Any failure rolls the whole account back and is recorded on its
        cycle row in a second, separate transaction; it never touches another account."""
        with self._scope():                 # in a worker thread of ``account_runner``: one connection for this account
            return self._account_in_scope(cycle_id, entry, pipeline, market, account, controls, now)

    def _account_in_scope(self, cycle_id: str, entry: RegistryEntry, pipeline: FamilyPipeline, market: Any,
                          account: AccountContext, controls: Mapping[str, Any], now: int) -> Dict[str, Any]:
        started = time.monotonic()
        d = entry.definition
        blocked = PLATFORM_KILL if controls["platform"] else FAMILY_KILL if controls["family"] else None
        if blocked is None and not account.model and controls["governance"].kill_switch_on(scope=account.account_id):
            blocked = ACCOUNT_KILL
        mode = self._mode_for(entry, account)
        try:
            with self._db.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(f"SELECT status, decision_id FROM {CYCLE_ACCOUNT_TABLE} WHERE cycle_id=? AND account_id=?",
                                   (cycle_id, account.account_id)).fetchone()
                if row is not None and row[0] == DONE:
                    return {"status": DONE, "decision_id": row[1], "replayed": True}
                out = pipeline.account(conn, market, account, cycle_id=cycle_id, now_ms=now,
                                       entries_blocked=blocked is not None, blocked_reason=blocked)
                if out.get("skipped"):
                    self._mark(conn, cycle_id, account.account_id, SKIPPED, None, started, {"reason": out["skipped"]})
                    return {"status": SKIPPED, "reason": out["skipped"]}
                decision_time = pipeline.decision_time_ms(market.cycle_key)
                expires = decision_time + d.decision_ttl_ms
                # construction is the validation: a decision that violates the trading contracts is not stored
                C.IntentBatch(
                    provenance=C.DecisionProvenance(
                        family_id=d.family_id, family_version=d.family_version, mandate_id=d.mandate_id or "NONE",
                        hypothesis_id=d.hypothesis_id or "NONE", rule_hash=d.rule_hash,
                        decision_id=DecisionStore.decision_id(cycle_id, account.account_id),
                        decision_hash="validated-before-recording",
                        decision_time_ms=decision_time, data_snapshot_id=market.data_snapshot_id,
                        timeframe=d.timeframe, mode=mode),
                    account=C.AccountRef(account.user_id or "MODEL", account.account_id, account.venue,
                                         "DEMO" if account.model else account.environment),
                    intents=tuple(out["intents"]), constraints=out["constraints"],
                    instruments=frozenset(out["instruments"]), created_at_ms=decision_time, expires_at_ms=expires)
                document = dict(out["document"])
                document["mode"] = mode
                document["order_authority"] = {"mode": mode, "blockers": list(entry.blockers),
                                               "handoff": "NONE" if self._handoff is None else "CONFIGURED"}
                stages = dict(document.get("stages") or {})
                stages["EXECUTION_BOUNDARY"] = {
                    "status": "SHADOW_ONLY" if mode == C.SHADOW else "HANDED_TO_BOUNDARY",
                    "reason": "no order authority: " + ", ".join(entry.blockers) if entry.blockers else "model account"
                    if account.model else "no execution hand-off is configured"}
                stages["PERSISTENCE_AND_MONITORING"] = {"status": "EXECUTED", "cycle_id": cycle_id}
                document["stages"] = stages
                recorded = DecisionStore.record_on(
                    conn, cycle_id=cycle_id, family_id=d.family_id, family_version=d.family_version,
                    rule_hash=d.rule_hash, mandate_id=d.mandate_id, account_id=account.account_id,
                    user_id=account.user_id, environment=account.environment, mode=mode, decision_time=decision_time,
                    data_cutoff=market.cycle_key, expires_at=expires, document=document, now_ms=now)
                self._mark(conn, cycle_id, account.account_id, DONE, recorded["decision_id"], started,
                           {"intents": len(out["intents"]), "mode": mode, "blocked": blocked,
                            "decision_hash": recorded["decision_hash"]})
                return {"status": DONE, "decision_id": recorded["decision_id"], "decision_hash": recorded["decision_hash"],
                        "mode": mode, "intents": len(out["intents"]), "blocked": blocked, "replayed": recorded["replayed"]}
        except Exception as exc:  # noqa: BLE001 -- isolation: this account failed, the cycle goes on
            code = getattr(exc, "code", None) or ("DECISION_CONFLICT" if isinstance(exc, DecisionConflict) else type(exc).__name__)
            with self._db.connect() as conn:
                self._mark(conn, cycle_id, account.account_id, ACCOUNT_FAILED, None, started,
                           {"error": code, "message": str(exc)[:300]})
            return {"status": ACCOUNT_FAILED, "error": code}

    def _mode_for(self, entry: RegistryEntry, account: AccountContext) -> str:
        """SHADOW unless the registry gives this family order authority in the account's environment AND a
        hand-off exists. A model account is always shadow."""
        if account.model or self._handoff is None:
            return C.SHADOW
        return account.environment if account.environment in entry.allowed_modes else C.SHADOW

    # ------------------------------------------------------------------------------------- cycle storage
    def _begin(self, cycle_id: str, entry: RegistryEntry, cycle_key: str, mode: str, now: int) -> str:
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(f"SELECT status, owner, heartbeat_at FROM {CYCLE_TABLE} WHERE cycle_id=?", (cycle_id,)).fetchone()
            wall = self._clock()
            if row is None:
                conn.execute(
                    f"INSERT INTO {CYCLE_TABLE} (cycle_id, family_id, cycle_key, mode, status, attempt, owner, started_at, "
                    "heartbeat_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (cycle_id, entry.definition.family_id, cycle_key, mode, STARTED, 1, self._owner, wall, wall))
                return "BEGUN"
            status, owner, heartbeat = row[0], row[1], int(row[2])
            if status in TERMINAL:
                return "ALREADY_COMPLETED"
            if status in (STARTED, MARKET_DECIDED) and owner != self._owner and wall - heartbeat < HEARTBEAT_STALE_MS:
                return "OWNED_ELSEWHERE"
            conn.execute(f"UPDATE {CYCLE_TABLE} SET status=?, attempt=attempt+1, owner=?, heartbeat_at=? WHERE cycle_id=?",
                         (STARTED, self._owner, wall, cycle_id))
            return "RESUMED"

    def _update(self, cycle_id: str, **fields: Any) -> None:
        sets = ", ".join(f"{k}=?" for k in fields)
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_owner(conn, cycle_id)
            conn.execute(f"UPDATE {CYCLE_TABLE} SET {sets} WHERE cycle_id=?", (*fields.values(), cycle_id))

    def _assert_owner(self, conn: Any, cycle_id: str) -> None:
        row = conn.execute(f"SELECT owner FROM {CYCLE_TABLE} WHERE cycle_id=?", (cycle_id,)).fetchone()
        if row is None or row[0] != self._owner:
            raise _CycleFailure("CYCLE_OWNERSHIP_LOST")

    def _enrol(self, cycle_id: str, accounts: Iterable[AccountContext]) -> None:
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_owner(conn, cycle_id)
            conn.executemany(f"INSERT OR IGNORE INTO {CYCLE_ACCOUNT_TABLE} (cycle_id, account_id, status) VALUES (?,?,?)",
                             [(cycle_id, a.account_id, PENDING) for a in accounts])

    def _mark(self, conn: Any, cycle_id: str, account_id: str, status: str, decision_id: Optional[str],
              started: float, detail: Mapping[str, Any]) -> None:
        self._assert_owner(conn, cycle_id)
        now = self._clock()
        conn.execute(
            f"INSERT INTO {CYCLE_ACCOUNT_TABLE} (cycle_id, account_id, status, attempts, decision_id, started_at, "
            "completed_at, duration_ms, detail) VALUES (?,?,?,1,?,?,?,?,?) ON CONFLICT(cycle_id, account_id) DO UPDATE SET "
            "status=excluded.status, attempts=attempts+1, decision_id=excluded.decision_id, "
            "completed_at=excluded.completed_at, duration_ms=excluded.duration_ms, detail=excluded.detail",
            (cycle_id, account_id, status, decision_id, now, now, int((time.monotonic() - started) * 1000),
             json.dumps(dict(detail), sort_keys=True, default=str)))

    def _counts(self, cycle_id: str) -> Dict[str, int]:
        counts = {PENDING: 0, DONE: 0, ACCOUNT_FAILED: 0, SKIPPED: 0, "total": 0}
        with self._db.connect() as conn:
            for status, n in conn.execute(f"SELECT status, COUNT(*) FROM {CYCLE_ACCOUNT_TABLE} WHERE cycle_id=? GROUP BY status",
                                          (cycle_id,)):
                counts[status] = counts.get(status, 0) + int(n)
                counts["total"] += int(n)
        return counts

    def _finish(self, result: CycleResult, status: str, reason: Optional[str], timer: "_Timer", now: int) -> CycleResult:
        result.status, result.reason, result.timings_ms = status, reason, dict(timer.durations)
        wall = self._clock()
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_owner(conn, result.cycle_id)
            conn.execute(
                f"UPDATE {CYCLE_TABLE} SET status=?, heartbeat_at=?, completed_at=?, accounts_total=?, accounts_done=?, "
                "accounts_failed=? WHERE cycle_id=?",
                (status, wall, wall if status in TERMINAL else None, result.accounts_total, result.accounts_done,
                 result.accounts_failed, result.cycle_id))
            if reason:
                row = conn.execute(f"SELECT detail FROM {CYCLE_TABLE} WHERE cycle_id=?", (result.cycle_id,)).fetchone()
                detail = json.loads(row[0]) if row and row[0] else {}
                detail["reason"] = reason
                conn.execute(f"UPDATE {CYCLE_TABLE} SET detail=? WHERE cycle_id=?",
                             (json.dumps(detail, sort_keys=True, default=str), result.cycle_id))
            conn.executemany(
                f"INSERT OR REPLACE INTO {CYCLE_METRIC_TABLE} (cycle_id, stage, account_id, duration_ms, items, recorded_at) "
                "VALUES (?,?,?,?,?,?)",
                [(result.cycle_id, stage, "", ms, result.accounts_total if stage == "ACCOUNT_STAGE" else 0, wall)
                 for stage, ms in timer.durations.items()])
        return result

    def _stored(self, result: CycleResult, *, replayed: bool) -> CycleResult:
        with self._db.connect() as conn:
            row = dict(conn.execute(f"SELECT * FROM {CYCLE_TABLE} WHERE cycle_id=?", (result.cycle_id,)).fetchone())
        result.status, result.replayed = row["status"], replayed
        result.accounts_total, result.accounts_done, result.accounts_failed = (
            row["accounts_total"], row["accounts_done"], row["accounts_failed"])
        return result

    def status(self) -> List[Dict[str, Any]]:
        """The latest cycle of every family and mode, for operators."""
        with self._db.connect() as conn:
            return [dict(r) for r in conn.execute(
                f"SELECT c.* FROM {CYCLE_TABLE} c JOIN (SELECT family_id, mode, MAX(cycle_key) AS k FROM {CYCLE_TABLE} "
                "GROUP BY family_id, mode) m ON m.family_id=c.family_id AND m.mode=c.mode AND m.k=c.cycle_key "
                "ORDER BY c.family_id")]


class _CycleFailure(RuntimeError):
    pass


class _Timer:
    def __init__(self) -> None:
        self.durations: Dict[str, float] = {}

    def __call__(self, stage: str) -> "_Timer":
        self._stage = stage
        return self

    def __enter__(self) -> "_Timer":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.durations[self._stage] = self.durations.get(self._stage, 0.0) + (time.perf_counter() - self._t0) * 1000.0


__all__ = ["DRIVER_VERSION", "STARTED", "MARKET_DECIDED", "COMPLETED", "PARTIAL", "WAITING_FOR_DATA", "FAILED",
           "PENDING", "DONE", "ACCOUNT_FAILED", "SKIPPED", "HEARTBEAT_STALE_MS", "MAX_CATCH_UP", "AccountContext",
           "AccountSource", "FamilyPipeline", "CycleResult", "PlatformCycleDriver", "cycle_id_of", "PLATFORM_KILL",
           "ACCOUNT_KILL", "FAMILY_KILL"]
