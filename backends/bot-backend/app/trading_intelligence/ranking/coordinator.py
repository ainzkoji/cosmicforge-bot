"""CATICycleCoordinator (Section 15.4) -- the cycle-level collector.

Lifecycle for ONE bot cycle::

    begin_bot_cycle(...)                 # opens the batch
    mark_due(cycle, instrument)          # a new-entry candle was claimed
    record_symbol_evaluation(cycle, ev)  # terminal CATI state for that symbol
    ...                                  # (never ranks / reserves / selects)
    finalize_bot_cycle(cycle)            # completeness proof -> rank ONCE

Ranking and any portfolio finalizer run ONLY inside ``finalize_bot_cycle``,
and only when every due instrument reached a terminal state with no
component failure (V1 default: incomplete batches are never ranked -- ranking
a surviving subset would reintroduce first-come selection bias). Nothing in
this module reserves a slot/capital/margin or submits an order.
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from app.trading_intelligence.contracts.ranking import (
    BATCH_COMPLETE, BATCH_INCOMPLETE, BotCycleEvaluationBatch, EvaluatedOpportunity, RankedOpportunity,
    RankingPolicy, SymbolEvalKind, SymbolEvaluation,
)
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.ranking.engine import rank_opportunities
from app.trading_intelligence.versions import BATCH_SCHEMA_VERSION

_MAX_RETAINED = 64


def universe_hash(symbols: Sequence[str]) -> str:
    return stable_hash(sorted({s.upper() for s in symbols}))


@dataclass
class _OpenCycle:
    user_id: Optional[str]
    bot_instance_id: str
    broker_account_id: Optional[str]
    run_id: Optional[str]
    cycle_id: str
    cycle_time: int
    universe_version: str
    universe_hash: str
    due: Set[str] = field(default_factory=set)
    records: Dict[str, SymbolEvaluation] = field(default_factory=dict)
    finalized: bool = False


@dataclass(frozen=True)
class CycleResult:
    batch: BotCycleEvaluationBatch
    ranked: Tuple[RankedOpportunity, ...]
    excluded_economic_opportunity_ids: Tuple[str, ...]
    ranking_performed: bool
    evaluations: Tuple[SymbolEvaluation, ...]
    #: Whatever the injected portfolio finalizer returned (Section 16), else None.
    portfolio: Any = None


class CATICycleCoordinator:
    def __init__(
        self,
        *,
        ranking_policy: Optional[RankingPolicy] = None,
        benign_error_reasons: Sequence[str] = (),
        portfolio_finalizer: Optional[Callable[[BotCycleEvaluationBatch, Tuple[RankedOpportunity, ...], Tuple[SymbolEvaluation, ...]], Any]] = None,
    ) -> None:
        self._policy = ranking_policy or RankingPolicy()
        #: Explicitly classified benign exclusions (V1 default: none => fail closed).
        self._benign = frozenset(benign_error_reasons)
        self._portfolio_finalizer = portfolio_finalizer
        self._open: Dict[Tuple[str, str], _OpenCycle] = {}
        self._done: "OrderedDict[Tuple[str, str], CycleResult]" = OrderedDict()
        self._lock = threading.RLock()
        #: Test/audit visibility: how many times ranking actually ran.
        self.ranking_calls = 0

    # -- lifecycle ---------------------------------------------------------------
    def begin_bot_cycle(
        self, *, bot_instance_id: str, cycle_id: str, cycle_time_ms: int, user_id: Optional[str] = None,
        broker_account_id: Optional[str] = None, run_id: Optional[str] = None,
        universe_version: str = "broker_universe", universe_symbols: Sequence[str] = (),
    ) -> Tuple[str, str]:
        key = (bot_instance_id, cycle_id)
        with self._lock:
            if key in self._open or key in self._done:
                raise ValueError(f"cycle already begun: {key}")
            self._open[key] = _OpenCycle(
                user_id=user_id, bot_instance_id=bot_instance_id, broker_account_id=broker_account_id, run_id=run_id,
                cycle_id=cycle_id, cycle_time=cycle_time_ms, universe_version=universe_version,
                universe_hash=universe_hash(universe_symbols),
            )
        return key

    def _cycle(self, key) -> _OpenCycle:
        cyc = self._open.get(key)
        if cyc is None:
            raise KeyError(f"no open cycle {key}")
        return cyc

    def mark_due(self, key: Tuple[str, str], instrument: str) -> None:
        """A new-entry candle was claimed for ``instrument``: it now MUST reach
        a terminal state before the batch can be ranked."""
        with self._lock:
            self._cycle(key).due.add(instrument.upper())

    def record_symbol_evaluation(self, key: Tuple[str, str], evaluation: SymbolEvaluation) -> None:
        """Records a terminal state. Deliberately incapable of ranking,
        reserving or selecting -- it only stores the evaluation."""
        with self._lock:
            cyc = self._cycle(key)
            inst = evaluation.instrument.upper()
            if evaluation.kind == SymbolEvalKind.NOT_DUE.value:
                cyc.due.discard(inst)
                cyc.records.pop(inst, None)
                return
            cyc.due.add(inst)
            cyc.records[inst] = evaluation

    def record_symbol_failure(self, key: Tuple[str, str], instrument: str, reason: str) -> None:
        self.record_symbol_evaluation(
            key, SymbolEvaluation(instrument.upper(), SymbolEvalKind.CATI_COMPONENT_ERROR.value, error=reason),
        )

    def finalize_bot_cycle(self, key: Tuple[str, str]) -> CycleResult:
        """Rank the whole batch once (idempotent). Section 21: stage latency +
        bounded ranking metrics are recorded here, so shadow, replay and test
        pipelines emit identically."""
        import time as _time

        first = key not in self._done
        t0 = _time.perf_counter()
        result = self._finalize_bot_cycle(key)
        if first:
            try:
                from app.trading_intelligence.observability.emitters import observe_ranking
                from app.trading_intelligence.observability.metrics import METRICS

                METRICS.observe("cati_stage_latency_ms", (_time.perf_counter() - t0) * 1000.0, stage="RANKING")
                observe_ranking(result)
            except Exception:
                pass
        return result

    def _finalize_bot_cycle(self, key: Tuple[str, str]) -> CycleResult:
        with self._lock:
            if key in self._done:
                return self._done[key]  # idempotent: never re-ranks
            cyc = self._cycle(key)
            cyc.finalized = True
            expected = tuple(sorted(cyc.due))
            missing = tuple(i for i in expected if i not in cyc.records)
            errored = tuple(i for i in expected if i in cyc.records
                            and cyc.records[i].kind == SymbolEvalKind.CATI_COMPONENT_ERROR.value
                            and (cyc.records[i].error or "").split(":")[0] not in self._benign)
            failed = tuple(sorted(set(missing) | set(errored)))
            completed = tuple(i for i in expected if i in cyc.records and i not in failed)
            reasons: List[str] = []
            if missing:
                reasons.append(f"{BATCH_INCOMPLETE}:MISSING_TERMINAL_STATE")
            if errored:
                reasons.append(f"{BATCH_INCOMPLETE}:CATI_COMPONENT_ERROR")
            complete = not failed
            evaluations = tuple(cyc.records[i] for i in expected if i in cyc.records)
            all_ops = [o for e in evaluations for o in e.opportunities]
            approved = tuple(sorted(o.opportunity.economic_opportunity_id for o in all_ops if o.approved))
            watch = tuple(sorted(o.opportunity.economic_opportunity_id for o in all_ops if o.veto.outcome == "WATCH"))
            rejected = tuple(sorted(o.opportunity.economic_opportunity_id for o in all_ops if o.veto.outcome == "REJECT"))
            batch = BotCycleEvaluationBatch(
                cycle_batch_id=BotCycleEvaluationBatch.build_id(
                    bot_instance_id=cyc.bot_instance_id, cycle_id=cyc.cycle_id, universe_hash=cyc.universe_hash,
                    expected=expected, completed=completed, failed=failed),
                user_id=cyc.user_id, bot_instance_id=cyc.bot_instance_id, broker_account_id=cyc.broker_account_id,
                run_id=cyc.run_id, cycle_id=cyc.cycle_id, universe_version=cyc.universe_version,
                universe_hash=cyc.universe_hash, decision_time=cyc.cycle_time,
                expected_due_instruments=expected, completed_instruments=completed, failed_instruments=failed,
                approved_opportunity_ids=approved, watch_ids=watch, rejected_ids=rejected,
                batch_complete=complete, reason_codes=tuple(reasons or [BATCH_COMPLETE]),
                batch_schema_version=BATCH_SCHEMA_VERSION,
            )
            ranked: Tuple[RankedOpportunity, ...] = ()
            excluded: Tuple[str, ...] = tuple(sorted(o.opportunity.economic_opportunity_id for o in all_ops))
            portfolio = None
            performed = False
            if complete:
                self.ranking_calls += 1  # exactly once per cycle, only here
                ranked, excluded = rank_opportunities(
                    all_ops, self._policy, bot_instance_id=cyc.bot_instance_id,
                    broker_account_id=cyc.broker_account_id, cycle_id=cyc.cycle_id,
                )
                performed = True
                if ranked and self._portfolio_finalizer is not None:
                    portfolio = self._portfolio_finalizer(batch, ranked, evaluations)
            result = CycleResult(batch, ranked, excluded, performed, evaluations, portfolio)
            del self._open[key]
            self._done[key] = result
            while len(self._done) > _MAX_RETAINED:
                self._done.popitem(last=False)
            return result


__all__ = ["universe_hash", "CycleResult", "CATICycleCoordinator"]
