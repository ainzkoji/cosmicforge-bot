"""PolicyFreezeManifest (Section 22.10).

Hashes EVERY policy that shapes a CATI decision -- setup specialists, regime,
forecast (cohort/backoff/posterior), OOD, economic admission, veto, ranking,
portfolio, venue cost, TradePlan, ExitPolicy, replay/label policy and the
certification policy itself -- plus the source commit. Policies that live as
module constants (forecast engine, OOD, cohorts, posterior) are hashed from
those constants, so changing any of them moves the freeze hash.

A HOLDOUT evaluation is bound to one freeze hash. Any policy change is a new
hash, a new experiment, and the old holdout result does not apply to it.
"""
from __future__ import annotations

import types
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Dict, Mapping, Optional

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import POLICY_FREEZE_SCHEMA_VERSION


def _module_constants(module: types.ModuleType) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name, value in vars(module).items():
        if name.isupper() and not name.startswith("_") and isinstance(value, (int, float, str, tuple, bool)):
            out[name] = list(value) if isinstance(value, tuple) else value
    return out


def _policy_payload(policy: Any) -> Any:
    if is_dataclass(policy):
        return asdict(policy)
    return policy


def _hash_of(policy: Any) -> str:
    h = getattr(policy, "policy_hash", None)
    return h if isinstance(h, str) else stable_hash(_policy_payload(policy))


def collect_policy_hashes(*, certification_policy: Any, setup_policies: Optional[Mapping[str, Any]] = None,
                          regime_policy: Any = None, admission_policy: Any = None, veto_policy: Any = None,
                          ranking_policy: Any = None, portfolio_policy: Any = None, venue_cost_policy: Any = None,
                          trade_plan_policy: Any = None, exit_policy: Any = None,
                          research_cost_model: Any = None, label_horizon_bars: int = 48) -> Dict[str, Any]:
    """The frozen defaults unless an experiment explicitly supplies a variant."""
    from app.replay.cost_model import BINANCE_FUTURES_STANDARD
    from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
    from app.trading_intelligence.contracts.position import ExitPolicy
    from app.trading_intelligence.contracts.ranking import RankingPolicy
    from app.trading_intelligence.contracts.trade_plan import TradePlanPolicy
    from app.trading_intelligence.contracts.veto import VetoPolicy
    from app.trading_intelligence.economics.policy import default_admission_policy
    from app.trading_intelligence.forecast import cohorts, engine as fengine, ood, posterior
    from app.trading_intelligence.regime.policy import default_policy as default_regime_policy
    from app.trading_intelligence.setups.policy import default_policies
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY
    from app.trading_intelligence.venue.policy import default_venue_cost_policy
    from app.trading_intelligence.versions import LABEL_POLICY_VERSION

    setups = setup_policies if setup_policies is not None else default_policies()
    cost_model = research_cost_model or BINANCE_FUTURES_STANDARD
    return {
        "setup_specialists": {fam: {"version": spec.setup_version, "policy_hash": _hash_of(setups[fam])}
                              for fam, spec in sorted(SPECIALIST_REGISTRY.items()) if fam in setups},
        "regime_policy": _hash_of(regime_policy or default_regime_policy()),
        "forecast_policy": stable_hash({"engine": _module_constants(fengine), "posterior": _module_constants(posterior)}),
        "cohort_backoff_policy": stable_hash(_module_constants(cohorts)),
        "ood_policy": stable_hash(_module_constants(ood)),
        "economic_admission_policy": _hash_of(admission_policy or default_admission_policy()),
        "veto_policy": _hash_of(veto_policy or VetoPolicy()),
        "ranking_policy": _hash_of(ranking_policy or RankingPolicy()),
        "portfolio_policy": _hash_of(portfolio_policy or PortfolioPolicy()),
        "venue_cost_policy": _hash_of(venue_cost_policy or default_venue_cost_policy()),
        "trade_plan_policy": _hash_of(trade_plan_policy or TradePlanPolicy()),
        "exit_policy": _hash_of(exit_policy or ExitPolicy()),
        "replay_label_policy": stable_hash({"label_policy_version": LABEL_POLICY_VERSION,
                                            "label_horizon_bars": int(label_horizon_bars),
                                            "same_bar_rule": "SL_FIRST_CONSERVATIVE",
                                            "research_cost_model_hash": cost_model.model_hash}),
        "certification_policy": certification_policy.policy_hash,
    }


@dataclass(frozen=True)
class PolicyFreezeManifest:
    policy_hashes: Mapping[str, Any]
    source_commit: Optional[str]
    source_tree_dirty: Optional[bool]
    schema_version: str = POLICY_FREEZE_SCHEMA_VERSION

    def to_dict(self) -> dict:
        return {"policy_hashes": dict(self.policy_hashes), "source_commit": self.source_commit,
                "source_tree_dirty": self.source_tree_dirty, "schema_version": self.schema_version}

    @property
    def freeze_hash(self) -> str:
        # uncommitted code is a DIFFERENT freeze (and never certifiable, see ``certifiable``)
        return stable_hash({"policy_hashes": dict(self.policy_hashes), "source_commit": self.source_commit,
                            "source_tree_dirty": self.source_tree_dirty, "schema_version": self.schema_version})

    @property
    def certifiable(self) -> bool:
        """A freeze bound to uncommitted code cannot anchor a holdout."""
        return bool(self.source_commit) and self.source_tree_dirty is False

    def diff(self, other: "PolicyFreezeManifest") -> Dict[str, Any]:
        a, b = self.to_dict()["policy_hashes"], other.to_dict()["policy_hashes"]
        changed = {k: (a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)}
        if self.source_commit != other.source_commit:
            changed["source_commit"] = (self.source_commit, other.source_commit)
        return changed


def build_policy_freeze(*, certification_policy: Any, source_commit: Optional[str] = None,
                        source_tree_dirty: Optional[bool] = None, **policies: Any) -> PolicyFreezeManifest:
    if source_commit is None and source_tree_dirty is None:
        from app.replay.identity import code_revision

        source_commit, _branch, source_tree_dirty = code_revision()
    return PolicyFreezeManifest(
        policy_hashes=collect_policy_hashes(certification_policy=certification_policy, **policies),
        source_commit=source_commit, source_tree_dirty=source_tree_dirty)


__all__ = ["PolicyFreezeManifest", "build_policy_freeze", "collect_policy_hashes"]
