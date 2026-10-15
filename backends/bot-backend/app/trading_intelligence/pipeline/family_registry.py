"""The one authoritative CATI strategy-family registry (Section H, Step 3.2).

A family is known to the platform because it has a **definition** here, and it may do something because of
what three independent sources say about it at the moment of asking:

* the **definition** (this module, code): what the family is, with the hash of the rules the code executes;
* the **research register** (a committed, hash-chained file): whether its hypothesis passed, failed or is
  still open. The registry never stores that answer; it reads it every time, and an unreadable register is
  ``UNKNOWN``, which authorizes nothing;
* the **family governance log** (database): its stage (M0 ... M6), its kill switch, whether it is disabled or
  its certification was revoked, and the definition hash that was registered.

``entry(family_id)`` joins the three into the registry entry the assignment lists (identity, rule hash,
markets, timeframe, direction, position model, certification status, governance stage, allowed modes, entry
and exit contracts, data requirements, activation policy, kill-switch state, evidence reference).

Nothing in the platform decides by a family's *name*. What may be evaluated is ``evaluable()``; what may send
an order is ``allowed_modes`` of the entry, checked again by ``pipeline.provenance`` at the final boundary. A
failed family stays in the registry, visible, with every mode that could reach an exchange removed.

The number of families is not fixed anywhere in this module. The separate invariant that
``setups.registry.SPECIALIST_REGISTRY`` holds exactly four specialists is untouched: those four are listed
here as families because they exist and are evaluated by the forecast pipeline, not counted.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from app.trading_intelligence.governance.phases import phase_index
from app.trading_intelligence.hashing import stable_hash

from . import contracts as C
from .family_governance import DEMO_STAGE, LIVE_STAGE, FamilyGovernance, FamilyState

REGISTRY_VERSION = "1.0.0"

CERTIFIED, NOT_CERTIFIED, FAILED, REVOKED, UNKNOWN = "CERTIFIED", "NOT_CERTIFIED", "FAILED", "REVOKED", "UNKNOWN"
CERTIFICATION_STATUSES: Tuple[str, ...] = (CERTIFIED, NOT_CERTIFIED, FAILED, REVOKED, UNKNOWN)
#: research register hypothesis status -> registry certification status. Anything not listed is UNKNOWN.
_RESEARCH_STATUS: Mapping[str, str] = {"RESEARCH_PASS": CERTIFIED, "FAILED": FAILED, "REJECTED_PRE_HOLDOUT": FAILED,
                                       "REGISTERED": NOT_CERTIFIED}

LONG_ONLY, LONG_SHORT = "LONG_ONLY", "LONG_SHORT"
PORTFOLIO_TARGETS, SINGLE_POSITION = "PORTFOLIO_TARGETS", "SINGLE_POSITION"
#: what a family kill switch does to positions the family already holds
MANAGE_TO_EXIT, CLOSE_ALL = "MANAGE_TO_EXIT", "CLOSE_ALL"
RULE_BASED, FORECAST_BASED = "RULE_BASED_PORTFOLIO_ADMISSION_V1", "SECTION_22_FORECAST_CERTIFICATION"


@dataclass(frozen=True)
class FamilyDefinition:
    family_id: str
    family_version: str
    mandate_id: Optional[str]           # None: the family was never given a registered mandate
    hypothesis_id: Optional[str]        # None: no numbered hypothesis in the research register
    rule_hash: str
    supported_markets: Tuple[Tuple[str, str], ...]   # (venue, product)
    timeframe: str
    trading_direction: str
    position_model: str
    entry_contract: str
    exit_contract: str
    data_requirements: Tuple[str, ...]
    activation_policy: Mapping[str, Any]
    evidence_reference: Tuple[str, ...]
    decision_ttl_ms: int
    implementation: Optional[str] = None  # where the family's rules live in the code
    pipeline_adapter: Optional[str] = None  # module the platform cycle driver runs; None = it cannot run the family
    admission_route: str = FORECAST_BASED
    description: str = ""

    def to_plain(self) -> Dict[str, Any]:
        return C.to_plain(self)

    @property
    def definition_hash(self) -> str:
        return stable_hash(self.to_plain())


@dataclass(frozen=True)
class RegistryEntry:
    definition: FamilyDefinition
    certification_status: str
    certification_detail: Mapping[str, Any]
    governance_stage: str
    allowed_modes: Tuple[str, ...]
    kill_switch_state: str               # OFF | ON
    registered: bool
    disabled: bool
    blockers: Tuple[str, ...] = field(default_factory=tuple)  # why the family may not execute, machine-readable

    @property
    def executable(self) -> bool:
        return any(m in C.EXECUTING_MODES for m in self.allowed_modes)

    def to_plain(self) -> Dict[str, Any]:
        d = self.definition.to_plain()
        d.update(certification_status=self.certification_status, certification_detail=dict(self.certification_detail),
                 governance_stage=self.governance_stage, allowed_modes=list(self.allowed_modes),
                 kill_switch_state=self.kill_switch_state, registered=self.registered, disabled=self.disabled,
                 executable=self.executable, blockers=list(self.blockers), registry_version=REGISTRY_VERSION)
        return d


# ------------------------------------------------------------------------------------------- definitions
def _daily_trend() -> FamilyDefinition:
    from app.trading_intelligence.families.daily_trend import spec

    return FamilyDefinition(
        family_id=spec.FAMILY_ID, family_version=spec.RULES_VERSION, mandate_id=spec.MANDATE_ID,
        hypothesis_id=spec.HYPOTHESIS_ID, rule_hash=stable_hash(dict(spec.RULE_ARTIFACT)),
        supported_markets=(("binance", "USDT_PERPETUAL"),), timeframe="1d", trading_direction=LONG_ONLY,
        position_model=PORTFOLIO_TARGETS,
        entry_contract="EntryIntent at the next daily open; stop = entry x (1 - d), d = 3 x ATR(20) / close in [5 %, 40 %]",
        exit_contract="ExitIntent on signal (strength 0), stop (ratchets up at each close), data gap or drawdown stop; "
                      "no profit target",
        data_requirements=("daily OHLC and quote volume of every in-scope Binance USD-M USDT perpetual",
                           "150 consecutive daily bars before a symbol can signal",
                           "funding records for the shadow book"),
        activation_policy={"shadow_requires": "REGISTERED", "demo_requires": [CERTIFIED, DEMO_STAGE],
                           "live_requires": [CERTIFIED, LIVE_STAGE, "GLOBAL_GOVERNANCE_SCOPE"],
                           "kill_switch_position_policy": MANAGE_TO_EXIT,
                           "sizing_policy_when_executing": "EXECUTABLE (0.40 % per trade, entry stop at most 15 %)"},
        evidence_reference=(spec.SPECIFICATION_PATH, "docs/research/mandate_004/CERTIFICATION_REPORT.md",
                            "docs/research/mandate_004/certification_result.json",
                            "docs/research/registry/research_register.jsonl"),
        decision_ttl_ms=24 * 3_600_000, implementation="app.trading_intelligence.families.daily_trend",
        pipeline_adapter="app.trading_intelligence.pipeline.daily_trend",
        admission_route=RULE_BASED,
        description="Long-or-flat breakout ensemble on daily bars of the 20 most liquid Binance USD-M USDT perpetuals.")


def _residual() -> FamilyDefinition:
    from app.trading_intelligence.integration.residual_prospective import FAMILY, REGISTRY_HASH

    return FamilyDefinition(
        family_id=FAMILY, family_version="mandate-003-frozen", mandate_id="CATI_NEXT_EDGE_DISCOVERY_MANDATE_003",
        hypothesis_id="H-015", rule_hash=REGISTRY_HASH, supported_markets=(("binance", "USDT_PERPETUAL"),),
        timeframe="1h", trading_direction=LONG_SHORT, position_model=SINGLE_POSITION,
        entry_contract="TradePlan: one position, the largest 24h BTC/ETH-residual momentum z-score with |z| >= 2",
        exit_contract="fixed stop, 2.5R target, 48h timeout",
        data_requirements=("hourly bars of the frozen eligible universe", "BTC and ETH hourly bars"),
        activation_policy={"shadow_requires": "REGISTERED", "demo_requires": [CERTIFIED, DEMO_STAGE],
                           "live_requires": [CERTIFIED, LIVE_STAGE, "GLOBAL_GOVERNANCE_SCOPE"],
                           "kill_switch_position_policy": MANAGE_TO_EXIT},
        evidence_reference=("docs/research/cati_alpha_root_cause/next_edge_registry.json",
                            "docs/research/cati_residual_prospective/",
                            "docs/research/registry/research_register.jsonl"),
        decision_ttl_ms=3_600_000,
        implementation="app.trading_intelligence.integration.residual_prospective",
        description="Hourly single-position residual momentum. Failed the Mandate 003 economic gate (hypothesis 15).")


def _specialists() -> List[FamilyDefinition]:
    """The forecast pipeline's setup specialists, as they are registered today (whatever their number)."""
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY

    out = []
    for family_id, specialist in SPECIALIST_REGISTRY.items():
        version = str(getattr(specialist, "version", None) or getattr(specialist, "VERSION", None) or "1")
        out.append(FamilyDefinition(
            family_id=family_id, family_version=version, mandate_id=None, hypothesis_id=None,
            rule_hash=stable_hash({"family": family_id, "class": type(specialist).__qualname__, "version": version}),
            supported_markets=(("binance", "USDT_PERPETUAL"),), timeframe="15m", trading_direction=LONG_SHORT,
            position_model=SINGLE_POSITION, entry_contract="SetupCandidate -> forecast -> TradePlan",
            exit_contract="structural invalidation and target zones",
            data_requirements=("15m market snapshot", "market state", "regime distribution"),
            activation_policy={"shadow_requires": "REGISTERED", "demo_requires": [CERTIFIED, DEMO_STAGE],
                               "live_requires": [CERTIFIED, LIVE_STAGE, "GLOBAL_GOVERNANCE_SCOPE"],
                               "kill_switch_position_policy": MANAGE_TO_EXIT},
            evidence_reference=("docs/research/registry/historical_hypotheses_v1.json#related_records_not_numbered",),
            decision_ttl_ms=900_000, implementation=f"{type(specialist).__module__}.{type(specialist).__qualname__}",
            description="Candidate generator of the forecast line; never given its own mandate, gate or verdict."))
    return out


def definitions() -> Dict[str, FamilyDefinition]:
    """Every family the code implements. Built on call: a family added to the code appears without any
    list being edited elsewhere."""
    found = [_daily_trend(), _residual(), *_specialists()]
    out: Dict[str, FamilyDefinition] = {}
    for d in found:
        if d.family_id in out:
            raise ValueError(f"family {d.family_id} is defined twice")
        if d.timeframe not in C.TIMEFRAMES:
            raise ValueError(f"family {d.family_id}: unknown timeframe {d.timeframe}")
        out[d.family_id] = d
    return out


# -------------------------------------------------------------------------------------------- the registry
class FamilyRegistry:
    """``research_register`` is a ``ResearchRegister`` or ``None``. With ``None`` (every production caller) the
    committed register is opened on every question and must still contain the anchored historical import: a
    register cut short or replaced is not accepted. An injected register (tests, tooling) is checked for an
    intact hash chain and, when ``research_anchor`` is given, for that anchor. ``definitions_source`` is
    injectable so tests can register synthetic families; production callers never pass it."""

    def __init__(self, db: Any, *, research_register: Any = None,
                 definitions_source: Optional[Mapping[str, FamilyDefinition]] = None,
                 research_anchor: Any = None) -> None:
        self._db = db
        self._register = research_register
        self._anchor = research_anchor
        self._definitions = dict(definitions_source) if definitions_source is not None else None
        self.governance = FamilyGovernance(db)

    def definitions(self) -> Dict[str, FamilyDefinition]:
        return dict(self._definitions) if self._definitions is not None else definitions()

    def _research(self) -> Any:
        if self._register is not None:
            return self._register
        from app.trading_intelligence.research.governance.register import ResearchRegister, default_register_path

        return ResearchRegister(default_register_path())

    # -------------------------------------------------------------------------------- certification status
    def research_certification(self, definition: FamilyDefinition) -> Dict[str, Any]:
        """What the research register says about the family's hypothesis, now. Fails closed: an unreadable
        or inconsistent register, or a hypothesis that is not in it, is UNKNOWN."""
        if definition.hypothesis_id is None:
            return {"status": NOT_CERTIFIED, "reason": "NO_REGISTERED_HYPOTHESIS", "research_status": None}
        try:
            register = self._research()
            anchor = self._anchor
            if self._register is None:
                from app.trading_intelligence.research.governance.history import HISTORICAL_ANCHOR as anchor
            register.records(anchor=anchor)                 # raises when the chain or the anchor is broken
        except Exception as exc:  # noqa: BLE001 -- any failure to read evidence authorizes nothing
            return {"status": UNKNOWN, "reason": f"RESEARCH_REGISTER_UNAVAILABLE:{type(exc).__name__}",
                    "research_status": None}
        try:
            hypothesis = register.hypothesis(definition.hypothesis_id)
        except Exception:  # noqa: BLE001
            return {"status": UNKNOWN, "reason": "HYPOTHESIS_NOT_IN_REGISTER", "research_status": None}
        research_status = hypothesis.get("status")
        return {"status": _RESEARCH_STATUS.get(research_status, UNKNOWN), "research_status": research_status,
                "hypothesis_id": definition.hypothesis_id,
                "reason": f"RESEARCH_REGISTER_STATUS_{research_status}",
                "failure_reasons": hypothesis.get("failure_reasons") or []}

    def promotion_prerequisites(self, family_id: str) -> Dict[str, Any]:
        """Everything a promotion decision needs from outside the governance log, verified now."""
        definition = self.definitions().get(family_id)
        if definition is None:
            return {"rule_hash": None, "certification_status": UNKNOWN, "mandate_verified": False, "valid_run": False}
        cert = self.research_certification(definition)
        out: Dict[str, Any] = {"rule_hash": definition.rule_hash, "certification_status": cert["status"],
                               "research_status": cert.get("research_status"), "mandate_verified": False,
                               "valid_run": False, "admission": None, "mandate_id": definition.mandate_id}
        if definition.mandate_id is None:
            return out
        try:
            register = self._research()
            out["mandate_verified"] = self._verify_mandate(definition, register)
            out["valid_run"] = any(r.get("state") == "RUN_COMPLETED" and (r.get("end") or {}).get("status") == "VALID"
                                   for r in register.runs(mandate_id=definition.mandate_id))
            if definition.admission_route == RULE_BASED:
                from app.trading_intelligence.research.governance.admission import evaluate_rule_based_admission

                result = self._certification_result(definition)
                out["admission"] = evaluate_rule_based_admission(
                    register, mandate_id=definition.mandate_id, certification=result)["status"]
        except Exception as exc:  # noqa: BLE001
            out["error"] = type(exc).__name__
            out["mandate_verified"] = False
            if definition.admission_route == RULE_BASED:
                out["admission"] = "NOT_ELIGIBLE"
        return out

    @staticmethod
    def _verify_mandate(definition: FamilyDefinition, register: Any) -> bool:
        """The family's own adapter verifies its mandate (specification, rule set and amendments against the
        pinned hashes). A family without an adapter is verified by the rule hash alone."""
        if definition.pipeline_adapter:
            from importlib import import_module

            pinned = import_module(definition.pipeline_adapter).verify_mandate(register)
            return pinned.get("rule_artifact_hash") == definition.rule_hash
        return register.mandate(definition.mandate_id).get("rule_artifact_hash") == definition.rule_hash

    @staticmethod
    def _certification_result(definition: FamilyDefinition) -> Mapping[str, Any]:
        from app.trading_intelligence.research.governance.register import repository_root

        for reference in definition.evidence_reference:
            if reference.endswith("certification_result.json"):
                path = Path(repository_root()) / reference
                if path.is_file():
                    return json.loads(path.read_text(encoding="utf-8"))
        return {}

    # ------------------------------------------------------------------------------------------ entries
    def entry(self, family_id: str) -> Optional[RegistryEntry]:
        definition = self.definitions().get(family_id)
        if definition is None:
            return None
        return self._entry(definition, self.governance.state(family_id))

    def entries(self) -> List[RegistryEntry]:
        return [self._entry(d, self.governance.state(fid)) for fid, d in sorted(self.definitions().items())]

    def _entry(self, definition: FamilyDefinition, state: FamilyState) -> RegistryEntry:
        cert = self.research_certification(definition)
        status = REVOKED if state.certification_revoked else cert["status"]
        blockers: List[str] = []
        if not state.registered:
            blockers.append("FAMILY_NOT_REGISTERED")
        elif state.rule_hash != definition.rule_hash or state.family_version != definition.family_version:
            blockers.append("FAMILY_RULE_HASH_MISMATCH")
        if state.disabled:
            blockers.append("FAMILY_DISABLED")
        if status != CERTIFIED:
            blockers.append({FAILED: "FAMILY_FAILED_RESEARCH", REVOKED: "FAMILY_CERTIFICATION_REVOKED",
                             NOT_CERTIFIED: "FAMILY_NOT_CERTIFIED"}.get(status, "FAMILY_EVIDENCE_MISSING"))
        if state.kill_switch:
            blockers.append("FAMILY_KILL_SWITCH")
        modes: List[str] = [C.RESEARCH_SIMULATION]
        usable = state.registered and not state.disabled and "FAMILY_RULE_HASH_MISMATCH" not in blockers
        if usable and definition.pipeline_adapter:
            modes.append(C.SHADOW)
        elif not definition.pipeline_adapter:
            blockers.append("NO_PLATFORM_PIPELINE_ADAPTER")
        stage = state.stage
        if usable and status == CERTIFIED and not state.kill_switch:
            if phase_index(stage) >= phase_index(DEMO_STAGE):
                modes.append(C.DEMO)
            if phase_index(stage) >= phase_index(LIVE_STAGE):
                modes.append(C.LIVE)
        if phase_index(stage) < phase_index(DEMO_STAGE):
            blockers.append(f"FAMILY_STAGE_{stage}_NO_ORDER_AUTHORITY")
        return RegistryEntry(definition=definition, certification_status=status, certification_detail=cert,
                             governance_stage=stage, allowed_modes=tuple(modes),
                             kill_switch_state="ON" if state.kill_switch else "OFF", registered=state.registered,
                             disabled=state.disabled, blockers=tuple(blockers))

    def evaluable(self, *, timeframe: Optional[str] = None) -> List[RegistryEntry]:
        """Families the platform cycle may evaluate in shadow: registered, enabled, with a pipeline adapter,
        their code still matching the registration. Selected by registry content only, never by name."""
        return [e for e in self.entries()
                if C.SHADOW in e.allowed_modes and (timeframe is None or e.definition.timeframe == timeframe)]

    def executable(self) -> List[RegistryEntry]:
        return [e for e in self.entries() if e.executable]

    def register_all(self, *, actor_ref: str, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        """Record every code-defined family in the governance log (idempotent). Registration gives a family an
        identity and M0; it gives no certification, no stage and no order authority."""
        return [self.governance.register_family(d, actor_ref=actor_ref, now_ms=now_ms)
                for _, d in sorted(self.definitions().items())]


__all__ = ["REGISTRY_VERSION", "CERTIFIED", "NOT_CERTIFIED", "FAILED", "REVOKED", "UNKNOWN", "CERTIFICATION_STATUSES",
           "LONG_ONLY", "LONG_SHORT", "PORTFOLIO_TARGETS", "SINGLE_POSITION", "MANAGE_TO_EXIT", "CLOSE_ALL", "RULE_BASED",
           "FORECAST_BASED", "FamilyDefinition", "RegistryEntry", "FamilyRegistry", "definitions"]
