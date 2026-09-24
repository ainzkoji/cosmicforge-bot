"""Section 25 promotion governance: persistent, auditable, versioned,
fail-closed phase transitions; M7 allowlist scopes; the CATI new-entry
kill switch; and the runtime authority the execution boundary consults.

* The current phase is the latest APPROVED transition (``M0`` when none):
  nothing else -- no git merge, deploy or flag -- can move it.
* Forward transitions go one phase at a time and must carry the target
  phase's evidence; rollbacks follow ``phases.rollback_targets``.
* Every transition records from/to, requested/approved time, source commit,
  certification run id, policy-freeze hash, reason, scope, a secret-free
  actor reference and the evidence hashes.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload

from .phases import M5_REPLAY_GATES, PHASE_BY_ID, next_phase, phase_index, rollback_targets

GOVERNANCE_VERSION = "1.0.0"
GLOBAL_SCOPE = "GLOBAL"
KILL_NEW_ENTRIES = "KILL_NEW_CATI_ENTRIES"
NO_CAPITAL_ENVIRONMENTS = ("DEMO", "TESTNET", "PAPER")


class GovernanceError(RuntimeError):
    pass


def scope_hash(*, broker_account_id: str, venue: str, environment: str, asset_class: Optional[str] = None,
               policy_hash: Optional[str] = None) -> str:
    return stable_hash({"broker_account_id": broker_account_id, "venue": str(venue).upper(),
                        "environment": str(environment).upper(), "asset_class": asset_class,
                        "policy_hash": policy_hash})[:32]


class PromotionGovernance:
    PHASES, SCOPES, CONTROLS = "cati_promotion_phase_history", "cati_promotion_scopes", "cati_governance_controls"

    def __init__(self, db: Any):
        self._db = db

    # ------------------------------------------------------------------ phase
    def history(self) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(f"SELECT * FROM {self.PHASES} ORDER BY recorded_at, rowid")]
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows

    def current_phase(self) -> str:
        approved = [r for r in self.history() if r["approved_at"] is not None]
        return approved[-1]["to_phase"] if approved else "M0"

    def transition(self, to_phase: str, *, reason: str, actor_ref: str, evidence: Optional[Mapping[str, Any]] = None,
                   source_commit: Optional[str] = None, certification_run_id: Optional[str] = None,
                   policy_freeze_hash: Optional[str] = None, scope: str = GLOBAL_SCOPE,
                   now_ms: Optional[int] = None) -> Dict[str, Any]:
        evidence = dict(evidence or {})  # forward transitions still fail closed on missing evidence
        current = self.current_phase()
        rollback = to_phase in rollback_targets(current)
        if to_phase not in PHASE_BY_ID:
            raise GovernanceError(f"unknown phase {to_phase!r}")
        if not rollback and to_phase != next_phase(current):
            raise GovernanceError(f"{current} -> {to_phase}: phases cannot be skipped (next is {next_phase(current)})")
        if not rollback:
            missing = self._missing_evidence(to_phase, evidence, certification_run_id, policy_freeze_hash)
            if missing:
                raise GovernanceError(f"{current} -> {to_phase} refused: missing/failed evidence {missing}")
            if not source_commit:
                raise GovernanceError("a forward transition must record the source commit it authorizes")
        if not reason or not actor_ref:
            raise GovernanceError("every transition records a reason and an actor reference")
        ts = int(now_ms or time.time() * 1000)
        payload = sanitize_payload(json.loads(json.dumps({
            "from_phase": current, "to_phase": to_phase, "rollback": rollback, "reason": reason, "actor_ref": actor_ref,
            "scope": scope, "evidence": dict(evidence), "evidence_hash": stable_hash(dict(evidence)),
            "governance_version": GOVERNANCE_VERSION}, default=str)))
        tid = short_id("phtr", {"from": current, "to": to_phase, "t": ts, "evidence": payload["evidence_hash"]})
        with self._db.connect() as conn:
            conn.execute(
                f"INSERT INTO {self.PHASES} (transition_id, from_phase, to_phase, scope_hash, source_commit, "
                "certification_run_id, policy_freeze_hash, requested_at, approved_at, recorded_at, schema_version, "
                "table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (tid, current, to_phase, scope, source_commit, certification_run_id, policy_freeze_hash, ts, ts, ts,
                 GOVERNANCE_VERSION, GOVERNANCE_VERSION, json.dumps(payload, sort_keys=True), stable_hash(payload)))
        return {"transition_id": tid, "from_phase": current, "to_phase": to_phase, "rollback": rollback}

    @staticmethod
    def _missing_evidence(to_phase: str, evidence: Mapping[str, Any], run_id: Optional[str],
                          freeze: Optional[str]) -> List[str]:
        missing = []
        for req in PHASE_BY_ID[to_phase].entry_requirements:
            if req == "certification_run_id":
                ok = bool(run_id)
            elif req == "policy_freeze_hash":
                ok = bool(freeze)
            elif req == "replay_gates_passed":
                gates = evidence.get("replay_gates") or {}
                ok = all(gates.get(g) == "PASS" for g in M5_REPLAY_GATES)
            elif req in ("forward_demo_gate_passed", "operational_gate_passed", "production_evidence_passed",
                         "certification_policy_frozen", "migration_window_elapsed") or req.endswith("_verified"):
                ok = evidence.get(req) is True
            else:
                ok = bool(evidence.get(req))
            if not ok:
                missing.append(req)
        return missing

    # ------------------------------------------------------------------ M7 scopes
    def _scope_event(self, event: str, *, broker_account_id: str, venue: str, environment: str, reason: str,
                     policy_hash: Optional[str], now_ms: Optional[int]) -> str:
        sh = scope_hash(broker_account_id=broker_account_id, venue=venue, environment=environment,
                        policy_hash=policy_hash)
        ts = int(now_ms or time.time() * 1000)
        payload = {"event": event, "reason": reason, "policy_hash": policy_hash}
        with self._db.connect() as conn:
            conn.execute(
                f"INSERT INTO {self.SCOPES} (scope_event_id, scope_hash, event, broker_account_id, venue, environment, "
                "recorded_at, schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (short_id("scev", {"s": sh, "e": event, "t": ts}), sh, event, broker_account_id, str(venue).upper(),
                 str(environment).upper(), ts, GOVERNANCE_VERSION, GOVERNANCE_VERSION, json.dumps(payload),
                 stable_hash(payload)))
        return sh

    def grant_scope(self, *, broker_account_id: str, venue: str, environment: str, reason: str,
                    policy_hash: Optional[str] = None, now_ms: Optional[int] = None) -> str:
        if not broker_account_id or not venue or not environment:
            raise GovernanceError("an M7 scope names an explicit account, venue and environment (no global grant)")
        return self._scope_event("GRANTED", broker_account_id=broker_account_id, venue=venue, environment=environment,
                                 reason=reason, policy_hash=policy_hash, now_ms=now_ms)

    def revoke_scope(self, *, broker_account_id: str, venue: str, environment: str, reason: str,
                     policy_hash: Optional[str] = None, now_ms: Optional[int] = None) -> str:
        return self._scope_event("REVOKED", broker_account_id=broker_account_id, venue=venue, environment=environment,
                                 reason=reason, policy_hash=policy_hash, now_ms=now_ms)

    def scope_granted(self, *, broker_account_id: str, venue: str, environment: str) -> bool:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT event FROM {self.SCOPES} WHERE broker_account_id=? AND venue=? AND environment=? "
                               "ORDER BY recorded_at DESC, rowid DESC LIMIT 1",
                               (broker_account_id, str(venue).upper(), str(environment).upper())).fetchone()
        return bool(row) and row[0] == "GRANTED"

    # ------------------------------------------------------------------ kill switch
    def set_kill_switch(self, on: bool, *, reason: str, actor_ref: str, scope: str = GLOBAL_SCOPE,
                        now_ms: Optional[int] = None) -> None:
        """Disables NEW CATI entries only: open positions keep their protection,
        reconciliation and exits; no evidence is touched."""
        ts = int(now_ms or time.time() * 1000)
        payload = {"reason": reason, "actor_ref": actor_ref}
        with self._db.connect() as conn:
            conn.execute(
                f"INSERT INTO {self.CONTROLS} (control_event_id, control, state, scope, recorded_at, schema_version, "
                "table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                (short_id("ctrl", {"s": scope, "on": on, "t": ts}), KILL_NEW_ENTRIES, "ON" if on else "OFF", scope, ts,
                 GOVERNANCE_VERSION, GOVERNANCE_VERSION, json.dumps(sanitize_payload(payload)), stable_hash(payload)))

    def kill_switch_on(self, scope: Optional[str] = None) -> bool:
        scopes = [GLOBAL_SCOPE] + ([scope] if scope else [])
        with self._db.connect() as conn:
            for s in scopes:
                row = conn.execute(f"SELECT state FROM {self.CONTROLS} WHERE control=? AND scope=? "
                                   "ORDER BY recorded_at DESC, rowid DESC LIMIT 1", (KILL_NEW_ENTRIES, s)).fetchone()
                if row and row[0] == "ON":
                    return True
        return False


class GovernanceAuthority:
    """What the CATI execution boundary asks before any NEW entry (dual key
    with ``CATI_ACTIVE_EXECUTION_ENABLED``: the flag alone authorizes nothing)."""

    def __init__(self, db: Any):
        self.gov = PromotionGovernance(db)

    def authorize_entry(self, plan: Any) -> Tuple[bool, str]:
        env = str(plan.environment).upper()
        if self.gov.kill_switch_on(scope=getattr(plan, "broker_account_id", None)):
            return False, "CATI_NEW_ENTRY_KILL_SWITCH"
        phase = self.gov.current_phase()
        i = phase_index(phase)
        if i < phase_index("M6"):
            return False, f"GOVERNANCE_PHASE_{phase}_NO_CATI_AUTHORITY"
        if env in NO_CAPITAL_ENVIRONMENTS:
            return True, f"{phase}_DEMO_AUTHORITY"
        if phase == "M6":
            return False, "M6_IS_DEMO_ONLY"
        if phase == "M7" and not self.gov.scope_granted(broker_account_id=plan.broker_account_id, venue=plan.venue,
                                                        environment=env):
            return False, "M7_SCOPE_NOT_PROMOTED"
        return True, f"{phase}_AUTHORITY"

    def v2_may_place_orders(self, *, environment: str, broker_account_id: Optional[str] = None,
                            venue: Optional[str] = None) -> bool:
        phase = self.gov.current_phase()
        spec = PHASE_BY_ID[phase]
        if spec.v2_authority == "ACTIVE":
            return True
        if spec.v2_authority == "BENCHMARK_ON_DEMO":
            return str(environment).upper() not in NO_CAPITAL_ENVIRONMENTS
        if spec.v2_authority == "BENCHMARK_ON_PROMOTED_SCOPES":
            env = str(environment).upper()
            if env in NO_CAPITAL_ENVIRONMENTS:
                return False
            return not (broker_account_id and venue and
                        self.gov.scope_granted(broker_account_id=broker_account_id, venue=venue, environment=env))
        return False

    def v2_fallback_allowed(self) -> bool:
        return PHASE_BY_ID[self.gov.current_phase()].v2_fallback_allowed


class StaticAuthority:
    """Test / tooling double with an explicit, fixed verdict."""

    def __init__(self, allowed: bool = True, reason: str = "STATIC"):
        self.allowed, self.reason = allowed, reason

    def authorize_entry(self, plan: Any) -> Tuple[bool, str]:
        return self.allowed, self.reason


__all__ = ["PromotionGovernance", "GovernanceAuthority", "StaticAuthority", "GovernanceError", "scope_hash",
           "KILL_NEW_ENTRIES", "GLOBAL_SCOPE", "GOVERNANCE_VERSION"]
