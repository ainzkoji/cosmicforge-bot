"""On-call ownership (Section H Step 8.5).

Rotations are named in code (``ROTATIONS``: which kind of problem goes to which team); the PEOPLE in them exist only
when an administrator assigned them through the admin API. Nothing here seeds, guesses or defaults a person or a
contact. A rotation with no active primary (level 0) member resolves to ``OPERATIONS_OWNER_REQUIRED``, and
``findings()`` reports every such rotation as a CRITICAL configuration finding -- the alerting system is not
healthy while an alert could be routed to nobody.

Levels: 0 = primary, 1 = secondary, 2 = escalation owner. A lookup for a level with nobody assigned falls back to
the nearest lower level that has someone (an escalation still reaches a human when one exists), and reports the
gap.

Every assignment and removal is appended to ``ops_oncall_history`` (append-only in the database).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from shared_lib.persistence.sql import Database

OPERATIONS_OWNER_REQUIRED = "OPERATIONS_OWNER_REQUIRED"
LEVELS = (0, 1, 2)
LEVEL_NAMES = {0: "PRIMARY", 1: "SECONDARY", 2: "ESCALATION_OWNER"}
CONTACT_CHANNELS = ("EMAIL", "TELEGRAM", "WEBHOOK", "PHONE", "OTHER")

#: rotation key -> what it covers. Routing (``alerting.CATEGORIES``) refers only to these keys.
ROTATIONS: Dict[str, str] = {
    "operations-owner": "Accountable owner of production operations; final escalation for every alert.",
    "trading-operations": "Exchange execution, protection, reconciliation and kill switches.",
    "platform-infrastructure": "Workers, leases, database, queues, API, disks, backups and latency.",
    "market-data": "Market data freshness and instrument rules.",
    "security": "Authentication anomalies and governance violation attempts.",
    "billing": "Billing provider webhooks.",
    "research-governance": "Exchange certification expiry and research governance.",
}


class OncallError(ValueError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True)
class Target:
    member_id: str
    rotation_key: str
    person_name: str
    level: int
    contact_channel: str
    contact_address: str

    def public(self) -> Dict[str, Any]:
        return {"member_id": self.member_id, "rotation_key": self.rotation_key, "person_name": self.person_name, "level": self.level,
                "level_name": LEVEL_NAMES[self.level], "contact_channel": self.contact_channel}


@dataclass(frozen=True)
class Resolution:
    rotation_key: str
    requested_level: int
    targets: List[Target]
    resolved_level: Optional[int]
    owner_required: bool
    gap: Optional[str]


class OncallDirectory:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ── administration ────────────────────────────────────────────────────────────────────────────────
    def assign(self, *, rotation_key: str, level: int, person_name: str, contact_channel: str, contact_address: str,
               actor: str, now_ms: Optional[int] = None) -> str:
        if rotation_key not in ROTATIONS:
            raise OncallError("UNKNOWN_ROTATION", f"unknown rotation {rotation_key!r}")
        if level not in LEVELS:
            raise OncallError("INVALID_LEVEL")
        if contact_channel not in CONTACT_CHANNELS:
            raise OncallError("INVALID_CONTACT_CHANNEL")
        person_name, contact_address, actor = (person_name or "").strip(), (contact_address or "").strip(), (actor or "").strip()
        if not person_name or not contact_address:
            raise OncallError("PERSON_AND_CONTACT_REQUIRED", "a named person and a contact address are required")
        if not actor:
            raise OncallError("ACTOR_REQUIRED")
        member_id = uuid.uuid4().hex
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else now_ms
            if conn.one("SELECT 1 FROM ops_oncall_rotations WHERE rotation_key=?", (rotation_key,)) is None:
                conn.execute("INSERT INTO ops_oncall_rotations (rotation_key, description, created_at, created_by) "
                             "VALUES (?,?,?,?)", (rotation_key, ROTATIONS[rotation_key], now, actor))
            conn.execute("INSERT INTO ops_oncall_members (member_id, rotation_key, level, person_name, contact_channel, "
                         "contact_address, assigned_by, assigned_at) VALUES (?,?,?,?,?,?,?,?)",
                         (member_id, rotation_key, level, person_name, contact_channel, contact_address, actor, now))
            conn.execute("INSERT INTO ops_oncall_history (rotation_key, member_id, action, level, person_name, actor, at) "
                         "VALUES (?,?,'ASSIGNED',?,?,?,?)", (rotation_key, member_id, level, person_name, actor, now))
        return member_id

    def remove(self, member_id: str, *, actor: str, now_ms: Optional[int] = None) -> None:
        if not actor:
            raise OncallError("ACTOR_REQUIRED")
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else now_ms
            row = conn.one("SELECT rotation_key, level, person_name FROM ops_oncall_members "
                           "WHERE member_id=? AND removed_at IS NULL", (member_id,))
            if row is None:
                raise OncallError("MEMBER_NOT_FOUND")
            conn.execute("UPDATE ops_oncall_members SET removed_at=?, removed_by=? WHERE member_id=?",
                         (now, actor, member_id))
            conn.execute("INSERT INTO ops_oncall_history (rotation_key, member_id, action, level, person_name, actor, at) "
                         "VALUES (?,?,'REMOVED',?,?,?,?)", (row[0], member_id, row[1], row[2], actor, now))

    # ── lookups ───────────────────────────────────────────────────────────────────────────────────────
    def members(self, rotation_key: Optional[str] = None) -> List[Target]:
        sql = ("SELECT member_id, person_name, level, contact_channel, contact_address, rotation_key "
               "FROM ops_oncall_members WHERE removed_at IS NULL")
        params: Sequence[Any] = ()
        if rotation_key:
            sql += " AND rotation_key=?"
            params = (rotation_key,)
        with self.db.transaction(write=False) as conn:
            rows = conn.all(sql + " ORDER BY rotation_key, level, assigned_at, member_id", params)
        return [Target(r[0], r[5], r[1], int(r[2]), r[3], r[4]) for r in rows]

    def resolve(self, rotation_key: str, level: int = 0) -> Resolution:
        """Who receives an alert for ``rotation_key`` at escalation ``level``."""
        members = self.members(rotation_key)
        for lvl in range(min(level, max(LEVELS)), -1, -1):
            at = [m for m in members if m.level == lvl]
            if at:
                gap = None if lvl == level else f"NO_{LEVEL_NAMES[level]}_FOR_{rotation_key}"
                return Resolution(rotation_key, level, at, lvl, False, gap)
        return Resolution(rotation_key, level, [], None, True, OPERATIONS_OWNER_REQUIRED)

    def overview(self) -> Dict[str, Any]:
        members = self.members()
        rotations = []
        for key, description in ROTATIONS.items():
            mine = [m for m in members if m.rotation_key == key]
            rotations.append({
                "rotation_key": key, "description": description,
                "status": "ASSIGNED" if any(m.level == 0 for m in mine) else OPERATIONS_OWNER_REQUIRED,
                "members": [m.public() for m in mine],
            })
        findings = self.findings()
        return {"rotations": rotations, "findings": findings,
                "owner_assigned": not any(f["rotation_key"] == "operations-owner" for f in findings),
                "status": "OK" if not findings else OPERATIONS_OWNER_REQUIRED}

    def findings(self) -> List[Dict[str, Any]]:
        """CRITICAL configuration findings: every rotation with no active primary."""
        with self.db.transaction(write=False) as conn:
            staffed = {r[0] for r in conn.all("SELECT DISTINCT rotation_key FROM ops_oncall_members "
                                               "WHERE removed_at IS NULL AND level=0")}
        return [{"code": OPERATIONS_OWNER_REQUIRED, "severity": "CRITICAL", "rotation_key": key,
                 "message": f"OPERATIONS OWNER REQUIRED: rotation '{key}' has no assigned primary; alerts routed "
                            "to it reach no named person."}
                for key in ROTATIONS if key not in staffed]

    def history(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self.db.transaction(write=False) as conn:
            rows = conn.all("SELECT id, rotation_key, member_id, action, level, person_name, actor, at "
                            "FROM ops_oncall_history ORDER BY id DESC LIMIT ?", (int(limit),))
        return [dict(zip(("id", "rotation_key", "member_id", "action", "level", "person_name", "actor", "at"), r))
                for r in rows]
