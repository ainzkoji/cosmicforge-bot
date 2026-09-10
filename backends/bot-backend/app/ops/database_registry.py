"""Phase 11 §23/§24 — classify database files instead of guessing between them.

The active database is whatever ``DATABASE_URL`` resolves to. Full stop. Other
``.db`` files sitting beside it are *registered and labelled*, never ranked by
recency, size or filename similarity, and never selected automatically.

The concrete case this exists for is ``cosmicforge-LAPTOP-5B3QOQDJ.db``: a
3.2 GB OneDrive sync-conflict copy sitting next to the 3.4 GB active file. It
must stay non-authoritative unless an operator explicitly configures it.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ACTIVE = "ACTIVE"
FORENSIC = "FORENSIC"
ARCHIVED = "ARCHIVED"
STALE_COPY = "STALE_COPY"
RESEARCH = "RESEARCH"
BACKUP = "BACKUP"
VALIDATION = "VALIDATION"

#: Filename markers that indicate a file is a copy, not the runtime database.
#: These classify; they never select.
_SYNC_CONFLICT_MARKERS = ("-LAPTOP-", "-DESKTOP-", " (1)", " - Copy", "conflicted copy")
_BACKUP_MARKERS = (".backup", ".bak", "pre_", "_backup")
_RESEARCH_MARKERS = ("research", "shadow", "backtest", "tmp_", "test")
#: A deliberately-created validation database is not a stale copy of anything.
#: ``phase12_paper_validation.db`` was being labelled STALE_COPY, which reads
#: as "leftover" and is the opposite of what it is.
_VALIDATION_MARKERS = ("validation", "phase12", "paper_forward")


def classify(path: Path, *, active_path: Path) -> str:
    """Label a database file. Classification is advisory, never authoritative."""
    if path.resolve() == active_path.resolve():
        return ACTIVE
    name = path.name
    if any(marker in name for marker in _SYNC_CONFLICT_MARKERS):
        # A sync conflict is potential evidence, so it is preserved, not deleted.
        return FORENSIC
    if any(marker in name for marker in _BACKUP_MARKERS):
        return BACKUP
    if any(marker in name.lower() for marker in _VALIDATION_MARKERS):
        return VALIDATION
    if any(marker in name.lower() for marker in _RESEARCH_MARKERS):
        return RESEARCH
    return STALE_COPY


def _schema_version(path: Path) -> int | None:
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2) as conn:
            return int(conn.execute("PRAGMA user_version").fetchone()[0])
    except Exception:
        return None


def register_database_candidates(
    db: Any, *, active_path: str | Path, database_role: str,
) -> list[dict[str, Any]]:
    """Register every database file beside the active one, with its label.

    Emits a warning when more than one candidate exists so the ambiguity is
    visible in the startup log — but the active file is decided by
    configuration, not by this function.
    """
    active = Path(active_path).resolve()
    candidates: list[dict[str, Any]] = []

    try:
        siblings = sorted(active.parent.glob("*.db"))
    except Exception as exc:
        logger.warning("[DATABASE_REGISTRY] could not scan %s: %s", active.parent, exc)
        return []

    for path in siblings:
        try:
            stat = path.stat()
        except Exception:
            continue
        classification = classify(path, active_path=active)
        row = {
            "database_path": str(path.resolve()),
            "classification": classification,
            "database_role": database_role if classification == ACTIVE else None,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "schema_version": _schema_version(path),
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "note": (
                "resolved from DATABASE_URL" if classification == ACTIVE
                else "non-authoritative; never selected automatically"
            ),
        }
        candidates.append(row)
        try:
            with db.connect() as conn:
                columns = ", ".join(row)
                placeholders = ", ".join("?" for _ in row)
                conn.execute(
                    f"INSERT OR REPLACE INTO database_registry ({columns}) VALUES ({placeholders})",
                    tuple(row.values()),
                )
        except Exception as exc:
            logger.warning("[DATABASE_REGISTRY] could not register %s: %s", path, exc)

    non_active = [c for c in candidates if c["classification"] != ACTIVE]
    if non_active:
        print(
            "[DATABASE_REGISTRY_WARNING] "
            f"active={active} role={database_role} "
            f"other_candidates={[(c['database_path'], c['classification']) for c in non_active]} "
            "(none will be selected automatically)"
        )
    return candidates
