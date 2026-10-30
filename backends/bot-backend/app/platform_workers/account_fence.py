"""The account fence of the production broker cycle (Section H, Step 5.2).

While a worker processes ONE account under its durable assignment (``assignments.WorkerAssignments``), the fence
-- (account_id, worker_id, fencing_token) -- is held in a context variable of the thread doing that work
(``active``). Every point that is about to mutate the account asks first:

* an entry at the execution boundary (``entry_refusal``), a protective order create / cancel, a reduce-only close,
  the account-management steps of the cycle (``runtime_authority``), the release of the account's own
  reservations (``require`` / ``permits``) and the transport of ANY broker mutation (``require_transport``, called
  from ``shared_lib.core.production.require_broker_mutation_permission``).

What is checked:

* a fence is active for THIS account -> it must still verify on the platform database (same worker, same token,
  not released, not expired) with more than its safety margin of lease left afterwards (a live claim is extended
  in the same transaction; an expired one is never revived). Otherwise ``AssignmentLost`` is raised -- before
  the mutation, never after it.
* a fence is active for ANOTHER account -> ``AssignmentLost``: one account's step never mutates another account.
* no fence is active -> nothing is added by ``require``; ``runtime_authority`` / ``entry_refusal`` then apply
  today's rule (the canonical runtime lease) and, when this process runs the broker cycle on a platform database
  (``broker_assignment.installed()``), additionally refuse an account that another worker holds; an assignment
  that cannot be read is a refusal.

This module never calls a venue.
"""
from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Tuple

from .assignments import AssignmentLost

logger = logging.getLogger(__name__)

#: Lease that must remain after a fence check (the venue call that follows has at least this long).
DEFAULT_MARGIN_MS = 15_000

#: Refusal codes of the no-fence path.
LEASE_REQUIRED = "CANONICAL_RUNTIME_LEASE_REQUIRED"
HELD_BY_ANOTHER_WORKER = "ACCOUNT_ASSIGNED_TO_ANOTHER_WORKER"
ASSIGNMENT_UNREADABLE = "ACCOUNT_ASSIGNMENT_UNREADABLE"


@dataclass(frozen=True)
class AccountFence:
    account_id: str
    worker_id: str
    fencing_token: int
    assignments: Any                      # WorkerAssignments (the platform database it verifies on)
    margin_ms: int = DEFAULT_MARGIN_MS
    #: ``owner`` mode (and an emergency flatten): the canonical runtime lease must still be held as well.
    requires_runtime_lease: bool = False

    @property
    def token(self) -> Tuple[str, str, int]:
        """The tuple form the durable job / intent ledgers take (``WorkerAssignments.verify_on(conn, *token)``)."""
        return (self.account_id, self.worker_id, int(self.fencing_token))

    def verify(self, margin_ms: Optional[int] = None) -> int:
        """Raise ``AssignmentLost`` unless the worker still owns the account with more than the margin left.
        Returns the (extended) expiry on the database clock. An unreadable assignment is a loss."""
        try:
            return self.assignments.verify_fresh(self.account_id, self.worker_id, self.fencing_token,
                                                 margin_ms=self.margin_ms if margin_ms is None else int(margin_ms))
        except AssignmentLost:
            raise
        except Exception as exc:  # noqa: BLE001 -- the platform database did not answer: nothing is proven
            raise AssignmentLost(self.account_id, f"ASSIGNMENT_UNVERIFIABLE:{type(exc).__name__}") from exc


_CURRENT: "contextvars.ContextVar[Optional[AccountFence]]" = contextvars.ContextVar("cf_account_fence", default=None)


def current() -> Optional[AccountFence]:
    """The fence of the account this thread is processing, or None (outside an account step)."""
    return _CURRENT.get()


@contextmanager
def active(fence: Optional[AccountFence]) -> Iterator[Optional[AccountFence]]:
    """Run the block under ``fence`` (None: under no fence, e.g. to leave an enclosing one)."""
    token = _CURRENT.set(fence)
    try:
        yield fence
    finally:
        _CURRENT.reset(token)


def _account_ref(value: Any) -> Optional[str]:
    return str(value) if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value) else None


def require(account_id: Any) -> None:
    """Under a fence: raise ``AssignmentLost`` unless ``account_id`` is the fenced account and the fence still
    verifies. Outside a fence: no-op (today's behaviour)."""
    fence = current()
    if fence is None:
        return
    account = _account_ref(account_id)
    if account != fence.account_id:
        raise AssignmentLost(account or "?", f"OUTSIDE_FENCED_ACCOUNT:{fence.account_id}")
    fence.verify()


def permits(account_id: Any) -> bool:
    """``require`` as a boolean, for cleanup paths that must not replace the exception they are handling."""
    try:
        require(account_id)
        return True
    except Exception:  # noqa: BLE001 -- AssignmentLost or an unreadable fence: not permitted
        return False


def require_transport(client: Any) -> None:
    """The fence at the broker transport: any mutation made while a fence is active must be made with a client
    of the fenced account, and the fence must still verify. Outside a fence: no-op."""
    fence = current()
    if fence is None:
        return
    account = _account_ref(getattr(client, "_production_account_id", None)) or \
        _account_ref(getattr(client, "_broker_account_id", None))
    require(account if account is not None else fence.account_id)


def _lease_of(lease: Optional[Callable[[Any], bool]]) -> Callable[[Any], bool]:
    if lease is not None:
        return lease
    from app.trading_intelligence.integration import residual_prospective
    return residual_prospective.owner_current


def unassigned_or_mine(account_id: Any) -> Optional[str]:
    """Outside a fence: None when no OTHER worker holds a live assignment of ``account_id`` (or this process runs
    no assignment layer); otherwise the refusal code."""
    try:
        from . import broker_assignment
        layer = broker_assignment.installed()
    except Exception:  # noqa: BLE001
        layer = None
    if layer is None or not layer.enforced:
        return None
    try:
        holder = layer.assignments.owner_of(str(account_id))
    except Exception as exc:  # noqa: BLE001 -- unreadable is a refusal
        logger.warning("[ACCOUNT_FENCE] assignment of %s unreadable (%s)", account_id, type(exc).__name__)
        return ASSIGNMENT_UNREADABLE
    if holder is not None and holder[0] != layer.worker_id:
        return HELD_BY_ANOTHER_WORKER
    return None


def runtime_authority(db: Any, account_id: Any, *, lease: Optional[Callable[[Any], bool]] = None) -> bool:
    """May THIS process act on ``account_id`` at a broker now? Replaces ``owner_current(db)`` at the
    account-mutation authority checks.

    Under a fence: the fence must be this account's and verify (``AssignmentLost`` otherwise -- raised); in
    ``owner`` mode the canonical runtime lease must also still be held. Outside a fence: the canonical runtime
    lease, and no other worker holding the account."""
    fence = current()
    if fence is not None:
        require(account_id)
        return bool(_lease_of(lease)(db)) if fence.requires_runtime_lease else True
    if not _lease_of(lease)(db):
        return False
    return unassigned_or_mine(account_id) is None


def entry_refusal(db: Any, account_id: Any, *, lease: Optional[Callable[[Any], bool]] = None) -> Optional[str]:
    """The execution boundary's last authority check before an entry CREATE: None when it may go on, else the
    refusal code. Under a fence it verifies strictly (with margin) and RAISES ``AssignmentLost`` when lost."""
    fence = current()
    if fence is not None:
        require(account_id)
        if fence.requires_runtime_lease and not _lease_of(lease)(db):
            return LEASE_REQUIRED
        return None
    if not _lease_of(lease)(db):
        return LEASE_REQUIRED
    return unassigned_or_mine(account_id)


def fence_for(account_id: Any) -> Optional[Tuple[str, str, int]]:
    """(account_id, worker_id, fencing_token) of ``account_id`` when THIS process's broker-cycle worker holds a
    live assignment of it, else None -- for durable job handlers (``runtime_jobs``), which verify it inside their
    own transaction (``WorkerAssignments.verify_on``) and may run their venue calls under ``activate(token)``."""
    try:
        from . import broker_assignment
        layer = broker_assignment.installed()
    except Exception:  # noqa: BLE001
        return None
    return None if layer is None else layer.fence_for(account_id)


@contextmanager
def activate(token: Tuple[str, str, int]) -> Iterator[AccountFence]:
    """Run a block (a job handler's venue work) under the fence ``token`` of this process's broker-cycle worker."""
    from . import broker_assignment
    layer = broker_assignment.installed()
    if layer is None:
        raise AssignmentLost(str(token[0]), "NO_ASSIGNMENT_LAYER")
    fence = layer.fence_from_token(token)
    fence.verify()
    with active(fence):
        yield fence


__all__ = ["AccountFence", "AssignmentLost", "DEFAULT_MARGIN_MS", "active", "activate", "current", "entry_refusal",
           "fence_for", "permits", "require", "require_transport", "runtime_authority", "unassigned_or_mine"]
