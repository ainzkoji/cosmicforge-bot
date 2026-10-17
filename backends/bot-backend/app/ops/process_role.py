"""Which half of the bot backend this process is (Section H, Step 5.2).

``PROCESS_ROLE``:

* ``all`` (default -- today's single-process deployment, unchanged): HTTP plus the trading runtime.
* ``trading``: the trading process. Same startup as ``all`` (runner, runtime-ownership lease, CATI production
  runtime, reconciliation, supervisor). It is the only process that may hold the runtime lease and the only one
  that serves the routes in ``TRADING_PROCESS_ROUTES``.
* ``api``: customer/admin HTTP only. It starts **no** runner, never asks for the runtime-ownership lease, starts
  no background worker and no supervisor, and refuses (409) the state-changing routes that act through the
  in-process runtime (emergency flatten/kill switch, runtime and runner controls), so an operator action is never
  accepted by a process that cannot perform it. Its readiness is the database answering.

A restart of an ``api`` process says nothing about the trading runtime; the trading process has its own
supervisor and readiness (``/health?ready=1``).
"""
from __future__ import annotations

import os
from typing import Tuple

ALL, API, TRADING = "all", "api", "trading"
ROLES = (ALL, API, TRADING)

#: State-changing routes that act through the trading process (its PreemptionGate, cycle lock, runner).
TRADING_PROCESS_ROUTES: Tuple[str, ...] = (
    "/api/v1/emergency", "/runner", "/trade", "/debug/run", "/api/v1/admin/trading",
)
_MUTATING = ("POST", "PUT", "PATCH", "DELETE")


class ProcessRoleInvalid(ValueError):
    code = "PROCESS_ROLE_INVALID"


def role() -> str:
    value = (os.environ.get("PROCESS_ROLE") or ALL).strip().lower()
    if value not in ROLES:
        raise ProcessRoleInvalid(f"PROCESS_ROLE={value!r}; expected one of {ROLES}")
    return value


def runs_trading() -> bool:
    return role() in (ALL, TRADING)


def refused_in_api_role(method: str, path: str) -> bool:
    return role() == API and method.upper() in _MUTATING and any(
        path == p or path.startswith(p + "/") for p in TRADING_PROCESS_ROUTES)
