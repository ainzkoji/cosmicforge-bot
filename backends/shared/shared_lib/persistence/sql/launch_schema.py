"""Platform tables for Steps 6 and 8 (go-live authorization, beta stages, incidents, track record, operations).

Filled in by those steps; kept separate from ``platform_schema`` so each step's migrations are reviewable.
"""
from __future__ import annotations

from typing import List

from .migrations import Migration

MIGRATIONS: List[Migration] = []
