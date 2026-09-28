"""Pre-submission revalidation (Sections 18.4, 18.5, 18.12, 18.13).

Immediately before an approved, capital-ready TradePlan may reach the broker adapter:

``instrument(plan, now)`` -- BEFORE hard risk, against the Section 7 instrument catalog:
    * the venue instrument is known, listed and fresh (``last_seen_ms`` within ``max_metadata_age_ms``);
      stale or unknown -> a Section 7 refresh is REQUESTED and, when a ``refresh`` callable is wired, run
      once; still stale / unknown -> ``CAPABILITY_STALE`` / ``INSTRUMENT_UNKNOWN`` (never submitted on old
      assumptions)
    * it is the SAME product the plan was built for (canonical symbol + asset class); an FX / TradFi / CFD
      plan is never pushed through a crypto-perpetual instrument (``PRODUCT_TYPE_MISMATCH``)
    * this ACCOUNT may execute it now (``exchange.instruments.execution_eligibility``: API execution,
      venue-evidenced classification, perpetual contract, account capability) -- reasons such as
      ``VENUE_API_NOT_SUPPORTED`` / ``CONTRACT_TYPE_NOT_SUPPORTED`` (= product type unsupported)
    * its metadata did not change after the plan was built (``CAPABILITY_CHANGED_SINCE_PLAN``): a capability
      decision is never grandfathered

``quantity(plan, quantity, price, now)`` -- AFTER hard-risk sizing: the executable quantity under the
CURRENT metadata (step rounding down, min / max quantity, min notional with contract size). A rounding that
materially changes the risk-sized quantity blocks (``QUANTITY_ROUNDING_MATERIAL``): viability must be
re-established, never assumed.

Pure reads plus an optional refresh; it submits nothing and never mutates a plan.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal
from typing import Any, Callable, Mapping, Optional, Tuple

PREFLIGHT_VERSION = "submission-preflight-v1"
TRIGGER_PRE_SUBMIT_STALE = "PRE_SUBMIT_METADATA_STALE"


@dataclass(frozen=True)
class PreflightResult:
    ok: bool
    reason_codes: Tuple[str, ...] = ()
    quantity: Optional[float] = None
    detail: Mapping[str, Any] = field(default_factory=dict)
    version: str = PREFLIGHT_VERSION


def _d(v: Any) -> Optional[Decimal]:
    try:
        d = Decimal(str(v))
        return d if d.is_finite() else None
    except Exception:
        return None


class SubmissionPreflight:
    def __init__(self, *, catalog: Any, broker: str, venue_key: str, catalog_environment: str,
                 account_environment: str, permissions: Optional[Mapping[str, Any]] = None,
                 max_metadata_age_ms: int = 3_600_000, refresh: Optional[Callable[[], Any]] = None,
                 material_rounding_fraction: float = 0.01) -> None:
        self.catalog, self.broker, self.venue_key = catalog, str(broker).lower(), venue_key
        self.catalog_environment, self.account_environment = catalog_environment, account_environment
        self.permissions = permissions
        self.max_age = int(max_metadata_age_ms)
        self.refresh = refresh
        self.material = float(material_rounding_fraction)

    def _record(self, symbol: str) -> Optional[Mapping[str, Any]]:
        return self.catalog.record(self.venue_key, self.catalog_environment, symbol)

    def _fresh(self, rec: Optional[Mapping[str, Any]], now: int) -> bool:
        return rec is not None and rec.get("last_seen_ms") is not None and 0 <= now - int(rec["last_seen_ms"]) <= self.max_age

    def instrument(self, plan: Any, now: int) -> PreflightResult:
        from app.exchange.catalog_refresh import request_refresh
        from app.exchange.instruments import execution_eligibility

        symbol = plan.instrument_key.venue_symbol.upper()
        rec = self._record(symbol)
        refreshed = None
        if not self._fresh(rec, now):
            request_refresh(self.broker, self.account_environment, TRIGGER_PRE_SUBMIT_STALE, detail=symbol, now_ms=now)
            if self.refresh is not None:
                try:
                    self.refresh()
                    refreshed = "REFRESHED"
                except Exception as exc:  # a failed refresh is never a pass
                    refreshed = f"REFRESH_FAILED:{type(exc).__name__}"
                rec = self._record(symbol)
        detail = {"refresh": refreshed, "catalog": f"{self.venue_key}/{self.catalog_environment}"}
        if rec is None:
            return PreflightResult(False, ("INSTRUMENT_UNKNOWN",), detail=detail)
        if not self._fresh(rec, now):
            return PreflightResult(False, ("CAPABILITY_STALE",), detail=detail)
        if rec.get("delisted_at_ms") is not None:
            return PreflightResult(False, ("INSTRUMENT_DELISTED",), detail=detail)
        ins = rec["instrument"]
        reasons = []
        key = plan.instrument_key
        if ins.canonical_symbol != key.canonical_symbol:
            reasons.append("INSTRUMENT_MAPPING_MISMATCH")
        if str(ins.asset_class) != str(key.asset_class):
            reasons.append("PRODUCT_TYPE_MISMATCH")  # never coerce FX / TradFi / CFD through crypto logic
        ok, why = execution_eligibility(ins, broker=self.broker, environment=self.account_environment,
                                        permissions=self.permissions)
        if not ok:
            reasons += list(why)
        changed = rec.get("metadata_changed_ms")
        if changed is not None and int(changed) > int(plan.decision_time):
            reasons.append("CAPABILITY_CHANGED_SINCE_PLAN")
        return PreflightResult(not reasons, tuple(dict.fromkeys(reasons)),
                               detail={**detail, "metadata_hash": rec.get("metadata_hash")})

    def quantity(self, plan: Any, quantity: float, price: float, now: int) -> PreflightResult:
        rec = self._record(plan.instrument_key.venue_symbol.upper())
        if rec is None or not self._fresh(rec, now):
            return PreflightResult(False, ("CAPABILITY_STALE",))
        ins = rec["instrument"]
        q, px = _d(quantity), _d(price)
        step, lo, hi = _d(ins.qty_step), _d(ins.min_qty), _d(ins.max_qty)
        min_notional, mult = _d(ins.min_notional), _d(ins.contract_multiplier) or Decimal("1")
        if q is None or q <= 0 or px is None or px <= 0 or step is None or step <= 0:
            return PreflightResult(False, ("EXECUTABLE_QUANTITY_UNKNOWN",))
        rounded = (q / step).to_integral_value(rounding=ROUND_DOWN) * step
        reasons = []
        if rounded <= 0 or (lo is not None and rounded < lo):
            reasons.append("QUANTITY_BELOW_MINIMUM")
        if hi is not None and rounded > hi:
            reasons.append("QUANTITY_ABOVE_MAXIMUM")
        if min_notional is not None and rounded * px * mult < min_notional:
            reasons.append("MIN_NOTIONAL_NOT_MET")
        if rounded > 0 and abs(q - rounded) / q > Decimal(str(self.material)):
            reasons.append("QUANTITY_ROUNDING_MATERIAL")
        return PreflightResult(not reasons, tuple(reasons), float(rounded),
                               detail={"requested": str(q), "rounded": str(rounded), "step": str(step)})


__all__ = ["PREFLIGHT_VERSION", "PreflightResult", "SubmissionPreflight", "TRIGGER_PRE_SUBMIT_STALE"]
