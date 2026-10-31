"""Test-only kit of the 1,000-account liquidity-coordination tests (``test_liquidity_at_scale.py``).

A separate module so a spawned process can import it by name (``multiprocessing`` "spawn" context).

Everything here is a TEST DOUBLE or a TEST-ONLY policy used inside a throwaway database:

* ``Book``: a venue client that answers an order-book read and nothing else (the only thing the spread gate asks);
* ``plan``: the fields of a TradePlan the gate reads, for one MARKET entry (what every entry adapter sends);
* ``UNCALIBRATED_MARKET_OK``: an UNCALIBRATED policy that permits market entries (the ``_exec`` harness policy's
  shape) -- the default policy refuses every market entry before the budget is reached, so this is how the
  budget's own calibration boundary is reached at all;
* ``test_only_calibrated``: a policy with ``calibrated=True`` and an approval reference that says it is not an owner
  approval. It exists only so the shared-cap mechanism can be exercised with more than one account; it is built
  only by tests, only against a temporary database (no product code builds one --
  ``test_spread_gate.test_no_product_code_builds_a_calibrated_policy``).
"""
from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from typing import Dict, List, Optional, Sequence

VENUE, SYMBOL, ENV = "binance", "BTCUSDT", "DEMO"
MID = 50_000.0
TEST_ONLY_APPROVAL = "TEST-ONLY-NOT-AN-OWNER-APPROVAL"


class Book:
    """``get_orderbook`` only: ``levels`` levels a side, 0.1 apart, ``qty`` each; venue time = now - ``age_ms``."""

    def __init__(self, mid: float = MID, *, levels: int = 20, qty: str = "5", age_ms: int = 0, crossed: bool = False,
                 step: float = 0.1) -> None:
        self.mid, self.levels, self.qty, self.age_ms, self.crossed, self.step = mid, levels, qty, age_ms, crossed, step
        self.reads = 0

    def get_orderbook(self, symbol, limit=50):
        self.reads += 1
        shift = 2 * self.step if self.crossed else 0.0
        return {"bids": [[f"{self.mid - self.step * (i + 1) + shift:.4f}", self.qty] for i in range(self.levels)],
                "asks": [[f"{self.mid + self.step * i:.4f}", self.qty] for i in range(self.levels)],
                "time": int(time.time() * 1000) - self.age_ms}


def accounts(n: int, prefix: str = "liq") -> List[str]:
    return [f"{prefix}-{i:05d}" for i in range(n)]


def tenant(account: str) -> str:
    return f"tenant-{int(account.rsplit('-', 1)[1]) % 97:03d}"


def register(db, ids: Sequence[str], *, venue: str = VENUE, environment: str = ENV) -> None:
    from app.platform_workers.assignments import WorkerAssignments
    WorkerAssignments(db).upsert_accounts([(a, tenant(a), venue, environment) for a in ids])


def plan(account: str, *, family: str = "LIQ_SCALE_FAMILY", decision_time: int = 1_780_000_000_000,
         venue: str = VENUE, symbol: str = SYMBOL, style: str = "MARKET", max_spread_bps=None):
    return SimpleNamespace(
        trade_plan_id=f"tp-{account}-{family}-{decision_time}", broker_account_id=account, user_id=tenant(account),
        side="LONG", venue=venue, environment=ENV, instrument_key=SimpleNamespace(venue_symbol=symbol),
        setup_family=family, setup_version="1.0.0", decision_time=decision_time, versions=(), mode=ENV,
        source_candidate_id="d", plan_expiry_time=decision_time + 1,
        execution_preferences=SimpleNamespace(preferred_order_style=style, fallback_order_styles=(),
                                              max_spread_bps=max_spread_bps))


def uncalibrated_market_ok():
    from app.trading_intelligence.scale.order_spreading import SpreadPolicy
    return SpreadPolicy(policy_version="test-liquidity-scale-market-entry-uncalibrated", allow_market_fallback=True)


def test_only_calibrated(cap: str = "10000", *, max_orders_per_slot: int = 100_000, **over):
    """TEMP-DB ONLY. See the module note."""
    from app.trading_intelligence.scale.order_spreading import SpreadPolicy
    kw = dict(policy_version="test-only-calibrated-liquidity-mechanism", allow_market_fallback=True,
              absolute_cap_notional=Decimal(cap), max_orders_per_slot=max_orders_per_slot)
    kw.update(over)
    return SpreadPolicy(calibrated=True, approval_ref=TEST_ONLY_APPROVAL, **kw)


POLICIES = {"default": None, "uncalibrated_market_ok": uncalibrated_market_ok, "test_only_calibrated": test_only_calibrated}


def reserve_one(gate, account: str, *, notional: str = "50", qty: str = "0.001", family: str = "LIQ_SCALE_FAMILY",
                client=None, decision_time: int = 1_780_000_000_000, stop_price=None, equity=None,
                max_spread_bps=None) -> str:
    """One account's entry through the gate: ``RESERVED`` or the refusal code."""
    from app.trading_intelligence.scale.spread_gate import SpreadRefused
    p = plan(account, family=family, decision_time=decision_time, max_spread_bps=max_spread_bps)
    try:
        gate.reserve(p, client=client or Book(), adapter=None, quantity=Decimal(qty), notional=Decimal(notional),
                     attempt_id=f"att-{p.trade_plan_id}", client_order_id=f"cid-{p.trade_plan_id}"[:40],
                     tick=Decimal("0.1"), stop_price=stop_price, equity=equity)
        return "RESERVED"
    except SpreadRefused as exc:
        return exc.code


def reserve_grant(gate, account: str, *, notional: str = "50", qty: str = "0.001", family: str = "LIQ_SCALE_FAMILY",
                  client=None, decision_time: int = 1_780_000_000_000):
    """``(code, grant)``: the grant when RESERVED, else ``None``."""
    from app.trading_intelligence.scale.spread_gate import SpreadRefused
    p = plan(account, family=family, decision_time=decision_time)
    try:
        grant = gate.reserve(p, client=client or Book(), adapter=None, quantity=Decimal(qty), notional=Decimal(notional),
                             attempt_id=f"att-{p.trade_plan_id}", client_order_id=f"cid-{p.trade_plan_id}"[:40],
                             tick=Decimal("0.1"))
        return "RESERVED", grant
    except SpreadRefused as exc:
        return exc.code, None


def attempt_id(account: str, family: str = "LIQ_SCALE_FAMILY", decision_time: int = 1_780_000_000_000) -> str:
    return f"att-tp-{account}-{family}-{decision_time}"


def process_worker(url: str, policy: str, ids: Sequence[str], families: Sequence[str], notional: str, start_event,
                   results) -> None:
    """A separate OS process reserving for ``ids`` against the shared platform database."""
    from shared_lib.persistence.sql import Database
    from app.trading_intelligence.scale.spread_gate import PlatformSpreadGate

    db = Database(url, pool_size=2)
    build = POLICIES[policy]
    gate = PlatformSpreadGate(db) if build is None else PlatformSpreadGate(db, policy=build())
    start_event.wait(120)
    out: Dict[str, str] = {}
    try:
        for i, a in enumerate(ids):
            out[a] = reserve_one(gate, a, notional=notional, family=families[i % len(families)])
        results.put(("ok", out))
    except Exception as exc:  # surfaced to the parent, never swallowed
        results.put(("error", f"{type(exc).__name__}: {exc}"))
    finally:
        db.close()


def slot_totals(db, *, venue: str = VENUE, symbol: str = SYMBOL, side: str = "BUY") -> Dict[str, Decimal]:
    """Active (RESERVED / SUBMITTED / FILLED) granted notional on unexpired budgets of one venue/symbol/side."""
    from shared_lib.persistence.sql import decimal_out
    with db.transaction(write=False) as conn:
        now = conn.now_ms()
        rows = conn.all("SELECT s.status, s.granted_notional FROM platform_order_slots s JOIN platform_symbol_budgets b "
                        "ON b.budget_id=s.budget_id WHERE b.venue=? AND b.symbol=? AND b.side=? AND b.expires_at > ? "
                        "AND s.status IN ('RESERVED','SUBMITTED','FILLED')", (venue, symbol, side, now))
    out: Dict[str, Decimal] = {"RESERVED": Decimal(0), "SUBMITTED": Decimal(0), "FILLED": Decimal(0)}
    for status, granted in rows:
        out[status] += decimal_out(granted)
    out["TOTAL"] = sum(out.values(), Decimal(0))
    return out


def reconcile(db, *, cap: Optional[Decimal] = None) -> Dict[str, int]:
    """Cross-account budget reconciliation: the decision links (``platform_spread_decisions``) and the slots
    (``platform_order_slots``) must tell the same story for every account. Returns counts; raises AssertionError
    on the first inconsistency."""
    from shared_lib.persistence.sql import decimal_out
    with db.transaction(write=False) as conn:
        slots = {r[0]: r for r in conn.all("SELECT slot_id, account_id, tenant_id, status, granted_notional, budget_id "
                                           "FROM platform_order_slots")}
        links = conn.all("SELECT decision_id, slot_id, account_id, tenant_id, outcome, settled_status, settled_notional, "
                         "granted_notional, budget_id FROM platform_spread_decisions")
        budgets = {r[0]: (decimal_out(r[1]), int(r[2])) for r in conn.all(
            "SELECT budget_id, cap_notional, expires_at FROM platform_symbol_budgets")}
        now = conn.now_ms()
    open_links: Dict[str, int] = {}
    for decision_id, slot_id, account, tenant_id, outcome, settled, settled_notional, granted, budget in links:
        if outcome == "REFUSED":
            assert slot_id is None, decision_id
            continue
        slot = slots[slot_id]
        assert (slot[1], slot[2], slot[5]) == (account, tenant_id, budget), decision_id
        if settled is None:
            open_links[slot_id] = open_links.get(slot_id, 0) + 1
            assert slot[3] in ("RESERVED", "SUBMITTED", "EXPIRED"), (decision_id, slot[3])
        elif settled == "FILLED":
            assert slot[3] == "FILLED", (decision_id, slot[3])
            if settled_notional is not None:
                assert decimal_out(settled_notional) == decimal_out(slot[4]), decision_id
        else:
            assert slot[3] in ("RELEASED", "EXPIRED"), (decision_id, settled, slot[3])
    assert all(v == 1 for v in open_links.values()), "a slot linked to two open decisions"
    for slot_id, row in slots.items():
        if row[3] in ("RESERVED", "SUBMITTED"):
            assert open_links.get(slot_id) == 1 or slot_id not in {l[1] for l in links}, slot_id
    per_budget: Dict[str, Decimal] = {}
    for row in slots.values():
        if row[3] in ("RESERVED", "SUBMITTED", "FILLED") and budgets[row[5]][1] > now:
            per_budget[row[5]] = per_budget.get(row[5], Decimal(0)) + decimal_out(row[4])
    if cap is not None:
        assert sum(per_budget.values(), Decimal(0)) <= cap, per_budget
    for budget_id, used in per_budget.items():
        assert used <= budgets[budget_id][0], (budget_id, used, budgets[budget_id][0])
    active_accounts: Dict[tuple, int] = {}
    for row in slots.values():
        if row[3] in ("RESERVED", "SUBMITTED"):
            key = (row[5], row[1])
            active_accounts[key] = active_accounts.get(key, 0) + 1
    assert all(v == 1 for v in active_accounts.values()), "an account with two active slots on one budget"
    return {"slots": len(slots), "links": len(links), "open_links": len(open_links),
            "active_slots": sum(1 for r in slots.values() if r[3] in ("RESERVED", "SUBMITTED", "FILLED"))}
