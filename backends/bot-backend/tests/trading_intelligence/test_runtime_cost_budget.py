"""Step 1.0b / 1.0c -- the per-cycle cost of one account, measured on the real
runtime entry point (``production_runtime.sync_account(execute=True)``).

Exchange cost is counted in Binance USDⓈ-M request-weight units per call of
the broker fake; database cost from the shared library's connection and
statement counters. The same scenarios run against the baseline export of the
repository to produce the "before" column of the Step 1 report (set
``RUNTIME_COST_BUDGET_RECORD_ONLY=1`` there: the budgets asserted below describe
the optimised runtime). Set ``RUNTIME_COST_BUDGET_OUT=<file>`` to dump the
measurements as JSON.
"""
import json
import os
import time
from types import SimpleNamespace

import pytest
from test_demo_boundary_certification import broker
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_research_separation import fresh
from app.execution import demo_boundary_certification as cert
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime

__all__ = ["broker", "live", "demo", "fresh"]

#: Request weight per call (Binance USDⓈ-M futures API documentation).
WEIGHTS = {
    "account": 5, "get_balance": 5, "account_balance": 5,
    "position_risk": 5, "get_position_info": 5, "get_position_amt": 5,
    "open_orders": 40,                 # account-wide (1 with a symbol; the runtime reads account-wide)
    "get_algo_orders": 1,
    "income_history": 30,
    "user_trades": 5,
    "get_order": 1, "get_order_by_client_order_id": 1,
    "exchange_info_cached": 1, "exchange_info": 1,
    "klines": 2, "last_price": 1, "get_orderbook": 5, "depth": 5,
    "_signed_post": 0, "_signed_delete": 1, "place_order": 0,
}
RECORD_ONLY = os.environ.get("RUNTIME_COST_BUDGET_RECORD_ONLY") == "1"


class Meter:
    """Counts every broker call of the fake by method and in request weight."""

    def __init__(self, client):
        self.calls = {}
        self.client = client
        for name in WEIGHTS:
            original = getattr(client, name)

            def wrapped(*a, _name=name, _original=original, **k):
                self.calls[_name] = self.calls.get(_name, 0) + 1
                return _original(*a, **k)
            setattr(client, name, wrapped)

    def reset(self):
        self.calls = {}

    @property
    def weight(self):
        return sum(WEIGHTS[n] * c for n, c in self.calls.items())


def _db_metrics():
    try:
        from shared_lib.persistence import db as shared_db
    except Exception:
        return None
    return shared_db if hasattr(shared_db, "METRICS") else None


@pytest.fixture
def measured(broker, monkeypatch):
    h, state = broker
    from app.activation import account_status
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(
        environment="demo", account_id=h.account["id"], user_id=h.account["user_id"], broker_type="binance",
        base_url=h.client.base_url, credential_version=1, key_fingerprint="fp"))
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    monkeypatch.setattr(runtime, "owner_current", lambda db: True)
    # The certification fixture's boundary (account-scoped to the fixture's own
    # adapter) is injected the way every runtime test injects it; the runtime's
    # own call keeps its signature.
    original_process = production.process_account
    monkeypatch.setattr(production, "process_account",
                        lambda db, account, client, snapshot, **kw: original_process(
                            db, account, client, snapshot, boundary_factory=h.boundary_for, **kw))
    runtime._clients.clear()
    runtime.initialize(h.db)                             # as the loop's sync() does before its accounts
    # The balance view the pre-optimisation runtime reads (the optimised one
    # reads the account document once and derives it).
    raw = h.client.account.return_value
    h.client.get_balance.return_value = {"wallet": raw["totalWalletBalance"], "equity": raw["totalMarginBalance"],
                                         "available": raw["availableBalance"]}
    shared_db = _db_metrics()
    if shared_db is not None:
        shared_db.instrument(True)
    meter = Meter(h.client)
    results = {}

    def cycle(label, now=None, db=None):
        meter.reset()
        if shared_db is not None:
            shared_db.METRICS.reset()
        started = time.perf_counter()
        error = None
        try:
            out = runtime.sync_account(db or h.db, h.account, factory=lambda auth: h.client, execute=True)
        except Exception as exc:  # a failing read is one of the scenarios
            out, error = None, type(exc).__name__
        elapsed_ms = (time.perf_counter() - started) * 1000
        metrics = shared_db.METRICS.snapshot() if shared_db is not None else {}
        results[label] = {"weight": meter.weight, "calls": dict(sorted(meter.calls.items())), "elapsed_ms": round(elapsed_ms, 1),
                          "error": error, **metrics}
        return out

    yield SimpleNamespace(h=h, state=state, cycle=cycle, results=results, meter=meter)
    if shared_db is not None:
        shared_db.instrument(False)
    out = os.environ.get("RUNTIME_COST_BUDGET_OUT")
    if out:
        existing = {}
        if os.path.exists(out):
            existing = json.loads(open(out, encoding="utf-8").read())
        existing.update(results)
        open(out, "w", encoding="utf-8").write(json.dumps(existing, indent=2, sort_keys=True))
    print("\nRUNTIME COST", json.dumps(results, indent=1, sort_keys=True))


def budget(results, label, *, weight, connections=None, schema=None):
    """Assert the optimised budget (skipped when recording the baseline)."""
    r = results[label]
    if RECORD_ONLY:
        return
    assert r["weight"] <= weight, (label, r)
    if connections is not None and "connections_opened" in r:
        assert r["connections_opened"] <= connections, (label, r)
    if schema is not None and "schema_statements" in r:
        assert r["schema_statements"] <= schema, (label, r)


def stop_bots(h):
    with h.db.connect() as c:
        c.execute("UPDATE bot_instances SET status='stopped' WHERE broker_account_id=?", (h.account["id"],))


def waiting_for_signal(monkeypatch):
    monkeypatch.setattr(production, "eligibility", lambda row, now: "AWAITING_NATURAL_CATI_DECISION")


# ── Scenarios ────────────────────────────────────────────────────────────────

def test_an_account_without_a_tradable_bot_costs_only_its_discovery_reads(measured):
    m = measured
    stop_bots(m.h)
    m.cycle("warm_up")                                   # schema and client caches
    out = m.cycle("no_bot")
    assert out["execution"]["evaluation_scope"] == "IDLE_NO_TRADABLE_BOT"
    assert out["orders_read"] == "SKIPPED_IDLE_ACCOUNT" and out["execution"]["reason"] == "AUTO_TRADING_DISABLED"
    # Account document + positions: nothing else is asked of the venue.
    budget(m.results, "no_bot", weight=10, connections=1, schema=0)
    assert m.results["no_bot"]["calls"].get("income_history", 0) == 0
    assert m.results["no_bot"]["calls"].get("open_orders", 0) == 0


def test_an_idle_running_bot_reads_the_venue_once_per_resource(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    m.cycle("warm_up")
    out = m.cycle("idle_running_bot")
    assert out["execution"]["execution_permission"] in ("WAITING_SIGNAL", "BLOCKED_DEMO_ORDER_GATE"), out["execution"]
    # account 5 + positions 5 + orders 40 + algo orders for the selected symbol 1;
    # the income ledger is on its 5-minute interval (no venue page) and the
    # account document is not read twice.
    budget(m.results, "idle_running_bot", weight=55, connections=1, schema=0)
    assert m.results["idle_running_bot"]["calls"].get("account", 0) == 1
    assert m.results["idle_running_bot"]["calls"].get("income_history", 0) == 0
    second = m.cycle("idle_running_bot_next")
    assert second["execution"]["risk"]["income_ledger"]["refreshed"] is False


def test_the_income_ledger_is_refreshed_when_the_latch_can_matter(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    m.cycle("warm_up")
    m.h.now += 6 * 60_000                                # past the refresh interval
    monkeypatch.setattr(production.time, "time", lambda: m.h.now / 1000)
    out = m.cycle("idle_running_bot_interval")
    assert out["execution"]["risk"]["income_ledger"]["refreshed"] is True
    assert m.results["idle_running_bot_interval"]["calls"]["income_history"] == 1   # one overlap page, not a month
    budget(m.results, "idle_running_bot_interval", weight=85, connections=1, schema=0)


def test_one_protected_position_is_verified_with_bounded_reads(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "cost-protected", action="hold")
    assert m.state["qty"] > 0 and len(m.state["legs"]) == 2
    m.cycle("warm_up")
    out = m.cycle("protected_position")
    history = out["execution"]["execution_history"]
    assert history and history[0]["protection"]["state"] == "CONFIRMED"
    assert m.state["qty"] > 0 and len(m.state["legs"]) == 2
    # account 5, positions 5, orders 40, income refresh 30 (position open),
    # order 1, positionRisk 5, fills 5, algo orders: snapshot + one replay read.
    budget(m.results, "protected_position", weight=100, connections=1, schema=0)
    assert m.results["protected_position"]["calls"]["get_algo_orders"] <= 2


def test_a_pending_close_is_resolved_without_extra_account_reads(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "cost-close", action="hold")
    original = m.h.client._signed_post
    def lost_ack(path, params):
        original(path, params)
        raise TimeoutError("acknowledgement lost")
    m.h.client._signed_post = lost_ack
    with pytest.raises(TimeoutError):
        cert.run(m.h.db, m.h.account["id"], "cost-close", action="close")
    m.h.client._signed_post = original
    out = m.cycle("pending_close")
    with m.h.db.connect() as c:
        assert c.execute("SELECT status FROM cati_production_closes").fetchone()[0] == "CLOSED"
    budget(m.results, "pending_close", weight=120, connections=1, schema=0)


def test_a_failed_read_costs_no_more_than_a_healthy_cycle(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "cost-failed-read", action="hold")
    m.cycle("warm_up")
    m.h.client.get_algo_orders = lambda *a, **k: (_ for _ in ()).throw(TimeoutError("read timed out"))
    out = m.cycle("failed_stop_read")
    assert out["execution"]["reason"] in ("PROTECTION_STATE_UNKNOWN", "NATIVE_PROTECTION_READ_UNAVAILABLE"), out["execution"]
    assert m.state["qty"] > 0                            # 1.0a: never a false liquidation
    budget(m.results, "failed_stop_read", weight=100)


def test_a_restart_with_a_protected_position_recovers_within_budget(measured, monkeypatch):
    from shared_lib.persistence.db import DB
    m = measured
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "cost-restart", action="hold")
    runtime._clients.clear()                             # a new process: no cached client, new DB handle
    out = m.cycle("restart_recovery", db=DB(m.h.db.path))
    assert out["execution"]["execution_history"][0]["protection"]["state"] == "CONFIRMED"
    assert m.state["qty"] > 0 and len(m.state["legs"]) == 2
    budget(m.results, "restart_recovery", weight=100)


def test_several_idle_accounts_cost_their_discovery_reads_each(measured):
    m = measured
    stop_bots(m.h)
    m.cycle("warm_up")
    base = dict(m.h.account)
    total = 0
    for n in range(3):
        account = {**base, "id": f"{base['id']}-copy{n}"}
        with m.h.db.connect() as c:
            c.execute("INSERT OR IGNORE INTO broker_accounts (id,user_id,broker_id,environment,status) VALUES(?,?,?,?,?)",
                      (account["id"], base["user_id"], base["broker_id"], "demo", "connected"))
        m.h.account = account
        m.cycle(f"several_accounts_{n}")
        total += m.results[f"several_accounts_{n}"]["weight"]
    m.h.account = base
    m.results["several_accounts_total"] = {"weight": total, "accounts": 3}
    budget(m.results, "several_accounts_total", weight=30)


# ── the same budget under Linux file semantics ──────────────────────────────

from test_production_storage_lifecycle import linux_identity  # noqa: E402,F401  (fixture)


def test_the_budget_holds_with_linux_file_semantics(linux_identity, measured, monkeypatch):
    """The first CI run of this work on Linux measured 23 schema statements in
    every account cycle where these tests assert 0: the once-per-database memo
    was keyed on a timestamp that is stable on Windows and moves with every
    write on Linux. Here the process sees files as Linux reports them, so the
    budget is guarded on a Windows workstation as well."""
    m = measured
    waiting_for_signal(monkeypatch)
    m.cycle("warm_up")
    for label in ("linux_idle_1", "linux_idle_2", "linux_idle_3"):
        out = m.cycle(label)                             # each cycle writes, which moves the Linux change time
        assert out["execution"]["evaluation_scope"] == "FULL"
        budget(m.results, label, weight=55, connections=1, schema=0)
        assert m.results[label]["schema_statements"] == 0 and m.results[label]["connections_opened"] == 1

