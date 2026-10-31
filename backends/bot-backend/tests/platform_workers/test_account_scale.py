"""Account-scale path at 10 and 100 accounts (master prompt section 20), and the recorded 1/10/100/1,000 evidence.

``scripts/perf/account_scale.py`` is the measurement: shared signal -> per-account sizing -> maintenance by fenced
trading workers -> REST reconciliation through the shared request budget -> durable ORDER and NOTIFICATION queues
-> symbol order spreading -> the entry gate, then the failure cases (worker killed mid-cycle, database restart,
user-stream disconnect, emergency flatten during load, queue consumer crash, interrupted deployment, lost order
acknowledgement, stale market data). This test runs THAT code, unchanged, at 10 and 100 accounts on SQLite (and on
PostgreSQL when ``PLATFORM_TEST_POSTGRES_URL`` is set) and asserts only structural properties: counts, refusal
codes, exactly-once, ownership and protection. No timing threshold is asserted (the figures are recorded, not
judged), so a slow CI runner cannot fail it and a fast one cannot hide a defect.

The 1,000-account run is the script's job on the workstation (about a minute); its output is committed as
``evidence/predeploy/scale_<N>_accounts.json`` and the SAME structural checker is applied to those files here, so
the recorded numbers cannot drift from what the code guarantees. Every figure is SYNTHETIC (FakeVenue, Windows,
threads): not venue evidence and not a Linux certification.
"""
from __future__ import annotations

import importlib.util
import json
import uuid
from pathlib import Path

import pytest

from _pg_server import PG_REQUIRED, PG_URL, _pg_admin, _pg_url

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "perf" / "account_scale.py"
EVIDENCE = ROOT / "evidence" / "predeploy"
CI_COUNTS = (10, 100)
RECORDED_COUNTS = (1, 10, 100, 1000)
DIALECTS = ["sqlite"] + (["postgresql"] if PG_URL or PG_REQUIRED else [])


def _load():
    spec = importlib.util.spec_from_file_location("account_scale_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scale = _load()


@pytest.fixture(scope="module", params=DIALECTS)
def rows(request, tmp_path_factory):
    out = {}
    for n in CI_COUNTS:
        work = tmp_path_factory.mktemp(f"scale_{request.param}_{n}")
        url = None
        name = None
        if request.param == "postgresql":
            if not PG_URL:
                pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
            name = f"cfscale_drill_{uuid.uuid4().hex[:10]}"
            _pg_admin(f"CREATE DATABASE {name}")
            url = _pg_url(name)
        try:
            out[n] = scale.run_count(n, url=url, workdir=str(work), ttl_ms=3_000, job_lease_ms=600)
        finally:
            if name:
                _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    return out


# ── the structural checker: applied to the CI runs and to the recorded evidence alike ─────────────────────────
def check_row(r):
    n = r["accounts"]
    venues = min(n, 3)
    s = r["shared_signal_and_sizing"]
    assert s["shared_signal_computations"] == 1                   # computed once per cycle, not per account
    assert s["fingerprint_unchanged_after_fanout"] is True
    assert (s["accounts_sized"], s["sizing_errors"]) == (n, 0)

    m = r["maintenance"]
    assert (m["owned"], m["accounts_maintained"], m["failed"]) == (n, n, 0)
    assert m["first_round"]["accounts_recovered_first"] == n      # every new assignment recovered before maintenance
    assert m["account_cycle_latency_ms"]["n"] == n
    # with a lease longer than the round, no lease lapses mid-round and no account moves in the steady state
    # (evidence/predeploy/scale_1000_accounts_ttl1500_lease_lapse.json records what happens when it is not)
    assert (m["fence_refused_lease_lapsed"], m["accounts_moved_and_recovered_in_round"]) == (0, 0)
    assert m["owned_as_reported_by_each_claim"] == n and m["maintenance_steps"] == n

    rec = r["reconciliation_and_request_budget"]
    assert sum(v["accounts"] for v in rec["by_venue"].values()) == n
    for venue, v in rec["by_venue"].items():
        assert set(v["outcomes"]) <= {"OK", "FAILED:RATE_LIMITED"}, (venue, v["outcomes"])
        refused = v["outcomes"].get("FAILED:RATE_LIMITED", 0)
        assert v["used_weight"] <= v["reconciliation_ceiling"] + 1e-9, (venue, v)   # never above its class ceiling
        assert sum(v["denials"].values()) == refused, (venue, v)
        assert v["accounts_reconciled_in_one_window"] + refused == v["accounts"]

    q = r["queue"]
    assert q["outcomes"].get("SUCCEEDED") == n and q["intent_statuses"] == {"ACKNOWLEDGED": n}
    assert q["duplicate_orders"] == 0 and q["venue_submits"] == n                  # a lost ack is never resent
    assert q["lost_acknowledgements_resolved_by_lookup"] == q["lost_acknowledgements_injected"] >= 1

    nt = r["notifications"]
    assert (nt["delivered"], nt["duplicate_deliveries"]) == (n, 0)

    sp = r["order_spreading"]
    assert sp["within_cap"] is True and sp["calibrated"] is False and sp["depth_snapshots_read"] == 1
    assert sp["max_orders_in_one_slot_interval"] <= sp["policy_max_orders_per_slot"]
    assert sp["reserved"] + sp["deferred"] == n

    g = r["entry_gate"]
    assert g["production_default_policy"]["codes"] == {"MARKET_FALLBACK_DISABLED_BY_POLICY": n}
    unc = g["uncalibrated_market_permitting_test_policy"]
    assert unc["codes"].get("RESERVED") == venues                # one account per venue/symbol/side, no more
    assert unc["codes"].get("ORDER_SPREADING_CALIBRATION_REQUIRED", 0) == n - venues
    assert all(c.get("RESERVED", 0) == 1 for c in unc["by_venue"].values())
    assert g["stale_market_data"]["codes"] == {"DEPTH_STALE": n}
    assert g["active_slots_created"] == venues

    f = r["failures"]
    k = f["worker_killed_mid_cycle"]
    assert k["worker_accounts"] >= 1 and k["orphans_left"] == 0 and k["all_accounts_owned_after"] is True
    assert k["stale_holder_mutation_refused"] in ("ASSIGNMENT_TAKEN_OVER", "ASSIGNMENT_EXPIRED")
    assert k["lease_takeover_ms"] is not None
    for case in ("worker_killed_mid_cycle", "database_restart", "interrupted_deployment"):
        assert (f[case]["accounts_wrongly_protected"], f[case]["accounts_with_duplicate_stops"],
                f[case]["open_protection_gaps"]) == (0, 0, 0), case
    d = f["database_restart"]
    assert d["accounts_maintained_during_outage"] == 0 and d["rounds_refused_during_outage"] >= 1
    assert d["recovered_full_ownership_ms"] is not None and d["connections_reopened"] >= 1
    u = f["user_stream_disconnect"]
    assert (u["live_after_connect"], u["entries_allowed_while_live"]) == (n, n)
    assert u["health_after_disconnect"] == {"DISCONNECTED": n}
    assert u["entry_refusals_while_down"] == {"USER_STREAM_DISCONNECTED": n}       # entries refused, nothing else
    assert u["protection_while_down"] == {"accounts_wrongly_protected": 0, "accounts_with_duplicate_stops": 0,
                                          "open_protection_gaps": 0}
    assert (u["live_after_reconnect"], u["entries_allowed_after_reconnect"]) == (n, n)
    assert u["connections_per_stream"] == {"2": n}                                  # one reconnect, never two at once
    e = f["emergency_flatten_during_load"]
    flat = e["accounts_flattened"]
    assert flat >= 1 and e["flat_after"] == flat and e["orphan_stops_after"] == 0 and e["reopened_after"] == 0
    assert e["previous_holder_fence_refused_after"] == flat and e["recovered_first_by_next_owner"] == flat
    assert e["other_accounts_wrongly_protected"] == 0
    qc = f["queue_consumer_crash"]
    assert qc["all_jobs_succeeded"] is True and qc["duplicate_orders"] == 0
    # the crashed consumer's claim, then at least one more (more when a busy consumer's own short drill lease lapsed
    # too); whatever the number of deliveries, the effect happened once (duplicate_orders above)
    attempts = qc["attempts_of_crashed_jobs"]
    assert all(int(k) >= 2 for k in attempts) and sum(attempts.values()) == qc["claimed_by_crashed_consumer"]
    dep = f["interrupted_deployment"]
    assert dep["all_accounts_on_new_version_ms"] is not None and dep["killed_mid_round"] == 1
    assert dep["accounts_recovered_first_by_new_version"] == n
    assert dep["assignment_history_reasons_during_stage"].get("DRAIN", 0) >= 1
    assert f["order_acknowledgement_lost"]["duplicate_orders"] == 0
    assert f["stale_market_data"]["entries_refused"] == {"DEPTH_STALE": n}

    res = r["resources"]
    assert res["db_connections_opened_total"] >= 1 and res["db_pool_peak_in_use"] <= res["db_pool_size"]
    if "tracemalloc_peak_bytes" in res:
        assert set(res["tracemalloc_peak_bytes"]) == {"signal_and_sizing", "fleet_and_maintenance_cycle"}
    assert res["cpu_ms_total"] > 0


@pytest.mark.parametrize("n", CI_COUNTS)
def test_the_account_scale_path_holds_its_guarantees(rows, n):
    check_row(rows[n])


def test_the_shared_signal_is_computed_once_whatever_the_account_count(rows):
    assert {rows[n]["shared_signal_and_sizing"]["shared_signal_computations"] for n in CI_COUNTS} == {1}
    assert rows[100]["shared_signal_and_sizing"]["accounts_sized"] == 10 * rows[10]["shared_signal_and_sizing"][
        "accounts_sized"]


def test_one_egress_address_cannot_reconcile_a_large_fleet_in_one_window(rows):
    """The request budget is per venue and server address, shared by every account behind it. At 100 accounts the
    configured BingX envelope (20 requests / 10 s, UNVERIFIED) already refuses most REST reconciliations in one
    window: refused by the LOCAL budget, before any request leaves -- the capacity planning input this records."""
    bingx = rows[100]["reconciliation_and_request_budget"]["by_venue"]["bingx"]
    assert bingx["outcomes"].get("FAILED:RATE_LIMITED", 0) > 0
    assert bingx["derived_windows_for_one_full_round"] > 1


# ── the recorded evidence (the script's own run on the workstation) ───────────────────────────────────────────
@pytest.mark.parametrize("n", RECORDED_COUNTS)
def test_the_recorded_evidence_exists_says_what_it_is_and_holds_the_same_guarantees(n):
    path = EVIDENCE / f"scale_{n}_accounts.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["schema"] == "cosmicforge-predeploy-account-scale-v1"
    assert doc["evidence_class"] == "SYNTHETIC_FAKEVENUE_WINDOWS_WORKSTATION"
    assert "SYNTHETIC" in doc["not_evidence_of"] and "Windows" in doc["not_evidence_of"]
    assert "not venue evidence" in doc["not_evidence_of"].lower() and "linux" in doc["not_evidence_of"].lower()
    assert doc["machine"]["os"] and doc["machine"]["cpu_count"] and doc["code"]["commit"]
    assert "working_tree_dirty" in doc["code"]
    assert doc["rows"][0]["dialect"] == "sqlite"              # SQLite always; PostgreSQL next to it when it was run
    assert {r["dialect"] for r in doc["rows"]} <= {"sqlite", "postgresql"}
    for row in doc["rows"]:
        assert row["accounts"] == n
        check_row(row)
