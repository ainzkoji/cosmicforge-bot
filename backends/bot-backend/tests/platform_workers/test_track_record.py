"""Steps 6.4 / 8.6: the track-record recorder, its append-only ledger, export and publication (both dialects).

Every row written here is a test fixture in a throwaway database. No live record exists; the LIVE rows below
exist only to prove the authorization gates and are never written anywhere else.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from app.golive import track_record as tr
from app.golive.track_record import DailyObservation, TrackRecordError, TrackRecordRecorder
from shared_lib.persistence.sql import migrate, rollback
from shared_lib.persistence.sql.platform_schema import all_migrations

KEY = "k" * 40
ENV = {tr.PSEUDONYM_KEY_ENV: KEY}


def obs(date="2026-01-01", start="1000", end="1010", kind="DEMO", cls="BETA_INVITED", risk="BALANCED",
        venue="BINANCE_FUTURES_DEMO", family="trend_core", version="1.0.0", status="RECONCILED", **kw):
    return DailyObservation(trading_date=date, venue=venue, family_id=family, family_version=version,
                            account_class=cls, risk_level=risk, source_kind=kind, starting_equity=start,
                            ending_equity=end, evidence_ref=kw.pop("evidence_ref", "recon_run_1"),
                            reconciliation_status=status, **kw)


@pytest.fixture
def rec(platform_db):
    return TrackRecordRecorder(platform_db, env=ENV)


def _rows(db):
    with db.transaction(write=False) as conn:
        return conn.all("SELECT * FROM track_record_daily")


# ── pseudonymity ────────────────────────────────────────────────────────────────────────────────────────────

def test_refuses_to_record_without_pseudonym_key(platform_db):
    with pytest.raises(TrackRecordError) as e:
        TrackRecordRecorder(platform_db, env={}).record_day(obs(), account_id="acc-1")
    assert e.value.code == "TRACK_RECORD_PSEUDONYM_KEY_MISSING"
    with pytest.raises(TrackRecordError) as e:
        TrackRecordRecorder(platform_db, env={tr.PSEUDONYM_KEY_ENV: "short"}).record_day(obs(), account_id="acc-1")
    assert e.value.code == "TRACK_RECORD_PSEUDONYM_KEY_WEAK"
    assert _rows(platform_db) == []


def test_account_id_is_never_stored_and_ref_is_keyed(rec, platform_db):
    rec.record_day(obs(), account_id="customer-account-77")
    stored = json.dumps([[str(v) for v in r] for r in _rows(platform_db)])
    assert "customer-account-77" not in stored
    ref = rec.account_ref("customer-account-77")
    assert ref in stored and ref.startswith("acct_")
    assert ref != TrackRecordRecorder(platform_db, env={tr.PSEUDONYM_KEY_ENV: "z" * 40}).account_ref("customer-account-77")


# ── provenance chain and append-only ────────────────────────────────────────────────────────────────────────

def test_rows_chain_per_series_and_verify(rec, platform_db):
    a1 = rec.record_day(obs("2026-01-01"), account_id="A")
    a2 = rec.record_day(obs("2026-01-02", "1010", "1020"), account_id="A")
    b1 = rec.record_day(obs("2026-01-01"), account_id="B")
    assert a1.prev_hash == tr.GENESIS_HASH and a2.prev_hash == a1.row_hash and a2.seq == 2
    assert b1.prev_hash == tr.GENESIS_HASH and b1.series_key != a1.series_key
    v = tr.verify_chain(platform_db)
    assert v == {"ok": True, "chains": 2, "rows": 3, "failures": []}


def test_append_only_in_code_and_in_database(rec, platform_db):
    r = rec.record_day(obs(), account_id="A")
    with pytest.raises(tr.AppendOnlyViolation):
        rec.update_record(r.record_id, ending_equity="2000")
    with pytest.raises(tr.AppendOnlyViolation):
        rec.delete_record(r.record_id)
    for sql in ("UPDATE track_record_daily SET ending_equity='2000'", "DELETE FROM track_record_daily"):
        with pytest.raises(Exception) as e:
            with platform_db.transaction() as conn:
                conn.execute(sql)
        assert "TRACK_RECORD_APPEND_ONLY" in str(e.value)
    assert len(_rows(platform_db)) == 1


def test_out_of_band_edit_is_detected_by_the_chain(rec, platform_db):
    rec.record_day(obs("2026-01-01"), account_id="A")
    rec.record_day(obs("2026-01-02", "1010", "1020"), account_id="A")
    with platform_db.transaction() as conn:       # simulate someone bypassing the trigger
        if conn.dialect == "sqlite":
            conn.execute("DROP TRIGGER trg_track_record_daily_no_update")
        else:
            conn.execute("ALTER TABLE track_record_daily DISABLE TRIGGER trg_track_record_daily_append_only")
        conn.execute("UPDATE track_record_daily SET ending_equity='5000' WHERE seq=1")
    v = tr.verify_chain(platform_db)
    assert not v["ok"] and v["failures"] == [{"series_key": v["failures"][0]["series_key"], "seq": 1,
                                              "reason": "ROW_HASH_MISMATCH"}]


def test_corrections_supersede_never_update(rec, platform_db):
    first = rec.record_day(obs("2026-01-04", "10000", "9500"), account_id="A")
    with pytest.raises(TrackRecordError) as e:
        rec.record_day(obs("2026-01-04", "10000", "9600"), account_id="A")
    assert e.value.code == "DUPLICATE_TRADING_DATE"
    with pytest.raises(TrackRecordError) as e:
        rec.correct_day(first.record_id, obs("2026-01-04", "10000", "9600"), account_id="A")
    assert e.value.code == "CORRECTION_REQUIRES_AMENDMENT_NOTE"
    with pytest.raises(TrackRecordError) as e:
        rec.correct_day(first.record_id, obs("2026-01-05", "10000", "9600", amendment_note="x"), account_id="A")
    assert e.value.code == "CORRECTION_SERIES_MISMATCH"
    with pytest.raises(TrackRecordError) as e:
        rec.correct_day(first.record_id, obs("2026-01-04", "10000", "9600", amendment_note="x"), account_id="B")
    assert e.value.code == "CORRECTION_SERIES_MISMATCH"
    fix = rec.correct_day(first.record_id, obs("2026-01-04", "10000", "9600",
                                               amendment_note="venue statement restated fees"), account_id="A")
    assert fix.supersedes_record_id == first.record_id and fix.prev_hash == first.row_hash
    with pytest.raises(TrackRecordError) as e:
        rec.correct_day(first.record_id, obs("2026-01-04", "10000", "9700", amendment_note="again"), account_id="A")
    assert e.value.code == "CORRECTION_TARGET_ALREADY_SUPERSEDED"
    rows = _rows(platform_db)
    assert len(rows) == 2                                  # the original is still there, unchanged
    assert tr.verify_chain(platform_db)["ok"]


# ── validation and the LIVE gates ───────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("kw,code", [
    ({"kind": "SIMULATED"}, "SOURCE_KIND_INVALID"),
    ({"kind": "BACKTEST"}, "ACCOUNT_CLASS_KIND_MISMATCH"),
    ({"cls": "SIMULATION"}, "ACCOUNT_CLASS_KIND_MISMATCH"),
    ({"status": "OK"}, "RECONCILIATION_STATUS_INVALID"),
    ({"start": 1000.0}, "MONEY_FLOAT_REJECTED"),
    ({"deposits": "-1"}, "MONEY_NEGATIVE"),
    ({"drawdown": "1.5"}, "DRAWDOWN_OUT_OF_RANGE"),
    ({"fees": "0.0000000000000000001"}, "MONEY_PRECISION_EXCEEDED"),
    ({"outage_minutes": 2000}, "OUTAGE_MINUTES_INVALID"),
    ({"date": "2026-13-01"}, "DATE_INVALID"),
    ({"date": "2999-01-01"}, "TRADING_DATE_IN_FUTURE"),
    ({"evidence_ref": ""}, "FIELD_REQUIRED"),
])
def test_invalid_observations_are_refused(rec, platform_db, kw, code):
    with pytest.raises(TrackRecordError) as e:
        rec.record_day(obs(**kw), account_id="A")
    assert e.value.code == code
    assert _rows(platform_db) == []


def test_publishing_a_mismatched_day_is_refused(rec):
    with pytest.raises(TrackRecordError) as e:
        rec.record_day(obs(status="MISMATCH"), account_id="A", publishable=True)
    assert e.value.code == "PUBLISH_REFUSED_MISMATCH"


def test_live_refused_until_a_first_authorized_date_is_configured(rec, platform_db):
    live = obs(kind="LIVE", cls="CUSTOMER", venue="BINANCE_FUTURES")
    with pytest.raises(TrackRecordError) as e:
        rec.record_day(live, account_id="A", authorization_ref="golive_1")
    assert e.value.code == "LIVE_NOT_AUTHORIZED_YET"
    assert rec.first_authorized_live_date is None
    assert _rows(platform_db) == []


def test_live_gates_once_configured(platform_db):
    rec = TrackRecordRecorder(platform_db, env={**ENV, tr.FIRST_LIVE_DATE_ENV: "2026-02-01"})

    def live(date, status="RECONCILED"):
        return obs(date, kind="LIVE", cls="CUSTOMER", venue="BINANCE_FUTURES", status=status)

    for args, kw, code in [
        ((live("2026-01-31"),), {"authorization_ref": "golive_1"}, "LIVE_BEFORE_FIRST_AUTHORIZED_DATE"),
        ((live("2026-02-01"),), {}, "LIVE_AUTHORIZATION_REF_REQUIRED"),
        ((live("2026-02-01", "PENDING"),), {"authorization_ref": "golive_1", "publishable": True},
         "LIVE_PUBLISH_REQUIRES_RECONCILED"),
    ]:
        with pytest.raises(TrackRecordError) as e:
            rec.record_day(*args, account_id="A", **kw)
        assert e.value.code == code
    unpublished = rec.record_day(live("2026-02-01", "PENDING"), account_id="A", authorization_ref="golive_1")
    assert not unpublished.publishable
    ok = rec.record_day(live("2026-02-02"), account_id="A", authorization_ref="golive_1", publishable=True)
    assert ok.publishable


def test_database_constraints_back_the_code(platform_db):
    """A writer that bypasses the recorder still cannot store an unknown kind or an unauthorized LIVE row."""
    cols = ("record_id, series_key, seq, trading_date, venue, family_id, family_version, account_class, risk_level, "
            "source_kind, starting_equity, ending_equity, deposits, withdrawals, fees, funding, realized_pnl, "
            "unrealized_pnl, drawdown, gross_exposure, reconciliation_status, outage_minutes, strategy_suspended, "
            "account_ref, evidence_ref, authorization_ref, publishable, prev_hash, row_hash, recorded_at")
    base = ["r1", "s", 1, "2026-01-01", "V", "F", "1", "CUSTOMER", "R", "LIVE", "1", "1", "0", "0", "0", "0", "0",
            "0", "0", "0", "RECONCILED", 0, 0, "acct_x", "e", None, 0, "0", "h1", 1]
    for change in ({9: "FOO"}, {}, {25: "golive", 26: 1, 20: "PENDING"}):
        vals = list(base)
        for i, v in change.items():
            vals[i] = v
        with pytest.raises(Exception):
            with platform_db.transaction() as conn:
                conn.execute(f"INSERT INTO track_record_daily ({cols}) VALUES ({','.join('?' * len(vals))})", vals)
    control = list(base)
    control[7], control[9] = "BETA_INVITED", "DEMO"      # the same row as a DEMO row is accepted
    with platform_db.transaction() as conn:
        conn.execute(f"INSERT INTO track_record_daily ({cols}) VALUES ({','.join('?' * len(control))})", control)


# ── export ──────────────────────────────────────────────────────────────────────────────────────────────────

ACCOUNTS = ["A", "B", "C", "D", "E"]


def _five_account_demo(rec):
    """Hand-computed composite: day 1 +1 %, day 2 A deposits 10 000 (0 %), day 3 everyone -5 %."""
    for a in ACCOUNTS:
        rec.record_day(obs("2026-01-01", "1000", "1010", fees="1"), account_id=a, publishable=True)
        if a == "A":
            rec.record_day(obs("2026-01-02", "1010", "11010", deposits="10000"), account_id=a, publishable=True)
            rec.record_day(obs("2026-01-03", "11010", "10459.5"), account_id=a, publishable=True)
        else:
            rec.record_day(obs("2026-01-02", "1010", "1010"), account_id=a, publishable=True)
            rec.record_day(obs("2026-01-03", "1010", "959.5"), account_id=a, publishable=True)


def _scan(node, refs):
    """Every key and string value of a public document."""
    if isinstance(node, dict):
        for k, v in node.items():
            yield str(k)
            yield from _scan(v, refs)
    elif isinstance(node, list):
        for v in node:
            yield from _scan(v, refs)
    elif isinstance(node, str):
        yield node


FORBIDDEN = ("account_ref", "account_id", "user_id", "email", "api_key", "secret", "token", "password",
             "credential", "tenant", "acct_", "evidence", "authorization", "recon_run", "series_key")


def _assert_clean(doc, rec):
    refs = {rec.account_ref(a) for a in ACCOUNTS}
    for s in _scan(doc, refs):
        low = s.lower()
        assert not any(f in low for f in FORBIDDEN), s
        assert s not in refs and s not in ACCOUNTS
    text = json.dumps(doc)
    assert "11010" not in text and "10459.5" not in text     # no equity amounts, only ratios


def test_export_composite_hand_computed_and_clean(rec, platform_db):
    _five_account_demo(rec)
    rec.record_day(obs("2026-01-01", kind="BACKTEST", cls="SIMULATION", venue="BINANCE_FUTURES_DEMO"),
                   account_id="bt", publishable=True)         # another kind: must not leak into the DEMO series
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    assert doc["state"] == "PUBLISHED_DATA" and doc["source_kind"] == "DEMO" and not doc["is_live"]
    assert doc["label"] == "Simulated — not achieved trading results" and doc["audited"] is False
    assert doc["observation_period"] == {"start": "2026-01-01", "end": "2026-01-03", "calendar_days_with_data": 3}
    assert [p["index"] for p in doc["daily_index"]] == ["1.0100000000", "1.0100000000", "0.9595000000"]
    assert [p["return"] for p in doc["daily_index"]] == ["0.0100000000", "0.0000000000", "-0.0500000000"]
    c = doc["composite"]
    assert c["status"] == "PUBLISHED" and c["account_count"] == 5
    assert c["metrics"]["time_weighted_return"] == "-0.0405000000"
    assert c["metrics"]["max_drawdown"] == "0.0500000000"
    # Fees 5 on 5000 pooled on day 1: gross day 1 = 1.011 -> 1.011 * 0.95 = 0.96045; drag = 0.96045 - 0.9595
    assert c["metrics"]["fee_drag"] == "0.0009500000"
    assert doc["strategy_versions"] == [{"family_version": "1.0.0", "first_date": "2026-01-01", "last_date": "2026-01-03"}]
    assert doc["provenance"]["rows_used"] == 15 and doc["provenance"]["chain_count"] == 5
    assert "Simulated" in doc["limitations"][0]
    _assert_clean(doc, rec)


def test_provenance_head_hash_is_reproducible(rec, platform_db):
    _five_account_demo(rec)
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    with platform_db.transaction(write=False) as conn:
        heads = conn.all("SELECT d.row_hash FROM track_record_daily d WHERE d.source_kind='DEMO' AND d.seq = "
                         "(SELECT MAX(e.seq) FROM track_record_daily e WHERE e.series_key = d.series_key)")
    expected = hashlib.sha256("\n".join(sorted(h[0] for h in heads)).encode()).hexdigest()
    assert doc["provenance"]["chain_head_hash"] == expected and tr.verify_chain(platform_db)["ok"]


def test_small_groups_and_days_are_suppressed(rec, platform_db):
    _five_account_demo(rec)
    rec.record_day(obs("2026-01-01", risk="AGGRESSIVE"), account_id="F", publishable=True)
    rec.record_day(obs("2026-01-04", "959.5", "1000"), account_id="B", publishable=True)   # only 1 account that day
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    groups = {(g["account_class"], g["risk_level"]): g for g in doc["aggregates"]}
    small = groups[("BETA_INVITED", "AGGRESSIVE")]
    assert small == {"account_class": "BETA_INVITED", "risk_level": "AGGRESSIVE", "status": "SUPPRESSED_MIN_COUNT",
                     "minimum_accounts": 5}
    big = groups[("BETA_INVITED", "BALANCED")]
    assert big["status"] == "PUBLISHED" and big["days_suppressed_min_count"] == 1
    assert [p["date"] for p in big["daily_index"]] == ["2026-01-01", "2026-01-02", "2026-01-03"]
    _assert_clean(doc, rec)


def test_customer_floor_cannot_be_configured_away(rec, platform_db):
    rec.record_day(obs(), account_id="A", publishable=True)
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"}, min_accounts=1)
    assert doc["composite"] == {"status": "SUPPRESSED_MIN_COUNT", "minimum_accounts": 5}
    assert doc["daily_index"] == []


def test_operator_series_can_use_a_configured_minimum(rec, platform_db):
    rec.record_day(obs(cls="OPERATOR"), account_id="op", publishable=True)
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"}, min_accounts=1)
    assert doc["composite"]["status"] == "PUBLISHED" and doc["daily_index"][0]["return"] == "0.0100000000"
    assert tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})["composite"]["status"] \
        == "SUPPRESSED_MIN_COUNT"


def test_only_publishable_active_rows_are_exported(rec, platform_db):
    _five_account_demo(rec)
    rec.record_day(obs("2026-01-04", "959.5", "900"), account_id="C")          # not publishable
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    assert doc["observation_period"]["end"] == "2026-01-03"
    # Correct account A's day 3 loss (-5 %) to 0 %: the pooled day changes, the original row is excluded.
    with platform_db.transaction(write=False) as conn:
        a_ref = rec.account_ref("A")
        rid = conn.scalar("SELECT record_id FROM track_record_daily WHERE account_ref=? AND trading_date='2026-01-03'",
                          (a_ref,))
    rec.correct_day(rid, obs("2026-01-03", "11010", "11010", amendment_note="restated"), account_id="A",
                    publishable=True)
    doc = tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    # Day 3 pooled: A 0, others 4 x -50.5 = -202 on 11010 + 4040 = 15050 -> -0.0134219269...
    assert doc["daily_index"][-1]["return"] == "-0.0134219269"
    assert doc["provenance"]["rows_used"] == 15


def test_export_refuses_mixed_or_missing_kind(platform_db):
    for f in ({"family_id": "trend_core", "source_kind": "ALL"}, {"family_id": "trend_core", "source_kind": None}):
        with pytest.raises(TrackRecordError) as e:
            tr.export_series(platform_db, f)
        assert e.value.code == "SOURCE_KIND_REQUIRED"
    with pytest.raises(TrackRecordError):
        tr.export_series(platform_db, {"source_kind": "DEMO", "family_id": "x", "account_id": "A"})


def test_live_export_is_not_yet_available_and_cannot_be_published(platform_db):
    doc = tr.export_series(platform_db, {"source_kind": "LIVE", "family_id": "trend_core"})
    assert doc["state"] == "NOT_YET_AVAILABLE" and doc["reason"] == "No authorized live trading has occurred"
    assert doc["daily_index"] == [] and doc["composite"] is None and doc["label"].startswith("Live")
    with pytest.raises(TrackRecordError) as e:
        tr.publish_series(platform_db, {"source_kind": "LIVE", "family_id": "trend_core"})
    assert e.value.code == "LIVE_NOT_AVAILABLE"


def test_public_document_scanner_refuses_identifying_content():
    for bad in ({"a": {"account_ref": "x"}}, {"a": ["acct_0123456789abcdef0123"]}, {"user_id": 1},
                {"note": "someone@example.com"}, {"x": [{"api_key": "k"}]}):
        with pytest.raises(TrackRecordError):
            tr.assert_public_safe(bad)


# ── publication and the public read model ───────────────────────────────────────────────────────────────────

def test_publish_and_public_read_model(rec, platform_db):
    idx = tr.public_index(platform_db)
    assert idx["live"] == {"state": "NOT_YET_AVAILABLE", "reason": "No authorized live trading has occurred"}
    assert all(not k["series"] for k in idx["kinds"].values())
    _five_account_demo(rec)
    pub = tr.publish_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    assert pub["series_id"] == "demo-trend_core-all-venues"
    idx = tr.public_index(platform_db)
    demo = idx["kinds"]["DEMO"]
    assert demo["state"] == "AVAILABLE" and demo["series"][0]["series_id"] == pub["series_id"]
    assert idx["kinds"]["LIVE"]["state"] == "NOT_YET_AVAILABLE" and idx["kinds"]["BACKTEST"]["series"] == []
    detail = tr.public_series(platform_db, pub["series_id"])
    assert detail["daily_index"] == pub["document"]["daily_index"] and detail["published_at"]
    assert tr.public_series(platform_db, "nope") is None and tr.public_series(platform_db, "../x") is None
    _assert_clean(idx, rec)
    _assert_clean(detail, rec)
    with pytest.raises(Exception) as e:
        with platform_db.transaction() as conn:
            conn.execute("DELETE FROM track_record_publications")
    assert "TRACK_RECORD_APPEND_ONLY" in str(e.value)


def test_public_index_without_a_store_still_answers_live_not_available():
    idx = tr.public_index(None)
    assert idx["store"] == "NOT_CONFIGURED" and idx["live"]["state"] == "NOT_YET_AVAILABLE"


# ── migration ───────────────────────────────────────────────────────────────────────────────────────────────

def test_track_record_migration_is_reversible(platform_db):
    def tables():
        with platform_db.transaction(write=False) as conn:
            if conn.dialect == "sqlite":
                return {r[0] for r in conn.all("SELECT name FROM sqlite_master WHERE type='table'")}
            return {r[0] for r in conn.all("SELECT tablename FROM pg_tables WHERE schemaname='public'")}

    assert {"track_record_daily", "track_record_publications"} <= tables()
    rollback(platform_db, all_migrations(), to_version=19)
    assert not {"track_record_daily", "track_record_publications"} & tables()
    migrate(platform_db, all_migrations())
    assert {"track_record_daily", "track_record_publications"} <= tables()
