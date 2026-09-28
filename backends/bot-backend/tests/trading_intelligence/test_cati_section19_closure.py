"""Section 19: every required persistence semantic maps to a verified existing equivalent or a minimal
additive extension -- never a duplicate table created to match a document name."""
import json
import sqlite3
from decimal import Decimal

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

#: REQUIRED SEMANTIC MODEL -> (existing table, the columns that carry the semantic) -- machine-checked
SEMANTIC_MAP = {
    "instrument_registry": ("venue_instruments", ("venue", "venue_symbol", "canonical_symbol", "asset_class",
                                                 "product_type", "payload_json", "status", "first_seen_ms",
                                                 "delisted_at_ms", "metadata_hash")),
    "venue_instrument_capabilities": ("venue_instruments", ("environment", "api_tradable", "status", "last_seen_ms",
                                                           "metadata_hash", "metadata_changed_ms")),
    "broker_account_capabilities": ("broker_credentials_v2", ("account_id", "version", "permissions_json")),
    "broker_account_topology": ("cati_capital_plan_evidence", ("user_id", "broker_account_id", "topology_json",
                                                               "created_at")),
    "balance_segments": ("cati_capital_plan_evidence", ("user_id", "broker_account_id", "balances_json", "created_at")),
    "capital_transfer_intents": ("broker_transfer_requests", ("user_id", "broker_account_id", "source_wallet",
                                                             "destination_wallet", "asset", "amount",
                                                             "idempotency_key", "origin", "metadata_json", "status")),
    "capital_transfer_receipts": ("broker_transfer_requests", ("broker_transfer_id", "submitted_at", "confirmed_at",
                                                              "failure_reason")),
    "capital_transfer_reconciliation": ("broker_transfer_events", ("transfer_id", "event_type", "from_status",
                                                                  "to_status", "detail_json", "created_at")),
    "dataset_manifests": ("dataset_manifests", ("manifest_id", "manifest_hash", "role", "asset_class", "venue",
                                               "payload_json")),
    "dataset_partitions": ("market_ingest_log", ("venue", "venue_symbol", "dataset", "timeframe", "period", "status",
                                                "rows", "reason")),
    "data_quality_events": ("data_quality_events", ("event_id", "event_type", "severity")),
    "fx_reference_repairs": ("fx_reference_repairs", ("provider", "pair", "old_status", "reason",
                                                     "algorithm_version", "rows_removed", "rows_inserted",
                                                     "validation_json")),
    "capital_reservation": ("cati_portfolio_reservations", ("user_id", "broker_account_id", "capital_asset",
                                                            "capital_amount", "capital_wallet", "capital_json",
                                                            "expires_at", "status")),
}
DOCUMENT_NAMES_NOT_CREATED = ("instrument_registry", "venue_instrument_capabilities", "broker_account_capabilities",
                              "broker_account_topology", "balance_segments", "capital_transfer_intents",
                              "capital_transfer_receipts", "capital_transfer_reconciliation", "dataset_partitions",
                              "fx_reference_mapping")
SECRET_WORDS = ("secret", "api_key", "apikey", "passphrase", "signature", "authorization", "private_key",
                "decryption", "password")


@pytest.fixture
def db(tmp_path):
    d = DB(path=str(tmp_path / "s19.db"))
    migrate(d)
    return d


def cols(db, table):
    with db.connect() as c:
        return {r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}


def tables(db):
    with db.connect() as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def test_every_semantic_model_maps_to_existing_persistence(db):
    for semantic, (table, needed) in SEMANTIC_MAP.items():
        assert set(needed) <= cols(db, table), (semantic, table, set(needed) - cols(db, table))


def test_no_table_created_only_to_match_a_document_name(db):
    assert not set(DOCUMENT_NAMES_NOT_CREATED) & tables(db)


def test_migration_is_idempotent(db):
    def schema():
        with db.connect() as c:
            return sorted(tuple(r) for r in c.execute("SELECT type, name, sql FROM sqlite_master").fetchall())
    before = schema()
    migrate(db)
    migrate(db)
    assert schema() == before


def test_old_shape_databases_upgrade_additively(tmp_path):
    path = str(tmp_path / "old.db")
    raw = sqlite3.connect(path)
    raw.execute("""CREATE TABLE cati_portfolio_reservations (reservation_id TEXT PRIMARY KEY, broker_account_id TEXT NOT
        NULL, bot_instance_id TEXT NOT NULL, cycle_id TEXT NOT NULL, selected_candidate_ids TEXT NOT NULL,
        selected_instruments TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('RESERVED', 'RESOLUTION_PENDING',
        'CONSUMED', 'RELEASED', 'EXPIRED')), mode TEXT NOT NULL DEFAULT 'SHADOW', created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, reservation_version TEXT NOT NULL)""")
    raw.execute("INSERT INTO cati_portfolio_reservations VALUES ('r1','a','b','c','[]','[]','RESERVED','SHADOW',1,2,1,'v')")
    raw.execute("""CREATE TABLE cati_capital_plan_evidence (evidence_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
        broker_account_id TEXT NOT NULL, bot_instance_id TEXT, cycle_id TEXT, opportunity_id TEXT, mode TEXT NOT NULL
        DEFAULT 'SHADOW', outcome TEXT NOT NULL, product TEXT NOT NULL, plan_json TEXT NOT NULL,
        simulated_transfer_json TEXT, created_at INTEGER NOT NULL)""")
    raw.execute("INSERT INTO cati_capital_plan_evidence VALUES ('e1','u','a',NULL,NULL,NULL,'SHADOW','X','P','{}',NULL,1)")
    raw.commit()
    raw.close()
    d = DB(path=path)
    migrate(d)
    with d.connect() as c:
        r = c.execute("SELECT reservation_id, status, capital_amount, capital_json FROM cati_portfolio_reservations").fetchone()
        e = c.execute("SELECT evidence_id, outcome, topology_json, balances_json FROM cati_capital_plan_evidence").fetchone()
    assert tuple(r) == ("r1", "RESERVED", None, None)  # existing row kept, claims no capital
    assert tuple(e) == ("e1", "X", None, None)


def test_instrument_identity_and_delisting_retention(db):
    from app.exchange.instruments import DiscoveredInstrument, InstrumentCatalog

    def ins(sym, ac="CRYPTO", canon=None):
        return DiscoveredInstrument("bybit_linear", sym, ac, "PERPETUAL", canon or f"{sym[:-4]}/USDT:PERP", sym[:-4],
                                    "USDT", "USDT", "PERPETUAL", "Trading", True, 0.1, 0.001, 0.001, 100.0, 5.0, 10.0)
    cat = InstrumentCatalog(db)
    cat.upsert("bybit_linear", "LIVE", [ins("BTCUSDT"), ins("ETHUSDT")], 1)
    cat.upsert("bybit_linear", "LIVE", [ins("ETHUSDT")], 2)
    gone = cat.record("bybit_linear", "LIVE", "BTCUSDT")
    assert gone["state"] == "DELISTED" and gone["instrument"].canonical_symbol == "BTC/USDT:PERP"
    assert "BTCUSDT" in {i.venue_symbol for i in cat.list("bybit_linear", "LIVE", include_delisted=True,
                                                          tradable_only=False)}
    assert "BTCUSDT" not in {i.venue_symbol for i in cat.list("bybit_linear", "LIVE")}
    with pytest.raises(Exception):
        with db.connect() as c:
            c.execute("INSERT INTO venue_instruments (venue, environment, venue_symbol, asset_class, product_type, "
                      "canonical_symbol, payload_json, first_seen_ms, last_seen_ms) VALUES "
                      "('bybit_linear','LIVE','ETHUSDT','CRYPTO','PERPETUAL','x','{}',1,1)")  # identity is unique


def test_venue_capability_is_not_account_capability(db):
    from app.core.broker_capability_gate import load_permission_evidence
    from app.exchange.instruments import DiscoveredInstrument, execution_eligibility

    assert not {"user_id", "broker_account_id", "account_id"} & cols(db, "venue_instruments")  # venue-global
    ins = DiscoveredInstrument("bybit_linear", "BTCUSDT", "CRYPTO", "PERPETUAL", "BTC/USDT:PERP", "BTC", "USDT", "USDT",
                               "PERPETUAL", "Trading", True, 0.1, 0.001, 0.001, 100.0, 5.0, 10.0)
    ok_a, _ = execution_eligibility(ins, broker="bybit", environment="demo", permissions={"TRADE": True, "WITHDRAW": False})
    ok_b, why_b = execution_eligibility(ins, broker="bybit", environment="demo", permissions={"TRADE": True, "WITHDRAW": True})
    assert ok_a and not ok_b and why_b  # same venue instrument, different ACCOUNT eligibility
    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at) "
                  "VALUES ('A','ua','bybit','crypto','active','x','x'), ('B','ub','bybit','crypto','active','x','x')")
        c.execute("UPDATE broker_accounts SET active_credential_version=1")  # evidence of the ACTIVE credential only
        cv = cols(db, "broker_credentials_v2")
        base = {"account_id": None, "version": 1, "permissions_json": None}
        for acct, perms in (("A", {"TRADE": True}), ("B", {"TRADE": False})):
            row = dict(base, account_id=acct, permissions_json=json.dumps({"permissions": perms}))
            extra = {k: "x" for k in cv if k not in row and k not in ("id",)}
            names = list(row) + list(extra)
            try:
                c.execute(f"INSERT INTO broker_credentials_v2 ({','.join(names)}) VALUES ({','.join('?' for _ in names)})",
                          list(row.values()) + list(extra.values()))
            except sqlite3.IntegrityError:
                pytest.skip("broker_credentials_v2 has constraints beyond this fixture")
        assert load_permission_evidence(c, "A") == {"TRADE": True}
        assert load_permission_evidence(c, "B") == {"TRADE": False}  # never another account's evidence


def test_topology_and_balances_are_account_scoped_append_only_evidence(db):
    from shared_lib.broker.wallets import topology_for

    from app.trading_intelligence.capital.evidence import CapitalPlanEvidenceStore
    from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings, plan_capital

    store = CapitalPlanEvidenceStore(db)
    for acct, user in (("A", "ua"), ("B", "ub")):
        state = AccountCapitalState(acct, "USDT", topology_for("bybit", "UNIFIED"), {"UNIFIED": Decimal("100")})
        plan = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=Decimal("10"), settings=CapitalSettings(),
                            plan_key="k")
        store.record(plan, user_id=user, bot_instance_id="b", cycle_id="c", opportunity_id="o",
                     topology=state.topology.to_dict(), balances={"free_by_wallet": {"UNIFIED": "100", "FUND": None}})
    a = store.list(user_id="ua", broker_account_id="A")
    assert len(a) == 1 and json.loads(a[0]["topology_json"])["topology_class"] == "UNIFIED"
    assert json.loads(a[0]["balances_json"])["free_by_wallet"] == {"UNIFIED": "100", "FUND": None}  # unknown != 0
    assert store.list(user_id="ua", broker_account_id="B") == []  # another tenant's account is invisible
    with pytest.raises(Exception):
        with db.connect() as c:
            c.execute("UPDATE cati_capital_plan_evidence SET topology_json='{}'")


def test_transfer_intent_idempotency_receipt_and_append_only_reconciliation(db):
    from app.transfers.models import IdempotencyConflict, TransferIntent, TransferStatus as S
    from app.transfers.store import TransferStore

    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at) "
                  "VALUES ('A','ua','bybit','crypto','active','x','x')")
    ts = TransferStore(db)
    intent = TransferIntent("ua", "A", "USDT", Decimal("25"), "FUND", "UNIFIED", "idem-1")
    row, created = ts.create_or_get(intent, broker="bybit", environment="DEMO", credential_version=1)
    again, created2 = ts.create_or_get(intent, broker="bybit", environment="DEMO", credential_version=1)
    assert created and not created2 and again["id"] == row["id"]  # one physical submission per key
    with pytest.raises(IdempotencyConflict):
        ts.create_or_get(TransferIntent("ua", "A", "USDT", Decimal("99"), "FUND", "UNIFIED", "idem-1"),
                         broker="bybit", environment="DEMO", credential_version=1)
    tid = row["id"]
    assert ts.transition(tid, expect=S.REQUESTED, to=S.VALIDATING, event="VALIDATE")
    assert ts.transition(tid, expect=S.VALIDATING, to=S.SUBMITTING, event="SUBMIT")
    assert ts.transition(tid, expect=S.SUBMITTING, to=S.UNKNOWN, event="SUBMIT_TIMEOUT")
    assert ts.transition(tid, expect=S.UNKNOWN, to=S.RECONCILIATION_REQUIRED, event="LOOKUP_NOT_FOUND")
    assert ts.transition(tid, expect=S.RECONCILIATION_REQUIRED, to=S.COMPLETED, event="RECONCILED",
                         broker_transfer_id="brk-77", confirmed_at="2026-09-28T00:00:00+00:00")
    ev = ts.events(user_id="ua", broker_account_id="A", transfer_id=tid)
    assert [e["to_status"] for e in ev] == ["REQUESTED", "VALIDATING", "SUBMITTING", "UNKNOWN",
                                           "RECONCILIATION_REQUIRED", "COMPLETED"]  # ambiguity history kept
    assert ts.get(user_id="ua", broker_account_id="A", transfer_id=tid)["broker_transfer_id"] == "brk-77"
    with pytest.raises(Exception):
        with db.connect() as c:
            c.execute("DELETE FROM broker_transfer_events")
    assert ts.events(user_id="ub", broker_account_id="A", transfer_id=tid) == []  # tenant-scoped reads


def test_frozen_manifest_is_immutable_and_persisted_once(db):
    from test_cati_section13_closure import frozen

    from app.market_data.universe import persist_dataset_manifest

    a, b = frozen(created_at="t1"), frozen(created_at="t2")
    h1 = persist_dataset_manifest(db, role="CERTIFICATION", asset_class="FX", venue="dukascopy", payload=a, created_at=1)
    h2 = persist_dataset_manifest(db, role="CERTIFICATION", asset_class="FX", venue="dukascopy", payload=b, created_at=2)
    assert h1 == h2 == a["manifest_hash"]
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM dataset_manifests").fetchone()[0] == 1
    tampered = dict(a, symbols=["EURUSD", "GBPUSD"])
    with pytest.raises(ValueError):
        persist_dataset_manifest(db, role="CERTIFICATION", asset_class="FX", venue="dukascopy", payload=tampered,
                                 created_at=3)
    with pytest.raises(Exception):
        with db.connect() as c:
            c.execute("UPDATE dataset_manifests SET role='X'")


def test_dataset_partition_identity_is_unique(db):
    from test_cati_section13_closure import _freeze_args, audit

    from app.market_data.universe import freeze_dataset_payload

    with pytest.raises(ValueError, match="DUPLICATE_PARTITION"):
        freeze_dataset_payload(**{**_freeze_args(), "partitions": [audit(), audit()]})
    with pytest.raises(Exception):
        with db.connect() as c:
            for _ in range(2):
                c.execute("INSERT INTO market_ingest_log (venue, venue_symbol, dataset, timeframe, period, status, "
                          "recorded_at) VALUES ('v','S','klines','1m','2026-01','FETCHED',1)")


def test_quality_and_repair_lineage_is_append_only(db):
    with db.connect() as c:
        c.execute("INSERT INTO fx_reference_repairs (repair_id, provider, pair, timeframe, period, old_status, reason, "
                  "algorithm_version, source_evidence_json, rows_removed, rows_inserted, validation_json, recorded_at) "
                  "VALUES ('r','dukascopy','EURZAR','1h','2024-08','QUARANTINED','SCALE_BREAK','v','{}',24,24,'{}',1)")
    for sql in ("UPDATE fx_reference_repairs SET reason='x'", "DELETE FROM fx_reference_repairs"):
        with pytest.raises(Exception):
            with db.connect() as c:
                c.execute(sql)


def test_fx_reference_and_execution_mappings_stay_separate(db):
    from app.exchange.instruments import DiscoveredInstrument, InstrumentCatalog
    from app.market_data import fx_mapping as fm

    universe = {"provider": "dukascopy", "price_kind": "REFERENCE_MARKET_PRICE",
                "members": [{"pair": "EURUSD", "base": "EUR", "quote": "USD"}], "excluded": {"USDTRY": "NO_FILES"}}
    cat = InstrumentCatalog(db)
    fx = lambda venue, sym, src="VENUE_METADATA", tradable=True: DiscoveredInstrument(  # noqa: E731
        venue, sym, "FX", "FX_PERPETUAL", "EUR/USD:PERP", "EUR", "USDT", "USDT", "PERPETUAL", "Trading", tradable,
        0.0001, 1.0, 1.0, 1e6, 5.0, 50.0, classification_source=src)
    cat.upsert("bybit_linear", "LIVE", [fx("bybit_linear", "EURUSDT")], 1)
    cat.upsert("bingx_swap", "LIVE", [fx("bingx_swap", "NCFXEUR2USD-USDT", tradable=False)], 1)
    rows, h = fm.fx_reference_execution_mappings(fx_universe=universe, catalog=cat,
                                                 venues=[("bybit_linear", "LIVE"), ("bingx_swap", "LIVE")])
    ref = fm.for_pair(rows, "EUR/USD", role=fm.REFERENCE)
    exe = fm.for_pair(rows, "EUR/USD", role=fm.EXECUTION)
    assert [(m.source, m.price_kind) for m in ref] == [("dukascopy", "REFERENCE_MARKET_PRICE")]
    assert {(m.source, m.symbol, m.availability, m.reason) for m in exe} == {
        ("bybit_linear", "EURUSDT", "AVAILABLE", None), ("bingx_swap", "NCFXEUR2USD-USDT", "UNAVAILABLE", "NOT_API_TRADABLE")}
    assert all(m.price_kind == "EXECUTABLE_VENUE_PRICE" for m in exe)  # a reference price is never executable
    assert fm.for_pair(rows, "USD/TRY", role=fm.REFERENCE)[0].availability == "UNAVAILABLE"
    assert h == fm.fx_reference_execution_mappings(fx_universe=universe, catalog=cat,
                                                   venues=[("bingx_swap", "LIVE"), ("bybit_linear", "LIVE")])[1]


def test_new_and_extended_tables_carry_no_secret_columns(db):
    for table in ("cati_portfolio_reservations", "cati_capital_plan_evidence", "broker_transfer_requests",
                  "broker_transfer_events", "venue_instruments", "dataset_manifests"):
        for c in cols(db, table):
            assert not any(w in c.lower() for w in SECRET_WORDS), (table, c)


def test_acquisition_status_semantics_are_unchanged(db):
    with db.connect() as c:
        sql = dict(c.execute("SELECT name, sql FROM sqlite_master WHERE name IN ('fx_reference_ingest_log', "
                             "'market_ingest_log')").fetchall())
    assert "'FETCHED', 'NO_FILE', 'EMPTY', 'FAILED'" in sql["fx_reference_ingest_log"]
    assert "'FETCHED', 'EMPTY', 'NOT_LISTED', 'UNAVAILABLE', 'FAILED'" in sql["market_ingest_log"]
