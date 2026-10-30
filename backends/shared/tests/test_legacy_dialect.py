"""The closed SQLite -> PostgreSQL translator of the legacy persistence (EXT-DB-01). Pure: no database needed.

Execution semantics (transactions, REPLACE, triggers, locks, pool) are proven against a real PostgreSQL server in
``bot-backend/tests/test_legacy_postgres_adapter.py``.
"""
from __future__ import annotations

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from shared_lib.persistence.sql import legacy_dialect as D  # noqa: E402

T = D.translate


def norm(text: str) -> str:
    """Whitespace-insensitive form: single spaces, none around parentheses and commas."""
    return re.sub(r"\s*([(),])\s*", lambda m: m.group(1), " ".join(text.split()))


def sql(text: str) -> str:
    return norm(T(text).sql)


# ------------------------------------------------------------------------------------------------ parameters
def test_positional_parameters_become_named_and_literals_are_respected():
    tr = T("SELECT * FROM t WHERE a = ? AND b = '?' AND c LIKE '50%' AND d = ?")
    assert tr.param_names == ("p0", "p1")
    s = " ".join(tr.sql.split())
    assert norm("%(p0)s") in s and norm("%(p1)s") in s and norm("'?'") in s
    assert norm("'50%%'") in s  # a literal % is doubled for psycopg


def test_numbered_and_named_parameters():
    assert T("SELECT ?2, ?1").param_names == ("p0", "p1")
    tr = T("UPDATE t SET a = :a WHERE b = :b AND c = :a")
    assert tr.positional is False and tr.param_names == ("a", "b")


def test_mixed_parameter_styles_are_refused():
    with pytest.raises(D.DialectUnsupported):
        T("SELECT ? , :a")


def test_more_than_one_statement_is_flagged():
    assert T("SELECT 1; SELECT 2").kind == "multiple"
    assert T("SELECT 1;").kind == "query"


# ------------------------------------------------------------------------------------------------ INSERT variants
def test_insert_or_ignore_becomes_on_conflict_do_nothing_before_returning():
    assert sql("INSERT OR IGNORE INTO t(a, b) VALUES (?, ?)").endswith("ON CONFLICT DO NOTHING")
    s = sql("INSERT OR IGNORE INTO t(a) VALUES (?) RETURNING a")
    assert s.index("ON CONFLICT DO NOTHING") < s.index("RETURNING")


def test_insert_or_replace_is_a_plan_with_and_without_column_list():
    tr = T("INSERT OR REPLACE INTO cati_production_state VALUES(?,?,?,?)")
    assert tr.kind == "replace" and tr.plan.columns is None
    assert tr.plan.rows_are_params == [["p0", "p1", "p2", "p3"]]
    tr = T("REPLACE INTO t(a, b) VALUES (?, datetime('now'))")
    assert tr.kind == "replace" and tr.plan.columns == ("a", "b") and "cf_datetime" in tr.plan.rows[0]
    tr = T("INSERT OR REPLACE INTO t(a) SELECT x FROM u WHERE y = ?")
    assert tr.plan.select_sql and tr.param_names == ("p0",)


@pytest.mark.parametrize("stmt", ["INSERT OR FAIL INTO t VALUES (1)", "INSERT OR ROLLBACK INTO t VALUES (1)",
                                  "UPDATE OR IGNORE t SET a = 1", "UPDATE t SET a = 1 LIMIT 1",
                                  "DELETE FROM t ORDER BY a LIMIT 1",
                                  "INSERT OR REPLACE INTO t(a) VALUES (1) RETURNING a"])
def test_unsupported_insert_and_update_forms_raise(stmt):
    with pytest.raises(D.DialectUnsupported):
        T(stmt)


def test_insert_or_abort_is_a_plain_insert_and_upsert_passes_through():
    assert sql("INSERT OR ABORT INTO t(a) VALUES (?)").startswith("INSERT INTO t")
    s = sql("INSERT INTO t(a, b) VALUES (?, ?) ON CONFLICT(a) DO UPDATE SET b = excluded.b")
    assert norm("ON CONFLICT(a) DO UPDATE SET b = excluded.b") in s.replace("( a )", "(a)").replace("ON CONFLICT (a)",
                                                                                                 "ON CONFLICT(a)")


def test_parameter_to_column_mapping_for_real_affinity():
    tr = T("INSERT INTO cati_execution_intents(intent_id, quantity) VALUES (?, ?)")
    assert tr.target_table == "cati_execution_intents" and tr.param_columns == {"p0": "intent_id", "p1": "quantity"}
    tr = T("UPDATE cati_execution_intents SET filled_quantity = ?, state = ? WHERE intent_id = ?")
    assert tr.param_columns == {"p0": "filled_quantity", "p1": "state"}


# ------------------------------------------------------------------------------------------------ PRAGMA / txn
@pytest.mark.parametrize("stmt,name,arg,value", [
    ("PRAGMA table_info(Trade_Fills)", "table_info", "trade_fills", None),
    ("PRAGMA index_list('t')", "index_list", "t", None),
    ("PRAGMA index_info(idx_a)", "index_info", "idx_a", None),
    ("PRAGMA journal_mode=WAL;", "journal_mode", None, "WAL"),
    ("PRAGMA busy_timeout=10000", "busy_timeout", None, "10000"),
    ("PRAGMA user_version", "user_version", None, None),
    ("PRAGMA user_version = 7", "user_version", None, "7"),
])
def test_pragmas_become_plans(stmt, name, arg, value):
    tr = T(stmt)
    assert tr.kind == "pragma" and (tr.plan.name, tr.plan.arg, tr.plan.value) == (name, arg, value)


def test_unknown_pragma_raises():
    with pytest.raises(D.DialectUnsupported):
        T("PRAGMA secure_delete=ON")


@pytest.mark.parametrize("stmt,action", [("BEGIN", "begin"), ("BEGIN DEFERRED TRANSACTION", "begin"),
                                         ("BEGIN IMMEDIATE", "begin_immediate"), ("BEGIN EXCLUSIVE", "begin_immediate"),
                                         ("COMMIT", "commit"), ("END TRANSACTION", "commit"),
                                         ("ROLLBACK", "rollback"), ("SAVEPOINT sp1", "savepoint"),
                                         ("RELEASE SAVEPOINT sp1", "release"), ("RELEASE sp1", "release"),
                                         ("ROLLBACK TO SAVEPOINT sp1", "rollback_to")])
def test_transaction_statements(stmt, action):
    tr = T(stmt)
    assert tr.kind == "txn" and tr.plan.action == action


def test_implicit_begin_rule_mirrors_cpython():
    assert D.is_implicit_begin_statement("  insert into t values (1)")
    assert D.is_implicit_begin_statement("\nREPLACE INTO t VALUES (1)")
    assert D.is_implicit_begin_statement("UPDATE t SET a=1") and D.is_implicit_begin_statement("delete from t")
    assert not D.is_implicit_begin_statement("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x")
    assert not D.is_implicit_begin_statement("SELECT 1")
    assert not D.is_implicit_begin_statement("-- c\nINSERT INTO t VALUES (1)")
    assert not D.is_implicit_begin_statement("CREATE TABLE t(a)")


# ------------------------------------------------------------------------------------------------ functions
@pytest.mark.parametrize("src,needle", [
    ("SELECT datetime('now')", "cf_datetime ( CAST ('now' AS TEXT ) )"),
    ("SELECT date(x, '+1 day')", "cf_date"),
    ("SELECT time(x)", "cf_time"),
    ("SELECT julianday(a) - julianday(b) FROM t", "cf_julianday"),
    ("SELECT strftime('%Y-%m', ts) FROM t", "cf_strftime ( CAST ('%%Y-%%m' AS TEXT )"),
    ("SELECT unixepoch(x)", "cf_unixepoch"),
    ("SELECT IFNULL(a, 0) FROM t", "COALESCE ( a , 0 )"),
    ("SELECT iif(a > 1, 'x', 'y') FROM t", "CASE WHEN a > 1 THEN 'x' ELSE 'y' END"),
    ("SELECT GROUP_CONCAT(a) FROM t", "string_agg ( CAST ( a AS TEXT ) , ',' )"),
    ("SELECT group_concat(DISTINCT a, '|') FROM t", "string_agg ( DISTINCT CAST ( a AS TEXT ) , '|' )"),
    ("SELECT total(a) FROM t", "COALESCE ( CAST ( SUM ( a ) AS DOUBLE PRECISION ) , 0.0 )"),
    ("SELECT ROUND(a, 2) FROM t", "CAST ( ROUND ( CAST ( a AS NUMERIC ) , CAST ( 2 AS INTEGER ) ) AS DOUBLE PRECISION )"),
    ("SELECT instr(a, 'x') FROM t", "strpos ( a , 'x' )"),
    ("SELECT CAST(a AS INTEGER) FROM t", "cf_cast_int ( a )"),
    ("SELECT CAST(a AS REAL) FROM t", "cf_cast_real ( a )"),
    ("SELECT CAST(a AS TEXT) FROM t", "CAST ( a AS TEXT )"),
    ("SELECT CAST(a AS BLOB) FROM t", "CAST ( a AS BYTEA )"),
    ("SELECT json_extract(m, '$.deviceId') FROM t", "cf_json_extract ( CAST ( m AS TEXT ) , '$.deviceId' )"),
    ("SELECT substr(a, 1, 10) FROM t", "substr ( a , 1 , 10 )"),
    ("SELECT trim(a, 'x') FROM t", "btrim ( a , 'x' )"),
])
def test_function_translations(src, needle):
    assert norm(needle) in sql(src)


def test_multi_argument_max_min_keep_sqlite_null_semantics():
    s = sql("SELECT MAX(a, b), MIN(a, ?) FROM t")
    assert norm("CASE WHEN (a ) IS NULL OR (b ) IS NULL THEN NULL ELSE GREATEST") in s
    assert norm("LEAST") in s
    assert sql("SELECT MAX(a) FROM t").startswith(norm("SELECT max ( a )"))


def test_last_insert_rowid_and_changes_are_bound_from_the_connection():
    tr = T("UPDATE t SET p = ? WHERE id = last_insert_rowid()")
    assert tr.needs_lastrowid and "%(cf_lastrowid)s" in tr.sql
    tr = T("SELECT changes()")
    assert tr.needs_changes and "%(cf_changes)s" in tr.sql


@pytest.mark.parametrize("src", ["SELECT random()", "SELECT printf('%d', a) FROM t", "SELECT hex(a) FROM t",
                                 "SELECT quote(a) FROM t", "SELECT a FROM t WHERE a GLOB 'x*'",
                                 "SELECT substr(a, ?) FROM t", "SELECT substr(a, -3) FROM t",
                                 "SELECT a FROM t ORDER BY a COLLATE NOCASE", "ATTACH DATABASE 'x' AS y", "VACUUM",
                                 "EXPLAIN QUERY PLAN SELECT 1", "SELECT json_extract(a, '$.x', '$.y') FROM t",
                                 "SELECT typeof(a) FROM t", "SELECT CAST(a AS DATETIME) FROM t"])
def test_untranslated_constructs_raise_and_are_recorded(src):
    before = len(D.unsupported_events())
    with pytest.raises(D.DialectUnsupported):
        T(src)
    assert len(D.unsupported_events()) == before + 1


# ------------------------------------------------------------------------------------------------ operators
def test_like_is_case_insensitive_without_default_escape():
    assert "a ILIKE %(p0)s ESCAPE ''" in sql("SELECT 1 FROM t WHERE a LIKE ?")
    assert "a NOT ILIKE 'x%%' ESCAPE ''" in sql("SELECT 1 FROM t WHERE a NOT LIKE 'x%'")
    s = sql("SELECT 1 FROM t WHERE a LIKE 'x!%' ESCAPE '!' AND b = 1")
    assert s.count("ESCAPE") == 1 and norm("ESCAPE '!'") in s


def test_is_with_a_value_becomes_distinctness():
    assert "ip IS NOT DISTINCT FROM %(p0)s" in sql("SELECT 1 FROM t WHERE ip IS ?")
    assert "a IS DISTINCT FROM b" in sql("SELECT 1 FROM t WHERE a IS NOT b")
    assert "a IS NULL" in sql("SELECT 1 FROM t WHERE a IS NULL")
    assert "a IS NOT NULL" in sql("SELECT 1 FROM t WHERE a IS NOT NULL")


def test_equality_booleans_and_empty_in():
    s = sql("SELECT 1 FROM t WHERE a == 1 AND b = TRUE AND c = FALSE AND d IS TRUE AND e IN ()")
    assert norm("a = 1") in s and norm("b = 1") in s and norm("c = 0") in s and norm("d IS TRUE") in s
    assert norm("e = ANY ('{}')") in s
    assert norm("f <> ALL ('{}')") in sql("SELECT 1 FROM t WHERE f NOT IN ()")


def test_order_by_sorts_nulls_like_sqlite():
    s = sql("SELECT a FROM t ORDER BY a, b DESC, c ASC, d DESC NULLS FIRST LIMIT 1")
    assert norm("a NULLS FIRST") in s and norm("b DESC NULLS LAST") in s and norm("c ASC NULLS FIRST") in s
    assert norm("d DESC NULLS FIRST") in s and s.count("NULLS") == 4


def test_limit_forms():
    assert "LIMIT 10 OFFSET 5" in sql("SELECT a FROM t LIMIT 5, 10")
    assert "LIMIT ALL" in sql("SELECT a FROM t LIMIT -1 OFFSET 3")
    assert "LIMIT %(p0)s OFFSET %(p1)s" in sql("SELECT a FROM t LIMIT ? OFFSET ?")


def test_select_names_and_rowid():
    s = sql("SELECT COUNT(*), MAX(price) AS Top, x.rowid, rowid, a b FROM t x")
    assert norm('AS "COUNT(*)"') in s and norm('AS "Top"') in s
    assert norm('x.cf_rowid AS "rowid"') in s and norm('cf_rowid AS "rowid"') in s
    assert "ORDER BY cf_rowid DESC NULLS LAST" in sql("SELECT * FROM t ORDER BY rowid DESC")


def test_current_timestamp_is_sqlite_text():
    assert norm("to_char(statement_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')") in sql(
        "SELECT CURRENT_TIMESTAMP")


# ------------------------------------------------------------------------------------------------ DDL
def test_create_table_type_mapping_and_rowid():
    s = sql("""CREATE TABLE IF NOT EXISTS cati_execution_intents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, quantity REAL, max_slippage_bps REAL, blob_col BLOB,
                 flag BOOLEAN DEFAULT FALSE, doc JSON, n INTEGER NOT NULL DEFAULT 0, c TEXT CHECK (c IN ('A','B')),
                 created_at TEXT DEFAULT (datetime('now')), owner TEXT REFERENCES users(id) ON DELETE CASCADE NOT NULL,
                 UNIQUE(n, c), FOREIGN KEY (c) REFERENCES other(x))""")
    assert norm("id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY") in s
    assert norm(f"quantity {D.money_type()}") in s and norm("max_slippage_bps DOUBLE PRECISION") in s
    assert norm("blob_col BYTEA") in s and norm("flag BIGINT DEFAULT 0") in s and norm("doc TEXT") in s
    assert norm("n BIGINT NOT NULL DEFAULT 0") in s and norm("CHECK (c IN ('A','B'))") in s
    assert norm("DEFAULT (cf_datetime") in s
    assert norm("owner TEXT NOT NULL") in s and "REFERENCES" not in s  # foreign keys stay declarative
    assert norm("cf_rowid BIGINT GENERATED ALWAYS AS (id) STORED") in s
    assert norm("UNIQUE (n, c)") in s


def test_table_level_integer_primary_key_is_a_rowid_alias_and_others_get_an_identity():
    s = sql("CREATE TABLE t (id INTEGER, v TEXT, PRIMARY KEY(id))")
    assert norm("id BIGINT GENERATED BY DEFAULT AS IDENTITY") in s and norm("GENERATED ALWAYS AS (id) STORED") in s
    s = sql("CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)")
    assert norm("cf_rowid BIGINT GENERATED BY DEFAULT AS IDENTITY") in s
    tr = T("CREATE TABLE t (k TEXT PRIMARY KEY)")
    assert tr.registry[0][:3] == ("create_table", "table", "t")


@pytest.mark.parametrize("stmt", ["CREATE TABLE t (a TEXT COLLATE NOCASE)", "CREATE TABLE t (a TEXT UNIQUE ON CONFLICT REPLACE)",
                                  "CREATE TABLE t AS SELECT 1", "CREATE TABLE t (a TEXT) WITHOUT ROWID",
                                  "CREATE VIRTUAL TABLE t USING fts5(a)"])
def test_unsupported_ddl_raises(stmt):
    with pytest.raises(D.DialectUnsupported):
        T(stmt)


def test_indexes_alter_and_drop():
    s = sql("CREATE UNIQUE INDEX IF NOT EXISTS ux ON t(a, b DESC) WHERE state IN ('OPEN') AND x IS NOT NULL")
    assert s.startswith(norm("CREATE UNIQUE INDEX IF NOT EXISTS ux ON t (a, b DESC) WHERE state IN"))
    tr = T("ALTER TABLE trade_fills ADD COLUMN slippage_pct REAL")
    assert "ADD COLUMN slippage_pct DOUBLE PRECISION" in tr.sql and tr.registry[0][0] == "add_column"
    tr = T("ALTER TABLE monthly_snapshots_new RENAME TO monthly_snapshots")
    assert isinstance(tr.plan, D.RetypeMoney) and tr.plan.table == "monthly_snapshots"
    assert T("DROP TRIGGER IF EXISTS trg_x").kind == "drop_trigger"
    assert sql("DROP TABLE IF EXISTS t") == "DROP TABLE IF EXISTS t"


def test_triggers_keep_their_guards():
    tr = T("""CREATE TRIGGER IF NOT EXISTS trg_no_update BEFORE UPDATE ON events
              BEGIN SELECT RAISE(ABORT, 'events is append-only (100%)'); END""")
    p = tr.plan
    assert tr.kind == "trigger" and p.if_not_exists and p.table == "events"
    assert f"RAISE EXCEPTION USING ERRCODE = '{D.TRIGGER_ABORT_SQLSTATE}', MESSAGE = 'events is append-only (100%%)'" \
        in p.function_sql
    assert "BEFORE UPDATE ON events FOR EACH ROW" in p.trigger_sql
    tr = T("""CREATE TRIGGER g BEFORE UPDATE OF state ON t WHEN OLD.state = 'X' AND NOT EXISTS (SELECT 1 FROM u)
              BEGIN SELECT RAISE(ABORT, 'no'); END""")
    assert "IS NOT TRUE THEN RETURN NEW" in tr.plan.function_sql and "UPDATE OF state" in tr.plan.trigger_sql
    tr = T("CREATE TRIGGER d BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE); END")
    assert "RETURN NULL;" in tr.plan.function_sql
    tr = T("""CREATE TRIGGER s AFTER UPDATE OF a ON t WHEN OLD.a IS NOT NULL
              BEGIN UPDATE t SET a = CASE WHEN NEW.a IS NULL THEN OLD.a ELSE NEW.a END WHERE k = NEW.k; END""")
    assert "pg_trigger_depth() > 1" in tr.plan.function_sql  # SQLite: a trigger never re-fires itself


def test_split_script_keeps_trigger_bodies_whole():
    parts = D.split_script("""CREATE TABLE a(x); CREATE TRIGGER t AFTER UPDATE ON a BEGIN
        UPDATE a SET x = CASE WHEN NEW.x IS NULL THEN 1 ELSE 2 END; INSERT INTO b VALUES (1); END;
        CREATE INDEX i ON a(x);""")
    assert len(parts) == 3 and parts[1].startswith("CREATE TRIGGER") and parts[1].rstrip().endswith("END")


def test_views():
    tr = T("CREATE VIEW IF NOT EXISTS v AS SELECT COUNT(*) FROM t")
    assert tr.kind == "view" and tr.plan.if_not_exists and 'AS "COUNT(*)"' in tr.plan.sql


# ------------------------------------------------------------------------------------------------ later rules
def test_lone_operands_as_case_conditions_are_tested_for_non_zero():
    s = sql("UPDATE t SET b = CASE WHEN ? THEN 0 ELSE b END, c = CASE s WHEN x THEN 1 END, "
            "d = CASE WHEN t.flag THEN 1 WHEN a > 1 THEN 2 END")
    assert norm("CASE WHEN (%(p0)s <> 0) THEN 0") in s
    assert norm("CASE s WHEN x THEN 1") in s  # a simple CASE compares values: untouched
    assert norm("CASE WHEN (t.flag <> 0) THEN 1 WHEN a > 1 THEN 2") in s


def test_an_untyped_parameter_tested_for_null_is_typed():
    assert norm("OR CAST(%(p1)s AS TEXT) IS NULL") in sql("SELECT 1 FROM t WHERE (a = ? OR ? IS NULL)")
    assert norm("CAST(%(p0)s AS TEXT) IS NOT NULL") in sql("SELECT 1 FROM t WHERE ? IS NOT NULL")


def test_upsert_bare_columns_refer_to_the_existing_row():
    s = sql("INSERT INTO c VALUES(?,?) ON CONFLICT(id) DO UPDATE SET pages=pages+excluded.pages, "
            "n = MAX(n, excluded.n) WHERE c.x > 0 RETURNING pages")
    assert norm("pages=c.pages+excluded.pages") in s
    assert norm("GREATEST((c.n),(excluded.n))") in s
    assert s.endswith("RETURNING pages")


def test_mixed_case_column_names_are_recorded_for_sqlite_result_names():
    tr = T("CREATE TABLE r (decision_id TEXT PRIMARY KEY, gross_R REAL, net_r REAL)")
    assert tr.post_sql == [f"COMMENT ON COLUMN r.gross_r IS '{D.DECLARED_NAME_PREFIX}gross_R'"]
    tr = T("ALTER TABLE r ADD COLUMN cost_R REAL")
    assert tr.post_sql == [f"COMMENT ON COLUMN r.cost_r IS '{D.DECLARED_NAME_PREFIX}cost_R'"]


def test_empty_in_lists_take_the_operand_type():
    assert norm("a = ANY ('{}')") in sql("SELECT 1 FROM t WHERE a IN ()")
    assert norm("a <> ALL ('{}')") in sql("SELECT 1 FROM t WHERE a NOT IN ()")


def test_insert_or_ignore_is_flagged_for_not_null_emulation():
    assert T("INSERT OR IGNORE INTO t(a) VALUES (?)").or_ignore
    assert not T("INSERT INTO t(a) VALUES (?)").or_ignore
