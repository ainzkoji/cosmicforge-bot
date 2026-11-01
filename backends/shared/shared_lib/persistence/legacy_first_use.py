"""Legacy tables and columns the runtime creates on FIRST USE, not through ``migrate()`` (EXT-DB-01).

``migrate()``, ``production_schema.ensure`` and the runtime-ownership lease create most of the legacy schema; the
creators below are everything else (found by the legacy persistence inventory). Both the inventory
(``scripts/legacy_persistence_inventory.py``) and ``legacy_postgres.create_schema`` run this one list, so a fresh
SQLite file and a fresh PostgreSQL database end up with the same complete schema.

Each creator is ``creator(db, conn)`` and runs the module's REAL schema code -- its own function, or its inline DDL
literal executed verbatim when the DDL sits inside a function that also needs a broker, a lease or research files.
Three modules open their own ``sqlite3`` connections to ``db.path`` (``GlobalAnalytics``, ``TradeTracker``,
``EventStore``); on PostgreSQL their module-level ``sqlite3.connect`` is pointed at the legacy adapter for the
duration of the schema step only (:func:`own_connections_on`). No creator writes a row.
"""
from __future__ import annotations

import ast
import contextlib
import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, List, Tuple

BACKENDS = Path(__file__).resolve().parents[3]

#: errors of the user backend's SQL migration files on a fresh schema (its runner prints and continues)
USER_SQL_MIGRATION_ERRORS: Dict[str, str] = {}


def ddl_from_source(module_file: Path, table: str) -> List[str]:
    """Every string literal of ``module_file`` that creates ``table`` (a DDL written inline in a function)."""
    tree = ast.parse(Path(module_file).read_text(encoding="utf-8"))
    pattern = re.compile(rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(table)}\s*\(", re.I)
    found = [node.value for node in ast.walk(tree)
             if isinstance(node, ast.Constant) and isinstance(node.value, str) and pattern.search(node.value)]
    if not found:
        raise LookupError(f"no CREATE TABLE {table} literal in {module_file}")
    return found


def function_from_source(module_file: Path, name: str) -> Callable:
    """A self-contained module-level function (it only uses its arguments and ``sqlite3``) compiled from the
    source of a package that cannot be imported next to the bot backend (the user backend's ``app``)."""
    text = Path(module_file).read_text(encoding="utf-8")
    node = next(n for n in ast.parse(text).body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace: Dict[str, Any] = {"sqlite3": sqlite3}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(module_file), "exec"), namespace)  # noqa: S102
    return namespace[name]


class _SqliteOnAdapter:
    """Stands in for a module's ``sqlite3`` name: ``connect()`` returns a legacy PostgreSQL adapter connection,
    everything else is the real module."""

    def __init__(self, url: str) -> None:
        self._url = url

    def connect(self, *_args: Any, **_kwargs: Any):
        from shared_lib.persistence import legacy_postgres

        return legacy_postgres.connect(self._url)

    def __getattr__(self, name: str) -> Any:
        return getattr(sqlite3, name)


@contextlib.contextmanager
def own_connections_on(db: Any, module: Any) -> Iterator[None]:
    """While the block runs, ``module``'s own ``sqlite3.connect(...)`` opens ``db``'s database: unchanged on SQLite,
    the legacy adapter on PostgreSQL."""
    url = getattr(db, "_pg_url", None)
    if url is None:
        yield
        return
    original = module.sqlite3
    module.sqlite3 = _SqliteOnAdapter(url)
    try:
        yield
    finally:
        module.sqlite3 = original


def first_use_creators() -> List[Tuple[str, str, Callable[[Any, Any], None]]]:
    """(name, method, creator(db, conn)) for every legacy table created on first use rather than by ``migrate()``.
    ``method`` says how: the module's own function, or its inline DDL literal executed verbatim."""
    app = BACKENDS / "bot-backend" / "app"

    def adaptive(db, conn):
        from app.risk.adaptive_daily_budget import AdaptiveDailyRiskBudgetEngine
        AdaptiveDailyRiskBudgetEngine._create(conn)

    def admin_analytics(db, conn):
        from shared_lib.persistence.admin_analytics import ensure_admin_analytics_snapshot_tables
        ensure_admin_analytics_snapshot_tables(conn)

    def global_analytics(db, conn):
        from shared_lib.persistence import global_analytics as module
        conn.commit()
        with own_connections_on(db, module):
            module.GlobalAnalytics(db_path=db.path)

    def operator_grants(db, conn):
        from shared_lib.billing.operator_grants import ensure_schema
        ensure_schema(conn)

    def auto_trading(db, conn):
        from shared_lib.broker.auto_trading import ensure
        ensure(conn)

    def experimental_demo(db, conn):
        from app.trading_intelligence.governance.experimental_demo import ensure
        ensure(conn)

    def safety_engine(db, conn):
        from app.risk.safety_engine import SafetyEngine
        conn.commit()
        SafetyEngine._init_monitoring_state(SimpleNamespace(db=db))

    def trade_tracker(db, conn):
        from shared_lib.persistence import trade_tracker as module
        conn.commit()
        with own_connections_on(db, module):
            module.TradeTracker(db_path=db.path)

    def event_store(db, conn):
        from shared_lib.persistence import events as module
        conn.commit()
        with own_connections_on(db, module):
            module.EventStore._init_table(SimpleNamespace(_db_path=db.path))  # the singleton's own schema step

    def run_tracker(db, conn):
        from app.ops.run_tracker import RunTracker
        tracker = RunTracker.__new__(RunTracker)
        tracker.db = db
        tracker._ensure_run_summary_schema(conn)
        tracker._ensure_runs_schema(conn)

    def deployment_consents(db, conn):
        from app.core.deployment_service import ensure_schema
        ensure_schema(conn)

    def billing_webhooks(db, conn):
        from shared_lib.billing.webhooks import _ensure_schema
        _ensure_schema(conn)

    user = BACKENDS / "user-backend"

    def user_sql_migrations(db, conn):
        """``user-backend/migrations/run_migration.py``: every ``*.sql`` file in name order through
        ``executescript``; a failing file is reported and the next one applied, as that runner does. That runner
        is a once-per-database operator step (004 rebuilds ``mt_pairing_sessions`` from an explicit column list,
        so running it again drops columns added later): the files are applied only while the table is absent."""
        conn.commit()
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mt_pairing_sessions'").fetchone():
            return
        for sql_file in sorted((user / "migrations").glob("*.sql")):
            try:
                conn.executescript(sql_file.read_text(encoding="utf-8"))
            except sqlite3.DatabaseError as exc:
                USER_SQL_MIGRATION_ERRORS[sql_file.name] = str(exc)[:160]
                conn.rollback()

    def user_lazy_columns(db, conn):
        function_from_source(user / "app" / "api" / "auth.py", "_ensure_totp_counter_column")(conn)
        function_from_source(user / "app" / "core" / "mt_pairing_service.py", "_ensure_claim_columns")(conn)

    def literal(path: Path, table: str):
        def run(db, conn):
            for ddl in ddl_from_source(path, table):
                conn.executescript(ddl) if ddl.count(";") > 1 else conn.execute(ddl)
        return run

    integ = app / "trading_intelligence" / "integration"
    return [
        ("app.risk.adaptive_daily_budget.AdaptiveDailyRiskBudgetEngine._create", "FUNCTION", adaptive),
        ("shared_lib.persistence.admin_analytics.ensure_admin_analytics_snapshot_tables", "FUNCTION", admin_analytics),
        ("shared_lib.persistence.global_analytics.GlobalAnalytics(db_path)", "FUNCTION", global_analytics),
        ("shared_lib.billing.operator_grants.ensure_schema", "FUNCTION", operator_grants),
        ("shared_lib.broker.auto_trading.ensure", "FUNCTION", auto_trading),
        ("app.trading_intelligence.governance.experimental_demo.ensure", "FUNCTION", experimental_demo),
        ("app.risk.safety_engine.SafetyEngine._init_monitoring_state", "FUNCTION", safety_engine),
        ("shared_lib.persistence.trade_tracker.TradeTracker(db_path)", "FUNCTION", trade_tracker),
        ("app.trading_intelligence.integration.residual_prospective.Tracker (inline DDL)", "SOURCE_LITERAL",
         literal(integ / "residual_prospective.py", "cati_residual_tracker")),
        ("app.trading_intelligence.integration.residual_simulation.Book (inline DDL)", "SOURCE_LITERAL",
         literal(integ / "residual_simulation.py", "cati_sim_account")),
        ("app.execution.demo_boundary_certification (inline DDL)", "SOURCE_LITERAL",
         literal(app / "execution" / "demo_boundary_certification.py", "cati_demo_certifications")),
        ("app.execution.demo_transport_smoke (inline DDL)", "SOURCE_LITERAL",
         literal(app / "execution" / "demo_transport_smoke.py", "demo_transport_smoke")),
        ("shared_lib.persistence.events.EventStore._init_table", "FUNCTION", event_store),
        ("app.ops.run_tracker.RunTracker._ensure_run_summary_schema/_ensure_runs_schema", "FUNCTION", run_tracker),
        ("app.core.deployment_service.ensure_schema", "FUNCTION", deployment_consents),
        ("shared_lib.billing.webhooks._ensure_schema", "FUNCTION", billing_webhooks),
        ("user-backend/migrations/*.sql (run_migration.py)", "SQL_FILES", user_sql_migrations),
        ("user-backend auth._ensure_totp_counter_column + mt_pairing_service._ensure_claim_columns", "SOURCE_FUNCTION",
         user_lazy_columns),
    ]


__all__ = ["BACKENDS", "USER_SQL_MIGRATION_ERRORS", "ddl_from_source", "first_use_creators", "function_from_source",
           "own_connections_on"]
