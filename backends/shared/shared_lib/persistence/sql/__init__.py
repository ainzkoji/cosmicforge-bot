"""Dialect-neutral (SQLite / PostgreSQL) persistence for the platform layer. See ``database.py``."""
from .database import (POSTGRESQL, SQLITE, Connection, Database, DatabaseUnavailable, PoolExhausted, decimal_in,
                       decimal_out, dialect_of)
from .migrations import (ChecksumMismatch, Compatibility, IrreversibleMigration, Migration, MigrationError,
                         compatibility, current_version, migrate, rollback)

__all__ = ["POSTGRESQL", "SQLITE", "Connection", "Database", "DatabaseUnavailable", "PoolExhausted", "decimal_in",
           "decimal_out", "dialect_of", "ChecksumMismatch", "Compatibility", "IrreversibleMigration", "Migration",
           "MigrationError", "compatibility", "current_version", "migrate", "rollback", "platform_database"]


def platform_database(url: str, **kw) -> Database:
    """A database with every platform migration applied."""
    from .platform_schema import all_migrations
    db = Database(url, **kw)
    migrate(db, all_migrations())
    return db
