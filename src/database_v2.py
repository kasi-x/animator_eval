"""DEPRECATED: Use src.db modules instead.

This module is maintained for backward compatibility only.
Schema initialization is now in src.db.schema.
SQLAlchemy engine creation is for documentation generation only (scripts/generate_dbml.py).

Deprecation timeline:
- generate_dbml.py: migrate to DuckDB schema inspection (Atlas config)
- init_db: already migrated to use src.db.schema.init_db_v2
"""

from __future__ import annotations

import sqlite3
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine as create_sqlmodel_engine

from src.models_v2 import SQLModel

log = structlog.get_logger(__name__)

# Default database path (shared with src/database.py)
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "animetor.db"

warnings.warn(
    "src.database_v2 is deprecated; use src.db modules instead",
    DeprecationWarning,
    stacklevel=2,
)


def create_sqlalchemy_engine(db_path: Path | None = None) -> Engine:
    """Create SQLAlchemy engine with SQLite + WAL optimizations.

    Args:
        db_path: Path to database file. Defaults to DEFAULT_DB_PATH.

    Returns:
        SQLAlchemy Engine instance configured for SQLite WAL mode.
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create engine with check_same_thread=False for multi-threaded environments
    engine = create_sqlmodel_engine(
        f"sqlite:///{db_path}",
        echo=False,
        connect_args={"check_same_thread": False, "timeout": 30.0},
    )

    # Enable WAL mode (Write-Ahead Logging) for better concurrency
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        """Apply SQLite pragmas for optimal performance."""
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=-64000")
        cursor.close()

    return engine


def get_session(db_path: Path | None = None) -> Session:
    """Get a new SQLModel session.

    Args:
        db_path: Path to database file. Defaults to DEFAULT_DB_PATH.

    Returns:
        SQLModel Session instance.
    """
    engine = create_sqlalchemy_engine(db_path)
    return Session(engine)


@contextmanager
def session_scope(db_path: Path | None = None) -> Generator[Session, None, None]:
    """Context manager for SQLModel sessions.

    Usage::

        with session_scope() as session:
            result = session.query(Anime).filter(...).all()

    Args:
        db_path: Path to database file. Defaults to DEFAULT_DB_PATH.

    Yields:
        SQLModel Session instance. Commits on success, rolls back on exception.
    """
    session = get_session(db_path)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db_v2(conn: sqlite3.Connection) -> None:
    """DEPRECATED: Use src.db.schema.init_db_v2 instead."""
    warnings.warn(
        "src.database_v2.init_db_v2 is deprecated; use src.db.schema.init_db_v2",
        DeprecationWarning,
        stacklevel=2,
    )
    from src.db.schema import init_db_v2 as _init_db_v2
    _init_db_v2(conn)
