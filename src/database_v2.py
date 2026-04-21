"""SQLModel + SQLAlchemy database connection management (v2 - Atlas-compatible).

This module provides:
1. SQLAlchemy engine creation with WAL mode
2. SQLModel session factory for ORM operations
3. Integration with existing sqlite3 code (dual-stack support)
4. Migration execution via Atlas
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine as create_sqlmodel_engine

from src.models_v2 import SQLModel

log = structlog.get_logger(__name__)

# Default database path (shared with src/database.py)
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "animetor.db"


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


def init_db_v2(db_path: Path | None = None) -> None:
    """Initialize database schema using SQLModel (v2 approach).

    This function:
    1. Creates SQLAlchemy engine
    2. Creates all tables defined in src.models_v2
    3. Seeds lookup tables (roles, sources)

    Args:
        db_path: Path to database file. Defaults to DEFAULT_DB_PATH.

    Note:
        For production use, prefer Atlas migrations via CLI:
        `atlas migrate apply --env prod`
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH

    engine = create_sqlalchemy_engine(db_path)

    log.info("init_db_v2_start", db_path=str(db_path))

    # Create all tables
    SQLModel.metadata.create_all(engine)

    # Seed lookup tables
    with session_scope(db_path) as session:
        # Seed roles (24 standardized roles)
        from src.models_v2 import Roles

        roles_seed = [
            Roles(code="director", name_ja="監督", name_en="Director", weight=2.0),
            Roles(
                code="animation_director",
                name_ja="作画監督",
                name_en="Animation Director",
                weight=1.8,
            ),
            Roles(code="key_animator", name_ja="原画", name_en="Key Animator", weight=1.5),
            Roles(
                code="second_key_animator",
                name_ja="第二原画",
                name_en="Second Key Animator",
                weight=1.2,
            ),
            Roles(code="in_between", name_ja="動画", name_en="In-Between", weight=0.8),
            Roles(
                code="episode_director",
                name_ja="演出",
                name_en="Episode Director",
                weight=1.6,
            ),
            Roles(
                code="character_designer",
                name_ja="キャラクターデザイン",
                name_en="Character Designer",
                weight=1.7,
            ),
            Roles(
                code="photography_director",
                name_ja="撮影監督",
                name_en="Photography Director",
                weight=1.3,
            ),
            Roles(code="producer", name_ja="プロデューサー", name_en="Producer", weight=1.5),
            Roles(
                code="production_manager",
                name_ja="制作進行",
                name_en="Production Manager",
                weight=0.9,
            ),
            Roles(
                code="sound_director", name_ja="音響監督", name_en="Sound Director", weight=1.4
            ),
            Roles(code="music", name_ja="音楽", name_en="Music", weight=1.2),
            Roles(code="screenplay", name_ja="脚本", name_en="Screenplay", weight=1.6),
            Roles(
                code="original_creator",
                name_ja="原作者",
                name_en="Original Creator",
                weight=1.5,
            ),
            Roles(
                code="background_art",
                name_ja="背景美術",
                name_en="Background Art",
                weight=1.1,
            ),
            Roles(code="cgi_director", name_ja="CGI監督", name_en="CGI Director", weight=1.3),
            Roles(code="layout", name_ja="レイアウト", name_en="Layout", weight=1.2),
            Roles(
                code="finishing",
                name_ja="仕上げ",
                name_en="Finishing/Color Design",
                weight=1.0,
            ),
            Roles(code="editing", name_ja="編集", name_en="Editing", weight=1.1),
            Roles(code="settings", name_ja="設定", name_en="Settings", weight=1.0),
            Roles(code="voice_actor", name_ja="声優", name_en="Voice Actor", weight=1.0),
            Roles(code="localization", name_ja="ローカライズ", name_en="Localization", weight=0.8),
            Roles(code="other", name_ja="その他", name_en="Other", weight=0.5),
            Roles(code="special", name_ja="スペシャル", name_en="Special", weight=0.5),
        ]

        for role in roles_seed:
            existing = session.query(Roles).filter(Roles.code == role.code).first()
            if not existing:
                session.add(role)

        # Seed sources (data sources)
        from src.models_v2 import Sources

        sources_seed = [
            Sources(
                code="anilist",
                name_ja="AniList",
                base_url="https://anilist.co",
                license="proprietary",
                description="Structured staff info, highest quality data source",
            ),
            Sources(
                code="mal",
                name_ja="MyAnimeList",
                base_url="https://myanimelist.net",
                license="proprietary",
                description="Staff credits with role information",
            ),
            Sources(
                code="ann",
                name_ja="Anime News Network",
                base_url="https://www.animenewsnetwork.com",
                license="proprietary",
                description="Deep historical staff records and role granularity",
            ),
            Sources(
                code="allcinema",
                name_ja="allcinema",
                base_url="https://www.allcinema.net",
                license="proprietary",
                description="Comprehensive Japanese film/OVA database",
            ),
            Sources(
                code="seesaawiki",
                name_ja="SeesaaWiki",
                base_url="https://seesaawiki.jp",
                license="CC-BY-SA",
                description="Fan-curated episode-level production information",
            ),
            Sources(
                code="keyframe",
                name_ja="Sakugabooru/Keyframe",
                base_url="https://www.sakugabooru.com",
                license="CC",
                description="Animation sakuga community staff database",
            ),
        ]

        for source in sources_seed:
            existing = session.query(Sources).filter(Sources.code == source.code).first()
            if not existing:
                session.add(source)

        session.commit()

    log.info("init_db_v2_complete", roles_seeded=len(roles_seed), sources_seeded=len(sources_seed))


def sync_db_schema(db_path: Path | None = None) -> None:
    """Sync database schema to match SQLModel definitions.

    This is a convenience function for dev environments. For production,
    use Atlas migrations via CLI.

    Args:
        db_path: Path to database file. Defaults to DEFAULT_DB_PATH.
    """
    log.warning("sync_db_schema called - only for development use")
    if db_path is None:
        db_path = DEFAULT_DB_PATH

    engine = create_sqlalchemy_engine(db_path)
    SQLModel.metadata.create_all(engine)
    log.info("schema_synced", db_path=str(db_path))
