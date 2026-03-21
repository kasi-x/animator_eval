"""SQLite データベース管理."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog

from src.models import (
    Anime,
    AnimeRelation,
    AnimeStudio,
    Character,
    CharacterVoiceActor,
    Credit,
    Person,
    Role,
    ScoreResult,
    Studio,
)
from src.utils.config import DB_DIR

logger = structlog.get_logger()

DEFAULT_DB_PATH = DB_DIR / "animetor_eval.db"

SCHEMA_VERSION = 17


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """SQLite 接続を取得する."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_connection(
    db_path: Path | None = None,
) -> Generator[sqlite3.Connection, None, None]:
    """SQLite 接続のコンテキストマネージャ.

    正常終了時は自動コミット、例外時はロールバック、常にクローズする。

    Usage::

        with db_connection() as conn:
            conn.execute("INSERT ...")
            # auto-commit on success, rollback on exception
    """
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    """テーブルを作成する."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            aliases TEXT NOT NULL DEFAULT '[]',
            mal_id INTEGER,
            anilist_id INTEGER,
            canonical_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            year INTEGER,
            season TEXT,
            episodes INTEGER,
            mal_id INTEGER,
            anilist_id INTEGER,
            score REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT,
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, role, episode)
        );

        CREATE TABLE IF NOT EXISTS scores (
            person_id TEXT PRIMARY KEY,
            person_fe REAL NOT NULL DEFAULT 0.0,
            studio_fe_exposure REAL NOT NULL DEFAULT 0.0,
            birank REAL NOT NULL DEFAULT 0.0,
            patronage REAL NOT NULL DEFAULT 0.0,
            dormancy REAL NOT NULL DEFAULT 1.0,
            awcc REAL NOT NULL DEFAULT 0.0,
            iv_score REAL NOT NULL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS data_sources (
            source TEXT PRIMARY KEY,
            last_scraped_at TIMESTAMP,
            item_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ok'
        );

        CREATE TABLE IF NOT EXISTS characters (
            id TEXT PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            aliases TEXT NOT NULL DEFAULT '[]',
            anilist_id INTEGER,
            image_large TEXT,
            image_medium TEXT,
            description TEXT,
            gender TEXT,
            date_of_birth TEXT,
            age TEXT,
            blood_type TEXT,
            favourites INTEGER,
            site_url TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS character_voice_actors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            character_id TEXT NOT NULL,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            character_role TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(character_id, person_id, anime_id)
        );

        CREATE TABLE IF NOT EXISTS studios (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            anilist_id INTEGER,
            is_animation_studio INTEGER,
            favourites INTEGER,
            site_url TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime_studios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            studio_id TEXT NOT NULL,
            is_main INTEGER NOT NULL DEFAULT 0,
            UNIQUE(anime_id, studio_id)
        );

        CREATE TABLE IF NOT EXISTS anime_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            related_anime_id TEXT NOT NULL,
            relation_type TEXT NOT NULL DEFAULT '',
            related_title TEXT NOT NULL DEFAULT '',
            related_format TEXT,
            UNIQUE(anime_id, related_anime_id, relation_type)
        );

        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
        CREATE INDEX IF NOT EXISTS idx_anime_year ON anime(year);
        CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id);
        CREATE INDEX IF NOT EXISTS idx_cva_character ON character_voice_actors(character_id);
        CREATE INDEX IF NOT EXISTS idx_cva_person ON character_voice_actors(person_id);
        CREATE INDEX IF NOT EXISTS idx_cva_anime ON character_voice_actors(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_anime ON anime_studios(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);
        CREATE TABLE IF NOT EXISTS person_affiliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            studio_name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, studio_name)
        );

        CREATE INDEX IF NOT EXISTS idx_anime_relations_anime ON anime_relations(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_relations_related ON anime_relations(related_anime_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_person ON person_affiliations(person_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_anime ON person_affiliations(anime_id);

        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    conn.commit()
    _run_migrations(conn)


def get_schema_version(conn: sqlite3.Connection) -> int:
    """現在のスキーマバージョンを取得する."""
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0
    except sqlite3.OperationalError:
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    """スキーマバージョンを設定する."""
    conn.execute(
        """INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
        (str(version),),
    )


def _run_migrations(conn: sqlite3.Connection) -> None:
    """未適用のマイグレーションを順番に実行する."""
    current = get_schema_version(conn)
    if current >= SCHEMA_VERSION:
        return

    migrations = {
        1: _migrate_v1_add_score_history,
        2: _migrate_v2_add_score_history_index,
        3: _migrate_v3_add_pipeline_meta,
        4: _migrate_v4_add_studio_column,
        5: _migrate_v5_add_person_metadata,
        6: _migrate_v6_add_anime_metadata,
        7: _migrate_v7_drop_credits_fk,
        8: _migrate_v8_raw_role_unique_and_anime_extra,
        9: _migrate_v9_add_studios_tables,
        10: _migrate_v10_schema_cleanup,
        11: _migrate_v11_add_madb_ids,
        12: _migrate_v12_add_person_fetch_status,
        13: _migrate_v13_add_structural_score_columns,
        14: _migrate_v14_drop_legacy_score_columns,
        15: _migrate_v15_add_va_scores,
        16: _migrate_v16_add_person_affiliations,
        17: _migrate_v17_add_llm_decisions,
    }

    for version in range(current + 1, SCHEMA_VERSION + 1):
        migration_fn = migrations.get(version)
        if migration_fn:
            logger.info("running_migration", version=version)
            migration_fn(conn)
        _set_schema_version(conn, version)
        conn.commit()

    logger.info("schema_up_to_date", version=SCHEMA_VERSION)


def _migrate_v1_add_score_history(conn: sqlite3.Connection) -> None:
    """v1: score_history テーブルを追加."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS score_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            authority REAL NOT NULL DEFAULT 0.0,
            trust REAL NOT NULL DEFAULT 0.0,
            skill REAL NOT NULL DEFAULT 0.0,
            composite REAL NOT NULL DEFAULT 0.0,
            run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v2_add_score_history_index(conn: sqlite3.Connection) -> None:
    """v2: score_history にインデックスを追加."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_score_history_person ON score_history(person_id);
        CREATE INDEX IF NOT EXISTS idx_score_history_run ON score_history(run_at);
    """)


def _migrate_v3_add_pipeline_meta(conn: sqlite3.Connection) -> None:
    """v3: pipeline_runs テーブルを追加."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            credit_count INTEGER DEFAULT 0,
            person_count INTEGER DEFAULT 0,
            elapsed_seconds REAL DEFAULT 0.0,
            mode TEXT DEFAULT 'full'
        );
    """)


def _migrate_v4_add_studio_column(conn: sqlite3.Connection) -> None:
    """v4: anime テーブルに studio カラムを追加."""
    try:
        conn.execute("ALTER TABLE anime ADD COLUMN studio TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # Column already exists


def _migrate_v5_add_person_metadata(conn: sqlite3.Connection) -> None:
    """v5: persons テーブルにメタデータカラムを追加."""
    new_columns = [
        "image_large TEXT",
        "image_medium TEXT",
        "image_large_path TEXT",
        "image_medium_path TEXT",
        "date_of_birth TEXT",
        "age INTEGER",
        "gender TEXT",
        "years_active TEXT DEFAULT '[]'",
        "hometown TEXT",
        "blood_type TEXT",
        "description TEXT",
        "favourites INTEGER",
        "site_url TEXT",
    ]
    for column in new_columns:
        try:
            conn.execute(f"ALTER TABLE persons ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def _migrate_v6_add_anime_metadata(conn: sqlite3.Connection) -> None:
    """v6: anime テーブルにメタデータカラムを追加."""
    new_columns = [
        "cover_large TEXT",
        "cover_extra_large TEXT",
        "cover_medium TEXT",
        "banner TEXT",
        "cover_large_path TEXT",
        "banner_path TEXT",
        "description TEXT",
        "format TEXT",
        "status TEXT",
        "start_date TEXT",
        "end_date TEXT",
        "duration INTEGER",
        "source TEXT",
        "genres TEXT DEFAULT '[]'",
        "tags TEXT DEFAULT '[]'",
        "popularity_rank INTEGER",
        "favourites INTEGER",
        "studios TEXT DEFAULT '[]'",
    ]
    for column in new_columns:
        try:
            conn.execute(f"ALTER TABLE anime ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def _migrate_v7_drop_credits_fk(conn: sqlite3.Connection) -> None:
    """v7: credits テーブルの外部キー制約を削除.

    二段階パイプラインでは credits が persons より先に保存されるため、
    FK 制約があると IntegrityError になる。
    SQLite は ALTER TABLE で FK を削除できないため、テーブル再作成が必要。
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT,
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            UNIQUE(person_id, anime_id, role, episode)
        );
        INSERT OR IGNORE INTO credits_new
            SELECT id, person_id, anime_id, role, NULL as raw_role, episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
    """)


def _migrate_v8_raw_role_unique_and_anime_extra(conn: sqlite3.Connection) -> None:
    """v8: credits の UNIQUE を raw_role ベースに変更 + anime に追加カラム.

    - UNIQUE(person_id, anime_id, role, episode) → UNIQUE(person_id, anime_id, raw_role, episode)
    - raw_role を NOT NULL DEFAULT '' に変更
    - anime テーブルに追加メタデータカラム
    """
    # 1. credits テーブル再作成（raw_role ベース UNIQUE）
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            UNIQUE(person_id, anime_id, raw_role, episode)
        );
        INSERT OR IGNORE INTO credits_new (person_id, anime_id, role, raw_role, episode, source)
            SELECT person_id, anime_id, role, COALESCE(raw_role, ''), episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
    """)

    # 2. anime テーブルに追加カラム
    new_columns = [
        "synonyms TEXT DEFAULT '[]'",
        "mean_score INTEGER",
        "country_of_origin TEXT",
        "is_licensed INTEGER",
        "is_adult INTEGER",
        "hashtag TEXT",
        "site_url TEXT",
        "trailer_url TEXT",
        "trailer_site TEXT",
        "relations_json TEXT",
        "external_links_json TEXT",
        "rankings_json TEXT",
    ]
    for column in new_columns:
        try:
            conn.execute(f"ALTER TABLE anime ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def _migrate_v9_add_studios_tables(conn: sqlite3.Connection) -> None:
    """v9: studios + anime_studios テーブルを追加."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS studios (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            anilist_id INTEGER,
            is_animation_studio INTEGER,
            favourites INTEGER,
            site_url TEXT,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime_studios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            studio_id TEXT NOT NULL,
            is_main INTEGER NOT NULL DEFAULT 0,
            UNIQUE(anime_id, studio_id)
        );

        CREATE INDEX IF NOT EXISTS idx_anime_studios_anime ON anime_studios(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);
    """)


def _migrate_v10_schema_cleanup(conn: sqlite3.Connection) -> None:
    """v10: スキーマ整理.

    - anime.studio (単数) カラム削除
    - credits.episode デフォルト -1 → NULL
    - updated_at カラム追加 (persons, anime, credits, characters, character_voice_actors, studios)
    - persons.canonical_id インデックス追加
    - anime_relations テーブル追加
    """
    # 1. credits テーブル再作成 (updated_at 追加)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, raw_role, episode)
        );
        INSERT OR IGNORE INTO credits_new (person_id, anime_id, role, raw_role, episode, source)
            SELECT person_id, anime_id, role, COALESCE(raw_role, ''),
                   episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
    """)

    # 2. anime.studio (単数) カラム削除
    try:
        conn.execute("ALTER TABLE anime DROP COLUMN studio")
    except sqlite3.OperationalError:
        pass  # Column doesn't exist or SQLite too old

    # 3. updated_at カラム追加
    for table in [
        "persons",
        "anime",
        "characters",
        "character_voice_actors",
        "studios",
    ]:
        try:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )
        except sqlite3.OperationalError:
            pass

    # 4. persons.canonical_id インデックス
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id)"
    )

    # 5. anime_relations テーブル
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS anime_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            related_anime_id TEXT NOT NULL,
            relation_type TEXT NOT NULL DEFAULT '',
            related_title TEXT NOT NULL DEFAULT '',
            related_format TEXT,
            UNIQUE(anime_id, related_anime_id, relation_type)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_relations_anime ON anime_relations(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_relations_related ON anime_relations(related_anime_id);
    """)


def _migrate_v11_add_madb_ids(conn: sqlite3.Connection) -> None:
    """v11: anime/persons テーブルに madb_id カラムを追加."""
    for table in ["anime", "persons"]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN madb_id TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anime_madb_id ON anime(madb_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_madb_id ON persons(madb_id)")


def _migrate_v12_add_person_fetch_status(conn: sqlite3.Connection) -> None:
    """v12: API取得失敗（404等）を記録するテーブルを追加."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS person_fetch_status (
            anilist_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'anilist',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v13_add_structural_score_columns(conn: sqlite3.Connection) -> None:
    """v13: 8-component structural estimation columns to scores and score_history."""
    # Add new columns to scores table
    for col in [
        "person_fe REAL DEFAULT 0.0",
        "studio_fe_exposure REAL DEFAULT 0.0",
        "birank REAL DEFAULT 0.0",
        "patronage REAL DEFAULT 0.0",
        "dormancy REAL DEFAULT 1.0",
        "awcc REAL DEFAULT 0.0",
        "iv_score REAL DEFAULT 0.0",
    ]:
        col_name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE scores ADD COLUMN {col}")
        except sqlite3.OperationalError:
            # Column already exists
            logger.debug("column_already_exists", table="scores", column=col_name)

    # Add new columns to score_history table
    for col in [
        "person_fe REAL DEFAULT 0.0",
        "studio_fe_exposure REAL DEFAULT 0.0",
        "birank REAL DEFAULT 0.0",
        "patronage REAL DEFAULT 0.0",
        "dormancy REAL DEFAULT 1.0",
        "awcc REAL DEFAULT 0.0",
        "iv_score REAL DEFAULT 0.0",
    ]:
        col_name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE score_history ADD COLUMN {col}")
        except sqlite3.OperationalError:
            logger.debug(
                "column_already_exists", table="score_history", column=col_name
            )


def _migrate_v14_drop_legacy_score_columns(conn: sqlite3.Connection) -> None:
    """v14: Drop legacy authority/trust/skill/composite columns from scores/score_history.

    These are replaced by the 8-component structural fields (person_fe, birank, etc.).
    Uses ALTER TABLE DROP COLUMN (SQLite 3.35.0+, 2021-03-12).
    """
    for table in ["scores", "score_history"]:
        for col in ["authority", "trust", "skill", "composite"]:
            try:
                conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
            except sqlite3.OperationalError:
                # Column doesn't exist (fresh DB) or SQLite too old
                logger.debug("drop_column_skipped", table=table, column=col)


def _migrate_v15_add_va_scores(conn: sqlite3.Connection) -> None:
    """v15: Add va_scores table for voice actor evaluation system."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS va_scores (
            person_id TEXT PRIMARY KEY,
            person_fe REAL DEFAULT 0.0,
            sd_fe_exposure REAL DEFAULT 0.0,
            birank REAL DEFAULT 0.0,
            patronage REAL DEFAULT 0.0,
            trust REAL DEFAULT 0.0,
            dormancy REAL DEFAULT 1.0,
            awcc REAL DEFAULT 0.0,
            va_iv_score REAL DEFAULT 0.0,
            character_diversity_index REAL DEFAULT 0.0,
            main_role_count INTEGER DEFAULT 0,
            supporting_role_count INTEGER DEFAULT 0,
            total_characters INTEGER DEFAULT 0,
            casting_tier TEXT DEFAULT 'newcomer',
            replacement_difficulty REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v16_add_person_affiliations(conn: sqlite3.Connection) -> None:
    """v16: person_affiliations テーブルを追加 (人物×作品×所属スタジオ)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS person_affiliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            studio_name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, studio_name)
        );
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_person
            ON person_affiliations(person_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_anime
            ON person_affiliations(anime_id);
    """)


def _migrate_v17_add_llm_decisions(conn: sqlite3.Connection) -> None:
    """v17: LLM判定結果テーブルを追加 (org分類・名前正規化・同一人物判定)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS llm_decisions (
            name TEXT NOT NULL,
            task TEXT NOT NULL,
            result_json TEXT NOT NULL,
            model TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (name, task)
        );
        CREATE INDEX IF NOT EXISTS idx_llm_decisions_task
            ON llm_decisions(task);
    """)


def insert_person_affiliation(
    conn: sqlite3.Connection,
    person_id: str,
    anime_id: str,
    studio_name: str,
    source: str = "",
) -> None:
    """人物の所属スタジオ情報を記録する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO person_affiliations
           (person_id, anime_id, studio_name, source)
           VALUES (?, ?, ?, ?)""",
        (person_id, anime_id, studio_name, source),
    )


def mark_person_unfetchable(
    conn: sqlite3.Connection,
    anilist_id: int,
    status: str = "not_found",
    source: str = "anilist",
) -> None:
    """APIで取得不可だった人物IDを記録する."""
    conn.execute(
        """INSERT INTO person_fetch_status (anilist_id, status, source, updated_at)
           VALUES (?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(anilist_id) DO UPDATE SET
               status = excluded.status,
               updated_at = CURRENT_TIMESTAMP""",
        (anilist_id, status, source),
    )


def get_unfetchable_person_ids(
    conn: sqlite3.Connection, source: str = "anilist"
) -> set[int]:
    """取得不可と記録された人物のanilist_idセットを返す."""
    rows = conn.execute(
        "SELECT anilist_id FROM person_fetch_status WHERE source = ?",
        (source,),
    ).fetchall()
    return {row[0] for row in rows}


def upsert_person(conn: sqlite3.Connection, person: Person) -> None:
    """人物を挿入または更新する（包括的データ対応）."""
    import json

    conn.execute(
        """INSERT INTO persons (
               id, name_ja, name_en, aliases, mal_id, anilist_id, madb_id,
               image_large, image_medium, image_large_path, image_medium_path,
               date_of_birth, age, gender, years_active, hometown, blood_type,
               description, favourites, site_url
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), persons.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), persons.name_en),
               aliases = excluded.aliases,
               mal_id = COALESCE(excluded.mal_id, persons.mal_id),
               anilist_id = COALESCE(excluded.anilist_id, persons.anilist_id),
               madb_id = COALESCE(excluded.madb_id, persons.madb_id),
               image_large = COALESCE(excluded.image_large, persons.image_large),
               image_medium = COALESCE(excluded.image_medium, persons.image_medium),
               image_large_path = COALESCE(excluded.image_large_path, persons.image_large_path),
               image_medium_path = COALESCE(excluded.image_medium_path, persons.image_medium_path),
               date_of_birth = COALESCE(excluded.date_of_birth, persons.date_of_birth),
               age = COALESCE(excluded.age, persons.age),
               gender = COALESCE(excluded.gender, persons.gender),
               years_active = COALESCE(excluded.years_active, persons.years_active),
               hometown = COALESCE(excluded.hometown, persons.hometown),
               blood_type = COALESCE(excluded.blood_type, persons.blood_type),
               description = COALESCE(excluded.description, persons.description),
               favourites = COALESCE(excluded.favourites, persons.favourites),
               site_url = COALESCE(excluded.site_url, persons.site_url)
        """,
        (
            person.id,
            person.name_ja,
            person.name_en,
            json.dumps(person.aliases, ensure_ascii=False),
            person.mal_id,
            person.anilist_id,
            person.madb_id,
            person.image_large,
            person.image_medium,
            person.image_large_path,
            person.image_medium_path,
            person.date_of_birth,
            person.age,
            person.gender,
            json.dumps(person.years_active, ensure_ascii=False),
            person.hometown,
            person.blood_type,
            person.description,
            person.favourites,
            person.site_url,
        ),
    )


def upsert_anime(conn: sqlite3.Connection, anime: Anime) -> None:
    """アニメを挿入または更新する（包括的データ対応）."""
    import json

    conn.execute(
        """INSERT INTO anime (
               id, title_ja, title_en, year, season, episodes, mal_id, anilist_id, madb_id, score,
               cover_large, cover_extra_large, cover_medium, banner, cover_large_path, banner_path,
               description, format, status, start_date, end_date, duration, source,
               genres, tags, popularity_rank, favourites, studios,
               synonyms, mean_score, country_of_origin, is_licensed, is_adult,
               hashtag, site_url, trailer_url, trailer_site,
               relations_json, external_links_json, rankings_json
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?,
                   ?, ?, ?, ?,
                   ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), anime.title_ja),
               title_en = COALESCE(NULLIF(excluded.title_en, ''), anime.title_en),
               year = COALESCE(excluded.year, anime.year),
               season = COALESCE(excluded.season, anime.season),
               episodes = COALESCE(excluded.episodes, anime.episodes),
               mal_id = COALESCE(excluded.mal_id, anime.mal_id),
               anilist_id = COALESCE(excluded.anilist_id, anime.anilist_id),
               madb_id = COALESCE(excluded.madb_id, anime.madb_id),
               score = COALESCE(excluded.score, anime.score),
               cover_large = COALESCE(excluded.cover_large, anime.cover_large),
               cover_extra_large = COALESCE(excluded.cover_extra_large, anime.cover_extra_large),
               cover_medium = COALESCE(excluded.cover_medium, anime.cover_medium),
               banner = COALESCE(excluded.banner, anime.banner),
               cover_large_path = COALESCE(excluded.cover_large_path, anime.cover_large_path),
               banner_path = COALESCE(excluded.banner_path, anime.banner_path),
               description = COALESCE(excluded.description, anime.description),
               format = COALESCE(excluded.format, anime.format),
               status = COALESCE(excluded.status, anime.status),
               start_date = COALESCE(excluded.start_date, anime.start_date),
               end_date = COALESCE(excluded.end_date, anime.end_date),
               duration = COALESCE(excluded.duration, anime.duration),
               source = COALESCE(excluded.source, anime.source),
               genres = COALESCE(excluded.genres, anime.genres),
               tags = COALESCE(excluded.tags, anime.tags),
               popularity_rank = COALESCE(excluded.popularity_rank, anime.popularity_rank),
               favourites = COALESCE(excluded.favourites, anime.favourites),
               studios = COALESCE(excluded.studios, anime.studios),
               synonyms = COALESCE(excluded.synonyms, anime.synonyms),
               mean_score = COALESCE(excluded.mean_score, anime.mean_score),
               country_of_origin = COALESCE(excluded.country_of_origin, anime.country_of_origin),
               is_licensed = COALESCE(excluded.is_licensed, anime.is_licensed),
               is_adult = COALESCE(excluded.is_adult, anime.is_adult),
               hashtag = COALESCE(excluded.hashtag, anime.hashtag),
               site_url = COALESCE(excluded.site_url, anime.site_url),
               trailer_url = COALESCE(excluded.trailer_url, anime.trailer_url),
               trailer_site = COALESCE(excluded.trailer_site, anime.trailer_site),
               relations_json = COALESCE(excluded.relations_json, anime.relations_json),
               external_links_json = COALESCE(excluded.external_links_json, anime.external_links_json),
               rankings_json = COALESCE(excluded.rankings_json, anime.rankings_json),
               updated_at = CURRENT_TIMESTAMP
        """,
        (
            anime.id,
            anime.title_ja,
            anime.title_en,
            anime.year,
            anime.season,
            anime.episodes,
            anime.mal_id,
            anime.anilist_id,
            anime.madb_id,
            anime.score,
            anime.cover_large,
            anime.cover_extra_large,
            anime.cover_medium,
            anime.banner,
            anime.cover_large_path,
            anime.banner_path,
            anime.description,
            anime.format,
            anime.status,
            anime.start_date,
            anime.end_date,
            anime.duration,
            anime.source,
            json.dumps(anime.genres, ensure_ascii=False),
            json.dumps(anime.tags, ensure_ascii=False),
            anime.popularity_rank,
            anime.favourites,
            json.dumps(anime.studios, ensure_ascii=False),
            json.dumps(anime.synonyms, ensure_ascii=False),
            anime.mean_score,
            anime.country_of_origin,
            1 if anime.is_licensed else (0 if anime.is_licensed is not None else None),
            1 if anime.is_adult else (0 if anime.is_adult is not None else None),
            anime.hashtag,
            anime.site_url,
            anime.trailer_url,
            anime.trailer_site,
            anime.relations_json,
            anime.external_links_json,
            anime.rankings_json,
        ),
    )


def insert_credit(conn: sqlite3.Connection, credit: Credit) -> None:
    """クレジットを挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO credits (person_id, anime_id, role, raw_role, episode, source)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            credit.person_id,
            credit.anime_id,
            credit.role.value,
            credit.raw_role or "",  # 元のロール文字列を保存（NOT NULL）
            credit.episode if credit.episode is not None else -1,
            credit.source,
        ),
    )


def upsert_character(conn: sqlite3.Connection, character: Character) -> None:
    """キャラクターを挿入または更新する."""
    import json

    conn.execute(
        """INSERT INTO characters (
               id, name_ja, name_en, aliases, anilist_id,
               image_large, image_medium, description, gender,
               date_of_birth, age, blood_type, favourites, site_url
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), characters.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), characters.name_en),
               aliases = excluded.aliases,
               anilist_id = COALESCE(excluded.anilist_id, characters.anilist_id),
               image_large = COALESCE(excluded.image_large, characters.image_large),
               image_medium = COALESCE(excluded.image_medium, characters.image_medium),
               description = COALESCE(excluded.description, characters.description),
               gender = COALESCE(excluded.gender, characters.gender),
               date_of_birth = COALESCE(excluded.date_of_birth, characters.date_of_birth),
               age = COALESCE(excluded.age, characters.age),
               blood_type = COALESCE(excluded.blood_type, characters.blood_type),
               favourites = COALESCE(excluded.favourites, characters.favourites),
               site_url = COALESCE(excluded.site_url, characters.site_url)
        """,
        (
            character.id,
            character.name_ja,
            character.name_en,
            json.dumps(character.aliases, ensure_ascii=False),
            character.anilist_id,
            character.image_large,
            character.image_medium,
            character.description,
            character.gender,
            character.date_of_birth,
            character.age,
            character.blood_type,
            character.favourites,
            character.site_url,
        ),
    )


def insert_character_voice_actor(
    conn: sqlite3.Connection, cva: CharacterVoiceActor
) -> None:
    """キャラクター×声優×作品の関係を挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO character_voice_actors
           (character_id, person_id, anime_id, character_role, source)
           VALUES (?, ?, ?, ?, ?)""",
        (cva.character_id, cva.person_id, cva.anime_id, cva.character_role, cva.source),
    )


def upsert_studio(conn: sqlite3.Connection, studio: Studio) -> None:
    """スタジオを挿入または更新する."""
    conn.execute(
        """INSERT INTO studios (id, name, anilist_id, is_animation_studio, favourites, site_url)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name = COALESCE(NULLIF(excluded.name, ''), studios.name),
               anilist_id = COALESCE(excluded.anilist_id, studios.anilist_id),
               is_animation_studio = COALESCE(excluded.is_animation_studio, studios.is_animation_studio),
               favourites = COALESCE(excluded.favourites, studios.favourites),
               site_url = COALESCE(excluded.site_url, studios.site_url)
        """,
        (
            studio.id,
            studio.name,
            studio.anilist_id,
            1
            if studio.is_animation_studio
            else (0 if studio.is_animation_studio is not None else None),
            studio.favourites,
            studio.site_url,
        ),
    )


def insert_anime_studio(conn: sqlite3.Connection, anime_studio: AnimeStudio) -> None:
    """アニメ×スタジオの関係を挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main)
           VALUES (?, ?, ?)""",
        (
            anime_studio.anime_id,
            anime_studio.studio_id,
            1 if anime_studio.is_main else 0,
        ),
    )


def insert_anime_relation(conn: sqlite3.Connection, relation: AnimeRelation) -> None:
    """アニメ間の関連を挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO anime_relations
           (anime_id, related_anime_id, relation_type, related_title, related_format)
           VALUES (?, ?, ?, ?, ?)""",
        (
            relation.anime_id,
            relation.related_anime_id,
            relation.relation_type,
            relation.related_title,
            relation.related_format,
        ),
    )


def upsert_score(conn: sqlite3.Connection, score: ScoreResult) -> None:
    """スコアを挿入または更新する."""
    conn.execute(
        """INSERT INTO scores
               (person_id, person_fe, studio_fe_exposure, birank,
                patronage, dormancy, awcc, iv_score)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(person_id) DO UPDATE SET
               person_fe = excluded.person_fe,
               studio_fe_exposure = excluded.studio_fe_exposure,
               birank = excluded.birank,
               patronage = excluded.patronage,
               dormancy = excluded.dormancy,
               awcc = excluded.awcc,
               iv_score = excluded.iv_score,
               updated_at = CURRENT_TIMESTAMP
        """,
        (
            score.person_id,
            score.person_fe,
            score.studio_fe_exposure,
            score.birank,
            score.patronage,
            score.dormancy,
            score.awcc,
            score.iv_score,
        ),
    )


def load_all_persons(conn: sqlite3.Connection) -> list[Person]:
    """全人物を読み込む."""
    import json

    rows = conn.execute("SELECT * FROM persons").fetchall()
    columns = set(rows[0].keys()) if rows else set()
    result = []
    for row in rows:
        kwargs: dict = {
            "id": row["id"],
            "name_ja": row["name_ja"],
            "name_en": row["name_en"],
            "aliases": json.loads(row["aliases"]),
            "mal_id": row["mal_id"],
            "anilist_id": row["anilist_id"],
        }
        if "madb_id" in columns:
            kwargs["madb_id"] = row["madb_id"]
        result.append(Person(**kwargs))
    return result


def load_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """全アニメを読み込む."""
    import json as _json

    rows = conn.execute("SELECT * FROM anime").fetchall()
    columns = set(rows[0].keys()) if rows else set()
    result = []
    for row in rows:
        kwargs: dict = {
            "id": row["id"],
            "title_ja": row["title_ja"],
            "title_en": row["title_en"],
            "year": row["year"],
            "season": row["season"],
            "episodes": row["episodes"],
            "mal_id": row["mal_id"],
            "anilist_id": row["anilist_id"],
            "score": row["score"],
        }
        # v6+ カラム
        if "studios" in columns:
            try:
                kwargs["studios"] = _json.loads(row["studios"] or "[]")
            except (ValueError, TypeError):
                kwargs["studios"] = []
        if "genres" in columns:
            try:
                kwargs["genres"] = _json.loads(row["genres"] or "[]")
            except (ValueError, TypeError):
                kwargs["genres"] = []
        if "tags" in columns:
            try:
                kwargs["tags"] = _json.loads(row["tags"] or "[]")
            except (ValueError, TypeError):
                kwargs["tags"] = []
        # スカラーカラム（存在すれば読む）
        for col in [
            "format",
            "status",
            "source",
            "description",
            "start_date",
            "end_date",
            "duration",
            "cover_large",
            "cover_extra_large",
            "cover_medium",
            "banner",
            "popularity_rank",
            "favourites",
            "mean_score",
            "country_of_origin",
            "is_licensed",
            "is_adult",
            "hashtag",
            "site_url",
            "trailer_url",
            "trailer_site",
            "relations_json",
            "external_links_json",
            "rankings_json",
            "madb_id",
        ]:
            if col in columns:
                kwargs[col] = row[col]
        if "synonyms" in columns:
            try:
                kwargs["synonyms"] = _json.loads(row["synonyms"] or "[]")
            except (ValueError, TypeError):
                kwargs["synonyms"] = []
        result.append(Anime(**kwargs))
    return result


_LEGACY_ROLE_MAP: dict[str, str] = {
    "chief_animation_director": "animation_director",
    "storyboard": "episode_director",
    "mechanical_designer": "character_designer",
    "art_director": "background_art",
    "color_designer": "finishing",
    "effects": "photography_director",
    "theme_song": "music",
    "series_composition": "screenplay",
    "adr": "voice_actor",
    "other": "special",
}


def load_all_credits(conn: sqlite3.Connection) -> list[Credit]:
    """全クレジットを読み込む."""
    rows = conn.execute("SELECT * FROM credits").fetchall()
    credits: list[Credit] = []
    for row in rows:
        role_str = row["role"]
        role_str = _LEGACY_ROLE_MAP.get(role_str, role_str)
        credits.append(Credit(
            person_id=row["person_id"],
            anime_id=row["anime_id"],
            role=Role(role_str),
            raw_role=row["raw_role"] or None,
            episode=row["episode"] if row["episode"] != -1 else None,
            source=row["source"],
        ))
    return credits


def get_db_stats(conn: sqlite3.Connection) -> dict[str, int | float]:
    """DB統計情報を取得する."""
    stats: dict[str, int | float] = {}

    for table in ("persons", "anime", "credits", "scores"):
        stats[f"{table}_count"] = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]  # noqa: S608

    # 役職分布
    role_counts = conn.execute("""
        SELECT role, COUNT(*) as cnt FROM credits
        GROUP BY role ORDER BY cnt DESC
    """).fetchall()
    stats["distinct_roles"] = len(role_counts)

    # 年代カバレッジ
    year_range = conn.execute("""
        SELECT MIN(year), MAX(year) FROM anime WHERE year IS NOT NULL
    """).fetchone()
    if year_range[0]:
        stats["year_min"] = year_range[0]
        stats["year_max"] = year_range[1]

    # ソース別クレジット数
    source_counts = conn.execute("""
        SELECT source, COUNT(*) as cnt FROM credits
        GROUP BY source ORDER BY cnt DESC
    """).fetchall()
    for source, cnt in source_counts:
        stats[f"credits_source_{source or 'unknown'}"] = cnt

    # 平均クレジット数/人
    avg = conn.execute("""
        SELECT AVG(cnt) FROM (
            SELECT COUNT(*) as cnt FROM credits GROUP BY person_id
        )
    """).fetchone()[0]
    if avg:
        stats["avg_credits_per_person"] = round(avg, 1)

    return stats


def search_persons(
    conn: sqlite3.Connection,
    query: str,
    limit: int = 20,
) -> list[dict]:
    """名前またはIDで人物を検索する.

    Args:
        conn: DB接続
        query: 検索文字列（部分一致）
        limit: 最大件数

    Returns:
        [{id, name_ja, name_en, iv_score, credit_count}]
    """
    pattern = f"%{query}%"
    rows = conn.execute(
        """SELECT p.id, p.name_ja, p.name_en,
                  s.iv_score, s.person_fe, s.birank, s.patronage,
                  (SELECT COUNT(*) FROM credits c WHERE c.person_id = p.id) as credit_count
           FROM persons p
           LEFT JOIN scores s ON p.id = s.person_id
           WHERE p.name_ja LIKE ? OR p.name_en LIKE ? COLLATE NOCASE OR p.id LIKE ?
           ORDER BY s.iv_score DESC NULLS LAST
           LIMIT ?""",
        (pattern, pattern, pattern, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def update_data_source(
    conn: sqlite3.Connection,
    source: str,
    item_count: int,
    status: str = "ok",
) -> None:
    """データソースの最終取得日時を更新する."""
    conn.execute(
        """INSERT INTO data_sources (source, last_scraped_at, item_count, status)
           VALUES (?, CURRENT_TIMESTAMP, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
               last_scraped_at = CURRENT_TIMESTAMP,
               item_count = excluded.item_count,
               status = excluded.status
        """,
        (source, item_count, status),
    )


def get_data_sources(conn: sqlite3.Connection) -> list[dict]:
    """全データソースの情報を取得する."""
    rows = conn.execute(
        "SELECT source, last_scraped_at, item_count, status FROM data_sources ORDER BY source"
    ).fetchall()
    return [dict(r) for r in rows]


def load_all_scores(conn: sqlite3.Connection) -> list[ScoreResult]:
    """全スコアを読み込む."""
    rows = conn.execute("SELECT * FROM scores").fetchall()
    cols = set(rows[0].keys()) if rows else set()
    result = []
    for row in rows:
        kwargs: dict = {"person_id": row["person_id"]}
        for field in ("person_fe", "studio_fe_exposure", "birank", "patronage",
                       "dormancy", "awcc", "iv_score"):
            if field in cols:
                kwargs[field] = row[field]
        result.append(ScoreResult(**kwargs))
    return result


def load_all_characters(conn: sqlite3.Connection) -> list:
    """Load all characters from the database.

    Returns:
        List of Character objects.
    """
    from src.models import Character

    try:
        rows = conn.execute("SELECT * FROM characters").fetchall()
    except Exception:
        return []
    if not rows:
        return []

    result = []
    cols = set(rows[0].keys())
    for row in rows:
        kwargs: dict = {"id": row["id"]}
        for field in ("name_ja", "name_en", "anilist_id", "image_large",
                       "image_medium", "description", "gender",
                       "date_of_birth", "age", "blood_type",
                       "favourites", "site_url"):
            if field in cols and row[field] is not None:
                kwargs[field] = row[field]
        if "aliases" in cols and row["aliases"]:
            import json as _json
            try:
                kwargs["aliases"] = _json.loads(row["aliases"])
            except (ValueError, TypeError):
                pass
        result.append(Character(**kwargs))
    return result


def load_all_voice_actor_credits(conn: sqlite3.Connection) -> list:
    """Load all character_voice_actors records.

    Returns:
        List of CharacterVoiceActor objects.
    """
    from src.models import CharacterVoiceActor

    try:
        rows = conn.execute("SELECT * FROM character_voice_actors").fetchall()
    except Exception:
        return []
    if not rows:
        return []

    result = []
    for row in rows:
        result.append(
            CharacterVoiceActor(
                character_id=row["character_id"],
                person_id=row["person_id"],
                anime_id=row["anime_id"],
                character_role=row["character_role"] or "",
                source=row.get("source", "") or "",
            )
        )
    return result


def record_pipeline_run(
    conn: sqlite3.Connection,
    credit_count: int,
    person_count: int,
    elapsed: float,
    mode: str = "full",
) -> int:
    """パイプライン実行を記録する."""
    cursor = conn.execute(
        """INSERT INTO pipeline_runs (credit_count, person_count, elapsed_seconds, mode)
           VALUES (?, ?, ?, ?)""",
        (credit_count, person_count, elapsed, mode),
    )
    return cursor.lastrowid or 0


def get_last_pipeline_run(conn: sqlite3.Connection) -> dict | None:
    """最後のパイプライン実行情報を取得する."""
    row = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def has_credits_changed_since_last_run(conn: sqlite3.Connection) -> bool:
    """最後のパイプライン実行以降にクレジットデータが変化したか判定する.

    credit_count と person_count を比較して変化を検出する。
    前回のパイプライン実行が存在しない場合は True を返す。

    Returns:
        True: クレジットまたは人物数が変化した、またはパイプライン未実行
        False: データに変化なし
    """
    last_run = get_last_pipeline_run(conn)
    if last_run is None:
        return True

    current_credit_count = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
    if current_credit_count != last_run["credit_count"]:
        return True

    current_person_count = conn.execute(
        "SELECT COUNT(DISTINCT person_id) FROM credits"
    ).fetchone()[0]
    if current_person_count != last_run.get("person_count", -1):
        return True

    return False


def get_persons_with_new_credits(
    conn: sqlite3.Connection,
    since_run_id: int,
) -> set[str]:
    """指定パイプライン実行以降に新しいクレジットが追加された人物IDを返す.

    新クレジットは id > (last run の最大 credit id) で近似する。
    """
    # Get max credit ID at the time of the reference run
    max_credit = conn.execute(
        """SELECT MAX(c.id) FROM credits c
           JOIN pipeline_runs pr ON pr.id = ?
           WHERE c.id <= (SELECT COALESCE(
               (SELECT MAX(id) FROM credits WHERE rowid <=
                   (SELECT credit_count FROM pipeline_runs WHERE id = ?)),
               (SELECT MAX(id) FROM credits)
           ))""",
        (since_run_id, since_run_id),
    ).fetchone()[0]

    if max_credit is None:
        # No reference point, all persons are "new"
        rows = conn.execute("SELECT DISTINCT person_id FROM credits").fetchall()
        return {r["person_id"] for r in rows}

    rows = conn.execute(
        "SELECT DISTINCT person_id FROM credits WHERE id > ?", (max_credit,)
    ).fetchall()
    return {r["person_id"] for r in rows}


def save_score_history(conn: sqlite3.Connection, score: ScoreResult) -> None:
    """スコア履歴を保存する."""
    conn.execute(
        """INSERT INTO score_history
               (person_id, person_fe, studio_fe_exposure, birank,
                patronage, dormancy, awcc, iv_score)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            score.person_id,
            score.person_fe,
            score.studio_fe_exposure,
            score.birank,
            score.patronage,
            score.dormancy,
            score.awcc,
            score.iv_score,
        ),
    )


def get_score_history(
    conn: sqlite3.Connection,
    person_id: str,
    limit: int = 50,
) -> list[dict]:
    """人物のスコア履歴を取得する（新しい順）."""
    rows = conn.execute(
        """SELECT person_fe, studio_fe_exposure, birank,
                  patronage, dormancy, awcc, iv_score, run_at
           FROM score_history
           WHERE person_id = ?
           ORDER BY id DESC
           LIMIT ?""",
        (person_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_persons(conn: sqlite3.Connection) -> list[Person]:
    """全人物データを取得する."""
    import json

    rows = conn.execute("SELECT * FROM persons").fetchall()
    return [
        Person(
            id=row["id"],
            name_ja=row["name_japanese"],
            name_en=row["name_english"],
            source=row["source"],
            mal_id=row["mal_id"],
            anilist_id=row["anilist_id"],
            aliases=json.loads(row["aliases"]) if row["aliases"] else [],
        )
        for row in rows
    ]


def get_all_person_ids(conn: sqlite3.Connection) -> set[str]:
    """既存の全人物IDを高速取得する（スキップ判定用）."""
    rows = conn.execute("SELECT id FROM persons").fetchall()
    return {row["id"] for row in rows}


def get_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """全アニメデータを取得する."""
    rows = conn.execute("SELECT * FROM anime").fetchall()
    return [
        Anime(
            id=row["id"],
            title_ja=row["title_japanese"],
            title_en=row["title_english"],
            year=row["year"],
            season=row["season"],
            episodes=row["episodes"],
            format=row["format"],
            source=row["source"],
            score=row["score"],
            mal_id=row["mal_id"],
            anilist_id=row["anilist_id"],
        )
        for row in rows
    ]


def get_all_credits(conn: sqlite3.Connection) -> list[Credit]:
    """全クレジットデータを取得する."""
    from src.models import Role

    rows = conn.execute("SELECT * FROM credits").fetchall()
    return [
        Credit(
            person_id=row["person_id"],
            anime_id=row["anime_id"],
            role=Role(row["role"]),
            episode=row["episode"] if row["episode"] != -1 else None,
            source=row["source"],
        )
        for row in rows
    ]


def get_all_scores(conn: sqlite3.Connection) -> list[ScoreResult]:
    """全スコアデータを取得する."""
    rows = conn.execute("SELECT * FROM scores").fetchall()
    cols = set(rows[0].keys()) if rows else set()
    result = []
    for row in rows:
        kwargs: dict = {"person_id": row["person_id"]}
        for field in ("person_fe", "studio_fe_exposure", "birank", "patronage",
                       "dormancy", "awcc", "iv_score"):
            if field in cols:
                kwargs[field] = row[field]
        result.append(ScoreResult(**kwargs))
    return result


# ---------------------------------------------------------------------------
# LLM decision cache — DB-backed persistence
# ---------------------------------------------------------------------------


def get_llm_decision(
    conn: sqlite3.Connection, name: str, task: str
) -> dict | None:
    """LLM判定キャッシュを取得する.

    Args:
        name: 対象の名前 (人物名・ペア名)
        task: タスク種別 ("org_classification" | "name_normalization" | "entity_match")

    Returns:
        result_json を dict にパースした結果、なければ None
    """
    import json

    row = conn.execute(
        "SELECT result_json FROM llm_decisions WHERE name = ? AND task = ?",
        (name, task),
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(row["result_json"])
    except (json.JSONDecodeError, TypeError):
        return None


def upsert_llm_decision(
    conn: sqlite3.Connection,
    name: str,
    task: str,
    result: dict,
    model: str = "",
) -> None:
    """LLM判定結果を保存/更新する."""
    import json

    conn.execute(
        """INSERT INTO llm_decisions (name, task, result_json, model, updated_at)
           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(name, task) DO UPDATE SET
               result_json = excluded.result_json,
               model = excluded.model,
               updated_at = CURRENT_TIMESTAMP""",
        (name, task, json.dumps(result, ensure_ascii=False), model),
    )


def get_all_llm_decisions(
    conn: sqlite3.Connection, task: str
) -> dict[str, dict]:
    """指定タスクの全LLM判定キャッシュを一括取得する.

    Returns:
        {name: result_dict} の辞書
    """
    import json

    rows = conn.execute(
        "SELECT name, result_json FROM llm_decisions WHERE task = ?",
        (task,),
    ).fetchall()
    result: dict[str, dict] = {}
    for row in rows:
        try:
            result[row["name"]] = json.loads(row["result_json"])
        except (json.JSONDecodeError, TypeError):
            continue
    return result
