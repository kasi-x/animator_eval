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
from src.utils.config import DB_PATH

logger = structlog.get_logger()

DEFAULT_DB_PATH = DB_PATH

SCHEMA_VERSION = 26

# Fuzzy match rules for unmatched anime titles (90%+ confidence)
# Entries where SeesaaWiki title slightly differs from AniList title
_FUZZY_MATCH_RULES = {
    "AKIBA'S TRIP -THE ANIMATION-": ("Akiba's Trip -The Animation-", 2017),
    "Vivy -Fluorite Eye's Song-": ("Vivy -Fluorite Eye's Song-", 2021),
    "妖怪ウォッチJam 妖怪学園Y 〜Nとの遭遇〜": ("妖怪ウォッチJam: 妖怪学園Y 〜Nとの遭遇〜", 2020),
    "ウルトラヴァイオレット：コード044": ("ウルトラヴァイオレットコード044", 2008),
    "魔法遊戯 飛び出す!!ハナマル大冒険": ("魔法遊戯: 飛び出す!!ハナマル大冒険", 2001),
    "お前が魔王に勝てると思うなと勇者パーティを追放されたので王都で気ままに暮らしたい": (
        "「お前ごときが魔王に勝てると思うな」と勇者パーティを追放されたので、王都で気ままに暮らしたい",
        2026,
    ),
    "神霊狩/GHOST HOUND": ("神霊狩 Ghost Hound", 2007),
    "うたの☆プリンスさまっ♪ マジLOVEレボリューションズ": ("うたの☆プリンスさまっ♪マジLOVEレボリューションス", 2015),
    "フォーチュンクエストL": ("フォーチュン·クエストL", 1997),
    "マンガで分かる！Fate/Grand Order": ("マンガでわかる！Fate/Grand Order", 2018),
    "シュヴァリエ 〜Le Chevalier D'Eon〜": ("シェヴァリエ ~Le Chevalier D'Eon~", 2006),
    "乙女はお姉さまに恋してる 2人のエルダー": ("処女はお姉さまに恋してる ～2人のエルダー～", 2012),
    "無職転生\ufffd\ufffd 〜異世界行ったら本気だす〜": ("無職転生 ～異世界行ったら本気だす～", 2021),
    "ドラゴノーツ -ザ・レゾナンス-": ("ドラゴノーツ-ザ・レソナンス-", 2007),
    "アクティヴレイド2nd 機動強襲室第八係": ("アクティヴレイド 機動強襲室第八係", 2016),
    "W'z《ウィズ》": ("W'ｚ《ウィズ》", 2019),
    "みだらな青ちゃんは勉強ができない": ("淫らな青ちゃんは勉強ができない", 2019),
    "天地無用! 魎皇鬼 第三期": ("天地無用! 魎皇鬼 第1期", 1992),
    '"文学少女"メモワール': ("文学少女 メモワール", 2014),
    # v24 追加 (85%+ confidence, 手動確認済)
    "ゾイドフューザーズ": ("ゾイド・フューザース", 2004),
    "ゆるゆり さん☆ハイ!": ("ゆるゆり さん☆はい！", 2015),
    "スラップアップパーティー -アラド戦記-": ("アラド戦記 ～スラップアップパーティー～", 2009),
    "TOKKO 特公": ("TOKKÔ[特公]", 2006),
    "I・R・I・A ZEIRAM THE ANIMATION": ("I・Я・I・A ZЁIЯAM THE ANIMATION", 1994),
    "ビーストウォーズネオ 超生命体トランスフォーマー": ("ビーストウォーズⅡ（セカンド） 超生命体トランスフォーマー", 1998),
    "おちゃめなふたご クレア学院物語": ("おちゃめな双子　－クレア学院物語－", 1991),
    "サンリオ世界名作劇場": ("サンリオ・アニメ世界名作劇場", 2001),
    "まほろまてぃっく特別編 ただいま◆おかえり": ("まほろまてぃっく ただいま◇おかえり", 2009),
    "空の境界 第五章　矛盾螺旋": ("空の境界 矛盾螺旋", 2008),
    "空の境界 第七章　殺人考察（後）": ("空の境界 殺人考察(後)", 2009),
    "東京魔人學園剣風帖 龍龍 第弐幕": ("東京魔人學園剣風帖　龖（トウ） 第弐幕", 2007),
    "メジャーセカンド（第2シリーズ）": ("メジャー2nd 第２シリーズ", 2020),
    "テイルズ オブ シンフォニア THE ANIMATION（第3期）": ("テイルズ オブ シンフォニア THE ANIMATION テセアラ編", 2010),
    "テイルズ オブ シンフォニア THE ANIMATION（第2期）": ("テイルズ オブ シンフォニア THE ANIMATION テセアラ編", 2010),
    "マジでオタクなイングリッシュ!りぼんちゃん the TV": (
        "マジでオタクなイングリッシュ! りぼんちゃん ~英語で戦う魔法少女~ the TV", 2013,
    ),
    "Bビーダマン爆外伝V": ("B[ボンバーマン]ビーダマン爆外伝Ｖ", 1999),
    "Bビーダマン爆外伝": ("B[ボンバーマン]ビーダマン爆外伝", 1998),
    "頭文字D Second Stage": ("頭文字〈イニシャル〉D SECOND STAGE", 1999),
    "頭文字D Fourth Stage": ("頭文字〈イニシャル〉D FOURTH STAGE", 2004),
    "頭文字D Fifth Stage": ("頭文字〈イニシャル〉D Fifth Stage", 2012),
    "攻殻機動隊 ARISE　border:1 Ghost Pain": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:1 Ghost Pain", 2013,
    ),
    "攻殻機動隊 ARISE　border:2 Ghost Whispers": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:2 Ghost Whispers", 2013,
    ),
    "攻殻機動隊 ARISE　border:3 Ghost Tears": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:3 Ghost Tears", 2014,
    ),
    "攻殻機動隊 ARISE　border:4 Ghost Stands Alone": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:4 Ghost Stands Alone", 2014,
    ),
}


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
        18: _migrate_v18_add_score_history_quarter,
        19: _migrate_v19_add_anime_quarter,
        20: _migrate_v20_add_credit_temporal,
        21: _migrate_v21_enhanced_title_matching,
        22: _migrate_v22_deep_title_matching,
        23: _migrate_v23_fuzzy_match_rules,
        24: _migrate_v24_improved_matching,
        25: _migrate_v25_kanji_hira_matching,
        26: _migrate_v26_anime_scale_classification,
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
            person_fe REAL DEFAULT 0.0,
            studio_fe_exposure REAL DEFAULT 0.0,
            birank REAL DEFAULT 0.0,
            patronage REAL DEFAULT 0.0,
            dormancy REAL DEFAULT 1.0,
            awcc REAL DEFAULT 0.0,
            iv_score REAL DEFAULT 0.0,
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


def _migrate_v18_add_score_history_quarter(conn: sqlite3.Connection) -> None:
    """v18: score_history に year/quarter カラムを追加（四半期集計用）."""
    for col in [
        "year INTEGER",
        "quarter INTEGER",
    ]:
        col_name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE score_history ADD COLUMN {col}")
        except sqlite3.OperationalError:
            logger.debug("column_already_exists", table="score_history", column=col_name)
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_score_history_yq
            ON score_history(year, quarter);
    """)


def _migrate_v19_add_anime_quarter(conn: sqlite3.Connection) -> None:
    """v19: anime に quarter カラムを追加し、season / start_date から一括算出.

    さらに SeesaaWiki/Keyframes 由来の year=NULL 作品を AniList タイトルマッチで補完。

    優先順: season → start_date の月 → NULL（不明）。
    season_to_quarter: winter=1, spring=2, summer=3, fall=4.
    start_date (YYYY-MM-DD) の月: 1-3=Q1, 4-6=Q2, 7-9=Q3, 10-12=Q4.
    """
    import re
    import unicodedata

    try:
        conn.execute("ALTER TABLE anime ADD COLUMN quarter INTEGER")
    except sqlite3.OperationalError:
        logger.debug("column_already_exists", table="anime", column="quarter")

    # Phase 1: season / start_date から quarter を直接算出
    conn.execute("""
        UPDATE anime SET quarter = CASE
            WHEN LOWER(season) = 'winter' THEN 1
            WHEN LOWER(season) = 'spring' THEN 2
            WHEN LOWER(season) = 'summer' THEN 3
            WHEN LOWER(season) = 'fall'   THEN 4
            WHEN start_date IS NOT NULL AND LENGTH(start_date) >= 7
                THEN (CAST(SUBSTR(start_date, 6, 2) AS INTEGER) - 1) / 3 + 1
            ELSE NULL
        END
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_anime_quarter ON anime(year, quarter)")

    phase1 = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE quarter IS NOT NULL"
    ).fetchone()[0]
    logger.info("anime_quarter_phase1", count=phase1)

    # Phase 2: SeesaaWiki/Keyframes → AniList タイトルマッチで year/quarter/format 補完
    _backfill_from_anilist_titles(conn)


def _backfill_from_anilist_titles(conn: sqlite3.Connection) -> None:
    """year=NULL の非 AniList 作品を AniList タイトルマッチで補完.

    マッチ戦略 (優先順):
      1. title_ja 完全一致
      2. NFKC正規化 + 記号/空白除去 後の一致
      3. 括弧注釈除去 (「（TV第2作）」「(第3期)」等) 後の一致
         → 同名シリーズが複数ある場合は年代注釈からヒント取得

    補完対象カラム: year, quarter, format, season, start_date
    """
    import re
    import unicodedata

    # --- ターゲット: year=NULL の非AniList作品 ---
    targets = conn.execute(
        "SELECT id, title_ja, title_en FROM anime "
        "WHERE year IS NULL AND id NOT LIKE 'anilist:%' AND title_ja != ''"
    ).fetchall()
    if not targets:
        return

    # --- AniList 参照データ ---
    refs = conn.execute(
        "SELECT id, title_ja, title_en, year, quarter, season, start_date, format "
        "FROM anime WHERE id LIKE 'anilist:%' AND year IS NOT NULL AND title_ja != ''"
    ).fetchall()

    def _normalize(s: str) -> str:
        """NFKC正規化 + 記号/空白除去 + 小文字."""
        import html
        # HTMLエンティティをデコード: &#9825; → ♡
        s = html.unescape(s)
        s = unicodedata.normalize("NFKC", s).lower().strip()
        # 〈〉を<>に統一してから除去対象に含める
        s = s.replace("\u3008", "<").replace("\u3009", ">")
        s = re.sub(
            r"[\s\u3000・\-–—―~〜!！?？、。,.'\"()\（\）\[\]【】「」『』《》☆★♪♡♥♡−＝<>〈〉◆◇#\$%@&*]+",
            "", s,
        )
        return s

    def _strip_movie_prefix(s: str) -> str:
        """「映画」「劇場版」等のメディア形式プレフィックスを除去."""
        s = re.sub(r"^(?:映画|劇場版|劇場版映画|特別版|OVA|OAD)\s*", "", s)
        return s.strip()

    def _sorted_word_key(s: str) -> str:
        """単語をソートしてキーを生成（順序不問マッチ用）.
        例: 'SHADOW SKILL 影技' → '影技shadowskill' (sorted bigrams)"""
        # スペース・記号で分割してソート
        words = re.split(r"[\s・\-_/]+", s)
        return "".join(sorted(w for w in words if w))

    def _normalize_roman_greek(s: str) -> str:
        """ギリシャ文字・ローマ数字を ASCII に変換."""
        for old, new in [
            ("ΖΖ", "ZZ"), ("Ζ", "Z"),
            ("Ⅲ", "III"), ("Ⅱ", "II"), ("Ⅰ", "I"),
            ("Ⅳ", "IV"), ("Ⅴ", "V"), ("Ⅵ", "VI"),
            ("Ⅶ", "VII"), ("Ⅷ", "VIII"), ("Ⅸ", "IX"), ("Ⅹ", "X"),
        ]:
            s = s.replace(old, new)
        return s

    def _kanji_hira_key(s: str) -> str:
        """漢字+ひらがなのみ抽出（カタカナ・ASCII・記号を除去）.

        英語部分(WEED)とカタカナ翻字(ウィード)を同時に除去して、
        残る漢字+ひらがなが一致していれば同一作品と判定する。
        """
        s = unicodedata.normalize("NFKC", s)
        return "".join(
            c for c in s
            if "\u4e00" <= c <= "\u9fff"   # CJK統合漢字
            or "\u3400" <= c <= "\u4dbf"   # CJK拡張A
            or "\u3040" <= c <= "\u309f"   # ひらがな
        )

    def _strip_reading_parens(s: str) -> str:
        """カタカナ/英字の読み括弧を除去: GATE（ゲート）→ GATE."""
        return re.sub(r"[（(][ァ-ヾA-Za-zＡ-Ｚ　\s]+[）)]", "", s).strip()

    def _strip_furigana(s: str) -> str:
        """AniList側のふりがな括弧を除去: 魔法騎士（マジックナイト）→ 魔法騎士."""
        return re.sub(r"（[ぁ-ヾァ-ヾA-Za-zＡ-Ｚ\w]+）", "", s).strip()

    def _strip_english_prefix(s: str) -> str:
        """日本語文字の前の英語プレフィックスを除去: MAJOR メジャー → メジャー."""
        return re.sub(r"^[A-Za-z0-9\s.\-/!?&:'+]+(?=[ぁ-ヾァ-ヾ一-龥])", "", s).strip()

    def _strip_annotations(s: str) -> str:
        """括弧注釈を除去: （TV第2作）、(第3期)、（2020年）等."""
        s = re.sub(r"[（(][^）)]*(?:シリーズ|期|作|版|年)[^）)]*[）)]", "", s)
        # 末尾の数字年号も除去: "ぼのぼの (2016)" → "ぼのぼの"
        s = re.sub(r"\s*[（(]\d{4}[）)]$", "", s)
        return s.strip()

    def _extract_year_hint(title: str) -> int | None:
        """タイトルから年代ヒントを抽出: （2020年）→2020."""
        m = re.search(r"[（(](\d{4})年?[）)]", title)
        return int(m.group(1)) if m else None

    def _strip_tv_prefix(s: str) -> str:
        """TVプレフィックスを除去: 'TVそれいけ!アンパンマン' → 'それいけ!アンパンマン'."""
        return re.sub(r"^TV\s*", "", s)

    # --- AniList インデックス構築 ---
    # exact title_ja → list of refs
    idx_exact: dict[str, list] = {}
    # normalized → list of refs
    idx_norm: dict[str, list] = {}
    # stripped + normalized → list of refs
    idx_stripped: dict[str, list] = {}
    # normalized title_en → list of refs (英語タイトルクロスマッチ用)
    idx_en: dict[str, list] = {}
    # (normalized_ja, entry) for containment lookups
    ref_entries_for_containment: list[tuple[str, tuple]] = []
    # furigana-stripped AniList title → list of refs
    idx_furigana: dict[str, list] = {}
    # movie-prefix-stripped → list of refs (「映画」「劇場版」除去)
    idx_movie: dict[str, list] = {}
    # sorted-word key → list of refs (単語順序不問マッチ)
    idx_sorted: dict[str, list] = {}
    # 漢字+ひらがなキー → list of refs (英語/カタカナ部分が違っても漢字が一致する場合)
    idx_khk: dict[str, list] = {}

    for row in refs:
        aid, ja, en, yr, q, sea, sd, fmt = row
        entry = (aid, yr, q, sea, sd, fmt, ja)

        idx_exact.setdefault(ja, []).append(entry)

        nk = _normalize(ja)
        if nk:
            idx_norm.setdefault(nk, []).append(entry)
            # 包含マッチ用にエントリを記録
            ref_entries_for_containment.append((nk, entry))

        sk = _normalize(_strip_annotations(ja))
        if sk and sk != nk:
            idx_stripped.setdefault(sk, []).append(entry)
        # stripped も同じキーなら norm に含まれるので追加不要
        if sk and sk not in idx_stripped:
            idx_stripped.setdefault(sk, []).append(entry)

        # title_en インデックス (英語タイトルクロスマッチ)
        if en:
            enk = _normalize(en)
            if enk:
                idx_en.setdefault(enk, []).append(entry)

        # ふりがな除去インデックス: 魔法騎士（マジックナイト）レイアース → 魔法騎士レイアース
        fk = _normalize(_strip_furigana(ja))
        if fk and fk != nk:
            idx_furigana.setdefault(fk, []).append(entry)

        # 映画/劇場版 プレフィックス除去インデックス
        mk = _normalize(_strip_movie_prefix(ja))
        if mk and mk != nk:
            idx_movie.setdefault(mk, []).append(entry)

        # 単語ソートインデックス (SHADOW SKILL 影技 ↔ 影技 SHADOW SKILL)
        # 5文字以上かつ複数単語の場合のみ
        words = [w for w in re.split(r"[\s\u3000・\-_/]+", ja) if w]
        if len(words) >= 2 and len(nk) >= 5:
            swk = _normalize(_sorted_word_key(ja))
            if swk and swk != nk:
                idx_sorted.setdefault(swk, []).append(entry)

        # 漢字+ひらがなキーインデックス (英語WEED ↔ カタカナウィード)
        khk = _kanji_hira_key(ja)
        if len(khk) >= 4:
            idx_khk.setdefault(khk, []).append(entry)

    def _pick_best(candidates: list, year_hint: int | None = None) -> tuple | None:
        """複数候補から最適な1件を選択."""
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        # 年代ヒントがあれば最も近いものを選択
        if year_hint:
            return min(candidates, key=lambda c: abs((c[1] or 0) - year_hint))
        # なければ最新のものを選択（長期シリーズの最新版が通常正しい）
        return max(candidates, key=lambda c: c[1] or 0)

    # --- マッチング実行 ---
    updates: list[tuple] = []  # (year, quarter, season, start_date, format, target_id)

    for sid, sja, sen in targets:
        match = None
        year_hint = _extract_year_hint(sja)

        # 1. 完全一致
        if sja in idx_exact:
            match = _pick_best(idx_exact[sja], year_hint)

        # 2. 正規化一致
        if not match:
            nk = _normalize(sja)
            if nk in idx_norm:
                match = _pick_best(idx_norm[nk], year_hint)

        # 3. 括弧注釈除去 + 正規化一致
        if not match:
            sk = _normalize(_strip_annotations(sja))
            if sk in idx_norm:
                match = _pick_best(idx_norm[sk], year_hint)
            elif sk in idx_stripped:
                match = _pick_best(idx_stripped[sk], year_hint)

        # 4. ベースタイトルマッチ（スペース前の主タイトルのみ）
        #    品質制御: ベースタイトル5文字以上、かつ候補が少数の場合のみ
        if not match:
            _base = re.sub(r"[（(][^）)]*[）)]", "", sja).strip()
            _parts = re.split(r"[\s　]+", _base)
            _base_title = _parts[0] if len(_parts) > 1 else _base
            _bk = _normalize(_base_title)
            # 短すぎるベースタイトルは誤マッチの原因なのでスキップ
            # カテゴリページ等もスキップ
            _SKIP_PATTERNS = {"年代", "シリーズリスト", "アニバーサリー", "music"}
            if (
                len(_bk) >= 5
                and not any(p in sja for p in _SKIP_PATTERNS)
                and _bk in idx_norm
            ):
                candidates = idx_norm[_bk]
                # 候補が多すぎる場合（汎用的なタイトル）はスキップ
                if len(candidates) <= 10:
                    match = _pick_best(candidates, year_hint)

        # 5. title_en クロスマッチ (SeesaaWiki title_ja ↔ AniList title_en)
        #    例: NANA, D.Gray-man, CLANNAD, MAJOR, BLACK CAT
        if not match:
            nk = _normalize(sja)
            if nk in idx_en:
                match = _pick_best(idx_en[nk], year_hint)

        # 6. TVプレフィックス除去マッチ
        #    例: TVそれいけ!アンパンマン → それいけ!アンパンマン
        if not match and (sja.startswith("TV") or sja.startswith("ＴＶ")):
            tv_stripped = _normalize(_strip_tv_prefix(sja))
            if tv_stripped and tv_stripped in idx_norm:
                match = _pick_best(idx_norm[tv_stripped], year_hint)

        # 7. 逆包含マッチ (AniList title_ja が SeesaaWiki title_ja を含む)
        #    例: "NANA" → "NANA-ナナ-", "D.Gray-man" → "D.Gray-man ディー・グレイマン"
        #    品質制御: 正規化後5文字以上、候補10件以下
        if not match:
            nk = _normalize(sja)
            if len(nk) >= 5:
                rev_candidates = [
                    entry for ref_nk, entry in ref_entries_for_containment
                    if ref_nk != nk and nk in ref_nk
                ]
                if 0 < len(rev_candidates) <= 10:
                    match = _pick_best(rev_candidates, year_hint)

        # 8. target title_en → AniList idx_norm (target英語タイトル ↔ AniList title_ja正規化)
        if not match and sen:
            senk = _normalize(sen)
            if senk and senk in idx_norm:
                match = _pick_best(idx_norm[senk], year_hint)

        # 9. ローマ/ギリシャ数字正規化 (Ζ→Z, Ⅲ→III)
        #    例: 機動戦士ガンダムΖΖ → 機動戦士ガンダムZZ
        if not match:
            rk = _normalize(_normalize_roman_greek(sja))
            if rk != _normalize(sja) and rk in idx_norm:
                match = _pick_best(idx_norm[rk], year_hint)

        # 10. 読み括弧除去 (SeesaaWiki側)
        #     例: GATE（ゲート）自衛隊 → GATE自衛隊
        if not match:
            rp = _normalize(_strip_reading_parens(sja))
            if rp != _normalize(sja) and rp in idx_norm:
                match = _pick_best(idx_norm[rp], year_hint)

        # 11. ふりがな除去 (AniList側) — SeesaaWiki title ↔ AniList stripped
        #     例: 魔法騎士レイアース → 魔法騎士（マジックナイト）レイアース
        if not match:
            nk = _normalize(sja)
            if nk in idx_furigana:
                match = _pick_best(idx_furigana[nk], year_hint)

        # 12. 英語プレフィックス除去
        #     例: MAJOR メジャー 第3シリーズ → メジャー第3シリーズ
        if not match:
            ep = _strip_english_prefix(sja)
            if ep and ep != sja:
                epk = _normalize(ep)
                if epk and epk in idx_norm:
                    match = _pick_best(idx_norm[epk], year_hint)

        # 13. 逆包含 4文字閾値 (SeesaaWiki ⊂ AniList, 4文字以上)
        #     例: 監獄学園 → 監獄学園〈プリズンスクール〉
        if not match:
            nk = _normalize(sja)
            if 4 <= len(nk) < 5:
                rev_candidates = [
                    entry for ref_nk, entry in ref_entries_for_containment
                    if ref_nk != nk and nk in ref_nk
                ]
                if 0 < len(rev_candidates) <= 10:
                    match = _pick_best(rev_candidates, year_hint)

        # 14. 前方包含 (AniList ⊂ SeesaaWiki, ratio>50%)
        #     例: それいけ!アンパンマン ⊂ TVそれいけ!アンパンマン（2009年）
        #     品質制御: AniList正規化タイトルが SeesaaWiki の50%以上
        if not match:
            nk = _normalize(sja)
            if len(nk) >= 8:
                fwd_candidates = []
                for ref_nk, entry in ref_entries_for_containment:
                    if (
                        ref_nk != nk
                        and len(ref_nk) >= 5
                        and ref_nk in nk
                        and len(ref_nk) / len(nk) >= 0.5
                    ):
                        fwd_candidates.append(entry)
                if 0 < len(fwd_candidates) <= 10:
                    # 最長一致を優先
                    fwd_candidates.sort(
                        key=lambda e: len(_normalize(e[6])), reverse=True,
                    )
                    match = _pick_best(fwd_candidates[:3], year_hint)

        # 15. 漢字+ひらがなキーマッチ (英語/カタカナ部分が異なっても漢字が一致)
        #     例: 銀牙伝説WEED ↔ 銀牙伝説ウィード (共通キー: 銀牙伝説)
        #     品質制御: キー4文字以上、AniList候補が1件のみ
        if not match:
            khk = _kanji_hira_key(sja)
            if len(khk) >= 4 and khk in idx_khk:
                cands = idx_khk[khk]
                if len(cands) == 1:
                    match = cands[0]

        # 16. Fuzzy match ルール辞書 (90%+ confidence, 手動確認済)
        if not match and sja in _FUZZY_MATCH_RULES:
            anilist_ja, rule_year = _FUZZY_MATCH_RULES[sja]
            for aid, ja, en, yr, q, sea, sd, fmt in refs:
                if ja == anilist_ja and yr == rule_year:
                    match = (aid, yr, q, sea, sd, fmt, ja)
                    break

        # 16. 「映画」「劇場版」プレフィックス除去マッチ
        #     例: 映画 聲の形 → 聲の形, 劇場版シティーハンター → シティーハンター
        if not match:
            mk = _normalize(_strip_movie_prefix(sja))
            if mk and mk != _normalize(sja) and mk in idx_norm:
                match = _pick_best(idx_norm[mk], year_hint)
            elif mk and mk in idx_movie:
                match = _pick_best(idx_movie[mk], year_hint)

        # 17. 単語順逆転マッチ (SHADOW SKILL -影技- ↔ 影技 SHADOW SKILL)
        #     品質制御: 5文字以上、候補10件以下
        if not match:
            nk = _normalize(sja)
            if len(nk) >= 5:
                words = [w for w in re.split(r"[\s\u3000・\-_/]+", sja) if w]
                if len(words) >= 2:
                    swk = _normalize(_sorted_word_key(sja))
                    if swk and swk in idx_sorted:
                        cands = idx_sorted[swk]
                        if 0 < len(cands) <= 10:
                            match = _pick_best(cands, year_hint)
                    # また AniList側の sorted key との一致も確認
                    if not match and swk in idx_norm:
                        match = _pick_best(idx_norm[swk], year_hint)

        # 18. 映画/劇場版 prefix 除去 + AniList側の movie index
        #     例: 映画 プリキュアオールスターズF → プリキュアオールスターズF
        if not match:
            mk = _normalize(_strip_movie_prefix(sja))
            if mk and mk != _normalize(sja):
                # 前方包含も試みる (ratio>60%)
                if len(mk) >= 6:
                    mv_cands = [
                        entry for ref_nk, entry in ref_entries_for_containment
                        if ref_nk != mk
                        and len(ref_nk) >= 5
                        and ref_nk in mk
                        and len(ref_nk) / len(mk) >= 0.6
                    ]
                    if 0 < len(mv_cands) <= 5:
                        mv_cands.sort(key=lambda e: len(_normalize(e[6])), reverse=True)
                        match = _pick_best(mv_cands[:3], year_hint)

        if match:
            _, yr, q, sea, sd, fmt, _ = match
            updates.append((yr, q, sea, sd, fmt, sid))

    # --- DB 一括更新 ---
    if updates:
        conn.executemany(
            "UPDATE anime SET year=?, quarter=?, season=?, start_date=?, format=? "
            "WHERE id=?",
            updates,
        )

    # 補完後に quarter が NULL のまま残っている作品の quarter を再計算
    # (season/start_date が補完されたが quarter が未設定のケース)
    conn.execute("""
        UPDATE anime SET quarter = CASE
            WHEN LOWER(season) = 'winter' THEN 1
            WHEN LOWER(season) = 'spring' THEN 2
            WHEN LOWER(season) = 'summer' THEN 3
            WHEN LOWER(season) = 'fall'   THEN 4
            WHEN start_date IS NOT NULL AND LENGTH(start_date) >= 7
                THEN (CAST(SUBSTR(start_date, 6, 2) AS INTEGER) - 1) / 3 + 1
            ELSE quarter
        END
        WHERE id IN (SELECT id FROM anime WHERE year IS NOT NULL AND quarter IS NULL)
    """)

    total_backfilled = len(updates)
    final_count = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE quarter IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "anime_backfill_from_anilist",
        matched=total_backfilled,
        total_quarter_populated=final_count,
    )


def _migrate_v20_add_credit_temporal(conn: sqlite3.Connection) -> None:
    """v20: credits に credit_year / credit_quarter を追加.

    短期作品 (≤12話 or episode=-1): anime の year/quarter をそのまま使用。
    長期作品 (>12話, episode情報あり): start_date + 週1放送仮定で各話の放送時期を推定。

    推定式: air_date = start_date + (episode - 1) * 7 days
    """
    from datetime import date, timedelta

    for col in ["credit_year INTEGER", "credit_quarter INTEGER"]:
        try:
            conn.execute(f"ALTER TABLE credits ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    # Phase 1: 全クレジットに anime の year/quarter をデフォルト設定
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a WHERE credits.anime_id = a.id AND a.year IS NOT NULL
    """)

    phase1 = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE credit_year IS NOT NULL"
    ).fetchone()[0]
    logger.info("credit_temporal_phase1", count=phase1)

    # Phase 2: 長期作品のエピソード別時期推定
    # 対象: start_date あり & 話数付きクレジットがある & 13話以上の作品
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        # 放送間隔の推定
        # end_date があれば (end - start) / max_ep で計算
        # なければ週1 (7日) を仮定
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = (ed - sd).days / (max_ep - 1)
                    # 妥当性チェック: 3-14日の範囲（週1±α）
                    interval_days = max(3, min(14, interval_days))
            except (ValueError, TypeError):
                pass

        # 各エピソードのクレジットを更新
        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits "
            "WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            updated += len(batch)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_credits_yq ON credits(credit_year, credit_quarter)"
    )

    final = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE credit_year IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "credit_temporal_phase2",
        long_anime=len(long_anime),
        episodes_updated=updated,
        total_with_temporal=final,
    )


def _migrate_v21_enhanced_title_matching(conn: sqlite3.Connection) -> None:
    """v21: 拡張タイトルマッチング (title_en, TVプレフィックス, 逆包含) で再マッチ.

    v19 で追加した _backfill_from_anilist_titles に新しいマッチ戦略を追加済み。
    ここでは再実行して残りの未マッチ作品を補完し、
    新規マッチした作品の credit_year/credit_quarter も設定する。
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    # 新規マッチした作品の credits にも credit_year/credit_quarter を設定
    # (credit_year IS NULL = v20 migration 時にマッチしていなかった作品)
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # 新規マッチの長期作品もエピソード別時期推定
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
          AND c.credit_year = a.year  -- v20で一括設定されたままのもの
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass

        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits "
            "WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v21_enhanced_title_matching",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v22_deep_title_matching(conn: sqlite3.Connection) -> None:
    """v22: 深層タイトルマッチング (ローマ数字, ふりがな, 英語prefix, 前方包含).

    v21 backfill に追加した戦略 9-14 で残りの未マッチ作品を補完。
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    # 新規マッチした作品の credits にも credit_year/credit_quarter を設定
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # 新規マッチの長期作品もエピソード別時期推定
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
          AND c.credit_year = a.year
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass

        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits "
            "WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v22_deep_title_matching",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v23_fuzzy_match_rules(conn: sqlite3.Connection) -> None:
    """v23: Fuzzy マッチングルール (90%+ confidence) を適用して残りを補完.

    v22 backfill に追加した phase 15 (fuzzy match rules) で
    高精度なタイトル置換ペアをマッチ。
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    # 新規マッチした作品の credits にも credit_year/credit_quarter を設定
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # 新規マッチの長期作品もエピソード別時期推定
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
          AND c.credit_year = a.year
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass

        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits "
            "WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v23_fuzzy_match_rules",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v24_improved_matching(conn: sqlite3.Connection) -> None:
    """v24: 改良マッチング (映画prefix, 単語順逆転, normalize改善, 追加fuzzy rules).

    改善内容:
    - normalize(): 〈〉→<>統一, HTMLエンティティデコード, ◆◇等追加
    - Phase 16: 「映画」「劇場版」プレフィックス除去マッチ
    - Phase 17: 単語順逆転マッチ (SHADOW SKILL-影技- ↔ 影技 SHADOW SKILL)
    - Phase 18: 映画prefix除去+前方包含
    - _FUZZY_MATCH_RULES: v24で25件追加 (攻殻機動隊ARISE, 頭文字D等)
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes, MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0 AND c.credit_year = a.year
        GROUP BY a.id HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass
        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()
        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            batch.append((air_date.year, (air_date.month - 1) // 3 + 1, rowid))
        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v24_improved_matching",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v25_kanji_hira_matching(conn: sqlite3.Connection) -> None:
    """v25: 漢字+ひらがなキーマッチ — 英語/カタカナ部分が異なっても漢字が一致すれば同定.

    例: 銀牙伝説WEED ↔ 銀牙伝説ウィード (キー: 銀牙伝説)
    品質制御: キー4文字以上、AniList候補が1件のみ受理。
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL AND credits.credit_year IS NULL
    """)

    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes, MAX(c.episode) as max_ep
        FROM anime a INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0 AND c.credit_year = a.year
        GROUP BY a.id HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, _, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass
        batch = []
        for rowid, ep in conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id=? AND episode>0", (anime_id,)
        ).fetchall():
            air = sd + timedelta(days=(ep - 1) * interval_days)
            batch.append((air.year, (air.month - 1) // 3 + 1, rowid))
        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?", batch
            )
            ep_updated += len(batch)

    logger.info(
        "v25_kanji_hira_matching",
        newly_matched=before - after,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def compute_anime_scale_classes(conn: sqlite3.Connection) -> dict[str, int]:
    """アニメを work_type (tv/tanpatsu) × scale_class (large/medium/small) に分類する.

    特徴量: log(total_animator_credits), log(unique_animators)
    最小データ閾値: total_animator_credits >= 10
    K-means K=3 を TV / 単発 それぞれ独立に適用。
    centroids は total_animator_credits の中央値でソートし 小→中→大 に対応付ける。

    Returns:
        {'tv_classified': n, 'tanpatsu_classified': n, 'null_assigned': n}
    """
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    ANIMATOR_ROLES = (
        "key_animator",
        "second_key_animator",
        "animation_director",
        "in_between",
        "finishing",
        "layout",
    )
    TV_FORMATS = {"TV", "TV_SHORT"}
    TANPATSU_FORMATS = {"OVA", "MOVIE", "ONA", "SPECIAL"}
    MIN_CREDITS = 10
    K = 3
    SCALE_LABELS = ["small", "medium", "large"]

    # Collect per-anime animator stats
    rows = conn.execute(
        """
        SELECT a.id, a.format,
               COUNT(c.id)              AS total,
               COUNT(DISTINCT c.person_id) AS unique_anim
        FROM anime a
        JOIN credits c ON c.anime_id = a.id
        WHERE c.role IN ({})
          AND a.format IS NOT NULL
        GROUP BY a.id
        """.format(",".join(f"'{r}'" for r in ANIMATOR_ROLES))
    ).fetchall()

    tv_rows = [r for r in rows if r[1] in TV_FORMATS and r[2] >= MIN_CREDITS]
    tan_rows = [r for r in rows if r[1] in TANPATSU_FORMATS and r[2] >= MIN_CREDITS]

    def _classify(data: list) -> dict[str, str]:
        """anime_id → scale_class ('small'/'medium'/'large')."""
        if len(data) < K:
            return {}
        X_raw = np.array([[r[2], r[3]] for r in data], dtype=float)
        X = StandardScaler().fit_transform(np.log1p(X_raw))
        km = KMeans(n_clusters=K, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        # Sort clusters by median total_credits → small/medium/large
        medians = [
            (int(np.median([data[i][2] for i in range(len(data)) if labels[i] == cl])), cl)
            for cl in range(K)
        ]
        medians.sort()
        cl_to_label = {cl: SCALE_LABELS[rank] for rank, (_, cl) in enumerate(medians)}
        return {data[i][0]: cl_to_label[labels[i]] for i in range(len(data))}

    tv_map = _classify(tv_rows)
    tan_map = _classify(tan_rows)

    # Reset columns first
    conn.execute("UPDATE anime SET work_type = NULL, scale_class = NULL")

    # Assign work_type for all anime with known format
    conn.execute(
        "UPDATE anime SET work_type = 'tv' WHERE format IN ('TV', 'TV_SHORT')"
    )
    conn.execute(
        "UPDATE anime SET work_type = 'tanpatsu'"
        " WHERE format IN ('OVA', 'MOVIE', 'ONA', 'SPECIAL')"
    )

    # Assign scale_class in batches
    for anime_id, scale in tv_map.items():
        conn.execute(
            "UPDATE anime SET scale_class = ? WHERE id = ?", (scale, anime_id)
        )
    for anime_id, scale in tan_map.items():
        conn.execute(
            "UPDATE anime SET scale_class = ? WHERE id = ?", (scale, anime_id)
        )

    null_count = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE work_type IS NOT NULL AND scale_class IS NULL"
    ).fetchone()[0]

    logger.info(
        "anime_scale_classified",
        tv_classified=len(tv_map),
        tanpatsu_classified=len(tan_map),
        null_scale=null_count,
    )
    return {
        "tv_classified": len(tv_map),
        "tanpatsu_classified": len(tan_map),
        "null_assigned": null_count,
    }


def _migrate_v26_anime_scale_classification(conn: sqlite3.Connection) -> None:
    """v26: anime テーブルに work_type / scale_class カラムを追加し K-means で分類."""
    conn.executescript("""
        ALTER TABLE anime ADD COLUMN work_type  TEXT;
        ALTER TABLE anime ADD COLUMN scale_class TEXT;
        CREATE INDEX IF NOT EXISTS idx_anime_work_type  ON anime(work_type);
        CREATE INDEX IF NOT EXISTS idx_anime_scale_class ON anime(scale_class);
    """)
    compute_anime_scale_classes(conn)


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
    from src.db_rows import PersonRow

    rows = conn.execute("SELECT * FROM persons").fetchall()
    return [Person.from_db_row(PersonRow.from_row(row)) for row in rows]


def load_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """全アニメを読み込む."""
    from src.db_rows import AnimeRow

    rows = conn.execute("SELECT * FROM anime").fetchall()
    return [Anime.from_db_row(AnimeRow.from_row(row)) for row in rows]


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
    from src.db_rows import CreditRow

    rows = conn.execute("SELECT * FROM credits").fetchall()
    credits: list[Credit] = []
    skipped = 0
    for row in rows:
        cr = CreditRow.from_row(row)
        role_str = _LEGACY_ROLE_MAP.get(cr.role, cr.role)
        try:
            credits.append(Credit.from_db_row(CreditRow(
                id=cr.id,
                person_id=cr.person_id,
                anime_id=cr.anime_id,
                role=role_str,
                raw_role=cr.raw_role,
                episode=cr.episode,
                source=cr.source,
                updated_at=cr.updated_at,
                credit_year=cr.credit_year,
                credit_quarter=cr.credit_quarter,
            )))
        except ValueError:
            skipped += 1
    if skipped:
        logger.warning("credits_skipped_unknown_role", count=skipped)
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


def save_score_history(
    conn: sqlite3.Connection,
    score: ScoreResult,
    year: int | None = None,
    quarter: int | None = None,
) -> None:
    """スコア履歴を保存する（年・四半期付き）."""
    conn.execute(
        """INSERT INTO score_history
               (person_id, person_fe, studio_fe_exposure, birank,
                patronage, dormancy, awcc, iv_score, year, quarter)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            score.person_id,
            score.person_fe,
            score.studio_fe_exposure,
            score.birank,
            score.patronage,
            score.dormancy,
            score.awcc,
            score.iv_score,
            year,
            quarter,
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
                  patronage, dormancy, awcc, iv_score,
                  year, quarter, run_at
           FROM score_history
           WHERE person_id = ?
           ORDER BY year DESC, quarter DESC, id DESC
           LIMIT ?""",
        (person_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_score_history_by_quarter(
    conn: sqlite3.Connection,
    year: int | None = None,
    quarter: int | None = None,
) -> list[dict]:
    """四半期ごとのスコア履歴を取得する."""
    conditions = []
    params: list = []
    if year is not None:
        conditions.append("year = ?")
        params.append(year)
    if quarter is not None:
        conditions.append("quarter = ?")
        params.append(quarter)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"""SELECT person_id, person_fe, studio_fe_exposure, birank,
                  patronage, dormancy, awcc, iv_score,
                  year, quarter, run_at
           FROM score_history
           {where}
           ORDER BY year, quarter, person_id""",
        params,
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
