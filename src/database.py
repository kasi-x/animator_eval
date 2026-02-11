"""SQLite データベース管理."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog

from src.models import Anime, Credit, Person, Role, ScoreResult
from src.utils.config import DB_DIR

logger = structlog.get_logger()

DEFAULT_DB_PATH = DB_DIR / "animetor_eval.db"

SCHEMA_VERSION = 7


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
def db_connection(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
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
            studio TEXT DEFAULT '',
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            UNIQUE(person_id, anime_id, role, episode)
        );

        CREATE TABLE IF NOT EXISTS scores (
            person_id TEXT PRIMARY KEY,
            authority REAL NOT NULL DEFAULT 0.0,
            trust REAL NOT NULL DEFAULT 0.0,
            skill REAL NOT NULL DEFAULT 0.0,
            composite REAL NOT NULL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS data_sources (
            source TEXT PRIMARY KEY,
            last_scraped_at TIMESTAMP,
            item_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ok'
        );

        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
        CREATE INDEX IF NOT EXISTS idx_anime_year ON anime(year);

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
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            UNIQUE(person_id, anime_id, role, episode)
        );
        INSERT OR IGNORE INTO credits_new
            SELECT id, person_id, anime_id, role, episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
    """)


def upsert_person(conn: sqlite3.Connection, person: Person) -> None:
    """人物を挿入または更新する（包括的データ対応）."""
    import json

    conn.execute(
        """INSERT INTO persons (
               id, name_ja, name_en, aliases, mal_id, anilist_id,
               image_large, image_medium, image_large_path, image_medium_path,
               date_of_birth, age, gender, years_active, hometown, blood_type,
               description, favourites, site_url
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), persons.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), persons.name_en),
               aliases = excluded.aliases,
               mal_id = COALESCE(excluded.mal_id, persons.mal_id),
               anilist_id = COALESCE(excluded.anilist_id, persons.anilist_id),
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
               id, title_ja, title_en, year, season, episodes, mal_id, anilist_id, score, studio,
               cover_large, cover_extra_large, cover_medium, banner, cover_large_path, banner_path,
               description, format, status, start_date, end_date, duration, source,
               genres, tags, popularity_rank, favourites, studios
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), anime.title_ja),
               title_en = COALESCE(NULLIF(excluded.title_en, ''), anime.title_en),
               year = COALESCE(excluded.year, anime.year),
               season = COALESCE(excluded.season, anime.season),
               episodes = COALESCE(excluded.episodes, anime.episodes),
               mal_id = COALESCE(excluded.mal_id, anime.mal_id),
               anilist_id = COALESCE(excluded.anilist_id, anime.anilist_id),
               score = COALESCE(excluded.score, anime.score),
               studio = COALESCE(NULLIF(excluded.studio, ''), anime.studio),
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
               studios = COALESCE(excluded.studios, anime.studios)
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
            anime.score,
            anime.studio or "",
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
        ),
    )


def insert_credit(conn: sqlite3.Connection, credit: Credit) -> None:
    """クレジットを挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO credits (person_id, anime_id, role, episode, source)
           VALUES (?, ?, ?, ?, ?)""",
        (
            credit.person_id,
            credit.anime_id,
            credit.role.value,
            credit.episode if credit.episode is not None else -1,
            credit.source,
        ),
    )


def upsert_score(conn: sqlite3.Connection, score: ScoreResult) -> None:
    """スコアを挿入または更新する."""
    conn.execute(
        """INSERT INTO scores (person_id, authority, trust, skill, composite)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(person_id) DO UPDATE SET
               authority = excluded.authority,
               trust = excluded.trust,
               skill = excluded.skill,
               composite = excluded.composite,
               updated_at = CURRENT_TIMESTAMP
        """,
        (
            score.person_id,
            score.authority,
            score.trust,
            score.skill,
            score.composite,
        ),
    )


def load_all_persons(conn: sqlite3.Connection) -> list[Person]:
    """全人物を読み込む."""
    import json

    rows = conn.execute("SELECT * FROM persons").fetchall()
    return [
        Person(
            id=row["id"],
            name_ja=row["name_ja"],
            name_en=row["name_en"],
            aliases=json.loads(row["aliases"]),
            mal_id=row["mal_id"],
            anilist_id=row["anilist_id"],
        )
        for row in rows
    ]


def load_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """全アニメを読み込む."""
    rows = conn.execute("SELECT * FROM anime").fetchall()
    return [
        Anime(
            id=row["id"],
            title_ja=row["title_ja"],
            title_en=row["title_en"],
            year=row["year"],
            season=row["season"],
            episodes=row["episodes"],
            mal_id=row["mal_id"],
            anilist_id=row["anilist_id"],
            score=row["score"],
        )
        for row in rows
    ]


def load_all_credits(conn: sqlite3.Connection) -> list[Credit]:
    """全クレジットを読み込む."""
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


def get_db_stats(conn: sqlite3.Connection) -> dict[str, int | float]:
    """DB統計情報を取得する."""
    stats: dict[str, int | float] = {}

    for table in ("persons", "anime", "credits", "scores"):
        stats[f"{table}_count"] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608

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
        [{id, name_ja, name_en, composite, credit_count}]
    """
    pattern = f"%{query}%"
    rows = conn.execute(
        """SELECT p.id, p.name_ja, p.name_en,
                  s.composite, s.authority, s.trust, s.skill,
                  (SELECT COUNT(*) FROM credits c WHERE c.person_id = p.id) as credit_count
           FROM persons p
           LEFT JOIN scores s ON p.id = s.person_id
           WHERE p.name_ja LIKE ? OR p.name_en LIKE ? COLLATE NOCASE OR p.id LIKE ?
           ORDER BY s.composite DESC NULLS LAST
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
    return [
        ScoreResult(
            person_id=row["person_id"],
            authority=row["authority"],
            trust=row["trust"],
            skill=row["skill"],
        )
        for row in rows
    ]


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

    current_credit_count = conn.execute(
        "SELECT COUNT(*) FROM credits"
    ).fetchone()[0]
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
        """INSERT INTO score_history (person_id, authority, trust, skill, composite)
           VALUES (?, ?, ?, ?, ?)""",
        (score.person_id, score.authority, score.trust, score.skill, score.composite),
    )


def get_score_history(
    conn: sqlite3.Connection,
    person_id: str,
    limit: int = 50,
) -> list[dict]:
    """人物のスコア履歴を取得する（新しい順）."""
    rows = conn.execute(
        """SELECT authority, trust, skill, composite, run_at
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
            episode=row["episode"],
            source=row["source"],
        )
        for row in rows
    ]


def get_all_scores(conn: sqlite3.Connection) -> list[ScoreResult]:
    """全スコアデータを取得する."""
    rows = conn.execute("SELECT * FROM scores").fetchall()
    return [
        ScoreResult(
            person_id=row["person_id"],
            authority=row["authority"],
            trust=row["trust"],
            skill=row["skill"],
            composite=row["composite"],
            authority_pct=row["authority_pct"],
            trust_pct=row["trust_pct"],
            skill_pct=row["skill_pct"],
            composite_pct=row["composite_pct"],
        )
        for row in rows
    ]
