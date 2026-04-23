"""SQLite database management."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import structlog

from src.models import (
    BronzeAnime as Anime,
    AnimeRelation,
    AnimeStudio,
    Character,
    CharacterVoiceActor,
    Credit,
    Person,
    ScoreResult,
    Studio,
)
from src.utils.config import DB_PATH

logger = structlog.get_logger()

DEFAULT_DB_PATH = DB_PATH

SCHEMA_VERSION = 58

# Fuzzy match rules for unmatched anime titles (90%+ confidence)
# Entries where SeesaaWiki title slightly differs from AniList title
_FUZZY_MATCH_RULES = {
    "AKIBA'S TRIP -THE ANIMATION-": ("Akiba's Trip -The Animation-", 2017),
    "Vivy -Fluorite Eye's Song-": ("Vivy -Fluorite Eye's Song-", 2021),
    "妖怪ウォッチJam 妖怪学園Y 〜Nとの遭遇〜": (
        "妖怪ウォッチJam: 妖怪学園Y 〜Nとの遭遇〜",
        2020,
    ),
    "ウルトラヴァイオレット：コード044": ("ウルトラヴァイオレットコード044", 2008),
    "魔法遊戯 飛び出す!!ハナマル大冒険": ("魔法遊戯: 飛び出す!!ハナマル大冒険", 2001),
    "お前が魔王に勝てると思うなと勇者パーティを追放されたので王都で気ままに暮らしたい": (
        "「お前ごときが魔王に勝てると思うな」と勇者パーティを追放されたので、王都で気ままに暮らしたい",
        2026,
    ),
    "神霊狩/GHOST HOUND": ("神霊狩 Ghost Hound", 2007),
    "うたの☆プリンスさまっ♪ マジLOVEレボリューションズ": (
        "うたの☆プリンスさまっ♪マジLOVEレボリューションス",
        2015,
    ),
    "フォーチュンクエストL": ("フォーチュン·クエストL", 1997),
    "マンガで分かる！Fate/Grand Order": ("マンガでわかる！Fate/Grand Order", 2018),
    "シュヴァリエ 〜Le Chevalier D'Eon〜": ("シェヴァリエ ~Le Chevalier D'Eon~", 2006),
    "乙女はお姉さまに恋してる 2人のエルダー": (
        "処女はお姉さまに恋してる ～2人のエルダー～",
        2012,
    ),
    "無職転生\ufffd\ufffd 〜異世界行ったら本気だす〜": (
        "無職転生 ～異世界行ったら本気だす～",
        2021,
    ),
    "ドラゴノーツ -ザ・レゾナンス-": ("ドラゴノーツ-ザ・レソナンス-", 2007),
    "アクティヴレイド2nd 機動強襲室第八係": ("アクティヴレイド 機動強襲室第八係", 2016),
    "W'z《ウィズ》": ("W'ｚ《ウィズ》", 2019),
    "みだらな青ちゃんは勉強ができない": ("淫らな青ちゃんは勉強ができない", 2019),
    "天地無用! 魎皇鬼 第三期": ("天地無用! 魎皇鬼 第1期", 1992),
    '"文学少女"メモワール': ("文学少女 メモワール", 2014),
    # added v24 (85%+ confidence, manually verified)
    "ゾイドフューザーズ": ("ゾイド・フューザース", 2004),
    "ゆるゆり さん☆ハイ!": ("ゆるゆり さん☆はい！", 2015),
    "スラップアップパーティー -アラド戦記-": (
        "アラド戦記 ～スラップアップパーティー～",
        2009,
    ),
    "TOKKO 特公": ("TOKKÔ[特公]", 2006),
    "I・R・I・A ZEIRAM THE ANIMATION": ("I・Я・I・A ZЁIЯAM THE ANIMATION", 1994),
    "ビーストウォーズネオ 超生命体トランスフォーマー": (
        "ビーストウォーズⅡ（セカンド） 超生命体トランスフォーマー",
        1998,
    ),
    "おちゃめなふたご クレア学院物語": ("おちゃめな双子　－クレア学院物語－", 1991),
    "サンリオ世界名作劇場": ("サンリオ・アニメ世界名作劇場", 2001),
    "まほろまてぃっく特別編 ただいま◆おかえり": (
        "まほろまてぃっく ただいま◇おかえり",
        2009,
    ),
    "空の境界 第五章　矛盾螺旋": ("空の境界 矛盾螺旋", 2008),
    "空の境界 第七章　殺人考察（後）": ("空の境界 殺人考察(後)", 2009),
    "東京魔人學園剣風帖 龍龍 第弐幕": ("東京魔人學園剣風帖　龖（トウ） 第弐幕", 2007),
    "メジャーセカンド（第2シリーズ）": ("メジャー2nd 第２シリーズ", 2020),
    "テイルズ オブ シンフォニア THE ANIMATION（第3期）": (
        "テイルズ オブ シンフォニア THE ANIMATION テセアラ編",
        2010,
    ),
    "テイルズ オブ シンフォニア THE ANIMATION（第2期）": (
        "テイルズ オブ シンフォニア THE ANIMATION テセアラ編",
        2010,
    ),
    "マジでオタクなイングリッシュ!りぼんちゃん the TV": (
        "マジでオタクなイングリッシュ! りぼんちゃん ~英語で戦う魔法少女~ the TV",
        2013,
    ),
    "Bビーダマン爆外伝V": ("B[ボンバーマン]ビーダマン爆外伝Ｖ", 1999),
    "Bビーダマン爆外伝": ("B[ボンバーマン]ビーダマン爆外伝", 1998),
    "頭文字D Second Stage": ("頭文字〈イニシャル〉D SECOND STAGE", 1999),
    "頭文字D Fourth Stage": ("頭文字〈イニシャル〉D FOURTH STAGE", 2004),
    "頭文字D Fifth Stage": ("頭文字〈イニシャル〉D Fifth Stage", 2012),
    "攻殻機動隊 ARISE　border:1 Ghost Pain": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:1 Ghost Pain",
        2013,
    ),
    "攻殻機動隊 ARISE　border:2 Ghost Whispers": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:2 Ghost Whispers",
        2013,
    ),
    "攻殻機動隊 ARISE　border:3 Ghost Tears": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:3 Ghost Tears",
        2014,
    ),
    "攻殻機動隊 ARISE　border:4 Ghost Stands Alone": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:4 Ghost Stands Alone",
        2014,
    ),
}


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Return a SQLite connection."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Check if new BEFORE connecting
    is_new_db = not db_path.exists()
    # Regular isolation mode for normal operations (init_db will set autocommit temporarily)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    # Only set WAL if DB already existed
    if not is_new_db:
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_connection(
    db_path: Path | None = None,
) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for a SQLite connection.

    Auto-commits on success, rolls back on exception, always closes.

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
    """Create tables (delegates to init_db_v2 target schema)."""
    from src.database_v2 import init_db_v2
    init_db_v2(conn)
def get_schema_version(conn: sqlite3.Connection) -> int:
    """Return the current schema version."""
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0
    except sqlite3.OperationalError:
        return 0
def compute_feat_career_annual(
    conn: sqlite3.Connection,
    batch_size: int = 2000,
) -> int:
    """Aggregate work/credit counts by person × career year × role category into feat_career_annual.

    career_year = credit_year - first_credit_year  (0 = debut year)

    Role categories follow ROLE_CATEGORY in src/utils/role_groups.py (14 categories).
    Unknown roles are counted under works_other.

    Args:
        conn: SQLite connection
        batch_size: batch size for bulk INSERT (per person)

    Returns:
        number of rows written
    """
    from src.utils.role_groups import ROLE_CATEGORY

    # role → column name mapping
    CAT_COL = {
        "direction": "works_direction",
        "animation_supervision": "works_animation_supervision",
        "animation": "works_animation",
        "design": "works_design",
        "technical": "works_technical",
        "art": "works_art",
        "sound": "works_sound",
        "writing": "works_writing",
        "production": "works_production",
        "production_management": "works_production_management",
        "finishing": "works_finishing",
        "editing": "works_editing",
        "settings": "works_settings",
    }
    logger.info("feat_career_annual_compute_start")

    # fetch debut year per person (minimum credit_year)
    debut_sql = """
        SELECT person_id, MIN(credit_year) AS debut_year
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id
    """
    debut_year = {
        r["person_id"]: r["debut_year"] for r in conn.execute(debut_sql).fetchall()
    }

    # aggregate person × year × role (unique work count and credit count)
    agg_sql = """
        SELECT
            person_id,
            credit_year,
            role,
            COUNT(DISTINCT anime_id) AS n_works,
            COUNT(*) AS n_credits
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id, credit_year, role
        ORDER BY person_id, credit_year
    """

    # process by person_id
    current_pid: str | None = None
    current_years: dict[int, dict] = {}  # credit_year → row_accumulator

    def _make_row_acc() -> dict:
        return {
            "n_works": 0,
            "n_credits": 0,
            "roles": set(),
            **{col: 0 for col in CAT_COL.values()},
            "works_other": 0,
        }

    rows_to_insert: list[tuple] = []
    total_written = 0

    def _flush_person(pid: str, years: dict[int, dict]) -> None:
        nonlocal rows_to_insert, total_written
        debut = debut_year.get(pid)
        if debut is None:
            return
        for credit_year, acc in sorted(years.items()):
            career_y = credit_year - debut
            rows_to_insert.append(
                (
                    pid,
                    career_y,
                    credit_year,
                    acc["n_works"],
                    acc["n_credits"],
                    len(acc["roles"]),
                    acc["works_direction"],
                    acc["works_animation_supervision"],
                    acc["works_animation"],
                    acc["works_design"],
                    acc["works_technical"],
                    acc["works_art"],
                    acc["works_sound"],
                    acc["works_writing"],
                    acc["works_production"],
                    acc["works_production_management"],
                    acc["works_finishing"],
                    acc["works_editing"],
                    acc["works_settings"],
                    acc["works_other"],
                )
            )
        if len(rows_to_insert) >= batch_size * 10:
            _insert_career_annual_batch(conn, rows_to_insert)
            total_written += len(rows_to_insert)
            rows_to_insert = []

    for row in conn.execute(agg_sql):
        pid = row["person_id"]
        if pid != current_pid:
            if current_pid is not None:
                _flush_person(current_pid, current_years)
            current_pid = pid
            current_years = {}

        yr = row["credit_year"]
        if yr not in current_years:
            current_years[yr] = _make_row_acc()
        acc = current_years[yr]

        role = row["role"]
        cat = ROLE_CATEGORY.get(role, "other")
        col = CAT_COL.get(cat, "works_other")
        acc[col] += row["n_works"]
        acc["n_works"] += row["n_works"]
        acc["n_credits"] += row["n_credits"]
        acc["roles"].add(role)

    if current_pid is not None:
        _flush_person(current_pid, current_years)

    if rows_to_insert:
        _insert_career_annual_batch(conn, rows_to_insert)
        total_written += len(rows_to_insert)

    conn.commit()
    logger.info("feat_career_annual_computed", rows=total_written)
    return total_written
def compute_feat_studio_affiliation(
    conn: sqlite3.Connection,
    batch_size: int = 10000,
) -> int:
    """Aggregate per-person × year × studio work participation into feat_studio_affiliation.

    Joins credits → anime_studios → studios to determine which studio's works
    each person participated in each year. Restricting to main studios (is_main=1)
    excludes production committees.

    Args:
        conn: SQLite connection
        batch_size: batch size for bulk INSERT

    Returns:
        number of rows written
    """
    logger.info("feat_studio_affiliation_compute_start")

    sql = """
        SELECT
            c.person_id,
            c.credit_year,
            ast.studio_id,
            COALESCE(s.name, '') AS studio_name,
            ast.is_main,
            COUNT(DISTINCT c.anime_id) AS n_works,
            COUNT(*) AS n_credits
        FROM credits c
        INNER JOIN anime_studios ast ON ast.anime_id = c.anime_id
        LEFT JOIN studios s ON s.id = ast.studio_id
        WHERE c.credit_year IS NOT NULL
        GROUP BY c.person_id, c.credit_year, ast.studio_id, ast.is_main
        ORDER BY c.person_id, c.credit_year
    """

    batch: list[tuple] = []
    total_written = 0

    for row in conn.execute(sql):
        batch.append(
            (
                row["person_id"],
                row["credit_year"],
                row["studio_id"],
                row["studio_name"],
                row["n_works"],
                row["n_credits"],
                row["is_main"],
            )
        )
        if len(batch) >= batch_size:
            conn.executemany(
                """
                INSERT INTO feat_studio_affiliation
                    (person_id, credit_year, studio_id, studio_name,
                     n_works, n_credits, is_main_studio)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(person_id, credit_year, studio_id) DO UPDATE SET
                    studio_name=excluded.studio_name,
                    n_works=excluded.n_works,
                    n_credits=excluded.n_credits,
                    is_main_studio=excluded.is_main_studio
            """,
                batch,
            )
            total_written += len(batch)
            batch = []

    if batch:
        conn.executemany(
            """
            INSERT INTO feat_studio_affiliation
                (person_id, credit_year, studio_id, studio_name,
                 n_works, n_credits, is_main_studio)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(person_id, credit_year, studio_id) DO UPDATE SET
                studio_name=excluded.studio_name,
                n_works=excluded.n_works,
                n_credits=excluded.n_credits,
                is_main_studio=excluded.is_main_studio
        """,
            batch,
        )
        total_written += len(batch)

    conn.commit()
    logger.info("feat_studio_affiliation_computed", rows=total_written)
    return total_written


_SOURCE_PRIORITY: dict[str, int] = {
    "anilist": 3,
    "mal": 2,
    "seesaawiki": 2,
    "mediaarts": 2,
    "ann": 1,
    "jvmg": 1,
    "keyframe": 1,
    "allcinema": 1,
}

_CJK_NAME_FIELDS: tuple[str, ...] = ("name_ja", "name_ko", "name_zh")


def _insert_career_annual_batch(conn: sqlite3.Connection, batch: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO feat_career_annual (
            person_id, career_year, credit_year,
            n_works, n_credits, n_roles,
            works_direction, works_animation_supervision, works_animation,
            works_design, works_technical, works_art, works_sound, works_writing,
            works_production, works_production_management, works_finishing,
            works_editing, works_settings, works_other
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(person_id, career_year) DO UPDATE SET
            credit_year=excluded.credit_year,
            n_works=excluded.n_works, n_credits=excluded.n_credits,
            n_roles=excluded.n_roles,
            works_direction=excluded.works_direction,
            works_animation_supervision=excluded.works_animation_supervision,
            works_animation=excluded.works_animation,
            works_design=excluded.works_design,
            works_technical=excluded.works_technical,
            works_art=excluded.works_art,
            works_sound=excluded.works_sound,
            works_writing=excluded.works_writing,
            works_production=excluded.works_production,
            works_production_management=excluded.works_production_management,
            works_finishing=excluded.works_finishing,
            works_editing=excluded.works_editing,
            works_settings=excluded.works_settings,
            works_other=excluded.works_other
        """,
        batch,
    )


def upsert_person(
    conn: sqlite3.Connection,
    person: Person,
    source: str = "",
) -> None:
    """Insert or update a person with source-aware primary name selection.

    Primary name determination:
    1. Higher-priority source wins (anilist=3 > mal/seesaawiki=2 > ann/others=1).
    2. When a CJK name field changes, the displaced value is added to aliases
       so no name history is lost.
    3. Non-name fields (bio, dates, social) always use COALESCE (first non-null wins).

    Call normalize_primary_names_by_credits() after full ETL + entity resolution
    to re-rank primary names by credit count (most-used name = primary).
    """
    import json

    incoming_priority = _SOURCE_PRIORITY.get(source, 0)

    existing = conn.execute(
        "SELECT name_ja, name_ko, name_zh, aliases, name_priority FROM persons WHERE id = ?",
        (person.id,),
    ).fetchone()

    if existing is None:
        # New record — straightforward insert
        conn.execute(
            """INSERT OR IGNORE INTO persons (
                   id, name_ja, name_en, name_ko, name_zh, aliases, nationality,
                   mal_id, anilist_id,
                   date_of_birth, hometown, blood_type, description, years_active, favourites, site_url,
                   name_priority
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                person.id,
                person.name_ja,
                person.name_en,
                person.name_ko,
                person.name_zh,
                json.dumps(person.aliases, ensure_ascii=False),
                json.dumps(person.nationality, ensure_ascii=False),
                person.mal_id,
                person.anilist_id,
                person.date_of_birth,
                person.hometown,
                person.blood_type,
                person.description,
                json.dumps(getattr(person, "years_active", []) or [], ensure_ascii=False),
                person.favourites,
                person.site_url,
                incoming_priority,
            ),
        )
        return

    # Existing record — check name field conflicts and preserve history in aliases.
    existing_priority: int = existing["name_priority"] or 0
    old_aliases: list[str] = json.loads(existing["aliases"] or "[]")

    # Decide whether incoming source can update primary name fields.
    update_primary = incoming_priority >= existing_priority

    # Collect the loser's CJK names into aliases so no name history is lost.
    # When incoming wins: existing names are displaced. When existing wins: incoming names are rejected.
    for field in _CJK_NAME_FIELDS:
        old_val: str = existing[field] or ""
        new_val: str = getattr(person, field, "") or ""
        if old_val and new_val and old_val != new_val:
            loser = old_val if update_primary else new_val
            if loser not in old_aliases:
                old_aliases.append(loser)

    # Also absorb any aliases the incoming record carries.
    for alias in person.aliases:
        if alias and alias not in old_aliases:
            old_aliases.append(alias)

    final_aliases = json.dumps(old_aliases, ensure_ascii=False)

    incoming_years_active = json.dumps(getattr(person, "years_active", []) or [], ensure_ascii=False)

    if update_primary:
        conn.execute(
            """UPDATE persons SET
                   name_ja   = COALESCE(NULLIF(?, ''), name_ja),
                   name_en   = COALESCE(NULLIF(?, ''), name_en),
                   name_ko   = COALESCE(NULLIF(?, ''), name_ko),
                   name_zh   = COALESCE(NULLIF(?, ''), name_zh),
                   aliases   = ?,
                   nationality = COALESCE(NULLIF(?, '[]'), nationality),
                   mal_id    = COALESCE(?, mal_id),
                   anilist_id = COALESCE(?, anilist_id),
                   date_of_birth = COALESCE(?, date_of_birth),
                   hometown  = COALESCE(?, hometown),
                   blood_type = COALESCE(?, blood_type),
                   description = COALESCE(?, description),
                   years_active = CASE WHEN ? != '[]' THEN ? ELSE years_active END,
                   favourites = COALESCE(?, favourites),
                   site_url  = COALESCE(?, site_url),
                   name_priority = ?
               WHERE id = ?""",
            (
                person.name_ja, person.name_en, person.name_ko, person.name_zh,
                final_aliases,
                json.dumps(person.nationality, ensure_ascii=False),
                person.mal_id, person.anilist_id,
                person.date_of_birth, person.hometown, person.blood_type,
                person.description,
                incoming_years_active, incoming_years_active,
                person.favourites, person.site_url,
                max(incoming_priority, existing_priority),
                person.id,
            ),
        )
    else:
        # Lower-priority source: skip primary name fields, update metadata + aliases only.
        conn.execute(
            """UPDATE persons SET
                   aliases   = ?,
                   nationality = COALESCE(NULLIF(?, '[]'), nationality),
                   mal_id    = COALESCE(?, mal_id),
                   anilist_id = COALESCE(?, anilist_id),
                   date_of_birth = COALESCE(?, date_of_birth),
                   hometown  = COALESCE(?, hometown),
                   blood_type = COALESCE(?, blood_type),
                   description = COALESCE(?, description),
                   years_active = CASE WHEN ? != '[]' THEN ? ELSE years_active END,
                   favourites = COALESCE(?, favourites),
                   site_url  = COALESCE(?, site_url)
               WHERE id = ?""",
            (
                final_aliases,
                json.dumps(person.nationality, ensure_ascii=False),
                person.mal_id, person.anilist_id,
                person.date_of_birth, person.hometown, person.blood_type,
                person.description,
                incoming_years_active, incoming_years_active,
                person.favourites, person.site_url,
                person.id,
            ),
        )


def normalize_primary_names_by_credits(conn: sqlite3.Connection) -> int:
    """Post-ETL: re-rank primary names by credit count (most-used name = primary).

    Run AFTER integrate_* functions and entity resolution are complete.

    For each person, finds which source contributed the most credits.
    If that source's name differs from the current primary, swaps:
      old primary → aliases, source name → primary.

    Returns the number of persons whose primary name was updated.
    """
    import json

    # Count credits per (person_id, evidence_source)
    credit_counts: dict[tuple[str, str], int] = {}
    for row in conn.execute(
        "SELECT person_id, evidence_source, COUNT(*) AS n FROM credits "
        "WHERE evidence_source != '' GROUP BY person_id, evidence_source"
    ):
        credit_counts[(row["person_id"], row["evidence_source"])] = row["n"]

    # Source → bronze name table mapping
    _BRONZE_NAME_QUERY: dict[str, str] = {
        "anilist": "SELECT name_ja, name_en FROM src_anilist_persons "
                   "WHERE anilist_id = CAST(? AS INT)",
        "ann":     "SELECT name_ja, name_en FROM src_ann_persons "
                   "WHERE ann_id = CAST(? AS INT)",
        "mal":     "SELECT name_ja, name_en FROM src_mal_persons "
                   "WHERE mal_id = CAST(? AS INT)",
    }

    # Build (person_id → best source) mapping by credit count
    best: dict[str, tuple[str, int]] = {}  # person_id → (source, n_credits)
    for (pid, src), n in credit_counts.items():
        cur_src, cur_n = best.get(pid, ("", 0))
        if n > cur_n or (n == cur_n and _SOURCE_PRIORITY.get(src, 0) > _SOURCE_PRIORITY.get(cur_src, 0)):
            best[pid] = (src, n)

    updated = 0
    for pid, (top_src, _) in best.items():
        query = _BRONZE_NAME_QUERY.get(top_src)
        if not query:
            continue  # source not in our name-lookup table

        # Look up external ID for this person/source
        ext_row = conn.execute(
            "SELECT external_id FROM person_external_ids WHERE person_id = ? AND source = ?",
            (pid, top_src),
        ).fetchone()
        if not ext_row:
            continue

        bronze_row = conn.execute(query, (ext_row["external_id"],)).fetchone()
        if not bronze_row:
            continue

        top_name_ja = bronze_row["name_ja"] or ""
        top_name_en = bronze_row["name_en"] or ""

        current = conn.execute(
            "SELECT name_ja, name_en, aliases FROM persons WHERE id = ?", (pid,)
        ).fetchone()
        if not current:
            continue

        cur_ja = current["name_ja"] or ""
        cur_en = current["name_en"] or ""
        if top_name_ja == cur_ja and top_name_en == cur_en:
            continue  # already correct

        old_aliases: list[str] = json.loads(current["aliases"] or "[]")
        for old_name in (cur_ja, cur_en):
            if old_name and old_name not in old_aliases:
                old_aliases.append(old_name)

        conn.execute(
            """UPDATE persons SET
                   name_ja = COALESCE(NULLIF(?, ''), name_ja),
                   name_en = COALESCE(NULLIF(?, ''), name_en),
                   aliases = ?
               WHERE id = ?""",
            (
                top_name_ja, top_name_en,
                json.dumps(old_aliases, ensure_ascii=False),
                pid,
            ),
        )
        updated += 1

    logger.info("normalize_primary_names_done", updated=updated)
    return updated


def upsert_anime(conn: sqlite3.Connection, anime: Anime) -> None:
    """Insert or update an anime (canonical silver: structural columns only)."""
    import json as _json

    conn.execute(
        """INSERT INTO anime (
               id, title_ja, title_en, year, season, episodes, format, status,
               start_date, end_date, duration, original_work_type, quarter, work_type, scale_class,
               country_of_origin, synonyms, is_adult
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
                title_ja = COALESCE(NULLIF(excluded.title_ja, ''), anime.title_ja),
                title_en = COALESCE(NULLIF(excluded.title_en, ''), anime.title_en),
                year = COALESCE(excluded.year, anime.year),
                season = COALESCE(excluded.season, anime.season),
                episodes = COALESCE(excluded.episodes, anime.episodes),
                format = COALESCE(excluded.format, anime.format),
                status = COALESCE(excluded.status, anime.status),
                start_date = COALESCE(excluded.start_date, anime.start_date),
                end_date = COALESCE(excluded.end_date, anime.end_date),
                duration = COALESCE(excluded.duration, anime.duration),
                original_work_type = COALESCE(excluded.original_work_type, anime.original_work_type),
                quarter = COALESCE(excluded.quarter, anime.quarter),
                work_type = COALESCE(excluded.work_type, anime.work_type),
                scale_class = COALESCE(excluded.scale_class, anime.scale_class),
                country_of_origin = COALESCE(excluded.country_of_origin, anime.country_of_origin),
                synonyms = CASE WHEN excluded.synonyms != '[]' THEN excluded.synonyms ELSE anime.synonyms END,
                is_adult = COALESCE(excluded.is_adult, anime.is_adult),
                updated_at = CURRENT_TIMESTAMP
        """,
        (
            anime.id,
            anime.title_ja,
            anime.title_en,
            anime.year,
            anime.season,
            anime.episodes,
            anime.format,
            anime.status,
            anime.start_date,
            anime.end_date,
            anime.duration,
            getattr(anime, "original_work_type", None) or getattr(anime, "source", None),
            anime.quarter,
            anime.work_type,
            anime.scale_class,
            getattr(anime, "country_of_origin", None),
            _json.dumps(getattr(anime, "synonyms", []) or [], ensure_ascii=False),
            1 if getattr(anime, "is_adult", None) else (0 if getattr(anime, "is_adult", None) is not None else None),
        ),
    )

    upsert_anime_analysis(
        conn,
        {
            "id": anime.id,
            "title_ja": anime.title_ja,
            "title_en": anime.title_en,
            "year": anime.year,
            "season": anime.season,
            "quarter": anime.quarter,
            "episodes": anime.episodes,
            "format": anime.format,
            "duration": anime.duration,
            "start_date": anime.start_date,
            "end_date": anime.end_date,
            "status": anime.status,
            "source": anime.source,
            "work_type": anime.work_type,
            "scale_class": anime.scale_class,
            "mal_id": anime.mal_id,
            "anilist_id": anime.anilist_id,
            "ann_id": getattr(anime, "ann_id", None),
            "allcinema_id": getattr(anime, "allcinema_id", None),
            "madb_id": getattr(anime, "madb_id", None),
        },
    )

    # Write studios to normalized tables (replaces anime_display.studios JSON denorm).
    for i, studio_name in enumerate(anime.studios or []):
        studio_id = studio_name.lower().replace(" ", "_")
        conn.execute(
            "INSERT OR IGNORE INTO studios (id, name) VALUES (?, ?)",
            (studio_id, studio_name),
        )
        conn.execute(
            "INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main) VALUES (?, ?, ?)",
            (anime.id, studio_id, 1 if i == 0 else 0),
        )


    # Keep external identifiers in normalized table (anime_external_ids).
    for source, external_id in (
        ("mal", anime.mal_id),
        ("anilist", anime.anilist_id),
        ("ann", getattr(anime, "ann_id", None)),
        ("allcinema", getattr(anime, "allcinema_id", None)),
        ("madb", getattr(anime, "madb_id", None)),
    ):
        if external_id is None:
            continue
        ext = str(external_id).strip()
        if not ext:
            continue
        conn.execute(
            """
            INSERT INTO anime_external_ids (anime_id, source, external_id)
            VALUES (?, ?, ?)
            ON CONFLICT(anime_id, source) DO UPDATE SET
                external_id = excluded.external_id
            """,
            (anime.id, source, ext),
        )


_ANIME_ANALYSIS_COLUMNS = (
    "id",
    "title_ja",
    "title_en",
    "year",
    "season",
    "quarter",
    "episodes",
    "format",
    "duration",
    "start_date",
    "end_date",
    "status",
    "source",
    "work_type",
    "scale_class",
    "mal_id",
    "anilist_id",
    "ann_id",
    "allcinema_id",
    "madb_id",
)


def upsert_anime_analysis(conn: sqlite3.Connection, row: dict) -> None:
    """No-op: anime_analysis table removed in target schema (v2)."""
    return


def ensure_meta_quality_snapshot(conn: sqlite3.Connection) -> None:
    """Create meta_quality_snapshot table/index if missing."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta_quality_snapshot (
            computed_at TEXT NOT NULL,
            table_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (computed_at, table_name, metric)
        );
        CREATE INDEX IF NOT EXISTS idx_quality_snapshot_metric
            ON meta_quality_snapshot(table_name, metric, computed_at);
        """
    )


def ensure_calc_execution_records(conn: sqlite3.Connection) -> None:
    """Create calc_execution_records table/index if missing."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS calc_execution_records (
            scope TEXT NOT NULL,
            calc_name TEXT NOT NULL,
            input_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'success',
            output_path TEXT NOT NULL DEFAULT '',
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (scope, calc_name)
        );
        CREATE INDEX IF NOT EXISTS idx_calc_exec_scope_hash
            ON calc_execution_records(scope, input_hash);
        """
    )


def get_calc_execution_hashes(
    conn: sqlite3.Connection,
    scope: str,
) -> dict[str, str]:
    """Get latest input_hash by calc_name for a scope."""
    ensure_calc_execution_records(conn)
    rows = conn.execute(
        "SELECT calc_name, input_hash FROM calc_execution_records WHERE scope = ?",
        (scope,),
    ).fetchall()
    return {row["calc_name"]: row["input_hash"] for row in rows}


def record_calc_execution(
    conn: sqlite3.Connection,
    scope: str,
    calc_name: str,
    input_hash: str,
    *,
    status: str = "success",
    output_path: str = "",
) -> None:
    """Upsert a calc execution record."""
    ensure_calc_execution_records(conn)
    conn.execute(
        """
        INSERT INTO calc_execution_records
            (scope, calc_name, input_hash, status, output_path, computed_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(scope, calc_name) DO UPDATE SET
            input_hash = excluded.input_hash,
            status = excluded.status,
            output_path = excluded.output_path,
            computed_at = CURRENT_TIMESTAMP
        """,
        (scope, calc_name, input_hash, status, output_path),
    )


def register_meta_lineage(
    conn: sqlite3.Connection,
    table_name: str,
    audience: str,
    source_silver_tables: list[str],
    formula_version: str,
    *,
    source_bronze_forbidden: int = 1,
    source_display_allowed: int = 0,
    ci_method: str | None = None,
    null_model: str | None = None,
    holdout_method: str | None = None,
    description: str = "",
    row_count: int | None = None,
    rng_seed: int | None = None,
    git_sha: str | None = None,
    inputs_hash: str | None = None,
    notes: str | None = None,
) -> None:
    """Register Gold table lineage information into the meta_lineage table."""
    import json as _json
    import hashlib as _hashlib
    import subprocess as _subprocess

    lineage_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(ops_lineage)").fetchall()
    }
    if row_count is None:
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except sqlite3.OperationalError:
            row_count = None

    if git_sha is None:
        try:
            git_sha = _subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip()
        except Exception:
            git_sha = ""
    if inputs_hash is None:
        payload = {
            "table_name": table_name,
            "source_silver_tables": sorted(source_silver_tables),
            "formula_version": formula_version,
        }
        inputs_hash = _hashlib.sha256(
            _json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    all_values = {
        "table_name": table_name,
        "audience": audience,
        "source_silver_tables": _json.dumps(source_silver_tables, ensure_ascii=False),
        "source_bronze_forbidden": source_bronze_forbidden,
        "source_display_allowed": source_display_allowed,
        "description": description,
        "formula_version": formula_version,
        "ci_method": ci_method,
        "null_model": null_model,
        "holdout_method": holdout_method,
        "row_count": row_count,
        "notes": notes,
        "rng_seed": rng_seed,
        "git_sha": git_sha or "",
        "inputs_hash": inputs_hash or "",
    }
    ordered_cols = [
        "table_name",
        "audience",
        "source_silver_tables",
        "source_bronze_forbidden",
        "source_display_allowed",
        "description",
        "formula_version",
        "computed_at",
        "ci_method",
        "null_model",
        "holdout_method",
        "row_count",
        "notes",
        "rng_seed",
        "git_sha",
        "inputs_hash",
    ]
    insert_cols: list[str] = []
    insert_values: list[Any] = []
    for col in ordered_cols:
        if col == "computed_at":
            continue
        if col in lineage_cols:
            insert_cols.append(col)
            insert_values.append(all_values[col])
    update_cols = [c for c in insert_cols if c != "table_name"]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    insert_cols_sql = ", ".join(insert_cols + ["computed_at"])
    placeholders = ", ".join(["?"] * len(insert_cols) + ["CURRENT_TIMESTAMP"])

    conn.execute(
        f"""INSERT INTO ops_lineage ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(table_name) DO UPDATE SET
                {update_clause},
                computed_at = CURRENT_TIMESTAMP""",
        insert_values,
    )


def upsert_meta_entity_resolution_audit(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert the entity-resolution audit table and update lineage."""
    if not rows:
        return 0

    cols = [
        "person_id",
        "canonical_name",
        "merge_method",
        "merge_confidence",
        "merged_from_keys",
        "merge_evidence",
        "reviewed_by",
        "reviewed_at",
    ]
    placeholders = ", ".join("?" for _ in cols)
    update_clause = ", ".join(
        f"{c}=excluded.{c}" for c in cols if c not in {"person_id", "reviewed_at"}
    )
    conn.executemany(
        f"""INSERT INTO ops_entity_resolution_audit ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT(person_id) DO UPDATE SET
                {update_clause},
                merged_at = CURRENT_TIMESTAMP""",
        [[r.get(c) for c in cols] for r in rows],
    )
    register_meta_lineage(
        conn,
        table_name="ops_entity_resolution_audit",
        audience="technical_appendix",
        source_silver_tables=["persons", "credits", "person_aliases"],
        formula_version="v2.0",
        description="Entity-resolution merge audit trail for legal verification.",
        ci_method="n/a",
        null_model="n/a",
        holdout_method="n/a",
        notes="Merge decisions logged without changing matching logic.",
    )
    return len(rows)


def insert_credit(conn: sqlite3.Connection, credit: Credit) -> None:
    """Insert a credit record (ignore duplicates)."""
    source = credit.evidence_source or credit.source
    raw_role = credit.raw_role or ""
    # SQLite UNIQUE treats NULL != NULL, so whole-series credits (episode=None)
    # need an explicit existence check to avoid duplicates.
    if credit.episode is None:
        exists = conn.execute(
            "SELECT 1 FROM credits WHERE person_id=? AND anime_id=? AND raw_role=? AND episode IS NULL",
            (credit.person_id, credit.anime_id, raw_role),
        ).fetchone()
        if exists:
            return
    conn.execute(
        """INSERT OR IGNORE INTO credits
           (person_id, anime_id, role, raw_role, episode, evidence_source, affiliation, position)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            credit.person_id,
            credit.anime_id,
            credit.role.value,
            raw_role,
            credit.episode,
            source,
            credit.affiliation,
            credit.position,
        ),
    )


def upsert_character(conn: sqlite3.Connection, character: Character) -> None:
    """Insert or update a character."""
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
    """Insert a character × voice actor × work relationship (ignore duplicates)."""
    conn.execute(
        """INSERT OR IGNORE INTO character_voice_actors
           (character_id, person_id, anime_id, character_role, source)
           VALUES (?, ?, ?, ?, ?)""",
        (cva.character_id, cva.person_id, cva.anime_id, cva.character_role, cva.source),
    )


def upsert_studio(conn: sqlite3.Connection, studio: Studio) -> None:
    """Insert or update a studio."""
    conn.execute(
        """INSERT INTO studios (id, name, anilist_id, is_animation_studio, country_of_origin, favourites, site_url)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name = COALESCE(NULLIF(excluded.name, ''), studios.name),
               anilist_id = COALESCE(excluded.anilist_id, studios.anilist_id),
               is_animation_studio = COALESCE(excluded.is_animation_studio, studios.is_animation_studio),
               country_of_origin = COALESCE(excluded.country_of_origin, studios.country_of_origin),
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
            getattr(studio, "country_of_origin", None),
            studio.favourites,
            studio.site_url,
        ),
    )


def insert_anime_studio(conn: sqlite3.Connection, anime_studio: AnimeStudio) -> None:
    """Insert an anime × studio relationship (ignore duplicates)."""
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
    """Insert an anime-to-anime relation (ignore duplicates)."""
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


def load_all_persons(conn: sqlite3.Connection) -> list[Person]:
    """Load all persons from the database."""
    from src.db_rows import PersonRow

    rows = conn.execute("SELECT * FROM persons").fetchall()
    return [Person.from_db_row(PersonRow.from_row(row)) for row in rows]


def load_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """Load all anime from the database."""
    from src.db_rows import AnimeRow

    rows = conn.execute("SELECT * FROM anime").fetchall()
    anime_list = [Anime.from_db_row(AnimeRow.from_row(row)) for row in rows]
    by_id = {a.id: a for a in anime_list}

    # Hydrate normalized external IDs into in-memory anime objects.
    for row in conn.execute(
        "SELECT anime_id, source, external_id FROM anime_external_ids"
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is None:
            continue
        source = row["source"]
        external_id = row["external_id"]
        if source in {"mal", "anilist", "ann", "allcinema"}:
            try:
                setattr(anime, f"{source}_id", int(external_id))
            except (TypeError, ValueError):
                continue
        elif source == "madb":
            anime.madb_id = external_id

    # Hydrate genres/tags from normalized tables.
    for row in conn.execute(
        "SELECT anime_id, genre_name FROM anime_genres ORDER BY genre_name"
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is not None:
            anime.genres.append(row["genre_name"])
    for row in conn.execute(
        "SELECT anime_id, tag_name, rank FROM anime_tags ORDER BY rank DESC, tag_name"
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is not None:
            anime.tags.append({"name": row["tag_name"], "rank": row["rank"]})

    # Hydrate studios from normalized relation table.
    for row in conn.execute(
        """
        SELECT ast.anime_id, s.name
        FROM anime_studios ast
        JOIN studios s ON s.id = ast.studio_id
        ORDER BY ast.is_main DESC, s.name
        """
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is not None and row["name"]:
            anime.studios.append(row["name"])

    return anime_list


def load_all_credits(conn: sqlite3.Connection) -> list[Credit]:
    """Load all credits from the database."""
    from src.db_rows import CreditRow

    rows = conn.execute("SELECT * FROM credits").fetchall()
    credits: list[Credit] = []
    skipped = 0
    for row in rows:
        try:
            credits.append(Credit.from_db_row(CreditRow.from_row(row)))
        except ValueError:
            skipped += 1
    if skipped:
        logger.warning("credits_skipped_unknown_role", count=skipped)
    return credits


def get_source_scrape_status(conn: sqlite3.Connection) -> list[dict]:
    """Return scrape sync state per source (last_scraped_at, item_count, status)."""
    rows = conn.execute(
        "SELECT source, last_scraped_at, item_count, status FROM ops_source_scrape_status ORDER BY source"
    ).fetchall()
    return [dict(r) for r in rows]


def load_all_scores(conn: sqlite3.Connection) -> list[ScoreResult]:
    """Load all scores from the database."""
    rows = conn.execute("SELECT * FROM person_scores").fetchall()
    return [
        ScoreResult(
            person_id=row["person_id"],
            person_fe=row["person_fe"],
            studio_fe_exposure=row["studio_fe_exposure"],
            birank=row["birank"],
            patronage=row["patronage"],
            dormancy=row["dormancy"],
            awcc=row["awcc"],
            iv_score=row["iv_score"],
            career_track=row["career_track"]
            if "career_track" in row.keys()
            else "multi_track",
        )
        for row in rows
    ]


def get_llm_decision(conn: sqlite3.Connection, name: str, task: str) -> dict | None:
    """Retrieve a cached LLM decision.

    Args:
        name: target name (person name or name pair)
        task: task type ("org_classification" | "name_normalization" | "entity_match")

    Returns:
        result_json parsed as dict, or None if not found
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
    """Save or update an LLM decision result."""
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
def compute_feat_career_gaps(
    conn: sqlite3.Connection,
    *,
    semi_exit_years: int = 3,
    exit_years: int = 5,
    reliable_max_year: int = 2025,
) -> int:
    """Compute career gaps and save to feat_career_gaps.

    Detects consecutive gaps in each person's credit year list:
    - semi_exit: semi_exit_years <= gap < exit_years
    - exit: gap >= exit_years
    - returned: True if credits exist after the gap

    Returns:
        Total rows written.
    """
    logger = structlog.get_logger()
    logger.info(
        "feat_career_gaps_compute_start",
        semi_exit_years=semi_exit_years,
        exit_years=exit_years,
    )

    # Get all person credit years (distinct)
    rows = conn.execute("""
        SELECT c.person_id, GROUP_CONCAT(DISTINCT a.year) AS years
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.year IS NOT NULL
        GROUP BY c.person_id
    """).fetchall()

    inserts: list[tuple] = []
    for row in rows:
        pid = row[0]
        year_strs = row[1]
        if not year_strs:
            continue
        years = sorted(set(int(y) for y in year_strs.split(",")))
        if len(years) < 2:
            continue

        for i in range(len(years) - 1):
            gap = years[i + 1] - years[i]
            if gap >= semi_exit_years:
                gap_start = years[i]
                gap_end = years[i + 1]
                gap_type = "exit" if gap >= exit_years else "semi_exit"
                inserts.append(
                    (
                        pid,
                        gap_start,
                        gap_end,
                        gap,
                        1,  # returned = True (there is a subsequent credit)
                        gap_type,
                    )
                )

        # Check if the person's last credit year indicates an ongoing gap
        last_year = years[-1]
        ongoing_gap = reliable_max_year - last_year
        if ongoing_gap >= semi_exit_years:
            gap_type = "exit" if ongoing_gap >= exit_years else "semi_exit"
            inserts.append(
                (
                    pid,
                    last_year,
                    None,  # gap_end = NULL (not yet returned)
                    ongoing_gap,
                    0,  # returned = False
                    gap_type,
                )
            )

    conn.execute("DELETE FROM feat_career_gaps")
    if inserts:
        conn.executemany(
            """INSERT OR REPLACE INTO feat_career_gaps
               (person_id, gap_start_year, gap_end_year, gap_length, returned, gap_type)
               VALUES (?,?,?,?,?,?)""",
            inserts,
        )
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM feat_career_gaps").fetchone()[0]
    n_returned = conn.execute(
        "SELECT COUNT(*) FROM feat_career_gaps WHERE returned = 1"
    ).fetchone()[0]
    n_exit = conn.execute(
        "SELECT COUNT(*) FROM feat_career_gaps WHERE gap_type = 'exit'"
    ).fetchone()[0]
    n_semi = conn.execute(
        "SELECT COUNT(*) FROM feat_career_gaps WHERE gap_type = 'semi_exit'"
    ).fetchone()[0]
    logger.info(
        "feat_career_gaps_computed",
        total=total,
        returned=n_returned,
        exits=n_exit,
        semi_exits=n_semi,
    )
    return total
def upsert_src_anilist_anime(conn: sqlite3.Connection, anime: "Anime") -> None:
    """Save raw AniList anime data to src_anilist_anime."""
    import json as _json

    if anime.anilist_id is None:
        return
    conn.execute(
        """INSERT INTO src_anilist_anime (
               anilist_id, title_ja, title_en, year, season, episodes, format,
               status, start_date, end_date, duration, source, description,
               score, genres, tags, studios, synonyms, cover_large, cover_medium,
               banner, popularity, favourites, site_url, mal_id,
               country_of_origin, is_licensed, is_adult, mean_score, relations_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(anilist_id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), src_anilist_anime.title_ja),
               title_en = COALESCE(NULLIF(excluded.title_en, ''), src_anilist_anime.title_en),
               year = COALESCE(excluded.year, src_anilist_anime.year),
               season = COALESCE(excluded.season, src_anilist_anime.season),
               episodes = COALESCE(excluded.episodes, src_anilist_anime.episodes),
               format = COALESCE(excluded.format, src_anilist_anime.format),
               status = COALESCE(excluded.status, src_anilist_anime.status),
               start_date = COALESCE(excluded.start_date, src_anilist_anime.start_date),
               end_date = COALESCE(excluded.end_date, src_anilist_anime.end_date),
               description = COALESCE(excluded.description, src_anilist_anime.description),
               score = COALESCE(excluded.score, src_anilist_anime.score),
               genres = excluded.genres,
               tags = excluded.tags,
               studios = excluded.studios,
               synonyms = excluded.synonyms,
               cover_large = COALESCE(excluded.cover_large, src_anilist_anime.cover_large),
               country_of_origin = COALESCE(excluded.country_of_origin, src_anilist_anime.country_of_origin),
               is_licensed = COALESCE(excluded.is_licensed, src_anilist_anime.is_licensed),
               is_adult = COALESCE(excluded.is_adult, src_anilist_anime.is_adult),
               mean_score = COALESCE(excluded.mean_score, src_anilist_anime.mean_score),
               relations_json = COALESCE(excluded.relations_json, src_anilist_anime.relations_json),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            anime.anilist_id,
            anime.title_ja,
            anime.title_en,
            anime.year,
            anime.season,
            anime.episodes,
            anime.format,
            anime.status,
            anime.start_date,
            anime.end_date,
            anime.duration,
            anime.source,
            anime.description,
            anime.score,
            _json.dumps(anime.genres, ensure_ascii=False),
            _json.dumps(anime.tags, ensure_ascii=False),
            _json.dumps(anime.studios, ensure_ascii=False),
            _json.dumps(anime.synonyms, ensure_ascii=False),
            anime.cover_large,
            anime.cover_medium,
            anime.banner,
            anime.popularity_rank,
            anime.favourites,
            anime.site_url,
            anime.mal_id,
            anime.country_of_origin,
            1 if anime.is_licensed else (0 if anime.is_licensed is not None else None),
            1 if anime.is_adult else (0 if anime.is_adult is not None else None),
            anime.mean_score,
            anime.relations_json,
        ),
    )
