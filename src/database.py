"""DEPRECATED: Use src.db modules instead.

This module is maintained for backward compatibility only.
New code should import from:
  - src.db.init (get_connection, init_db, get_schema_version)
  - src.db.etl (upsert_person, upsert_anime, insert_credit, etc.)
  - src.db.scraper (upsert_character, upsert_studio, etc.)
"""

import sqlite3
import warnings

import structlog

# Re-export for backward compatibility
from src.db import (
    # init
    get_connection,
    db_connection,
    init_db,
    get_schema_version,
    DEFAULT_DB_PATH,
    SCHEMA_VERSION,
    _FUZZY_MATCH_RULES,
    # etl
    upsert_person,
    normalize_primary_names_by_credits,
    upsert_anime,
    upsert_anime_analysis,
    ensure_meta_quality_snapshot,
    ensure_calc_execution_records,
    get_calc_execution_hashes,
    record_calc_execution,
    register_meta_lineage,
    upsert_meta_entity_resolution_audit,
    insert_credit,
    # scraper
    upsert_character,
    insert_character_voice_actor,
    upsert_studio,
    insert_anime_studio,
    insert_anime_relation,
    get_llm_decision,
    upsert_llm_decision,
    upsert_src_anilist_anime,
)
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

warnings.warn(
    "src.database is deprecated; use src.db modules instead",
    DeprecationWarning,
    stacklevel=2,
)

logger = structlog.get_logger()


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


__all__ = [
    # init
    "get_connection",
    "db_connection",
    "init_db",
    "get_schema_version",
    "DEFAULT_DB_PATH",
    "SCHEMA_VERSION",
    "_FUZZY_MATCH_RULES",
    # etl
    "upsert_person",
    "normalize_primary_names_by_credits",
    "upsert_anime",
    "upsert_anime_analysis",
    "ensure_meta_quality_snapshot",
    "ensure_calc_execution_records",
    "get_calc_execution_hashes",
    "record_calc_execution",
    "register_meta_lineage",
    "upsert_meta_entity_resolution_audit",
    "insert_credit",
    # scraper
    "upsert_character",
    "insert_character_voice_actor",
    "upsert_studio",
    "insert_anime_studio",
    "insert_anime_relation",
    "get_llm_decision",
    "upsert_llm_decision",
    "upsert_src_anilist_anime",
    # load/access
    "load_all_persons",
    "load_all_anime",
    "load_all_credits",
    "get_source_scrape_status",
    "load_all_scores",
    # models
    "Anime",
    "AnimeRelation",
    "AnimeStudio",
    "Character",
    "CharacterVoiceActor",
    "Credit",
    "Person",
    "ScoreResult",
    "Studio",
]
