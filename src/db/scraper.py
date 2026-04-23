"""Scraper-specific database insertion operations."""

import sqlite3
from typing import Any

from src.models import (
    Character,
    CharacterVoiceActor,
    Studio,
    AnimeStudio,
    AnimeRelation,
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


def upsert_src_anilist_anime(conn: sqlite3.Connection, anime: Any) -> None:
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
