"""Bronze → Silver ETL: integrate src_* tables into canonical anime/persons/credits tables."""

from __future__ import annotations

import json
import sqlite3

import structlog

from src.models import BronzeAnime, Credit, Person, parse_role
from src.etl.sources import get_source_prefix, get_all_sources

log = structlog.get_logger()


def _anime_to_analysis(anime: BronzeAnime) -> dict:
    """Build an anime_analysis dict from BronzeAnime (excludes score column)."""
    return {
        k: v
        for k, v in {
            "id": anime.id,
            "title_ja": anime.title_ja,
            "title_en": anime.title_en,
            "year": anime.year,
            "season": anime.season,
            "quarter": getattr(anime, "quarter", None),
            "episodes": anime.episodes,
            "format": anime.format,
            "duration": anime.duration,
            "start_date": anime.start_date,
            "end_date": anime.end_date,
            "status": anime.status,
            "source": anime.source,
            "work_type": getattr(anime, "work_type", None),
            "scale_class": getattr(anime, "scale_class", None),
            "country_of_origin": getattr(anime, "country_of_origin", None),
            "synonyms": getattr(anime, "synonyms", None),
            "is_adult": getattr(anime, "is_adult", None),
            "mal_id": anime.mal_id,
            "anilist_id": anime.anilist_id,
            "ann_id": getattr(anime, "ann_id", None),
            "allcinema_id": getattr(anime, "allcinema_id", None),
            "madb_id": getattr(anime, "madb_id", None),
        }.items()
        if v is not None
    }


def _upsert_anime_genres_tags(
    conn: sqlite3.Connection,
    anime_id: str,
    genres: list | None,
    tags: list | None,
) -> None:
    """Normalize genre/tag JSON into silver anime_genres/anime_tags."""
    conn.execute("DELETE FROM anime_genres WHERE anime_id = ?", (anime_id,))
    conn.execute("DELETE FROM anime_tags WHERE anime_id = ?", (anime_id,))

    for g in genres or []:
        if isinstance(g, str) and g:
            conn.execute(
                "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?, ?)",
                (anime_id, g),
            )
        elif isinstance(g, dict):
            name = g.get("name")
            if isinstance(name, str) and name:
                conn.execute(
                    "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?, ?)",
                    (anime_id, name),
                )

    for t in tags or []:
        if isinstance(t, str) and t:
            conn.execute(
                "INSERT OR IGNORE INTO anime_tags (anime_id, tag_name, rank) VALUES (?, ?, NULL)",
                (anime_id, t),
            )
            continue
        if not isinstance(t, dict):
            continue
        name = t.get("name")
        if not isinstance(name, str) or not name:
            continue
        rank = t.get("rank")
        if rank is not None and not (isinstance(rank, int) and 0 <= rank <= 100):
            rank = None
        conn.execute(
            "INSERT OR IGNORE INTO anime_tags (anime_id, tag_name, rank) VALUES (?, ?, ?)",
            (anime_id, name, rank),
        )


def upsert_canonical_anime(
    conn: sqlite3.Connection,
    anime_model: object,
    *,
    evidence_source: str,
) -> None:
    """Single entry point for scrapers to persist an anime to the silver layer.

    All scrapers must call this instead of importing upsert_anime directly.
    Schema evolution (new columns, dual-write to bronze) only requires
    touching this function.

    Args:
        conn: SQLite connection.
        anime_model: Pydantic Anime or BronzeAnime model instance.
        evidence_source: Source identifier (e.g. 'anilist', 'keyframe').
    """
    from src.database import upsert_anime

    upsert_anime(conn, anime_model)  # type: ignore[arg-type]


def integrate_anilist(conn: sqlite3.Connection) -> dict[str, int]:
    """Integrate src_anilist_* → canonical anime / persons / credits."""
    from src.database import (
        insert_credit,
        upsert_anime,
        upsert_anime_analysis,
        upsert_person,
    )

    stats = {"anime": 0, "persons": 0, "credits": 0}
    prefix = get_source_prefix(conn, "anilist")

    # Anime
    for row in conn.execute("SELECT * FROM src_anilist_anime"):
        anime = BronzeAnime(
            id=f"{prefix}{row['anilist_id']}",
            title_ja=row["title_ja"] or "",
            title_en=row["title_en"] or "",
            year=row["year"],
            season=row["season"],
            episodes=row["episodes"],
            format=row["format"],
            status=row["status"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            duration=row["duration"],
            source=row["source"],
            description=row["description"],
            score=row["score"],
            genres=json.loads(row["genres"] or "[]"),
            tags=json.loads(row["tags"] or "[]"),
            studios=json.loads(row["studios"] or "[]"),
            synonyms=json.loads(row["synonyms"] or "[]"),
            cover_large=row["cover_large"],
            cover_medium=row["cover_medium"],
            banner=row["banner"],
            popularity_rank=row["popularity"],
            favourites=row["favourites"],
            site_url=row["site_url"],
            mal_id=row["mal_id"],
            anilist_id=row["anilist_id"],
            country_of_origin=row["country_of_origin"] if "country_of_origin" in row.keys() else None,
            is_licensed=bool(row["is_licensed"]) if row["is_licensed"] is not None and "is_licensed" in row.keys() else None,
            is_adult=bool(row["is_adult"]) if row["is_adult"] is not None and "is_adult" in row.keys() else None,
            mean_score=row["mean_score"] if "mean_score" in row.keys() else None,
        )
        upsert_anime(conn, anime)
        upsert_anime_analysis(conn, _anime_to_analysis(anime))
        _upsert_anime_genres_tags(
            conn,
            anime.id,
            genres=json.loads(row["genres"] or "[]"),
            tags=json.loads(row["tags"] or "[]"),
        )
        stats["anime"] += 1

    # Persons
    for row in conn.execute("SELECT * FROM src_anilist_persons"):
        row_keys = row.keys()
        person = Person(
            id=f"{prefix}p{row['anilist_id']}",
            name_ja=row["name_ja"] or "",
            name_en=row["name_en"] or "",
            name_ko=row["name_ko"] or "" if "name_ko" in row_keys else "",
            name_zh=row["name_zh"] or "" if "name_zh" in row_keys else "",
            aliases=json.loads(row["aliases"] or "[]"),
            nationality=json.loads(row["nationality"] or "[]") if "nationality" in row_keys else [],
            date_of_birth=row["date_of_birth"],
            age=row["age"],
            gender=row["gender"],
            years_active=json.loads(row["years_active"] or "[]"),
            hometown=row["hometown"],
            blood_type=row["blood_type"],
            description=row["description"],
            image_large=row["image_large"],
            image_medium=row["image_medium"],
            favourites=row["favourites"],
            site_url=row["site_url"],
            anilist_id=row["anilist_id"],
        )
        upsert_person(conn, person, source="anilist")
        stats["persons"] += 1

    # Credits
    for row in conn.execute("SELECT * FROM src_anilist_credits"):
        anime_id = f"{prefix}{row['anilist_anime_id']}"
        person_id = f"{prefix}p{row['anilist_person_id']}"
        credit = Credit(
            person_id=person_id,
            anime_id=anime_id,
            role=parse_role(row["role"]),
            raw_role=row["role_raw"],
            source="anilist",
        )
        insert_credit(conn, credit)
        stats["credits"] += 1

    log.info("etl_anilist_done", **stats)
    return stats


def integrate_ann(conn: sqlite3.Connection) -> dict[str, int]:
    """Integrate src_ann_* → canonical anime / persons / credits."""
    from src.database import (
        insert_credit,
        upsert_anime,
        upsert_anime_analysis,
        upsert_person,
    )

    stats = {"anime": 0, "persons": 0, "credits": 0}
    prefix = get_source_prefix(conn, "ann")

    # Anime
    for row in conn.execute("SELECT * FROM src_ann_anime"):
        anime = BronzeAnime(
            id=f"{prefix}{row['ann_id']}",
            title_en=row["title_en"] or "",
            title_ja=row["title_ja"] or "",
            year=row["year"],
            episodes=row["episodes"],
            format=row["format"],
            genres=json.loads(row["genres"] or "[]"),
            start_date=row["start_date"],
            end_date=row["end_date"],
            ann_id=row["ann_id"],
        )
        upsert_anime(conn, anime)
        upsert_anime_analysis(conn, _anime_to_analysis(anime))
        _upsert_anime_genres_tags(
            conn,
            anime.id,
            genres=json.loads(row["genres"] or "[]"),
            tags=[],
        )
        stats["anime"] += 1

    # Persons
    for row in conn.execute("SELECT * FROM src_ann_persons"):
        person = Person(
            id=f"{prefix}{row['ann_id']}",
            name_en=row["name_en"] or "",
            name_ja=row["name_ja"] or "",
            date_of_birth=row["date_of_birth"],
            hometown=row["hometown"],
            blood_type=row["blood_type"],
            site_url=row["website"],
            description=row["description"],
            ann_id=row["ann_id"],
        )
        upsert_person(conn, person, source="ann")
        stats["persons"] += 1

    # Credits — only insert if both anime and person already exist in canonical tables
    for row in conn.execute("""
        SELECT c.*, a.id AS canon_anime_id, p.id AS canon_person_id
        FROM src_ann_credits c
        JOIN anime_external_ids x
          ON x.source = 'ann'
         AND x.external_id = CAST(c.ann_anime_id AS TEXT)
        JOIN anime a ON a.id = x.anime_id
        JOIN person_external_ids pei
          ON pei.source = 'ann'
         AND pei.external_id = CAST(c.ann_person_id AS TEXT)
        JOIN persons p ON p.id = pei.person_id
    """):
        credit = Credit(
            person_id=row["canon_person_id"],
            anime_id=row["canon_anime_id"],
            role=parse_role(row["role"]),
            raw_role=row["role_raw"],
            source="ann",
        )
        insert_credit(conn, credit)
        stats["credits"] += 1

    log.info("etl_ann_done", **stats)
    return stats


def integrate_allcinema(conn: sqlite3.Connection) -> dict[str, int]:
    """Integrate src_allcinema_* → canonical anime / persons / credits."""
    from src.database import (
        insert_credit,
        upsert_anime,
        upsert_anime_analysis,
        upsert_person,
    )
    from src.scrapers.allcinema_scraper import _JOB_ROLE_MAP

    stats = {"anime": 0, "persons": 0, "credits": 0}
    prefix = get_source_prefix(conn, "allcinema")

    # Anime
    for row in conn.execute("SELECT * FROM src_allcinema_anime"):
        anime = BronzeAnime(
            id=f"{prefix}{row['allcinema_id']}",
            title_ja=row["title_ja"] or "",
            year=row["year"],
            start_date=row["start_date"],
            description=row["synopsis"],
            allcinema_id=row["allcinema_id"],
        )
        upsert_anime(conn, anime)
        upsert_anime_analysis(conn, _anime_to_analysis(anime))
        stats["anime"] += 1

    # Persons
    for row in conn.execute("SELECT * FROM src_allcinema_persons"):
        person = Person(
            id=f"{prefix}{row['allcinema_id']}",
            name_ja=row["name_ja"] or "",
            name_en=row["name_en"] or "",
            allcinema_id=row["allcinema_id"],
        )
        upsert_person(conn, person, source="allcinema")
        conn.execute(
            "UPDATE persons SET allcinema_id = ? WHERE id = ?",
            (row["allcinema_id"], f"{prefix}{row['allcinema_id']}"),
        )
        stats["persons"] += 1

    # Credits — resolve role from job_name
    for row in conn.execute("SELECT * FROM src_allcinema_credits"):
        anime_id = f"{prefix}{row['allcinema_anime_id']}"
        person_id = f"{prefix}{row['allcinema_person_id']}"
        job_name = row["job_name"] or ""
        role_str = _JOB_ROLE_MAP.get(job_name, "other")
        try:
            role = parse_role(role_str)
        except Exception:
            role = parse_role("other")
        credit = Credit(
            person_id=person_id,
            anime_id=anime_id,
            role=role,
            raw_role=job_name,
            source="allcinema",
        )
        insert_credit(conn, credit)
        stats["credits"] += 1

    log.info("etl_allcinema_done", **stats)
    return stats


def integrate_seesaawiki(conn: sqlite3.Connection) -> dict[str, int]:
    """Integrate src_seesaawiki_* → canonical anime / persons / credits.

    SeesaaWiki has no global person ID, so persons are created by name.
    """
    from src.database import (
        insert_credit,
        upsert_anime,
        upsert_anime_analysis,
        upsert_person,
    )
    from src.scrapers.seesaawiki_scraper import make_seesaa_person_id

    stats = {"anime": 0, "persons": 0, "credits": 0}
    person_cache: dict[str, str] = {}

    # Anime
    for row in conn.execute("SELECT * FROM src_seesaawiki_anime"):
        anime = BronzeAnime(
            id=row["id"],
            title_ja=row["title_ja"] or "",
            year=row["year"],
            episodes=row["episodes"],
        )
        upsert_anime(conn, anime)
        upsert_anime_analysis(conn, _anime_to_analysis(anime))
        stats["anime"] += 1

    # Credits (persons created on demand)
    for row in conn.execute(
        "SELECT * FROM src_seesaawiki_credits WHERE is_company = 0"
    ):
        name = row["person_name"]
        if name not in person_cache:
            pid = make_seesaa_person_id(name)
            person = Person(id=pid, name_ja=name)
            upsert_person(conn, person, source="seesaawiki")
            person_cache[name] = pid
            stats["persons"] += 1
        person_id = person_cache[name]
        credit = Credit(
            person_id=person_id,
            anime_id=row["anime_src_id"],
            role=parse_role(row["role"]),
            raw_role=row["role_raw"],
            episode=row["episode"],
            source="seesaawiki",
        )
        insert_credit(conn, credit)
        stats["credits"] += 1

    log.info("etl_seesaawiki_done", **stats)
    return stats


def integrate_keyframe(conn: sqlite3.Connection) -> dict[str, int]:
    """Integrate src_keyframe_* → canonical anime / persons / credits."""
    from src.database import (
        insert_credit,
        upsert_anime,
        upsert_anime_analysis,
        upsert_person,
    )
    from src.scrapers.keyframe_scraper import (
        make_keyframe_anime_id,
        make_keyframe_person_id,
    )

    stats = {"anime": 0, "persons": 0, "credits": 0}
    person_cache: set[str] = set()

    # Anime
    for row in conn.execute("SELECT * FROM src_keyframe_anime"):
        # If anilist_id maps to an existing canonical entry, skip creating a new one
        existing_id: str | None = None
        if row["anilist_id"]:
            r = conn.execute(
                """
                SELECT anime_id AS id
                FROM anime_external_ids
                WHERE source = 'anilist' AND external_id = ?
                """,
                (str(row["anilist_id"]),),
            ).fetchone()
            if r:
                existing_id = r[0]

        if existing_id:
            conn.execute(
                "UPDATE anime SET title_en = COALESCE(NULLIF(?, ''), title_en) WHERE id = ?",
                (row["title_en"], existing_id),
            )
            upsert_anime_analysis(
                conn,
                {
                    "id": existing_id,
                    "title_en": row["title_en"] or "",
                },
            )
        else:
            anime = BronzeAnime(
                id=make_keyframe_anime_id(row["slug"]),
                title_en=row["title_en"] or "",
                title_ja=row["title_ja"] or "",
                anilist_id=row["anilist_id"],
            )
            upsert_anime(conn, anime)
            upsert_anime_analysis(conn, _anime_to_analysis(anime))
            stats["anime"] += 1

    # Credits (persons created on demand)
    for row in conn.execute("SELECT * FROM src_keyframe_credits"):
        slug = row["keyframe_slug"]
        person_id = make_keyframe_person_id(row["kf_person_id"])
        if person_id not in person_cache:
            person = Person(
                id=person_id,
                name_ja=row["name_ja"] or "",
                name_en=row["name_en"] or "",
            )
            upsert_person(conn, person, source="keyframe")
            person_cache.add(person_id)
            stats["persons"] += 1

        # Resolve anime_id: check if anilist-matched ID exists
        kf_anime_id = make_keyframe_anime_id(slug)
        r = conn.execute(
            "SELECT id FROM src_keyframe_anime WHERE slug = ?", (slug,)
        ).fetchone()
        anilist_id = r["anilist_id"] if r else None
        if anilist_id:
            existing = conn.execute(
                """
                SELECT anime_id AS id
                FROM anime_external_ids
                WHERE source = 'anilist' AND external_id = ?
                """,
                (str(anilist_id),),
            ).fetchone()
            if existing:
                kf_anime_id = existing[0]

        role_ja = row["role_ja"] or ""
        role_en = row["role_en"] or ""
        role = parse_role(role_ja) if role_ja else parse_role(role_en)
        credit = Credit(
            person_id=person_id,
            anime_id=kf_anime_id,
            role=role,
            raw_role=role_ja or role_en,
            episode=row["episode"],
            source="keyframe",
        )
        insert_credit(conn, credit)
        stats["credits"] += 1

    log.info("etl_keyframe_done", **stats)
    return stats


def run_integration(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    """Integrate all source tables into the canonical tables."""
    results: dict[str, dict[str, int]] = {}
    
    # Get available sources from database (or use defaults)
    source_funcs = {
        "anilist": integrate_anilist,
        "ann": integrate_ann,
        "allcinema": integrate_allcinema,
        "seesaawiki": integrate_seesaawiki,
        "keyframe": integrate_keyframe,
    }
    
    # Get sources from DB or use all defined functions
    available_sources = get_all_sources(conn)
    
    for source in available_sources:
        if source not in source_funcs:
            log.warning("unknown_source_in_db", source=source)
            continue
        
        fn = source_funcs[source]
        try:
            results[source] = fn(conn)
        except Exception as exc:
            log.error("etl_source_failed", source=source, error=str(exc))
            results[source] = {"error": str(exc)}
    
    conn.commit()
    log.info("etl_integration_done", sources=list(results.keys()))
    return results
