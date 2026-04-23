"""ETL & data insertion operations."""

import sqlite3
from typing import Any

import structlog

from src.runtime.models import Person, Credit, BronzeAnime

logger = structlog.get_logger()

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


def upsert_anime(conn: sqlite3.Connection, anime: BronzeAnime) -> None:
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
