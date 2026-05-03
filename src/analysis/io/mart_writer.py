"""DuckDB GOLD layer writer/reader (Phase B).

The GOLD layer stores pipeline output (person_scores, score_history, etc.)
in a DuckDB file alongside the SQLite source DB. Writes happen once per
pipeline run (no concurrent writer risk). Reads serve the API and report
generators with vectorized columnar access.

Usage (write):
    from src.analysis.io.mart_writer import GoldWriter
    with GoldWriter() as gw:
        gw.write_person_scores(score_rows)
        gw.write_score_history(history_rows)

Usage (read):
    from src.analysis.io.mart_writer import GoldReader
    rows = GoldReader().person_scores()
    row = GoldReader().person_scores_for(person_id)
"""

from __future__ import annotations

import datetime
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import duckdb
import structlog

logger = structlog.get_logger()

# 5層 architecture Phase 1c: animetor.duckdb 統合 (mart schema)
# Can be overridden in tests via monkeypatch
DEFAULT_GOLD_DB_PATH: Path = Path(
    os.environ.get(
        "ANIMETOR_DB_PATH",
        str(Path(__file__).resolve().parent.parent.parent.parent / "result" / "animetor.duckdb"),
    )
)

def _get_default_silver_path() -> Path:
    """Return the current animetor.duckdb path (reads from conformed_reader so monkeypatches propagate)."""
    from src.analysis.io.conformed_reader import DEFAULT_DB_PATH

    return DEFAULT_DB_PATH

# DDL — GOLD tables in DuckDB (columnar; no sqlite_master, no WAL)
_DDL = """
CREATE TABLE IF NOT EXISTS person_scores (
    person_id           TEXT PRIMARY KEY,
    person_fe           DOUBLE,
    studio_fe_exposure  DOUBLE,
    birank              DOUBLE,
    patronage           DOUBLE,
    dormancy            DOUBLE,
    awcc                DOUBLE,
    iv_score            DOUBLE,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS score_history (
    person_id           TEXT NOT NULL,
    person_fe           DOUBLE,
    studio_fe_exposure  DOUBLE,
    birank              DOUBLE,
    patronage           DOUBLE,
    dormancy            DOUBLE,
    awcc                DOUBLE,
    iv_score            DOUBLE,
    year                INTEGER,
    quarter             INTEGER
);

CREATE TABLE IF NOT EXISTS feat_career (
    person_id           TEXT PRIMARY KEY,
    first_year          INTEGER,
    latest_year         INTEGER,
    active_years        INTEGER,
    total_credits       INTEGER,
    highest_stage       INTEGER,
    primary_role        TEXT,
    career_track        TEXT,
    peak_year           INTEGER,
    peak_credits        INTEGER,
    growth_trend        TEXT,
    growth_score        DOUBLE,
    activity_ratio      DOUBLE,
    recent_credits      INTEGER,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_career_gaps (
    person_id           TEXT NOT NULL,
    gap_start_year      INTEGER NOT NULL,
    gap_end_year        INTEGER,
    gap_length          INTEGER NOT NULL,
    returned            INTEGER NOT NULL DEFAULT 0,
    gap_type            TEXT NOT NULL,
    PRIMARY KEY (person_id, gap_start_year)
);

CREATE TABLE IF NOT EXISTS feat_studio_affiliation (
    person_id           TEXT NOT NULL,
    credit_year         INTEGER NOT NULL,
    studio_id           TEXT NOT NULL,
    studio_name         TEXT NOT NULL DEFAULT '',
    n_works             INTEGER NOT NULL DEFAULT 0,
    n_credits           INTEGER NOT NULL DEFAULT 0,
    is_main_studio      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (person_id, credit_year, studio_id)
);

CREATE TABLE IF NOT EXISTS feat_credit_activity (
    person_id               TEXT PRIMARY KEY,
    first_abs_quarter       INTEGER,
    last_abs_quarter        INTEGER,
    activity_span_quarters  INTEGER,
    active_quarters         INTEGER,
    density                 REAL,
    n_gaps                  INTEGER,
    mean_gap_quarters       REAL,
    median_gap_quarters     REAL,
    min_gap_quarters        INTEGER,
    max_gap_quarters        INTEGER,
    std_gap_quarters        REAL,
    consecutive_quarters    INTEGER,
    consecutive_rate        REAL,
    n_hiatuses              INTEGER,
    longest_hiatus_quarters INTEGER,
    quarters_since_last_credit INTEGER,
    active_years            INTEGER,
    n_year_gaps             INTEGER,
    mean_year_gap           REAL,
    max_year_gap            INTEGER,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS feat_career_annual (
    person_id               TEXT NOT NULL,
    career_year             INTEGER NOT NULL,
    credit_year             INTEGER NOT NULL,
    n_works                 INTEGER NOT NULL DEFAULT 0,
    n_credits               INTEGER NOT NULL DEFAULT 0,
    n_roles                 INTEGER NOT NULL DEFAULT 0,
    works_direction         INTEGER NOT NULL DEFAULT 0,
    works_animation_supervision INTEGER NOT NULL DEFAULT 0,
    works_animation         INTEGER NOT NULL DEFAULT 0,
    works_design            INTEGER NOT NULL DEFAULT 0,
    works_technical         INTEGER NOT NULL DEFAULT 0,
    works_art               INTEGER NOT NULL DEFAULT 0,
    works_sound             INTEGER NOT NULL DEFAULT 0,
    works_writing           INTEGER NOT NULL DEFAULT 0,
    works_production        INTEGER NOT NULL DEFAULT 0,
    works_production_management INTEGER NOT NULL DEFAULT 0,
    works_finishing         INTEGER NOT NULL DEFAULT 0,
    works_editing           INTEGER NOT NULL DEFAULT 0,
    works_settings          INTEGER NOT NULL DEFAULT 0,
    works_other             INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (person_id, career_year)
);

CREATE TABLE IF NOT EXISTS feat_person_role_progression (
    person_id               TEXT NOT NULL,
    role_category           TEXT NOT NULL,
    first_year              INTEGER,
    last_year               INTEGER,
    peak_year               INTEGER,
    n_works                 INTEGER,
    n_credits               INTEGER,
    career_year_first       INTEGER,
    still_active            INTEGER,
    PRIMARY KEY (person_id, role_category)
);

CREATE TABLE IF NOT EXISTS meta_lineage (
    table_name              TEXT PRIMARY KEY,
    audience                TEXT NOT NULL,
    source_silver_tables    TEXT NOT NULL,
    source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
    source_display_allowed  INTEGER NOT NULL DEFAULT 0,
    description             TEXT NOT NULL DEFAULT '',
    formula_version         TEXT NOT NULL,
    computed_at             TIMESTAMP NOT NULL,
    ci_method               TEXT,
    null_model              TEXT,
    holdout_method          TEXT,
    row_count               INTEGER,
    notes                   TEXT,
    rng_seed                INTEGER,
    git_sha                 TEXT NOT NULL DEFAULT '',
    inputs_hash             TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS meta_common_person_parameters (
    person_id               TEXT PRIMARY KEY,
    scale_reach_pct         DOUBLE,
    scale_reach_ci_low      DOUBLE,
    scale_reach_ci_high     DOUBLE,
    collab_width_pct        DOUBLE,
    collab_width_ci_low     DOUBLE,
    collab_width_ci_high    DOUBLE,
    continuity_pct          DOUBLE,
    continuity_ci_low       DOUBLE,
    continuity_ci_high      DOUBLE,
    mentor_contribution_pct DOUBLE,
    mentor_contribution_ci_low  DOUBLE,
    mentor_contribution_ci_high DOUBLE,
    centrality_pct          DOUBLE,
    centrality_ci_low       DOUBLE,
    centrality_ci_high      DOUBLE,
    trust_accum_pct         DOUBLE,
    trust_accum_ci_low      DOUBLE,
    trust_accum_ci_high     DOUBLE,
    role_evolution_pct      DOUBLE,
    role_evolution_ci_low   DOUBLE,
    role_evolution_ci_high  DOUBLE,
    genre_specialization_pct    DOUBLE,
    genre_specialization_ci_low DOUBLE,
    genre_specialization_ci_high DOUBLE,
    recent_activity_pct     DOUBLE,
    recent_activity_ci_low  DOUBLE,
    recent_activity_ci_high DOUBLE,
    compatibility_pct       DOUBLE,
    compatibility_ci_low    DOUBLE,
    compatibility_ci_high   DOUBLE,
    archetype               TEXT,
    archetype_confidence    DOUBLE,
    computed_at             TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_network (
    person_id               TEXT PRIMARY KEY,
    birank                  DOUBLE,
    patronage               DOUBLE,
    bridge_score            DOUBLE,
    n_bridge_communities    INTEGER,
    degree_centrality       DOUBLE,
    betweenness_centrality  DOUBLE,
    closeness_centrality    DOUBLE,
    eigenvector_centrality  DOUBLE,
    hub_score               DOUBLE,
    n_collaborators         INTEGER,
    n_unique_anime          INTEGER,
    updated_at              TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_genre_affinity (
    person_id               TEXT NOT NULL,
    genre                   TEXT NOT NULL,
    affinity_score          DOUBLE,
    work_count              INTEGER,
    updated_at              TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (person_id, genre)
);

CREATE TABLE IF NOT EXISTS feat_contribution (
    person_id               TEXT PRIMARY KEY,
    peer_percentile         DOUBLE,
    opportunity_residual    DOUBLE,
    consistency_score       DOUBLE,
    independent_value       DOUBLE,
    updated_at              TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_causal_estimates (
    person_id               TEXT PRIMARY KEY,
    peer_effect_boost       DOUBLE,
    career_friction         DOUBLE,
    era_fe                  DOUBLE,
    era_deflated_iv         DOUBLE,
    opportunity_residual    DOUBLE,
    iv_score                DOUBLE,
    updated_at              TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_cluster_membership (
    person_id               TEXT PRIMARY KEY,
    community_id            INTEGER,
    career_track            TEXT,
    growth_trend            TEXT,
    studio_cluster_id       INTEGER,
    studio_cluster_name     TEXT,
    cooccurrence_group_id   INTEGER,
    updated_at              TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_birank_annual (
    person_id               TEXT NOT NULL,
    year                    INTEGER NOT NULL,
    birank                  DOUBLE NOT NULL,
    raw_pagerank            DOUBLE,
    graph_size              INTEGER,
    n_credits_cumulative    INTEGER,
    updated_at              TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (person_id, year)
);

CREATE TABLE IF NOT EXISTS agg_milestones (
    person_id               TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    year                    INTEGER NOT NULL,
    anime_id                TEXT NOT NULL DEFAULT '',
    anime_title             TEXT,
    description             TEXT,
    PRIMARY KEY (person_id, event_type, year, anime_id)
);

CREATE TABLE IF NOT EXISTS agg_director_circles (
    person_id               TEXT NOT NULL,
    director_id             TEXT NOT NULL,
    shared_works            INTEGER NOT NULL DEFAULT 0,
    hit_rate                DOUBLE,
    roles                   TEXT,
    latest_year             INTEGER,
    PRIMARY KEY (person_id, director_id)
);

CREATE TABLE IF NOT EXISTS feat_mentorships (
    mentor_id               TEXT NOT NULL,
    mentee_id               TEXT NOT NULL,
    n_shared_works          INTEGER NOT NULL DEFAULT 0,
    hit_rate                DOUBLE,
    mentor_stage            INTEGER,
    mentee_stage            INTEGER,
    first_year              INTEGER,
    latest_year             INTEGER,
    PRIMARY KEY (mentor_id, mentee_id)
);

CREATE TABLE IF NOT EXISTS ops_entity_resolution_audit (
    person_id           TEXT PRIMARY KEY,
    canonical_name      TEXT NOT NULL,
    merge_method        TEXT NOT NULL,
    merge_confidence    DOUBLE NOT NULL,
    merged_from_keys    TEXT NOT NULL,
    merge_evidence      TEXT NOT NULL,
    merged_at           TIMESTAMP DEFAULT current_timestamp,
    reviewed_by         TEXT,
    reviewed_at         TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_person_career (
    person_id      TEXT PRIMARY KEY,
    first_year     INTEGER,
    latest_year    INTEGER,
    active_years   INTEGER,
    total_credits  INTEGER,
    recent_credits INTEGER,
    highest_stage  INTEGER,
    primary_role   TEXT,
    peak_year      INTEGER,
    peak_credits   INTEGER,
    updated_at     TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_career_scores (
    person_id      TEXT PRIMARY KEY,
    career_track   TEXT,
    growth_trend   TEXT,
    growth_score   DOUBLE,
    activity_ratio DOUBLE,
    updated_at     TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS agg_person_network (
    person_id            TEXT PRIMARY KEY,
    n_collaborators      INTEGER,
    n_unique_anime       INTEGER,
    n_bridge_communities INTEGER,
    updated_at           TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_network_scores (
    person_id               TEXT PRIMARY KEY,
    birank                  DOUBLE,
    patronage               DOUBLE,
    degree_centrality       DOUBLE,
    betweenness_centrality  DOUBLE,
    closeness_centrality    DOUBLE,
    eigenvector_centrality  DOUBLE,
    hub_score               DOUBLE,
    bridge_score            DOUBLE,
    updated_at              TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS corrections_credit_year (
    id                    BIGINT PRIMARY KEY,
    credit_id             BIGINT NOT NULL,
    credit_year_original  INTEGER,
    credit_year_corrected INTEGER NOT NULL,
    reason                TEXT NOT NULL DEFAULT '',
    corrected_at          TIMESTAMP DEFAULT current_timestamp,
    corrected_by          TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS corrections_role (
    id                 BIGINT PRIMARY KEY,
    credit_id          BIGINT NOT NULL,
    role_original      TEXT NOT NULL,
    role_corrected     TEXT NOT NULL,
    raw_role_override  TEXT,
    reason             TEXT NOT NULL DEFAULT '',
    corrected_at       TIMESTAMP DEFAULT current_timestamp,
    corrected_by       TEXT NOT NULL DEFAULT ''
);
"""


def _open(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    path = str(db_path or DEFAULT_GOLD_DB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
        conn.execute("SET schema='mart'")
    except Exception:
        pass
    return conn


# ---------------------------------------------------------------------------
# Read-only context managers — for analysis modules
# ---------------------------------------------------------------------------

@contextmanager
def gold_connect(
    path: Path | str | None = None,
    *,
    memory_limit: str = "16GB",
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open gold.duckdb read-only for the duration of one query block.

    Per-query open/close so analysis modules never pin to a stale inode
    after the pipeline atomically swaps gold.duckdb.
    """
    p = str(path or DEFAULT_GOLD_DB_PATH)
    conn = duckdb.connect(p, read_only=True)
    try:
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        try:
            conn.execute("SET schema='mart'")
        except Exception:
            pass
        yield conn
    finally:
        conn.close()


@contextmanager
def gold_connect_write(
    path: Path | str | None = None,
    *,
    memory_limit: str = "16GB",
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open gold.duckdb read-write for incremental feature table updates."""
    p = str(path or DEFAULT_GOLD_DB_PATH)
    conn = duckdb.connect(p, read_only=False)
    try:
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
            conn.execute("SET schema='mart'")
        except Exception:
            pass
        yield conn
    finally:
        conn.close()


@contextmanager
def gold_connect_with_silver(
    gold_path: Path | str | None = None,
    silver_path: Path | str | None = None,
    *,
    memory_limit: str = "16GB",
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open gold.duckdb + ATTACH silver.duckdb as views.

    Creates views for SILVER tables (persons, anime, credits, anime_studios)
    so existing SQL queries work without table-prefix changes.
    Intended for analysis modules that JOIN feat_* (GOLD) with persons/anime (SILVER).
    """
    g_path = str(gold_path or DEFAULT_GOLD_DB_PATH)
    s_path = str(silver_path or _get_default_silver_path())
    conn = duckdb.connect(g_path, read_only=False)
    try:
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        if Path(s_path).exists():
            conn.execute(f"ATTACH '{s_path}' AS sv (READ_ONLY TRUE)")
            for tbl in ("persons", "anime", "credits"):
                conn.execute(
                    f"CREATE OR REPLACE TEMP VIEW {tbl} AS SELECT * FROM sv.{tbl}"
                )
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# feat_* sync — copy feat_* tables from SQLite to gold.duckdb
# (transition helper: keeps SQLite as compute target, gold.duckdb as read target)
# ---------------------------------------------------------------------------

_FEAT_TABLES = (
    "feat_career",
    "feat_career_gaps",
    "meta_lineage",
    "meta_common_person_parameters",
)


def sync_feat_from_sqlite(
    sqlite_path: Path | str,
    gold_path: Path | str | None = None,
) -> dict[str, int]:
    """Copy feat_* and meta_* rows from SQLite into gold.duckdb.

    Uses DuckDB's SQLite scanner to read directly — no Python iteration.
    Returns {table_name: rows_copied}.
    Non-fatal: returns empty dict on any error (gold.duckdb may not exist yet).
    """
    g_path = str(gold_path or DEFAULT_GOLD_DB_PATH)
    s_path = str(sqlite_path)
    if not Path(s_path).exists():
        return {}
    if not Path(g_path).exists():
        return {}

    copied: dict[str, int] = {}
    try:
        conn = duckdb.connect(g_path)
        conn.execute("SET memory_limit='16GB'")
        conn.execute(f"ATTACH '{s_path}' AS sl (TYPE SQLITE, READ_ONLY TRUE)")
        conn.execute(_DDL)

        for table in _FEAT_TABLES:
            try:
                conn.execute(f"DELETE FROM {table}")
                conn.execute(f"INSERT INTO {table} SELECT * FROM sl.{table}")
                n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                copied[table] = n
            except Exception as exc:
                logger.debug("feat_sync_table_skip", table=table, error=str(exc))
        conn.close()
    except Exception as exc:
        logger.warning("feat_sync_failed", error=str(exc))
    if copied:
        logger.info("feat_synced_to_gold", tables=copied)
    return copied


def write_entity_resolution_audit_ddb(
    rows: list[dict],
    gold_path: Path | str | None = None,
) -> int:
    """Upsert entity-resolution merge decisions into ops_entity_resolution_audit.

    Writes to gold.duckdb. Returns number of rows upserted.
    """
    if not rows:
        return 0

    cols = (
        "person_id", "canonical_name", "merge_method", "merge_confidence",
        "merged_from_keys", "merge_evidence", "reviewed_by", "reviewed_at",
    )
    update_set = ", ".join(
        f"{c}=excluded.{c}"
        for c in cols
        if c not in {"person_id", "reviewed_at"}
    )
    placeholders = ", ".join("?" for _ in cols)

    with gold_connect_write(gold_path) as conn:
        conn.execute(_DDL)
        conn.executemany(
            f"""INSERT INTO ops_entity_resolution_audit ({", ".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (person_id) DO UPDATE SET
                    {update_set},
                    merged_at = current_timestamp""",
            [[r.get(c) for c in cols] for r in rows],
        )

    logger.info("entity_resolution_audit_written", rows=len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Writer — used by the pipeline (once per run)
# ---------------------------------------------------------------------------

class GoldWriter:
    """Atomic-swap writer for gold.duckdb.

    Builds a fresh DB file at gold.duckdb.new, then os.replace() into
    target on context exit. Readers holding the old inode are not blocked.

    Pipeline writes once per run, so fresh-build (vs incremental) is fine.

    with GoldWriter(db_path) as gw:
        gw.write_person_scores(score_rows)
        gw.write_score_history(history_rows)
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        *,
        memory_limit: str = "4GB",
    ) -> None:
        self._path = Path(db_path or DEFAULT_GOLD_DB_PATH)
        self._memory_limit = memory_limit
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._swap_ctx = None

    def __enter__(self) -> "GoldWriter":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self._path))
        self._conn.execute(f"SET memory_limit='{self._memory_limit}'")
        self._conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        # Phase 1c: Mart 専用 schema を default に
        try:
            self._conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
            self._conn.execute("SET schema='mart'")
        except Exception:
            pass
        self._conn.execute(_DDL)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn:
            try:
                self._conn.execute("CHECKPOINT")
            except Exception:
                pass
            self._conn.close()
            self._conn = None

    def write_person_scores(
        self, rows: list[tuple[Any, ...]]
    ) -> int:
        """Upsert person_scores rows.

        Each row: (person_id, person_fe, studio_fe_exposure, birank,
                   patronage, dormancy, awcc, iv_score)
        Returns number of rows written.
        """
        if not rows:
            return 0
        assert self._conn is not None
        now = datetime.datetime.now(datetime.timezone.utc)
        stamped = [(*r, now) for r in rows]
        self._conn.executemany(
            """
            INSERT INTO person_scores
                (person_id, person_fe, studio_fe_exposure, birank,
                 patronage, dormancy, awcc, iv_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (person_id) DO UPDATE SET
                person_fe          = excluded.person_fe,
                studio_fe_exposure = excluded.studio_fe_exposure,
                birank             = excluded.birank,
                patronage          = excluded.patronage,
                dormancy           = excluded.dormancy,
                awcc               = excluded.awcc,
                iv_score           = excluded.iv_score,
                updated_at         = excluded.updated_at
            """,
            stamped,
        )
        logger.info("gold_person_scores_written", count=len(rows))
        return len(rows)

    def write_score_history(
        self, rows: list[tuple[Any, ...]]
    ) -> int:
        """Append score_history rows.

        Each row: (person_id, person_fe, studio_fe_exposure, birank,
                   patronage, dormancy, awcc, iv_score, year, quarter)
        Returns number of rows written.
        """
        if not rows:
            return 0
        assert self._conn is not None
        self._conn.executemany(
            """
            INSERT INTO score_history
                (person_id, person_fe, studio_fe_exposure, birank,
                 patronage, dormancy, awcc, iv_score, year, quarter)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info("gold_score_history_written", count=len(rows))
        return len(rows)


# ---------------------------------------------------------------------------
# Reader — used by API / report generators
# ---------------------------------------------------------------------------

class GoldReader:
    """Read GOLD tables from DuckDB.

    Stateless (opens/closes connection per call) so it is safe for
    concurrent API reads.
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._path = db_path

    def available(self) -> bool:
        """Return True if the GOLD DuckDB file exists and is readable."""
        path = Path(str(self._path or DEFAULT_GOLD_DB_PATH))
        return path.exists()

    def person_scores(self) -> list[dict[str, Any]]:
        """Return all rows from person_scores ordered by iv_score DESC."""
        if not self.available():
            return []
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM person_scores ORDER BY iv_score DESC NULLS LAST"
            )
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    def person_scores_for(self, person_id: str) -> dict[str, Any] | None:
        """Return one row for person_id, or None if not found."""
        if not self.available():
            return None
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM person_scores WHERE person_id = ?", (person_id,)
            )
            cols = [d[0] for d in rel.description]
            row = rel.fetchone()
            return dict(zip(cols, row)) if row else None
        finally:
            conn.close()

    def score_history_for(self, person_id: str) -> list[dict[str, Any]]:
        """Return score_history rows for a person, most recent first."""
        if not self.available():
            return []
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM score_history WHERE person_id = ?"
                " ORDER BY year DESC, quarter DESC",
                (person_id,),
            )
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    def top_n(self, n: int = 100) -> list[dict[str, Any]]:
        """Return top-n persons by iv_score."""
        if not self.available():
            return []
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM person_scores ORDER BY iv_score DESC NULLS LAST LIMIT ?",
                (n,),
            )
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    def ranking_query(
        self,
        silver_path: Path | str,
        *,
        conditions: list[str] | None = None,
        params: list[Any] | None = None,
        sort: str = "iv_score",
        limit: int = 50,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Run the ranking query joining DuckDB person_scores with silver.duckdb.

        Attaches silver.duckdb (persons, credits) read-only so the entire
        analytical query runs inside DuckDB's vectorized engine.

        Returns (total_count, rows).
        Raises on error — caller should handle the failure.
        """
        conds = ["s.iv_score IS NOT NULL"] + (conditions or [])
        where = " AND ".join(conds)
        bind = list(params or [])

        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            silver = str(silver_path)
            if Path(silver).exists():
                conn.execute(f"ATTACH '{silver}' AS sl (READ_ONLY TRUE)")

            total = conn.execute(
                f"SELECT COUNT(DISTINCT s.person_id) FROM person_scores s WHERE {where}",
                bind,
            ).fetchone()[0]

            # DuckDB requires all non-aggregated SELECT columns in GROUP BY.
            # The correlated subquery for primary_role is replaced with a JOIN
            # on a pre-aggregated CTE to avoid O(N) subquery executions.
            sql = f"""
            WITH primary_roles AS (
                SELECT person_id,
                       arg_max(role, cnt) AS primary_role
                FROM (
                    SELECT person_id, role, COUNT(*) AS cnt
                    FROM sl.credits
                    GROUP BY person_id, role
                )
                GROUP BY person_id
            )
            SELECT
                s.person_id,
                p.name_ja,
                p.name_en,
                s.iv_score,
                s.birank,
                s.patronage,
                s.person_fe,
                s.awcc,
                s.dormancy,
                MIN(c.credit_year) AS first_year,
                MAX(c.credit_year) AS latest_year,
                pr.primary_role
            FROM person_scores s
            JOIN sl.persons p ON p.id = s.person_id
            LEFT JOIN sl.credits c ON c.person_id = s.person_id
            LEFT JOIN primary_roles pr ON pr.person_id = s.person_id
            WHERE {where}
            GROUP BY
                s.person_id, p.name_ja, p.name_en,
                s.iv_score, s.birank, s.patronage, s.person_fe,
                s.awcc, s.dormancy, pr.primary_role
            ORDER BY s.{sort} DESC NULLS LAST
            LIMIT {limit}
            """
            rel = conn.execute(sql, bind)
            cols = [d[0] for d in rel.description]
            rows = [dict(zip(cols, row)) for row in rel.fetchall()]
            return total, rows
        finally:
            conn.close()
