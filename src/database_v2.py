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


def init_db_v2(conn: sqlite3.Connection) -> None:
    """Target schema DDL — all naming decisions from _naming_decisions/ applied.

    No migration history. Use scripts/migrate_to_v2.py to copy data from legacy DB.
    """
    old_isolation = conn.isolation_level
    conn.isolation_level = None  # autocommit for executescript
    try:
        conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;

        -- ============================================================
        -- Canonical layer (Silver)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS persons (
            id               TEXT PRIMARY KEY,
            name_ja          TEXT NOT NULL DEFAULT '',
            name_en          TEXT NOT NULL DEFAULT '',
            aliases          TEXT NOT NULL DEFAULT '[]',
            mal_id           INTEGER,
            anilist_id       INTEGER,
            canonical_id     TEXT,
            date_of_birth    TEXT,
            blood_type       TEXT,
            description      TEXT,
            favourites       INTEGER,
            site_url         TEXT,
            image_medium     TEXT,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );
        CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id);

        CREATE TABLE IF NOT EXISTS anime (
            id                 TEXT PRIMARY KEY,
            title_ja           TEXT NOT NULL DEFAULT '',
            title_en           TEXT NOT NULL DEFAULT '',
            year               INTEGER,
            season             TEXT,
            quarter            INTEGER,
            episodes           INTEGER,
            format             TEXT,
            duration           INTEGER,
            start_date         TEXT CHECK (
                start_date IS NULL
                OR start_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            ),
            end_date           TEXT,
            status             TEXT,
            original_work_type TEXT,
            work_type          TEXT,
            scale_class        TEXT,
            updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_anime_year    ON anime(year);
        CREATE INDEX IF NOT EXISTS idx_anime_quarter ON anime(year, quarter);

        CREATE TABLE IF NOT EXISTS credits (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id       TEXT NOT NULL,
            anime_id        TEXT NOT NULL,
            role            TEXT NOT NULL,
            raw_role        TEXT NOT NULL DEFAULT '',
            episode         INTEGER,
            evidence_source TEXT NOT NULL DEFAULT '',
            credit_year     INTEGER,
            credit_quarter  INTEGER,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, raw_role, episode)
        );
        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime  ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role   ON credits(role);
        CREATE INDEX IF NOT EXISTS idx_credits_yq     ON credits(credit_year, credit_quarter);

        CREATE TABLE IF NOT EXISTS sources (
            code        TEXT PRIMARY KEY,
            name_ja     TEXT NOT NULL,
            base_url    TEXT NOT NULL,
            license     TEXT NOT NULL,
            added_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            retired_at  TIMESTAMP,
            description TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS roles (
            code           TEXT PRIMARY KEY,
            name_ja        TEXT NOT NULL,
            name_en        TEXT NOT NULL,
            role_group     TEXT NOT NULL CHECK (role_group IN
                ('director','animator','sound','production','writer',
                 'voice_actor','other')),
            weight_default REAL NOT NULL CHECK (weight_default >= 0),
            description_ja TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS anime_external_ids (
            anime_id    TEXT NOT NULL,
            source      TEXT NOT NULL REFERENCES sources(code),
            external_id TEXT NOT NULL,
            PRIMARY KEY (anime_id, source),
            UNIQUE (source, external_id)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_ext_ids_source
            ON anime_external_ids(source, external_id);

        CREATE TABLE IF NOT EXISTS person_external_ids (
            person_id   TEXT NOT NULL,
            source      TEXT NOT NULL REFERENCES sources(code),
            external_id TEXT NOT NULL,
            PRIMARY KEY (person_id, source),
            UNIQUE (source, external_id)
        );
        CREATE INDEX IF NOT EXISTS idx_person_ext_ids_source
            ON person_external_ids(source, external_id);

        CREATE TABLE IF NOT EXISTS person_aliases (
            person_id  TEXT NOT NULL,
            alias      TEXT NOT NULL,
            source     TEXT NOT NULL REFERENCES sources(code),
            confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
            added_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (person_id, alias, source)
        );
        CREATE INDEX IF NOT EXISTS idx_person_aliases_alias
            ON person_aliases(alias, person_id);

        CREATE TABLE IF NOT EXISTS anime_genres (
            anime_id   TEXT NOT NULL,
            genre_name TEXT NOT NULL,
            PRIMARY KEY (anime_id, genre_name)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_genres_genre
            ON anime_genres(genre_name, anime_id);

        CREATE TABLE IF NOT EXISTS anime_tags (
            anime_id TEXT NOT NULL,
            tag_name TEXT NOT NULL,
            rank     INTEGER CHECK (rank IS NULL OR rank BETWEEN 0 AND 100),
            PRIMARY KEY (anime_id, tag_name)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_tags_tag
            ON anime_tags(tag_name, rank, anime_id);

        CREATE TABLE IF NOT EXISTS studios (
            id                  TEXT PRIMARY KEY,
            name                TEXT NOT NULL DEFAULT '',
            anilist_id          INTEGER,
            is_animation_studio INTEGER,
            favourites          INTEGER,
            site_url            TEXT,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime_studios (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id  TEXT NOT NULL,
            studio_id TEXT NOT NULL,
            is_main   INTEGER NOT NULL DEFAULT 0,
            UNIQUE(anime_id, studio_id)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_studios_anime  ON anime_studios(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);

        CREATE TABLE IF NOT EXISTS anime_relations (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id         TEXT NOT NULL,
            related_anime_id TEXT NOT NULL,
            relation_type    TEXT NOT NULL DEFAULT '',
            related_title    TEXT NOT NULL DEFAULT '',
            related_format   TEXT,
            UNIQUE(anime_id, related_anime_id, relation_type)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_relations_anime
            ON anime_relations(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_relations_related
            ON anime_relations(related_anime_id);

        CREATE TABLE IF NOT EXISTS characters (
            id            TEXT PRIMARY KEY,
            name_ja       TEXT NOT NULL DEFAULT '',
            name_en       TEXT NOT NULL DEFAULT '',
            aliases       TEXT NOT NULL DEFAULT '[]',
            anilist_id    INTEGER,
            image_large   TEXT,
            image_medium  TEXT,
            description   TEXT,
            gender        TEXT,
            date_of_birth TEXT,
            age           TEXT,
            blood_type    TEXT,
            favourites    INTEGER,
            site_url      TEXT,
            updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS character_voice_actors (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            character_id   TEXT NOT NULL,
            person_id      TEXT NOT NULL,
            anime_id       TEXT NOT NULL,
            character_role TEXT NOT NULL DEFAULT '',
            source         TEXT NOT NULL DEFAULT '',
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(character_id, person_id, anime_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cva_character
            ON character_voice_actors(character_id);
        CREATE INDEX IF NOT EXISTS idx_cva_person
            ON character_voice_actors(person_id);
        CREATE INDEX IF NOT EXISTS idx_cva_anime
            ON character_voice_actors(anime_id);

        CREATE TABLE IF NOT EXISTS person_affiliations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id   TEXT NOT NULL,
            anime_id    TEXT NOT NULL,
            studio_name TEXT NOT NULL,
            source      TEXT NOT NULL DEFAULT '',
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, studio_name)
        );
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_person
            ON person_affiliations(person_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_anime
            ON person_affiliations(anime_id);

        -- ============================================================
        -- Score tables
        -- ============================================================

        CREATE TABLE IF NOT EXISTS person_scores (
            person_id          TEXT PRIMARY KEY,
            person_fe          REAL NOT NULL DEFAULT 0.0,
            studio_fe_exposure REAL NOT NULL DEFAULT 0.0,
            birank             REAL NOT NULL DEFAULT 0.0,
            patronage          REAL NOT NULL DEFAULT 0.0,
            dormancy           REAL NOT NULL DEFAULT 1.0,
            awcc               REAL NOT NULL DEFAULT 0.0,
            iv_score           REAL NOT NULL DEFAULT 0.0,
            career_track       TEXT,
            updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS voice_actor_scores (
            person_id                TEXT PRIMARY KEY,
            person_fe                REAL DEFAULT 0.0,
            sd_fe_exposure           REAL DEFAULT 0.0,
            birank                   REAL DEFAULT 0.0,
            patronage                REAL DEFAULT 0.0,
            trust                    REAL DEFAULT 0.0,
            dormancy                 REAL DEFAULT 1.0,
            awcc                     REAL DEFAULT 0.0,
            va_iv_score              REAL DEFAULT 0.0,
            character_diversity_index REAL DEFAULT 0.0,
            main_role_count          INTEGER DEFAULT 0,
            supporting_role_count    INTEGER DEFAULT 0,
            total_characters         INTEGER DEFAULT 0,
            casting_tier             TEXT DEFAULT 'newcomer',
            replacement_difficulty   REAL DEFAULT 0.0,
            updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS score_history (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id          TEXT NOT NULL,
            person_fe          REAL DEFAULT 0.0,
            studio_fe_exposure REAL DEFAULT 0.0,
            birank             REAL DEFAULT 0.0,
            patronage          REAL DEFAULT 0.0,
            dormancy           REAL DEFAULT 1.0,
            awcc               REAL DEFAULT 0.0,
            iv_score           REAL DEFAULT 0.0,
            year               INTEGER,
            quarter            INTEGER,
            run_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_score_history_person ON score_history(person_id);
        CREATE INDEX IF NOT EXISTS idx_score_history_run    ON score_history(run_at);

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            credit_count    INTEGER DEFAULT 0,
            person_count    INTEGER DEFAULT 0,
            elapsed_seconds REAL DEFAULT 0.0,
            mode            TEXT DEFAULT 'full'
        );

        -- ============================================================
        -- Ops layer (infra/audit — was meta_lineage/entity_res/quality)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS ops_source_scrape_status (
            source          TEXT PRIMARY KEY,
            last_scraped_at TIMESTAMP,
            item_count      INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'ok'
        );

        CREATE TABLE IF NOT EXISTS ops_lineage (
            table_name             TEXT PRIMARY KEY,
            audience               TEXT NOT NULL,
            source_silver_tables   TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed  INTEGER NOT NULL DEFAULT 0,
            description            TEXT NOT NULL DEFAULT '',
            formula_version        TEXT NOT NULL,
            computed_at            TIMESTAMP NOT NULL,
            ci_method              TEXT,
            null_model             TEXT,
            holdout_method         TEXT,
            row_count              INTEGER,
            notes                  TEXT,
            rng_seed               INTEGER,
            git_sha                TEXT NOT NULL DEFAULT '',
            inputs_hash            TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS ops_quality_snapshot (
            computed_at TEXT NOT NULL,
            table_name  TEXT NOT NULL,
            metric      TEXT NOT NULL,
            value       REAL NOT NULL,
            PRIMARY KEY (computed_at, table_name, metric)
        );
        CREATE INDEX IF NOT EXISTS idx_ops_quality_metric
            ON ops_quality_snapshot(table_name, metric, computed_at);

        CREATE TABLE IF NOT EXISTS ops_entity_resolution_audit (
            person_id        TEXT PRIMARY KEY,
            canonical_name   TEXT NOT NULL,
            merge_method     TEXT NOT NULL CHECK (merge_method IN
                ('exact_match','cross_source','romaji','similarity','ai_assisted','manual')),
            merge_confidence REAL NOT NULL CHECK (merge_confidence BETWEEN 0 AND 1),
            merged_from_keys TEXT NOT NULL,
            merge_evidence   TEXT NOT NULL,
            merged_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reviewed_by      TEXT,
            reviewed_at      TIMESTAMP,
            FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_ops_era_method
            ON ops_entity_resolution_audit(merge_method, merge_confidence);

        CREATE TABLE IF NOT EXISTS calc_execution_records (
            scope       TEXT NOT NULL,
            calc_name   TEXT NOT NULL,
            input_hash  TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'success',
            output_path TEXT NOT NULL DEFAULT '',
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (scope, calc_name)
        );
        CREATE INDEX IF NOT EXISTS idx_calc_exec_scope_hash
            ON calc_execution_records(scope, input_hash);

        CREATE TABLE IF NOT EXISTS schema_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        -- ============================================================
        -- Report layer (meta_* = audience-specific outputs)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS meta_common_person_parameters (
            person_id                    TEXT PRIMARY KEY,
            scale_reach_pct              REAL,
            scale_reach_ci_low           REAL,
            scale_reach_ci_high          REAL,
            collab_width_pct             REAL,
            collab_width_ci_low          REAL,
            collab_width_ci_high         REAL,
            continuity_pct               REAL,
            continuity_ci_low            REAL,
            continuity_ci_high           REAL,
            mentor_contribution_pct      REAL,
            mentor_contribution_ci_low   REAL,
            mentor_contribution_ci_high  REAL,
            centrality_pct               REAL,
            centrality_ci_low            REAL,
            centrality_ci_high           REAL,
            trust_accum_pct              REAL,
            trust_accum_ci_low           REAL,
            trust_accum_ci_high          REAL,
            role_evolution_pct           REAL,
            role_evolution_ci_low        REAL,
            role_evolution_ci_high       REAL,
            genre_specialization_pct     REAL,
            genre_specialization_ci_low  REAL,
            genre_specialization_ci_high REAL,
            recent_activity_pct          REAL,
            recent_activity_ci_low       REAL,
            recent_activity_ci_high      REAL,
            compatibility_pct            REAL,
            compatibility_ci_low         REAL,
            compatibility_ci_high        REAL,
            archetype                    TEXT,
            archetype_confidence         REAL,
            computed_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS meta_policy_attrition (
            cohort_year  INTEGER NOT NULL,
            treatment    TEXT NOT NULL,
            ate          REAL,
            ate_ci_low   REAL,
            ate_ci_high  REAL,
            hazard_ratio REAL,
            hr_ci_low    REAL,
            hr_ci_high   REAL,
            n_treated    INTEGER,
            n_control    INTEGER,
            p_value      REAL,
            computed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cohort_year, treatment)
        );

        CREATE TABLE IF NOT EXISTS meta_policy_monopsony (
            year              INTEGER NOT NULL,
            studio            TEXT NOT NULL,
            hhi               REAL,
            hhi_star          REAL,
            hhi_ci_low        REAL,
            hhi_ci_high       REAL,
            logit_stay_beta   REAL,
            logit_stay_se     REAL,
            n_persons         INTEGER,
            computed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, studio)
        );

        CREATE TABLE IF NOT EXISTS meta_policy_gender (
            transition_stage TEXT NOT NULL,
            cohort           TEXT NOT NULL,
            survival_prob    REAL,
            survival_ci_low  REAL,
            survival_ci_high REAL,
            log_rank_chi2    REAL,
            log_rank_p       REAL,
            n_female         INTEGER,
            n_male           INTEGER,
            computed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (transition_stage, cohort)
        );

        CREATE TABLE IF NOT EXISTS meta_policy_generation (
            cohort           TEXT NOT NULL,
            career_year_bin  INTEGER NOT NULL,
            survival_rate    REAL,
            survival_ci_low  REAL,
            survival_ci_high REAL,
            n_at_risk        INTEGER,
            n_events         INTEGER,
            computed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cohort, career_year_bin)
        );

        CREATE TABLE IF NOT EXISTS meta_hr_studio_benchmark (
            studio          TEXT NOT NULL,
            year            INTEGER NOT NULL,
            r5_retention    REAL,
            r5_ci_low       REAL,
            r5_ci_high      REAL,
            value_added     REAL,
            va_ci_low       REAL,
            va_ci_high      REAL,
            h_score         REAL,
            attraction_rate REAL,
            n_persons       INTEGER,
            computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (studio, year)
        );

        CREATE TABLE IF NOT EXISTS meta_hr_mentor_card (
            director_id          TEXT PRIMARY KEY,
            mentor_score         REAL,
            mentor_ci_low        REAL,
            mentor_ci_high       REAL,
            null_permutation_p   REAL,
            n_mentees            INTEGER,
            n_works              INTEGER,
            archetype            TEXT,
            computed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS meta_hr_attrition_risk (
            person_id      TEXT PRIMARY KEY,
            predicted_risk REAL,
            risk_ci_low    REAL,
            risk_ci_high   REAL,
            c_index        REAL,
            shap_feature1  TEXT, shap_value1 REAL,
            shap_feature2  TEXT, shap_value2 REAL,
            shap_feature3  TEXT, shap_value3 REAL,
            shap_feature4  TEXT, shap_value4 REAL,
            shap_feature5  TEXT, shap_value5 REAL,
            computed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS meta_hr_succession (
            veteran_id      TEXT NOT NULL,
            candidate_id    TEXT NOT NULL,
            successor_score REAL,
            role            TEXT,
            overlap_works   INTEGER,
            career_gap_years REAL,
            computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (veteran_id, candidate_id)
        );

        CREATE TABLE IF NOT EXISTS meta_biz_whitespace (
            genre            TEXT NOT NULL,
            year             INTEGER NOT NULL,
            cagr             REAL,
            cagr_ci_low      REAL,
            cagr_ci_high     REAL,
            penetration      REAL,
            whitespace_score REAL,
            n_anime          INTEGER,
            n_staff          INTEGER,
            computed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (genre, year)
        );

        CREATE TABLE IF NOT EXISTS meta_biz_undervalued (
            person_id            TEXT PRIMARY KEY,
            undervaluation_score REAL,
            archetype            TEXT,
            network_reach        REAL,
            opportunity_residual REAL,
            career_band          TEXT,
            computed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS meta_biz_trust_entry (
            gatekeeper_id    TEXT PRIMARY KEY,
            gatekeeper_score REAL,
            reach_score      REAL,
            n_new_entrants   INTEGER,
            avg_entry_speed  REAL,
            community_diversity REAL,
            computed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS meta_biz_team_template (
            cluster_id          TEXT NOT NULL,
            tier                TEXT NOT NULL,
            role_distribution   TEXT,
            avg_career_years    REAL,
            silhouette_score    REAL,
            n_teams             INTEGER,
            representative_works TEXT,
            computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cluster_id, tier)
        );

        CREATE TABLE IF NOT EXISTS meta_biz_independent_unit (
            community_id    TEXT PRIMARY KEY,
            coverage        REAL,
            density         REAL,
            value_generated REAL,
            n_members       INTEGER,
            n_works         INTEGER,
            core_studio     TEXT,
            computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ============================================================
        -- Feature layer (feat_*)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS feat_person_scores (
            person_id          TEXT PRIMARY KEY,
            run_id             INTEGER REFERENCES pipeline_runs(id),
            person_fe          REAL,
            person_fe_se       REAL,
            person_fe_n_obs    INTEGER,
            studio_fe_exposure REAL,
            birank             REAL,
            patronage          REAL,
            awcc               REAL,
            dormancy           REAL,
            ndi                REAL,
            career_friction    REAL,
            peer_boost         REAL,
            iv_score           REAL,
            iv_score_pct       REAL,
            person_fe_pct      REAL,
            birank_pct         REAL,
            patronage_pct      REAL,
            awcc_pct           REAL,
            dormancy_pct       REAL,
            confidence         REAL,
            score_range_low    REAL,
            score_range_high   REAL,
            updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_person_scores_iv
            ON feat_person_scores(iv_score);

        CREATE TABLE IF NOT EXISTS feat_network (
            person_id               TEXT PRIMARY KEY,
            run_id                  INTEGER REFERENCES pipeline_runs(id),
            degree_centrality       REAL,
            betweenness_centrality  REAL,
            closeness_centrality    REAL,
            eigenvector_centrality  REAL,
            hub_score               REAL,
            n_collaborators         INTEGER,
            n_unique_anime          INTEGER,
            bridge_score            REAL,
            n_bridge_communities    INTEGER,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_career (
            person_id      TEXT PRIMARY KEY,
            run_id         INTEGER REFERENCES pipeline_runs(id),
            first_year     INTEGER,
            latest_year    INTEGER,
            active_years   INTEGER,
            total_credits  INTEGER,
            highest_stage  INTEGER,
            primary_role   TEXT,
            career_track   TEXT,
            peak_year      INTEGER,
            peak_credits   INTEGER,
            growth_trend   TEXT,
            growth_score   REAL,
            activity_ratio REAL,
            recent_credits INTEGER,
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_career_first_year
            ON feat_career(first_year);
        CREATE INDEX IF NOT EXISTS idx_feat_career_track
            ON feat_career(career_track);

        CREATE TABLE IF NOT EXISTS feat_genre_affinity (
            person_id     TEXT NOT NULL,
            genre         TEXT NOT NULL,
            run_id        INTEGER REFERENCES pipeline_runs(id),
            affinity_score REAL,
            work_count    INTEGER,
            PRIMARY KEY (person_id, genre)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_genre_genre
            ON feat_genre_affinity(genre);

        CREATE TABLE IF NOT EXISTS feat_contribution (
            person_id            TEXT PRIMARY KEY,
            run_id               INTEGER REFERENCES pipeline_runs(id),
            peer_percentile      REAL,
            opportunity_residual REAL,
            consistency          REAL,
            independent_value    REAL,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_credit_activity (
            person_id                  TEXT PRIMARY KEY,
            first_abs_quarter          INTEGER,
            last_abs_quarter           INTEGER,
            activity_span_quarters     INTEGER,
            active_quarters            INTEGER,
            density                    REAL,
            n_gaps                     INTEGER,
            mean_gap_quarters          REAL,
            median_gap_quarters        REAL,
            min_gap_quarters           INTEGER,
            max_gap_quarters           INTEGER,
            std_gap_quarters           REAL,
            consecutive_quarters       INTEGER,
            consecutive_rate           REAL,
            n_hiatuses                 INTEGER,
            longest_hiatus_quarters    INTEGER,
            quarters_since_last_credit INTEGER,
            active_years               INTEGER,
            n_year_gaps                INTEGER,
            mean_year_gap              REAL,
            max_year_gap               INTEGER,
            updated_at                 TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_credit_activity_span
            ON feat_credit_activity(activity_span_quarters);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_activity_last
            ON feat_credit_activity(last_abs_quarter);

        CREATE TABLE IF NOT EXISTS feat_career_annual (
            person_id                    TEXT NOT NULL,
            career_year                  INTEGER NOT NULL,
            credit_year                  INTEGER NOT NULL,
            n_works                      INTEGER NOT NULL DEFAULT 0,
            n_credits                    INTEGER NOT NULL DEFAULT 0,
            n_roles                      INTEGER NOT NULL DEFAULT 0,
            works_direction              INTEGER NOT NULL DEFAULT 0,
            works_animation_supervision  INTEGER NOT NULL DEFAULT 0,
            works_animation              INTEGER NOT NULL DEFAULT 0,
            works_design                 INTEGER NOT NULL DEFAULT 0,
            works_technical              INTEGER NOT NULL DEFAULT 0,
            works_art                    INTEGER NOT NULL DEFAULT 0,
            works_sound                  INTEGER NOT NULL DEFAULT 0,
            works_writing                INTEGER NOT NULL DEFAULT 0,
            works_production             INTEGER NOT NULL DEFAULT 0,
            works_production_management  INTEGER NOT NULL DEFAULT 0,
            works_finishing              INTEGER NOT NULL DEFAULT 0,
            works_editing                INTEGER NOT NULL DEFAULT 0,
            works_settings               INTEGER NOT NULL DEFAULT 0,
            works_other                  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (person_id, career_year)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_career_annual_year
            ON feat_career_annual(career_year);
        CREATE INDEX IF NOT EXISTS idx_feat_career_annual_credit_year
            ON feat_career_annual(credit_year);

        CREATE TABLE IF NOT EXISTS feat_birank_annual (
            person_id            TEXT NOT NULL,
            year                 INTEGER NOT NULL,
            birank               REAL NOT NULL,
            raw_pagerank         REAL,
            graph_size           INTEGER,
            n_credits_cumulative INTEGER,
            PRIMARY KEY (person_id, year)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_birank_annual_year
            ON feat_birank_annual(year);

        CREATE TABLE IF NOT EXISTS birank_compute_state (
            year         INTEGER PRIMARY KEY,
            credit_count INTEGER NOT NULL,
            anime_count  INTEGER NOT NULL,
            person_count INTEGER NOT NULL,
            computed_at  REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS feat_studio_affiliation (
            person_id      TEXT NOT NULL,
            credit_year    INTEGER NOT NULL,
            studio_id      TEXT NOT NULL,
            studio_name    TEXT NOT NULL DEFAULT '',
            n_works        INTEGER NOT NULL DEFAULT 0,
            n_credits      INTEGER NOT NULL DEFAULT 0,
            is_main_studio INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (person_id, credit_year, studio_id)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_person
            ON feat_studio_affiliation(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_studio
            ON feat_studio_affiliation(studio_id, credit_year);

        CREATE TABLE IF NOT EXISTS feat_credit_contribution (
            person_id           TEXT NOT NULL,
            anime_id            TEXT NOT NULL,
            role                TEXT NOT NULL,
            credit_year         INTEGER,
            production_scale    REAL,
            role_weight         REAL,
            episode_coverage    REAL,
            dur_mult            REAL,
            edge_weight         REAL,
            edge_weight_share   REAL,
            iv_contrib_est      REAL,
            debut_year          INTEGER,
            career_year_at_credit INTEGER,
            is_debut_work       INTEGER,
            PRIMARY KEY (person_id, anime_id, role)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_anime
            ON feat_credit_contribution(anime_id);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_year
            ON feat_credit_contribution(credit_year);

        CREATE TABLE IF NOT EXISTS feat_person_work_summary (
            person_id                TEXT PRIMARY KEY,
            n_distinct_works         INTEGER,
            total_production_scale   REAL,
            mean_production_scale    REAL,
            max_production_scale     REAL,
            best_work_anime_id       TEXT,
            total_edge_weight        REAL,
            mean_edge_weight_per_work REAL,
            max_edge_weight          REAL,
            top_contrib_anime_id     TEXT,
            total_iv_contrib_est     REAL,
            updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_work_context (
            anime_id                 TEXT PRIMARY KEY,
            credit_year              INTEGER,
            n_staff                  INTEGER,
            n_distinct_roles         INTEGER,
            n_direction              INTEGER,
            n_animation_supervision  INTEGER,
            n_animation              INTEGER,
            n_design                 INTEGER,
            n_technical              INTEGER,
            n_art                    INTEGER,
            n_sound                  INTEGER,
            n_writing                INTEGER,
            n_production             INTEGER,
            n_other                  INTEGER,
            mean_career_year         REAL,
            median_career_year       REAL,
            max_career_year          INTEGER,
            production_scale         REAL,
            difficulty_score         REAL,
            scale_tier               INTEGER,
            scale_label              TEXT,
            scale_raw                REAL,
            format_group             TEXT,
            updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_work_context_year
            ON feat_work_context(credit_year);

        CREATE TABLE IF NOT EXISTS feat_person_role_progression (
            person_id         TEXT NOT NULL,
            role_category     TEXT NOT NULL,
            first_year        INTEGER,
            last_year         INTEGER,
            peak_year         INTEGER,
            n_works           INTEGER,
            n_credits         INTEGER,
            career_year_first INTEGER,
            still_active      INTEGER,
            PRIMARY KEY (person_id, role_category)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_role_prog_person
            ON feat_person_role_progression(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_role_prog_category
            ON feat_person_role_progression(role_category);

        CREATE TABLE IF NOT EXISTS feat_causal_estimates (
            person_id            TEXT PRIMARY KEY,
            peer_effect_boost    REAL,
            career_friction      REAL,
            era_fe               REAL,
            era_deflated_iv      REAL,
            opportunity_residual REAL,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_cluster_membership (
            person_id            TEXT PRIMARY KEY,
            community_id         INTEGER,
            career_track         TEXT,
            growth_trend         TEXT,
            studio_cluster_id    INTEGER,
            studio_cluster_name  TEXT,
            cooccurrence_group_id INTEGER,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_community
            ON feat_cluster_membership(community_id);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_career_track
            ON feat_cluster_membership(career_track);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_growth
            ON feat_cluster_membership(growth_trend);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_studio
            ON feat_cluster_membership(studio_cluster_id);

        CREATE TABLE IF NOT EXISTS feat_mentorships (
            mentor_id      TEXT NOT NULL,
            mentee_id      TEXT NOT NULL,
            n_shared_works INTEGER DEFAULT 0,
            hit_rate       REAL,
            mentor_stage   INTEGER,
            mentee_stage   INTEGER,
            first_year     INTEGER,
            latest_year    INTEGER,
            PRIMARY KEY (mentor_id, mentee_id)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_mentorships_mentor
            ON feat_mentorships(mentor_id);
        CREATE INDEX IF NOT EXISTS idx_feat_mentorships_mentee
            ON feat_mentorships(mentee_id);

        CREATE TABLE IF NOT EXISTS feat_career_gaps (
            person_id      TEXT NOT NULL,
            gap_start_year INTEGER NOT NULL,
            gap_end_year   INTEGER,
            gap_length     INTEGER NOT NULL,
            returned       INTEGER NOT NULL DEFAULT 0,
            gap_type       TEXT NOT NULL,
            PRIMARY KEY (person_id, gap_start_year)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_career_gaps_type
            ON feat_career_gaps(gap_type);
        CREATE INDEX IF NOT EXISTS idx_feat_career_gaps_returned
            ON feat_career_gaps(returned);

        -- ============================================================
        -- Aggregation layer (agg_*)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS agg_milestones (
            person_id   TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            year        INTEGER NOT NULL DEFAULT 0,
            anime_id    TEXT NOT NULL DEFAULT '',
            anime_title TEXT,
            description TEXT,
            PRIMARY KEY (person_id, event_type, year, anime_id)
        );
        CREATE INDEX IF NOT EXISTS idx_agg_milestones_person
            ON agg_milestones(person_id);
        CREATE INDEX IF NOT EXISTS idx_agg_milestones_year
            ON agg_milestones(year);

        CREATE TABLE IF NOT EXISTS agg_director_circles (
            person_id    TEXT NOT NULL,
            director_id  TEXT NOT NULL,
            shared_works INTEGER DEFAULT 0,
            hit_rate     REAL,
            roles        TEXT DEFAULT '[]',
            latest_year  INTEGER,
            PRIMARY KEY (person_id, director_id)
        );
        CREATE INDEX IF NOT EXISTS idx_agg_dir_circles_person
            ON agg_director_circles(person_id);
        CREATE INDEX IF NOT EXISTS idx_agg_dir_circles_director
            ON agg_director_circles(director_id);

        -- ============================================================
        -- Source layer (src_* = Bronze)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS src_anilist_anime (
            anilist_id   INTEGER PRIMARY KEY,
            title_ja     TEXT NOT NULL DEFAULT '',
            title_en     TEXT NOT NULL DEFAULT '',
            year         INTEGER,
            season       TEXT,
            episodes     INTEGER,
            format       TEXT,
            status       TEXT,
            start_date   TEXT,
            end_date     TEXT,
            duration     INTEGER,
            source       TEXT,
            description  TEXT,
            score        REAL,
            genres       TEXT DEFAULT '[]',
            tags         TEXT DEFAULT '[]',
            studios      TEXT DEFAULT '[]',
            synonyms     TEXT DEFAULT '[]',
            cover_large  TEXT,
            cover_medium TEXT,
            banner       TEXT,
            popularity   INTEGER,
            favourites   INTEGER,
            site_url     TEXT,
            mal_id       INTEGER,
            scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_anilist_persons (
            anilist_id    INTEGER PRIMARY KEY,
            name_ja       TEXT NOT NULL DEFAULT '',
            name_en       TEXT NOT NULL DEFAULT '',
            aliases       TEXT DEFAULT '[]',
            date_of_birth TEXT,
            age           INTEGER,
            gender        TEXT,
            years_active  TEXT DEFAULT '[]',
            hometown      TEXT,
            blood_type    TEXT,
            description   TEXT,
            image_large   TEXT,
            image_medium  TEXT,
            favourites    INTEGER,
            site_url      TEXT,
            scraped_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_anilist_credits (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            anilist_anime_id  INTEGER NOT NULL,
            anilist_person_id INTEGER NOT NULL,
            role             TEXT NOT NULL,
            role_raw         TEXT,
            scraped_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_anime_id, anilist_person_id, role)
        );
        CREATE INDEX IF NOT EXISTS idx_src_anilist_credits_anime
            ON src_anilist_credits(anilist_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_anilist_credits_person
            ON src_anilist_credits(anilist_person_id);

        CREATE TABLE IF NOT EXISTS src_ann_anime (
            ann_id      INTEGER PRIMARY KEY,
            title_en    TEXT NOT NULL DEFAULT '',
            title_ja    TEXT NOT NULL DEFAULT '',
            year        INTEGER,
            episodes    INTEGER,
            format      TEXT,
            genres      TEXT DEFAULT '[]',
            start_date  TEXT,
            end_date    TEXT,
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_ann_persons (
            ann_id        INTEGER PRIMARY KEY,
            name_en       TEXT NOT NULL DEFAULT '',
            name_ja       TEXT NOT NULL DEFAULT '',
            date_of_birth TEXT,
            hometown      TEXT,
            blood_type    TEXT,
            website       TEXT,
            description   TEXT,
            scraped_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_ann_credits (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ann_anime_id  INTEGER NOT NULL,
            ann_person_id INTEGER NOT NULL,
            name_en      TEXT NOT NULL DEFAULT '',
            role         TEXT NOT NULL,
            role_raw     TEXT,
            scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ann_anime_id, ann_person_id, role)
        );
        CREATE INDEX IF NOT EXISTS idx_src_ann_credits_anime
            ON src_ann_credits(ann_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_ann_credits_person
            ON src_ann_credits(ann_person_id);

        CREATE TABLE IF NOT EXISTS src_allcinema_anime (
            allcinema_id INTEGER PRIMARY KEY,
            title_ja     TEXT NOT NULL DEFAULT '',
            year         INTEGER,
            start_date   TEXT,
            synopsis     TEXT,
            scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_allcinema_persons (
            allcinema_id INTEGER PRIMARY KEY,
            name_ja      TEXT NOT NULL DEFAULT '',
            yomigana     TEXT NOT NULL DEFAULT '',
            name_en      TEXT NOT NULL DEFAULT '',
            scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_allcinema_credits (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            allcinema_anime_id   INTEGER NOT NULL,
            allcinema_person_id  INTEGER NOT NULL,
            name_ja              TEXT NOT NULL DEFAULT '',
            name_en              TEXT NOT NULL DEFAULT '',
            job_name             TEXT NOT NULL,
            job_id               INTEGER,
            scraped_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(allcinema_anime_id, allcinema_person_id, job_name)
        );
        CREATE INDEX IF NOT EXISTS idx_src_allcinema_credits_anime
            ON src_allcinema_credits(allcinema_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_allcinema_credits_person
            ON src_allcinema_credits(allcinema_person_id);

        CREATE TABLE IF NOT EXISTS src_seesaawiki_anime (
            id         TEXT PRIMARY KEY,
            title_ja   TEXT NOT NULL DEFAULT '',
            year       INTEGER,
            episodes   INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_seesaawiki_credits (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_src_id TEXT NOT NULL,
            person_name  TEXT NOT NULL,
            role         TEXT NOT NULL,
            role_raw     TEXT,
            episode      INTEGER,
            affiliation  TEXT,
            is_company   INTEGER DEFAULT 0,
            scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anime_src_id, person_name, role, episode)
        );
        CREATE INDEX IF NOT EXISTS idx_src_seesaawiki_credits_anime
            ON src_seesaawiki_credits(anime_src_id);

        CREATE TABLE IF NOT EXISTS src_keyframe_anime (
            slug       TEXT PRIMARY KEY,
            title_ja   TEXT NOT NULL DEFAULT '',
            title_en   TEXT NOT NULL DEFAULT '',
            anilist_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_keyframe_credits (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            keyframe_slug   TEXT NOT NULL,
            kf_person_id    INTEGER NOT NULL,
            name_ja         TEXT NOT NULL DEFAULT '',
            name_en         TEXT NOT NULL DEFAULT '',
            role_ja         TEXT NOT NULL,
            role_en         TEXT NOT NULL DEFAULT '',
            episode         INTEGER,
            scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(keyframe_slug, kf_person_id, role_ja, episode)
        );
        CREATE INDEX IF NOT EXISTS idx_src_keyframe_credits_slug
            ON src_keyframe_credits(keyframe_slug);
        """)
    finally:
        conn.isolation_level = old_isolation
        conn.execute("PRAGMA journal_mode=WAL")

    # Seed lookup tables and set schema version
    _seed_sources(conn)
    _seed_roles(conn)
    conn.execute(
        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', '55')"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value"
    )
    conn.commit()
    log.info("init_db_v2_complete")


def _seed_sources(conn: sqlite3.Connection) -> None:
    """Seed the sources lookup table with canonical data sources."""
    SOURCE_SEEDS = [
        ("anilist",    "AniList",             "https://anilist.co",                "proprietary", "GraphQL で structured staff 情報が最も豊富"),
        ("mal",        "MyAnimeList",          "https://myanimelist.net",           "proprietary", "viewer ratings の参照源 (表示のみ、分析不使用)"),
        ("ann",        "Anime News Network",   "https://www.animenewsnetwork.com",  "proprietary", "historical depth と職種粒度"),
        ("allcinema",  "allcinema",            "https://www.allcinema.net",         "proprietary", "邦画・OVA の網羅性"),
        ("seesaawiki", "SeesaaWiki",           "https://seesaawiki.jp",             "CC-BY-SA",    "fan-curated 詳細エピソード情報"),
        ("keyframe",   "Sakugabooru/Keyframe", "https://www.sakugabooru.com",       "CC",          "sakuga コミュニティ別名情報"),
        ("madb",       "メディア芸術DB",         "https://mediaarts-db.bunka.go.jp",  "public",      "文化庁 メディア芸術データベース (日本政府公開)"),
    ]
    for code, name_ja, base_url, license_, desc in SOURCE_SEEDS:
        conn.execute(
            "INSERT OR IGNORE INTO sources (code, name_ja, base_url, license, description)"
            " VALUES (?, ?, ?, ?, ?)",
            (code, name_ja, base_url, license_, desc),
        )


def _seed_roles(conn: sqlite3.Connection) -> None:
    """Seed the roles lookup table with 24 standardized roles."""
    ROLE_SEEDS = [
        ("director",            "監督",               "Director",            "director",    2.0, "作品全体の演出・制作統括"),
        ("episode_director",    "演出",               "Episode Director",    "director",    1.6, "個別エピソードの演出"),
        ("animation_director",  "作画監督",            "Animation Director",  "animator",    1.8, "作画品質の統括"),
        ("key_animator",        "原画",               "Key Animator",        "animator",    1.5, "アニメーションの原画担当"),
        ("second_key_animator", "第二原画",            "Second Key Animator", "animator",    1.2, "原画の補佐担当"),
        ("in_between",          "動画",               "In-Between",          "animator",    0.8, "中割アニメーション担当"),
        ("character_designer",  "キャラクターデザイン", "Character Designer",  "animator",    1.7, "キャラクターの視覚デザイン"),
        ("layout",              "レイアウト",           "Layout",              "animator",    1.2, "画面構成・レイアウト担当"),
        ("settings",            "設定",               "Settings",            "animator",    1.0, "世界観・設定デザイン担当"),
        ("photography_director","撮影監督",             "Photography Director","production",  1.3, "撮影・合成の統括"),
        ("cgi_director",        "CGI監督",             "CGI Director",        "production",  1.3, "CG制作の統括"),
        ("background_art",      "背景美術",             "Background Art",      "production",  1.1, "背景・美術担当"),
        ("finishing",           "仕上げ",              "Finishing/Color",     "production",  1.0, "仕上げ・色彩設計"),
        ("editing",             "編集",               "Editing",             "production",  1.1, "映像編集担当"),
        ("producer",            "プロデューサー",        "Producer",            "production",  1.5, "制作プロデュース統括"),
        ("production_manager",  "制作進行",             "Production Manager",  "production",  0.9, "制作スケジュール管理"),
        ("sound_director",      "音響監督",             "Sound Director",      "sound",       1.4, "音響・SE・アフレコの統括"),
        ("music",               "音楽",               "Music",               "sound",       1.2, "劇伴・音楽担当"),
        ("screenplay",          "脚本",               "Screenplay",          "writer",      1.6, "脚本・シナリオ担当"),
        ("original_creator",    "原作者",              "Original Creator",    "writer",      1.5, "原作（漫画・小説等）の作者"),
        ("voice_actor",         "声優",               "Voice Actor",         "voice_actor", 1.0, "キャラクターの声優"),
        ("localization",        "ローカライズ",          "Localization",        "other",       0.8, "翻訳・ローカライズ担当"),
        ("other",               "その他",              "Other",               "other",       0.5, "上記以外の担当"),
        ("special",             "スペシャル",           "Special",             "other",       0.5, "特別クレジット"),
    ]
    for code, name_ja, name_en, role_group, weight, desc_ja in ROLE_SEEDS:
        conn.execute(
            "INSERT OR IGNORE INTO roles"
            " (code, name_ja, name_en, role_group, weight_default, description_ja)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (code, name_ja, name_en, role_group, weight, desc_ja),
        )


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
