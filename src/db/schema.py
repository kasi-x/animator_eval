"""BRONZE layer schema initialization.

Consolidates all schema DDL and initialization functions for BRONZE layer.
BRONZE tables store raw scraper data (legacy SQLite, now parquet).
SILVER/GOLD tables are in DuckDB.
"""

import sqlite3

import structlog

log = structlog.get_logger(__name__)


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
            name_ko          TEXT NOT NULL DEFAULT '',
            name_zh          TEXT NOT NULL DEFAULT '',
            names_alt        TEXT NOT NULL DEFAULT '{}',
            aliases          TEXT NOT NULL DEFAULT '[]',
            nationality      TEXT NOT NULL DEFAULT '[]',
            mal_id           INTEGER,
            anilist_id       INTEGER,
            canonical_id     TEXT,
            date_of_birth    TEXT,
            hometown         TEXT,
            blood_type       TEXT,
            description      TEXT,
            gender           TEXT,
            years_active     TEXT NOT NULL DEFAULT '[]',
            favourites       INTEGER,
            site_url         TEXT,
            image_medium     TEXT,
            name_priority    INTEGER NOT NULL DEFAULT 0,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );
        CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id);

        CREATE TABLE IF NOT EXISTS anime (
            id                 TEXT PRIMARY KEY,
            title_ja           TEXT NOT NULL DEFAULT '',
            title_en           TEXT NOT NULL DEFAULT '',
            titles_alt         TEXT NOT NULL DEFAULT '{}',
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
            country_of_origin  TEXT,
            synonyms           TEXT NOT NULL DEFAULT '[]',
            is_adult           INTEGER,
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
            affiliation     TEXT,
            position        INTEGER,
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
            lang       TEXT,
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
            country_of_origin   TEXT,
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
            anilist_id        INTEGER PRIMARY KEY,
            title_ja          TEXT NOT NULL DEFAULT '',
            title_en          TEXT NOT NULL DEFAULT '',
            year              INTEGER,
            season            TEXT,
            episodes          INTEGER,
            format            TEXT,
            status            TEXT,
            start_date        TEXT,
            end_date          TEXT,
            duration          INTEGER,
            source            TEXT,
            description       TEXT,
            score             REAL,
            genres            TEXT DEFAULT '[]',
            tags              TEXT DEFAULT '[]',
            studios           TEXT DEFAULT '[]',
            synonyms          TEXT DEFAULT '[]',
            cover_large       TEXT,
            cover_medium      TEXT,
            banner            TEXT,
            popularity        INTEGER,
            favourites        INTEGER,
            site_url          TEXT,
            mal_id            INTEGER,
            country_of_origin TEXT,
            is_licensed       INTEGER,
            is_adult          INTEGER,
            mean_score        INTEGER,
            relations_json    TEXT,
            scraped_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_anilist_persons (
            anilist_id    INTEGER PRIMARY KEY,
            name_ja       TEXT NOT NULL DEFAULT '',
            name_en       TEXT NOT NULL DEFAULT '',
            name_ko       TEXT NOT NULL DEFAULT '',
            name_zh       TEXT NOT NULL DEFAULT '',
            names_alt     TEXT NOT NULL DEFAULT '{}',
            aliases       TEXT DEFAULT '[]',
            nationality   TEXT NOT NULL DEFAULT '[]',
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
            name_ko       TEXT NOT NULL DEFAULT '',
            name_zh       TEXT NOT NULL DEFAULT '',
            names_alt     TEXT NOT NULL DEFAULT '{}',
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

        CREATE TABLE IF NOT EXISTS src_tmdb_anime (
            tmdb_id              INTEGER NOT NULL,
            media_type           TEXT NOT NULL,
            title                TEXT NOT NULL DEFAULT '',
            original_title       TEXT NOT NULL DEFAULT '',
            original_lang        TEXT,
            origin_countries     TEXT NOT NULL DEFAULT '[]',
            year                 INTEGER,
            first_air_date       TEXT,
            last_air_date        TEXT,
            release_date         TEXT,
            episodes             INTEGER,
            seasons              INTEGER,
            runtime              INTEGER,
            status               TEXT,
            genres               TEXT NOT NULL DEFAULT '[]',
            production_companies TEXT NOT NULL DEFAULT '[]',
            overview             TEXT,
            poster_path          TEXT,
            backdrop_path        TEXT,
            imdb_id              TEXT,
            tvdb_id              INTEGER,
            wikidata_id          TEXT,
            display_vote_avg     REAL,
            display_vote_count   INTEGER,
            display_popularity   REAL,
            scraped_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (media_type, tmdb_id)
        );
        CREATE INDEX IF NOT EXISTS idx_src_tmdb_anime_year
            ON src_tmdb_anime(year);
        CREATE INDEX IF NOT EXISTS idx_src_tmdb_anime_imdb
            ON src_tmdb_anime(imdb_id);

        CREATE TABLE IF NOT EXISTS src_tmdb_persons (
            tmdb_id            INTEGER PRIMARY KEY,
            name               TEXT NOT NULL DEFAULT '',
            also_known_as      TEXT NOT NULL DEFAULT '[]',
            gender             INTEGER,
            birthday           TEXT,
            deathday           TEXT,
            place_of_birth     TEXT,
            biography          TEXT,
            known_for_dept     TEXT,
            profile_path       TEXT,
            imdb_id            TEXT,
            display_popularity REAL,
            scraped_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_src_tmdb_persons_imdb
            ON src_tmdb_persons(imdb_id);

        CREATE TABLE IF NOT EXISTS src_tmdb_credits (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            tmdb_anime_id  INTEGER NOT NULL,
            media_type     TEXT NOT NULL,
            tmdb_person_id INTEGER NOT NULL,
            credit_type    TEXT NOT NULL,
            character      TEXT,
            department     TEXT,
            job            TEXT,
            role           TEXT NOT NULL,
            role_raw       TEXT NOT NULL DEFAULT '',
            episode_count  INTEGER,
            scraped_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (media_type, tmdb_anime_id, tmdb_person_id, credit_type, role_raw)
        );
        CREATE INDEX IF NOT EXISTS idx_src_tmdb_credits_anime
            ON src_tmdb_credits(media_type, tmdb_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_tmdb_credits_person
            ON src_tmdb_credits(tmdb_person_id);
        """)
    finally:
        conn.isolation_level = old_isolation
        conn.execute("PRAGMA journal_mode=WAL")

    # Seed lookup tables and set schema version
    _seed_sources(conn)
    _seed_roles(conn)
    _upgrade_v56_multilang(conn)
    _upgrade_v57_structural_metadata(conn)
    _upgrade_v58_credits_metadata(conn)
    _upgrade_v59_names_alt(conn)
    _upgrade_v60_feat_split(conn)
    _upgrade_v60_corrections(conn)
    _upgrade_v61_src_multilang(conn)
    _upgrade_v61_titles_alt(conn)
    _upgrade_v62_canonical_name_ja(conn)
    conn.execute(
        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', '62')"
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
        ("seesaawiki", "SeesaaWiki",           "https://seesaawiki.jp",             "CC-BY-SA",    "fan-curated 詳細エピソード情報"),
        ("keyframe",   "Sakugabooru/Keyframe", "https://www.sakugabooru.com",       "CC",          "sakuga コミュニティ別名情報"),
        ("madb",       "メディア芸術DB",         "https://mediaarts-db.bunka.go.jp",  "public",      "文化庁 メディア芸術データベース (日本政府公開)"),
        ("tmdb",       "The Movie Database",   "https://www.themoviedb.org",        "CC-BY-NC",    "海外配信メタ + 越境アニメ作品のクレジット"),
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


def _upgrade_v56_multilang(conn: sqlite3.Connection) -> None:
    """Add multi-language name columns to existing v55 databases.

    Safe to call on fresh DBs too — ALTER TABLE failures are caught.
    """
    for table, col, defn in [
        ("persons",            "name_ko",    "TEXT NOT NULL DEFAULT ''"),
        ("persons",            "name_zh",    "TEXT NOT NULL DEFAULT ''"),
        ("persons",            "nationality", "TEXT NOT NULL DEFAULT '[]'"),
        ("persons",            "hometown",   "TEXT"),
        ("src_anilist_persons", "name_ko",   "TEXT NOT NULL DEFAULT ''"),
        ("src_anilist_persons", "name_zh",   "TEXT NOT NULL DEFAULT ''"),
        ("src_anilist_persons", "nationality", "TEXT NOT NULL DEFAULT '[]'"),
        ("person_aliases",     "lang",       "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


def _upgrade_v57_structural_metadata(conn: sqlite3.Connection) -> None:
    """Add structural metadata columns missing from v56 databases.

    Safe to call on fresh DBs too — ALTER TABLE failures are caught.
    """
    for table, col, defn in [
        # Silver anime: factual production metadata
        ("anime",               "country_of_origin", "TEXT"),
        ("anime",               "synonyms",          "TEXT NOT NULL DEFAULT '[]'"),
        ("anime",               "is_adult",          "INTEGER"),
        # Silver studios: studio nationality
        ("studios",             "country_of_origin", "TEXT"),
        # Silver persons: career timeline
        ("persons",             "years_active",      "TEXT NOT NULL DEFAULT '[]'"),
        # BRONZE tables: raw field preservation
        ("src_anilist_anime",   "country_of_origin", "TEXT"),
        ("src_anilist_anime",   "is_licensed",       "INTEGER"),
        ("src_anilist_anime",   "is_adult",          "INTEGER"),
        ("src_anilist_anime",   "mean_score",        "INTEGER"),
        ("src_anilist_anime",   "relations_json",    "TEXT"),
        ("src_anilist_persons", "years_active",      "TEXT DEFAULT '[]'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


def _upgrade_v58_credits_metadata(conn: sqlite3.Connection) -> None:
    """Add metadata columns to credits table for v58.

    Safe to call on fresh DBs too — ALTER TABLE failures are caught.
    """
    for table, col, defn in [
        ("credits", "affiliation",    "TEXT"),
        ("credits", "position",       "INTEGER"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


def _upgrade_v59_names_alt(conn: sqlite3.Connection) -> None:
    """Add names_alt JSON column for non-JA/EN/KO/ZH native names (th, ar, hi, vi, etc.).

    Safe to call on fresh DBs too — ALTER TABLE failures are caught.
    """
    for table, col, defn in [
        ("persons",             "names_alt", "TEXT NOT NULL DEFAULT '{}'"),
        ("src_anilist_persons", "names_alt", "TEXT NOT NULL DEFAULT '{}'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


def _upgrade_v60_feat_split(conn: sqlite3.Connection) -> None:
    """Split feat_career/feat_network into L2 (agg_*) and L3 (feat_*_scores).

    L2 = raw aggregates (pure GROUP BY/MAX/MIN from SILVER).
    L3 = derived scores (growth trends, centrality metrics, etc.).

    Safe to call on fresh DBs — CREATE TABLE IF NOT EXISTS will not error.
    """
    stmts = [
        # agg_person_career (L2: 生集約)
        """CREATE TABLE IF NOT EXISTS agg_person_career (
            person_id      TEXT PRIMARY KEY,
            run_id         INTEGER REFERENCES pipeline_runs(id),
            first_year     INTEGER,
            latest_year    INTEGER,
            active_years   INTEGER,
            total_credits  INTEGER,
            recent_credits INTEGER,
            highest_stage  INTEGER,
            primary_role   TEXT,
            peak_year      INTEGER,
            peak_credits   INTEGER,
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agg_person_career_first ON agg_person_career(first_year)",
        "CREATE INDEX IF NOT EXISTS idx_agg_person_career_role  ON agg_person_career(primary_role)",
        # feat_career_scores (L3: 派生スコア)
        """CREATE TABLE IF NOT EXISTS feat_career_scores (
            person_id      TEXT PRIMARY KEY,
            run_id         INTEGER REFERENCES pipeline_runs(id),
            career_track   TEXT,
            growth_trend   TEXT,
            growth_score   REAL,
            activity_ratio REAL,
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_feat_career_scores_track ON feat_career_scores(career_track)",
        # agg_person_network (L2: 生集約)
        """CREATE TABLE IF NOT EXISTS agg_person_network (
            person_id            TEXT PRIMARY KEY,
            run_id               INTEGER REFERENCES pipeline_runs(id),
            n_collaborators      INTEGER,
            n_unique_anime       INTEGER,
            n_bridge_communities INTEGER,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        # feat_network_scores (L3: 派生スコア)
        """CREATE TABLE IF NOT EXISTS feat_network_scores (
            person_id               TEXT PRIMARY KEY,
            run_id                  INTEGER REFERENCES pipeline_runs(id),
            birank                  REAL,
            patronage               REAL,
            degree_centrality       REAL,
            betweenness_centrality  REAL,
            closeness_centrality    REAL,
            eigenvector_centrality  REAL,
            hub_score               REAL,
            bridge_score            REAL,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    ]
    for s in stmts:
        try:
            conn.execute(s)
        except Exception:
            pass


def _upgrade_v60_corrections(conn: sqlite3.Connection) -> None:
    """Add corrections_* tables for tracking credit year and role corrections.

    Both tables are INSERT-ONLY for audit trail.
    corrections_credit_year: tracks manual year corrections for credits.
    corrections_role: tracks role normalization corrections.
    """
    stmts = [
        """CREATE TABLE IF NOT EXISTS corrections_credit_year (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            credit_id             INTEGER NOT NULL REFERENCES credits(id),
            credit_year_original  INTEGER,
            credit_year_corrected INTEGER NOT NULL,
            reason                TEXT NOT NULL DEFAULT '',
            corrected_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            corrected_by          TEXT NOT NULL DEFAULT ''
        )""",
        "CREATE INDEX IF NOT EXISTS idx_corr_year_credit ON corrections_credit_year(credit_id)",
        "CREATE INDEX IF NOT EXISTS idx_corr_year_at ON corrections_credit_year(corrected_at)",
        """CREATE TABLE IF NOT EXISTS corrections_role (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            credit_id          INTEGER NOT NULL REFERENCES credits(id),
            role_original      TEXT NOT NULL,
            role_corrected     TEXT NOT NULL,
            raw_role_override  TEXT,
            reason             TEXT NOT NULL DEFAULT '',
            corrected_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            corrected_by       TEXT NOT NULL DEFAULT ''
        )""",
        "CREATE INDEX IF NOT EXISTS idx_corr_role_credit ON corrections_role(credit_id)",
        "CREATE INDEX IF NOT EXISTS idx_corr_role_at ON corrections_role(corrected_at)",
    ]
    for s in stmts:
        try:
            conn.execute(s)
        except Exception:
            pass


def _upgrade_v61_src_multilang(conn: sqlite3.Connection) -> None:
    """Add name_ko/name_zh/names_alt to src_ann_persons."""
    for table, col, defn in [
        ("src_ann_persons",       "name_ko",   "TEXT NOT NULL DEFAULT ''"),
        ("src_ann_persons",       "name_zh",   "TEXT NOT NULL DEFAULT ''"),
        ("src_ann_persons",       "names_alt", "TEXT NOT NULL DEFAULT '{}'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


def _upgrade_v61_titles_alt(conn: sqlite3.Connection) -> None:
    """Add titles_alt JSON column to anime for non-JA native titles (KR/CN/TW etc.)."""
    try:
        conn.execute("ALTER TABLE anime ADD COLUMN titles_alt TEXT NOT NULL DEFAULT '{}'")
    except Exception:
        pass  # column already exists


# ===== 21_silver_enrichment/03: persons.canonical_name_ja (Card 21/03) =====
# NFKC + 旧字体→新字体変換 + 全角/半角統一で正規化した名前を保持。
# Scoring 経路には使用しない (検索/dedup 補助のみ)。H1 対象外 (主観値なし)。
# DuckDB 側は backfill スクリプトで ADD COLUMN IF NOT EXISTS + UPDATE を実行。


def _upgrade_v62_canonical_name_ja(conn: sqlite3.Connection) -> None:
    """Add canonical_name_ja column to persons (Card 21/03).

    Holds NFKC-normalized + 旧字体→新字体 converted name for search/dedup.
    Safe to call on fresh DBs — ALTER TABLE failure is silently caught.
    """
    try:
        conn.execute("ALTER TABLE persons ADD COLUMN canonical_name_ja TEXT")
    except Exception:
        pass  # column already exists
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_persons_canonical_name_ja"
            " ON persons(canonical_name_ja)"
        )
    except Exception:
        pass  # index already exists


# ===== ann extension =====
# DuckDB SILVER schema additions for ANN BRONZE → SILVER integration.
# Applied by src/etl/silver_loaders/ann.py via _apply_ddl().
#
# H1 compliance: ANN rating columns are prefixed display_rating_* to exclude
# them from all scoring paths. Only display_rating_votes /
# display_rating_weighted / display_rating_bayesian are permitted.
#
# ALTER TABLE (anime):
#   themes TEXT, plot_summary TEXT, running_time_raw TEXT,
#   objectionable_content TEXT, opening_themes_json TEXT,
#   ending_themes_json TEXT, insert_songs_json TEXT,
#   official_websites_json TEXT, vintage_raw TEXT, image_url TEXT,
#   display_rating_votes INTEGER, display_rating_weighted REAL,
#   display_rating_bayesian REAL
#
# Card 20/03: _ann suffix aliases (cross-source disambiguation, H1):
#   display_rating_count_ann    INTEGER — alias of display_rating_votes
#   display_rating_avg_ann      REAL    — alias of display_rating_weighted (best proxy for avg)
#   display_rating_weighted_ann REAL    — alias of display_rating_weighted
#   display_rating_bayesian_ann REAL    — alias of display_rating_bayesian
#
# ALTER TABLE (persons):
#   gender TEXT, height_raw TEXT, family_name_ja TEXT,
#   given_name_ja TEXT, hometown TEXT, image_url_ann TEXT
#   Note: hometown / gender may already exist in some SQLite DBs
#   (from _upgrade_v56_multilang / base DDL). DuckDB ADD COLUMN IF NOT EXISTS
#   handles this safely.
#
# New tables:
#   anime_episodes  (anime_id, episode_num, lang, title, aired_date)
#   anime_companies (anime_id, company_name, task, company_id, source)
#   anime_releases  (anime_id, product_title, release_date, href, region, source)
#   anime_news      (anime_id, datetime, title, href, source)
#
# Existing tables extended (INSERT only, no DDL change needed):
#   anime_relations        — ANN related data (relation_type = rel value)
#   character_voice_actors — ANN cast (character_id='ann:c<id>',
#                            person_id='ann:p<id>', anime_id='ann:a<id>')


# ===== anilist extension (Card 14/01) =====
# anime 拡張列 (display 系は display_* prefix で H1 隔離)
_ANILIST_EXTENSION_COLUMNS: list[tuple[str, str]] = [
    ("synonyms",               "TEXT"),
    ("country_of_origin",      "TEXT"),
    ("is_licensed",            "INTEGER"),
    ("is_adult",               "INTEGER"),
    ("hashtag",                "TEXT"),
    ("site_url",               "TEXT"),
    ("trailer_url",            "TEXT"),
    ("trailer_site",           "TEXT"),
    ("description",            "TEXT"),
    ("cover_large",            "TEXT"),
    ("cover_extra_large",      "TEXT"),
    ("cover_medium",           "TEXT"),
    ("banner",                 "TEXT"),
    ("external_links_json",    "TEXT"),
    ("airing_schedule_json",   "TEXT"),
    ("relations_json",         "TEXT"),
    ("display_score",          "REAL"),
    ("display_mean_score",     "REAL"),
    ("display_favourites",     "INTEGER"),
    ("display_popularity_rank","INTEGER"),
    ("display_rankings_json",  "TEXT"),
]


def _upgrade_anilist_anime_extension(conn: sqlite3.Connection) -> None:
    """Add AniList-sourced display/structural columns to anime (Card 14/01).

    All subjective/popularity columns are prefixed display_* (H1 compliance).
    Safe to run multiple times — ignores 'duplicate column' errors.
    """
    for col, col_type in _ANILIST_EXTENSION_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE anime ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # column already exists


# ===== keyframe extension (Card 14/06) =====

# anime 拡張列 — keyframe 固有メタデータ (slug / status / delimiter設定)。
# display 系なし: 全列が構造的メタデータ (H1 対象外)。
_KEYFRAME_ANIME_EXTENSION_COLUMNS: list[tuple[str, str]] = [
    ("kf_uuid",                "TEXT"),
    ("kf_status",              "TEXT"),
    ("kf_slug",                "TEXT"),
    ("kf_delimiters",          "TEXT"),  # JSON
    ("kf_episode_delimiters",  "TEXT"),
    ("kf_role_delimiters",     "TEXT"),
    ("kf_staff_delimiters",    "TEXT"),
]

# persons 拡張列 — Card 04 (seesaawiki) との共有。image_large が未追加の場合のみ追加。
_KEYFRAME_PERSONS_EXTENSION_COLUMNS: list[tuple[str, str]] = [
    ("image_large", "TEXT"),
]


def _upgrade_keyframe_extension(conn: sqlite3.Connection) -> None:
    """Add keyframe-sourced columns to anime and persons (Card 14/06).

    New SILVER tables (person_jobs, person_studio_affiliations,
    anime_settings_categories) are created by the DuckDB loader
    (src/etl/silver_loaders/keyframe.py) at runtime.

    Safe to run multiple times — ignores 'duplicate column' errors.
    """
    for col, col_type in _KEYFRAME_ANIME_EXTENSION_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE anime ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # column already exists

    for col, col_type in _KEYFRAME_PERSONS_EXTENSION_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE persons ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # column already exists


# ===== sakuga_atwiki extension (Card 14/07) =====
# work_title → silver anime_id title-matching result cache.
# Conservative 2-stage matcher: exact_title / normalized / unresolved.
# resolved_anime_id is NULL for unresolved rows (future downstream resolution).
_SAKUGA_ATWIKI_EXTENSION_DDL = [
    """CREATE TABLE IF NOT EXISTS sakuga_work_title_resolution (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        work_title        TEXT NOT NULL,
        work_year         INTEGER,
        work_format       TEXT,
        resolved_anime_id TEXT,
        match_method      TEXT,
        match_score       REAL,
        UNIQUE(work_title, work_year, work_format)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_swtr_anime
       ON sakuga_work_title_resolution(resolved_anime_id)""",
]


def _upgrade_sakuga_atwiki_extension(conn: sqlite3.Connection) -> None:
    """Create sakuga_work_title_resolution table (Card 14/07).

    Safe to run multiple times — CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
    """
    for stmt in _SAKUGA_ATWIKI_EXTENSION_DDL:
        try:
            conn.execute(stmt)
        except Exception:
            pass  # table / index already exists


# ===== madb extension (Card 14/02) =====
# DuckDB SILVER テーブル 6 本。SQLite 非対象。
# 実行は src/etl/silver_loaders/madb.py の create_tables() で行う。
# このブロックは single source of truth としての DDL 参照定義。
_MADB_SILVER_DDL = """
-- ===== madb extension =====

CREATE SEQUENCE IF NOT EXISTS seq_anime_broadcasters_id;
CREATE TABLE IF NOT EXISTS anime_broadcasters (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_broadcasters_id'),
    anime_id            VARCHAR NOT NULL,
    broadcaster_name    VARCHAR NOT NULL,
    is_network_station  INTEGER,
    UNIQUE (anime_id, broadcaster_name)
);
CREATE INDEX IF NOT EXISTS idx_anime_broadcasters_anime ON anime_broadcasters(anime_id);

CREATE TABLE IF NOT EXISTS anime_broadcast_schedule (
    anime_id  VARCHAR PRIMARY KEY,
    raw_text  VARCHAR NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS seq_anime_production_committee_id;
CREATE TABLE IF NOT EXISTS anime_production_committee (
    id            INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_production_committee_id'),
    anime_id      VARCHAR NOT NULL,
    company_name  VARCHAR NOT NULL,
    role_label    VARCHAR NOT NULL DEFAULT '',
    UNIQUE (anime_id, company_name, role_label)
);
CREATE INDEX IF NOT EXISTS idx_apc_anime ON anime_production_committee(anime_id);

CREATE SEQUENCE IF NOT EXISTS seq_anime_production_companies_id;
CREATE TABLE IF NOT EXISTS anime_production_companies (
    id            INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_production_companies_id'),
    anime_id      VARCHAR NOT NULL,
    company_name  VARCHAR NOT NULL,
    role_label    VARCHAR NOT NULL DEFAULT '',
    is_main       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (anime_id, company_name, role_label)
);
CREATE INDEX IF NOT EXISTS idx_apco_anime ON anime_production_companies(anime_id);

CREATE SEQUENCE IF NOT EXISTS seq_anime_video_releases_id;
CREATE TABLE IF NOT EXISTS anime_video_releases (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_video_releases_id'),
    release_madb_id VARCHAR NOT NULL UNIQUE,
    anime_id        VARCHAR,
    media_format    VARCHAR,
    date_published  VARCHAR,
    publisher       VARCHAR,
    product_id      VARCHAR,
    gtin            VARCHAR,
    runtime_min     INTEGER,
    volume_number   VARCHAR,
    release_title   VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_avr_anime ON anime_video_releases(anime_id);

CREATE SEQUENCE IF NOT EXISTS seq_anime_original_work_links_id;
CREATE TABLE IF NOT EXISTS anime_original_work_links (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_original_work_links_id'),
    anime_id        VARCHAR NOT NULL,
    work_name       VARCHAR,
    creator_text    VARCHAR,
    series_link_id  VARCHAR,
    UNIQUE (anime_id, work_name)
);
CREATE INDEX IF NOT EXISTS idx_aow_anime ON anime_original_work_links(anime_id);
"""

# ===== mal extension (Card 14/08) =====
# DuckDB SILVER テーブル 1 本 + anime ALTER 列群。
# 実行は src/etl/silver_loaders/mal.py の integrate() で行う。

# anime ALTER 列 — H1: display 系は display_*_mal suffix で隔離。
# mal_id_int は構造的 ID (integer 形式、既存 TEXT id とは別)。
_MAL_EXTENSION_COLUMNS: list[tuple[str, str]] = [
    ("mal_id_int",               "INTEGER"),
    ("display_score_mal",        "REAL"),
    ("display_popularity_mal",   "INTEGER"),
    ("display_members_mal",      "INTEGER"),
    ("display_favorites_mal",    "INTEGER"),
    ("display_rank_mal",         "INTEGER"),
    ("display_scored_by_mal",    "INTEGER"),
]

# DDL for anime_recommendations (new SILVER table).
_MAL_SILVER_DDL = """
-- ===== mal extension =====

CREATE SEQUENCE IF NOT EXISTS seq_anime_recommendations_id;
CREATE TABLE IF NOT EXISTS anime_recommendations (
    id                   INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_recommendations_id'),
    anime_id             VARCHAR NOT NULL,
    recommended_anime_id VARCHAR NOT NULL,
    votes                INTEGER,
    source               VARCHAR NOT NULL DEFAULT 'mal',
    UNIQUE(anime_id, recommended_anime_id, source)
);
CREATE INDEX IF NOT EXISTS idx_arec_anime ON anime_recommendations(anime_id);
"""


# ===== seesaawiki extension (Card 14/04) =====
# DuckDB SILVER テーブル 4 本 + shared table + persons 拡張列。
# SQLite 用 DDL は _upgrade_seesaawiki_extension() に記述。
# DuckDB 用 DDL は src/etl/silver_loaders/seesaawiki.py の _DDL_* constants。


def _upgrade_seesaawiki_extension(conn: sqlite3.Connection) -> None:
    """Create SeesaaWiki-specific SILVER tables for SQLite (Card 14/04).

    anime_production_committee は Card 14/02 (madb) と共有 — CREATE TABLE IF NOT EXISTS
    で重複定義を吸収する。どちらが先に走っても安全。

    Safe to run multiple times — all stmts use CREATE TABLE IF NOT EXISTS.
    """
    stmts = [
        # Shared with Card 14/02 (madb) — IF NOT EXISTS handles parallel card execution.
        """CREATE TABLE IF NOT EXISTS anime_production_committee (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id     TEXT NOT NULL,
            company_name TEXT NOT NULL,
            role_label   TEXT,
            UNIQUE (anime_id, company_name, role_label)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_apc_anime ON anime_production_committee(anime_id)",
        """CREATE TABLE IF NOT EXISTS anime_theme_songs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id   TEXT NOT NULL,
            song_type  TEXT,
            song_title TEXT,
            role       TEXT,
            name       TEXT,
            UNIQUE (anime_id, song_type, song_title, role, name)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_ats_anime ON anime_theme_songs(anime_id)",
        """CREATE TABLE IF NOT EXISTS anime_episode_titles (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            episode  INTEGER,
            title    TEXT,
            source   TEXT NOT NULL DEFAULT 'seesaawiki',
            UNIQUE (anime_id, episode, source)
        )""",
        """CREATE TABLE IF NOT EXISTS anime_gross_studios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id    TEXT NOT NULL,
            studio_name TEXT NOT NULL,
            episode     INTEGER,
            UNIQUE (anime_id, studio_name, episode)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_ags_anime ON anime_gross_studios(anime_id)",
        """CREATE TABLE IF NOT EXISTS anime_original_work_info (
            anime_id           TEXT PRIMARY KEY,
            author             TEXT,
            publisher          TEXT,
            label              TEXT,
            magazine           TEXT,
            serialization_type TEXT
        )""",
    ]
    for s in stmts:
        try:
            conn.execute(s)
        except Exception:
            pass  # table / index already exists

    # persons 拡張列 (DuckDB loader と対称)
    for col, defn in [
        ("name_native_raw",     "TEXT"),
        ("aliases",             "TEXT"),
        ("nationality",         "TEXT"),
        ("primary_occupations", "TEXT"),
        ("years_active",        "TEXT"),
        ("description",         "TEXT"),
        ("image_large",         "TEXT"),
        ("image_medium",        "TEXT"),
        ("hometown",            "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE persons ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


# ===== bangumi extension (Card 14/05) =====
# bangumi.tv BRONZE → SILVER 統合。6 BRONZE テーブルから 5 SILVER ターゲット。
# DuckDB SILVER 対象。SQLite 非対象。
# DDL は src/etl/silver_loaders/bangumi.py の _apply_ddl() で実行。
# このブロックは single source of truth としての列定義参照。
#
# H1 compliance: score / rank / favorite 系は display_* prefix で scoring 経路から隔離。
#
# anime 拡張列:
#   infobox_json                TEXT    — bangumi infobox raw JSON
#   platform                   TEXT    — bangumi platform code (TV/OVA/Movie 等)
#   meta_tags_json             TEXT    — bangumi meta tags JSON
#   series_flag                INTEGER — 0/1 シリーズ所属フラグ
#   display_score_bgm          REAL    — H1: display only
#   display_score_details_json TEXT    — H1: display only
#   display_rank_bgm           INTEGER — H1: display only
#   display_favorite_bgm       INTEGER — H1: display only (TRY_CAST of JSON; may be NULL)
#
# Card 20/03: additional display columns:
#   display_collect_count_bgm  INTEGER — H1: sum of wish+done+doing+on_hold+dropped
#                                        from favorite JSON (collection total)
#
# persons 拡張列 (Card 03 との衝突注意: gender/blood_type は Card 03 ALTER 済):
#   career_json  TEXT    — bangumi career array JSON
#   infobox_json TEXT    — bangumi person infobox JSON
#   summary_bgm  TEXT    — bangumi person summary text
#   bgm_id       INTEGER — bangumi person integer ID
#   person_type  INTEGER — 1=individual, 2=company, etc.
#
# characters 拡張列:
#   infobox_json   TEXT    — bangumi character infobox JSON
#   summary_bgm    TEXT    — bangumi character summary text
#   bgm_id         INTEGER — bangumi character integer ID
#   character_type INTEGER — character type code
#   images_json    TEXT    — bangumi character images JSON

# ===== 19_silver_postprocess/03: anime_relations source column (Card 19/03) =====
# anime_relations 拡張 — cross-source 行管理 (H4):
#   source  VARCHAR  DEFAULT ''
#           — 行の出所: 'anilist' | 'mal' | 'ann'
#           — PK を (anime_id, related_anime_id, relation_type, source) に拡張
#           — 同一 relation が複数 source に存在する場合、各行を保持可能
#
# DuckDB DDL 側変更箇所:
#   src/etl/silver_loaders/mal.py    — _DDL_ANIME_RELATIONS + _DDL_ANIME_RELATIONS_SOURCE_COL
#   src/etl/silver_loaders/ann.py    — _DDL_ANIME_RELATIONS + _DDL_ANIME_RELATIONS_SOURCE_COL
#   src/etl/silver_loaders/anilist.py — _DDL_ANIME_RELATIONS + _DDL_ANIME_RELATIONS_SOURCE_COL
#                                       + _ANIME_RELATIONS_FROM_JSON_SQL (relations_json パース)
#
# anime_recommendations は Card 14/08 で source='mal' 実装済。
# AniList / bangumi recommendations は BRONZE 未取得のため対象外 (Stop-if 済確認)。

# ===== 21_silver_enrichment/04: anime series_cluster_id (Card 21/04) =====
# anime 拡張列 — Union-Find シリーズクラスタ ID (post-hoc ETL):
#   series_cluster_id VARCHAR
#           — SEQUEL/PREQUEL/PARENT/SIDE_STORY/SUMMARY/ALTERNATIVE/FULL_STORY
#             関係を Union-Find でクラスタリングし、連結成分内の lex-min anime_id を格納。
#           — 孤立 anime (連結関係なし) は自身の id を格納 → 全行 non-NULL。
#           — H1 compliance: scoring 経路に流入しない (cluster 識別子のみ)。
#           — 実装: src/etl/cluster/series_cluster.py の backfill() が書込む。
#           — DDL (DuckDB):
#               ALTER TABLE anime ADD COLUMN IF NOT EXISTS series_cluster_id VARCHAR;
#               CREATE INDEX IF NOT EXISTS idx_anime_series_cluster ON anime(series_cluster_id);
