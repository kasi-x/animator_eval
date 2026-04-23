2026-04-24 03:19:36 [debug    ] dotenv_loaded                  path=/home/user/dev/animetor_eval/.env

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

