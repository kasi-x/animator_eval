-- Migration v55: Add GOLD analysis layer tables
-- Phase C: Complete SQLModel schema implementation
-- 
-- This migration adds 13 missing tables to complete the medallion architecture:
-- - 3 GOLD agg_* tables (genre affinity, role ecosystem, studio team composition)
-- - 2 GOLD feat_* tables (career dynamics, network centrality)
-- - 2 GOLD meta_* tables (hr observation, policy score)
-- - 2 SILVER metadata tables (ext_ids, display_lookup, analysis)
-- - 3 BRONZE MAL/MADB tables (anime, characters)

-- ============================================================================
-- GOLD LAYER: Aggregation tables for reporting
-- ============================================================================

-- agg_genre_affinity: Person-genre preference matrix
CREATE TABLE IF NOT EXISTS agg_genre_affinity (
	id INTEGER NOT NULL PRIMARY KEY,
	person_id VARCHAR NOT NULL,
	genre VARCHAR NOT NULL,
	work_count INTEGER NOT NULL,
	avg_role_weight FLOAT,
	computed_at DATETIME NOT NULL,
	UNIQUE(person_id, genre),
	FOREIGN KEY(person_id) REFERENCES persons(id)
);

-- agg_role_ecosystem: Role-level statistics by year
CREATE TABLE IF NOT EXISTS agg_role_ecosystem (
	id INTEGER NOT NULL PRIMARY KEY,
	role VARCHAR NOT NULL,
	year INTEGER NOT NULL,
	total_persons INTEGER NOT NULL,
	active_persons INTEGER NOT NULL,
	avg_credits_per_person FLOAT,
	median_salary_estimate FLOAT,
	computed_at DATETIME NOT NULL,
	UNIQUE(role, year),
	FOREIGN KEY(role) REFERENCES roles(code)
);

-- agg_studio_team_composition: Team by studio/year/role
CREATE TABLE IF NOT EXISTS agg_studio_team_composition (
	id INTEGER NOT NULL PRIMARY KEY,
	studio_id VARCHAR,
	year INTEGER NOT NULL,
	role VARCHAR NOT NULL,
	person_count INTEGER NOT NULL,
	avg_person_score FLOAT,
	computed_at DATETIME NOT NULL,
	UNIQUE(studio_id, year, role),
	FOREIGN KEY(role) REFERENCES roles(code)
);

-- ============================================================================
-- GOLD LAYER: Feature engineering tables
-- ============================================================================

-- feat_career_dynamics: Career progression metrics
CREATE TABLE IF NOT EXISTS feat_career_dynamics (
	id INTEGER NOT NULL PRIMARY KEY,
	person_id VARCHAR NOT NULL,
	career_start_year INTEGER,
	career_end_year INTEGER,
	role_changes INTEGER NOT NULL,
	studio_switches INTEGER NOT NULL,
	dormancy_periods INTEGER NOT NULL,
	computed_at DATETIME NOT NULL,
	UNIQUE(person_id),
	FOREIGN KEY(person_id) REFERENCES persons(id)
);

-- feat_network_centrality: Graph metrics (betweenness, closeness, eigenvector)
CREATE TABLE IF NOT EXISTS feat_network_centrality (
	id INTEGER NOT NULL PRIMARY KEY,
	person_id VARCHAR NOT NULL,
	betweenness FLOAT,
	closeness FLOAT,
	eigenvector FLOAT,
	degree INTEGER,
	computed_at DATETIME NOT NULL,
	UNIQUE(person_id),
	FOREIGN KEY(person_id) REFERENCES persons(id)
);

-- ============================================================================
-- GOLD LAYER: Metadata tables
-- ============================================================================

-- meta_hr_observation: Labor economics observations
CREATE TABLE IF NOT EXISTS meta_hr_observation (
	id INTEGER NOT NULL PRIMARY KEY,
	person_id VARCHAR NOT NULL,
	observation_type VARCHAR NOT NULL,
	observation_value VARCHAR,
	source VARCHAR,
	year INTEGER,
	confidence FLOAT,
	recorded_at DATETIME NOT NULL,
	FOREIGN KEY(person_id) REFERENCES persons(id)
);

-- meta_policy_score: Scoring decision audit
CREATE TABLE IF NOT EXISTS meta_policy_score (
	id INTEGER NOT NULL PRIMARY KEY,
	policy_version VARCHAR NOT NULL,
	score_type VARCHAR NOT NULL,
	component_name VARCHAR NOT NULL,
	component_value VARCHAR NOT NULL,
	rationale VARCHAR,
	created_at DATETIME NOT NULL,
	UNIQUE(policy_version, score_type, component_name)
);

-- ============================================================================
-- SILVER LAYER: Metadata tables (completing schema)
-- ============================================================================

-- ext_ids: Generic external ID repository (normalized)
CREATE TABLE IF NOT EXISTS ext_ids (
	id INTEGER NOT NULL PRIMARY KEY,
	entity_type VARCHAR NOT NULL,
	entity_id VARCHAR NOT NULL,
	source VARCHAR NOT NULL,
	external_id VARCHAR NOT NULL,
	added_at DATETIME NOT NULL,
	UNIQUE(entity_type, entity_id, source),
	FOREIGN KEY(source) REFERENCES sources(code)
);

-- display_lookup: Bronze access audit log (data provenance for display metrics)
CREATE TABLE IF NOT EXISTS display_lookup (
	id INTEGER NOT NULL PRIMARY KEY,
	silver_id VARCHAR NOT NULL,
	bronze_table VARCHAR NOT NULL,
	bronze_id VARCHAR,
	accessed_at DATETIME NOT NULL,
	accessed_by VARCHAR
);

-- analysis: Analysis metadata (free-form key-value for metrics)
CREATE TABLE IF NOT EXISTS analysis (
	id INTEGER NOT NULL PRIMARY KEY,
	entity_type VARCHAR NOT NULL,
	entity_id VARCHAR NOT NULL,
	metric_name VARCHAR NOT NULL,
	metric_value VARCHAR,
	computed_at DATETIME NOT NULL,
	UNIQUE(entity_type, entity_id, metric_name)
);

-- ============================================================================
-- BRONZE LAYER: MAL / MADB data sources (completing data collection)
-- ============================================================================

-- src_mal_anime: MyAnimeList anime data
CREATE TABLE IF NOT EXISTS src_mal_anime (
	mal_id INTEGER NOT NULL PRIMARY KEY,
	title VARCHAR,
	title_ja VARCHAR,
	episodes INTEGER,
	updated_at DATETIME NOT NULL
);

-- src_mal_characters: MyAnimeList characters (for voice actor linking)
CREATE TABLE IF NOT EXISTS src_mal_characters (
	mal_id INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR
);

-- src_madb_anime: MADB anime data (Japanese production database)
CREATE TABLE IF NOT EXISTS src_madb_anime (
	madb_id VARCHAR NOT NULL PRIMARY KEY,
	title VARCHAR,
	updated_at DATETIME NOT NULL
);
