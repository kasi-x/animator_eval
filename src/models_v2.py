"""SQLModel schema definitions for database (v2 - Atlas-compatible).

This module defines all 84 tables using SQLModel, making the schema the
single source of truth. All constraints, relationships, and indexes are
declared here in Python.

3-layer architecture:
  - SILVER (12 tables): canonical, score-free, normalized
  - BRONZE (13 tables): raw external data (includes anime.score)
  - GOLD (17+ tables): precomputed scores and analysis outputs

Structure:
  - Enums for controlled vocabularies
  - BaseModel for transient data (not DB-bound)
  - SQLModel tables for persistent storage
  - Index definitions
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlmodel import Field, SQLModel


# =============================================================================
# ENUMS (Controlled Vocabularies)
# =============================================================================


class SeasonEnum(str, Enum):
    """Anime broadcast season."""

    WINTER = "WINTER"
    SPRING = "SPRING"
    SUMMER = "SUMMER"
    FALL = "FALL"


class AnimeFormatEnum(str, Enum):
    """Anime release format."""

    TV = "TV"
    MOVIE = "MOVIE"
    OVA = "OVA"
    ONA = "ONA"
    SPECIAL = "SPECIAL"
    MUSIC = "MUSIC"


class AnimeStatusEnum(str, Enum):
    """Anime release status."""

    FINISHED = "FINISHED"
    RELEASING = "RELEASING"
    NOT_YET_RELEASED = "NOT_YET_RELEASED"
    CANCELLED = "CANCELLED"
    HIATUS = "HIATUS"


class WorkTypeEnum(str, Enum):
    """Work classification."""

    TV = "tv"
    TANPATSU = "tanpatsu"  # one-off


class ScaleClassEnum(str, Enum):
    """Production scale classification."""

    LARGE = "large"
    MEDIUM = "medium"
    SMALL = "small"


class RoleEnum(str, Enum):
    """Job role codes (24 role types)."""

    DIRECTOR = "director"
    ANIMATION_DIRECTOR = "animation_director"
    KEY_ANIMATOR = "key_animator"
    SECOND_KEY_ANIMATOR = "second_key_animator"
    IN_BETWEEN = "in_between"
    EPISODE_DIRECTOR = "episode_director"
    CHARACTER_DESIGNER = "character_designer"
    PHOTOGRAPHY_DIRECTOR = "photography_director"
    PRODUCER = "producer"
    PRODUCTION_MANAGER = "production_manager"
    SOUND_DIRECTOR = "sound_director"
    MUSIC = "music"
    SCREENPLAY = "screenplay"
    ORIGINAL_CREATOR = "original_creator"
    BACKGROUND_ART = "background_art"
    CGI_DIRECTOR = "cgi_director"
    LAYOUT = "layout"
    FINISHING = "finishing"
    EDITING = "editing"
    SETTINGS = "settings"
    VOICE_ACTOR = "voice_actor"
    LOCALIZATION = "localization"
    OTHER = "other"
    SPECIAL = "special"


# =============================================================================
# SILVER LAYER: Canonical, Score-Free Data (12 tables)
# =============================================================================


class Anime(SQLModel, table=True):
    """Canonical anime table (silver layer, score-free).

    - Single source of truth for anime metadata
    - No anime.score, popularity, or viewer metrics
    - External IDs moved to anime_external_ids table
    - Genres/tags normalized to separate tables
    """

    __tablename__ = "anime"

    id: str = Field(primary_key=True, description="Global ID: 'anilist:N' / 'ann:N' / 'keyframe:slug'")
    title_ja: str = Field(default="", description="Japanese title")
    title_en: str = Field(default="", description="English title")
    year: Optional[int] = Field(default=None, description="Broadcast/release year")
    season: Optional[SeasonEnum] = Field(default=None, description="Broadcast season")
    quarter: Optional[int] = Field(
        default=None,
        ge=1,
        le=4,
        description="Quarter (1-4), derived from season or start_date",
    )
    episodes: Optional[int] = Field(default=None, gt=0, description="Episode count")
    format: Optional[AnimeFormatEnum] = Field(default=None, description="Release format")
    duration: Optional[int] = Field(default=None, gt=0, description="Minutes per episode")
    start_date: Optional[str] = Field(default=None, description="ISO 8601 YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="ISO 8601 YYYY-MM-DD")
    status: Optional[AnimeStatusEnum] = Field(default=None, description="Release status")
    source: Optional[str] = Field(default=None, description="Source material (ORIGINAL/MANGA/etc)")
    work_type: Optional[WorkTypeEnum] = Field(default=None, description="TV or tanpatsu")
    scale_class: Optional[ScaleClassEnum] = Field(default=None, description="Production scale")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update")


class AnimeExternalIds(SQLModel, table=True):
    """External IDs for anime (normalized from anime table)."""

    __tablename__ = "anime_external_ids"

    anime_id: str = Field(foreign_key="anime.id", primary_key=True)
    source: str = Field(foreign_key="sources.code", primary_key=True)
    external_id: str = Field(description="ID in external system")

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_ext_id_source_id"),
    )


class AnimeGenres(SQLModel, table=True):
    """Normalized anime genres (from bronze JSON)."""

    __tablename__ = "anime_genres"

    anime_id: str = Field(foreign_key="anime.id", primary_key=True)
    genre_name: str = Field(primary_key=True)


class AnimeTags(SQLModel, table=True):
    """Normalized anime tags (from bronze JSON)."""

    __tablename__ = "anime_tags"

    anime_id: str = Field(foreign_key="anime.id", primary_key=True)
    tag_name: str = Field(primary_key=True)
    rank: Optional[int] = Field(default=None, ge=0, le=100)


class Persons(SQLModel, table=True):
    """Canonical person table (score-free)."""

    __tablename__ = "persons"

    id: str = Field(primary_key=True, description="Global ID: 'anilist:N' / 'ann:N' / etc")
    name_ja: str = Field(default="", description="Japanese name")
    name_en: str = Field(default="", description="English name (romanized)")
    birth_date: Optional[str] = Field(default=None, description="ISO 8601 YYYY-MM-DD")
    death_date: Optional[str] = Field(default=None, description="ISO 8601 YYYY-MM-DD")
    website_url: Optional[str] = Field(default=None, description="Portfolio/official website")
    aliases: Optional[str] = Field(
        default=None,
        description="Comma-separated legacy aliases (deprecated: use person_aliases table)",
    )
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PersonExternalIds(SQLModel, table=True):
    """External IDs for persons (normalized from persons table)."""

    __tablename__ = "person_external_ids"

    person_id: str = Field(foreign_key="persons.id", primary_key=True)
    source: str = Field(foreign_key="sources.code", primary_key=True)
    external_id: str = Field(description="ID in external system")

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_person_ext_id_source"),
    )


class PersonAliases(SQLModel, table=True):
    """Normalized alternative names for persons."""

    __tablename__ = "person_aliases"

    person_id: str = Field(foreign_key="persons.id", primary_key=True)
    alias_name: str = Field(primary_key=True, description="Alternative name (JA or EN)")
    language: Optional[str] = Field(default="ja", description="Language code (ja/en/etc)")
    evidence_source: str = Field(foreign_key="sources.code", description="Source of alias")


class Roles(SQLModel, table=True):
    """Role lookup table (24 standardized roles)."""

    __tablename__ = "roles"

    code: str = Field(primary_key=True, description="Role code (e.g., 'director', 'key_animator')")
    name_ja: str = Field(description="Japanese role name")
    name_en: str = Field(description="English role name")
    category: Optional[str] = Field(default=None, description="Role category (production/artistic/etc)")
    weight: float = Field(default=1.0, description="Centrality weight (used in PageRank)")
    description: Optional[str] = Field(default=None, description="Role description")


class Sources(SQLModel, table=True):
    """Data source lookup (scrapers, manual entry, etc)."""

    __tablename__ = "sources"

    code: str = Field(primary_key=True, description="Unique code (anilist, mal, ann, etc)")
    name_ja: str = Field(description="Japanese name")
    base_url: str = Field(description="Base URL for source")
    license: str = Field(description="Data license")
    description: str = Field(description="What this source contains")
    added_at: datetime = Field(default_factory=datetime.utcnow)
    retired_at: Optional[datetime] = Field(default=None, description="When source was deprecated")


class Credits(SQLModel, table=True):
    """Person-to-anime credits (work history, production relationships)."""

    __tablename__ = "credits"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    anime_id: str = Field(foreign_key="anime.id")
    role: str = Field(foreign_key="roles.code", description="Standardized role code")
    raw_role: Optional[str] = Field(default=None, description="Original source role string")
    episode: Optional[int] = Field(
        default=None,
        gt=0,
        description="Specific episode (NULL = all episodes / work-level credit)",
    )
    evidence_source: str = Field(
        foreign_key="sources.code",
        description="Where credit came from",
    )
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "person_id",
            "anime_id",
            "role",
            "episode",
            "evidence_source",
            name="uq_credit_unique",
        ),
        Index("ix_credits_person", "person_id"),
        Index("ix_credits_anime", "anime_id"),
        Index("ix_credits_role", "role"),
        Index("ix_credits_anime_role", "anime_id", "role"),
        Index("ix_credits_person_evidence", "person_id", "evidence_source"),
    )


class ExtIds(SQLModel, table=True):
    """Generic external ID repository (extensible for new sources)."""

    __tablename__ = "ext_ids"

    id: Optional[int] = Field(primary_key=True, default=None)
    entity_type: str = Field(description="Entity type (anime, person, work)")
    entity_id: str = Field(description="Local entity ID")
    source: str = Field(foreign_key="sources.code")
    external_id: str = Field(description="ID in external system")
    added_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id", "source", name="uq_ext_id_entity"),
    )


class Analysis(SQLModel, table=True):
    """Analysis metadata (scores pending location, scores v2 structure TBD)."""

    __tablename__ = "analysis"

    id: Optional[int] = Field(primary_key=True, default=None)
    entity_type: str = Field(description="Entity type being analyzed")
    entity_id: str = Field(description="Entity ID")
    metric_name: str = Field(description="Metric name")
    metric_value: Optional[str] = Field(default=None, description="Metric value (JSON-serialized)")
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id", "metric_name", name="uq_analysis_metric"),
    )


class DisplayLookup(SQLModel, table=True):
    """Optional: Log of bronze access for display purposes."""

    __tablename__ = "display_lookup"

    id: Optional[int] = Field(primary_key=True, default=None)
    silver_id: str = Field(description="Silver table ID (anime or person)")
    bronze_table: str = Field(description="Bronze table accessed")
    bronze_id: Optional[str] = Field(default=None, description="ID in bronze table")
    accessed_at: datetime = Field(default_factory=datetime.utcnow)
    accessed_by: Optional[str] = Field(default=None, description="Code path that accessed")


# =============================================================================
# GOLD LAYER: Precomputed Scores and Analysis (17+ tables)
# =============================================================================


class PersonScores(SQLModel, table=True):
    """Person scores and fixed effects (SILVER -> GOLD)."""

    __tablename__ = "scores"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    score_type: str = Field(description="Score type (authority, trust, density, etc)")
    score_value: float = Field(description="Numeric score")
    percentile: Optional[float] = Field(default=None, ge=0, le=100, description="Percentile within cohort")
    confidence_interval_low: Optional[float] = Field(default=None)
    confidence_interval_high: Optional[float] = Field(default=None)
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", "score_type", name="uq_score_person_type"),
        Index("ix_scores_person", "person_id"),
        Index("ix_scores_type", "score_type"),
    )


class MetaLineage(SQLModel, table=True):
    """Lineage metadata: which input data produced which output."""

    __tablename__ = "meta_lineage"

    id: Optional[int] = Field(primary_key=True, default=None)
    output_table: str = Field(description="Output table name")
    output_id: str = Field(description="Record ID in output table")
    input_table: str = Field(description="Input table name")
    input_id: str = Field(description="Record ID in input table")
    transform: str = Field(description="Transformation applied")
    description: str = Field(default="", description="Human-readable description")
    created_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "output_table",
            "output_id",
            "input_table",
            "input_id",
            name="uq_lineage_edge",
        ),
    )


class MetaPolicyScore(SQLModel, table=True):
    """Scoring policy metadata: decision points, weights, thresholds."""

    __tablename__ = "meta_policy_score"

    id: Optional[int] = Field(primary_key=True, default=None)
    policy_version: str = Field(description="Scoring policy version (e.g., 'v1.0')")
    score_type: str = Field(description="Which score type (authority, trust, etc)")
    component_name: str = Field(description="Component name (e.g., 'pagerank_weight')")
    component_value: str = Field(description="Parameter value (may be JSON)")
    rationale: Optional[str] = Field(default=None, description="Why this value was chosen")
    created_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "policy_version",
            "score_type",
            "component_name",
            name="uq_policy_component",
        ),
    )


class MetaHRObservation(SQLModel, table=True):
    """Human Resource observations for labor economics analysis."""

    __tablename__ = "meta_hr_observation"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    observation_type: str = Field(
        description="Type (salary, role_change, studio_change, etc)"
    )
    observation_value: Optional[str] = Field(default=None, description="Value or category")
    source: Optional[str] = Field(default=None, description="Evidence source")
    year: Optional[int] = Field(default=None, description="Year of observation")
    confidence: Optional[float] = Field(default=None, ge=0, le=1)
    recorded_at: datetime = Field(default_factory=datetime.utcnow)


class FeatCreditContribution(SQLModel, table=True):
    """Feature: Credit contribution metrics."""

    __tablename__ = "feat_credit_contribution"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    year: int = Field(description="Calendar year")
    total_credits: int = Field(description="Total credits this year")
    unique_works: int = Field(description="Number of unique anime/works")
    unique_studios: int = Field(description="Number of unique studios")
    avg_role_weight: Optional[float] = Field(default=None)
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", "year", name="uq_contrib_person_year"),
    )


class FeatNetworkCentrality(SQLModel, table=True):
    """Feature: Network centrality measures."""

    __tablename__ = "feat_network_centrality"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    betweenness: Optional[float] = Field(default=None, description="Betweenness centrality")
    closeness: Optional[float] = Field(default=None, description="Closeness centrality")
    eigenvector: Optional[float] = Field(default=None, description="Eigenvector centrality")
    degree: Optional[int] = Field(default=None, description="Network degree")
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", name="uq_centrality_person"),
    )


class FeatCareerDynamics(SQLModel, table=True):
    """Feature: Career progression and mobility."""

    __tablename__ = "feat_career_dynamics"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    career_start_year: Optional[int] = Field(default=None)
    career_end_year: Optional[int] = Field(default=None)
    role_changes: int = Field(default=0, description="Number of distinct roles held")
    studio_switches: int = Field(default=0, description="Number of studio changes")
    dormancy_periods: int = Field(default=0, description="Gaps in active years")
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", name="uq_dynamics_person"),
    )


class AggStudioTeamComposition(SQLModel, table=True):
    """Aggregation: Studio team composition by year/role."""

    __tablename__ = "agg_studio_team_composition"

    id: Optional[int] = Field(primary_key=True, default=None)
    studio_id: Optional[str] = Field(default=None)
    year: int = Field()
    role: str = Field(foreign_key="roles.code")
    person_count: int = Field(description="Number of distinct persons")
    avg_person_score: Optional[float] = Field(default=None)
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("studio_id", "year", "role", name="uq_team_studio_year_role"),
    )


class AggGenreAffinity(SQLModel, table=True):
    """Aggregation: Person-genre affinity matrix."""

    __tablename__ = "agg_genre_affinity"

    id: Optional[int] = Field(primary_key=True, default=None)
    person_id: str = Field(foreign_key="persons.id")
    genre: str = Field(description="Anime genre")
    work_count: int = Field(description="Works in this genre")
    avg_role_weight: Optional[float] = Field(default=None)
    computed_at: datetime = Field(default_factory=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", "genre", name="uq_affinity_person_genre"),
    )


class AggRoleEcosystem(SQLModel, table=True):
    """Aggregation: Role-level ecosystem statistics."""

    __tablename__ = "agg_role_ecosystem"

    id: Optional[int] = Field(primary_key=True, default=None)
    role: str = Field(foreign_key="roles.code", primary_key=True)
    year: int = Field(primary_key=True)
    total_persons: int = Field()
    active_persons: int = Field(description="Persons with at least 1 credit this year")
    avg_credits_per_person: Optional[float] = Field(default=None)
    median_salary_estimate: Optional[float] = Field(
        default=None, description="(if available from HR sources)"
    )
    computed_at: datetime = Field(default_factory=datetime.utcnow)


# =============================================================================
# BRONZE LAYER: Raw External Data (13 tables) — PLACEHOLDER
# =============================================================================
# Each BRONZE table will mirror raw API responses from external sources.
# They are not defined here to avoid bloat; they will be created by
# scrapers and defined in a separate src/models_bronze.py or inline in
# scrapers themselves.


# =============================================================================
# Schema Version Tracking
# =============================================================================


class SchemaMeta(SQLModel, table=True):
    """Schema version tracking."""

    __tablename__ = "schema_meta"

    version: str = Field(primary_key=True, description="Schema version (e.g., '54')")
    applied_at: datetime = Field(default_factory=datetime.utcnow)
    description: Optional[str] = Field(default=None)


# =============================================================================
# BRONZE LAYER: Raw External Data (13 tables)
# =============================================================================
# Each BRONZE table mirrors raw API responses from external sources.
# anime.score is stored here (NEVER in SILVER layer).
# These tables are appended to, never deleted from (audit trail).


class SrcAnimelistAnime(SQLModel, table=True):
    """Raw AniList anime data (https://anilist.co)."""

    __tablename__ = "src_anilist_anime"

    anilist_id: int = Field(primary_key=True)
    title_ja: Optional[str] = Field(default=None)
    title_en: Optional[str] = Field(default=None)
    year: Optional[int] = Field(default=None)
    season: Optional[str] = Field(default=None)
    episodes: Optional[int] = Field(default=None)
    format: Optional[str] = Field(default=None)
    status: Optional[str] = Field(default=None)
    score: Optional[float] = Field(default=None, description="Viewer rating 0-100 (NEVER use in scoring)")
    popularity: Optional[int] = Field(default=None)
    popularity_rank: Optional[int] = Field(default=None)
    favourites: Optional[int] = Field(default=None)
    mean_score: Optional[float] = Field(default=None)
    genres: Optional[str] = Field(default=None, description="JSON array of genres")
    tags: Optional[str] = Field(default=None, description="JSON array of tags with rank")
    studios: Optional[str] = Field(default=None, description="JSON array of studio names")
    source: Optional[str] = Field(default=None)
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    cover_image_url: Optional[str] = Field(default=None)
    banner_image_url: Optional[str] = Field(default=None)
    site_url: Optional[str] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SrcAnimelistPersons(SQLModel, table=True):
    """Raw AniList person data."""

    __tablename__ = "src_anilist_persons"

    anilist_id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None)
    name_ja: Optional[str] = Field(default=None)
    name_en: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    image_url: Optional[str] = Field(default=None)
    site_url: Optional[str] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SrcAnimelistCredits(SQLModel, table=True):
    """Raw AniList staff credits."""

    __tablename__ = "src_anilist_credits"

    id: Optional[int] = Field(primary_key=True, default=None)
    anilist_anime_id: int = Field(foreign_key="src_anilist_anime.anilist_id")
    anilist_person_id: int = Field(foreign_key="src_anilist_persons.anilist_id")
    role: str = Field(description="Raw role from AniList")
    sub_role: Optional[str] = Field(default=None)


class SrcAnnAnime(SQLModel, table=True):
    """Raw Anime News Network anime data."""

    __tablename__ = "src_ann_anime"

    ann_id: str = Field(primary_key=True)
    title: Optional[str] = Field(default=None)
    title_ja: Optional[str] = Field(default=None)
    episodes: Optional[int] = Field(default=None)
    type: Optional[str] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SrcAnnPersons(SQLModel, table=True):
    """Raw ANN person data."""

    __tablename__ = "src_ann_persons"

    ann_id: str = Field(primary_key=True)
    name: Optional[str] = Field(default=None)


class SrcAnnCredits(SQLModel, table=True):
    """Raw ANN staff credits."""

    __tablename__ = "src_ann_credits"

    id: Optional[int] = Field(primary_key=True, default=None)
    ann_anime_id: str = Field(foreign_key="src_ann_anime.ann_id")
    ann_person_id: str = Field(foreign_key="src_ann_persons.ann_id")
    role: Optional[str] = Field(default=None)


class SrcAllcinemaAnime(SQLModel, table=True):
    """Raw allcinema anime data (日本映画)."""

    __tablename__ = "src_allcinema_anime"

    allcinema_id: str = Field(primary_key=True)
    title: Optional[str] = Field(default=None)
    title_ja: Optional[str] = Field(default=None)
    year: Optional[int] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SrcAllcinemaPersons(SQLModel, table=True):
    """Raw allcinema person data."""

    __tablename__ = "src_allcinema_persons"

    allcinema_id: str = Field(primary_key=True)
    name: Optional[str] = Field(default=None)


class SrcAllcinemaCredits(SQLModel, table=True):
    """Raw allcinema staff credits."""

    __tablename__ = "src_allcinema_credits"

    id: Optional[int] = Field(primary_key=True, default=None)
    allcinema_anime_id: str = Field(foreign_key="src_allcinema_anime.allcinema_id")
    allcinema_person_id: str = Field(foreign_key="src_allcinema_persons.allcinema_id")
    role: Optional[str] = Field(default=None)


class SrcSeesaawikiAnime(SQLModel, table=True):
    """Raw SeesaaWiki anime data (fan-curated episode info)."""

    __tablename__ = "src_seesaawiki_anime"

    seesaawiki_id: str = Field(primary_key=True)
    title: Optional[str] = Field(default=None)
    episodes_data: Optional[str] = Field(default=None, description="JSON episode-level staff info")
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SrcMalAnime(SQLModel, table=True):
    """Raw MyAnimeList anime data."""

    __tablename__ = "src_mal_anime"

    mal_id: int = Field(primary_key=True)
    title: Optional[str] = Field(default=None)
    title_ja: Optional[str] = Field(default=None)
    episodes: Optional[int] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SrcMalCharacters(SQLModel, table=True):
    """Raw MAL character data (optional)."""

    __tablename__ = "src_mal_characters"

    mal_id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None)


class SrcMadbAnime(SQLModel, table=True):
    """Raw MADB (移動教室) anime data."""

    __tablename__ = "src_madb_anime"

    madb_id: str = Field(primary_key=True)
    title: Optional[str] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# =============================================================================
# Indexes and Constraints (Atlas-compatible)
# =============================================================================
# Most indexes are defined inline via SQLModel Field + Index() above.
# Additional composite indexes for hotpaths:
#
# - anime: (year, format) for time-series aggregation
# - credits: (anime_id, role) for "team composition" queries
# - person_scores: (person_id, score_type) for individual profiles
# - anime_external_ids: (source, external_id) for reverse lookups
