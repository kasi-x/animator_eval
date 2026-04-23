"""Typed dataclasses representing one row per DB table.

Each dataclass holds all columns as fields.
Raw values from SQLite are stored as-is; JSON parsing and type conversion
are the responsibility of Pydantic models.

## Design rationale

Replaces `sqlite3.Row`'s `row["col_name"]` string access with `.col_name`
attribute access so that:
- IDEs provide auto-completion
- Type checkers catch column renames
- `tests/test_db_schema.py` can diff against PRAGMA to detect schema drift

## Steps when changing columns

1. Add a migration (`database.py`)
2. Update the corresponding dataclass field here (`db_rows.py`)
3. Update `from_db_row()` in the Pydantic model (`models.py`)
4. Run tests — `test_db_schema.py` will catch any mismatch
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _from_sqlite_row(cls, row) -> object:
    """Build a dataclass instance from a sqlite3.Row or dict.

    Fields absent from the table use the dataclass default.
    Columns absent from the dataclass are ignored (forward-compatible).
    """
    field_names = {f.name for f in dataclasses.fields(cls)}
    if hasattr(row, "keys"):
        kwargs = {k: row[k] for k in row.keys() if k in field_names}
    else:
        kwargs = {k: v for k, v in zip(row.keys(), tuple(row)) if k in field_names}
    return cls(**kwargs)


# ---------------------------------------------------------------------------
# persons table
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PersonRow:
    """One row of the persons table."""

    id: str
    name_ja: str = ""
    name_en: str = ""
    name_ko: str = ""
    name_zh: str = ""
    names_alt: str = "{}"  # JSON dict: {"th": "...", "ar": "...", "vi": "..."} etc.
    aliases: str = "[]"  # JSON array string
    nationality: str = "[]"  # JSON array string
    mal_id: int | None = None
    anilist_id: int | None = None
    canonical_id: str | None = None
    date_of_birth: str | None = None
    hometown: str | None = None
    blood_type: str | None = None
    description: str | None = None
    gender: str | None = None
    years_active: str = "[]"  # JSON array string
    favourites: int | None = None
    site_url: str | None = None
    image_medium: str | None = None
    name_priority: int = 0
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "PersonRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# anime table
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AnimeRow:
    """One row of the anime table."""

    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    episodes: int | None = None
    format: str | None = None
    status: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: int | None = None
    original_work_type: str | None = None
    quarter: int | None = None
    work_type: str | None = None
    scale_class: str | None = None
    country_of_origin: str | None = None
    synonyms: str = "[]"  # JSON array string
    is_adult: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "AnimeRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# credits table
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CreditRow:
    """One row of the credits table."""

    id: int
    person_id: str
    anime_id: str
    role: str
    raw_role: str = ""
    episode: int | None = None
    evidence_source: str | None = None
    updated_at: str | None = None
    credit_year: int | None = None
    credit_quarter: int | None = None
    affiliation: str | None = None
    position: int | None = None

    @classmethod
    def from_row(cls, row) -> "CreditRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# scores table
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ScoreRow:
    """One row of the scores table."""

    person_id: str
    person_fe: float = 0.0
    studio_fe_exposure: float = 0.0
    birank: float = 0.0
    patronage: float = 0.0
    dormancy: float = 1.0
    awcc: float = 0.0
    iv_score: float = 0.0
    career_track: str = "multi_track"
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "ScoreRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# score_history table
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ScoreHistoryRow:
    """One row of the score_history table."""

    id: int
    person_id: str
    person_fe: float = 0.0
    studio_fe_exposure: float = 0.0
    birank: float = 0.0
    patronage: float = 0.0
    dormancy: float = 1.0
    awcc: float = 0.0
    iv_score: float = 0.0
    run_at: str | None = None
    year: int | None = None
    quarter: int | None = None

    @classmethod
    def from_row(cls, row) -> "ScoreHistoryRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# character_voice_actors table
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class VARow:
    """One row of the character_voice_actors table."""

    id: int
    character_id: str
    person_id: str
    anime_id: str
    character_role: str = ""
    source: str = ""
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "VARow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_person_scores table (L3: algorithm scores)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatPersonScoresRow:
    """One row of the feat_person_scores table."""

    person_id: str
    run_id: int | None = None
    person_fe: float | None = None
    person_fe_se: float | None = None
    person_fe_n_obs: int | None = None
    studio_fe_exposure: float | None = None
    birank: float | None = None
    patronage: float | None = None
    awcc: float | None = None
    dormancy: float | None = None
    ndi: float | None = None
    career_friction: float | None = None
    peer_boost: float | None = None
    iv_score: float | None = None
    iv_score_pct: float | None = None
    person_fe_pct: float | None = None
    birank_pct: float | None = None
    patronage_pct: float | None = None
    awcc_pct: float | None = None
    dormancy_pct: float | None = None
    confidence: float | None = None
    score_range_low: float | None = None
    score_range_high: float | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatPersonScoresRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_network table (L3: network metrics)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatNetworkRow:
    """One row of the feat_network table."""

    person_id: str
    run_id: int | None = None
    degree_centrality: float | None = None
    betweenness_centrality: float | None = None
    closeness_centrality: float | None = None
    eigenvector_centrality: float | None = None
    hub_score: float | None = None
    n_collaborators: int | None = None
    n_unique_anime: int | None = None
    bridge_score: float | None = None
    n_bridge_communities: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatNetworkRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_career table (L2/L3 mixed; planned for future separation)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCareerRow:
    """One row of the feat_career table."""

    person_id: str
    run_id: int | None = None
    first_year: int | None = None
    latest_year: int | None = None
    active_years: int | None = None
    total_credits: int | None = None
    highest_stage: int | None = None
    primary_role: str | None = None
    career_track: str | None = None
    peak_year: int | None = None
    peak_credits: int | None = None
    growth_trend: str | None = None
    growth_score: float | None = None
    activity_ratio: float | None = None
    recent_credits: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatCareerRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_genre_affinity table (L3: genre affinity scores)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatGenreAffinityRow:
    """One row of the feat_genre_affinity table (PK: person_id + genre)."""

    person_id: str
    genre: str
    run_id: int | None = None
    affinity_score: float | None = None
    work_count: int | None = None

    @classmethod
    def from_row(cls, row) -> "FeatGenreAffinityRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_contribution table (L3: individual contribution profile, Layer 2)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatContributionRow:
    """One row of the feat_contribution table."""

    person_id: str
    run_id: int | None = None
    peer_percentile: float | None = None
    opportunity_residual: float | None = None
    consistency: float | None = None
    independent_value: float | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatContributionRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_credit_activity table (L2: credit activity patterns)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCreditActivityRow:
    """One row of the feat_credit_activity table."""

    person_id: str
    first_abs_quarter: int | None = None
    last_abs_quarter: int | None = None
    activity_span_quarters: int | None = None
    active_quarters: int | None = None
    density: float | None = None
    n_gaps: int | None = None
    mean_gap_quarters: float | None = None
    median_gap_quarters: float | None = None
    min_gap_quarters: int | None = None
    max_gap_quarters: int | None = None
    std_gap_quarters: float | None = None
    consecutive_quarters: int | None = None
    consecutive_rate: float | None = None
    n_hiatuses: int | None = None
    longest_hiatus_quarters: int | None = None
    quarters_since_last_credit: int | None = None
    active_years: int | None = None
    n_year_gaps: int | None = None
    mean_year_gap: float | None = None
    max_year_gap: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatCreditActivityRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_career_annual table (L2: per-year per-role career aggregation)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCareerAnnualRow:
    """One row of the feat_career_annual table (PK: person_id + career_year)."""

    person_id: str
    career_year: int = 0
    credit_year: int = 0
    n_works: int = 0
    n_credits: int = 0
    n_roles: int = 0
    works_direction: int = 0
    works_animation_supervision: int = 0
    works_animation: int = 0
    works_design: int = 0
    works_technical: int = 0
    works_art: int = 0
    works_sound: int = 0
    works_writing: int = 0
    works_production: int = 0
    works_production_management: int = 0
    works_finishing: int = 0
    works_editing: int = 0
    works_settings: int = 0
    works_other: int = 0

    @classmethod
    def from_row(cls, row) -> "FeatCareerAnnualRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_studio_affiliation table (L2: studio membership aggregated by year)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatStudioAffiliationRow:
    """One row of the feat_studio_affiliation table (PK: person_id + credit_year + studio_id)."""

    person_id: str
    credit_year: int = 0
    studio_id: str = ""
    studio_name: str = ""
    n_works: int = 0
    n_credits: int = 0
    is_main_studio: int = 0

    @classmethod
    def from_row(cls, row) -> "FeatStudioAffiliationRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_credit_contribution table (L3: per-work per-role score contribution estimate)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCreditContributionRow:
    """One row of the feat_credit_contribution table (PK: person_id + anime_id + role)."""

    person_id: str
    anime_id: str = ""
    role: str = ""
    credit_year: int | None = None
    production_scale: float | None = None
    role_weight: float | None = None
    episode_coverage: float | None = None
    dur_mult: float | None = None
    edge_weight: float | None = None
    edge_weight_share: float | None = None
    iv_contrib_est: float | None = None
    debut_year: int | None = None
    career_year_at_credit: int | None = None
    is_debut_work: int | None = None

    @classmethod
    def from_row(cls, row) -> "FeatCreditContributionRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_person_work_summary table (L3: per-work contribution aggregation)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatPersonWorkSummaryRow:
    """One row of the feat_person_work_summary table."""

    person_id: str
    n_distinct_works: int | None = None
    total_production_scale: float | None = None
    mean_production_scale: float | None = None
    max_production_scale: float | None = None
    best_work_anime_id: str | None = None
    total_edge_weight: float | None = None
    mean_edge_weight_per_work: float | None = None
    max_edge_weight: float | None = None
    top_contrib_anime_id: str | None = None
    total_iv_contrib_est: float | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatPersonWorkSummaryRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# agg_milestones table (L2: career events)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AggMilestoneRow:
    """One row of the agg_milestones table (PK: person_id + event_type + year + anime_id)."""

    person_id: str
    event_type: str = ""
    year: int = 0
    anime_id: str = ""
    anime_title: str | None = None
    description: str | None = None

    @classmethod
    def from_row(cls, row) -> "AggMilestoneRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# agg_director_circles table (L2: co-credit aggregation)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AggDirectorCircleRow:
    """One row of the agg_director_circles table (PK: person_id + director_id)."""

    person_id: str
    director_id: str = ""
    shared_works: int = 0
    hit_rate: float | None = None
    roles: str = "[]"  # JSON array string
    latest_year: int | None = None

    @classmethod
    def from_row(cls, row) -> "AggDirectorCircleRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_mentorships table (L3: inferred mentor-mentee relationships)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatMentorshipRow:
    """One row of the feat_mentorships table (PK: mentor_id + mentee_id)."""

    mentor_id: str
    mentee_id: str = ""
    n_shared_works: int = 0
    hit_rate: float | None = None
    mentor_stage: int | None = None
    mentee_stage: int | None = None
    first_year: int | None = None
    latest_year: int | None = None

    @classmethod
    def from_row(cls, row) -> "FeatMentorshipRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_cluster_membership table (L3: cluster assignments)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatClusterMembershipRow:
    """One row of the feat_cluster_membership table (PK: person_id)."""

    person_id: str
    community_id: int | None = None
    career_track: str | None = None
    growth_trend: str | None = None
    studio_cluster_id: int | None = None
    studio_cluster_name: str | None = None
    cooccurrence_group_id: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatClusterMembershipRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_causal_estimates table (L3: causal inference scores)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCausalEstimatesRow:
    """One row of the feat_causal_estimates table (PK: person_id)."""

    person_id: str
    peer_effect_boost: float | None = None
    career_friction: float | None = None
    era_fe: float | None = None
    era_deflated_iv: float | None = None
    opportunity_residual: float | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatCausalEstimatesRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_work_context table (L2: work context)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatWorkContextRow:
    """One row of the feat_work_context table (PK: anime_id + credit_year)."""

    anime_id: str
    credit_year: int | None = None
    n_staff: int | None = None
    n_distinct_roles: int | None = None
    n_direction: int | None = None
    n_animation_supervision: int | None = None
    n_animation: int | None = None
    n_design: int | None = None
    n_technical: int | None = None
    n_art: int | None = None
    n_sound: int | None = None
    n_writing: int | None = None
    n_production: int | None = None
    n_other: int | None = None
    mean_career_year: float | None = None
    median_career_year: float | None = None
    max_career_year: int | None = None
    production_scale: float | None = None
    difficulty_score: float | None = None
    scale_raw: float | None = None
    scale_tier: int | None = None
    scale_label: str | None = None
    format_group: str | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatWorkContextRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_person_role_progression table (L2: role progression)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatPersonRoleProgressionRow:
    """One row of the feat_person_role_progression table (PK: person_id + role_category)."""

    person_id: str
    role_category: str = ""
    first_year: int | None = None
    last_year: int | None = None
    peak_year: int | None = None
    n_works: int | None = None
    n_credits: int | None = None
    career_year_first: int | None = None
    still_active: int | None = None

    @classmethod
    def from_row(cls, row) -> "FeatPersonRoleProgressionRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_birank_annual table (L3: annual BiRank snapshots)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatBirankAnnualRow:
    """One row of the feat_birank_annual table (PK: person_id + year)."""

    person_id: str
    year: int = 0
    birank: float = 0.0
    raw_pagerank: float | None = None
    graph_size: int | None = None
    n_credits_cumulative: int | None = None

    @classmethod
    def from_row(cls, row) -> "FeatBirankAnnualRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# agg_person_career table (L2: raw career aggregates)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AggPersonCareerRow:
    """One row of the agg_person_career table (L2 layer)."""

    person_id: str
    run_id: int | None = None
    first_year: int | None = None
    latest_year: int | None = None
    active_years: int | None = None
    total_credits: int | None = None
    recent_credits: int | None = None
    highest_stage: int | None = None
    primary_role: str | None = None
    peak_year: int | None = None
    peak_credits: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "AggPersonCareerRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_career_scores table (L3: derived career scores)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCareerScoresRow:
    """One row of the feat_career_scores table (L3 layer)."""

    person_id: str
    run_id: int | None = None
    career_track: str | None = None
    growth_trend: str | None = None
    growth_score: float | None = None
    activity_ratio: float | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatCareerScoresRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# agg_person_network table (L2: raw network aggregates)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AggPersonNetworkRow:
    """One row of the agg_person_network table (L2 layer)."""

    person_id: str
    run_id: int | None = None
    n_collaborators: int | None = None
    n_unique_anime: int | None = None
    n_bridge_communities: int | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "AggPersonNetworkRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_network_scores table (L3: derived network scores)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatNetworkScoresRow:
    """One row of the feat_network_scores table (L3 layer)."""

    person_id: str
    run_id: int | None = None
    birank: float | None = None
    patronage: float | None = None
    degree_centrality: float | None = None
    betweenness_centrality: float | None = None
    closeness_centrality: float | None = None
    eigenvector_centrality: float | None = None
    hub_score: float | None = None
    bridge_score: float | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "FeatNetworkScoresRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# corrections_credit_year table (audit: credit year corrections)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CorrectionsCreditYearRow:
    """One row of the corrections_credit_year table (INSERT-ONLY audit log)."""

    id: int = 0
    credit_id: int = 0
    credit_year_original: int | None = None
    credit_year_corrected: int = 0
    reason: str = ""
    corrected_at: str | None = None
    corrected_by: str = ""

    @classmethod
    def from_row(cls, row) -> "CorrectionsCreditYearRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# corrections_role table (audit: role normalization corrections)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CorrectionsRoleRow:
    """One row of the corrections_role table (INSERT-ONLY audit log)."""

    id: int = 0
    credit_id: int = 0
    role_original: str = ""
    role_corrected: str = ""
    raw_role_override: str | None = None
    reason: str = ""
    corrected_at: str | None = None
    corrected_by: str = ""

    @classmethod
    def from_row(cls, row) -> "CorrectionsRoleRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# mapping from table name → Row class (used by schema tests)
# ---------------------------------------------------------------------------

TABLE_ROW_MAP: dict[str, type] = {
    # L1: raw source data
    "persons": PersonRow,
    "anime": AnimeRow,
    "credits": CreditRow,
    "person_scores": ScoreRow,
    "score_history": ScoreHistoryRow,
    "character_voice_actors": VARow,
    # L2: aggregated numeric features (agg_*)
    "feat_credit_activity": FeatCreditActivityRow,
    "feat_career_annual": FeatCareerAnnualRow,
    "feat_studio_affiliation": FeatStudioAffiliationRow,
    "agg_milestones": AggMilestoneRow,
    "agg_director_circles": AggDirectorCircleRow,
    "agg_person_career": AggPersonCareerRow,
    "agg_person_network": AggPersonNetworkRow,
    # L3: computed features (feat_*)
    "feat_person_scores": FeatPersonScoresRow,
    "feat_network": FeatNetworkRow,
    "feat_network_scores": FeatNetworkScoresRow,
    "feat_career": FeatCareerRow,
    "feat_career_scores": FeatCareerScoresRow,
    "feat_genre_affinity": FeatGenreAffinityRow,
    "feat_contribution": FeatContributionRow,
    "feat_credit_contribution": FeatCreditContributionRow,
    "feat_person_work_summary": FeatPersonWorkSummaryRow,
    "feat_mentorships": FeatMentorshipRow,
    # tables added in v38-v41
    "feat_work_context": FeatWorkContextRow,
    "feat_person_role_progression": FeatPersonRoleProgressionRow,
    "feat_causal_estimates": FeatCausalEstimatesRow,
    "feat_cluster_membership": FeatClusterMembershipRow,
    # v42
    "feat_birank_annual": FeatBirankAnnualRow,
    # v60 L2/L3 split and corrections
    "corrections_credit_year": CorrectionsCreditYearRow,
    "corrections_role": CorrectionsRoleRow,
}
