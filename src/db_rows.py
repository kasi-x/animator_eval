"""DB テーブルの行を表す typed dataclasses.

各 dataclass はテーブルの全カラムをフィールドとして持つ。
SQLite から取得した raw 値をそのまま保持し、JSON 文字列のパースや
型変換は行わない（それは Pydantic モデルの責務）。

## 設計意図

`sqlite3.Row` の `row["col_name"]` 文字列アクセスをなくし、
`.col_name` 属性アクセスに変換することで:
- IDE の補完が効く
- カラム名リネーム時に型チェッカーが検出できる
- `tests/test_db_schema.py` が PRAGMA と突き合わせてドリフトを検出する

## カラム変更時の手順

1. マイグレーションを追加 (`database.py`)
2. 対応する dataclass フィールドを更新 (`db_rows.py`)
3. Pydantic モデルの `from_db_row()` を更新 (`models.py`)
4. テスト実行 — `test_db_schema.py` が不整合を検出する
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _from_sqlite_row(cls, row) -> object:
    """sqlite3.Row または dict から dataclass インスタンスを生成する.

    テーブルに存在しないフィールドは dataclass のデフォルト値を使用する。
    dataclass に存在しないカラムは無視する（前方互換）。
    """
    field_names = {f.name for f in dataclasses.fields(cls)}
    if hasattr(row, "keys"):
        kwargs = {k: row[k] for k in row.keys() if k in field_names}
    else:
        kwargs = {k: v for k, v in zip(row.keys(), tuple(row)) if k in field_names}
    return cls(**kwargs)


# ---------------------------------------------------------------------------
# persons テーブル
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PersonRow:
    """persons テーブルの 1 行."""

    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: str = "[]"  # JSON 文字列
    mal_id: int | None = None
    anilist_id: int | None = None
    canonical_id: str | None = None
    updated_at: str | None = None
    image_large: str | None = None
    image_medium: str | None = None
    image_large_path: str | None = None
    image_medium_path: str | None = None
    date_of_birth: str | None = None
    age: int | None = None
    gender: str | None = None
    years_active: str = "[]"  # JSON 文字列
    hometown: str | None = None
    blood_type: str | None = None
    description: str | None = None
    favourites: int | None = None
    site_url: str | None = None
    madb_id: str | None = None
    ann_id: int | None = None
    allcinema_id: int | None = None

    @classmethod
    def from_row(cls, row) -> "PersonRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# anime テーブル
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AnimeRow:
    """anime テーブルの 1 行."""

    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    episodes: int | None = None
    mal_id: int | None = None
    anilist_id: int | None = None
    score: float | None = None
    updated_at: str | None = None
    cover_large: str | None = None
    cover_extra_large: str | None = None
    cover_medium: str | None = None
    banner: str | None = None
    cover_large_path: str | None = None
    banner_path: str | None = None
    description: str | None = None
    format: str | None = None
    status: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: int | None = None
    source: str | None = None
    genres: str = "[]"  # JSON 文字列
    tags: str = "[]"  # JSON 文字列
    popularity_rank: int | None = None
    favourites: int | None = None
    studios: str = "[]"  # JSON 文字列
    synonyms: str = "[]"  # JSON 文字列
    mean_score: int | None = None
    country_of_origin: str | None = None
    is_licensed: int | None = None  # SQLite BOOLEAN → 0/1
    is_adult: int | None = None
    hashtag: str | None = None
    site_url: str | None = None
    trailer_url: str | None = None
    trailer_site: str | None = None
    relations_json: str | None = None
    external_links_json: str | None = None
    rankings_json: str | None = None
    madb_id: str | None = None
    ann_id: int | None = None
    allcinema_id: int | None = None
    quarter: int | None = None
    work_type: str | None = None
    scale_class: str | None = None

    @classmethod
    def from_row(cls, row) -> "AnimeRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# credits テーブル
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CreditRow:
    """credits テーブルの 1 行."""

    id: int
    person_id: str
    anime_id: str
    role: str
    raw_role: str = ""
    episode: int = -1
    source: str = ""
    updated_at: str | None = None
    credit_year: int | None = None
    credit_quarter: int | None = None

    @classmethod
    def from_row(cls, row) -> "CreditRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# scores テーブル
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ScoreRow:
    """scores テーブルの 1 行."""

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
# score_history テーブル
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ScoreHistoryRow:
    """score_history テーブルの 1 行."""

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
# character_voice_actors テーブル
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class VARow:
    """character_voice_actors テーブルの 1 行."""

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
# feat_person_scores テーブル (L3: アルゴリズムスコア)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatPersonScoresRow:
    """feat_person_scores テーブルの 1 行."""

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
# feat_network テーブル (L3: ネットワーク指標)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatNetworkRow:
    """feat_network テーブルの 1 行."""

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
# feat_career テーブル (L2/L3 混在: 将来分離予定)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCareerRow:
    """feat_career テーブルの 1 行."""

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
# feat_genre_affinity テーブル (L3: ジャンル親和性スコア)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatGenreAffinityRow:
    """feat_genre_affinity テーブルの 1 行 (PK: person_id + genre)."""

    person_id: str
    genre: str
    run_id: int | None = None
    affinity_score: float | None = None
    work_count: int | None = None

    @classmethod
    def from_row(cls, row) -> "FeatGenreAffinityRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_contribution テーブル (L3: 個人貢献プロファイル Layer 2)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatContributionRow:
    """feat_contribution テーブルの 1 行."""

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
# feat_credit_activity テーブル (L2: クレジット活動パターン)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCreditActivityRow:
    """feat_credit_activity テーブルの 1 行."""

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
# feat_career_annual テーブル (L2: キャリア年×職種別集計)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCareerAnnualRow:
    """feat_career_annual テーブルの 1 行 (PK: person_id + career_year)."""

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
# feat_studio_affiliation テーブル (L2: スタジオ所属年別集計)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatStudioAffiliationRow:
    """feat_studio_affiliation テーブルの 1 行 (PK: person_id + credit_year + studio_id)."""

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
# feat_credit_contribution テーブル (L3: 作品×役職ごとのスコア貢献推定)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCreditContributionRow:
    """feat_credit_contribution テーブルの 1 行 (PK: person_id + anime_id + role)."""

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
# feat_person_work_summary テーブル (L3: 作品貢献集計)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatPersonWorkSummaryRow:
    """feat_person_work_summary テーブルの 1 行."""

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
# agg_milestones テーブル (L2: キャリアイベント)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AggMilestoneRow:
    """agg_milestones テーブルの 1 行 (PK: person_id + event_type + year + anime_id)."""

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
# agg_director_circles テーブル (L2: 共同クレジット集計)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AggDirectorCircleRow:
    """agg_director_circles テーブルの 1 行 (PK: person_id + director_id)."""

    person_id: str
    director_id: str = ""
    shared_works: int = 0
    hit_rate: float | None = None
    roles: str = "[]"  # JSON 文字列
    latest_year: int | None = None

    @classmethod
    def from_row(cls, row) -> "AggDirectorCircleRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# feat_mentorships テーブル (L3: メンター推定)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatMentorshipRow:
    """feat_mentorships テーブルの 1 行 (PK: mentor_id + mentee_id)."""

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
# feat_cluster_membership テーブル (L3: クラスタリング帰属)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatClusterMembershipRow:
    """feat_cluster_membership テーブルの 1 行 (PK: person_id)."""

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
# feat_causal_estimates テーブル (L3: 因果推論スコア)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatCausalEstimatesRow:
    """feat_causal_estimates テーブルの 1 行 (PK: person_id)."""

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
# feat_work_context テーブル (L2: 作品コンテキスト)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatWorkContextRow:
    """feat_work_context テーブルの 1 行 (PK: anime_id + credit_year)."""

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
# feat_person_role_progression テーブル (L2: ロール進行)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatPersonRoleProgressionRow:
    """feat_person_role_progression テーブルの 1 行 (PK: person_id + role_category)."""

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
# feat_birank_annual テーブル (L3: 年次BiRankスナップショット)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FeatBirankAnnualRow:
    """feat_birank_annual テーブルの 1 行 (PK: person_id + year)."""

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
# テーブル名 → Row クラスの対応表 (スキーマテスト用)
# ---------------------------------------------------------------------------

TABLE_ROW_MAP: dict[str, type] = {
    # L1: 生データ
    "persons": PersonRow,
    "anime": AnimeRow,
    "credits": CreditRow,
    "scores": ScoreRow,
    "score_history": ScoreHistoryRow,
    "character_voice_actors": VARow,
    # L2: 集約数値 (agg_*)
    "feat_credit_activity": FeatCreditActivityRow,
    "feat_career_annual": FeatCareerAnnualRow,
    "feat_studio_affiliation": FeatStudioAffiliationRow,
    "agg_milestones": AggMilestoneRow,
    "agg_director_circles": AggDirectorCircleRow,
    # L3: 独自計算 (feat_*)
    "feat_person_scores": FeatPersonScoresRow,
    "feat_network": FeatNetworkRow,
    "feat_career": FeatCareerRow,
    "feat_genre_affinity": FeatGenreAffinityRow,
    "feat_contribution": FeatContributionRow,
    "feat_credit_contribution": FeatCreditContributionRow,
    "feat_person_work_summary": FeatPersonWorkSummaryRow,
    "feat_mentorships": FeatMentorshipRow,
    # v38-v41 追加テーブル
    "feat_work_context": FeatWorkContextRow,
    "feat_person_role_progression": FeatPersonRoleProgressionRow,
    "feat_causal_estimates": FeatCausalEstimatesRow,
    "feat_cluster_membership": FeatClusterMembershipRow,
    # v42
    "feat_birank_annual": FeatBirankAnnualRow,
}
