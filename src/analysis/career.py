"""Career analysis — analyse the career timeline and role transitions of each person.

個人のキャリアを時系列で追跡し、
- 活動開始・最新年
- 年ごとの活動量
- 役職の変遷（動画→原画→作画監督 等）
を算出する。
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import NamedTuple

import structlog

from src.models import AnimeAnalysis as Anime, Credit, Role
from src.utils.role_groups import CAREER_STAGE
from src.utils.time_utils import get_year_quarter, yq_label

logger = structlog.get_logger()


class RoleProgressionRecord(NamedTuple):
    """Role transition record.

    Records a role held in a specific year with its career stage.
    """

    year: int
    role: str
    stage: int


@dataclass(frozen=True)
class CareerSnapshot:
    """Career analysis result.

    Complete career analysis result for a person.
    """

    first_year: int | None
    latest_year: int | None
    active_years: int
    total_credits: int
    yearly_activity: dict[int, int]
    quarterly_activity: dict[str, int]  # "2020-Q1" → credit count
    role_progression: list[RoleProgressionRecord]
    highest_stage: int
    highest_roles: list[str]
    peak_year: int | None = None
    peak_credits: int = 0
    first_quarter: str | None = None  # "2020-Q2"
    latest_quarter: str | None = None
    # distribution by work-type × scale (e.g. {"tv_large": 5, "tv_medium": 12, "tanpatsu_small": 3})
    scale_profile: dict[str, int] = field(default_factory=dict)

    @property
    def career_span_years(self) -> int | None:
        """Career span in years (first → latest).

        Years between first and latest activity.
        """
        if self.first_year and self.latest_year:
            return self.latest_year - self.first_year + 1
        return None


def analyze_career(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    *,
    _person_credits: list[Credit] | None = None,
) -> CareerSnapshot:
    """Analyse the career timeline of a specific person.

    Analyzes a person's career timeline including activity patterns and role progression.

    Args:
        person_id: 人物ID / Person ID to analyze
        credits: 全クレジット / All credit records
        anime_map: anime_id → Anime
        _person_credits: Pre-filtered credits for this person (batch optimization)

    Returns:
        CareerSnapshot with complete career analysis
    """
    person_credit_records = (
        _person_credits
        if _person_credits is not None
        else [credit for credit in credits if credit.person_id == person_id]
    )
    if not person_credit_records:
        return CareerSnapshot(
            first_year=None,
            latest_year=None,
            active_years=0,
            total_credits=0,
            yearly_activity={},
            quarterly_activity={},
            role_progression=[],
            highest_stage=0,
            highest_roles=[],
            peak_year=None,
            peak_credits=0,
            first_quarter=None,
            latest_quarter=None,
        )

    credits_per_year: dict[int, int] = defaultdict(int)
    credits_per_quarter: dict[str, int] = defaultdict(int)
    roles_by_year: dict[int, set[Role]] = defaultdict(set)
    years_with_activity = set()
    scale_counter: dict[str, int] = defaultdict(int)

    for credit in person_credit_records:
        anime = anime_map.get(credit.anime_id)
        year = anime.year if anime and anime.year else None
        if year:
            credits_per_year[year] += 1
            roles_by_year[year].add(credit.role)
            years_with_activity.add(year)
        yq = get_year_quarter(anime) if anime else None
        if yq:
            credits_per_quarter[yq_label(*yq)] += 1
        if anime and anime.work_type and anime.scale_class:
            scale_counter[f"{anime.work_type}_{anime.scale_class}"] += 1

    first_year = min(years_with_activity) if years_with_activity else None
    latest_year = max(years_with_activity) if years_with_activity else None
    active_years = len(years_with_activity)

    # role transitions: record highest-stage role per year
    role_progression_records = []
    for year in sorted(roles_by_year):
        roles_in_year = roles_by_year[year]
        highest_ranking_role = max(
            roles_in_year, key=lambda role: CAREER_STAGE.get(role, 0)
        )
        record = RoleProgressionRecord(
            year=year,
            role=highest_ranking_role.value,
            stage=CAREER_STAGE.get(highest_ranking_role, 0),
        )
        role_progression_records.append(record)

    # peak stage reached
    all_career_stages = [
        CAREER_STAGE.get(credit.role, 0) for credit in person_credit_records
    ]
    highest_stage = max(all_career_stages) if all_career_stages else 0
    highest_roles = sorted(
        {
            credit.role.value
            for credit in person_credit_records
            if CAREER_STAGE.get(credit.role, 0) == highest_stage
        }
    )

    # Peak year (most credits)
    peak_year = None
    peak_credit_count = 0
    for year, credit_count in credits_per_year.items():
        if credit_count > peak_credit_count:
            peak_credit_count = credit_count
            peak_year = year

    sorted_quarters = sorted(credits_per_quarter.keys())
    first_q = sorted_quarters[0] if sorted_quarters else None
    latest_q = sorted_quarters[-1] if sorted_quarters else None

    return CareerSnapshot(
        first_year=first_year,
        latest_year=latest_year,
        active_years=active_years,
        total_credits=len(person_credit_records),
        yearly_activity=dict(sorted(credits_per_year.items())),
        quarterly_activity=dict(sorted(credits_per_quarter.items())),
        role_progression=role_progression_records,
        highest_stage=highest_stage,
        highest_roles=highest_roles,
        peak_year=peak_year,
        peak_credits=peak_credit_count,
        first_quarter=first_q,
        latest_quarter=latest_q,
        scale_profile=dict(scale_counter),
    )


def batch_career_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_ids: set[str] | None = None,
) -> dict[str, CareerSnapshot]:
    """Run career analysis for multiple persons in batch.

    Performs career analysis for multiple people in batch.

    Args:
        credits: 全クレジット / All credit records
        anime_map: anime_id → Anime
        person_ids: 対象人物ID / Target person IDs (None = all)

    Returns:
        {person_id: CareerSnapshot}
    """
    # Pre-group credits by person_id: O(m) instead of O(n*m)
    credits_by_person: dict[str, list[Credit]] = defaultdict(list)
    for credit in credits:
        credits_by_person[credit.person_id].append(credit)

    if person_ids is None:
        person_ids = set(credits_by_person.keys())

    career_snapshots = {}
    for person_id in person_ids:
        career_snapshots[person_id] = analyze_career(
            person_id,
            credits,
            anime_map,
            _person_credits=credits_by_person.get(person_id, []),
        )

    logger.info("career_analysis_complete", persons=len(career_snapshots))
    return career_snapshots


# ---------------------------------------------------------------------------
# ordered scale-category keys (for display and sorting)
# ---------------------------------------------------------------------------
SCALE_KEYS_ORDERED = [
    "tv_large",
    "tv_medium",
    "tv_small",
    "tanpatsu_large",
    "tanpatsu_medium",
    "tanpatsu_small",
]

SCALE_KEY_LABELS: dict[str, str] = {
    "tv_large": "TV大",
    "tv_medium": "TV中",
    "tv_small": "TV小",
    "tanpatsu_large": "単発大",
    "tanpatsu_medium": "単発中",
    "tanpatsu_small": "単発小",
}


@dataclass
class DirectorScaleProfile:
    """Work-type × scale profile for a single director."""

    person_id: str
    name: str
    total_director_credits: int
    scale_counts: dict[str, int]  # {"tv_large": n, ...}
    scale_fractions: dict[str, float]  # 0-1, 合計 ≤ 1 (未分類を除く)
    dominant_type: str  # 最多カテゴリキー ("tv_large" 等)
    career_span: int | None  # first_year → latest_year
    first_year: int | None
    latest_year: int | None


def compute_director_scale_profiles(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_name_map: dict[str, str],
    *,
    director_roles: set[str] | None = None,
    min_credits: int = 3,
) -> list[DirectorScaleProfile]:
    """Compute work-type × scale profiles for persons with director roles.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_name_map: person_id → 表示名
        director_roles: 監督と見なすロール集合 (None = {'director'})
        min_credits: 最低監督クレジット数（これ未満は除外）

    Returns:
        DirectorScaleProfile のリスト（total_director_credits 降順）
    """
    if director_roles is None:
        director_roles = {"director"}

    # Extract credits for the director role only
    dir_credits_by_person: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        if c.role.value in director_roles:
            dir_credits_by_person[c.person_id].append(c)

    profiles = []
    for person_id, pcredits in dir_credits_by_person.items():
        if len(pcredits) < min_credits:
            continue

        scale_counts: dict[str, int] = defaultdict(int)
        years = []
        for c in pcredits:
            anime = anime_map.get(c.anime_id)
            if not anime:
                continue
            if anime.work_type and anime.scale_class:
                scale_counts[f"{anime.work_type}_{anime.scale_class}"] += 1
            if anime.year:
                years.append(anime.year)

        total_classified = sum(scale_counts.values())
        scale_fractions = (
            {k: v / total_classified for k, v in scale_counts.items()}
            if total_classified > 0
            else {}
        )
        dominant_type = (
            max(scale_counts, key=lambda k: scale_counts[k])
            if scale_counts
            else "unknown"
        )

        profiles.append(
            DirectorScaleProfile(
                person_id=person_id,
                name=person_name_map.get(person_id, person_id),
                total_director_credits=len(pcredits),
                scale_counts=dict(scale_counts),
                scale_fractions=scale_fractions,
                dominant_type=dominant_type,
                career_span=(max(years) - min(years) + 1) if years else None,
                first_year=min(years) if years else None,
                latest_year=max(years) if years else None,
            )
        )

    profiles.sort(key=lambda p: p.total_director_credits, reverse=True)
    logger.info("director_scale_profiles", directors=len(profiles))
    return profiles


# ---------------------------------------------------------------------------
# director career trajectory + cross-scale mobility
# ---------------------------------------------------------------------------


@dataclass
class DirectorTrajectoryResult:
    """Aggregated result for director career trajectories and cross-scale transitions."""

    # years-to-first-director credit by scale category (from first any-role credit)
    years_to_first_dir_by_scale: dict[str, list[int]]

    # cross-scale transition matrix: (from_scale, to_scale) -> count (consecutive credits within 3 years)
    transition_counts: dict[tuple[str, str], int]

    # normalised transition probability: (from_scale, to_scale) -> 0.0-1.0
    transition_probs: dict[tuple[str, str], float]

    # total number of directors
    n_directors: int


def compute_director_trajectories(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    *,
    director_roles: set[str] | None = None,
    min_director_credits: int = 3,
    max_gap_years: int = 3,
) -> DirectorTrajectoryResult:
    """Aggregate director career trajectories and cross-scale mobility.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        director_roles: 監督ロール集合 (None → {"director"})
        min_director_credits: 最低監督クレジット数
        max_gap_years: 連続とみなす最大年間ギャップ

    Returns:
        DirectorTrajectoryResult
    """
    if director_roles is None:
        director_roles = {"director"}

    # person_id → all credits (any role, with year)
    all_credits_by_person: dict[str, list[Credit]] = defaultdict(list)
    dir_credits_by_person: dict[str, list[Credit]] = defaultdict(list)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        all_credits_by_person[c.person_id].append(c)
        if c.role.value in director_roles:
            dir_credits_by_person[c.person_id].append(c)

    # minimum credit count filter
    director_ids = {
        pid
        for pid, crds in dir_credits_by_person.items()
        if len(crds) >= min_director_credits
    }

    years_to_first: dict[str, list[int]] = defaultdict(list)
    transition_counts: dict[tuple[str, str], int] = defaultdict(int)

    for pid in director_ids:
        all_years = [
            anime_map[c.anime_id].year
            for c in all_credits_by_person[pid]
            if anime_map.get(c.anime_id) and anime_map[c.anime_id].year
        ]
        if not all_years:
            continue
        first_any_year = min(all_years)

        dir_recs = dir_credits_by_person[pid]

        # years to first director credit by scale
        for scale_key in SCALE_KEYS_ORDERED:
            wt, sc = scale_key.split("_", 1)
            scale_dir = [
                c
                for c in dir_recs
                if (a := anime_map.get(c.anime_id))
                and a.work_type == wt
                and a.scale_class == sc
                and a.year
            ]
            if scale_dir:
                first_scale_yr = min(anime_map[c.anime_id].year for c in scale_dir)  # type: ignore[arg-type]
                years_to_first[scale_key].append(first_scale_yr - first_any_year)

        # cross-scale mobility: dominant scale per year → transitions
        year_to_scales: dict[int, list[str]] = defaultdict(list)
        for c in dir_recs:
            anime = anime_map.get(c.anime_id)
            if anime and anime.year and anime.work_type and anime.scale_class:
                year_to_scales[anime.year].append(
                    f"{anime.work_type}_{anime.scale_class}"
                )

        if len(year_to_scales) < 2:
            continue

        sorted_years = sorted(year_to_scales.keys())
        year_dominant = [
            (yr, max(set(year_to_scales[yr]), key=year_to_scales[yr].count))
            for yr in sorted_years
        ]
        for i in range(len(year_dominant) - 1):
            yr_from, sc_from = year_dominant[i]
            yr_to, sc_to = year_dominant[i + 1]
            if yr_to - yr_from <= max_gap_years:
                transition_counts[(sc_from, sc_to)] += 1

    # compute transition probabilities via row normalisation
    transition_probs: dict[tuple[str, str], float] = {}
    for sc_from in SCALE_KEYS_ORDERED:
        row_total = sum(
            transition_counts.get((sc_from, sc_to), 0) for sc_to in SCALE_KEYS_ORDERED
        )
        if row_total > 0:
            for sc_to in SCALE_KEYS_ORDERED:
                cnt = transition_counts.get((sc_from, sc_to), 0)
                transition_probs[(sc_from, sc_to)] = cnt / row_total

    logger.info(
        "director_trajectories_computed",
        n_directors=len(director_ids),
        scale_keys=len(years_to_first),
    )
    return DirectorTrajectoryResult(
        years_to_first_dir_by_scale=dict(years_to_first),
        transition_counts=dict(transition_counts),
        transition_probs=transition_probs,
        n_directors=len(director_ids),
    )
