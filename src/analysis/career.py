"""キャリア分析 — 人物の経歴タイムラインと役職遷移を分析する.

個人のキャリアを時系列で追跡し、
- 活動開始・最新年
- 年ごとの活動量
- 役職の変遷（動画→原画→作画監督 等）
を算出する。
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import NamedTuple

import structlog

from src.models import Anime, Credit, Role

logger = structlog.get_logger()


class RoleProgressionRecord(NamedTuple):
    """役職遷移の記録.

    Records a role held in a specific year with its career stage.
    """

    year: int
    role: str
    stage: int


@dataclass(frozen=True)
class CareerSnapshot:
    """キャリア分析の結果.

    Complete career analysis result for a person.
    """

    first_year: int | None
    latest_year: int | None
    active_years: int
    total_credits: int
    yearly_activity: dict[int, int]
    role_progression: list[RoleProgressionRecord]
    highest_stage: int
    highest_roles: list[str]
    peak_year: int | None = None
    peak_credits: int = 0

    @property
    def career_span_years(self) -> int | None:
        """キャリアの年数スパン (first → latest).

        Years between first and latest activity.
        """
        if self.first_year and self.latest_year:
            return self.latest_year - self.first_year + 1
        return None


# 役職のキャリアステージ順序（低→高）
CAREER_STAGE = {
    Role.IN_BETWEEN: 1,
    Role.SECOND_KEY_ANIMATOR: 2,
    Role.LAYOUT: 2,
    Role.KEY_ANIMATOR: 3,
    Role.EFFECTS: 3,
    Role.ANIMATION_DIRECTOR: 4,
    Role.CHARACTER_DESIGNER: 4,
    Role.STORYBOARD: 4,
    Role.CHIEF_ANIMATION_DIRECTOR: 5,
    Role.EPISODE_DIRECTOR: 5,
    Role.DIRECTOR: 6,
}


def analyze_career(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    *,
    _person_credits: list[Credit] | None = None,
) -> CareerSnapshot:
    """特定人物のキャリアタイムラインを分析する.

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
            role_progression=[],
            highest_stage=0,
            highest_roles=[],
            peak_year=None,
            peak_credits=0,
        )

    credits_per_year: dict[int, int] = defaultdict(int)
    roles_by_year: dict[int, set[Role]] = defaultdict(set)
    years_with_activity = set()

    for credit in person_credit_records:
        anime = anime_map.get(credit.anime_id)
        year = anime.year if anime and anime.year else None
        if year:
            credits_per_year[year] += 1
            roles_by_year[year].add(credit.role)
            years_with_activity.add(year)

    first_year = min(years_with_activity) if years_with_activity else None
    latest_year = max(years_with_activity) if years_with_activity else None
    active_years = len(years_with_activity)

    # 役職遷移: 年ごとの最高ステージの役職を記録
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

    # 最高到達ステージ
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

    return CareerSnapshot(
        first_year=first_year,
        latest_year=latest_year,
        active_years=active_years,
        total_credits=len(person_credit_records),
        yearly_activity=dict(sorted(credits_per_year.items())),
        role_progression=role_progression_records,
        highest_stage=highest_stage,
        highest_roles=highest_roles,
        peak_year=peak_year,
        peak_credits=peak_credit_count,
    )


def batch_career_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_ids: set[str] | None = None,
) -> dict[str, CareerSnapshot]:
    """複数人物のキャリア分析を一括実行する.

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
