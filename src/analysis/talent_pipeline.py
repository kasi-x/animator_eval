"""Talent Pipeline Analysis — junior development, talent flow, brain drain.

Measures studio capacity for talent development and retention using
structural data only (credit records, career progression).
"""

from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit
from src.utils.role_groups import CAREER_STAGE_BY_VALUE

logger = structlog.get_logger()

# Career stage mapping — single source of truth in role_groups.py
_STAGE_MAP = CAREER_STAGE_BY_VALUE


@dataclass
class JuniorDevScore:
    """Junior development metrics for a studio.

    Attributes:
        studio: studio name
        juniors: number of juniors who debuted at this studio
        promotions: number who reached stage >= 4
        promotion_rate: promotions / juniors
        avg_promotion_speed: average years to reach stage 4
    """

    studio: str = ""
    juniors: int = 0
    promotions: int = 0
    promotion_rate: float = 0.0
    avg_promotion_speed: float = 0.0


@dataclass
class TalentFlowResult:
    """Talent flow analysis result.

    Attributes:
        junior_dev: studio → JuniorDevScore
        flow_matrix: (from_studio, to_studio) → count of movers
        brain_drain_index: studio → net talent value outflow
        retention_rates: studio → 3-year retention rate
    """

    junior_dev: dict[str, JuniorDevScore] = field(default_factory=dict)
    flow_matrix: dict[tuple[str, str], int] = field(default_factory=dict)
    brain_drain_index: dict[str, float] = field(default_factory=dict)
    retention_rates: dict[str, float] = field(default_factory=dict)


def compute_talent_pipeline(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    current_year: int = 2026,
) -> TalentFlowResult:
    """Compute talent pipeline metrics for all studios.

    Args:
        credits: all production credits
        anime_map: anime_id → Anime
        person_fe: person_id → person fixed effect
        current_year: reference year

    Returns:
        TalentFlowResult with junior dev, flow matrix, and brain drain.
    """
    # Build person → {year → set of studios} and career data
    person_year_studios: dict[str, dict[int, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    person_first_year: dict[str, int] = {}
    person_highest_stage: dict[str, int] = {}
    person_first_studio: dict[str, str] = {}

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or not anime.studios:
            continue
        pid = c.person_id
        yr = anime.year

        for studio in anime.studios:
            person_year_studios[pid][yr].add(studio)

        if pid not in person_first_year or yr < person_first_year[pid]:
            person_first_year[pid] = yr
            if anime.studios:
                person_first_studio[pid] = anime.studios[0]

        stage = _STAGE_MAP.get(c.role.value, 0)
        if stage > person_highest_stage.get(pid, 0):
            person_highest_stage[pid] = stage

    # 1. Junior Development Score
    # Pre-build person → credits index (O(credits) once, avoids O(persons × credits))
    person_credits_idx: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        person_credits_idx[c.person_id].append(c)

    # Junior = person whose first credit was at this studio, stage <= 2
    studio_juniors: dict[str, set[str]] = defaultdict(set)
    studio_promoted: dict[str, list[tuple[str, int]]] = defaultdict(list)

    for pid, first_studio in person_first_studio.items():
        fy = person_first_year.get(pid)
        hs = person_highest_stage.get(pid, 0)
        if fy is None:
            continue
        # Consider as junior if started at low stage
        early_stages = [
            _STAGE_MAP.get(c.role.value, 0)
            for c in person_credits_idx[pid]
            if anime_map.get(c.anime_id) and anime_map[c.anime_id].year == fy
        ]
        if early_stages and max(early_stages) <= 2:
            studio_juniors[first_studio].add(pid)
            if hs >= 4:
                ly = max(
                    (
                        anime_map[c.anime_id].year
                        for c in person_credits_idx[pid]
                        if anime_map.get(c.anime_id) and anime_map[c.anime_id].year
                    ),
                    default=current_year,
                )
                years_to_promote = ly - fy
                studio_promoted[first_studio].append((pid, years_to_promote))

    junior_dev: dict[str, JuniorDevScore] = {}
    for studio, juniors in studio_juniors.items():
        promoted = studio_promoted.get(studio, [])
        promotion_speeds = [yrs for _, yrs in promoted]
        junior_dev[studio] = JuniorDevScore(
            studio=studio,
            juniors=len(juniors),
            promotions=len(promoted),
            promotion_rate=len(promoted) / len(juniors) if juniors else 0.0,
            avg_promotion_speed=float(np.mean(promotion_speeds))
            if promotion_speeds
            else 0.0,
        )

    # 2. Talent Flow Matrix
    # Detect studio transitions: person at studio A in year Y, studio B in year Y+1...Y+3
    flow_matrix: dict[tuple[str, str], int] = defaultdict(int)
    for pid, year_studios in person_year_studios.items():
        years = sorted(year_studios.keys())
        for i in range(len(years) - 1):
            studios_before = year_studios[years[i]]
            studios_after = year_studios[years[i + 1]]
            for sb in studios_before:
                for sa in studios_after:
                    if sb != sa:
                        flow_matrix[(sb, sa)] += 1

    # 3. Brain Drain Index
    # Pre-compute per-transition FE sums (O(persons), not O(flow_pairs × persons))
    # Collect (from_studio, to_studio) → list of mover FEs during flow detection
    transition_fes: dict[tuple[str, str], list[float]] = defaultdict(list)
    for pid, year_studios in person_year_studios.items():
        fe = person_fe.get(pid, 0.0)
        years = sorted(year_studios.keys())
        for i in range(len(years) - 1):
            studios_before = year_studios[years[i]]
            studios_after = year_studios[years[i + 1]]
            for sb in studios_before:
                for sa in studios_after:
                    if sb != sa:
                        transition_fes[(sb, sa)].append(fe)

    brain_drain: dict[str, float] = defaultdict(float)
    for (from_s, to_s), fes in transition_fes.items():
        avg_fe = float(np.mean(fes)) if fes else 0.0
        count = len(fes)
        brain_drain[from_s] += count * avg_fe  # outflow
        brain_drain[to_s] -= count * avg_fe  # inflow

    # 4. Retention rates (3-year)
    retention: dict[str, float] = {}
    studio_staff_3yr_ago: dict[str, set[str]] = defaultdict(set)
    studio_staff_recent: dict[str, set[str]] = defaultdict(set)

    for pid, year_studios in person_year_studios.items():
        for yr, studios in year_studios.items():
            for studio in studios:
                if current_year - 6 <= yr <= current_year - 3:
                    studio_staff_3yr_ago[studio].add(pid)
                if yr >= current_year - 3:
                    studio_staff_recent[studio].add(pid)

    for studio, old_staff in studio_staff_3yr_ago.items():
        if not old_staff:
            continue
        retained = old_staff & studio_staff_recent.get(studio, set())
        retention[studio] = len(retained) / len(old_staff)

    logger.info(
        "talent_pipeline_computed",
        studios_with_juniors=len(junior_dev),
        flow_pairs=len(flow_matrix),
        studios_with_drain=len(brain_drain),
    )

    return TalentFlowResult(
        junior_dev=junior_dev,
        flow_matrix=dict(flow_matrix),
        brain_drain_index=dict(brain_drain),
        retention_rates=retention,
    )
