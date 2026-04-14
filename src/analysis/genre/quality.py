"""Genre Quality Metrics — prestige, saturation, and talent mobility.

All metrics use structural data only (person FE, BiRank, production scale).
No viewer ratings (anime.score) are used in scoring.
"""

import math
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


@dataclass
class GenreQuality:
    """Quality metrics for a single genre.

    Attributes:
        genre: genre name
        staff_quality: mean person FE for staff with ≥2 credits in genre
        avg_birank: mean BiRank of staff in this genre
        avg_production_scale: mean production scale (staff_count)
        prestige: composite prestige score
        n_staff: number of qualifying staff
    """

    genre: str = ""
    staff_quality: float = 0.0
    avg_birank: float = 0.0
    avg_production_scale: float = 0.0
    prestige: float = 0.0
    n_staff: int = 0


@dataclass
class GenreSaturation:
    """Saturation detection for a genre.

    Attributes:
        genre: genre name
        anime_count_trend: OLS slope of anime count over time
        supply_demand_ratio_trend: OLS slope of staff/anime ratio
        newcomer_fe_trend: OLS slope of newcomer avg FE
        is_saturated: whether saturation conditions are met
    """

    genre: str = ""
    anime_count_trend: float = 0.0
    supply_demand_ratio_trend: float = 0.0
    newcomer_fe_trend: float = 0.0
    is_saturated: bool = False


@dataclass
class GenreMobility:
    """Genre-level talent mobility metrics.

    Attributes:
        genre: genre name
        transition_matrix_row: target_genre → transition probability
        stickiness: P(staying in same genre) = diagonal element
        entropy: Shannon entropy of transition distribution
        loyalty: max(credits_in_g / total_credits) averaged over staff
    """

    genre: str = ""
    transition_matrix_row: dict[str, float] = field(default_factory=dict)
    stickiness: float = 0.0
    entropy: float = 0.0
    loyalty: float = 0.0


@dataclass
class GenreQualityResult:
    """Combined genre quality analysis.

    Attributes:
        quality: genre → GenreQuality
        saturation: genre → GenreSaturation
        mobility: genre → GenreMobility
    """

    quality: dict[str, GenreQuality] = field(default_factory=dict)
    saturation: dict[str, GenreSaturation] = field(default_factory=dict)
    mobility: dict[str, GenreMobility] = field(default_factory=dict)


def _ols_slope(x: np.ndarray, y: np.ndarray) -> float:
    """Simple OLS slope coefficient."""
    if len(x) < 2:
        return 0.0
    x_mean = np.mean(x)
    denom = np.sum((x - x_mean) ** 2)
    if denom < 1e-10:
        return 0.0
    return float(np.sum((x - x_mean) * (y - np.mean(y))) / denom)


def _normalize_0_1(values: list[float]) -> list[float]:
    """Min-max normalize to [0, 1]."""
    if not values:
        return []
    mn, mx = min(values), max(values)
    rng = mx - mn
    if rng < 1e-10:
        return [0.5] * len(values)
    return [(v - mn) / rng for v in values]


def compute_genre_quality(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    birank_scores: dict[str, float] | None = None,
    current_year: int = 2026,
) -> GenreQualityResult:
    """Compute genre quality, saturation, and mobility metrics.

    Args:
        credits: all production credits
        anime_map: anime_id → Anime
        person_fe: person_id → person fixed effect
        birank_scores: person_id → BiRank (optional)
        current_year: reference year

    Returns:
        GenreQualityResult
    """
    birank = birank_scores or {}

    # Build genre → person credits mapping
    genre_person_credits: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    genre_anime_years: dict[str, dict[int, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    anime_staff_count: dict[str, int] = defaultdict(int)
    person_anime_genres: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)

    # Staff count per anime
    _seen_pa: set[tuple[str, str]] = set()
    for c in credits:
        key = (c.person_id, c.anime_id)
        if key not in _seen_pa:
            _seen_pa.add(key)
            anime_staff_count[c.anime_id] += 1

    # Person first year for newcomer detection
    person_first_year: dict[str, int] = {}
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            if (
                c.person_id not in person_first_year
                or anime.year < person_first_year[c.person_id]
            ):
                person_first_year[c.person_id] = anime.year

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.genres:
            continue
        for genre in anime.genres:
            genre_person_credits[genre][c.person_id] += 1
            if anime.year:
                genre_anime_years[genre][anime.year].add(anime.id)
        if anime.year:
            person_anime_genres[c.person_id].append((c.anime_id, anime.genres))

    # 1. Genre Quality
    quality: dict[str, GenreQuality] = {}
    for genre, person_counts in genre_person_credits.items():
        # Staff with ≥2 credits in this genre
        qualifying = [pid for pid, cnt in person_counts.items() if cnt >= 2]
        if len(qualifying) < 5:
            continue

        fes = [person_fe.get(pid, 0.0) for pid in qualifying]
        biranks = [birank.get(pid, 0.0) for pid in qualifying]

        # Production scale: average staff count of anime in this genre
        genre_anime = set()
        for yr_anime in genre_anime_years[genre].values():
            genre_anime.update(yr_anime)
        scales = [anime_staff_count.get(aid, 0) for aid in genre_anime]

        quality[genre] = GenreQuality(
            genre=genre,
            staff_quality=float(np.mean(fes)),
            avg_birank=float(np.mean(biranks)),
            avg_production_scale=float(np.mean(scales)) if scales else 0.0,
            n_staff=len(qualifying),
        )

    # Compute prestige (normalized composite)
    if quality:
        all_sq = [q.staff_quality for q in quality.values()]
        all_br = [q.avg_birank for q in quality.values()]
        all_ps = [q.avg_production_scale for q in quality.values()]
        norm_sq = _normalize_0_1(all_sq)
        norm_br = _normalize_0_1(all_br)
        norm_ps = _normalize_0_1(all_ps)

        for i, (genre, q) in enumerate(quality.items()):
            q.prestige = 0.4 * norm_sq[i] + 0.3 * norm_br[i] + 0.3 * norm_ps[i]

    # 2. Saturation Detection
    # Pre-build genre → year → set[person_id] index (O(credits), not O(genres×persons×credits))
    genre_year_staff: dict[str, dict[int, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or not anime.genres:
            continue
        for g in anime.genres:
            genre_year_staff[g][anime.year].add(c.person_id)

    saturation: dict[str, GenreSaturation] = {}
    for genre, year_anime in genre_anime_years.items():
        years = sorted(year_anime.keys())
        if len(years) < 5:
            continue

        counts = np.array([len(year_anime[y]) for y in years], dtype=np.float64)
        x = np.array(years, dtype=np.float64)
        anime_slope = _ols_slope(x, counts)

        # Staff/anime ratio per year — use pre-built index
        gys = genre_year_staff.get(genre, {})
        ratios = []
        ratio_years = []
        for y in years:
            n_anime = len(year_anime[y])
            n_staff = len(gys.get(y, set()))
            if n_anime > 0:
                ratios.append(n_staff / n_anime)
                ratio_years.append(y)
        ratio_slope = (
            _ols_slope(
                np.array(ratio_years, dtype=np.float64),
                np.array(ratios, dtype=np.float64),
            )
            if len(ratios) >= 3
            else 0.0
        )

        # Newcomer FE trend
        newcomer_fe_by_year: dict[int, list[float]] = defaultdict(list)
        for pid, cnt in genre_person_credits[genre].items():
            fy = person_first_year.get(pid)
            if fy and fy in year_anime:
                fe = person_fe.get(pid)
                if fe is not None:
                    newcomer_fe_by_year[fy].append(fe)
        nc_years = sorted(newcomer_fe_by_year.keys())
        nc_fes = [float(np.mean(newcomer_fe_by_year[y])) for y in nc_years]
        newcomer_slope = (
            _ols_slope(
                np.array(nc_years, dtype=np.float64),
                np.array(nc_fes, dtype=np.float64),
            )
            if len(nc_fes) >= 3
            else 0.0
        )

        is_saturated = anime_slope > 0.3 and ratio_slope < -0.1 and newcomer_slope < 0

        saturation[genre] = GenreSaturation(
            genre=genre,
            anime_count_trend=anime_slope,
            supply_demand_ratio_trend=ratio_slope,
            newcomer_fe_trend=newcomer_slope,
            is_saturated=is_saturated,
        )

    # 3. Genre Mobility
    mobility: dict[str, GenreMobility] = {}

    # Build person → genre credit counts
    person_genre_credits: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.genres:
            for g in anime.genres:
                person_genre_credits[c.person_id][g] += 1

    # Genre transition matrix (consecutive anime)
    person_ordered_genres: dict[str, list[tuple[int, list[str]]]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year and anime.genres:
            person_ordered_genres[c.person_id].append((anime.year, anime.genres))

    genre_transitions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for pid, year_genres_list in person_ordered_genres.items():
        sorted_yg = sorted(year_genres_list, key=lambda x: x[0])
        for i in range(len(sorted_yg) - 1):
            _, genres_before = sorted_yg[i]
            _, genres_after = sorted_yg[i + 1]
            for gb in genres_before:
                for ga in genres_after:
                    genre_transitions[gb][ga] += 1

    for genre, targets in genre_transitions.items():
        total = sum(targets.values())
        if total < 10:
            continue

        probs = {t: c / total for t, c in targets.items()}
        stickiness = probs.get(genre, 0.0)

        # Shannon entropy
        entropy = 0.0
        for p in probs.values():
            if p > 0:
                entropy -= p * math.log2(p)

        # Average loyalty of staff in this genre
        loyalties = []
        for pid in genre_person_credits[genre]:
            total_c = sum(person_genre_credits[pid].values())
            if total_c > 0:
                max_g_c = max(person_genre_credits[pid].values())
                loyalties.append(max_g_c / total_c)

        mobility[genre] = GenreMobility(
            genre=genre,
            transition_matrix_row=probs,
            stickiness=stickiness,
            entropy=entropy,
            loyalty=float(np.mean(loyalties)) if loyalties else 0.0,
        )

    logger.info(
        "genre_quality_computed",
        quality_genres=len(quality),
        saturation_genres=len(saturation),
        mobility_genres=len(mobility),
    )

    return GenreQualityResult(
        quality=quality,
        saturation=saturation,
        mobility=mobility,
    )
