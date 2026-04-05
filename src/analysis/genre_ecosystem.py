"""Genre Ecosystem Analysis — production trends, staffing density, seasonality, career paths.

All metrics use structural data only (credit counts, role progression,
staff counts). No viewer ratings (anime.score) are used.
"""

from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog

from src.models import Anime, Credit
from src.utils.role_groups import CAREER_STAGE_BY_VALUE as _STAGE_MAP

logger = structlog.get_logger()


@dataclass
class GenreTrend:
    """Production trend for a single genre.

    Attributes:
        genre: genre name
        yearly_counts: year → anime count
        total_anime: total anime in this genre
        ols_slope: linear trend slope (anime/year)
        recent_momentum: average count in last 5 years / overall average
        trend_class: classification string
    """

    genre: str = ""
    yearly_counts: dict[int, int] = field(default_factory=dict)
    total_anime: int = 0
    ols_slope: float = 0.0
    recent_momentum: float = 0.0
    trend_class: str = "stable"


@dataclass
class GenreStaffingDensity:
    """Staffing density metrics for a single genre per year.

    Attributes:
        genre: genre name
        yearly_density: year → unique_staff / anime_count
        avg_density: average staffing density across years
        trend_slope: OLS slope of density over time
    """

    genre: str = ""
    yearly_density: dict[int, float] = field(default_factory=dict)
    avg_density: float = 0.0
    trend_slope: float = 0.0


@dataclass
class GenreSeasonality:
    """Seasonal pattern for a single genre.

    Attributes:
        genre: genre name
        seasonal_index: season → index (>1 = overrepresented)
        peak_season: season with highest index
    """

    genre: str = ""
    seasonal_index: dict[str, float] = field(default_factory=dict)
    peak_season: str = ""


@dataclass
class GenreCareerProfile:
    """Career progression profile within a genre.

    Attributes:
        genre: genre name
        avg_years_to_stage_4: average years to reach AD level
        promotion_rate: fraction reaching stage >= 4
        retention_rate: fraction active in last 3 years
        unique_staff: total unique staff in this genre
    """

    genre: str = ""
    avg_years_to_stage_4: float = 0.0
    promotion_rate: float = 0.0
    retention_rate: float = 0.0
    unique_staff: int = 0


@dataclass
class GenreEcosystemResult:
    """Combined genre ecosystem analysis result.

    Attributes:
        trends: genre → GenreTrend
        staffing: genre → GenreStaffingDensity
        seasonality: genre → GenreSeasonality
        careers: genre → GenreCareerProfile
    """

    trends: dict[str, GenreTrend] = field(default_factory=dict)
    staffing: dict[str, GenreStaffingDensity] = field(default_factory=dict)
    seasonality: dict[str, GenreSeasonality] = field(default_factory=dict)
    careers: dict[str, GenreCareerProfile] = field(default_factory=dict)


_SEASONS = ["winter", "spring", "summer", "fall"]


def _classify_trend(slope: float, momentum: float) -> str:
    """Classify genre production trend."""
    if slope > 0.5 and momentum > 1.2:
        return "accelerating_growth"
    if slope > 0.2:
        return "decelerating_growth" if momentum < 1.0 else "steady_growth"
    if slope < -0.5 and momentum < 0.8:
        return "accelerating_decline"
    if slope < -0.2:
        return "recovering" if momentum > 1.0 else "declining"
    return "stable"


def _ols_slope(x: np.ndarray, y: np.ndarray) -> float:
    """Simple OLS slope coefficient."""
    if len(x) < 2:
        return 0.0
    x_mean = np.mean(x)
    y_mean = np.mean(y)
    denom = np.sum((x - x_mean) ** 2)
    if denom < 1e-10:
        return 0.0
    return float(np.sum((x - x_mean) * (y - y_mean)) / denom)


def compute_genre_ecosystem(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int = 2026,
) -> GenreEcosystemResult:
    """Compute comprehensive genre ecosystem analysis.

    Args:
        credits: all production credits
        anime_map: anime_id → Anime
        current_year: reference year for recency calculations

    Returns:
        GenreEcosystemResult with trends, staffing, seasonality, and careers.
    """
    # Build genre → anime mapping
    genre_anime_years: dict[str, dict[int, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    genre_anime_seasons: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    total_season_counts: dict[str, int] = defaultdict(int)
    total_year_count = 0

    for anime in anime_map.values():
        if not anime.year or not anime.genres:
            continue
        for genre in anime.genres:
            genre_anime_years[genre][anime.year].add(anime.id)
            if anime.season and anime.season in _SEASONS:
                genre_anime_seasons[genre][anime.season] += 1
        if anime.season and anime.season in _SEASONS:
            total_season_counts[anime.season] += 1
        total_year_count += 1

    # Build genre → person → credit data
    genre_persons: dict[str, dict[str, list[Credit]]] = defaultdict(
        lambda: defaultdict(list)
    )
    person_first_year: dict[str, int] = {}
    person_last_year: dict[str, int] = {}
    person_highest_stage: dict[str, int] = {}

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue

        # Track career data
        pid = c.person_id
        yr = anime.year
        if pid not in person_first_year or yr < person_first_year[pid]:
            person_first_year[pid] = yr
        if pid not in person_last_year or yr > person_last_year[pid]:
            person_last_year[pid] = yr
        stage = _STAGE_MAP.get(c.role.value, 0)
        if stage > person_highest_stage.get(pid, 0):
            person_highest_stage[pid] = stage

        if not anime.genres:
            continue
        for genre in anime.genres:
            genre_persons[genre][pid].append(c)

    # 1. Production trends
    trends: dict[str, GenreTrend] = {}
    for genre, year_anime in genre_anime_years.items():
        if not year_anime:
            continue
        years = sorted(year_anime.keys())
        counts = [len(year_anime[y]) for y in years]
        x = np.array(years, dtype=np.float64)
        y = np.array(counts, dtype=np.float64)
        slope = _ols_slope(x, y)

        # Recent momentum (last 5 years vs overall)
        overall_avg = float(np.mean(y)) if len(y) > 0 else 0.0
        recent_years = [yr for yr in years if yr >= current_year - 5]
        recent_counts = [len(year_anime[yr]) for yr in recent_years]
        recent_avg = float(np.mean(recent_counts)) if recent_counts else 0.0
        momentum = recent_avg / overall_avg if overall_avg > 0 else 1.0

        trends[genre] = GenreTrend(
            genre=genre,
            yearly_counts={y: len(aids) for y, aids in year_anime.items()},
            total_anime=sum(len(aids) for aids in year_anime.values()),
            ols_slope=slope,
            recent_momentum=momentum,
            trend_class=_classify_trend(slope, momentum),
        )

    # 2. Staffing density
    staffing: dict[str, GenreStaffingDensity] = {}
    for genre in genre_anime_years:
        # Year → unique staff count
        genre_year_staff: dict[int, set[str]] = defaultdict(set)
        for pid, pcredits in genre_persons.get(genre, {}).items():
            for c in pcredits:
                anime = anime_map.get(c.anime_id)
                if anime and anime.year:
                    genre_year_staff[anime.year].add(pid)

        yearly_density: dict[int, float] = {}
        for yr, anime_ids in genre_anime_years[genre].items():
            n_anime = len(anime_ids)
            n_staff = len(genre_year_staff.get(yr, set()))
            yearly_density[yr] = n_staff / n_anime if n_anime > 0 else 0.0

        if yearly_density:
            years = sorted(yearly_density.keys())
            densities = [yearly_density[y] for y in years]
            staffing[genre] = GenreStaffingDensity(
                genre=genre,
                yearly_density=yearly_density,
                avg_density=float(np.mean(densities)),
                trend_slope=_ols_slope(
                    np.array(years, dtype=np.float64),
                    np.array(densities, dtype=np.float64),
                ),
            )

    # 3. Seasonality
    total_anime = max(total_year_count, 1)
    total_per_season = {s: total_season_counts.get(s, 0) for s in _SEASONS}
    seasonality: dict[str, GenreSeasonality] = {}

    for genre, season_counts in genre_anime_seasons.items():
        genre_total = sum(season_counts.values())
        if genre_total < 5:
            continue

        seasonal_index: dict[str, float] = {}
        for s in _SEASONS:
            p_genre_season = season_counts.get(s, 0) / genre_total
            p_season = total_per_season.get(s, 0) / total_anime
            seasonal_index[s] = p_genre_season / p_season if p_season > 0 else 1.0

        peak = max(seasonal_index, key=seasonal_index.get)  # type: ignore[arg-type]
        seasonality[genre] = GenreSeasonality(
            genre=genre,
            seasonal_index=seasonal_index,
            peak_season=peak,
        )

    # 4. Career profiles
    careers: dict[str, GenreCareerProfile] = {}
    for genre, persons in genre_persons.items():
        if len(persons) < 10:
            continue

        years_to_stage4: list[float] = []
        n_promoted = 0
        n_active_recent = 0
        n_total = len(persons)

        for pid in persons:
            hs = person_highest_stage.get(pid, 0)
            fy = person_first_year.get(pid)
            ly = person_last_year.get(pid)

            if hs >= 4:
                n_promoted += 1
                if fy is not None:
                    # Rough estimate: assume stage 4 reached mid-career
                    span = (ly or current_year) - fy
                    years_to_stage4.append(span * 0.6)

            if ly is not None and ly >= current_year - 3:
                n_active_recent += 1

        careers[genre] = GenreCareerProfile(
            genre=genre,
            avg_years_to_stage_4=float(np.mean(years_to_stage4)) if years_to_stage4 else 0.0,
            promotion_rate=n_promoted / n_total if n_total > 0 else 0.0,
            retention_rate=n_active_recent / n_total if n_total > 0 else 0.0,
            unique_staff=n_total,
        )

    logger.info(
        "genre_ecosystem_computed",
        genres_trends=len(trends),
        genres_staffing=len(staffing),
        genres_seasonal=len(seasonality),
        genres_careers=len(careers),
    )

    return GenreEcosystemResult(
        trends=trends,
        staffing=staffing,
        seasonality=seasonality,
        careers=careers,
    )
