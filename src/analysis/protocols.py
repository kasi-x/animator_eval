"""Standard protocols and type definitions for analysis functions.

Provides dataclass definitions for common return patterns to enable:
- Type checking with mypy
- IDE autocomplete
- Runtime validation
- Consistent structure
"""

from dataclasses import dataclass, field
from typing import Protocol

from src.runtime.models import AnimeAnalysis as Anime, Credit

# ============================================================================
# Person Metrics Dataclasses (dict[str, dataclass] pattern)
# ============================================================================


@dataclass
class VersatilityMetrics:
    """Return type for compute_versatility() per person.

    Attributes:
        categories: List of role categories worked in
        category_count: Number of distinct categories
        roles: List of specific roles worked
        role_count: Number of distinct roles
        versatility_score: Computed versatility score (0-100)
        category_credits: Dict mapping category name to credit count
    """

    categories: list[str] = field(default_factory=list)
    category_count: int = 0
    roles: list[str] = field(default_factory=list)
    role_count: int = 0
    versatility_score: float = 0.0
    category_credits: dict[str, int] = field(default_factory=dict)


@dataclass
class GrowthMetrics:
    """Return type for compute_growth_trends() per person.

    Attributes:
        yearly_credits: Dict mapping year to credit count
        trend: Growth trend classification ("rising" | "stable" | "declining" | "inactive" | "new")
        total_credits: Total credits across all years
        recent_credits: Credits in recent period
        total_years: Number of years active
        career_span: Years between first and last credit
        activity_ratio: Ratio of active years to career span
        recent_avg_anime_score: Average anime score in recent period (optional)
        career_avg_anime_score: Average anime score across career (optional)
        current_score: Current composite score (optional)
    """

    yearly_credits: dict[int, int] = field(default_factory=dict)
    trend: str = "new"
    total_credits: int = 0
    recent_credits: int = 0
    total_years: int = 0
    career_span: int = 0
    activity_ratio: float = 0.0
    recent_avg_anime_score: float | None = None
    career_avg_anime_score: float | None = None
    current_score: float | None = None


@dataclass
class NetworkDensityMetrics:
    """Return type for compute_network_density() per person.

    Attributes:
        collaborator_count: Number of unique collaborators
        unique_anime: Number of unique anime worked on
        hub_score: Network hub score (collaborator_count / unique_anime)
        avg_collaborator_score: Average composite score of collaborators (optional)
    """

    collaborator_count: int = 0
    unique_anime: int = 0
    hub_score: float = 0.0
    avg_collaborator_score: float | None = None


@dataclass
class ProductivityMetrics:
    """Return type for compute_productivity() per person.

    Attributes:
        total_credits: Total credits across career
        unique_anime: Number of unique anime worked on
        active_years: Number of years with at least one credit
        career_span: Years between first and last credit
        credits_per_year: Average credits per active year
        peak_year: Year with most credits (optional)
        peak_credits: Number of credits in peak year
        consistency_score: Measure of consistent output (0-100)
    """

    total_credits: int = 0
    unique_anime: int = 0
    active_years: int = 0
    career_span: int = 0
    credits_per_year: float = 0.0
    peak_year: int | None = None
    peak_credits: int = 0
    consistency_score: float = 0.0


# ============================================================================
# Function Protocols (standard signatures)
# ============================================================================


class PersonMetricsFunction(Protocol):
    """Standard signature for person-level metric computation.

    Functions following this protocol return dict[str, dataclass] where keys are
    person IDs and values are metric dataclass instances.

    Example implementations:
        - compute_versatility
        - compute_growth_trends
        - compute_network_density
        - compute_productivity
    """

    def __call__(
        self,
        credits: list[Credit],
        anime_map: dict[str, Anime] | None = None,
        **kwargs,
    ) -> dict[str, object]:
        """Compute per-person metrics.

        Args:
            credits: All credits in dataset
            anime_map: Optional anime ID to Anime object mapping
            **kwargs: Additional function-specific parameters

        Returns:
            Dict mapping person_id to metrics dataclass
        """
        ...


class ScoreFunction(Protocol):
    """Standard signature for scoring functions (returns scalar per person).

    Functions following this protocol return dict[str, float] where keys are
    person IDs and values are float scores.

    Example implementations:
        - compute_pagerank (Authority)
        - compute_trust_scores (Trust)
        - compute_skill_scores (Skill)
    """

    def __call__(
        self,
        credits: list[Credit],
        anime_map: dict[str, Anime],
        **kwargs,
    ) -> dict[str, float]:
        """Compute scores for each person.

        Args:
            credits: All credits in dataset
            anime_map: Anime ID to Anime object mapping
            **kwargs: Additional function-specific parameters

        Returns:
            Dict mapping person_id to float score
        """
        ...
