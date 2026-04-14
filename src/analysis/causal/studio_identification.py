"""Causal identification of major studio effects.

This module addresses the identification problem:
Are people from major studios truly skilled (selection effect), or is their
high evaluation driven by studio brand name (treatment/structural effect)?

Identification strategies:
1. Career trajectory analysis: Pre-post comparison of skill scores
2. Studio transition analysis: Performance changes when moving between studios
3. Fixed effects regression: Within-person comparison controlling for innate ability
4. Heterogeneous effects: Treatment effects by career stage, role, potential
5. Environmental adaptation: Performance stability across studio transitions

Causal inference assumptions:
- Selection effect: Major studios hire already-talented people
- Treatment effect: Major studios provide superior training/opportunities
- Brand effect: Major studio affiliation inflates PageRank via network structure

Confounders and moderators considered:
- Potential (ポテンシャル): Unrealized ability that emerges over time
- Age/Career stage (年齢/キャリアステージ): Young vs experienced effects
- Pre-existing trends (トレンド): Growth trajectories before major studio
- Collaboration synergy (集団シナジー): Performance in collaborative environments
- Environmental adaptation (環境適応性): Ability to maintain performance across changes
"""

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog
from scipy import stats

from src.models import Anime, Credit

logger = structlog.get_logger()

# Major studio threshold (top N studios by average person score)
MAJOR_STUDIO_THRESHOLD = 10
MIN_CREDITS_FOR_ANALYSIS = 5


class EffectType(Enum):
    """Type of causal effect identified."""

    SELECTION = "selection"  # Already talented people selected into major studios
    TREATMENT = "treatment"  # Major studios improve performance through training
    BRAND = "brand"  # Studio name inflates scores via network effects
    MIXED = "mixed"  # Both selection and treatment effects present
    INCONCLUSIVE = "inconclusive"  # Insufficient evidence


class CareerStage(Enum):
    """Career stage classification."""

    NEWCOMER = "newcomer"  # 0-3 years of experience
    MID_CAREER = "mid_career"  # 4-7 years of experience
    VETERAN = "veteran"  # 8+ years of experience


@dataclass
class StudioAffiliation:
    """Studio affiliation period for a person."""

    person_id: str
    studio_id: str
    is_major: bool
    start_year: int
    end_year: int
    credits_count: int
    avg_person_fe_score: float  # Average person_fe during this period
    avg_birank_score: float


@dataclass
class CareerTrajectory:
    """Career trajectory before and after major studio affiliation."""

    person_id: str
    person_name: str

    # Pre-major studio period
    pre_years: list[int] = field(default_factory=list)
    pre_person_fe_scores: list[float] = field(default_factory=list)
    pre_avg_person_fe: float = 0.0
    pre_credits: int = 0

    # Major studio period
    major_studio_id: str = ""
    major_years: list[int] = field(default_factory=list)
    major_person_fe_scores: list[float] = field(default_factory=list)
    major_avg_person_fe: float = 0.0
    major_credits: int = 0

    # Post-major studio period (if left)
    post_years: list[int] = field(default_factory=list)
    post_person_fe_scores: list[float] = field(default_factory=list)
    post_avg_person_fe: float = 0.0
    post_credits: int = 0

    # Causal estimates
    pre_to_major_change: float = 0.0  # Treatment effect estimate (person_fe change)
    major_to_post_change: float = 0.0  # Brand effect estimate (should drop if brand)
    trend_before_major: float = 0.0  # Pre-existing trend (selection indicator)

    # Enhanced controls for confounders
    career_stage_at_entry: CareerStage = (
        CareerStage.NEWCOMER
    )  # Career stage when joining major studio
    years_of_experience_at_entry: int = 0  # Years since debut when joining major
    potential_score_at_entry: float = (
        0.0  # Potential value score at entry (if available)
    )
    growth_acceleration_pre_entry: float = 0.0  # Growth acceleration before entry
    environmental_adaptation_score: float = 0.0  # Stability across transitions (0-100)
    collaboration_synergy_score: float = (
        0.0  # Performance boost in collaborative settings
    )


@dataclass
class StudioTransition:
    """Studio transition event (movement between studios)."""

    person_id: str
    person_name: str
    from_studio: str
    to_studio: str
    from_is_major: bool
    to_is_major: bool
    transition_year: int

    # Performance metrics
    before_person_fe: float  # Avg person_fe in 2 years before transition
    after_person_fe: float  # Avg person_fe in 2 years after transition
    person_fe_change: float  # after - before

    before_birank: float
    after_birank: float
    birank_change: float


@dataclass
class CausalEstimate:
    """Causal effect estimate with statistical inference."""

    effect_type: EffectType
    estimate: float  # Point estimate of effect size
    std_error: float
    confidence_interval: tuple[float, float]  # 95% CI
    p_value: float
    sample_size: int
    interpretation: str  # Human-readable interpretation


@dataclass
class IdentificationResult:
    """Complete identification result for studio effects."""

    # Studio classification
    major_studios: list[str]
    major_studio_names: dict[str, str]
    minor_studios: list[str]

    # Trajectory analysis
    trajectories: list[CareerTrajectory]
    avg_pre_to_major_change: float
    avg_major_to_post_change: float

    # Transition analysis
    transitions: list[StudioTransition]
    major_to_minor_transitions: list[StudioTransition]
    minor_to_major_transitions: list[StudioTransition]

    # Causal estimates
    selection_effect: CausalEstimate
    treatment_effect: CausalEstimate
    brand_effect: CausalEstimate

    # Overall conclusion
    dominant_effect: EffectType
    confidence_level: str  # "high", "medium", "low"
    summary: str


# ============================================================================
# Helper Functions for Enhanced Controls
# ============================================================================


def classify_career_stage(years_of_experience: int) -> CareerStage:
    """Classify career stage based on years of experience.

    Args:
        years_of_experience: Years since debut

    Returns:
        CareerStage enum value
    """
    if years_of_experience <= 3:
        return CareerStage.NEWCOMER
    elif years_of_experience <= 7:
        return CareerStage.MID_CAREER
    else:
        return CareerStage.VETERAN


def compute_environmental_adaptation(
    skill_scores_by_studio: list[float],
) -> float:
    """Compute environmental adaptation score based on performance stability.

    Higher score means more stable performance across different studios
    (less affected by environment changes).

    Args:
        skill_scores_by_studio: List of average skill scores at each studio

    Returns:
        Adaptation score (0-100), where 100 = perfectly stable
    """
    if len(skill_scores_by_studio) < 2:
        return 100.0  # Insufficient data, assume stable

    # Compute coefficient of variation (CV)
    mean_skill = sum(skill_scores_by_studio) / len(skill_scores_by_studio)
    if mean_skill == 0:
        return 0.0

    variance = sum((s - mean_skill) ** 2 for s in skill_scores_by_studio) / len(
        skill_scores_by_studio
    )
    std_dev = variance**0.5
    cv = std_dev / mean_skill

    # Convert CV to 0-100 scale (lower CV = higher adaptation)
    # CV of 0.2 (20% variation) → 80 score
    # CV of 0.5 (50% variation) → 50 score
    adaptation_score = max(0.0, 100.0 - cv * 100.0)
    return min(100.0, adaptation_score)


def compute_collaboration_synergy(
    person_credits: list[Credit],
    anime_map: dict[str, Anime],
    skill_scores_by_anime: dict[str, float],
) -> float:
    """Compute collaboration synergy score.

    Measures whether person performs better in high-quality collaborative
    environments (large teams with high-scoring members).

    Args:
        person_credits: All credits for this person
        anime_map: Anime ID to Anime object
        skill_scores_by_anime: Person's skill score for each anime

    Returns:
        Synergy score (0-100), where >50 means performs better in good teams
    """
    # Group by anime and count team size
    anime_team_sizes: dict[str, int] = {}
    for credit in person_credits:
        anime = anime_map.get(credit.anime_id)
        if anime:
            anime_team_sizes[credit.anime_id] = (
                anime_team_sizes.get(credit.anime_id, 0) + 1
            )

    # Compute correlation between team size and performance
    team_sizes = []
    performances = []
    for anime_id, team_size in anime_team_sizes.items():
        if anime_id in skill_scores_by_anime:
            team_sizes.append(team_size)
            performances.append(skill_scores_by_anime[anime_id])

    if len(team_sizes) < 3:
        return 50.0  # Insufficient data, assume neutral

    # Compute correlation
    try:
        corr, _ = stats.pearsonr(team_sizes, performances)
        # Convert correlation (-1 to 1) to 0-100 scale
        synergy_score = (corr + 1) * 50
        return max(0.0, min(100.0, synergy_score))
    except (ValueError, ZeroDivisionError):
        return 50.0  # Neutral if calculation fails


# ============================================================================
# Main Identification Functions
# ============================================================================


def identify_major_studios(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict],
    top_n: int = MAJOR_STUDIO_THRESHOLD,
    min_credits: int = 10,
) -> tuple[list[str], dict[str, str]]:
    """Identify major studios based on average person score.

    Args:
        credits: All credits
        anime_map: Anime ID to Anime object
        person_scores: Person ID to score dict (must include 'iv_score' key)
        top_n: Number of top studios to classify as major
        min_credits: Minimum credits required for a studio to be considered

    Returns:
        Tuple of (major_studio_ids, studio_name_map)
    """
    # Aggregate scores by studio
    studio_scores: dict[str, list[float]] = defaultdict(list)
    studio_names: dict[str, str] = {}

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime or not anime.studio:
            continue

        studio_id = anime.studio
        studio_names[studio_id] = anime.studio  # Store for later

        if credit.person_id in person_scores:
            composite = person_scores[credit.person_id].get("iv_score", 0)
            studio_scores[studio_id].append(composite)

    # Compute average scores
    studio_avg_scores = {
        studio: sum(scores) / len(scores)
        for studio, scores in studio_scores.items()
        if len(scores) >= min_credits
    }

    # Select top N
    sorted_studios = sorted(studio_avg_scores.items(), key=lambda x: x[1], reverse=True)
    major_studios = [studio_id for studio_id, _ in sorted_studios[:top_n]]

    logger.info(
        "major_studios_identified",
        count=len(major_studios),
        top_3_scores=[score for _, score in sorted_studios[:3]],
    )

    return major_studios, studio_names


def build_studio_affiliations(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    major_studios: set[str],
    person_fe_scores: dict[str, float],
    birank_scores: dict[str, float],
) -> dict[str, list[StudioAffiliation]]:
    """Build studio affiliation history for each person.

    Args:
        credits: All credits
        anime_map: Anime ID to Anime object
        major_studios: Set of major studio IDs
        person_fe_scores: Person ID to person_fe score
        birank_scores: Person ID to birank score

    Returns:
        Dict mapping person_id to list of StudioAffiliation objects
    """
    # Group credits by person and studio
    person_studio_credits: dict[str, dict[str, list[Credit]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime or not anime.studio or not anime.year:
            continue

        person_studio_credits[credit.person_id][anime.studio].append(credit)

    # Build affiliation objects
    affiliations: dict[str, list[StudioAffiliation]] = {}

    for person_id, studio_credits in person_studio_credits.items():
        person_affiliations = []

        for studio_id, studio_creds in studio_credits.items():
            years = [
                anime_map[c.anime_id].year
                for c in studio_creds
                if anime_map.get(c.anime_id) and anime_map[c.anime_id].year
            ]
            if not years:
                continue

            affiliation = StudioAffiliation(
                person_id=person_id,
                studio_id=studio_id,
                is_major=studio_id in major_studios,
                start_year=min(years),
                end_year=max(years),
                credits_count=len(studio_creds),
                avg_person_fe_score=person_fe_scores.get(person_id, 0),
                avg_birank_score=birank_scores.get(person_id, 0),
            )
            person_affiliations.append(affiliation)

        # Sort by start year
        person_affiliations.sort(key=lambda a: a.start_year)
        affiliations[person_id] = person_affiliations

    return affiliations


def analyze_career_trajectories(
    affiliations: dict[str, list[StudioAffiliation]],
    person_names: dict[str, str],
    min_credits: int = MIN_CREDITS_FOR_ANALYSIS,
    potential_value_scores: dict[str, Any] | None = None,
    growth_acceleration_data: dict[str, Any] | None = None,
) -> list[CareerTrajectory]:
    """Analyze career trajectories before, during, and after major studio affiliation.

    Focus on people who:
    1. Have pre-major studio experience (to measure selection)
    2. Worked at a major studio (to measure treatment)
    3. Optionally have post-major experience (to measure brand effect)

    Enhanced with controls for:
    - Career stage at entry (年齢/キャリアステージ)
    - Potential score at entry (ポテンシャル)
    - Growth acceleration before entry (トレンド)
    - Environmental adaptation (環境適応性)
    - Collaboration synergy (集団シナジー)

    Args:
        affiliations: Person ID to list of StudioAffiliation
        person_names: Person ID to name
        min_credits: Minimum credits required for inclusion
        potential_value_scores: Optional potential value scores dict
        growth_acceleration_data: Optional growth acceleration data dict

    Returns:
        List of CareerTrajectory objects with enhanced controls
    """
    trajectories = []

    for person_id, person_affiliations in affiliations.items():
        # Check if person has major studio experience
        major_affiliations = [a for a in person_affiliations if a.is_major]
        if not major_affiliations:
            continue

        # Find first major studio affiliation
        major_aff = major_affiliations[0]

        # Pre-major affiliations
        pre_affiliations = [
            a for a in person_affiliations if a.end_year < major_aff.start_year
        ]
        pre_credits = sum(a.credits_count for a in pre_affiliations)
        if pre_credits < min_credits:
            continue  # Need sufficient pre-major data

        # Post-major affiliations
        post_affiliations = [
            a for a in person_affiliations if a.start_year > major_aff.end_year
        ]

        # Build trajectory
        trajectory = CareerTrajectory(
            person_id=person_id,
            person_name=person_names.get(person_id, person_id),
            major_studio_id=major_aff.studio_id,
        )

        # Pre-major data
        trajectory.pre_years = [a.start_year for a in pre_affiliations]
        trajectory.pre_person_fe_scores = [
            a.avg_person_fe_score for a in pre_affiliations
        ]
        trajectory.pre_avg_person_fe = (
            sum(trajectory.pre_person_fe_scores) / len(trajectory.pre_person_fe_scores)
            if trajectory.pre_person_fe_scores
            else 0
        )
        trajectory.pre_credits = pre_credits

        # Major studio data
        trajectory.major_years = list(
            range(major_aff.start_year, major_aff.end_year + 1)
        )
        trajectory.major_person_fe_scores = [major_aff.avg_person_fe_score]
        trajectory.major_avg_person_fe = major_aff.avg_person_fe_score
        trajectory.major_credits = major_aff.credits_count

        # Post-major data
        if post_affiliations:
            trajectory.post_years = [a.start_year for a in post_affiliations]
            trajectory.post_person_fe_scores = [
                a.avg_person_fe_score for a in post_affiliations
            ]
            trajectory.post_avg_person_fe = (
                sum(trajectory.post_person_fe_scores)
                / len(trajectory.post_person_fe_scores)
                if trajectory.post_person_fe_scores
                else 0
            )
            trajectory.post_credits = sum(a.credits_count for a in post_affiliations)

        # Compute causal estimates
        trajectory.pre_to_major_change = (
            trajectory.major_avg_person_fe - trajectory.pre_avg_person_fe
        )

        if trajectory.post_avg_person_fe > 0:
            trajectory.major_to_post_change = (
                trajectory.post_avg_person_fe - trajectory.major_avg_person_fe
            )

        # Estimate pre-existing trend (linear regression on pre-major scores)
        if len(trajectory.pre_person_fe_scores) >= 2:
            x = list(range(len(trajectory.pre_person_fe_scores)))
            y = trajectory.pre_person_fe_scores
            slope, _, _, _, _ = stats.linregress(x, y)
            trajectory.trend_before_major = slope
        else:
            trajectory.trend_before_major = 0.0

        # ===== Enhanced controls =====
        # Career stage at entry
        if trajectory.pre_years:
            debut_year = min(trajectory.pre_years)
            years_of_experience = major_aff.start_year - debut_year
            trajectory.years_of_experience_at_entry = years_of_experience
            trajectory.career_stage_at_entry = classify_career_stage(
                years_of_experience
            )
        else:
            trajectory.years_of_experience_at_entry = 0
            trajectory.career_stage_at_entry = CareerStage.NEWCOMER

        # Growth acceleration before entry (2nd derivative of person_fe)
        if len(trajectory.pre_person_fe_scores) >= 3:
            # Compute first derivatives (velocities)
            velocities = [
                trajectory.pre_person_fe_scores[i + 1]
                - trajectory.pre_person_fe_scores[i]
                for i in range(len(trajectory.pre_person_fe_scores) - 1)
            ]
            # Compute second derivative (acceleration) - average change in velocity
            if len(velocities) >= 2:
                accelerations = [
                    velocities[i + 1] - velocities[i]
                    for i in range(len(velocities) - 1)
                ]
                trajectory.growth_acceleration_pre_entry = sum(accelerations) / len(
                    accelerations
                )
            else:
                trajectory.growth_acceleration_pre_entry = 0.0
        else:
            trajectory.growth_acceleration_pre_entry = 0.0

        # Environmental adaptation score (performance stability across studios)
        all_studio_scores = (
            trajectory.pre_person_fe_scores
            + trajectory.major_person_fe_scores
            + trajectory.post_person_fe_scores
        )
        trajectory.environmental_adaptation_score = compute_environmental_adaptation(
            all_studio_scores
        )

        # Potential score at entry (if available)
        if potential_value_scores and person_id in potential_value_scores:
            potential_data = potential_value_scores[person_id]
            if isinstance(potential_data, dict):
                trajectory.potential_score_at_entry = potential_data.get(
                    "potential_score", 0.0
                )
            else:
                # Handle dataclass case
                trajectory.potential_score_at_entry = getattr(
                    potential_data, "potential_score", 0.0
                )
        else:
            trajectory.potential_score_at_entry = 0.0

        # Override growth acceleration if data available
        if growth_acceleration_data and person_id in growth_acceleration_data:
            growth_data = growth_acceleration_data[person_id]
            if isinstance(growth_data, dict):
                trajectory.growth_acceleration_pre_entry = growth_data.get(
                    "acceleration", trajectory.growth_acceleration_pre_entry
                )
            else:
                # Handle dataclass case
                trajectory.growth_acceleration_pre_entry = getattr(
                    growth_data,
                    "acceleration",
                    trajectory.growth_acceleration_pre_entry,
                )

        # Collaboration synergy score (requires credit-level data, set to neutral for now)
        # This would need to be computed from individual credits, not studio aggregates
        trajectory.collaboration_synergy_score = 50.0  # Neutral default

        trajectories.append(trajectory)

    logger.info(
        "career_trajectories_analyzed",
        total_trajectories=len(trajectories),
        with_post_data=sum(1 for t in trajectories if t.post_credits > 0),
    )

    return trajectories


def analyze_studio_transitions(
    affiliations: dict[str, list[StudioAffiliation]],
    person_names: dict[str, str],
) -> list[StudioTransition]:
    """Analyze studio transition events (movements between studios).

    Args:
        affiliations: Person ID to list of StudioAffiliation
        person_names: Person ID to name

    Returns:
        List of StudioTransition objects
    """
    transitions = []

    for person_id, person_affiliations in affiliations.items():
        if len(person_affiliations) < 2:
            continue  # Need at least 2 studios for transition

        # Analyze consecutive affiliations
        for i in range(len(person_affiliations) - 1):
            from_aff = person_affiliations[i]
            to_aff = person_affiliations[i + 1]

            # Skip if transition is more than 5 years apart (likely unrelated)
            if to_aff.start_year - from_aff.end_year > 5:
                continue

            transition = StudioTransition(
                person_id=person_id,
                person_name=person_names.get(person_id, person_id),
                from_studio=from_aff.studio_id,
                to_studio=to_aff.studio_id,
                from_is_major=from_aff.is_major,
                to_is_major=to_aff.is_major,
                transition_year=to_aff.start_year,
                before_person_fe=from_aff.avg_person_fe_score,
                after_person_fe=to_aff.avg_person_fe_score,
                person_fe_change=to_aff.avg_person_fe_score
                - from_aff.avg_person_fe_score,
                before_birank=from_aff.avg_birank_score,
                after_birank=to_aff.avg_birank_score,
                birank_change=to_aff.avg_birank_score - from_aff.avg_birank_score,
            )

            transitions.append(transition)

    logger.info("studio_transitions_analyzed", total_transitions=len(transitions))

    return transitions


def estimate_causal_effects(
    trajectories: list[CareerTrajectory],
    transitions: list[StudioTransition],
) -> tuple[CausalEstimate, CausalEstimate, CausalEstimate]:
    """Estimate causal effects using trajectory and transition data.

    Returns:
        Tuple of (selection_effect, treatment_effect, brand_effect)
    """
    # 1. Selection effect: Pre-existing trend before major studio
    selection_trends = [t.trend_before_major for t in trajectories]
    selection_estimate = (
        sum(selection_trends) / len(selection_trends) if selection_trends else 0
    )
    selection_std = stats.sem(selection_trends) if len(selection_trends) > 1 else 0
    selection_ci = (
        stats.t.interval(
            0.95, len(selection_trends) - 1, loc=selection_estimate, scale=selection_std
        )
        if len(selection_trends) > 1
        else (0, 0)
    )
    selection_pval = (
        stats.ttest_1samp(selection_trends, 0).pvalue
        if len(selection_trends) > 1
        else 1.0
    )

    selection_effect = CausalEstimate(
        effect_type=EffectType.SELECTION,
        estimate=selection_estimate,
        std_error=selection_std,
        confidence_interval=selection_ci,
        p_value=selection_pval,
        sample_size=len(selection_trends),
        interpretation=(
            f"Pre-major studio trend: {selection_estimate:.2f} points/period. "
            f"{'Significant' if selection_pval < 0.05 else 'Not significant'} evidence of positive selection."
        ),
    )

    # 2. Treatment effect: Person FE increase from pre to major (controlling for trend)
    treatment_changes = [
        t.pre_to_major_change - t.trend_before_major * len(t.pre_person_fe_scores)
        for t in trajectories
    ]
    treatment_estimate = (
        sum(treatment_changes) / len(treatment_changes) if treatment_changes else 0
    )
    treatment_std = stats.sem(treatment_changes) if len(treatment_changes) > 1 else 0
    treatment_ci = (
        stats.t.interval(
            0.95,
            len(treatment_changes) - 1,
            loc=treatment_estimate,
            scale=treatment_std,
        )
        if len(treatment_changes) > 1
        else (0, 0)
    )
    treatment_pval = (
        stats.ttest_1samp(treatment_changes, 0).pvalue
        if len(treatment_changes) > 1
        else 1.0
    )

    treatment_effect = CausalEstimate(
        effect_type=EffectType.TREATMENT,
        estimate=treatment_estimate,
        std_error=treatment_std,
        confidence_interval=treatment_ci,
        p_value=treatment_pval,
        sample_size=len(treatment_changes),
        interpretation=(
            f"Treatment effect: {treatment_estimate:.2f} points (trend-adjusted). "
            f"{'Significant' if treatment_pval < 0.05 else 'Not significant'} evidence of training effect."
        ),
    )

    # 3. Brand effect: BiRank change for major-to-minor transitions
    major_to_minor = [t for t in transitions if t.from_is_major and not t.to_is_major]
    brand_changes = [t.birank_change for t in major_to_minor]
    brand_estimate = sum(brand_changes) / len(brand_changes) if brand_changes else 0
    brand_std = stats.sem(brand_changes) if len(brand_changes) > 1 else 0
    brand_ci = (
        stats.t.interval(
            0.95, len(brand_changes) - 1, loc=brand_estimate, scale=brand_std
        )
        if len(brand_changes) > 1
        else (0, 0)
    )
    brand_pval = (
        stats.ttest_1samp(brand_changes, 0).pvalue if len(brand_changes) > 1 else 1.0
    )

    brand_effect = CausalEstimate(
        effect_type=EffectType.BRAND,
        estimate=brand_estimate,
        std_error=brand_std,
        confidence_interval=brand_ci,
        p_value=brand_pval,
        sample_size=len(brand_changes),
        interpretation=(
            f"Brand effect: {brand_estimate:.2f} birank points lost when leaving major studio. "
            f"{'Significant' if brand_pval < 0.05 else 'Not significant'} evidence of network effect."
        ),
    )

    return selection_effect, treatment_effect, brand_effect


def determine_dominant_effect(
    selection: CausalEstimate, treatment: CausalEstimate, brand: CausalEstimate
) -> tuple[EffectType, str]:
    """Determine which effect is dominant based on causal estimates.

    Returns:
        Tuple of (dominant_effect, confidence_level)
    """
    # Check significance (p < 0.05) and effect size
    effects = [
        (EffectType.SELECTION, abs(selection.estimate), selection.p_value),
        (EffectType.TREATMENT, abs(treatment.estimate), treatment.p_value),
        (EffectType.BRAND, abs(brand.estimate), brand.p_value),
    ]

    # Filter significant effects
    significant = [
        (effect_type, magnitude)
        for effect_type, magnitude, p_val in effects
        if p_val < 0.05
    ]

    if not significant:
        return EffectType.INCONCLUSIVE, "low"

    # Find dominant effect
    dominant = max(significant, key=lambda x: x[1])

    # Check if multiple effects are present
    if len(significant) > 1:
        # Check if other effects are within 50% of dominant
        other_magnitudes = [mag for _, mag in significant if mag != dominant[1]]
        if any(mag > dominant[1] * 0.5 for mag in other_magnitudes):
            return EffectType.MIXED, "medium"

    # Single dominant effect
    confidence = "high" if dominant[1] > 5.0 else "medium"
    return dominant[0], confidence


def identify_studio_effects(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict],
    potential_value_scores: dict[str, Any] | None = None,
    growth_acceleration_data: dict[str, Any] | None = None,
) -> IdentificationResult:
    """Main function to identify causal studio effects with enhanced controls.

    Args:
        credits: All credits
        anime_map: Anime ID to Anime object
        person_scores: Person scores dict with 'iv_score', 'person_fe', 'birank' keys
        potential_value_scores: Optional potential value scores (ポテンシャル)
        growth_acceleration_data: Optional growth acceleration data (トレンド)

    Returns:
        IdentificationResult with causal estimates and interpretation
    """
    logger.info("causal_identification_start")

    # Step 1: Identify major studios
    major_studios, studio_names = identify_major_studios(
        credits, anime_map, person_scores
    )

    # Step 2: Build studio affiliations
    person_fe_scores = {
        pid: scores.get("person_fe", 0) for pid, scores in person_scores.items()
    }
    birank_scores = {
        pid: scores.get("birank", 0) for pid, scores in person_scores.items()
    }
    affiliations = build_studio_affiliations(
        credits, anime_map, set(major_studios), person_fe_scores, birank_scores
    )

    # Step 3: Analyze trajectories (with enhanced controls)
    person_names = {
        pid: scores.get("name", pid) for pid, scores in person_scores.items()
    }
    trajectories = analyze_career_trajectories(
        affiliations,
        person_names,
        potential_value_scores=potential_value_scores,
        growth_acceleration_data=growth_acceleration_data,
    )

    # Step 4: Analyze transitions
    transitions = analyze_studio_transitions(affiliations, person_names)

    # Step 5: Estimate causal effects
    selection_effect, treatment_effect, brand_effect = estimate_causal_effects(
        trajectories, transitions
    )

    # Step 6: Determine dominant effect
    dominant_effect, confidence_level = determine_dominant_effect(
        selection_effect, treatment_effect, brand_effect
    )

    # Step 7: Generate summary
    summary_parts = [
        f"分析対象: {len(trajectories)}人のキャリア軌跡、{len(transitions)}件のスタジオ移籍",
        "",
        f"【選択効果】{selection_effect.interpretation}",
        f"【処置効果】{treatment_effect.interpretation}",
        f"【ブランド効果】{brand_effect.interpretation}",
        "",
        f"結論: {dominant_effect.value.upper()}が支配的（信頼度: {confidence_level}）",
    ]

    if dominant_effect == EffectType.SELECTION:
        summary_parts.append("→ 大手スタジオは既に優秀な人材を採用している（逆選択）")
    elif dominant_effect == EffectType.TREATMENT:
        summary_parts.append("→ 大手スタジオの教育・環境が実力向上に貢献している")
    elif dominant_effect == EffectType.BRAND:
        summary_parts.append(
            "→ スタジオのネームバリューがPageRankスコアを押し上げている"
        )
    elif dominant_effect == EffectType.MIXED:
        summary_parts.append("→ 選択効果と処置効果の両方が存在する")

    summary = "\n".join(summary_parts)

    # Separate transitions by type
    major_to_minor = [t for t in transitions if t.from_is_major and not t.to_is_major]
    minor_to_major = [t for t in transitions if not t.from_is_major and t.to_is_major]

    result = IdentificationResult(
        major_studios=major_studios,
        major_studio_names=studio_names,
        minor_studios=[sid for sid in studio_names if sid not in major_studios],
        trajectories=trajectories,
        avg_pre_to_major_change=(
            sum(t.pre_to_major_change for t in trajectories) / len(trajectories)
            if trajectories
            else 0
        ),
        avg_major_to_post_change=(
            sum(t.major_to_post_change for t in trajectories if t.post_credits > 0)
            / sum(1 for t in trajectories if t.post_credits > 0)
            if any(t.post_credits > 0 for t in trajectories)
            else 0
        ),
        transitions=transitions,
        major_to_minor_transitions=major_to_minor,
        minor_to_major_transitions=minor_to_major,
        selection_effect=selection_effect,
        treatment_effect=treatment_effect,
        brand_effect=brand_effect,
        dominant_effect=dominant_effect,
        confidence_level=confidence_level,
        summary=summary,
    )

    logger.info(
        "causal_identification_complete",
        dominant_effect=dominant_effect.value,
        confidence=confidence_level,
    )

    return result


def export_identification_report(result: IdentificationResult) -> dict[str, Any]:
    """Export identification result as JSON-serializable dict.

    Args:
        result: IdentificationResult object

    Returns:
        JSON-serializable dict
    """
    return {
        "major_studios": {
            "studio_ids": result.major_studios,
            "studio_names": result.major_studio_names,
            "count": len(result.major_studios),
        },
        "sample_statistics": {
            "total_trajectories": len(result.trajectories),
            "total_transitions": len(result.transitions),
            "major_to_minor_transitions": len(result.major_to_minor_transitions),
            "minor_to_major_transitions": len(result.minor_to_major_transitions),
        },
        "causal_estimates": {
            "selection_effect": {
                "estimate": result.selection_effect.estimate,
                "std_error": result.selection_effect.std_error,
                "confidence_interval": result.selection_effect.confidence_interval,
                "p_value": result.selection_effect.p_value,
                "sample_size": result.selection_effect.sample_size,
                "interpretation": result.selection_effect.interpretation,
            },
            "treatment_effect": {
                "estimate": result.treatment_effect.estimate,
                "std_error": result.treatment_effect.std_error,
                "confidence_interval": result.treatment_effect.confidence_interval,
                "p_value": result.treatment_effect.p_value,
                "sample_size": result.treatment_effect.sample_size,
                "interpretation": result.treatment_effect.interpretation,
            },
            "brand_effect": {
                "estimate": result.brand_effect.estimate,
                "std_error": result.brand_effect.std_error,
                "confidence_interval": result.brand_effect.confidence_interval,
                "p_value": result.brand_effect.p_value,
                "sample_size": result.brand_effect.sample_size,
                "interpretation": result.brand_effect.interpretation,
            },
        },
        "aggregate_metrics": {
            "avg_pre_to_major_person_fe_change": result.avg_pre_to_major_change,
            "avg_major_to_post_person_fe_change": result.avg_major_to_post_change,
        },
        "conclusion": {
            "dominant_effect": result.dominant_effect.value,
            "confidence_level": result.confidence_level,
            "summary": result.summary,
        },
        "top_trajectories": [
            {
                "person_name": t.person_name,
                "major_studio_id": t.major_studio_id,
                "pre_avg_person_fe": t.pre_avg_person_fe,
                "major_avg_person_fe": t.major_avg_person_fe,
                "post_avg_person_fe": t.post_avg_person_fe,
                "pre_to_major_change": t.pre_to_major_change,
                "major_to_post_change": t.major_to_post_change,
                "trend_before_major": t.trend_before_major,
            }
            for t in sorted(
                result.trajectories,
                key=lambda x: abs(x.pre_to_major_change),
                reverse=True,
            )[:20]
        ],
        "top_transitions": [
            {
                "person_name": t.person_name,
                "from_studio": t.from_studio,
                "to_studio": t.to_studio,
                "from_is_major": t.from_is_major,
                "to_is_major": t.to_is_major,
                "transition_year": t.transition_year,
                "person_fe_change": t.person_fe_change,
                "birank_change": t.birank_change,
            }
            for t in sorted(
                result.transitions, key=lambda x: abs(x.person_fe_change), reverse=True
            )[:20]
        ],
    }
