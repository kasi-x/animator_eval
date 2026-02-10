"""Tests for causal studio identification module."""

import pytest

from src.analysis.causal_studio_identification import (
    CausalEstimate,
    CareerTrajectory,
    EffectType,
    StudioAffiliation,
    StudioTransition,
    analyze_career_trajectories,
    analyze_studio_transitions,
    build_studio_affiliations,
    determine_dominant_effect,
    estimate_causal_effects,
    export_identification_report,
    identify_major_studios,
    identify_studio_effects,
)
from src.models import Anime, Credit, Person, Role


class TestIdentifyMajorStudios:
    """Tests for identify_major_studios function."""

    def test_identifies_top_studios_by_avg_score(self):
        """Top N studios by average person score are identified as major."""
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p3", anime_id="a2", role=Role.KEY_ANIMATOR),
            Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR),
            Credit(person_id="p5", anime_id="a3", role=Role.KEY_ANIMATOR),
            Credit(person_id="p6", anime_id="a3", role=Role.KEY_ANIMATOR),
            Credit(person_id="p7", anime_id="a3", role=Role.KEY_ANIMATOR),
            Credit(person_id="p8", anime_id="a3", role=Role.KEY_ANIMATOR),
            Credit(person_id="p9", anime_id="a3", role=Role.KEY_ANIMATOR),
            Credit(person_id="p10", anime_id="a3", role=Role.KEY_ANIMATOR),
        ]

        anime_map = {
            "a1": Anime(id="a1", title_ja="アニメ1", studio="Studio A", year=2020),
            "a2": Anime(id="a2", title_ja="アニメ2", studio="Studio B", year=2020),
            "a3": Anime(id="a3", title_ja="アニメ3", studio="Studio C", year=2020),
        }

        person_scores = {
            "p1": {"composite": 90.0},
            "p2": {"composite": 85.0},  # Studio A avg: 87.5
            "p3": {"composite": 70.0},
            "p4": {"composite": 65.0},  # Studio B avg: 67.5
            "p5": {"composite": 50.0},
            "p6": {"composite": 55.0},
            "p7": {"composite": 52.0},
            "p8": {"composite": 48.0},
            "p9": {"composite": 53.0},
            "p10": {"composite": 52.0},  # Studio C avg: 51.67
        }

        major_studios, studio_names = identify_major_studios(
            credits, anime_map, person_scores, top_n=2, min_credits=2
        )

        assert len(major_studios) == 2
        assert "Studio A" in major_studios  # Highest avg score
        assert "Studio B" in major_studios  # Second highest
        assert "Studio C" not in major_studios  # Lowest avg score

    def test_requires_minimum_credits(self):
        """Studios with fewer than 10 credits are excluded."""
        credits = [
            Credit(person_id=f"p{i}", anime_id="a1", role=Role.KEY_ANIMATOR)
            for i in range(5)
        ]

        anime_map = {"a1": Anime(id="a1", title_ja="Test", studio="Small Studio", year=2020)}

        person_scores = {f"p{i}": {"composite": 100.0} for i in range(5)}

        major_studios, _ = identify_major_studios(
            credits, anime_map, person_scores, top_n=1
        )

        assert len(major_studios) == 0  # Not enough credits

    def test_handles_missing_studios(self):
        """Anime without studios are skipped."""
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR)]

        anime_map = {"a1": Anime(id="a1", title_ja="Test", studio=None, year=2020)}

        person_scores = {"p1": {"composite": 100.0}}

        major_studios, _ = identify_major_studios(credits, anime_map, person_scores)

        assert len(major_studios) == 0


class TestBuildStudioAffiliations:
    """Tests for build_studio_affiliations function."""

    def test_builds_affiliation_history(self):
        """Creates affiliation objects for each person-studio pair."""
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR),
            Credit(person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR),
        ]

        anime_map = {
            "a1": Anime(id="a1", title_ja="Test1", studio="Studio A", year=2018),
            "a2": Anime(id="a2", title_ja="Test2", studio="Studio A", year=2019),
            "a3": Anime(id="a3", title_ja="Test3", studio="Studio B", year=2020),
        }

        major_studios = {"Studio A"}
        skill_scores = {"p1": 80.0}
        authority_scores = {"p1": 75.0}

        affiliations = build_studio_affiliations(
            credits, anime_map, major_studios, skill_scores, authority_scores
        )

        assert "p1" in affiliations
        assert len(affiliations["p1"]) == 2  # Two studios

        # Check first affiliation (Studio A)
        aff_a = affiliations["p1"][0]
        assert aff_a.studio_id == "Studio A"
        assert aff_a.is_major is True
        assert aff_a.start_year == 2018
        assert aff_a.end_year == 2019
        assert aff_a.credits_count == 2

        # Check second affiliation (Studio B)
        aff_b = affiliations["p1"][1]
        assert aff_b.studio_id == "Studio B"
        assert aff_b.is_major is False
        assert aff_b.start_year == 2020
        assert aff_b.end_year == 2020
        assert aff_b.credits_count == 1


class TestAnalyzeCareerTrajectories:
    """Tests for analyze_career_trajectories function."""

    def test_requires_pre_major_experience(self):
        """Only includes people with sufficient pre-major credits."""
        affiliations = {
            "p1": [
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Major",
                    is_major=True,
                    start_year=2020,
                    end_year=2021,
                    credits_count=10,
                    avg_skill_score=80.0,
                    avg_authority_score=75.0,
                )
            ],
            "p2": [
                StudioAffiliation(
                    person_id="p2",
                    studio_id="Minor1",
                    is_major=False,
                    start_year=2018,
                    end_year=2019,
                    credits_count=5,
                    avg_skill_score=60.0,
                    avg_authority_score=50.0,
                ),
                StudioAffiliation(
                    person_id="p2",
                    studio_id="Major",
                    is_major=True,
                    start_year=2020,
                    end_year=2021,
                    credits_count=10,
                    avg_skill_score=80.0,
                    avg_authority_score=75.0,
                ),
            ],
        }

        person_names = {"p1": "Person 1", "p2": "Person 2"}

        trajectories = analyze_career_trajectories(affiliations, person_names)

        # Only p2 should be included (has pre-major experience)
        assert len(trajectories) == 1
        assert trajectories[0].person_id == "p2"

    def test_computes_skill_changes(self):
        """Computes pre-to-major and major-to-post skill changes."""
        affiliations = {
            "p1": [
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Minor",
                    is_major=False,
                    start_year=2018,
                    end_year=2019,
                    credits_count=5,
                    avg_skill_score=60.0,
                    avg_authority_score=50.0,
                ),
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Major",
                    is_major=True,
                    start_year=2020,
                    end_year=2021,
                    credits_count=10,
                    avg_skill_score=80.0,
                    avg_authority_score=75.0,
                ),
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Minor2",
                    is_major=False,
                    start_year=2022,
                    end_year=2023,
                    credits_count=5,
                    avg_skill_score=75.0,
                    avg_authority_score=70.0,
                ),
            ]
        }

        person_names = {"p1": "Person 1"}

        trajectories = analyze_career_trajectories(affiliations, person_names)

        assert len(trajectories) == 1
        traj = trajectories[0]

        assert traj.pre_avg_skill == 60.0
        assert traj.major_avg_skill == 80.0
        assert traj.post_avg_skill == 75.0
        assert traj.pre_to_major_change == 20.0  # 80 - 60
        assert traj.major_to_post_change == -5.0  # 75 - 80


class TestAnalyzeStudioTransitions:
    """Tests for analyze_studio_transitions function."""

    def test_identifies_transitions(self):
        """Identifies studio-to-studio transitions."""
        affiliations = {
            "p1": [
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Studio A",
                    is_major=True,
                    start_year=2018,
                    end_year=2019,
                    credits_count=5,
                    avg_skill_score=80.0,
                    avg_authority_score=75.0,
                ),
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Studio B",
                    is_major=False,
                    start_year=2020,
                    end_year=2021,
                    credits_count=5,
                    avg_skill_score=70.0,
                    avg_authority_score=60.0,
                ),
            ]
        }

        person_names = {"p1": "Person 1"}

        transitions = analyze_studio_transitions(affiliations, person_names)

        assert len(transitions) == 1
        trans = transitions[0]

        assert trans.from_studio == "Studio A"
        assert trans.to_studio == "Studio B"
        assert trans.from_is_major is True
        assert trans.to_is_major is False
        assert trans.skill_change == -10.0  # 70 - 80
        assert trans.authority_change == -15.0  # 60 - 75

    def test_skips_large_gaps(self):
        """Skips transitions with >5 year gaps."""
        affiliations = {
            "p1": [
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Studio A",
                    is_major=True,
                    start_year=2010,
                    end_year=2011,
                    credits_count=5,
                    avg_skill_score=80.0,
                    avg_authority_score=75.0,
                ),
                StudioAffiliation(
                    person_id="p1",
                    studio_id="Studio B",
                    is_major=False,
                    start_year=2020,
                    end_year=2021,
                    credits_count=5,
                    avg_skill_score=70.0,
                    avg_authority_score=60.0,
                ),
            ]
        }

        person_names = {"p1": "Person 1"}

        transitions = analyze_studio_transitions(affiliations, person_names)

        assert len(transitions) == 0  # Gap too large (9 years)


class TestEstimateCausalEffects:
    """Tests for estimate_causal_effects function."""

    def test_computes_three_effects(self):
        """Computes selection, treatment, and brand effects."""
        trajectories = [
            CareerTrajectory(
                person_id="p1",
                person_name="Person 1",
                pre_skill_scores=[60.0, 62.0],
                major_avg_skill=80.0,
                post_avg_skill=75.0,
                pre_to_major_change=20.0,
                trend_before_major=1.0,  # Positive trend (selection indicator)
            ),
            CareerTrajectory(
                person_id="p2",
                person_name="Person 2",
                pre_skill_scores=[55.0, 56.0],
                major_avg_skill=75.0,
                post_avg_skill=70.0,
                pre_to_major_change=20.0,
                trend_before_major=0.5,
            ),
        ]

        transitions = [
            StudioTransition(
                person_id="p1",
                person_name="Person 1",
                from_studio="Major",
                to_studio="Minor",
                from_is_major=True,
                to_is_major=False,
                transition_year=2022,
                before_skill=80.0,
                after_skill=75.0,
                skill_change=-5.0,
                before_authority=75.0,
                after_authority=60.0,
                authority_change=-15.0,  # Brand effect indicator
            )
        ]

        selection, treatment, brand = estimate_causal_effects(trajectories, transitions)

        # Check that all three effects are computed
        assert selection.effect_type == EffectType.SELECTION
        assert treatment.effect_type == EffectType.TREATMENT
        assert brand.effect_type == EffectType.BRAND

        # Selection effect should be positive (average trend = 0.75)
        assert selection.estimate > 0

        # Brand effect should be negative (authority drops when leaving major)
        assert brand.estimate < 0

        # All should have sample sizes
        assert selection.sample_size == 2
        assert treatment.sample_size == 2
        assert brand.sample_size == 1


class TestDetermineDominantEffect:
    """Tests for determine_dominant_effect function."""

    def test_identifies_single_significant_effect(self):
        """Identifies dominant effect when only one is significant."""
        selection = CausalEstimate(
            effect_type=EffectType.SELECTION,
            estimate=10.0,
            std_error=1.0,
            confidence_interval=(8.0, 12.0),
            p_value=0.001,  # Significant
            sample_size=50,
            interpretation="Significant selection",
        )

        treatment = CausalEstimate(
            effect_type=EffectType.TREATMENT,
            estimate=2.0,
            std_error=1.5,
            confidence_interval=(-1.0, 5.0),
            p_value=0.15,  # Not significant
            sample_size=50,
            interpretation="No treatment effect",
        )

        brand = CausalEstimate(
            effect_type=EffectType.BRAND,
            estimate=-1.0,
            std_error=1.0,
            confidence_interval=(-3.0, 1.0),
            p_value=0.3,  # Not significant
            sample_size=50,
            interpretation="No brand effect",
        )

        dominant, confidence = determine_dominant_effect(selection, treatment, brand)

        assert dominant == EffectType.SELECTION
        assert confidence == "high"  # Large effect size

    def test_identifies_mixed_effects(self):
        """Identifies mixed when multiple effects are significant and similar."""
        selection = CausalEstimate(
            effect_type=EffectType.SELECTION,
            estimate=8.0,
            std_error=1.0,
            confidence_interval=(6.0, 10.0),
            p_value=0.001,
            sample_size=50,
            interpretation="Significant selection",
        )

        treatment = CausalEstimate(
            effect_type=EffectType.TREATMENT,
            estimate=6.0,  # Within 50% of selection
            std_error=1.0,
            confidence_interval=(4.0, 8.0),
            p_value=0.001,  # Also significant
            sample_size=50,
            interpretation="Significant treatment",
        )

        brand = CausalEstimate(
            effect_type=EffectType.BRAND,
            estimate=-1.0,
            std_error=1.0,
            confidence_interval=(-3.0, 1.0),
            p_value=0.3,
            sample_size=50,
            interpretation="No brand effect",
        )

        dominant, confidence = determine_dominant_effect(selection, treatment, brand)

        assert dominant == EffectType.MIXED
        assert confidence == "medium"

    def test_inconclusive_when_no_significance(self):
        """Returns inconclusive when no effects are significant."""
        selection = CausalEstimate(
            effect_type=EffectType.SELECTION,
            estimate=1.0,
            std_error=2.0,
            confidence_interval=(-3.0, 5.0),
            p_value=0.6,
            sample_size=10,
            interpretation="No selection",
        )

        treatment = CausalEstimate(
            effect_type=EffectType.TREATMENT,
            estimate=0.5,
            std_error=2.0,
            confidence_interval=(-3.5, 4.5),
            p_value=0.8,
            sample_size=10,
            interpretation="No treatment",
        )

        brand = CausalEstimate(
            effect_type=EffectType.BRAND,
            estimate=-0.3,
            std_error=2.0,
            confidence_interval=(-4.3, 3.7),
            p_value=0.9,
            sample_size=10,
            interpretation="No brand",
        )

        dominant, confidence = determine_dominant_effect(selection, treatment, brand)

        assert dominant == EffectType.INCONCLUSIVE
        assert confidence == "low"


class TestExportIdentificationReport:
    """Tests for export_identification_report function."""

    def test_exports_complete_report(self):
        """Exports all required fields."""
        from src.analysis.causal_studio_identification import IdentificationResult

        result = IdentificationResult(
            major_studios=["Studio A", "Studio B"],
            major_studio_names={"Studio A": "Studio A", "Studio B": "Studio B"},
            minor_studios=["Studio C"],
            trajectories=[],
            avg_pre_to_major_change=15.0,
            avg_major_to_post_change=-5.0,
            transitions=[],
            major_to_minor_transitions=[],
            minor_to_major_transitions=[],
            selection_effect=CausalEstimate(
                effect_type=EffectType.SELECTION,
                estimate=2.0,
                std_error=0.5,
                confidence_interval=(1.0, 3.0),
                p_value=0.01,
                sample_size=50,
                interpretation="Positive selection",
            ),
            treatment_effect=CausalEstimate(
                effect_type=EffectType.TREATMENT,
                estimate=10.0,
                std_error=1.0,
                confidence_interval=(8.0, 12.0),
                p_value=0.001,
                sample_size=50,
                interpretation="Strong treatment effect",
            ),
            brand_effect=CausalEstimate(
                effect_type=EffectType.BRAND,
                estimate=-8.0,
                std_error=1.5,
                confidence_interval=(-11.0, -5.0),
                p_value=0.002,
                sample_size=30,
                interpretation="Significant brand penalty",
            ),
            dominant_effect=EffectType.TREATMENT,
            confidence_level="high",
            summary="Treatment effect is dominant",
        )

        report = export_identification_report(result)

        # Check structure
        assert "major_studios" in report
        assert "sample_statistics" in report
        assert "causal_estimates" in report
        assert "aggregate_metrics" in report
        assert "conclusion" in report

        # Check values
        assert report["major_studios"]["count"] == 2
        assert report["conclusion"]["dominant_effect"] == "treatment"
        assert report["conclusion"]["confidence_level"] == "high"
        assert report["causal_estimates"]["treatment_effect"]["estimate"] == 10.0
