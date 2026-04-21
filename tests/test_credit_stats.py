"""Tests for credit statistics analysis module."""

from pathlib import Path

from src.analysis.credit_stats import compute_credit_statistics
from src.analysis.credit_stats_html import generate_credit_stats_html
from src.models import Credit, BronzeAnime as Anime


def test_compute_credit_statistics_basic():
    """Test basic credit statistics computation."""
    credits = [
        Credit(
            id=1,
            person_id="anilist:p1",
            anime_id="anime1",
            role="director",
            episode=None,
            source="anilist",
        ),
        Credit(
            id=2,
            person_id="anilist:p2",
            anime_id="anime1",
            role="key_animator",
            episode=None,
            source="anilist",
        ),
        Credit(
            id=3,
            person_id="anilist:p1",
            anime_id="anime2",
            role="director",
            episode=None,
            source="anilist",
        ),
        Credit(
            id=4,
            person_id="anilist:p2",
            anime_id="anime2",
            role="key_animator",
            episode=None,
            source="anilist",
        ),
        Credit(
            id=5,
            person_id="anilist:p3",
            anime_id="anime2",
            role="character_designer",
            episode=None,
            source="anilist",
        ),
    ]

    anime_map = {
        "anime1": Anime(
            id="anime1",
            title_ja="テストアニメ1",
            title_en="Test Anime 1",
            year=2020,
            season="spring",
            episodes=12,
        ),
        "anime2": Anime(
            id="anime2",
            title_ja="テストアニメ2",
            title_en="Test Anime 2",
            year=2021,
            season="fall",
            episodes=24,
        ),
    }

    result = compute_credit_statistics(credits, anime_map)

    # Verify summary
    assert result["summary"]["total_credits"] == 5
    assert result["summary"]["unique_persons"] == 3
    assert result["summary"]["unique_anime"] == 2

    # Verify role distribution
    assert "director" in result["role_distribution"]
    assert result["role_distribution"]["director"] == 2
    assert result["role_distribution"]["key_animator"] == 2
    assert result["role_distribution"]["character_designer"] == 1

    # Verify collaboration stats
    assert result["collaboration_stats"]["total_pair_instances"] > 0

    # Verify timeline
    assert len(result["timeline_stats"]["by_year"]) == 2

    # Verify person stats
    assert len(result["person_id_stats"]["top_persons_by_credits"]) == 3


def test_compute_credit_statistics_empty():
    """Test with empty credits."""
    result = compute_credit_statistics([], {})
    assert result == {}


def test_generate_html_report(tmp_path: Path):
    """Test HTML report generation."""
    # Create sample data
    credit_stats = {
        "summary": {
            "total_credits": 100,
            "unique_persons": 20,
            "unique_anime": 10,
            "avg_credits_per_person": 5.0,
            "avg_staff_per_anime": 10.0,
        },
        "role_distribution": {
            "director": 10,
            "key_animator": 30,
            "character_designer": 5,
        },
        "top_roles": [
            {"role": "key_animator", "count": 30, "percentage": 30.0},
            {"role": "director", "count": 10, "percentage": 10.0},
            {"role": "character_designer", "count": 5, "percentage": 5.0},
        ],
        "collaboration_stats": {
            "total_pairs": 50,
            "avg_shared_anime": 2.5,
            "top_collaborations": [
                {
                    "person_id_1": "anilist:p1",
                    "person_id_2": "anilist:p2",
                    "shared_anime_count": 10,
                }
            ],
        },
        "timeline_stats": {
            "by_year": [
                {"year": 2020, "credits": 40, "anime_count": 5, "person_count": 15},
                {"year": 2021, "credits": 60, "anime_count": 5, "person_count": 18},
            ],
            "total_years": 2,
            "year_range": "2020-2021",
        },
        "person_id_stats": {
            "top_persons_by_credits": [
                {"person_id": "anilist:p1", "credit_count": 15},
                {"person_id": "anilist:p2", "credit_count": 12},
            ],
            "role_diversity_distribution": {1: 10, 2: 8, 3: 2},
            "avg_roles_per_person": 1.6,
            "max_roles_single_person": 3,
        },
    }

    output_path = tmp_path / "credit_stats_report.html"
    generate_credit_stats_html(credit_stats, output_path)

    # Verify file was created
    assert output_path.exists()

    # Verify content contains key elements
    content = output_path.read_text(encoding="utf-8")
    assert "Credit Statistics Report" in content
    assert "d3.v7.min.js" in content
    assert "Total Credits" in content
    assert "Role Distribution" in content
    assert "Timeline Analysis" in content
    assert "Collaboration Statistics" in content


def test_generate_html_report_empty(tmp_path: Path):
    """Test HTML generation with empty data."""
    output_path = tmp_path / "empty_report.html"
    generate_credit_stats_html({}, output_path)

    # Should not create file for empty data
    assert not output_path.exists()
