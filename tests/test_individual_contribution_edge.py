"""individual_contribution.py edge case tests."""
from src.models import BronzeAnime as Anime, Credit, Role

import networkx as nx



def _anime(
    aid: str,
    *,
    year: int = 2020,
    score: float | None = 75.0,
    studio: str | None = None,
    studios: list[str] | None = None,
    tags: list[dict] | None = None,
    genres: list[str] | None = None,
) -> Anime:
    resolved_studios = studios or ([studio] if studio else [])
    return Anime(
        id=aid,
        title_ja=f"Anime_{aid}",
        title_en=f"Anime_{aid}",
        year=year,
        score=score,
        studios=resolved_studios,
        tags=tags or [],
        genres=genres or [],
    )


def _credit(pid: str, aid: str, role: Role = Role.KEY_ANIMATOR) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role, source="test")

class TestIndividualContributionEdgeCases:
    def test_consistency_with_zero_mean(self):
        """anime.score=0 should result in None consistency."""
        from src.analysis.scoring.individual_contribution import compute_consistency

        features = {"p1": {"iv_score": 50}}
        anime_map = {
            f"a{i}": Anime(id=f"a{i}", title_ja=f"a{i}") for i in range(6)
        }
        credits = [_credit("p1", f"a{i}") for i in range(6)]
        result = compute_consistency(features, credits, anime_map)
        assert result["p1"] is None

    def test_independent_value_with_collaboration_graph(self):
        """Test independent_value uses collaboration_graph when provided."""
        from src.analysis.scoring.individual_contribution import (
            compute_independent_value,
        )

        # independent_value now compares collaborator IV residuals with/without
        # the target person, rather than comparing anime.score. Scores on
        # anime_map are irrelevant; only participant IV scores matter.
        features = {f"p{i}": {"iv_score": 50 + i * 5} for i in range(6)}
        anime_map = {
            "shared": _anime("shared"),
            "solo1": _anime("solo1"),
            "solo2": _anime("solo2"),
            "solo3": _anime("solo3"),
        }
        credits = [
            _credit("p0", "shared"),
            _credit("p1", "shared"),
            _credit("p1", "solo1"),
            _credit("p2", "shared"),
            _credit("p2", "solo2"),
            _credit("p3", "shared"),
            _credit("p3", "solo3"),
            _credit("p4", "solo1"),
            _credit("p5", "solo2"),
        ]
        G = nx.Graph()
        G.add_edges_from([("p0", "p1"), ("p0", "p2"), ("p0", "p3")])

        result = compute_independent_value(
            features, credits, anime_map, collaboration_graph=G
        )
        # p0 has 3 collaborators from graph (p1, p2, p3) — meets MIN_COLLABORATORS
        # The collaboration_graph restricts neighbors instead of credit co-occurrence
        assert result["p0"] is not None
