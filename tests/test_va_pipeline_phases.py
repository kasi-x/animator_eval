"""Smoke tests for VA pipeline phases (§6.3 pipeline phase coverage)."""

import pytest

from src.models import BronzeAnime, CharacterVoiceActor, Credit, Role
from src.pipeline_phases.context import PipelineContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _anime(aid, year=2020, episodes=12, duration=24):
    # Use BronzeAnime: production load_all_anime() returns BronzeAnime objects,
    # and VA analysis modules access .genres which only BronzeAnime has.
    return BronzeAnime(id=aid, title_en=f"Anime {aid}", year=year,
                       episodes=episodes, duration=duration)


def _cva(person_id, anime_id, char_id="c1", role="MAIN"):
    return CharacterVoiceActor(person_id=person_id, character_id=char_id,
                               anime_id=anime_id, character_role=role)


def _credit(pid, aid, role):
    return Credit(person_id=pid, anime_id=aid, role=role, raw_role=role.value)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_va_context():
    """PipelineContext with minimal VA data — 3 VAs, 2 SDs, 4 anime."""
    anime_map = {f"a{i}": _anime(f"a{i}", year=2020 + i) for i in range(4)}

    va_credits = [
        _cva("va1", "a0", "c1a", "MAIN"),
        _cva("va1", "a1", "c1b", "MAIN"),
        _cva("va2", "a0", "c2a", "MAIN"),
        _cva("va2", "a2", "c2b", "SUPPORTING"),
        _cva("va3", "a1", "c3a", "MAIN"),
        _cva("va3", "a3", "c3b", "MAIN"),
    ]

    prod_credits = [
        _credit("sd1", "a0", Role.SOUND_DIRECTOR),
        _credit("sd1", "a1", Role.SOUND_DIRECTOR),
        _credit("sd2", "a2", Role.SOUND_DIRECTOR),
        _credit("sd2", "a3", Role.SOUND_DIRECTOR),
    ]

    ctx = PipelineContext(visualize=False, dry_run=True)
    ctx.anime_map = anime_map
    ctx.anime_list = list(anime_map.values())
    ctx.va_credits = va_credits
    ctx.credits = prod_credits
    ctx.va_person_ids = {"va1", "va2", "va3"}
    return ctx


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestVaGraphConstructionPhase:
    def test_builds_va_graphs(self, minimal_va_context):
        from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
        build_va_graphs_phase(minimal_va_context)
        assert minimal_va_context.va_anime_graph is not None
        assert minimal_va_context.va_collaboration_graph is not None

    def test_va_anime_graph_has_edges(self, minimal_va_context):
        from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
        build_va_graphs_phase(minimal_va_context)
        assert minimal_va_context.va_anime_graph.number_of_edges() > 0

    def test_empty_va_credits_no_crash(self):
        ctx = PipelineContext(visualize=False, dry_run=True)
        ctx.va_credits = []
        ctx.credits = []
        ctx.anime_map = {}
        ctx.va_person_ids = set()
        from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
        build_va_graphs_phase(ctx)  # should not raise


class TestVaCoreScoringPhase:
    def test_scores_populated_after_run(self, minimal_va_context):
        from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
        from src.pipeline_phases.va_core_scoring import compute_va_core_scores_phase
        build_va_graphs_phase(minimal_va_context)
        compute_va_core_scores_phase(minimal_va_context)
        assert isinstance(minimal_va_context.va_iv_scores, dict)

    def test_skips_gracefully_without_va_credits(self):
        ctx = PipelineContext(visualize=False, dry_run=True)
        ctx.va_credits = []
        from src.pipeline_phases.va_core_scoring import compute_va_core_scores_phase
        compute_va_core_scores_phase(ctx)  # should not raise
        assert ctx.va_iv_scores == {}


class TestVaSupplementaryMetricsPhase:
    def test_runs_without_error(self, minimal_va_context):
        from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
        from src.pipeline_phases.va_core_scoring import compute_va_core_scores_phase
        from src.pipeline_phases.va_supplementary_metrics import (
            compute_va_supplementary_metrics_phase,
        )
        build_va_graphs_phase(minimal_va_context)
        compute_va_core_scores_phase(minimal_va_context)
        compute_va_supplementary_metrics_phase(minimal_va_context)

    def test_skips_gracefully_without_va_credits(self):
        ctx = PipelineContext(visualize=False, dry_run=True)
        ctx.va_credits = []
        from src.pipeline_phases.va_supplementary_metrics import (
            compute_va_supplementary_metrics_phase,
        )
        compute_va_supplementary_metrics_phase(ctx)  # should not raise


class TestVaResultAssemblyPhase:
    def test_assembles_results_list(self, minimal_va_context):
        from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
        from src.pipeline_phases.va_core_scoring import compute_va_core_scores_phase
        from src.pipeline_phases.va_result_assembly import assemble_va_results
        build_va_graphs_phase(minimal_va_context)
        compute_va_core_scores_phase(minimal_va_context)
        assemble_va_results(minimal_va_context)
        assert isinstance(minimal_va_context.va_results, list)

    def test_empty_credits_yields_empty_results(self):
        ctx = PipelineContext(visualize=False, dry_run=True)
        ctx.va_credits = []
        ctx.va_person_ids = set()
        from src.pipeline_phases.va_result_assembly import assemble_va_results
        assemble_va_results(ctx)
        assert ctx.va_results == []
