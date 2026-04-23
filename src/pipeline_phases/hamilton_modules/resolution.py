"""Phase 3+4: Entity Resolution and Graph Construction nodes for Hamilton DAG (H-3).

Two nodes:
  - entity_resolution_run: 5-step person/anime deduplication (Phase 3)
  - graphs_built: bipartite + collaboration graph + community detection (Phase 4)

Both use ctx: PipelineContext (H-3 pattern).
H-4 will expose entity_resolution's 5 steps as separate nodes for independent testing.
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.context import PipelineContext

NODE_NAMES: list[str] = [
    "entity_resolution_run",
    "graphs_built",
]


@tag(stage="phase3", cost="moderate", domain="resolution")
def entity_resolution_run(ctx: PipelineContext, data_validated: Any) -> Any:
    """5-step entity resolution: exact → cross-source → romaji → similarity → AI (Phase 3).

    Deduplicates person and anime entities across sources (AniList, MADB, etc.),
    merges duplicate credits, and updates ctx.credits with resolved IDs.

    Writes: ctx.canonical_map, ctx.credits (resolved and merged).
    Depends on data_validated to run after Phase 2.
    """
    from src.pipeline_phases.entity_resolution import run_entity_resolution

    run_entity_resolution(ctx)
    return {
        "canonical_persons": len(ctx.canonical_map),
        "resolved_credits": len(ctx.credits),
    }


@tag(stage="phase4", cost="expensive", domain="resolution")
def graphs_built(ctx: PipelineContext, entity_resolution_run: Any) -> Any:
    """Build person-anime bipartite graph, collaboration graph, community map (Phase 4).

    Graph edges use structural weights only: role_weight × episode_coverage × duration_mult.
    Community detection uses Louvain (≤1M edges) or LPA (>1M edges).

    Writes: ctx.person_anime_graph, ctx.collaboration_graph, ctx.community_map.
    Depends on entity_resolution_run to use resolved person IDs.
    """
    from src.pipeline_phases.graph_construction import build_graphs_phase

    build_graphs_phase(ctx)
    return {
        "graph_nodes": ctx.person_anime_graph.number_of_nodes() if ctx.person_anime_graph else 0,
        "collab_edges": (
            ctx.collaboration_graph.number_of_edges()
            if ctx.collaboration_graph is not None
            else 0
        ),
        "communities": len(set(ctx.community_map.values())) if ctx.community_map else 0,
    }
