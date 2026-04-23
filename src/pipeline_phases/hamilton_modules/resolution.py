"""Phase 3+4: Entity Resolution and Graph Construction nodes for Hamilton DAG (H-4).

Four nodes:
  - entity_resolution_run: 5-step person/anime deduplication (Phase 3)
  - graphs_built: bipartite + collaboration graph + community detection (Phase 4)
  - entity_resolved: H-4 bridge — exposes EntityResolutionResult for scoring nodes
  - graphs_result: H-4 bridge — exposes GraphsResult for scoring nodes
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.pipeline_types import EntityResolutionResult, GraphsResult

NODE_NAMES: list[str] = [
    "entity_resolution_run",
    "graphs_built",
    "entity_resolved",
    "graphs_result",
]


@tag(stage="phase3", cost="moderate", domain="resolution")
def entity_resolution_run(ctx: dict, data_validated: Any) -> Any:
    """5-step entity resolution: exact → cross-source → romaji → similarity → AI (Phase 3).

    Deduplicates person and anime entities across sources (AniList, MADB, etc.),
    merges duplicate credits, and updates ctx.credits with resolved IDs.

    Writes: ctx.canonical_map, ctx.credits (resolved and merged).
    Depends on data_validated to run after Phase 2.
    """
    from src.pipeline_phases.entity_resolution import run_entity_resolution
    from src.pipeline_phases.pipeline_types import LoadedData

    loaded = LoadedData(
        persons=list(ctx.persons),
        credits=list(ctx.credits),
        anime_list=list(ctx.anime_list),
    )
    result = run_entity_resolution(loaded)

    ctx.canonical_map = result.canonical_map
    ctx.credits = result.resolved_credits
    ctx.persons = result.persons
    ctx.anime_list = result.anime_list
    ctx.anime_map = result.anime_map

    return {
        "canonical_persons": len(result.canonical_map),
        "resolved_credits": len(result.resolved_credits),
    }


@tag(stage="phase4", cost="expensive", domain="resolution")
def graphs_built(ctx: dict, entity_resolution_run: Any) -> Any:
    """Build person-anime bipartite graph, collaboration graph, community map (Phase 4).

    Graph edges use structural weights only: role_weight × episode_coverage × duration_mult.
    Community detection uses Louvain (≤1M edges) or LPA (>1M edges).

    Writes: ctx.person_anime_graph, ctx.collaboration_graph, ctx.community_map.
    Depends on entity_resolution_run to use resolved person IDs.
    """
    from src.pipeline_phases.graph_construction import build_graphs_phase
    from src.pipeline_phases.pipeline_types import EntityResolutionResult

    resolved = EntityResolutionResult(
        resolved_credits=list(ctx.credits),
        canonical_map=ctx.canonical_map,
        persons=list(ctx.persons),
        anime_list=list(ctx.anime_list),
        anime_map=dict(ctx.anime_map),
    )
    result = build_graphs_phase(resolved)

    ctx.person_anime_graph = result.person_anime_graph
    ctx.collaboration_graph = result.collaboration_graph
    ctx.community_map = result.community_map

    return {
        "graph_nodes": ctx.person_anime_graph.number_of_nodes() if ctx.person_anime_graph else 0,
        "collab_edges": (
            ctx.collaboration_graph.number_of_edges()
            if ctx.collaboration_graph is not None
            else 0
        ),
        "communities": len(set(ctx.community_map.values())) if ctx.community_map else 0,
    }


@tag(stage="phase4", cost="cheap", domain="resolution")
def entity_resolved(entity_resolution_run: Any, ctx: dict) -> EntityResolutionResult:
    """H-4 bridge: expose post-resolution data as EntityResolutionResult for scoring nodes."""
    return EntityResolutionResult(
        resolved_credits=ctx.credits,
        canonical_map=ctx.canonical_map,
        persons=ctx.persons,
        anime_list=ctx.anime_list,
        anime_map=ctx.anime_map,
    )


@tag(stage="phase4", cost="cheap", domain="resolution")
def graphs_result(graphs_built: Any, ctx: dict) -> GraphsResult:
    """H-4 bridge: expose post-graph-construction data as GraphsResult for scoring nodes."""
    return GraphsResult(
        person_anime_graph=ctx.person_anime_graph,
        collaboration_graph=ctx.collaboration_graph,
        community_map=ctx.community_map,
    )
