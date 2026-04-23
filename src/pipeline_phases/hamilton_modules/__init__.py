"""Hamilton module package for pipeline nodes (Phase 1-9).

H-1 (Phase 9 analysis): core, studio, genre, network, causal — ctx-based nodes.
H-2 (Phase 5-8 scoring): scoring, metrics, assembly — ctx-based, chained ordering.
H-3 (Phase 1-4 load/graph): loading, resolution — ctx-based, chained ordering.
H-4 will replace ctx with explicit typed inputs throughout.

Usage::

    from hamilton import driver
    from src.pipeline_phases.hamilton_modules import (
        loading, resolution, scoring, metrics, assembly,
        core, studio, genre, network, causal,
    )

    dr = (
        driver.Builder()
        .with_modules(loading, resolution, scoring, metrics, assembly,
                      core, studio, genre, network, causal)
        .build()
    )
    results = dr.execute(
        final_vars=["results_post_processed"] + core.NODE_NAMES + ...,
        inputs={"ctx": context},
    )
"""

from src.pipeline_phases.hamilton_modules import (
    assembly,
    causal,
    core,
    genre,
    loading,
    metrics,
    network,
    resolution,
    scoring,
    studio,
)

# Phase 9 analysis nodes (H-1)
ANALYSIS_NODE_NAMES: list[str] = (
    core.NODE_NAMES
    + studio.NODE_NAMES
    + genre.NODE_NAMES
    + network.NODE_NAMES
    + causal.NODE_NAMES
)

# Phase 5-8 scoring nodes (H-2)
SCORING_NODE_NAMES: list[str] = (
    scoring.NODE_NAMES
    + metrics.NODE_NAMES
    + assembly.NODE_NAMES
)

# Phase 1-4 loading/graph nodes (H-3)
LOADING_NODE_NAMES: list[str] = (
    loading.NODE_NAMES
    + resolution.NODE_NAMES
)

# All node names (Phase 1-9)
ALL_NODE_NAMES: list[str] = (
    LOADING_NODE_NAMES
    + SCORING_NODE_NAMES
    + ANALYSIS_NODE_NAMES
)

__all__ = [
    "assembly",
    "causal",
    "core",
    "genre",
    "loading",
    "metrics",
    "network",
    "resolution",
    "scoring",
    "studio",
    "ALL_NODE_NAMES",
    "ANALYSIS_NODE_NAMES",
    "LOADING_NODE_NAMES",
    "SCORING_NODE_NAMES",
]
