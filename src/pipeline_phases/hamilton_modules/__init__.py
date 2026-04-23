"""Hamilton module package for Phase 9 analysis nodes.

Each submodule contains pure functions that Hamilton treats as DAG nodes.
All functions take ctx: PipelineContext as their sole input (H-1 pattern).
H-2 will decompose PipelineContext into explicit typed inputs.

Usage::

    from hamilton import driver
    from hamilton.execution import executors
    from src.pipeline_phases.hamilton_modules import core, studio, genre, network

    dr = (
        driver.Builder()
        .with_modules(core, studio, genre, network)
        .with_executor(executors.SynchronousLocalTaskExecutor())
        .build()
    )
    results = dr.execute(
        final_vars=core.NODE_NAMES + studio.NODE_NAMES + genre.NODE_NAMES + network.NODE_NAMES,
        inputs={"ctx": context},
    )
"""

from src.pipeline_phases.hamilton_modules import core, studio, genre, network, causal

ALL_NODE_NAMES: list[str] = (
    core.NODE_NAMES
    + studio.NODE_NAMES
    + genre.NODE_NAMES
    + network.NODE_NAMES
    + causal.NODE_NAMES
)

__all__ = ["core", "studio", "genre", "network", "causal", "ALL_NODE_NAMES"]
