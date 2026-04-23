"""Hamilton lifecycle hooks — observability (H-5) and crash resume (§5.6).

TimingHook:      logs per-node wall-clock time via structlog.
CheckpointHook:  saves a gzip-JSON checkpoint after results_post_processed (Phase 8).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import structlog
from hamilton.lifecycle import NodeExecutionHook

log = structlog.get_logger()


class TimingHook(NodeExecutionHook):
    """Records wall-clock execution time for each Hamilton node via structlog.

    Usage::

        from hamilton import driver
        from src.pipeline_phases.lifecycle import TimingHook

        dr = driver.Builder().with_modules(...).with_adapters(TimingHook()).build()

    Log line example::

        node_executed node=akm_estimation stage=phase5 cost=expensive elapsed_s=2.341
    """

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._t0: float = time.perf_counter()

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        elapsed = time.perf_counter() - self._t0
        log.info(
            "node_executed",
            node=node_name,
            stage=node_tags.get("stage", "unknown"),
            cost=node_tags.get("cost", "unknown"),
            domain=node_tags.get("domain", "unknown"),
            elapsed_s=round(elapsed, 4),
        )


class CheckpointHook(NodeExecutionHook):
    """Saves a crash-resume checkpoint after results_post_processed completes (Phase 8).

    Checkpoint is written to ``checkpoint_dir/pipeline_checkpoint.json.gz`` via
    :class:`~src.pipeline_phases.context.PipelineCheckpoint`.  On resume,
    ``pipeline.py`` loads the checkpoint, re-runs Phases 1-4 to reconstruct raw
    data, restores Phase 5-8 scores/results from the checkpoint, then skips
    straight to Phase 9 analysis and export.

    Usage::

        hook = CheckpointHook(checkpoint_dir=JSON_DIR)
        dr = driver.Builder().with_modules(...).with_adapters(TimingHook(), hook).build()
    """

    _CHECKPOINT_NODE = "results_post_processed"

    def __init__(self, checkpoint_dir: Path) -> None:
        self._checkpoint_dir = checkpoint_dir

    def run_before_node_execution(self, *, node_name: str, **kwargs: Any) -> None:
        pass

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_kwargs: dict[str, Any],
        success: bool,
        **kwargs: Any,
    ) -> None:
        if node_name != self._CHECKPOINT_NODE or not success:
            return
        ctx = node_kwargs.get("ctx")
        if ctx is None:
            return
        from src.pipeline_phases.context import PipelineCheckpoint

        try:
            PipelineCheckpoint(self._checkpoint_dir).save(8, ctx)
        except Exception:
            log.warning("checkpoint_save_failed", node=node_name)
