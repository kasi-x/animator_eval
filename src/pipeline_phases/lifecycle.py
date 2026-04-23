"""Hamilton lifecycle hooks — observability (H-5) and crash resume (§5.6).

TimingHook:      logs per-node wall-clock time via structlog.
CheckpointHook:  saves a gzip-JSON checkpoint after ctx_results_populated (Phase 8).
"""

from __future__ import annotations

import gzip
import json
import time
from pathlib import Path
from typing import Any

import structlog
from hamilton.lifecycle import NodeExecutionHook

log = structlog.get_logger()

_CHECKPOINT_NODE = "ctx_results_populated"
_CHECKPOINT_FILE = "pipeline_checkpoint.json.gz"

_CHECKPOINT_FIELDS = [
    "iv_scores", "iv_scores_historical", "person_fe", "studio_fe",
    "birank_person_scores", "birank_anime_scores",
    "patronage_scores", "dormancy_scores", "iv_lambda_weights", "results",
]


class PipelineCheckpoint:
    """Load, validate, restore, and delete gzip-JSON pipeline checkpoints."""

    def __init__(self, checkpoint_dir: Path) -> None:
        self._path = Path(checkpoint_dir) / _CHECKPOINT_FILE

    def load(self) -> dict | None:
        if not self._path.exists():
            return None
        try:
            with gzip.open(self._path, "rt", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            log.warning("checkpoint_load_failed", path=str(self._path))
            return None

    def is_compatible(self, checkpoint: dict, ctx: Any) -> bool:
        ckpt_persons = set(checkpoint.get("person_fe", {}).keys())
        ctx_persons = {c.person_id for c in getattr(ctx, "credits", [])}
        return bool(ckpt_persons) and ckpt_persons == ctx_persons

    def restore_to_context(self, checkpoint: dict, ctx: Any) -> None:
        for field in _CHECKPOINT_FIELDS:
            if field in checkpoint:
                setattr(ctx, field, checkpoint[field])

    def delete(self) -> None:
        self._path.unlink(missing_ok=True)


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
    """Saves a gzip-JSON checkpoint after ctx_results_populated (Phase 8).

    Extracts ctx from Hamilton node_kwargs and serializes score fields + results.
    The checkpoint is used by the resume path in pipeline.py.
    """

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
        if node_name != _CHECKPOINT_NODE or not success:
            return
        ctx = node_kwargs.get("ctx")
        if ctx is None:
            return
        payload = {
            "last_completed_phase": 8,
            "iv_scores": dict(getattr(ctx, "iv_scores", {})),
            "iv_scores_historical": dict(getattr(ctx, "iv_scores_historical", {})),
            "person_fe": dict(getattr(ctx, "person_fe", {})),
            "studio_fe": dict(getattr(ctx, "studio_fe", {})),
            "birank_person_scores": dict(getattr(ctx, "birank_person_scores", {})),
            "birank_anime_scores": dict(getattr(ctx, "birank_anime_scores", {})),
            "patronage_scores": dict(getattr(ctx, "patronage_scores", {})),
            "dormancy_scores": dict(getattr(ctx, "dormancy_scores", {})),
            "iv_lambda_weights": dict(getattr(ctx, "iv_lambda_weights", {})),
            "results": list(getattr(ctx, "results", [])),
        }
        ckpt_path = self._checkpoint_dir / _CHECKPOINT_FILE
        with gzip.open(ckpt_path, "wt", encoding="utf-8") as f:
            json.dump(payload, f)
        log.info("checkpoint_saved", path=str(ckpt_path), phase=8)
