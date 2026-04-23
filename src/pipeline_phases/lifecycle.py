"""Hamilton lifecycle hooks for observability (H-5).

TimingHook: logs per-node execution time via structlog.
"""

from __future__ import annotations

import time
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
