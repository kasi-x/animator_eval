"""Common utilities for pipeline phases — shared patterns for DRY.

This module provides reusable helpers for both main and VA pipeline phases,
located in src/utils to avoid circular import issues.

Helpers:
- phase_step: Unified logging + monitoring wrapper
- skip_if_no_credits: Generic credit existence check
"""

from contextlib import contextmanager

import structlog

logger = structlog.get_logger()


@contextmanager
def phase_step(context, name: str):
    """Start-log + performance-measure wrapper for any pipeline step.

    Args:
        context: dict instance with monitor attribute
        name: Step name for logging

    Usage:
        with phase_step(context, "my_step"):
            # compute results
    """
    logger.info("step_start", step=name)
    with context.monitor.measure(name):
        yield


def skip_if_no_credits(context, credits: list, phase_log_name: str) -> bool:
    """Return True (and log skip) when credits list is empty.

    Generic version supporting both main and VA pipelines.

    Args:
        context: dict instance (for type hint context)
        credits: Credit list to check
        phase_log_name: Phase identifier for logging

    Returns:
        True if credits is empty (caller should return early), False otherwise.
    """
    if not credits:
        logger.debug(phase_log_name, reason="no_credits")
        return True
    return False


__all__ = [
    "phase_step",
    "skip_if_no_credits",
]
