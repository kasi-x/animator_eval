"""Shared helpers for VA pipeline phases."""

from contextlib import contextmanager

import structlog

from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


@contextmanager
def va_step(context: PipelineContext, name: str):
    """Start-log + performance-measure wrapper for a VA pipeline step.

    Replaces the repeated pair:
        logger.info("step_start", step="va_foo")
        with context.monitor.measure("va_foo"):
            ...
    """
    logger.info("step_start", step=name)
    with context.monitor.measure(name):
        yield


def skip_if_no_va_credits(context: PipelineContext, phase_log_name: str) -> bool:
    """Return True (and log skip) when no VA credits exist, so the phase should return early."""
    if not context.va_credits:
        logger.debug(phase_log_name, reason="no_va_credits")
        return True
    return False
