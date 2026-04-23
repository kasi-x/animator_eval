"""Shared helpers for VA pipeline phases.

Re-exports common helpers from src.utils.pipeline_common and provides
VA-specific wrappers for convenience.
"""

import structlog

from src.utils.pipeline_common import phase_step, skip_if_no_credits

logger = structlog.get_logger()

# Re-export common helpers (prefer direct use of src.utils.pipeline_common)
__all__ = ["va_step", "skip_if_no_va_credits"]


def va_step(context, name: str):
    """Alias for phase_step — convenience wrapper for VA phases.

    Args:
        context: PipelineContext with monitor attribute
        name: Step name for logging

    Usage: with va_step(context, "va_foo"):
    """
    return phase_step(context, name)


def skip_if_no_va_credits(context, phase_log_name: str) -> bool:
    """Convenience wrapper: skip if context.va_credits is empty.

    Args:
        context: PipelineContext with va_credits attribute
        phase_log_name: Phase identifier for logging

    Returns:
        True if context.va_credits is empty, False otherwise.
    """
    return skip_if_no_credits(context, context.va_credits, phase_log_name)
