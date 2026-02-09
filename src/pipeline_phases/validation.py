"""Phase 2: Validation — data quality checks."""
import sqlite3

import structlog

from src.pipeline_phases.context import PipelineContext
from src.validation import ValidationResult, validate_all

logger = structlog.get_logger()


def run_validation_phase(context: PipelineContext, conn: sqlite3.Connection) -> ValidationResult:
    """Run data validation checks.

    Args:
        context: Pipeline context
        conn: Database connection

    Returns:
        ValidationResult with passed flag, errors, and warnings
    """
    with context.monitor.measure("validation"):
        validation = validate_all(conn)

    if not validation.passed:
        for err in validation.errors:
            logger.error("validation_error", message=err)
    for warn in validation.warnings:
        logger.warning("validation_warning", message=warn)

    return validation
