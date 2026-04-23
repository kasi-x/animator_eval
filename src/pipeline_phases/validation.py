"""Phase 2: Validation — data quality checks."""

import structlog

from src.analysis.io.silver_reader import silver_connect
from src.pipeline_phases.context import PipelineContext
from src.infra.validation import ValidationResult, validate_all

logger = structlog.get_logger()


def run_validation_phase(context: PipelineContext) -> ValidationResult:
    """Run data validation checks against silver.duckdb.

    Returns:
        ValidationResult with passed flag, errors, and warnings
    """
    with context.monitor.measure("validation"):
        with silver_connect() as conn:
            validation = validate_all(conn)

    if not validation.passed:
        for err in validation.errors:
            logger.error("validation_error", message=err)
    for warn in validation.warnings:
        logger.warning("validation_warning", message=warn)

    return validation
