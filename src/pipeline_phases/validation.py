"""Phase 2: Validation — data quality checks."""

import structlog

from src.analysis.io.silver_reader import silver_connect
from src.infra.validation import ValidationResult, validate_all

logger = structlog.get_logger()


def run_validation_phase(loaded_data) -> ValidationResult:
    """Run data validation checks against silver.duckdb.

    Args:
        loaded_data: LoadedData from Phase 1 (used for logging)

    Returns:
        ValidationResult with passed flag, errors, and warnings
    """
    with silver_connect() as conn:
        validation = validate_all(conn)

    if not validation.passed:
        for err in validation.errors:
            logger.error("validation_error", message=err)
    for warn in validation.warnings:
        logger.warning("validation_warning", message=warn)

    return validation
