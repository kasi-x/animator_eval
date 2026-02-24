"""Structured logging utilities for scrapers."""

import time
from contextlib import contextmanager

import structlog

logger = structlog.get_logger()


@contextmanager
def scraper_timer(source: str, operation: str):
    """Context manager for timing scraper operations with structured logging.

    Args:
        source: Data source name (e.g., 'anilist', 'mal', 'jvmg')
        operation: Operation name (e.g., 'fetch_anime', 'fetch_staff')

    Yields:
        Dictionary to update with operation metrics
    """
    metrics = {}
    start = time.time()
    try:
        yield metrics
        elapsed_ms = int((time.time() - start) * 1000)
        metrics["elapsed_ms"] = elapsed_ms
        metrics["source"] = source
    finally:
        if "elapsed_ms" not in metrics:
            metrics["elapsed_ms"] = int((time.time() - start) * 1000)
        if "source" not in metrics:
            metrics["source"] = source


def log_scraper_start(source: str, operation: str, **kwargs):
    """Log scraper operation start.

    Args:
        source: Data source name
        operation: Operation name
        **kwargs: Additional fields
    """
    event = f"{source}_{operation}_started"
    logger.info(event, source=source, **kwargs)


def log_scraper_complete(
    source: str, operation: str, item_count: int, elapsed_ms: int, **kwargs
):
    """Log scraper operation completion.

    Args:
        source: Data source name
        operation: Operation name
        item_count: Number of items processed
        elapsed_ms: Elapsed time in milliseconds
        **kwargs: Additional fields
    """
    event = f"{source}_{operation}_complete"
    logger.info(
        event, source=source, item_count=item_count, elapsed_ms=elapsed_ms, **kwargs
    )


def log_rate_limit(source: str, retry_after_seconds: int, attempt: int = 1, **kwargs):
    """Log rate limit event.

    Args:
        source: Data source name
        retry_after_seconds: Seconds to wait before retry
        attempt: Current attempt number
        **kwargs: Additional fields
    """
    logger.warning(
        "rate_limited",
        source=source,
        retry_after_seconds=retry_after_seconds,
        attempt=attempt,
        **kwargs,
    )


def log_error(
    source: str, operation: str, error: Exception, attempt: int = 1, **kwargs
):
    """Log error event with structured fields.

    Args:
        source: Data source name
        operation: Operation name
        error: Exception that was raised
        attempt: Current attempt number
        **kwargs: Additional fields
    """
    event = f"{source}_{operation}_failed"
    logger.error(
        event,
        source=source,
        error_type=type(error).__name__,
        error_message=str(error),
        attempt=attempt,
        **kwargs,
    )


def log_retry(source: str, operation: str, attempt: int, wait_seconds: float, **kwargs):
    """Log retry event.

    Args:
        source: Data source name
        operation: Operation name
        attempt: Attempt number
        wait_seconds: Seconds to wait before retry
        **kwargs: Additional fields
    """
    logger.warning(
        "retry_attempt",
        source=source,
        operation=operation,
        attempt=attempt,
        wait_seconds=wait_seconds,
        **kwargs,
    )
