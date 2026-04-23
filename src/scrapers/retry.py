"""Shared async retry utility for scrapers."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

from src.scrapers.exceptions import (
    EndpointUnreachableError,
    RateLimitError,
    ScraperError,
)

log = structlog.get_logger()

T = TypeVar("T")


async def retry_async(
    fn: Callable[..., Awaitable[T]],
    *args: object,
    max_attempts: int = 5,
    base_delay: float = 2.0,
    source: str = "",
    **kwargs: object,
) -> T:
    """Retry an async function with exponential backoff.

    Respects RateLimitError.retry_after. Raises EndpointUnreachableError
    when all attempts are exhausted.

    Args:
        fn: async function to retry
        *args: positional arguments passed to fn
        max_attempts: maximum number of attempts
        base_delay: initial backoff seconds (grows exponentially)
        source: data source name for logging
        **kwargs: keyword arguments passed to fn

    Returns:
        return value of fn

    Raises:
        EndpointUnreachableError: when all retries fail
    """
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await fn(*args, **kwargs)
        except RateLimitError as e:
            last_error = e
            wait = e.retry_after
            log.warning(
                "rate_limited",
                source=source,
                attempt=attempt,
                max_attempts=max_attempts,
                retry_after_seconds=wait,
            )
            await asyncio.sleep(wait)
        except ScraperError as e:
            last_error = e
            if attempt >= max_attempts:
                break
            wait = base_delay * (2 ** (attempt - 1))
            log.warning(
                "retry_attempt",
                source=source,
                attempt=attempt,
                max_attempts=max_attempts,
                wait_seconds=wait,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            await asyncio.sleep(wait)

    raise EndpointUnreachableError(
        f"Failed after {max_attempts} attempts: {last_error}",
        source=source,
        metadata={"last_error": str(last_error)},
    )
