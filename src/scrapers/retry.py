"""スクレイパー共通の非同期リトライユーティリティ."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

from src.scrapers.exceptions import EndpointUnreachableError, RateLimitError, ScraperError

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
    """非同期関数を指数バックオフ付きでリトライする.

    RateLimitError の retry_after を尊重する。
    全リトライ失敗時は EndpointUnreachableError を送出する。

    Args:
        fn: リトライ対象の非同期関数
        *args: fn に渡す位置引数
        max_attempts: 最大試行回数
        base_delay: 初回バックオフ秒数 (指数的に増加)
        source: ログ用のデータソース名
        **kwargs: fn に渡すキーワード引数

    Returns:
        fn の戻り値

    Raises:
        EndpointUnreachableError: 全リトライ失敗時
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
