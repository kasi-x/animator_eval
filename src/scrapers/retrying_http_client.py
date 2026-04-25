"""Async HTTP client with exponential backoff retry + X-RateLimit-* callback."""

import time
import asyncio
import httpx
import structlog

log = structlog.get_logger()


class RetryingHttpClient:
    """Wrapper around httpx.AsyncClient with retry logic + rate limit callbacks.

    Handles:
    - Exponential backoff retries (5 attempts)
    - 429 rate limit detection with X-RateLimit-* header parsing
    - Async rate limit wait with optional callback
    - Per-request or global rate limit callback hook
    """

    def __init__(self, timeout: float = 60.0):
        """Initialize with httpx AsyncClient."""
        self._client = httpx.AsyncClient(timeout=timeout)
        # Global rate limit callback: signature: on_rate_limit(remaining_secs: int | None)
        # None = wait ended, int = remaining seconds
        self.on_rate_limit: callable | None = None

    async def close(self) -> None:
        """Close underlying httpx client."""
        await self._client.aclose()

    async def post(
        self,
        url: str,
        json: dict = None,
        headers: dict = None,
        on_rate_limit: callable = None,
        rate_limit_context: dict = None,
    ) -> httpx.Response:
        """POST request with retries + rate limit handling.

        Args:
            url: Target URL
            json: JSON payload
            headers: HTTP headers
            on_rate_limit: Optional per-request callback (overrides global)
            rate_limit_context: dict to store X-RateLimit-* header values
                                (e.g., rate_limit_context={})

        Returns:
            httpx.Response (raises httpx.HTTPError on final failure)

        Callbacks receive:
            - secs (int): remaining seconds in rate limit window
            - None: wait ended
        """
        callback = on_rate_limit or self.on_rate_limit
        # Use provided dict if not None (even if empty), otherwise create new one
        context = rate_limit_context if rate_limit_context is not None else {}

        for attempt in range(5):
            try:
                resp = await self._client.post(
                    url, json=json, headers=headers
                )

                # Store rate limit headers
                if "X-RateLimit-Remaining" in resp.headers:
                    context["remaining"] = int(resp.headers["X-RateLimit-Remaining"])
                    context["reset_at"] = int(resp.headers.get("X-RateLimit-Reset", 0))
                    context["limit"] = int(resp.headers.get("X-RateLimit-Limit", 0))
                    log.debug(
                        "rate_limit_headers_stored",
                        remaining=context["remaining"],
                        reset_at=context["reset_at"],
                        limit=context["limit"],
                    )

                # 429: rate limited
                if resp.status_code == 429:
                    await self._handle_rate_limit(
                        resp, callback, context, attempt
                    )
                    continue

                # Permanent errors (no retry)
                if resp.status_code in (400, 401, 404):
                    return resp

                # Other errors: retry
                if not (200 <= resp.status_code < 300):
                    if attempt < 4:
                        await asyncio.sleep(2 ** (attempt + 1))
                    else:
                        resp.raise_for_status()
                    continue

                return resp

            except httpx.HTTPError as e:
                if attempt < 4:
                    await asyncio.sleep(2 ** (attempt + 1))
                else:
                    raise httpx.RequestError(f"All 5 attempts failed: {e}") from e

        raise httpx.RequestError("All 5 attempts returned 429")

    async def _handle_rate_limit(
        self, resp: httpx.Response, callback: callable, context: dict, attempt: int
    ) -> None:
        """Handle 429 rate limit response with exponential backoff.

        Waits using X-RateLimit-Reset header if available, otherwise Retry-After.
        Calls callback during wait in 0.5-second steps.
        """
        retry_after = int(resp.headers.get("Retry-After", 10))
        reset_header = resp.headers.get("X-RateLimit-Reset")

        if reset_header:
            reset_at = int(reset_header)
            wait_seconds = max(reset_at - time.time(), retry_after) + 1
        else:
            wait_seconds = retry_after + 1

        context["wait_seconds"] = int(wait_seconds)
        context["reset_header"] = reset_header

        log.warning(
            "rate_limited",
            wait_seconds=int(wait_seconds),
            reset_header=reset_header,
            retry_after=retry_after,
            status=resp.status_code,
        )

        # Wait in 0.5-second steps, calling callback each tick
        remaining = wait_seconds
        while remaining > 0:
            if callback:
                callback(int(remaining))
            await asyncio.sleep(0.5)
            remaining -= 0.5

        # Wait ended
        if callback:
            callback(None)

        # Probe after wait
        try:
            probe_resp = await self._client.post(
                "https://graphql.anilist.co",
                json={
                    "query": "query { SiteStatistics { anime { pageInfo { total } } } }",
                    "variables": {},
                },
                headers=resp.request.headers,
            )
            if "X-RateLimit-Remaining" in probe_resp.headers:
                context["remaining"] = int(probe_resp.headers["X-RateLimit-Remaining"])

            if probe_resp.status_code == 429:
                # Still rate limited, wait more
                extra_wait = int(probe_resp.headers.get("Retry-After", 30)) + 1
                log.warning(
                    "rate_limit_still_active",
                    extra_wait_seconds=extra_wait,
                )
                await asyncio.sleep(extra_wait)
        except Exception as e:
            log.warning(
                "rate_limit_probe_failed",
                error_type=type(e).__name__,
                error_message=str(e),
            )
