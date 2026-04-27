# P3-B: Bangumi GraphQL HTTP Client Unification

**Status:** ⏳ Pending  
**Priority:** Medium (HTTP client standardization)  
**Complexity:** Medium

## Overview

Replace manual retry loops in bangumi_graphql_scraper with `RetryingHttpClient`.

## Current Implementation

### Manual Retry Loops

**REST GET** (lines 334-382):
```python
for attempt in range(1, _MAX_ATTEMPTS + 1):
    await self._limiter.throttle()
    try:
        resp = await self._client.get(url)
    except httpx.TransportError as exc:
        # custom backoff + retry logic
```

**GraphQL POST** (lines 407-470):
```python
for attempt in range(1, _MAX_ATTEMPTS + 1):
    await self._limiter.throttle()
    try:
        resp = await self._client.post(BANGUMI_GRAPHQL_URL, ...)
    except httpx.TransportError as exc:
        # custom backoff + retry logic with Retry-After header
```

### Custom Backoff Calculation

```python
def _compute_backoff_sleep(attempt, status_code, retry_after_header):
    # Exponential backoff with Retry-After header support
    # Cap at 120 seconds
```

### Rate Limiting

```python
class _HostRateLimiter:
    """Async rate limiter for api.bgm.tv (1 req/sec floor)."""
    async def throttle(self):
        # Enforces 1 req/sec minimum
```

## RetryingHttpClient Features ✅

RetryingHttpClient already supports:
- ✅ HTTP 429 (rate limit) retry with Retry-After header support (http_client.py:58)
- ✅ Exponential backoff calculation (http_client.py:55, 62)
- ✅ Configurable max_attempts (default 8, need 5 for bangumi)
- ✅ Configurable initial_backoff (default 4.0)
- ✅ Network error retry (httpx.TransportError types)
- ✅ Structured logging via structlog

## Refactoring Steps

### Step 1: Create RetryingHttpClient instance

In BangumiGraphQLClient.__aenter__():
```python
self._retrying_client = RetryingHttpClient(
    source="bangumi",
    delay=0.0,  # Rate limiting handled by _HostRateLimiter
    timeout=30.0,
    headers={"User-Agent": DEFAULT_USER_AGENT},
    max_attempts=_MAX_ATTEMPTS,  # 5
    initial_backoff=_BASE_DELAY,  # 2.0
)
```

### Step 2: Replace REST GET retry loop

Before:
```python
for attempt in range(1, _MAX_ATTEMPTS + 1):
    await self._limiter.throttle()
    try:
        resp = await self._client.get(url)
    except httpx.TransportError as exc:
        # retry logic
```

After:
```python
await self._limiter.throttle()
resp = await self._retrying_client.get(url)  # Built-in retry
```

### Step 3: Replace GraphQL POST retry loop

Similar pattern for POST requests.

### Step 4: Update error handling

RetryingHttpClient raises on final failure, so update try-except blocks:
```python
try:
    resp = await self._retrying_client.post(...)
except RateLimitError:
    # Handle rate limit
except httpx.HTTPStatusError:
    # Handle 4xx (non-429)
```

### Step 5: Remove manual retry logic

- Delete `_compute_backoff_sleep()` function (no longer needed)
- Delete manual `for attempt in range()` loops
- Keep `_HostRateLimiter` for rate floor (not retry logic)

## Considerations

### Rate Limiter Interaction

- `_HostRateLimiter` enforces 1 req/sec floor (throttle() in loop)
- RetryingHttpClient has its own `_delay` for request spacing (not rate limiting)
- Both can coexist: throttle() for floor, RetryingHttpClient for retry

### Error Logging

RetryingHttpClient logs:
```python
"http_request_error"  # on network error
"http_rate_limited"   # on 429
```

Custom bangumi logging:
```python
"bangumi_graphql_transport_error"
"bangumi_graphql_rate_limited"
"bangumi_graphql_retry_after_honored"
```

**Decision:** Keep bangumi-specific logging in error handlers, let RetryingHttpClient do the automatic retry.

### Status Code Handling

RetryingHttpClient retries:
- 429 (rate limit)
- 500, 502, 503, 504, 522, 524 (server errors)

Bangumi custom:
- 404 → returns `not_found` (special case)
- Non-2xx, non-429 → raise ScraperError

**Action:** Handle 404 before calling RetryingHttpClient, or use custom retry predicate.

## Testing

1. Mock RetryingHttpClient in tests
2. Verify error handling: 404, 429, 5xx, transport error
3. Verify rate limiter still enforces 1 req/sec floor
4. Check logs are still generated correctly

## Related Files

- `src/scrapers/bangumi_graphql_scraper.py` (lines 334-382, 407-470)
- `src/scrapers/http_client.py` (RetryingHttpClient)
- `src/scrapers/exceptions.py` (RateLimitError, ScraperError)
- `tests/` (if bangumi tests exist)
