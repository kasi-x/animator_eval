"""Common scraper exception hierarchy."""


class ScraperError(Exception):
    """Base exception for scrapers.

    Attributes:
        source: data source name (e.g. "anilist", "mal", "mediaarts", "wikidata")
        url: request target URL
        metadata: dict of additional information
    """

    def __init__(
        self,
        message: str,
        *,
        source: str = "",
        url: str = "",
        metadata: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.source = source
        self.url = url
        self.metadata = metadata or {}


class RateLimitError(ScraperError):
    """Exception raised when a rate limit is hit.

    Attributes:
        retry_after: seconds to wait before retrying (from Retry-After header)
    """

    def __init__(
        self,
        message: str = "Rate limited",
        *,
        source: str = "",
        url: str = "",
        retry_after: float = 60.0,
        metadata: dict | None = None,
    ) -> None:
        super().__init__(message, source=source, url=url, metadata=metadata)
        self.retry_after = retry_after


class AuthenticationError(ScraperError):
    """Authentication failure (invalid token, etc.)."""


class DataParseError(ScraperError):
    """Failed to parse the response."""


class EndpointUnreachableError(ScraperError):
    """Endpoint unreachable (retry limit exceeded)."""


class ContentValidationError(ScraperError):
    """Downloaded content failed validation (wrong Content-Type, insufficient size, etc.)."""
