"""API input validation using Pydantic."""

import re
from typing import Annotated

from fastapi import HTTPException, Path, Query
from pydantic import AfterValidator, ValidationError


def validate_person_id(value: str) -> str:
    """Validate person_id format: source:p?digits (e.g., anilist:123 or mal:p456).

    Also allows simple alphanumeric IDs for backward compatibility.

    Args:
        value: Person ID string

    Returns:
        Validated person ID

    Raises:
        HTTPException: If format is invalid
    """
    # Length limit
    if len(value) > 100:
        raise HTTPException(status_code=400, detail="person_id too long (max 100 chars)")

    # Check for SQL injection-like patterns
    if re.search(r"[;'\"`]|--", value):
        raise HTTPException(status_code=400, detail="person_id contains invalid characters")

    # Alphanumeric + colons + underscores only
    if not re.match(r"^[a-zA-Z0-9:_-]+$", value):
        raise HTTPException(
            status_code=400,
            detail="person_id must contain only alphanumeric characters, colons, underscores, and hyphens",
        )

    return value


def validate_anime_id(value: str) -> str:
    """Validate anime_id format: source:digits (e.g., anilist:123, mal:456).

    Also allows simple alphanumeric IDs for backward compatibility.

    Args:
        value: Anime ID string

    Returns:
        Validated anime ID

    Raises:
        HTTPException: If format is invalid
    """
    # Length limit
    if len(value) > 100:
        raise HTTPException(status_code=400, detail="anime_id too long (max 100 chars)")

    # Check for SQL injection-like patterns
    if re.search(r"[;'\"`]|--", value):
        raise HTTPException(status_code=400, detail="anime_id contains invalid characters")

    # Alphanumeric + colons + underscores only
    if not re.match(r"^[a-zA-Z0-9:_-]+$", value):
        raise HTTPException(
            status_code=400,
            detail="anime_id must contain only alphanumeric characters, colons, underscores, and hyphens",
        )

    return value


def validate_query_string(value: str | None) -> str | None:
    """Validate search query strings for SQL injection patterns.

    Args:
        value: Query string

    Returns:
        Validated query string or None

    Raises:
        HTTPException: If dangerous patterns detected
    """
    if value is None:
        return None

    # Length limit
    if len(value) > 500:
        raise HTTPException(status_code=400, detail="Query string too long (max 500 chars)")

    # Check for SQL injection-like patterns (semicolons, quotes, comments, etc.)
    dangerous_patterns = [
        r"--",  # SQL comments
        r"/\*",  # Multi-line comments
        r"\*/",
        r";",   # Statement separator
        r"<script",  # XSS attempts
        r"javascript:",
        r"on\w+\s*=",  # Event handlers
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, value, re.IGNORECASE):
            raise HTTPException(
                status_code=400,
                detail=f"Query contains disallowed pattern: {pattern}",
            )

    return value


# Type annotations for FastAPI path and query parameters
PersonId = Annotated[str, Path(description="Person ID (format: source:p?digits)"), AfterValidator(validate_person_id)]
AnimeId = Annotated[str, Path(description="Anime ID (format: source:digits)"), AfterValidator(validate_anime_id)]

# SafeQueryString is just a validator function, use with Query() separately
SafeQueryString = str
