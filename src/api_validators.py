"""Re-export shim — canonical location is ``src.routers.validators``.

Keep this file for backwards compatibility with existing imports.
"""

from src.routers.validators import (  # noqa: F401
    AnimeId,
    PersonId,
    SafeQueryString,
    validate_anime_id,
    validate_person_id,
    validate_query_string,
)
