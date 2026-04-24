"""bangumi API URL builders and constants.

Mirrors the structure of ``queries/anilist.py`` but for the bangumi /v0 REST API.
"""

BANGUMI_API_BASE = "https://api.bgm.tv"

DEFAULT_USER_AGENT = "animetor_eval/0.1 (https://github.com/kashi-x)"


def subject_persons_url(subject_id: int) -> str:
    """Return the URL for GET /v0/subjects/{id}/persons."""
    return f"{BANGUMI_API_BASE}/v0/subjects/{subject_id}/persons"


def subject_characters_url(subject_id: int) -> str:
    """Return the URL for GET /v0/subjects/{id}/characters."""
    return f"{BANGUMI_API_BASE}/v0/subjects/{subject_id}/characters"


def person_url(person_id: int) -> str:
    """Return the URL for GET /v0/persons/{id}."""
    return f"{BANGUMI_API_BASE}/v0/persons/{person_id}"


def character_url(character_id: int) -> str:
    """Return the URL for GET /v0/characters/{id}."""
    return f"{BANGUMI_API_BASE}/v0/characters/{character_id}"
