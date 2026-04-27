"""Role mapper registry: source-specific raw role string → normalized Role.value.

Each mapper module registers itself via @register(source).
map_role(source, raw) is the single entry point used by ETL loaders.
"""
from __future__ import annotations

from collections.abc import Callable

MAPPERS: dict[str, Callable[[str], str]] = {}


def register(source: str) -> Callable[[Callable[[str], str]], Callable[[str], str]]:
    """Class decorator that registers a mapper function for the given source name."""
    def deco(fn: Callable[[str], str]) -> Callable[[str], str]:
        MAPPERS[source] = fn
        return fn
    return deco


def map_role(source: str, raw: str) -> str:
    """Map a raw role string to a normalized Role.value for the given source.

    Falls back to Role.OTHER.value when a mapper exists but raw is unmapped.
    Falls back to the identity (raw unchanged) when no mapper is registered
    for the source — preserves forward-compatibility with new sources.
    """
    fn = MAPPERS.get(source)
    if fn is None:
        return raw
    return fn(raw)


# Import all mapper modules so their @register decorators execute.
from . import anilist, ann, bangumi, keyframe, mal, mediaarts, sakuga_atwiki, seesaawiki  # noqa: E402, F401
