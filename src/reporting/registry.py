"""Report registry: maps slug → (definition builder, data provider).

``REPORT_REGISTRY`` is populated by ``register()`` calls in each
``definitions/{slug}.py`` module. The CLI iterates over this registry to
discover available reports.

Example usage inside ``definitions/compensation_fairness.py``::

    from src.reporting.registry import register

    register(
        slug="compensation_fairness",
        build_spec=build_spec,       # () -> ReportSpec
        provide=provide,             # (json_dir: Path) -> dict
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class RegistryEntry:
    """One entry in the report registry."""

    slug: str
    build_spec: Callable[[], Any]          # () -> ReportSpec
    provide: Callable[[Path], dict]        # (json_dir) -> data dict


# Global mutable registry
_REGISTRY: dict[str, RegistryEntry] = {}


def register(
    slug: str,
    build_spec: Callable[[], Any],
    provide: Callable[[Path], dict],
) -> None:
    """Register a report definition + provider pair."""
    if slug in _REGISTRY:
        raise ValueError(f"Duplicate report slug: {slug!r}")
    _REGISTRY[slug] = RegistryEntry(slug=slug, build_spec=build_spec, provide=provide)


def get_entry(slug: str) -> RegistryEntry:
    """Look up a registered report by slug."""
    if slug not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise KeyError(f"Unknown report slug {slug!r}. Available: {available}")
    return _REGISTRY[slug]


def all_slugs() -> list[str]:
    """Return all registered report slugs, sorted."""
    return sorted(_REGISTRY)


def get_registry() -> dict[str, RegistryEntry]:
    """Return a copy of the full registry."""
    return dict(_REGISTRY)


def clear_registry() -> None:
    """Clear the registry (for testing)."""
    _REGISTRY.clear()
