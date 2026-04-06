"""Tests for report registry."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.reporting.registry import (
    all_slugs,
    clear_registry,
    get_entry,
    register,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Ensure each test starts and ends with a clean registry."""
    clear_registry()
    yield
    clear_registry()


def _dummy_spec():
    return "spec"


def _dummy_provide(json_dir: Path) -> dict:
    return {"key": "value"}


def test_register_and_get() -> None:
    register("test_report", _dummy_spec, _dummy_provide)
    entry = get_entry("test_report")
    assert entry.slug == "test_report"
    assert entry.build_spec() == "spec"
    assert entry.provide(Path(".")) == {"key": "value"}


def test_all_slugs_sorted() -> None:
    register("zzz", _dummy_spec, _dummy_provide)
    register("aaa", _dummy_spec, _dummy_provide)
    assert all_slugs() == ["aaa", "zzz"]


def test_duplicate_slug_raises() -> None:
    register("dup", _dummy_spec, _dummy_provide)
    with pytest.raises(ValueError, match="Duplicate"):
        register("dup", _dummy_spec, _dummy_provide)


def test_get_unknown_raises() -> None:
    with pytest.raises(KeyError, match="Unknown"):
        get_entry("does_not_exist")


def test_clear_registry() -> None:
    register("x", _dummy_spec, _dummy_provide)
    assert all_slugs() == ["x"]
    clear_registry()
    assert all_slugs() == []
