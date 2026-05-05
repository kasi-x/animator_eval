"""Regression tests for Checkpoint resume / silent-fail prevention.

Background: anilist_scraper used `Checkpoint(path)` expecting auto-load.
The old API returned an empty dict instead, silently re-processing all
fetched IDs (20K+ anime). These tests pin the contract so it cannot
regress.
"""
from pathlib import Path

from src.scrapers.checkpoint import Checkpoint, resolve_checkpoint


def _seed(path: Path, payload: dict) -> None:
    Checkpoint(path, data=payload).save(stamp_time=False)


def test_init_auto_loads_existing_file(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1", "a:2"], "last_index": 2})

    cp = Checkpoint(p)

    assert cp.get("last_index") == 2
    assert cp.get("fetched_ids") == ["a:1", "a:2"]


def test_init_missing_file_returns_default_schema(tmp_path: Path) -> None:
    cp = Checkpoint(tmp_path / "missing.json")

    assert cp.get("fetched_ids", []) == []
    assert cp.get("last_index", 0) == 0
    assert "completed_ids" in cp.data


def test_init_force_empty_ignores_existing_file(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1"], "last_index": 1})

    cp = Checkpoint(p, force_empty=True)

    assert cp.get("fetched_ids", []) == []
    assert cp.get("last_index", 0) == 0


def test_init_explicit_data_overrides_file(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1"]})

    cp = Checkpoint(p, data={"fetched_ids": ["b:2"]})

    assert cp.get("fetched_ids") == ["b:2"]


def test_load_classmethod_equivalent_to_init(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1"], "last_index": 1})

    cp_init = Checkpoint(p)
    cp_load = Checkpoint.load(p)

    assert cp_init.data == cp_load.data


def test_resolve_resume_loads_existing(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1", "a:2"], "last_index": 2})

    cp = resolve_checkpoint(p, force=False, resume=True)

    assert cp.get("last_index") == 2
    assert cp.get("fetched_ids") == ["a:1", "a:2"]


def test_resolve_force_returns_empty(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1"], "last_index": 5})

    cp = resolve_checkpoint(p, force=True, resume=True)

    assert cp.get("fetched_ids", []) == []
    assert cp.get("last_index", 0) == 0


def test_resolve_resume_false_returns_empty(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    _seed(p, {"fetched_ids": ["a:1"]})

    cp = resolve_checkpoint(p, force=False, resume=False)

    assert cp.get("fetched_ids", []) == []


def test_resolve_missing_file_returns_empty(tmp_path: Path) -> None:
    cp = resolve_checkpoint(tmp_path / "absent.json", force=False, resume=True)

    assert cp.get("fetched_ids", []) == []


def test_save_then_reload_round_trip(tmp_path: Path) -> None:
    """End-to-end: save 48 IDs, reload via Checkpoint(path), confirm 48 IDs visible."""
    p = tmp_path / "cp.json"
    cp1 = Checkpoint(p)
    cp1.data["fetched_ids"] = [f"anilist:{i}" for i in range(48)]
    cp1.data["last_index"] = 48
    cp1.save(stamp_time=False)

    cp2 = Checkpoint(p)

    assert len(cp2.get("fetched_ids", [])) == 48
    assert cp2.get("last_index") == 48
