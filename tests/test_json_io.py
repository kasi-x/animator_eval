"""Tests for unified JSON I/O utilities."""

import json
from pathlib import Path

import pytest

from src.utils import json_io


def test_load_json_file_or_return_default_with_valid_file(tmp_path: Path) -> None:
    """Test loading a valid JSON file returns parsed data."""
    test_file = tmp_path / "test.json"
    test_data = {"key": "value", "count": 42}

    with open(test_file, "w") as f:
        json.dump(test_data, f)

    result = json_io.load_json_file_or_return_default(test_file, {})
    assert result == test_data


def test_load_json_file_or_return_default_with_missing_file(tmp_path: Path) -> None:
    """Test loading a missing file returns default."""
    missing_file = tmp_path / "missing.json"
    default = {"default": True}

    result = json_io.load_json_file_or_return_default(missing_file, default)
    assert result == default


def test_load_json_file_or_return_default_with_malformed_json(tmp_path: Path) -> None:
    """Test loading malformed JSON returns default."""
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{ invalid json }")

    default = []
    result = json_io.load_json_file_or_return_default(bad_file, default)
    assert result == default


def test_load_json_file_by_name_or_return_default(tmp_path: Path) -> None:
    """Test loading JSON by filename from a directory."""
    test_data = [1, 2, 3]
    test_file = tmp_path / "data.json"

    with open(test_file, "w") as f:
        json.dump(test_data, f)

    result = json_io.load_json_file_by_name_or_return_default(
        "data.json",
        [],
        json_dir=tmp_path,
    )
    assert result == test_data


def test_load_json_file_by_name_or_return_default_missing(tmp_path: Path) -> None:
    """Test loading missing file by name returns default."""
    result = json_io.load_json_file_by_name_or_return_default(
        "missing.json",
        {"empty": True},
        json_dir=tmp_path,
    )
    assert result == {"empty": True}


def test_load_json_file_with_caching_returns_data(tmp_path: Path) -> None:
    """Test cached loading returns correct data."""
    test_file = tmp_path / "cached.json"
    test_data = {"cached": True}

    with open(test_file, "w") as f:
        json.dump(test_data, f)

    result = json_io.load_json_file_with_caching(str(test_file), "dict")
    assert result == test_data


def test_load_json_file_with_caching_uses_cache(tmp_path: Path) -> None:
    """Test that subsequent calls use cached data."""
    test_file = tmp_path / "cached.json"
    test_data = {"version": 1}

    with open(test_file, "w") as f:
        json.dump(test_data, f)

    # First call loads from disk
    result1 = json_io.load_json_file_with_caching(str(test_file), "dict")
    assert result1 == test_data

    # Modify file on disk
    with open(test_file, "w") as f:
        json.dump({"version": 2}, f)

    # Second call should return cached data (version 1)
    result2 = json_io.load_json_file_with_caching(str(test_file), "dict")
    assert result2 == test_data
    assert result2["version"] == 1


def test_clear_json_cache_invalidates_cache(tmp_path: Path) -> None:
    """Test that clearing cache forces reload from disk."""
    test_file = tmp_path / "cached.json"

    with open(test_file, "w") as f:
        json.dump({"version": 1}, f)

    # First call loads from disk
    result1 = json_io.load_json_file_with_caching(str(test_file), "dict")
    assert result1["version"] == 1

    # Modify file
    with open(test_file, "w") as f:
        json.dump({"version": 2}, f)

    # Clear cache
    json_io.clear_json_cache()

    # Next call should load new data
    result2 = json_io.load_json_file_with_caching(str(test_file), "dict")
    assert result2["version"] == 2


def test_load_json_file_with_caching_list_default(tmp_path: Path) -> None:
    """Test cached loading with list default."""
    missing = tmp_path / "missing.json"
    result = json_io.load_json_file_with_caching(str(missing), "list")
    assert result == []


def test_load_json_file_with_caching_dict_default(tmp_path: Path) -> None:
    """Test cached loading with dict default."""
    missing = tmp_path / "missing.json"
    result = json_io.load_json_file_with_caching(str(missing), "dict")
    assert result == {}


def test_load_pipeline_json_with_caching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test loading pipeline JSON with caching."""
    # Monkeypatch JSON_DIR
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)

    test_data = [{"person_id": "p1", "score": 100}]
    test_file = tmp_path / "scores.json"

    with open(test_file, "w") as f:
        json.dump(test_data, f)

    result = json_io.load_pipeline_json_with_caching("scores.json", [])
    assert result == test_data


def test_save_json_to_file(tmp_path: Path) -> None:
    """Test saving data to JSON file."""
    output_file = tmp_path / "output.json"
    test_data = {"saved": True, "count": 42}

    json_io.save_json_to_file(test_data, output_file)

    # Verify file exists and contains correct data
    assert output_file.exists()
    with open(output_file) as f:
        loaded = json.load(f)
    assert loaded == test_data


def test_save_json_to_file_creates_parent_dir(tmp_path: Path) -> None:
    """Test saving creates parent directory if missing."""
    nested_dir = tmp_path / "nested" / "deep"
    output_file = nested_dir / "output.json"

    json_io.save_json_to_file({"test": True}, output_file, ensure_parent_dir=True)

    assert output_file.exists()
    assert nested_dir.exists()


def test_save_pipeline_json_if_data_present_saves_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test saving pipeline JSON when data is present."""
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)

    test_data = {"result": "success"}
    saved = json_io.save_pipeline_json_if_data_present(
        "test.json",
        test_data,
        log_message="test_saved",
    )

    assert saved is True
    output_file = tmp_path / "test.json"
    assert output_file.exists()

    with open(output_file) as f:
        loaded = json.load(f)
    assert loaded == test_data


def test_save_pipeline_json_if_data_present_skips_empty_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test saving pipeline JSON saves empty containers (but not None)."""
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)

    # Empty dict - should save (changed behavior to save empty containers)
    saved = json_io.save_pipeline_json_if_data_present("empty.json", {})
    assert saved is True
    assert (tmp_path / "empty.json").exists()

    # Empty list - should save
    saved = json_io.save_pipeline_json_if_data_present("empty_list.json", [])
    assert saved is True
    assert (tmp_path / "empty_list.json").exists()

    # None - should NOT save
    saved = json_io.save_pipeline_json_if_data_present("none.json", None)
    assert saved is False
    assert not (tmp_path / "none.json").exists()


def test_save_pipeline_json_if_data_present_skips_false_condition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test saving pipeline JSON skips when condition is False."""
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)

    test_data = {"data": "present"}
    saved = json_io.save_pipeline_json_if_data_present(
        "conditional.json",
        test_data,
        condition=False,
    )

    assert saved is False
    assert not (tmp_path / "conditional.json").exists()


def test_save_pipeline_json_if_data_present_respects_true_condition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test saving pipeline JSON respects True condition."""
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)

    test_data = {"data": "present"}
    saved = json_io.save_pipeline_json_if_data_present(
        "conditional.json",
        test_data,
        condition=True,
    )

    assert saved is True
    assert (tmp_path / "conditional.json").exists()


def test_named_loaders_return_defaults_when_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test all named loaders return appropriate defaults."""
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)
    json_io.clear_json_cache()

    # List loaders
    assert json_io.load_person_scores_from_json() == []
    assert json_io.load_collaboration_pairs_from_json() == []

    # Dict loaders
    assert json_io.load_anime_statistics_from_json() == {}
    assert json_io.load_pipeline_summary_from_json() == {}
    assert json_io.load_role_transitions_from_json() == {}
    assert json_io.load_cross_validation_results_from_json() == {}
    assert json_io.load_influence_tree_from_json() == {}
    assert json_io.load_studio_analysis_from_json() == {}
    assert json_io.load_seasonal_trends_from_json() == {}
    assert json_io.load_outlier_analysis_from_json() == {}
    assert json_io.load_team_patterns_from_json() == {}
    assert json_io.load_growth_trends_from_json() == {}
    assert json_io.load_time_series_from_json() == {}
    assert json_io.load_decade_analysis_from_json() == {}
    assert json_io.load_person_tags_from_json() == {}
    assert json_io.load_role_flow_from_json() == {}
    assert json_io.load_bridge_analysis_from_json() == {}
    assert json_io.load_mentorship_relationships_from_json() == {}
    assert json_io.load_career_milestones_from_json() == {}
    assert json_io.load_network_evolution_from_json() == {}
    assert json_io.load_genre_affinity_from_json() == {}
    assert json_io.load_productivity_metrics_from_json() == {}


def test_named_loaders_return_data_when_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test named loaders return correct data when files exist."""
    monkeypatch.setattr(json_io, "JSON_DIR", tmp_path)
    json_io.clear_json_cache()

    # Create test files
    scores_data = [{"person_id": "p1", "composite": 85.5}]
    (tmp_path / "scores.json").write_text(json.dumps(scores_data))

    anime_data = {"anime_1": {"credit_count": 50}}
    (tmp_path / "anime_stats.json").write_text(json.dumps(anime_data))

    summary_data = {"total_persons": 100}
    (tmp_path / "summary.json").write_text(json.dumps(summary_data))

    # Test loaders
    assert json_io.load_person_scores_from_json() == scores_data
    assert json_io.load_anime_statistics_from_json() == anime_data
    assert json_io.load_pipeline_summary_from_json() == summary_data


def test_cache_is_isolated_per_file(tmp_path: Path) -> None:
    """Test that cache correctly isolates different files."""
    file1 = tmp_path / "file1.json"
    file2 = tmp_path / "file2.json"

    file1.write_text(json.dumps({"file": 1}))
    file2.write_text(json.dumps({"file": 2}))

    result1 = json_io.load_json_file_with_caching(str(file1), "dict")
    result2 = json_io.load_json_file_with_caching(str(file2), "dict")

    assert result1["file"] == 1
    assert result2["file"] == 2
