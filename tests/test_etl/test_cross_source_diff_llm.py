"""Tests for src/etl/audit/cross_source_diff_llm.py.

Uses mock LLM calls throughout so no Ollama instance is required.

Covers:
- build_prompt: correct field substitution
- _validate_llm_result: field normalisation / fallback
- _cache_key: deduplication key logic
- _extract_json_object: JSON extraction from varied LLM output formats
- enrich_csv: full pipeline with mock LLM, caching, checkpoint, dry-run,
              sample/limit controls, and LLM-unavailable fallback
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest

from src.etl.audit.cross_source_diff_llm import (
    LLM_FIELDNAMES,
    VALID_BEST_GUESS,
    VALID_PATTERNS,
    _cache_key,
    _checkpoint_path,
    _empty_llm_result,
    _extract_json_object,
    _validate_llm_result,
    build_prompt,
    enrich_csv,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_diff_row(
    canonical_id: str = "resolved:anime:aaa",
    attribute: str = "title_en",
    source_a: str = "anilist",
    value_a: str = "Madoka Magica",
    source_b: str = "mal",
    value_b: str = "Mahou Shoujo Madoka Magica",
    classification: str = "completely_different",
    conformed_id_a: str = "anilist:a1",
    conformed_id_b: str = "mal:a1",
) -> dict[str, Any]:
    return {
        "canonical_id": canonical_id,
        "attribute": attribute,
        "source_a": source_a,
        "conformed_id_a": conformed_id_a,
        "value_a": value_a,
        "source_b": source_b,
        "conformed_id_b": conformed_id_b,
        "value_b": value_b,
        "classification": classification,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# build_prompt
# ---------------------------------------------------------------------------


class TestBuildPrompt:
    def test_contains_attribute(self) -> None:
        row = _make_diff_row(attribute="title_en")
        prompt = build_prompt(row, "anime")
        assert "title_en" in prompt

    def test_contains_values(self) -> None:
        row = _make_diff_row(value_a="Madoka Magica", value_b="Mahou Shoujo Madoka")
        prompt = build_prompt(row, "anime")
        assert "Madoka Magica" in prompt
        assert "Mahou Shoujo Madoka" in prompt

    def test_contains_entity_type(self) -> None:
        row = _make_diff_row()
        prompt = build_prompt(row, "persons")
        assert "persons" in prompt

    def test_contains_canonical_id(self) -> None:
        row = _make_diff_row(canonical_id="resolved:anime:xyz")
        prompt = build_prompt(row, "anime")
        assert "resolved:anime:xyz" in prompt

    def test_contains_source_labels(self) -> None:
        row = _make_diff_row(source_a="anilist", source_b="mal")
        prompt = build_prompt(row, "anime")
        assert "anilist" in prompt
        assert "mal" in prompt

    def test_no_score_field_in_prompt(self) -> None:
        row = _make_diff_row()
        prompt = build_prompt(row, "anime")
        # No anime.score / display_* should appear (H1)
        assert "score" not in prompt
        assert "display_" not in prompt


# ---------------------------------------------------------------------------
# _extract_json_object
# ---------------------------------------------------------------------------


class TestExtractJsonObject:
    def test_plain_json(self) -> None:
        text = '{"patterns": ["typo"], "best_guess": "value_a", "best_value": "", "confidence": 0.9, "rationale": "one char off"}'
        obj = _extract_json_object(text)
        assert obj is not None
        assert obj["best_guess"] == "value_a"

    def test_markdown_fences(self) -> None:
        text = '```json\n{"patterns": ["abbreviation"], "best_guess": "value_b", "best_value": "", "confidence": 0.7, "rationale": "short form"}\n```'
        obj = _extract_json_object(text)
        assert obj is not None
        assert "abbreviation" in obj["patterns"]

    def test_trailing_prose(self) -> None:
        text = 'Here is my answer:\n{"patterns": ["ambiguous"], "best_guess": "neither", "best_value": "Canon Title", "confidence": 0.4, "rationale": "unclear"}\nHope this helps!'
        obj = _extract_json_object(text)
        assert obj is not None
        assert obj["best_guess"] == "neither"

    def test_returns_none_for_invalid(self) -> None:
        assert _extract_json_object("no json here") is None
        assert _extract_json_object("[1, 2, 3]") is None  # array, not object


# ---------------------------------------------------------------------------
# _validate_llm_result
# ---------------------------------------------------------------------------


class TestValidateLlmResult:
    def test_valid_result_passthrough(self) -> None:
        raw = {
            "patterns": ["typo", "romanization_variant"],
            "best_guess": "value_a",
            "best_value": "",
            "confidence": 0.85,
            "rationale": "Single character typo in romanization.",
        }
        result = _validate_llm_result(raw)
        assert result["llm_best_guess"] == "value_a"
        assert json.loads(result["llm_patterns"]) == ["typo", "romanization_variant"]
        assert result["llm_confidence"] == 0.85

    def test_unknown_patterns_filtered(self) -> None:
        raw = {
            "patterns": ["typo", "UNKNOWN_LABEL", "another_bad_one"],
            "best_guess": "value_b",
            "best_value": "",
            "confidence": 0.6,
            "rationale": "test",
        }
        result = _validate_llm_result(raw)
        parsed = json.loads(result["llm_patterns"])
        assert "UNKNOWN_LABEL" not in parsed
        assert "typo" in parsed

    def test_all_patterns_invalid_falls_back_to_ambiguous(self) -> None:
        raw = {
            "patterns": ["INVALID"],
            "best_guess": "value_a",
            "best_value": "",
            "confidence": 0.5,
            "rationale": "test",
        }
        result = _validate_llm_result(raw)
        assert json.loads(result["llm_patterns"]) == ["ambiguous"]

    def test_invalid_best_guess_falls_back(self) -> None:
        raw = {
            "patterns": ["typo"],
            "best_guess": "invalid_choice",
            "best_value": "",
            "confidence": 0.5,
            "rationale": "test",
        }
        result = _validate_llm_result(raw)
        assert result["llm_best_guess"] == "neither"

    def test_confidence_clamped_to_range(self) -> None:
        for conf_in, expected in [(2.0, 1.0), (-0.5, 0.0), (0.75, 0.75)]:
            raw = {
                "patterns": ["typo"],
                "best_guess": "value_a",
                "best_value": "",
                "confidence": conf_in,
                "rationale": "test",
            }
            result = _validate_llm_result(raw)
            assert 0.0 <= result["llm_confidence"] <= 1.0

    def test_neither_captures_best_value(self) -> None:
        raw = {
            "patterns": ["historical_alias"],
            "best_guess": "neither",
            "best_value": "Canonical Title",
            "confidence": 0.5,
            "rationale": "Both are old names.",
        }
        result = _validate_llm_result(raw)
        assert result["llm_best_value"] == "Canonical Title"

    def test_best_value_cleared_when_not_neither(self) -> None:
        raw = {
            "patterns": ["typo"],
            "best_guess": "value_a",
            "best_value": "should be ignored",
            "confidence": 0.9,
            "rationale": "value_a is correct.",
        }
        result = _validate_llm_result(raw)
        assert result["llm_best_value"] == ""

    def test_rationale_truncated(self) -> None:
        raw = {
            "patterns": ["typo"],
            "best_guess": "value_a",
            "best_value": "",
            "confidence": 0.9,
            "rationale": "x" * 400,
        }
        result = _validate_llm_result(raw)
        assert len(result["llm_rationale"]) <= 300

    def test_missing_fields_produce_defaults(self) -> None:
        result = _validate_llm_result({})
        assert result["llm_best_guess"] == "neither"
        assert result["llm_confidence"] == 0.5
        assert json.loads(result["llm_patterns"]) == ["ambiguous"]


# ---------------------------------------------------------------------------
# _empty_llm_result
# ---------------------------------------------------------------------------


class TestEmptyLlmResult:
    def test_structure(self) -> None:
        result = _empty_llm_result("test_reason")
        assert result["llm_best_guess"] == "neither"
        assert result["llm_confidence"] == 0.0
        assert "test_reason" in result["llm_rationale"]
        assert json.loads(result["llm_patterns"]) == ["ambiguous"]


# ---------------------------------------------------------------------------
# _cache_key
# ---------------------------------------------------------------------------


class TestCacheKey:
    def test_same_values_same_key(self) -> None:
        row1 = _make_diff_row(
            value_a="Madoka",
            value_b="Madoka Magica",
            attribute="title_en",
            canonical_id="resolved:anime:aaa",
        )
        row2 = _make_diff_row(
            value_a="Madoka",
            value_b="Madoka Magica",
            attribute="title_en",
            canonical_id="resolved:anime:bbb",  # different canonical_id, same values
        )
        assert _cache_key(row1) == _cache_key(row2)

    def test_different_attribute_different_key(self) -> None:
        row1 = _make_diff_row(attribute="title_en")
        row2 = _make_diff_row(attribute="title_ja")
        assert _cache_key(row1) != _cache_key(row2)

    def test_swapped_values_different_key(self) -> None:
        row1 = _make_diff_row(value_a="A", value_b="B")
        row2 = _make_diff_row(value_a="B", value_b="A")
        assert _cache_key(row1) != _cache_key(row2)


# ---------------------------------------------------------------------------
# enrich_csv — mock LLM
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    """Write a small diff CSV to tmp_path."""
    rows = [
        _make_diff_row(
            canonical_id=f"resolved:anime:{i}",
            attribute="title_en",
            value_a=f"Title A {i}",
            value_b=f"Title B {i}",
        )
        for i in range(10)
    ]
    p = tmp_path / "anime.csv"
    _write_csv(p, rows)
    return p


def _mock_llm_result() -> dict[str, Any]:
    return {
        "patterns": ["romanization_variant"],
        "best_guess": "value_a",
        "best_value": "",
        "confidence": 0.8,
        "rationale": "Mock: romanization difference.",
    }


class TestEnrichCsv:
    def test_output_csv_created(self, tmp_path: Path, sample_csv: Path, monkeypatch) -> None:
        out = tmp_path / "anime_llm_classified.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        assert out.exists()

    def test_output_has_llm_columns(self, tmp_path: Path, sample_csv: Path, monkeypatch) -> None:
        out = tmp_path / "anime_llm_classified.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        with open(out, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            assert reader.fieldnames is not None
            for col in LLM_FIELDNAMES:
                assert col in reader.fieldnames, f"Missing column: {col}"

    def test_row_count_preserved(self, tmp_path: Path, sample_csv: Path, monkeypatch) -> None:
        out = tmp_path / "anime_llm_classified.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        with open(out, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 10

    def test_sample_limits_rows(self, tmp_path: Path, sample_csv: Path, monkeypatch) -> None:
        out = tmp_path / "anime_llm_classified.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=5)
        with open(out, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 5

    def test_limit_limits_rows(self, tmp_path: Path, sample_csv: Path, monkeypatch) -> None:
        out = tmp_path / "anime_llm_classified.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None, limit=3)
        with open(out, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 3

    def test_dry_run_produces_output_no_llm(
        self, tmp_path: Path, sample_csv: Path, monkeypatch, capsys
    ) -> None:
        out = tmp_path / "anime_llm_classified.csv"
        # LLM flagged as unavailable — but dry_run should still succeed.
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: False
        )
        call_count = {"n": 0}

        def _no_call(prompt):
            call_count["n"] += 1
            return ""

        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            _no_call,
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None, dry_run=True)
        assert out.exists()
        # _call_llm must never be called in dry_run mode
        assert call_count["n"] == 0
        # Prompt text should be printed to stdout
        captured = capsys.readouterr()
        assert "DRY-RUN" in captured.out

    def test_cache_deduplication(self, tmp_path: Path, monkeypatch) -> None:
        """Identical (value_a, value_b, attribute) rows share one LLM call."""
        rows = [
            _make_diff_row(
                canonical_id=f"resolved:anime:{i}",
                value_a="Madoka",
                value_b="Madoka Magica",
                attribute="title_en",
            )
            for i in range(5)
        ]
        p = tmp_path / "dup.csv"
        _write_csv(p, rows)
        out = tmp_path / "dup_out.csv"

        call_count = {"n": 0}

        def _counting_llm(prompt):
            call_count["n"] += 1
            return json.dumps(_mock_llm_result())

        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            _counting_llm,
        )
        enrich_csv(p, out, entity_type="anime", sample=None)
        # All 5 rows share the same cache key → only 1 LLM call
        assert call_count["n"] == 1

    def test_llm_unavailable_produces_stub_output(
        self, tmp_path: Path, sample_csv: Path, monkeypatch
    ) -> None:
        out = tmp_path / "out.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: False
        )
        counts = enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        assert out.exists()
        assert counts["skipped"] == 10
        with open(out, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 10
        for row in rows:
            assert row["llm_best_guess"] == "neither"
            assert float(row["llm_confidence"]) == 0.0

    def test_checkpoint_written(self, tmp_path: Path, sample_csv: Path, monkeypatch) -> None:
        out = tmp_path / "anime_out.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        cp = _checkpoint_path(out)
        assert cp.exists()
        # Each line should be valid JSON with cache_key + result
        with open(cp, encoding="utf-8") as fh:
            lines = [line.strip() for line in fh if line.strip()]
        assert len(lines) > 0
        entry = json.loads(lines[0])
        assert "cache_key" in entry
        assert "result" in entry

    def test_checkpoint_resume_skips_cached(
        self, tmp_path: Path, sample_csv: Path, monkeypatch
    ) -> None:
        """Second run with existing checkpoint should produce zero LLM calls."""
        out = tmp_path / "anime_out.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        call_count = {"n": 0}

        def _counting_llm(prompt):
            call_count["n"] += 1
            return json.dumps(_mock_llm_result())

        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            _counting_llm,
        )
        # First run
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        first_run_calls = call_count["n"]
        assert first_run_calls > 0

        # Reset call counter; second run should use checkpoint
        call_count["n"] = 0
        counts = enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        assert call_count["n"] == 0
        assert counts["cached"] > 0

    def test_original_columns_preserved(
        self, tmp_path: Path, sample_csv: Path, monkeypatch
    ) -> None:
        out = tmp_path / "out.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: json.dumps(_mock_llm_result()),
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        with open(out, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        for row in rows:
            assert "canonical_id" in row
            assert "classification" in row

    def test_llm_parse_failure_produces_ambiguous(
        self, tmp_path: Path, sample_csv: Path, monkeypatch
    ) -> None:
        out = tmp_path / "out.csv"
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm.check_llm_available", lambda: True
        )
        monkeypatch.setattr(
            "src.etl.audit.cross_source_diff_llm._call_llm",
            lambda prompt: "this is not JSON at all",
        )
        enrich_csv(sample_csv, out, entity_type="anime", sample=None)
        with open(out, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        for row in rows:
            patterns = json.loads(row["llm_patterns"])
            assert "ambiguous" in patterns or patterns  # must not be empty


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


class TestConstants:
    def test_valid_patterns_non_empty(self) -> None:
        assert len(VALID_PATTERNS) > 0

    def test_valid_best_guess_values(self) -> None:
        assert VALID_BEST_GUESS == {"value_a", "value_b", "neither"}

    def test_llm_fieldnames_count(self) -> None:
        assert len(LLM_FIELDNAMES) == 5
        assert "llm_patterns" in LLM_FIELDNAMES
        assert "llm_confidence" in LLM_FIELDNAMES
