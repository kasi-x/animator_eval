"""Tests for scripts/report_generators/reports/studio_pipeline.py.

Coverage:
- smoke: StudioPipelineReport.generate() with in-memory SQLite + empty data
- smoke: StudioPipelineReport.generate() with synthetic JSON data via tmp_path
- lint_vocab: forbidden vocabulary absent from report source
- No anime.score reference
- method gate: insert_lineage called with CI + null_model
- V2_REPORT_CLASSES contains StudioPipelineReport
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def minimal_conn() -> sqlite3.Connection:
    """In-memory SQLite with meta_lineage table only."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ci_method TEXT,
            null_model TEXT,
            holdout_method TEXT,
            description TEXT,
            inputs_hash TEXT,
            notes TEXT,
            rng_seed INTEGER,
            row_count INTEGER
        );
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _write_pipeline_json(tmp_path: Path) -> Path:
    """Write a minimal studio_pipeline_strength.json in tmp_path/result/json/."""
    json_dir = tmp_path / "result" / "json"
    json_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "studioA": {
            "name": "Studio A",
            "young_theta_growth_mean": 0.05,
            "young_theta_growth_ci": [-0.01, 0.11],
            "mid_career_retention_mean": 0.72,
            "mid_career_retention_ci": [0.65, 0.79],
            "key_person_concentration_mean": 0.45,
            "key_person_concentration_ci": [0.38, 0.52],
            "bus_factor_mean": 3.2,
            "bus_factor_ci": [2.8, 3.6],
            "n_cells": 8,
            "latest_year": 2022,
        },
        "studioB": {
            "name": "Studio B",
            "young_theta_growth_mean": -0.02,
            "young_theta_growth_ci": [-0.08, 0.04],
            "mid_career_retention_mean": 0.55,
            "mid_career_retention_ci": [0.46, 0.64],
            "key_person_concentration_mean": 0.68,
            "key_person_concentration_ci": [0.60, 0.76],
            "bus_factor_mean": 1.8,
            "bus_factor_ci": [1.5, 2.1],
            "n_cells": 6,
            "latest_year": 2022,
        },
    }
    p = json_dir / "studio_pipeline_strength.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return json_dir


# ─────────────────────────────────────────────────────────────────────────────
# Smoke tests
# ─────────────────────────────────────────────────────────────────────────────


def test_generate_empty_data_returns_path(
    minimal_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """generate() returns a valid HTML path even when JSON data is absent."""
    from scripts.report_generators.reports.studio_pipeline import StudioPipelineReport

    report = StudioPipelineReport(minimal_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path, not None"
    assert result.exists(), f"Output file must exist: {result}"
    html = result.read_text(encoding="utf-8")
    assert len(html) > 200, "HTML must not be trivially empty"


def test_generate_with_data_returns_path(
    minimal_conn: sqlite3.Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """generate() returns HTML containing pipeline content when JSON data is present."""
    import scripts.report_generators.reports.studio_pipeline as mod

    _write_pipeline_json(tmp_path)
    monkeypatch.setattr(mod, "_JSON_DIR", tmp_path / "result" / "json")

    from scripts.report_generators.reports.studio_pipeline import StudioPipelineReport

    report = StudioPipelineReport(minimal_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None
    assert result.exists()
    html = result.read_text(encoding="utf-8")
    # Should contain section headings and chart content
    assert "pipeline" in html.lower() or "パイプライン" in html, (
        "HTML must reference pipeline content"
    )


def test_generate_creates_lineage(
    minimal_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """insert_lineage must populate meta_lineage with CI and null_model."""
    from scripts.report_generators.reports.studio_pipeline import StudioPipelineReport

    report = StudioPipelineReport(minimal_conn, output_dir=tmp_path)
    report.generate()

    row = minimal_conn.execute(
        "SELECT ci_method, null_model FROM meta_lineage "
        "WHERE table_name = 'meta_studio_pipeline'"
    ).fetchone()

    assert row is not None, "meta_lineage row must be inserted"
    ci_method, null_model = row
    assert ci_method, "ci_method must be non-empty"
    assert null_model, "null_model must be non-empty"
    assert "bootstrap" in ci_method.lower(), "CI method must mention bootstrap"


# ─────────────────────────────────────────────────────────────────────────────
# lint_vocab: forbidden vocabulary absent from report source
# ─────────────────────────────────────────────────────────────────────────────

_FORBIDDEN_PATTERN = re.compile(
    r"\b(ability|talent|talented|competent|incompetent|capable|incapable|aptitude)\b"
    r"|能力|実力|才能|優秀|優れた|劣る|劣った|有能|無能",
    re.IGNORECASE,
)

_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "studio_pipeline.py"
)


def test_lint_vocab_report_source() -> None:
    """studio_pipeline.py must not contain forbidden vocabulary."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, (
        f"Forbidden vocabulary found in studio_pipeline.py: {matches}"
    )


def test_no_anime_score_in_report_usage() -> None:
    """studio_pipeline.py must not use anime.score as a computation input.

    Negation disclosures ("no anime.score", "anime.score is not used") are
    allowed; any line containing "anime.score" must also contain a negation.
    """
    text = _REPORT_SRC.read_text(encoding="utf-8")
    for line in text.split("\n"):
        if "anime.score" in line:
            lowered = line.lower()
            assert "not" in lowered or "never" in lowered or "no " in lowered, (
                f"Unexpected non-negation use of anime.score: {line.strip()}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Method gate: CI + null_model present in report source
# ─────────────────────────────────────────────────────────────────────────────


def test_method_gate_ci_present() -> None:
    """Report source must declare a CI method."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "ci_method" in text, "ci_method must be declared in insert_lineage call"
    assert "bootstrap" in text.lower(), (
        "CI method must mention bootstrap"
    )


def test_method_gate_null_model_present() -> None:
    """Report source must declare a null model."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "null_model" in text, "null_model must be declared in insert_lineage call"


# ─────────────────────────────────────────────────────────────────────────────
# V2_REPORT_CLASSES registration
# ─────────────────────────────────────────────────────────────────────────────


def test_studio_pipeline_in_v2_report_classes() -> None:
    """StudioPipelineReport must appear in V2_REPORT_CLASSES."""
    from scripts.report_generators.reports import V2_REPORT_CLASSES

    class_names = [cls.__name__ for cls in V2_REPORT_CLASSES]
    assert "StudioPipelineReport" in class_names, (
        "StudioPipelineReport must be registered in V2_REPORT_CLASSES"
    )
