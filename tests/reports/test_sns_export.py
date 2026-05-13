"""Tests for SNS export orchestrator + base SNS post models.

Coverage:
- SnsPost / NotePost Pydantic v2 validators (char limits)
- to_sns_post() / to_note_post() on implemented reports
  (o3_ip_dependency, o2_mid_management, studio_pipeline)
- NotImplementedError for unimplemented reports
- export_report_sns() orchestrator: output files created
- forbidden vocab: 0 violations in generated posts
- lint_text helper correctness
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts.report_generators.reports._base import NotePost, SnsPost
from scripts.report_generators.reports.o3_ip_dependency import O3IpDependencyReport
from scripts.report_generators.reports.o2_mid_management import O2MidManagementReport
from scripts.report_generators.reports.studio_pipeline import StudioPipelineReport
from scripts.report_generators.sns_export import (
    _SNS_CAPABLE_REPORTS,
    _lint_text,
    export_report_sns,
    export_all_sns,
)

_X_CHAR_LIMIT = 280
_NOTE_CHAR_MIN = 1500
_NOTE_CHAR_MAX = 3000


# ---------------------------------------------------------------------------
# Minimal in-memory DB shared across tests
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS anime (
    id TEXT PRIMARY KEY,
    title_romaji TEXT,
    episodes INTEGER,
    duration INTEGER,
    relations_json TEXT,
    series_cluster_id TEXT
);
CREATE TABLE IF NOT EXISTS persons (
    id TEXT PRIMARY KEY,
    name_romaji TEXT
);
CREATE TABLE IF NOT EXISTS credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS roles (
    name TEXT PRIMARY KEY,
    weight REAL
);
CREATE TABLE IF NOT EXISTS studios (
    id TEXT PRIMARY KEY,
    name TEXT
);
CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id TEXT,
    studio_id TEXT
);
"""


def _build_minimal_db() -> sqlite3.Connection:
    """Return a minimal in-memory SQLite with enough data for SNS tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)

    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("a0", "Alpha S1", 12, 24,
             '[{"relation_type":"SEQUEL","related_anime_id":"a1"}]', "c1"),
            ("a1", "Alpha S2", 12, 24,
             '[{"relation_type":"PREQUEL","related_anime_id":"a0"}]', "c1"),
            ("b0", "Standalone B", 6, 30, None, "c2"),
        ],
    )
    conn.executemany(
        "INSERT INTO persons VALUES (?, ?)",
        [("p1", "Dir One"), ("p2", "Anim Two"), ("p3", "Anim Three")],
    )
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role) VALUES (?, ?, ?)",
        [
            ("p1", "a0", "director"),
            ("p1", "a1", "director"),
            ("p2", "a0", "key_animator"),
            ("p2", "a1", "key_animator"),
            ("p3", "a0", "animation_director"),
            ("p1", "b0", "director"),
            ("p3", "b0", "animation_director"),
        ],
    )
    conn.executemany(
        "INSERT INTO roles VALUES (?, ?)",
        [
            ("director", 3.0),
            ("key_animator", 2.0),
            ("animation_director", 2.8),
        ],
    )
    conn.commit()
    return conn


@pytest.fixture(scope="module")
def db() -> sqlite3.Connection:
    return _build_minimal_db()


# ---------------------------------------------------------------------------
# SnsPost model validation
# ---------------------------------------------------------------------------


class TestSnsPostModel:
    def test_valid_post_accepted(self):
        post = SnsPost(text="A" * 280, url="https://example.com")
        assert len(post.text) == 280

    def test_text_within_limit(self):
        post = SnsPost(text="short text #アニメ制作")
        assert len(post.text) <= _X_CHAR_LIMIT

    def test_text_over_limit_raises(self):
        with pytest.raises(Exception):
            SnsPost(text="A" * 281)

    def test_default_platform_is_x(self):
        post = SnsPost(text="test")
        assert post.platform == "x"

    def test_model_dump(self):
        post = SnsPost(text="hello", url="http://x.com", figure_path="chart.png")
        d = post.model_dump()
        assert "text" in d
        assert "platform" in d
        assert d["url"] == "http://x.com"


# ---------------------------------------------------------------------------
# NotePost model validation
# ---------------------------------------------------------------------------


class TestNotePostModel:
    def _valid_body(self, n: int = 1500) -> str:
        # Build a body of exactly n chars with no forbidden vocab.
        # "構造的観察。" = 7 chars per repetition.
        unit = "構造的観察。"  # 7 chars
        reps = (n // len(unit)) + 1
        return (unit * reps)[:n]

    def test_valid_body_accepted(self):
        body = self._valid_body(1500)
        post = NotePost(title="Test", body=body)
        assert len(post.body) >= _NOTE_CHAR_MIN

    def test_body_too_short_raises(self):
        with pytest.raises(Exception):
            NotePost(title="T", body="A" * 1499)

    def test_body_too_long_raises(self):
        with pytest.raises(Exception):
            NotePost(title="T", body="A" * 3001)

    def test_default_platform_is_note(self):
        post = NotePost(title="T", body=self._valid_body())
        assert post.platform == "note"

    def test_model_dump_includes_title(self):
        post = NotePost(title="My Title", body=self._valid_body())
        d = post.model_dump()
        assert d["title"] == "My Title"


# ---------------------------------------------------------------------------
# o3_ip_dependency SNS methods
# ---------------------------------------------------------------------------


class TestO3SnsPost:
    def test_to_sns_post_returns_sns_post(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        assert isinstance(post, SnsPost)

    def test_x_post_within_char_limit(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        assert len(post.text) <= _X_CHAR_LIMIT, (
            f"X post too long: {len(post.text)} chars"
        )

    def test_x_post_has_text(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        assert post.text.strip()

    def test_to_note_post_returns_note_post(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        assert isinstance(post, NotePost)

    def test_note_post_within_char_range(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        assert _NOTE_CHAR_MIN <= len(post.body) <= _NOTE_CHAR_MAX, (
            f"note body out of range: {len(post.body)} chars"
        )

    def test_x_post_no_forbidden_vocab(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        violations = _lint_text(post.text, "o3/x")
        assert violations == [], f"Forbidden vocab violations: {violations}"

    def test_note_post_no_forbidden_vocab(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        violations = _lint_text(post.body, "o3/note")
        assert violations == [], f"Forbidden vocab violations: {violations}"

    def test_note_has_disclaimer(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        assert "免責" in post.body or "Disclaimer" in post.body

    def test_note_has_interpretation_label(self, db, tmp_path):
        report = O3IpDependencyReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        # Must have explicit interpretation labeling
        assert "解釈" in post.body or "interpret" in post.body.lower()


# ---------------------------------------------------------------------------
# o2_mid_management SNS methods
# ---------------------------------------------------------------------------


class TestO2SnsPost:
    def test_x_post_within_char_limit(self, db, tmp_path):
        report = O2MidManagementReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        assert isinstance(post, SnsPost)
        assert len(post.text) <= _X_CHAR_LIMIT

    def test_note_post_within_char_range(self, db, tmp_path):
        report = O2MidManagementReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        assert isinstance(post, NotePost)
        assert _NOTE_CHAR_MIN <= len(post.body) <= _NOTE_CHAR_MAX

    def test_x_post_no_forbidden_vocab(self, db, tmp_path):
        report = O2MidManagementReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        violations = _lint_text(post.text, "o2/x")
        assert violations == [], f"Violations: {violations}"

    def test_note_post_no_forbidden_vocab(self, db, tmp_path):
        report = O2MidManagementReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        violations = _lint_text(post.body, "o2/note")
        assert violations == [], f"Violations: {violations}"


# ---------------------------------------------------------------------------
# studio_pipeline SNS methods
# ---------------------------------------------------------------------------


class TestStudioPipelineSnsPost:
    def test_x_post_within_char_limit(self, db, tmp_path):
        report = StudioPipelineReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        assert isinstance(post, SnsPost)
        assert len(post.text) <= _X_CHAR_LIMIT

    def test_note_post_within_char_range(self, db, tmp_path):
        report = StudioPipelineReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        assert isinstance(post, NotePost)
        assert _NOTE_CHAR_MIN <= len(post.body) <= _NOTE_CHAR_MAX

    def test_x_post_no_forbidden_vocab(self, db, tmp_path):
        report = StudioPipelineReport(db, output_dir=tmp_path)
        post = report.to_sns_post()
        violations = _lint_text(post.text, "studio_pipeline/x")
        assert violations == [], f"Violations: {violations}"

    def test_note_post_no_forbidden_vocab(self, db, tmp_path):
        report = StudioPipelineReport(db, output_dir=tmp_path)
        post = report.to_note_post()
        violations = _lint_text(post.body, "studio_pipeline/note")
        assert violations == [], f"Violations: {violations}"


# ---------------------------------------------------------------------------
# Base class default behaviour
# ---------------------------------------------------------------------------


class TestBaseDefaultBehaviour:
    def test_to_sns_post_raises_not_implemented(self, db, tmp_path):
        """A report that does not override to_sns_post() raises NotImplementedError."""
        from scripts.report_generators.reports.growth_scores import GrowthScoresReport

        report = GrowthScoresReport(db, output_dir=tmp_path)
        with pytest.raises(NotImplementedError):
            report.to_sns_post()

    def test_to_note_post_raises_not_implemented(self, db, tmp_path):
        """A report that does not override to_note_post() raises NotImplementedError."""
        from scripts.report_generators.reports.growth_scores import GrowthScoresReport

        report = GrowthScoresReport(db, output_dir=tmp_path)
        with pytest.raises(NotImplementedError):
            report.to_note_post()


# ---------------------------------------------------------------------------
# sns_export orchestrator
# ---------------------------------------------------------------------------


class TestExportReportSns:
    def test_export_creates_md_files(self, db, tmp_path):
        result = export_report_sns("o3_ip_dependency", conn=db, output_dir=tmp_path)
        assert Path(result["x_path"]).exists()
        assert Path(result["note_path"]).exists()

    def test_export_creates_json_files(self, db, tmp_path):
        export_report_sns("o3_ip_dependency", conn=db, output_dir=tmp_path)
        assert (tmp_path / "x" / "o3_ip_dependency.json").exists()
        assert (tmp_path / "note" / "o3_ip_dependency.json").exists()

    def test_json_is_valid(self, db, tmp_path):
        export_report_sns("o3_ip_dependency", conn=db, output_dir=tmp_path)
        x_data = json.loads((tmp_path / "x" / "o3_ip_dependency.json").read_text())
        note_data = json.loads((tmp_path / "note" / "o3_ip_dependency.json").read_text())
        assert "text" in x_data
        assert "body" in note_data
        assert "title" in note_data

    def test_export_returns_char_counts(self, db, tmp_path):
        result = export_report_sns("o3_ip_dependency", conn=db, output_dir=tmp_path)
        assert 0 < result["x_chars"] <= _X_CHAR_LIMIT
        assert _NOTE_CHAR_MIN <= result["note_chars"] <= _NOTE_CHAR_MAX

    def test_export_zero_lint_violations(self, db, tmp_path):
        result = export_report_sns("o3_ip_dependency", conn=db, output_dir=tmp_path)
        assert result["lint_violations"] == [], (
            f"Lint violations in exported post: {result['lint_violations']}"
        )

    def test_export_unknown_report_raises(self, db, tmp_path):
        with pytest.raises(KeyError):
            export_report_sns("nonexistent_report", conn=db, output_dir=tmp_path)

    def test_export_all_creates_index(self, db, tmp_path):
        export_all_sns(conn=db, output_dir=tmp_path)
        index_path = tmp_path / "sns_export_index.json"
        assert index_path.exists()
        index = json.loads(index_path.read_text())
        assert isinstance(index, list)
        assert len(index) == len(_SNS_CAPABLE_REPORTS)

    def test_export_all_zero_total_violations(self, db, tmp_path):
        results = export_all_sns(conn=db, output_dir=tmp_path)
        for result in results:
            assert result.get("lint_violations", []) == [], (
                f"Violations in {result.get('report')}: {result.get('lint_violations')}"
            )


# ---------------------------------------------------------------------------
# lint_text helper
# ---------------------------------------------------------------------------


class TestLintText:
    def test_clean_text_returns_empty(self):
        clean = "アニメ制作の構造的観察。ネットワーク密度指標。"
        assert _lint_text(clean, "test") == []

    def test_ability_framing_detected(self):
        bad = "This animator has exceptional ability in key frame work."
        violations = _lint_text(bad, "test")
        assert violations, "Expected ability violation not detected"

    def test_evaluative_adjective_detected(self):
        bad = "The studio is excellent at developing its pipeline."
        violations = _lint_text(bad, "test")
        assert violations, "Expected evaluative adjective violation not detected"
