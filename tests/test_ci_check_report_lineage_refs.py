"""Unit tests for scripts/report_generators/ci_check_report_lineage_refs.py.

Guards against regressions in the mandatory-report lineage gate.
"""
from __future__ import annotations

from pathlib import Path

from scripts.report_generators.ci_check_report_lineage_refs import (
    IGNORED_STEMS,
    LINEAGE_TOKENS,
    MANDATORY_REPORT_STEMS,
    REPORTS_DIR,
    _defines_report_class,
    _file_has_lineage_reference,
    scan_reports,
)


def test_mandatory_reports_present_on_disk() -> None:
    """Every stem in MANDATORY_REPORT_STEMS must map to an existing file."""
    for stem in MANDATORY_REPORT_STEMS:
        path = REPORTS_DIR / f"{stem}.py"
        assert path.exists(), f"mandatory report missing on disk: {path}"


def test_mandatory_reports_reference_lineage() -> None:
    """All mandatory reports must contain a lineage token.

    This is the load-bearing regression gate — if someone removes
    insert_lineage() from a mandatory report, CI must fail.
    """
    missing_mandatory, _ = scan_reports()
    assert missing_mandatory == [], (
        "mandatory report(s) regressed — no lineage reference: "
        f"{[p.name for p in missing_mandatory]}"
    )


def test_base_class_is_ignored() -> None:
    """_base.py must be in IGNORED_STEMS so the scanner doesn't flag it."""
    assert "_base" in IGNORED_STEMS
    assert "__init__" in IGNORED_STEMS


def test_lineage_tokens_non_empty() -> None:
    """LINEAGE_TOKENS must not be empty or the gate would pass trivially."""
    assert LINEAGE_TOKENS
    assert all(isinstance(t, str) and t for t in LINEAGE_TOKENS)


def test_defines_report_class_detects_subclass(tmp_path: Path) -> None:
    """_defines_report_class should return True only for BaseReportGenerator
    subclasses with a generate() method."""
    ok = tmp_path / "ok.py"
    ok.write_text(
        "class Foo(BaseReportGenerator):\n"
        "    def generate(self):\n"
        "        return None\n"
    )
    not_subclass = tmp_path / "not_subclass.py"
    not_subclass.write_text(
        "class Foo:\n"
        "    def generate(self):\n"
        "        return None\n"
    )
    no_generate = tmp_path / "no_generate.py"
    no_generate.write_text(
        "class Foo(BaseReportGenerator):\n"
        "    pass\n"
    )

    assert _defines_report_class(ok) is True
    assert _defines_report_class(not_subclass) is False
    assert _defines_report_class(no_generate) is False


def test_file_has_lineage_reference(tmp_path: Path) -> None:
    hit = tmp_path / "hit.py"
    hit.write_text("insert_lineage(conn, table_name='x')\n")
    miss = tmp_path / "miss.py"
    miss.write_text("# nothing to see here\n")

    assert _file_has_lineage_reference(hit) is True
    assert _file_has_lineage_reference(miss) is False
