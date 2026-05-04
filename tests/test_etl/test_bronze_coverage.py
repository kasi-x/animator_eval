"""Tests for src/etl/audit/bronze_to_conformed_coverage.py.

Uses synthetic BRONZE parquet (written via BronzeWriter) and in-memory
data structures.  No real result/bronze files are required.

Coverage:
- discover_bronze_tables: finds parquet tables, counts rows, skips bak dirs
- build_coverage_table: joins with INTEGRATION_MAP, handles UNKNOWN tables
- generate_report: markdown output shape, sections, priority list
- INTEGRATION_MAP: sanity checks (all values, no orphan keys)
"""
from __future__ import annotations

from pathlib import Path

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.audit.bronze_to_conformed_coverage import (
    INTEGRATION_MAP,
    BronzeTableInfo,
    CoverageEntry,
    build_coverage_table,
    discover_bronze_tables,
    generate_report,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(bronze_root: Path, source: str, table: str, rows: list[dict]) -> None:
    """Write rows to a synthetic BRONZE parquet."""
    with BronzeWriter(source, table=table, root=bronze_root, compact_on_exit=False) as bw:
        for row in rows:
            bw.append(row)


def _make_entries(specs: list[tuple[str, str, int, str]]) -> list[CoverageEntry]:
    """Build CoverageEntry list from (source, table, rows, status) tuples."""
    return [
        CoverageEntry(
            source=src,
            bronze_table=tbl,
            bronze_rows=rows,
            status=status,
            silver_target="some_table" if status == "INTEGRATED" else "",
            notes="",
            in_map=status != "UNKNOWN",
        )
        for src, tbl, rows, status in specs
    ]


# ---------------------------------------------------------------------------
# discover_bronze_tables tests
# ---------------------------------------------------------------------------


def test_discover_finds_written_table(tmp_path: Path) -> None:
    """Tables written via BronzeWriter must appear in discover output."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "anilist", "anime", [
        {"id": "anilist:1", "title": "A"},
        {"id": "anilist:2", "title": "B"},
    ])
    results = discover_bronze_tables(bronze_root)
    assert any(r.source == "anilist" and r.table == "anime" for r in results)


def test_discover_row_count_correct(tmp_path: Path) -> None:
    """Row count must match the number of rows written."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "mal", "anime", [
        {"id": f"mal:{i}"} for i in range(7)
    ])
    results = discover_bronze_tables(bronze_root)
    row = next(r for r in results if r.source == "mal" and r.table == "anime")
    assert row.row_count == 7
    assert row.error is None


def test_discover_multiple_sources(tmp_path: Path) -> None:
    """Multiple sources in the same bronze_root must all be discovered."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "anilist", "anime", [{"id": "a:1"}])
    _write_parquet(bronze_root, "mal", "anime", [{"id": "m:1"}])
    _write_parquet(bronze_root, "bangumi", "subjects", [{"id": "b:1"}])

    results = discover_bronze_tables(bronze_root)
    sources = {r.source for r in results}
    assert "anilist" in sources
    assert "mal" in sources
    assert "bangumi" in sources


def test_discover_skips_bak_dirs(tmp_path: Path) -> None:
    """Directories named *.bak-* must be excluded from discovery."""
    bronze_root = tmp_path / "bronze"
    # Create a bak directory manually (BronzeWriter would write source=... dirs)
    bak_dir = bronze_root / "source=ann.bak-20260425" / "table=anime" / "date=2026-04-25"
    bak_dir.mkdir(parents=True)
    # Write real data so there's something to find if bak is accidentally scanned
    (bak_dir / "part.parquet").touch()

    _write_parquet(bronze_root, "ann", "anime", [{"id": "ann:1"}])

    results = discover_bronze_tables(bronze_root)
    sources = {r.source for r in results}
    assert "ann.bak-20260425" not in sources
    assert "ann" in sources


def test_discover_empty_bronze_root(tmp_path: Path) -> None:
    """Empty bronze_root returns an empty list without raising."""
    bronze_root = tmp_path / "empty_bronze"
    bronze_root.mkdir()
    results = discover_bronze_tables(bronze_root)
    assert results == []


def test_discover_returns_bronze_table_info(tmp_path: Path) -> None:
    """Each result must be a BronzeTableInfo instance."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "anilist", "persons", [{"id": "p1"}])
    results = discover_bronze_tables(bronze_root)
    assert all(isinstance(r, BronzeTableInfo) for r in results)


# ---------------------------------------------------------------------------
# build_coverage_table tests
# ---------------------------------------------------------------------------


def test_build_coverage_maps_integrated_table() -> None:
    """Known INTEGRATED table must appear with correct status."""
    info = [BronzeTableInfo(source="anilist", table="anime", row_count=51174)]
    entries = build_coverage_table(info)
    assert len(entries) == 1
    e = entries[0]
    assert e.status == "INTEGRATED"
    assert e.bronze_rows == 51174
    assert e.in_map is True
    assert "anime" in e.silver_target.lower() or e.silver_target != ""


def test_build_coverage_maps_unused_table() -> None:
    """Known UNUSED table must appear with status UNUSED."""
    info = [BronzeTableInfo(source="mal", table="anime_pictures", row_count=46370)]
    entries = build_coverage_table(info)
    e = entries[0]
    assert e.status == "UNUSED"
    assert e.silver_target == ""
    assert e.in_map is True


def test_build_coverage_unknown_table_marked() -> None:
    """Table absent from INTEGRATION_MAP must be marked UNKNOWN."""
    info = [BronzeTableInfo(source="anilist", table="hypothetical_new_table", row_count=100)]
    entries = build_coverage_table(info)
    e = entries[0]
    assert e.status == "UNKNOWN"
    assert e.in_map is False


def test_build_coverage_all_statuses_present() -> None:
    """build_coverage_table must handle all declared status values."""
    infos = [
        BronzeTableInfo(source="anilist",      table="anime",            row_count=51174),
        BronzeTableInfo(source="anilist",      table="relations",         row_count=1248),
        BronzeTableInfo(source="mal",          table="anime_statistics",  row_count=19122),
        BronzeTableInfo(source="mal",          table="anime_external",    row_count=105508),
    ]
    entries = build_coverage_table(infos)
    statuses = {e.status for e in entries}
    # Must include INTEGRATED and FOLDED at minimum; DISPLAY_ONLY and UNUSED if in map
    assert "INTEGRATED" in statuses
    assert "FOLDED" in statuses


def test_build_coverage_returns_coverage_entry_instances() -> None:
    """Each result must be a CoverageEntry instance."""
    info = [BronzeTableInfo(source="bangumi", table="subjects", row_count=3715)]
    entries = build_coverage_table(info)
    assert all(isinstance(e, CoverageEntry) for e in entries)


def test_build_coverage_tmdb_unused() -> None:
    """TMDb tables must be UNUSED (no loader implemented yet)."""
    infos = [
        BronzeTableInfo(source="tmdb", table="anime",   row_count=79658),
        BronzeTableInfo(source="tmdb", table="credits", row_count=1174486),
        BronzeTableInfo(source="tmdb", table="persons", row_count=293115),
    ]
    entries = build_coverage_table(infos)
    assert all(e.status == "UNUSED" for e in entries)


# ---------------------------------------------------------------------------
# generate_report tests
# ---------------------------------------------------------------------------


def test_generate_report_creates_file(tmp_path: Path) -> None:
    """generate_report must create the output file."""
    entries = _make_entries([
        ("anilist", "anime",     51174, "INTEGRATED"),
        ("mal",     "anime_ext", 105508, "UNUSED"),
    ])
    out = tmp_path / "audit" / "bronze_conformed_coverage.md"
    generate_report(entries, out)
    assert out.exists()


def test_generate_report_has_required_sections(tmp_path: Path) -> None:
    """Report must contain all major section headings."""
    entries = _make_entries([
        ("anilist", "anime", 51174, "INTEGRATED"),
        ("mal", "anime_external", 105508, "UNUSED"),
        ("bangumi", "relations", 1248, "FOLDED"),
    ])
    out = tmp_path / "report.md"
    generate_report(entries, out)
    text = out.read_text(encoding="utf-8")

    assert "# BRONZE → Conformed Table-Level Coverage Audit" in text
    assert "## Summary" in text
    assert "## Full Coverage Table" in text
    assert "## Un-integrated Tables" in text
    assert "## Priority List" in text
    assert "## Sub-Card Recommendations" in text


def test_generate_report_summary_counts(tmp_path: Path) -> None:
    """Summary section must show correct table counts per status."""
    entries = _make_entries([
        ("anilist", "anime",    51174, "INTEGRATED"),
        ("anilist", "persons",   7528, "INTEGRATED"),
        ("mal",     "unused1",  10000, "UNUSED"),
    ])
    out = tmp_path / "report.md"
    generate_report(entries, out)
    text = out.read_text(encoding="utf-8")

    # Summary table row for INTEGRATED should show 2
    assert "| INTEGRATED | 2 |" in text
    assert "| UNUSED | 1 |" in text


def test_generate_report_disclaimers(tmp_path: Path) -> None:
    """Both JA and EN disclaimers must be present."""
    entries = _make_entries([("anilist", "anime", 51174, "INTEGRATED")])
    out = tmp_path / "report.md"
    generate_report(entries, out)
    text = out.read_text(encoding="utf-8")
    assert "Disclaimer (JA)" in text
    assert "Disclaimer (EN)" in text


def test_generate_report_creates_parent_dirs(tmp_path: Path) -> None:
    """generate_report must create missing parent directories."""
    entries = _make_entries([("anilist", "anime", 51174, "INTEGRATED")])
    out = tmp_path / "a" / "b" / "c" / "report.md"
    generate_report(entries, out)
    assert out.exists()


def test_generate_report_full_coverage_table_has_all_entries(tmp_path: Path) -> None:
    """Every entry must appear in the full coverage table."""
    entries = _make_entries([
        ("anilist",  "anime",   51174, "INTEGRATED"),
        ("mal",      "ext",    105508, "UNUSED"),
        ("bangumi",  "rel",      1248, "FOLDED"),
        ("seesaa",   "new",        99, "UNKNOWN"),
    ])
    out = tmp_path / "report.md"
    generate_report(entries, out)
    text = out.read_text(encoding="utf-8")

    for e in entries:
        assert e.bronze_table in text, f"Expected '{e.bronze_table}' in report"


def test_generate_report_no_unused_section_when_all_integrated(tmp_path: Path) -> None:
    """If all tables are INTEGRATED, no Un-integrated section should appear."""
    entries = _make_entries([
        ("anilist", "anime",   51174, "INTEGRATED"),
        ("anilist", "persons",  7528, "INTEGRATED"),
    ])
    out = tmp_path / "report.md"
    generate_report(entries, out)
    text = out.read_text(encoding="utf-8")
    assert "## Un-integrated Tables" not in text


# ---------------------------------------------------------------------------
# INTEGRATION_MAP sanity checks
# ---------------------------------------------------------------------------


def test_integration_map_keys_are_tuples() -> None:
    """All INTEGRATION_MAP keys must be (source, table) string tuples."""
    for key in INTEGRATION_MAP:
        assert isinstance(key, tuple), f"Expected tuple key, got {type(key)}: {key!r}"
        assert len(key) == 2
        source, table = key
        assert isinstance(source, str)
        assert isinstance(table, str)


def test_integration_map_values_are_triples() -> None:
    """All INTEGRATION_MAP values must be (status, silver_target, notes) triples."""
    valid_statuses = {"INTEGRATED", "FOLDED", "UNUSED", "DISPLAY_ONLY"}
    for key, value in INTEGRATION_MAP.items():
        assert isinstance(value, tuple), f"Expected tuple for {key}, got {type(value)}"
        assert len(value) == 3, f"Expected 3-tuple for {key}, got length {len(value)}"
        status, silver_target, notes = value
        assert status in valid_statuses, f"Unknown status '{status}' for {key}"
        assert isinstance(silver_target, str)
        assert isinstance(notes, str)


def test_integration_map_integrated_has_silver_target() -> None:
    """All INTEGRATED entries must have a non-empty silver_target."""
    for key, (status, silver_target, _) in INTEGRATION_MAP.items():
        if status == "INTEGRATED":
            assert silver_target, (
                f"INTEGRATED entry {key} must declare a silver_target"
            )


def test_integration_map_covers_expected_sources() -> None:
    """INTEGRATION_MAP must cover all expected data sources."""
    sources = {src for src, _ in INTEGRATION_MAP}
    expected = {"anilist", "ann", "bangumi", "keyframe", "mal", "mediaarts",
                "sakuga_atwiki", "seesaawiki", "tmdb"}
    for src in expected:
        assert src in sources, f"Expected source '{src}' missing from INTEGRATION_MAP"


def test_integration_map_anilist_all_tables_present() -> None:
    """All known AniList BRONZE tables must appear in INTEGRATION_MAP."""
    anilist_tables_in_map = {tbl for (src, tbl) in INTEGRATION_MAP if src == "anilist"}
    expected = {
        "anime", "persons", "credits", "characters",
        "character_voice_actors", "studios", "anime_studios", "relations",
    }
    for tbl in expected:
        assert tbl in anilist_tables_in_map, (
            f"AniList table '{tbl}' missing from INTEGRATION_MAP"
        )


def test_integration_map_mal_known_unused_tables() -> None:
    """Known UNUSED MAL tables must be present and correctly classified."""
    unused_mal = {
        "anime_external", "anime_moreinfo", "anime_pictures",
        "anime_videos_ep", "anime_videos_promo", "anime_streaming",
    }
    for tbl in unused_mal:
        key = ("mal", tbl)
        assert key in INTEGRATION_MAP, f"MAL table '{tbl}' missing from INTEGRATION_MAP"
        status, _, _ = INTEGRATION_MAP[key]
        assert status in ("UNUSED", "DISPLAY_ONLY"), (
            f"MAL table '{tbl}' expected UNUSED/DISPLAY_ONLY, got '{status}'"
        )


def test_integration_map_tmdb_tables_unused() -> None:
    """TMDb tables must be marked UNUSED (no conformed loader implemented)."""
    for tbl in ("anime", "credits", "persons"):
        key = ("tmdb", tbl)
        assert key in INTEGRATION_MAP, f"TMDb table '{tbl}' missing from INTEGRATION_MAP"
        status, _, _ = INTEGRATION_MAP[key]
        assert status == "UNUSED", f"TMDb table '{tbl}' expected UNUSED, got '{status}'"


def test_integration_map_no_empty_source_or_table() -> None:
    """No key must have an empty source or table string."""
    for source, table in INTEGRATION_MAP:
        assert source, "Empty source string in INTEGRATION_MAP key"
        assert table, "Empty table string in INTEGRATION_MAP key"
