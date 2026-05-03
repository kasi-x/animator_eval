"""Tests for src/etl/audit/silver_column_coverage.py.

Uses synthetic BRONZE parquet (written via BronzeWriter) and an in-memory
DuckDB as the SILVER database.  No real silver.duckdb or result/bronze files
are required.

Coverage:
- measure_column_coverage: NULL/empty rate measurement
- find_bronze_source_with_value: BRONZE value count
- gap_analysis: end-to-end gap detection
- _classify_severity: threshold logic
- generate_report: markdown output shape
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.audit.silver_column_coverage import (
    COLUMN_BRONZE_MAP,
    SILVER_AUDIT_TARGETS,
    BronzeSourceStat,
    GapRow,
    _classify_severity,
    find_bronze_source_with_value,
    gap_analysis,
    generate_report,
    measure_column_coverage,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(bronze_root: Path, source: str, table: str, rows: list[dict]) -> None:
    """Write rows to a BRONZE parquet under bronze_root."""
    with BronzeWriter(source, table=table, root=bronze_root, compact_on_exit=False) as bw:
        for row in rows:
            bw.append(row)


def _make_silver_conn(
    persons: list[dict] | None = None,
    anime: list[dict] | None = None,
    characters: list[dict] | None = None,
    studios: list[dict] | None = None,
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory SILVER DuckDB with minimal schema and optional rows.

    Returns an **open** connection — caller must close it.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE persons (
            id          VARCHAR PRIMARY KEY,
            name_ja     VARCHAR NOT NULL DEFAULT '',
            name_en     VARCHAR NOT NULL DEFAULT '',
            name_ko     VARCHAR NOT NULL DEFAULT '',
            name_zh     VARCHAR NOT NULL DEFAULT '',
            names_alt   VARCHAR NOT NULL DEFAULT '{}',
            birth_date  VARCHAR,
            death_date  VARCHAR,
            website_url VARCHAR,
            image_large VARCHAR,
            image_medium VARCHAR,
            hometown    VARCHAR,
            nationality VARCHAR,
            description VARCHAR,
            blood_type  VARCHAR,
            gender      VARCHAR,
            aliases     VARCHAR,
            primary_occupations VARCHAR,
            years_active VARCHAR,
            updated_at  TIMESTAMP DEFAULT now()
        );
        CREATE TABLE anime (
            id               VARCHAR PRIMARY KEY,
            title_ja         VARCHAR NOT NULL DEFAULT '',
            title_en         VARCHAR NOT NULL DEFAULT '',
            year             INTEGER,
            season           VARCHAR,
            quarter          INTEGER,
            episodes         INTEGER,
            format           VARCHAR,
            duration         INTEGER,
            start_date       VARCHAR,
            end_date         VARCHAR,
            status           VARCHAR,
            source_mat       VARCHAR,
            work_type        VARCHAR,
            scale_class      VARCHAR,
            synonyms         VARCHAR,
            country_of_origin VARCHAR,
            is_adult         INTEGER,
            trailer_url      VARCHAR,
            external_links_json VARCHAR,
            airing_schedule_json VARCHAR,
            description      VARCHAR,
            updated_at       TIMESTAMP DEFAULT now()
        );
        CREATE TABLE characters (
            id            VARCHAR PRIMARY KEY,
            name_ja       VARCHAR NOT NULL DEFAULT '',
            name_en       VARCHAR NOT NULL DEFAULT '',
            aliases       VARCHAR NOT NULL DEFAULT '[]',
            anilist_id    INTEGER,
            image_large   VARCHAR,
            image_medium  VARCHAR,
            description   VARCHAR,
            gender        VARCHAR,
            date_of_birth VARCHAR,
            age           VARCHAR,
            blood_type    VARCHAR,
            favourites    INTEGER,
            site_url      VARCHAR,
            updated_at    TIMESTAMP DEFAULT now()
        );
        CREATE TABLE studios (
            id                  VARCHAR PRIMARY KEY,
            name                VARCHAR NOT NULL DEFAULT '',
            anilist_id          INTEGER,
            is_animation_studio BOOLEAN,
            country_of_origin   VARCHAR,
            favourites          INTEGER,
            site_url            VARCHAR,
            updated_at          TIMESTAMP DEFAULT now()
        );
    """)

    if persons:
        for p in persons:
            placeholders = ", ".join(["?"] * len(p))
            cols = ", ".join(p.keys())
            conn.execute(f"INSERT INTO persons ({cols}) VALUES ({placeholders})", list(p.values()))
    if anime:
        for a in anime:
            placeholders = ", ".join(["?"] * len(a))
            cols = ", ".join(a.keys())
            conn.execute(f"INSERT INTO anime ({cols}) VALUES ({placeholders})", list(a.values()))
    if characters:
        for c in characters:
            placeholders = ", ".join(["?"] * len(c))
            cols = ", ".join(c.keys())
            conn.execute(f"INSERT INTO characters ({cols}) VALUES ({placeholders})", list(c.values()))
    if studios:
        for s in studios:
            placeholders = ", ".join(["?"] * len(s))
            cols = ", ".join(s.keys())
            conn.execute(f"INSERT INTO studios ({cols}) VALUES ({placeholders})", list(s.values()))

    return conn


# ---------------------------------------------------------------------------
# measure_column_coverage tests
# ---------------------------------------------------------------------------


def test_measure_column_coverage_all_null() -> None:
    """Column entirely NULL → null_rate = 1.0."""
    conn = _make_silver_conn(
        persons=[
            {"id": "p1", "name_ja": "A"},
            {"id": "p2", "name_ja": "B"},
        ]
    )
    try:
        row = measure_column_coverage(conn, "persons", "gender")
        assert row.silver_table == "persons"
        assert row.silver_col == "gender"
        assert row.total == 2
        assert row.non_null == 0
        assert row.null_rate == pytest.approx(1.0)
        assert row.error is None
    finally:
        conn.close()


def test_measure_column_coverage_partial_fill() -> None:
    """Half filled → null_rate = 0.5."""
    conn = _make_silver_conn(
        persons=[
            {"id": "p1", "name_ja": "A", "gender": "male"},
            {"id": "p2", "name_ja": "B"},
        ]
    )
    try:
        row = measure_column_coverage(conn, "persons", "gender")
        assert row.total == 2
        assert row.non_null == 1
        assert row.null_rate == pytest.approx(0.5)
    finally:
        conn.close()


def test_measure_column_coverage_full_fill() -> None:
    """All rows have value → null_rate = 0.0."""
    conn = _make_silver_conn(
        persons=[
            {"id": "p1", "name_ja": "A", "gender": "female"},
            {"id": "p2", "name_ja": "B", "gender": "male"},
        ]
    )
    try:
        row = measure_column_coverage(conn, "persons", "gender")
        assert row.null_rate == pytest.approx(0.0)
    finally:
        conn.close()


def test_measure_column_coverage_empty_string() -> None:
    """Empty-string values count as null in empty_rate but not null_rate."""
    conn = _make_silver_conn(
        persons=[
            {"id": "p1", "name_ja": "A", "gender": ""},
            {"id": "p2", "name_ja": "B", "gender": "male"},
        ]
    )
    try:
        row = measure_column_coverage(conn, "persons", "gender")
        # gender='' is not NULL → non_null = 2
        assert row.non_null == 2
        # but non_empty excludes '' → non_empty = 1
        assert row.non_empty == 1
        assert row.empty_rate == pytest.approx(0.5)
    finally:
        conn.close()


def test_measure_column_coverage_nonexistent_column() -> None:
    """Non-existent column → error field is set."""
    conn = _make_silver_conn()
    try:
        row = measure_column_coverage(conn, "persons", "nonexistent_col_xyz")
        assert row.error is not None
        assert row.total == 0
    finally:
        conn.close()


def test_measure_column_coverage_empty_table() -> None:
    """Empty table → null_rate = 0.0 (no rows → nothing missing)."""
    conn = _make_silver_conn()
    try:
        row = measure_column_coverage(conn, "persons", "gender")
        assert row.total == 0
        assert row.null_rate == pytest.approx(0.0)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# find_bronze_source_with_value tests
# ---------------------------------------------------------------------------


def test_find_bronze_source_basic(tmp_path: Path) -> None:
    """BRONZE with 3 persons, 2 have gender → rows_with_value = 2."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "anilist", "persons", [
        {"id": "anilist:p1", "gender": "male"},
        {"id": "anilist:p2", "gender": "female"},
        {"id": "anilist:p3", "gender": None},
    ])
    bronze_conn = duckdb.connect(":memory:")
    try:
        stat = find_bronze_source_with_value(
            bronze_conn, bronze_root, "anilist", "persons", "gender"
        )
        assert stat.rows_with_value == 2
        assert stat.error is None
    finally:
        bronze_conn.close()


def test_find_bronze_source_empty_string_excluded(tmp_path: Path) -> None:
    """Empty-string values must NOT count as 'with value'."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "anilist", "persons", [
        {"id": "anilist:p1", "gender": ""},
        {"id": "anilist:p2", "gender": "male"},
    ])
    bronze_conn = duckdb.connect(":memory:")
    try:
        stat = find_bronze_source_with_value(
            bronze_conn, bronze_root, "anilist", "persons", "gender"
        )
        assert stat.rows_with_value == 1
    finally:
        bronze_conn.close()


def test_find_bronze_source_missing_column(tmp_path: Path) -> None:
    """BRONZE table without the target column → error set."""
    bronze_root = tmp_path / "bronze"
    _write_parquet(bronze_root, "anilist", "persons", [
        {"id": "anilist:p1", "name_ja": "A"},
    ])
    bronze_conn = duckdb.connect(":memory:")
    try:
        stat = find_bronze_source_with_value(
            bronze_conn, bronze_root, "anilist", "persons", "gender"
        )
        assert stat.error is not None
        assert stat.rows_with_value == 0
    finally:
        bronze_conn.close()


def test_find_bronze_source_nonexistent_parquet(tmp_path: Path) -> None:
    """No parquet files → error set, rows_with_value = 0."""
    bronze_root = tmp_path / "bronze"
    bronze_conn = duckdb.connect(":memory:")
    try:
        stat = find_bronze_source_with_value(
            bronze_conn, bronze_root, "anilist", "persons", "gender"
        )
        assert stat.rows_with_value == 0
        assert stat.error is not None
    finally:
        bronze_conn.close()


def test_find_bronze_source_dedup_across_snapshots(tmp_path: Path) -> None:
    """Two snapshot partitions with the same 2 rows → rows_with_value = 2 (not 4)."""
    import datetime as _dt
    bronze_root = tmp_path / "bronze"
    rows = [
        {"id": "anilist:p1", "gender": "male"},
        {"id": "anilist:p2", "gender": "female"},
    ]
    with BronzeWriter(
        "anilist", table="persons", root=bronze_root, compact_on_exit=False,
        date=_dt.date(2026, 4, 1)
    ) as bw:
        for row in rows:
            bw.append(row)
    with BronzeWriter(
        "anilist", table="persons", root=bronze_root, compact_on_exit=False,
        date=_dt.date(2026, 4, 2)
    ) as bw:
        for row in rows:
            bw.append(row)

    bronze_conn = duckdb.connect(":memory:")
    try:
        stat = find_bronze_source_with_value(
            bronze_conn, bronze_root, "anilist", "persons", "gender"
        )
        # Dedup: 2 distinct ids, both have gender
        assert stat.rows_with_value == 2
        assert stat.error is None
    finally:
        bronze_conn.close()


# ---------------------------------------------------------------------------
# _classify_severity tests
# ---------------------------------------------------------------------------


def test_classify_severity_critical() -> None:
    assert _classify_severity(0.95, 50_000) == "CRITICAL"


def test_classify_severity_critical_low_bronze() -> None:
    """null > 80% but BRONZE < 10K → not CRITICAL."""
    assert _classify_severity(0.95, 500) != "CRITICAL"


def test_classify_severity_high() -> None:
    assert _classify_severity(0.60, 5_000) == "HIGH"


def test_classify_severity_medium() -> None:
    assert _classify_severity(0.40, 0) == "MEDIUM"


def test_classify_severity_low() -> None:
    assert _classify_severity(0.20, 0) == "LOW"


def test_classify_severity_ok() -> None:
    assert _classify_severity(0.05, 0) == "OK"


def test_classify_severity_exactly_thresholds() -> None:
    """Boundary: null_rate = 0.80 exactly → not CRITICAL (need > 0.80)."""
    result = _classify_severity(0.80, 50_000)
    assert result != "CRITICAL"
    # 0.80 is not > 0.80, but it is > 0.50 and BRONZE > 1K → HIGH
    assert result == "HIGH"


# ---------------------------------------------------------------------------
# gap_analysis tests
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_bronze_root(tmp_path: Path) -> Path:
    """BRONZE with anilist persons (gender filled for 2 out of 3)."""
    root = tmp_path / "bronze"
    _write_parquet(root, "anilist", "persons", [
        {"id": "anilist:p1", "gender": "male"},
        {"id": "anilist:p2", "gender": "female"},
        {"id": "anilist:p3", "gender": None},
    ])
    return root


def test_gap_analysis_returns_gap_rows(
    minimal_bronze_root: Path,
) -> None:
    """gap_analysis must return a list of GapRow instances."""
    silver_conn = _make_silver_conn(
        persons=[{"id": "p1", "name_ja": "A"}, {"id": "p2", "name_ja": "B"}]
    )
    bronze_conn = duckdb.connect(":memory:")
    try:
        rows = gap_analysis(silver_conn, bronze_conn, minimal_bronze_root)
        assert isinstance(rows, list)
        assert len(rows) > 0
        assert all(isinstance(r, GapRow) for r in rows)
    finally:
        silver_conn.close()
        bronze_conn.close()


def test_gap_analysis_detects_gender_gap(
    minimal_bronze_root: Path,
) -> None:
    """When SILVER persons.gender is all-NULL but BRONZE has values, gap is detected."""
    # 100 persons with no gender in SILVER
    silver_conn = _make_silver_conn(
        persons=[{"id": f"p{i}", "name_ja": f"P{i}"} for i in range(100)]
    )
    bronze_conn = duckdb.connect(":memory:")
    try:
        rows = gap_analysis(silver_conn, bronze_conn, minimal_bronze_root)
        gender_row = next(
            (r for r in rows if r.silver_table == "persons" and r.silver_col == "gender"),
            None,
        )
        assert gender_row is not None
        assert gender_row.null_rate == pytest.approx(1.0)
        # severity: null_rate=1.0 but BRONZE only has 2 rows with value < 10K → HIGH or MEDIUM
        assert gender_row.severity in ("HIGH", "MEDIUM", "CRITICAL")
        assert gender_row.mapped is True
    finally:
        silver_conn.close()
        bronze_conn.close()


def test_gap_analysis_ok_when_silver_full(
    minimal_bronze_root: Path,
) -> None:
    """When SILVER column is fully filled → severity OK."""
    silver_conn = _make_silver_conn(
        persons=[
            {"id": "p1", "name_ja": "A", "gender": "male"},
            {"id": "p2", "name_ja": "B", "gender": "female"},
        ]
    )
    bronze_conn = duckdb.connect(":memory:")
    try:
        rows = gap_analysis(silver_conn, bronze_conn, minimal_bronze_root)
        gender_row = next(
            r for r in rows if r.silver_table == "persons" and r.silver_col == "gender"
        )
        assert gender_row.severity == "OK"
    finally:
        silver_conn.close()
        bronze_conn.close()


def test_gap_analysis_unmapped_column(tmp_path: Path) -> None:
    """Columns in SILVER_AUDIT_TARGETS but not in COLUMN_BRONZE_MAP → mapped=False."""
    silver_conn = _make_silver_conn(
        persons=[{"id": "p1", "name_ja": "A"}]
    )
    bronze_conn = duckdb.connect(":memory:")
    bronze_root = tmp_path / "bronze_empty"
    try:
        rows = gap_analysis(silver_conn, bronze_conn, bronze_root)
        # death_date has no BRONZE mapping
        death_row = next(
            (r for r in rows if r.silver_table == "persons" and r.silver_col == "death_date"),
            None,
        )
        assert death_row is not None
        assert death_row.mapped is False
    finally:
        silver_conn.close()
        bronze_conn.close()


def test_gap_analysis_no_bronze_dir(tmp_path: Path) -> None:
    """Non-existent bronze_root is handled gracefully (no exception)."""
    silver_conn = _make_silver_conn(
        persons=[{"id": "p1", "name_ja": "A"}]
    )
    bronze_conn = duckdb.connect(":memory:")
    bronze_root = tmp_path / "no_such_dir"
    try:
        rows = gap_analysis(silver_conn, bronze_conn, bronze_root)
        assert isinstance(rows, list)
    finally:
        silver_conn.close()
        bronze_conn.close()


# ---------------------------------------------------------------------------
# generate_report tests
# ---------------------------------------------------------------------------


def _make_gap_rows() -> list[GapRow]:
    """Build a minimal set of synthetic GapRows for report tests."""
    return [
        GapRow(
            silver_table="persons",
            silver_col="gender",
            total_silver=10_000,
            non_null_silver=460,
            null_rate=0.954,
            bronze_sources=[
                BronzeSourceStat("anilist", "persons", "gender", 120_000),
                BronzeSourceStat("bangumi", "persons", "gender", 80_000),
            ],
            severity="CRITICAL",
            mapped=True,
        ),
        GapRow(
            silver_table="anime",
            silver_col="country_of_origin",
            total_silver=562_191,
            non_null_silver=19_915,
            null_rate=0.965,
            bronze_sources=[
                BronzeSourceStat("anilist", "anime", "country_of_origin", 19_915),
            ],
            severity="HIGH",
            mapped=True,
        ),
        GapRow(
            silver_table="persons",
            silver_col="death_date",
            total_silver=10_000,
            non_null_silver=0,
            null_rate=1.0,
            bronze_sources=[],
            severity="MEDIUM",
            mapped=False,
        ),
        GapRow(
            silver_table="anime",
            silver_col="year",
            total_silver=562_191,
            non_null_silver=430_746,
            null_rate=0.234,
            bronze_sources=[
                BronzeSourceStat("anilist", "anime", "year", 19_915),
            ],
            severity="LOW",
            mapped=True,
        ),
    ]


def test_generate_report_creates_file(tmp_path: Path) -> None:
    """generate_report must create the output file."""
    rows = _make_gap_rows()
    out = tmp_path / "audit" / "silver_column_coverage.md"
    generate_report(rows, out)
    assert out.exists()


def test_generate_report_sections(tmp_path: Path) -> None:
    """Report must contain required section headers."""
    rows = _make_gap_rows()
    out = tmp_path / "report.md"
    generate_report(rows, out)
    text = out.read_text(encoding="utf-8")

    assert "# SILVER Column Coverage Audit" in text
    assert "## Summary" in text
    assert "## CRITICAL Gaps" in text
    assert "## Per-Table NULL Rate Overview" in text
    assert "## Full Gap Analysis" in text
    assert "Disclaimer" in text


def test_generate_report_severity_counts(tmp_path: Path) -> None:
    """Summary section must list correct CRITICAL count."""
    rows = _make_gap_rows()
    out = tmp_path / "report.md"
    generate_report(rows, out)
    text = out.read_text(encoding="utf-8")
    # One CRITICAL row
    assert "| CRITICAL (null>80%, BRONZE>10K) | 1 |" in text


def test_generate_report_disclaimers(tmp_path: Path) -> None:
    """Both JA and EN disclaimers must be present."""
    rows = _make_gap_rows()
    out = tmp_path / "report.md"
    generate_report(rows, out)
    text = out.read_text(encoding="utf-8")
    assert "Disclaimer (JA)" in text
    assert "Disclaimer (EN)" in text


def test_generate_report_no_critical_skips_section(tmp_path: Path) -> None:
    """If no CRITICAL gaps, the CRITICAL section must not appear."""
    rows = [
        GapRow(
            silver_table="anime",
            silver_col="year",
            total_silver=1000,
            non_null_silver=900,
            null_rate=0.1,
            bronze_sources=[],
            severity="LOW",
            mapped=True,
        )
    ]
    out = tmp_path / "report.md"
    generate_report(rows, out)
    text = out.read_text(encoding="utf-8")
    assert "## CRITICAL Gaps" not in text


def test_generate_report_creates_parent_dirs(tmp_path: Path) -> None:
    """generate_report must create parent directories if they don't exist."""
    rows = _make_gap_rows()
    out = tmp_path / "a" / "b" / "c" / "report.md"
    generate_report(rows, out)
    assert out.exists()


# ---------------------------------------------------------------------------
# COLUMN_BRONZE_MAP and SILVER_AUDIT_TARGETS sanity checks
# ---------------------------------------------------------------------------


def test_column_bronze_map_keys_are_tuples() -> None:
    """All COLUMN_BRONZE_MAP keys must be (table, col) tuples."""
    for key in COLUMN_BRONZE_MAP:
        assert isinstance(key, tuple), f"Expected tuple key, got {type(key)}"
        assert len(key) == 2, f"Expected 2-tuple, got {key}"
        silver_table, silver_col = key
        assert isinstance(silver_table, str)
        assert isinstance(silver_col, str)


def test_column_bronze_map_values_are_dicts() -> None:
    """All COLUMN_BRONZE_MAP values must be dicts of source → (table, col)."""
    for (_, _), sources in COLUMN_BRONZE_MAP.items():
        assert isinstance(sources, dict)
        for source, mapping in sources.items():
            assert isinstance(source, str)
            assert isinstance(mapping, tuple)
            assert len(mapping) == 2


def test_silver_audit_targets_coverage() -> None:
    """All tables in SILVER_AUDIT_TARGETS must include core tables."""
    tables = {t for t, _ in SILVER_AUDIT_TARGETS}
    assert "persons" in tables
    assert "anime" in tables
    assert "characters" in tables
    assert "studios" in tables


def test_silver_audit_targets_persons_gender() -> None:
    """persons.gender must be in SILVER_AUDIT_TARGETS."""
    persons_cols = next(
        cols for table, cols in SILVER_AUDIT_TARGETS if table == "persons"
    )
    assert "gender" in persons_cols


def test_silver_audit_targets_anime_country_of_origin() -> None:
    """anime.country_of_origin must be in SILVER_AUDIT_TARGETS."""
    anime_cols = next(
        cols for table, cols in SILVER_AUDIT_TARGETS if table == "anime"
    )
    assert "country_of_origin" in anime_cols
