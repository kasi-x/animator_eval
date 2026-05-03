"""Unit tests for src/etl/silver_loaders/madb.py

Verifies BRONZE parquet → SILVER DuckDB integration for all 6 mediaarts tables:
  - anime_broadcasters
  - anime_broadcast_schedule
  - anime_production_committee
  - anime_production_companies
  - anime_video_releases
  - anime_original_work_links

Uses synthetic parquet files written to a tmp directory.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.etl.silver_loaders import madb


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with tables pre-created."""
    c = duckdb.connect(":memory:")
    madb.create_tables(c)
    return c


@pytest.fixture()
def bronze_root(tmp_path: Path) -> Path:
    """Write minimal synthetic parquet files under tmp_path/source=mediaarts/..."""
    root = tmp_path

    def _write(table_name: str, schema: pa.Schema, rows: dict) -> None:
        date_dir = root / "source=mediaarts" / f"table={table_name}" / "date=2026-04-27"
        date_dir.mkdir(parents=True, exist_ok=True)
        arrays = [pa.array(v, type=schema.field(k).type) for k, v in rows.items()]
        tbl = pa.Table.from_arrays(arrays, schema=schema)
        pq.write_table(tbl, date_dir / "data.parquet")

    # broadcasters
    _write(
        "broadcasters",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("name", pa.string()),
            pa.field("is_network_station", pa.bool_()),
        ]),
        {
            "madb_id":             ["C001", "C001", "C002"],
            "name":                ["NHK", "NHK", "TBS"],
            "is_network_station":  [True, True, False],
        },
    )

    # broadcast_schedule
    _write(
        "broadcast_schedule",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("raw_text", pa.string()),
        ]),
        {
            "madb_id":  ["C001", "C002"],
            "raw_text": ["日曜日09:00~", "月曜日23:00~"],
        },
    )

    # production_committee
    _write(
        "production_committee",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("company_name", pa.string()),
            pa.field("role_label", pa.string()),
        ]),
        {
            "madb_id":      ["C001", "C001"],
            "company_name": ["Studio A", "Distributor B"],
            "role_label":   ["幹事", None],
        },
    )

    # production_companies
    _write(
        "production_companies",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("company_name", pa.string()),
            pa.field("role_label", pa.string()),
            pa.field("is_main", pa.bool_()),
        ]),
        {
            "madb_id":      ["C001", "C001"],
            "company_name": ["Studio A", "Studio B"],
            "role_label":   ["制作", "制作協力"],
            "is_main":      [True, False],
        },
    )

    # video_releases
    _write(
        "video_releases",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("series_madb_id", pa.string()),
            pa.field("media_format", pa.string()),
            pa.field("date_published", pa.string()),
            pa.field("publisher", pa.string()),
            pa.field("product_id", pa.string()),
            pa.field("gtin", pa.string()),
            pa.field("runtime_min", pa.int64()),
            pa.field("volume_number", pa.string()),
            pa.field("release_title", pa.string()),
        ]),
        {
            "madb_id":        ["R001", "R002"],
            "series_madb_id": ["C001", "C001"],
            "media_format":   ["BD", "DVD"],
            "date_published": ["2020-01-01", "2020-03-01"],
            "publisher":      ["Aniplex", "Aniplex"],
            "product_id":     ["ANZX-1234", "ANSB-5678"],
            "gtin":           ["", ""],
            "runtime_min":    [120, 90],
            "volume_number":  ["Vol.1", "Vol.2"],
            "release_title":  ["Title A Vol.1", "Title A Vol.2"],
        },
    )

    # original_work_links
    _write(
        "original_work_links",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("work_name", pa.string()),
            pa.field("creator_text", pa.string()),
            pa.field("series_link_id", pa.string()),
        ]),
        {
            "madb_id":        ["C001"],
            "work_name":      ["原作マンガ"],
            "creator_text":   ["作者 太郎"],
            "series_link_id": ["SL001"],
        },
    )

    # anime (for anime.studios[] array path — 22/02)
    # C001 has Studio A (also in production_companies), C002 has Studio C (only in array)
    _write(
        "anime",
        pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("title_ja", pa.string()),
            pa.field("studios", pa.list_(pa.string())),
        ]),
        {
            "madb_id":   ["C001", "C002"],
            "title_ja":  ["テスト1", "テスト2"],
            "studios":   [["Studio A"], ["Studio C"]],
        },
    )

    return root


# ---------------------------------------------------------------------------
# create_tables
# ---------------------------------------------------------------------------

class TestCreateTables:
    def test_all_tables_exist(self, conn: duckdb.DuckDBPyConnection) -> None:
        expected = {
            "anime_broadcasters",
            "anime_broadcast_schedule",
            "anime_production_committee",
            "anime_production_companies",
            "anime_video_releases",
            "anime_original_work_links",
        }
        tables = {
            row[0]
            for row in conn.execute("SHOW TABLES").fetchall()
        }
        assert expected.issubset(tables)

    def test_idempotent(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Calling create_tables twice must not raise."""
        madb.create_tables(conn)  # second call


# ---------------------------------------------------------------------------
# integrate
# ---------------------------------------------------------------------------

class TestIntegrate:
    def test_returns_counts_for_all_tables(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        expected_keys = {
            "anime_broadcasters",
            "anime_broadcast_schedule",
            "anime_production_committee",
            "anime_production_companies",
            "anime_video_releases",
            "anime_original_work_links",
        }
        assert expected_keys.issubset(counts.keys())

    def test_no_errors(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        error_keys = [k for k in counts if k.endswith("_error")]
        assert error_keys == [], f"Unexpected errors: {error_keys}"

    def test_broadcasters_row_count(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        # 3 input rows but "C001 / NHK" is duplicated → 2 distinct rows
        assert counts["anime_broadcasters"] == 2

    def test_broadcast_schedule_row_count(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        assert counts["anime_broadcast_schedule"] == 2

    def test_production_committee_row_count(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        assert counts["anime_production_committee"] == 2

    def test_production_companies_row_count(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        assert counts["anime_production_companies"] == 2

    def test_video_releases_row_count(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        assert counts["anime_video_releases"] == 2

    def test_original_work_links_row_count(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        counts = madb.integrate(conn, bronze_root)
        assert counts["anime_original_work_links"] == 1

    def test_idempotent_insert(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """Calling integrate twice must not increase row counts."""
        counts1 = madb.integrate(conn, bronze_root)
        counts2 = madb.integrate(conn, bronze_root)
        for key in [
            "anime_broadcasters",
            "anime_broadcast_schedule",
            "anime_production_committee",
            "anime_production_companies",
            "anime_video_releases",
            "anime_original_work_links",
        ]:
            assert counts1[key] == counts2[key], f"{key}: {counts1[key]} != {counts2[key]}"

    def test_is_network_station_cast(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """is_network_station BOOLEAN → INTEGER cast."""
        madb.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT is_network_station FROM anime_broadcasters WHERE broadcaster_name = 'NHK'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1  # True → 1

    def test_is_main_cast(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """is_main BOOLEAN → INTEGER cast."""
        madb.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT is_main FROM anime_production_companies WHERE company_name = 'Studio A'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1  # True → 1

    def test_video_releases_anime_id_from_series(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """BRONZE series_madb_id → SILVER anime_id."""
        madb.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT anime_id FROM anime_video_releases WHERE release_madb_id = 'R001'"
        ).fetchall()
        assert rows[0][0] == "C001"

    def test_missing_bronze_dir_records_error(
        self, conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """Empty bronze root (no parquet files) records errors but does not raise."""
        counts = madb.integrate(conn, tmp_path)
        error_keys = [k for k in counts if k.endswith("_error")]
        assert len(error_keys) > 0

    def test_null_madb_id_skipped(
        self, tmp_path: Path
    ) -> None:
        """Rows with NULL madb_id are not inserted."""
        root = tmp_path
        date_dir = root / "source=mediaarts" / "table=broadcasters" / "date=2026-04-27"
        date_dir.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("name", pa.string()),
            pa.field("is_network_station", pa.bool_()),
        ])
        tbl = pa.Table.from_arrays(
            [
                pa.array([None, "C001"], type=pa.string()),
                pa.array(["Ghost", "NHK"], type=pa.string()),
                pa.array([False, True], type=pa.bool_()),
            ],
            schema=schema,
        )
        pq.write_table(tbl, date_dir / "data.parquet")

        c = duckdb.connect(":memory:")
        counts = madb.integrate(c, root)
        assert counts["anime_broadcasters"] == 1

    # -- 22/02 tests: anime_studios coverage improvement --

    def test_is_main_false_gets_support_role(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """22/02: production_companies rows with is_main=False get role='support'."""
        madb.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT role FROM anime_studios WHERE studio_id = 'madb:n:Studio B'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "support"

    def test_is_main_true_preserves_role_label(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """22/02: production_companies rows with is_main=True retain role_label."""
        madb.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT role FROM anime_studios WHERE studio_id = 'madb:n:Studio A' AND source = 'mediaarts'"
        ).fetchall()
        # Studio A has is_main=True + role_label='制作' from production_companies
        assert len(rows) >= 1
        role_labels = {r[0] for r in rows}
        assert "制作" in role_labels

    def test_anime_studios_array_path_adds_new_anime(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """22/02: anime.studios[] array inserts anime_studios for anime not in production_companies."""
        madb.integrate(conn, bronze_root)
        # C002 has no production_companies entry but IS in anime.studios[] (Studio C)
        rows = conn.execute(
            "SELECT anime_id, studio_id, source FROM anime_studios WHERE anime_id = 'madb:C002'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "madb:C002"
        assert rows[0][1] == "madb:n:Studio C"
        assert rows[0][2] == "mediaarts"

    def test_anime_studios_array_studio_registered(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """22/02: studios inserted from anime.studios[] appear in the studios table."""
        madb.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT id, name FROM studios WHERE id = 'madb:n:Studio C'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "Studio C"

    def test_anime_studios_idempotent_with_array_path(
        self, conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        """22/02: running integrate twice does not duplicate anime_studios rows."""
        counts1 = madb.integrate(conn, bronze_root)
        counts2 = madb.integrate(conn, bronze_root)
        assert counts1["anime_studios_mediaarts"] == counts2["anime_studios_mediaarts"]

    def test_anime_studios_missing_anime_parquet_no_error(
        self, conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """22/02: missing anime parquet for the array path does not add _error keys."""
        # Only write production_companies, not anime table
        date_dir = tmp_path / "source=mediaarts" / "table=production_companies" / "date=2026-04-27"
        date_dir.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([
            pa.field("madb_id", pa.string()),
            pa.field("company_name", pa.string()),
            pa.field("role_label", pa.string()),
            pa.field("is_main", pa.bool_()),
        ])
        tbl = pa.Table.from_arrays(
            [pa.array(["C001"]), pa.array(["Studio A"]), pa.array(["アニメーション制作"]), pa.array([True])],
            schema=schema,
        )
        pq.write_table(tbl, date_dir / "data.parquet")

        counts = madb.integrate(conn, tmp_path)
        array_errors = [k for k in counts if "array_error" in k]
        assert array_errors == [], f"Unexpected array errors: {array_errors}"
