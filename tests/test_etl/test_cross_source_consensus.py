"""Tests for src/etl/audit/cross_source_consensus.py.

Covers:
- classify_consensus: all 5 consensus_flag categories
- classify_consensus_date: subset-compatible date grouping
- collect_consensus: integration with in-memory DuckDB fixtures
- export_consensus: CSV output with header + correct filename suffix
- date_parser: parse_date, is_date_subset_compatible, normalize_date
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from src.etl.audit.cross_source_consensus import (
    ConsensusResult,
    classify_consensus,
    classify_consensus_date,
    collect_consensus,
    export_consensus,
)
from src.etl.normalize.date_parser import (
    is_date_subset_compatible,
    normalize_date,
    parse_date,
    pick_most_precise_date,
    precision_score,
    to_iso8601,
)


# ---------------------------------------------------------------------------
# Fixtures: minimal in-memory DBs (shared schema with test_cross_source_diff.py
# but extended to 3+ sources for consensus testing)
# ---------------------------------------------------------------------------


@pytest.fixture()
def resolved_conn() -> duckdb.DuckDBPyConnection:
    """Resolved.duckdb equivalent with anime / persons / studios."""
    conn = duckdb.connect(":memory:")

    for table in ("anime", "persons", "studios"):
        conn.execute(
            f"""
            CREATE TABLE {table} (
                canonical_id    VARCHAR PRIMARY KEY,
                source_ids_json VARCHAR NOT NULL DEFAULT '[]'
            )
            """
        )

    # anime: mix of 2-source, 3-source and 4-source canonical entities
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?)",
        [
            # 3 sources — title_ja unanimous, year majority
            (
                "resolved:anime:3src",
                json.dumps(["anilist:a10", "mal:a10", "bgm:a10"]),
            ),
            # 4 sources — title_ja tie (2 vs 2)
            (
                "resolved:anime:tie",
                json.dumps(["anilist:a20", "mal:a20", "bgm:a20", "ann:a20"]),
            ),
            # 3 sources — unique_outlier (2/3 agree, 1 odd)
            (
                "resolved:anime:outlier",
                json.dumps(["anilist:a30", "mal:a30", "bgm:a30"]),
            ),
            # 2 sources — plurality (each has different value → tie at 1 each)
            (
                "resolved:anime:2src",
                json.dumps(["anilist:a40", "mal:a40"]),
            ),
            # single-source → excluded
            (
                "resolved:anime:single",
                json.dumps(["anilist:a99"]),
            ),
        ],
    )

    # persons: 3 sources
    conn.executemany(
        "INSERT INTO persons VALUES (?, ?)",
        [
            (
                "resolved:person:p10",
                json.dumps(["anilist:p10", "mal:p10", "keyframe:p10"]),
            ),
        ],
    )

    # studios: 2 sources
    conn.executemany(
        "INSERT INTO studios VALUES (?, ?)",
        [
            (
                "resolved:studio:s10",
                json.dumps(["anilist:s10", "mal:s10"]),
            ),
        ],
    )

    yield conn
    conn.close()


@pytest.fixture()
def silver_conn() -> duckdb.DuckDBPyConnection:
    """Silver.duckdb with anime / persons / studios tables."""
    conn = duckdb.connect(":memory:")

    conn.execute(
        """
        CREATE TABLE anime (
            id          VARCHAR PRIMARY KEY,
            title_ja    VARCHAR,
            title_en    VARCHAR,
            year        VARCHAR,
            start_date  VARCHAR,
            end_date    VARCHAR,
            episodes    VARCHAR,
            format      VARCHAR,
            duration    VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE persons (
            id          VARCHAR PRIMARY KEY,
            name_ja     VARCHAR,
            name_en     VARCHAR,
            birth_date  VARCHAR,
            gender      VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE studios (
            id                VARCHAR PRIMARY KEY,
            name              VARCHAR,
            country_of_origin VARCHAR
        )
        """
    )

    # --- 3src: title_ja unanimous ("進撃の巨人" × 3), year majority (2011 × 2, 2010 × 1)
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("anilist:a10", "進撃の巨人", "Attack on Titan", "2011", None, None, "25", "TV", "24"),
            ("mal:a10",    "進撃の巨人", "Attack on Titan", "2011", None, None, "25", "TV", "24"),
            ("bgm:a10",    "進撃の巨人", "Attack on Titan", "2010", None, None, "25", "TV", "24"),
        ],
    )

    # --- tie: title_ja 2 × "魔法少女まどか☆マギカ" vs 2 × "魔法少女まどかマギカ"
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("anilist:a20", "魔法少女まどか☆マギカ", "Madoka Magica", "2011", None, None, "12", "TV", "24"),
            ("mal:a20",    "魔法少女まどか☆マギカ", "Madoka Magica", "2011", None, None, "12", "TV", "24"),
            ("bgm:a20",    "魔法少女まどかマギカ",   "Madoka Magica", "2011", None, None, "12", "TV", "24"),
            ("ann:a20",    "魔法少女まどかマギカ",   "Madoka Magica", "2011", None, None, "12", "TV", "24"),
        ],
    )

    # --- outlier: 2/3 title_ja = "ソードアートオンライン"; bgm has a typo
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("anilist:a30", "ソードアートオンライン", "Sword Art Online", "2012", None, None, "25", "TV", "23"),
            ("mal:a30",    "ソードアートオンライン", "Sword Art Online", "2012", None, None, "25", "TV", "23"),
            ("bgm:a30",    "ソードアートオンラィン", "Sword Art Online", "2012", None, None, "25", "TV", "23"),
        ],
    )

    # --- 2src: two different title_ja values → tie at 1 each
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("anilist:a40", "鬼滅の刃", "Demon Slayer", "2019", None, None, "26", "TV", "23"),
            ("mal:a40",    "鬼滅の刃：無限列車編", "Demon Slayer", "2020", None, None, "7", "TV", "23"),
        ],
    )

    # --- persons: name_ja unanimous, name_en unanimous, birth_date majority
    conn.executemany(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        [
            ("anilist:p10",  "田中 一郎", "Ichiro Tanaka", "1985-03-15", "Male"),
            ("mal:p10",      "田中 一郎", "Ichiro Tanaka", "1985-03-15", "Male"),
            ("keyframe:p10", "田中 一郎", "Ichiro Tanaka", "1985-03-16", "Male"),
        ],
    )

    # --- studios: name identical_after_normalize → normalized unanimous
    conn.executemany(
        "INSERT INTO studios VALUES (?, ?, ?)",
        [
            ("anilist:s10", "J.C.STAFF", "JP"),
            ("mal:s10",     "JC STAFF",  "JP"),
        ],
    )

    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# classify_consensus unit tests
# ---------------------------------------------------------------------------


class TestClassifyConsensusUnanimous:
    """n_distinct_values == 1 → unanimous."""

    def test_all_agree(self) -> None:
        result = classify_consensus(
            {"anilist": "進撃の巨人", "mal": "進撃の巨人", "bgm": "進撃の巨人"}
        )
        assert result.consensus_flag == "unanimous"
        assert result.majority_value == "進撃の巨人"
        assert result.majority_count == 3
        assert result.majority_share == 1.0
        assert result.outlier_sources == []
        assert result.outlier_values == []

    def test_two_sources_agree(self) -> None:
        result = classify_consensus({"anilist": "Attack on Titan", "mal": "Attack on Titan"})
        assert result.consensus_flag == "unanimous"
        assert result.majority_share == 1.0

    def test_empty_sources_returns_unanimous(self) -> None:
        result = classify_consensus({})
        assert result.consensus_flag == "unanimous"
        assert result.majority_value is None
        assert result.majority_count == 0

    def test_all_null_sources_unanimous(self) -> None:
        result = classify_consensus({"anilist": None, "mal": None})
        assert result.consensus_flag == "unanimous"
        assert result.majority_value is None

    def test_null_sources_excluded_from_count(self) -> None:
        # one null + two agree → unanimous on the two non-null
        result = classify_consensus({"anilist": "進撃の巨人", "mal": "進撃の巨人", "bgm": None})
        assert result.consensus_flag == "unanimous"
        assert result.majority_count == 2


class TestClassifyConsensusMajority:
    """majority_share > 50%, multiple outliers (not unique_outlier).

    unique_outlier requires exactly 1 minority source; majority requires 2+.
    """

    def test_3_of_4_is_unique_outlier_not_majority(self) -> None:
        # 3/4 agree, 1 outlier → unique_outlier (exactly 1 minority source)
        result = classify_consensus(
            {"anilist": "A", "mal": "A", "bgm": "A", "ann": "B"}
        )
        assert result.consensus_flag == "unique_outlier"
        assert result.majority_value == "A"
        assert result.majority_count == 3
        assert abs(result.majority_share - 0.75) < 1e-6

    def test_majority_with_two_outlier_sources(self) -> None:
        """3/5 agree with 2 distinct outlier sources → majority (not unique_outlier)."""
        result = classify_consensus(
            {
                "anilist": "Sword Art Online",
                "mal":     "Sword Art Online",
                "bgm":     "Sword Art Online",
                "ann":     "SAO",
                "madb":    "other",
            }
        )
        assert result.consensus_flag == "majority"
        assert result.majority_value == "Sword Art Online"
        assert set(result.outlier_sources) == {"ann", "madb"}


class TestClassifyConsensusUniqueOutlier:
    """majority_share > 50% AND exactly one outlier source."""

    def test_2_of_3_unique_outlier(self) -> None:
        result = classify_consensus(
            {"anilist": "ソードアートオンライン", "mal": "ソードアートオンライン", "bgm": "ソードアートオンラィン"}
        )
        assert result.consensus_flag == "unique_outlier"
        assert result.majority_value == "ソードアートオンライン"
        assert result.majority_count == 2
        assert result.outlier_sources == ["bgm"]
        assert result.outlier_values == ["ソードアートオンラィン"]

    def test_4_of_5_unique_outlier(self) -> None:
        result = classify_consensus(
            {
                "anilist": "X", "mal": "X", "bgm": "X", "ann": "X", "madb": "Y"
            }
        )
        assert result.consensus_flag == "unique_outlier"
        assert result.outlier_sources == ["madb"]


class TestClassifyConsensusPlurality:
    """Leader has ≤ 50% share and no tie at the top."""

    def test_2_1_1_plurality(self) -> None:
        # 4 sources: A=2, B=1, C=1 → A is plurality leader (50% = not majority)
        result = classify_consensus(
            {"anilist": "A", "mal": "A", "bgm": "B", "ann": "C"}
        )
        # 2/4 = 50% which is NOT > 50%, so it should be plurality (not tie since only A leads)
        assert result.consensus_flag == "plurality"
        assert result.majority_value == "A"
        assert result.majority_count == 2

    def test_3_sources_3_distinct_values_plurality(self) -> None:
        # 3 sources, all different → each has 1/3; top_count == second_count → tie
        result = classify_consensus({"anilist": "A", "mal": "B", "bgm": "C"})
        # All tied at 1 each → tie
        assert result.consensus_flag == "tie"


class TestClassifyConsensusTie:
    """Two or more values share the top count."""

    def test_2_vs_2_tie(self) -> None:
        result = classify_consensus(
            {
                "anilist": "魔法少女まどか☆マギカ",
                "mal":     "魔法少女まどか☆マギカ",
                "bgm":     "魔法少女まどかマギカ",
                "ann":     "魔法少女まどかマギカ",
            }
        )
        assert result.consensus_flag == "tie"
        assert result.majority_count == 2

    def test_tie_majority_value_resolved_by_priority(self) -> None:
        # anilist > mal: if tied, anilist's value wins.
        result = classify_consensus({"anilist": "A", "mal": "B"})
        assert result.consensus_flag == "tie"
        assert result.majority_value == "A"  # anilist priority


class TestClassifyConsensusReturnType:
    """ConsensusResult is a frozen dataclass."""

    def test_returns_consensus_result(self) -> None:
        result = classify_consensus({"anilist": "X", "mal": "X"})
        assert isinstance(result, ConsensusResult)

    def test_majority_share_rounded(self) -> None:
        result = classify_consensus({"a": "X", "b": "X", "c": "Y"})
        # 2/3 ≈ 0.6667 — check share is a float in [0, 1]
        assert 0.0 <= result.majority_share <= 1.0


# ---------------------------------------------------------------------------
# collect_consensus integration tests
# ---------------------------------------------------------------------------


class TestCollectConsensus:
    """Integration tests using in-memory DuckDB fixtures."""

    def test_anime_records_produced(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        assert len(records) > 0

    def test_single_source_excluded(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        cids = {r["canonical_id"] for r in records}
        assert "resolved:anime:single" not in cids

    def test_record_schema(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        required_keys = {
            "canonical_id", "attribute", "n_sources", "n_distinct_values",
            "values_json", "majority_value", "majority_count", "majority_share",
            "consensus_flag", "outlier_sources", "outlier_values",
            "normalized_consensus_flag", "normalized_majority_value",
        }
        for rec in records:
            assert required_keys.issubset(rec.keys()), f"Missing keys in: {rec}"

    def test_3src_title_ja_unanimous(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """All 3 sources agree on title_ja → unanimous."""
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:anime:3src" and r["attribute"] == "title_ja"
        )
        assert rec["consensus_flag"] == "unanimous"
        assert rec["majority_value"] == "進撃の巨人"
        assert rec["n_sources"] == 3
        assert rec["n_distinct_values"] == 1
        assert json.loads(rec["outlier_sources"]) == []  # type: ignore[arg-type]

    def test_3src_year_majority(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """2/3 sources agree on year=2011 → unique_outlier (bgm has 2010)."""
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:anime:3src" and r["attribute"] == "year"
        )
        assert rec["consensus_flag"] == "unique_outlier"
        assert rec["majority_value"] == "2011"
        assert json.loads(rec["outlier_sources"]) == ["bgm"]  # type: ignore[arg-type]

    def test_tie_entity(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """4 sources split 2-2 on title_ja → tie."""
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:anime:tie" and r["attribute"] == "title_ja"
        )
        assert rec["consensus_flag"] == "tie"
        assert rec["n_sources"] == 4
        assert rec["n_distinct_values"] == 2

    def test_outlier_entity(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """3 sources with 1 outlier → unique_outlier for title_ja."""
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:anime:outlier" and r["attribute"] == "title_ja"
        )
        assert rec["consensus_flag"] == "unique_outlier"
        assert rec["majority_value"] == "ソードアートオンライン"
        assert rec["majority_count"] == 2
        outlier_srcs = json.loads(rec["outlier_sources"])  # type: ignore[arg-type]
        assert outlier_srcs == ["bgm"]

    def test_normalized_consensus_flag_present(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        for rec in records:
            assert rec["normalized_consensus_flag"] in {
                "unanimous", "majority", "unique_outlier", "plurality", "tie"
            }

    def test_normalized_flag_can_differ_from_raw(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """studios name: raw=tie (J.C.STAFF vs JC STAFF), normalized=unanimous."""
        records = collect_consensus(resolved_conn, "studios", silver_conn)
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:studio:s10" and r["attribute"] == "name"
        )
        # Raw values differ → not unanimous; normalized both reduce to "jcstaff" → unanimous
        assert rec["normalized_consensus_flag"] == "unanimous"

    def test_persons_records_produced(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "persons", silver_conn)
        assert len(records) > 0

    def test_persons_birth_date_unique_outlier(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        """2/3 agree on birth_date; keyframe differs by 1 day → unique_outlier."""
        records = collect_consensus(resolved_conn, "persons", silver_conn)
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:person:p10" and r["attribute"] == "birth_date"
        )
        assert rec["consensus_flag"] == "unique_outlier"
        assert rec["majority_value"] == "1985-03-15"

    def test_all_consensus_flags_valid(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        valid_flags = {"unanimous", "majority", "unique_outlier", "plurality", "tie"}
        for entity in ("anime", "persons", "studios"):
            records = collect_consensus(resolved_conn, entity, silver_conn)
            for rec in records:
                assert rec["consensus_flag"] in valid_flags, (
                    f"Unknown flag {rec['consensus_flag']!r} for {entity}"
                )

    def test_values_json_is_valid_json(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        for rec in records:
            parsed = json.loads(rec["values_json"])  # type: ignore[arg-type]
            assert isinstance(parsed, dict)

    def test_majority_share_in_range(
        self,
        resolved_conn: duckdb.DuckDBPyConnection,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        records = collect_consensus(resolved_conn, "anime", silver_conn)
        for rec in records:
            share = float(rec["majority_share"])  # type: ignore[arg-type]
            assert 0.0 <= share <= 1.0


# ---------------------------------------------------------------------------
# export_consensus integration tests
# ---------------------------------------------------------------------------


class TestExportConsensus:
    """Test export_consensus CSV output via in-memory DBs written to tmp paths."""

    @pytest.fixture()
    def tmp_resolved_db(
        self,
        tmp_path: Path,
        resolved_conn: duckdb.DuckDBPyConnection,
    ) -> Path:
        """Export in-memory resolved DB to a temp file."""
        p = tmp_path / "resolved.duckdb"
        out = duckdb.connect(str(p))
        for table in ("anime", "persons", "studios"):
            out.execute(
                f"CREATE TABLE {table} (canonical_id VARCHAR, source_ids_json VARCHAR)"
            )
            for row in resolved_conn.execute(f"SELECT * FROM {table}").fetchall():
                out.execute(f"INSERT INTO {table} VALUES (?, ?)", list(row))
        out.close()
        return p

    @pytest.fixture()
    def tmp_silver_db(
        self,
        tmp_path: Path,
        silver_conn: duckdb.DuckDBPyConnection,
    ) -> Path:
        """Export in-memory silver DB to a temp file."""
        p = tmp_path / "silver.duckdb"
        out = duckdb.connect(str(p))

        out.execute(
            """
            CREATE TABLE anime (
                id VARCHAR, title_ja VARCHAR, title_en VARCHAR, year VARCHAR,
                start_date VARCHAR, end_date VARCHAR, episodes VARCHAR,
                format VARCHAR, duration VARCHAR
            )
            """
        )
        out.execute(
            """
            CREATE TABLE persons (
                id VARCHAR, name_ja VARCHAR, name_en VARCHAR,
                birth_date VARCHAR, gender VARCHAR
            )
            """
        )
        out.execute(
            """
            CREATE TABLE studios (
                id VARCHAR, name VARCHAR, country_of_origin VARCHAR
            )
            """
        )

        for table, ncols in [("anime", 9), ("persons", 5), ("studios", 3)]:
            placeholders = "(" + ",".join(["?"] * ncols) + ")"
            for row in silver_conn.execute(f"SELECT * FROM {table}").fetchall():
                out.execute(f"INSERT INTO {table} VALUES {placeholders}", list(row))

        out.close()
        return p

    def test_csv_files_created_with_consensus_suffix(
        self,
        tmp_path: Path,
        tmp_resolved_db: Path,
        tmp_silver_db: Path,
    ) -> None:
        out_dir = tmp_path / "audit"
        export_consensus(tmp_resolved_db, tmp_silver_db, out_dir)
        for entity in ("anime", "persons", "studios"):
            assert (out_dir / f"{entity}_consensus.csv").exists(), (
                f"Missing {entity}_consensus.csv"
            )

    def test_no_plain_entity_csv_overwrite(
        self,
        tmp_path: Path,
        tmp_resolved_db: Path,
        tmp_silver_db: Path,
    ) -> None:
        """Output must use _consensus.csv suffix — never overwrite pairwise diff CSVs."""
        out_dir = tmp_path / "audit"
        export_consensus(tmp_resolved_db, tmp_silver_db, out_dir)
        for entity in ("anime", "persons", "studios"):
            assert not (out_dir / f"{entity}.csv").exists(), (
                f"{entity}.csv should not be created by export_consensus"
            )

    def test_csv_has_expected_header(
        self,
        tmp_path: Path,
        tmp_resolved_db: Path,
        tmp_silver_db: Path,
    ) -> None:
        out_dir = tmp_path / "audit"
        export_consensus(tmp_resolved_db, tmp_silver_db, out_dir)
        with open(out_dir / "anime_consensus.csv", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            fieldnames = set(reader.fieldnames or [])
        expected = {
            "canonical_id", "attribute", "n_sources", "n_distinct_values",
            "values_json", "majority_value", "majority_count", "majority_share",
            "consensus_flag", "outlier_sources", "outlier_values",
            "normalized_consensus_flag", "normalized_majority_value",
        }
        assert expected == fieldnames

    def test_counts_returned(
        self,
        tmp_path: Path,
        tmp_resolved_db: Path,
        tmp_silver_db: Path,
    ) -> None:
        out_dir = tmp_path / "audit"
        counts = export_consensus(tmp_resolved_db, tmp_silver_db, out_dir)
        assert set(counts.keys()) == {"anime", "persons", "studios"}
        assert counts["anime"] > 0
        assert counts["persons"] > 0
        assert counts["studios"] > 0

    def test_anime_csv_row_count_matches(
        self,
        tmp_path: Path,
        tmp_resolved_db: Path,
        tmp_silver_db: Path,
    ) -> None:
        out_dir = tmp_path / "audit"
        counts = export_consensus(tmp_resolved_db, tmp_silver_db, out_dir)
        with open(out_dir / "anime_consensus.csv", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            actual_rows = list(reader)
        assert len(actual_rows) == counts["anime"]

    def test_unanimous_row_has_no_outliers(
        self,
        tmp_path: Path,
        tmp_resolved_db: Path,
        tmp_silver_db: Path,
    ) -> None:
        out_dir = tmp_path / "audit"
        export_consensus(tmp_resolved_db, tmp_silver_db, out_dir)
        with open(out_dir / "anime_consensus.csv", newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

        unanimous_rows = [
            r for r in rows
            if r["consensus_flag"] == "unanimous"
            and r["canonical_id"] == "resolved:anime:3src"
            and r["attribute"] == "title_ja"
        ]
        assert len(unanimous_rows) == 1
        assert json.loads(unanimous_rows[0]["outlier_sources"]) == []
        assert json.loads(unanimous_rows[0]["outlier_values"]) == []


# ---------------------------------------------------------------------------
# date_parser unit tests
# ---------------------------------------------------------------------------


class TestParseDate:
    """parse_date: multi-format input → (year, month, day) tuple."""

    def test_iso_full(self) -> None:
        assert parse_date("2020-04-15") == (2020, 4, 15)

    def test_iso_year_month(self) -> None:
        assert parse_date("2020-04") == (2020, 4, None)

    def test_iso_year_only(self) -> None:
        assert parse_date("2020") == (2020, None, None)

    def test_slash_full(self) -> None:
        assert parse_date("2020/04/15") == (2020, 4, 15)

    def test_dot_full(self) -> None:
        assert parse_date("2020.04.15") == (2020, 4, 15)

    def test_english_month_day_year(self) -> None:
        assert parse_date("April 15, 2020") == (2020, 4, 15)

    def test_english_abbreviated_month(self) -> None:
        assert parse_date("Apr 15 2020") == (2020, 4, 15)

    def test_english_year_month_day(self) -> None:
        assert parse_date("2020 Apr 15") == (2020, 4, 15)

    def test_json_struct(self) -> None:
        assert parse_date('{"year": 2025, "month": 4, "day": 8}') == (2025, 4, 8)

    def test_json_struct_partial(self) -> None:
        parsed = parse_date('{"year": 2025, "month": 4}')
        assert parsed is not None
        assert parsed[0] == 2025
        assert parsed[1] == 4
        assert parsed[2] is None

    def test_none_returns_none(self) -> None:
        assert parse_date(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert parse_date("") is None

    def test_unparseable_returns_none(self) -> None:
        assert parse_date("not-a-date") is None

    def test_unparseable_partial_returns_none(self) -> None:
        assert parse_date("abc-04-15") is None


class TestToIso8601:
    """to_iso8601: DateTuple → ISO string with XX placeholders."""

    def test_full_date(self) -> None:
        assert to_iso8601((2020, 4, 15)) == "2020-04-15"

    def test_year_month(self) -> None:
        assert to_iso8601((2020, 4, None)) == "2020-04-XX"

    def test_year_only(self) -> None:
        assert to_iso8601((2020, None, None)) == "2020-XX-XX"

    def test_no_year(self) -> None:
        assert to_iso8601((None, None, None)) == "XXXX-XX-XX"

    def test_single_digit_month_padded(self) -> None:
        assert to_iso8601((2020, 1, 5)) == "2020-01-05"


class TestNormalizeDate:
    """normalize_date: convenience parse+serialize wrapper."""

    def test_slash_to_iso(self) -> None:
        assert normalize_date("2020/04/15") == "2020-04-15"

    def test_year_only_to_iso(self) -> None:
        assert normalize_date("2020") == "2020-XX-XX"

    def test_english_to_iso(self) -> None:
        assert normalize_date("April 15, 2020") == "2020-04-15"

    def test_none_returns_none(self) -> None:
        assert normalize_date(None) is None

    def test_unparseable_returns_none(self) -> None:
        assert normalize_date("garbage") is None


class TestPrecisionScore:
    """precision_score: 0 (no data) → 3 (full date)."""

    def test_no_data(self) -> None:
        assert precision_score((None, None, None)) == 0

    def test_year_only(self) -> None:
        assert precision_score((2020, None, None)) == 1

    def test_year_month(self) -> None:
        assert precision_score((2020, 4, None)) == 2

    def test_full(self) -> None:
        assert precision_score((2020, 4, 15)) == 3


class TestIsDateSubsetCompatible:
    """is_date_subset_compatible: subset hierarchy detection."""

    def test_year_vs_full_date_compatible(self) -> None:
        """Core requirement: "2020" vs "2020-04-15" must be subset_compatible."""
        assert is_date_subset_compatible("2020", "2020-04-15") is True

    def test_full_date_vs_year_compatible(self) -> None:
        assert is_date_subset_compatible("2020-04-15", "2020") is True

    def test_year_month_vs_full_date_compatible(self) -> None:
        assert is_date_subset_compatible("2020-04", "2020-04-15") is True

    def test_identical_full_dates_compatible(self) -> None:
        assert is_date_subset_compatible("2020-04-15", "2020-04-15") is True

    def test_different_years_incompatible(self) -> None:
        assert is_date_subset_compatible("2020", "2021") is False

    def test_different_full_dates_incompatible(self) -> None:
        assert is_date_subset_compatible("2020-04-15", "2020-04-16") is False

    def test_different_month_incompatible(self) -> None:
        assert is_date_subset_compatible("2020-04", "2020-05-15") is False

    def test_slash_vs_iso_compatible(self) -> None:
        """2020/04/15 vs 2020-04-15 — same date, different separator."""
        assert is_date_subset_compatible("2020/04/15", "2020-04-15") is True

    def test_english_vs_iso_compatible(self) -> None:
        assert is_date_subset_compatible("April 15, 2020", "2020-04-15") is True

    def test_none_none_compatible(self) -> None:
        assert is_date_subset_compatible(None, None) is True

    def test_none_value_incompatible(self) -> None:
        assert is_date_subset_compatible(None, "2020-04-15") is False

    def test_both_unparseable_same_string(self) -> None:
        assert is_date_subset_compatible("garbage", "garbage") is True

    def test_both_unparseable_different_strings(self) -> None:
        assert is_date_subset_compatible("foo", "bar") is False


class TestPickMostPreciseDate:
    """pick_most_precise_date: select highest-precision date from list."""

    def test_prefers_full_date_over_year(self) -> None:
        result = pick_most_precise_date(["2020", "2020-04-15"])
        assert result == "2020-04-15"

    def test_prefers_year_month_over_year(self) -> None:
        result = pick_most_precise_date(["2020", "2020-04"])
        assert result == "2020-04-XX"

    def test_all_year_only(self) -> None:
        result = pick_most_precise_date(["2020", "2020"])
        assert result == "2020-XX-XX"

    def test_empty_list_returns_none(self) -> None:
        assert pick_most_precise_date([]) is None

    def test_all_none_returns_none(self) -> None:
        assert pick_most_precise_date([None, None]) is None


# ---------------------------------------------------------------------------
# classify_consensus_date unit tests
# ---------------------------------------------------------------------------


class TestClassifyConsensusDate:
    """classify_consensus_date: subset-compatible date grouping."""

    def test_year_only_vs_full_date_unanimous(self) -> None:
        """Core requirement: "2020" vs "2020-04-15" → unanimous + precise value."""
        result = classify_consensus_date({
            "anilist": "2020",
            "mal": "2020-04-15",
        })
        assert result.consensus_flag == "unanimous"
        assert result.majority_value == "2020-04-15"
        assert result.majority_count == 2

    def test_three_sources_year_vs_full_unanimous(self) -> None:
        """3 sources: 2 × "2020", 1 × "2020-04-15" → unanimous (most precise)."""
        result = classify_consensus_date({
            "anilist": "2020",
            "mal": "2020",
            "bgm": "2020-04-15",
        })
        assert result.consensus_flag == "unanimous"
        assert result.majority_value == "2020-04-15"
        assert result.outlier_sources == []

    def test_all_same_unanimous(self) -> None:
        result = classify_consensus_date({
            "anilist": "2020-04-15",
            "mal": "2020-04-15",
            "bgm": "2020-04-15",
        })
        assert result.consensus_flag == "unanimous"
        assert result.majority_value == "2020-04-15"

    def test_different_years_not_unanimous(self) -> None:
        result = classify_consensus_date({
            "anilist": "2020",
            "mal": "2021",
        })
        assert result.consensus_flag == "tie"

    def test_majority_over_outlier(self) -> None:
        """2020 compatible cluster (anilist+mal) vs 2021 (bgm) → unique_outlier."""
        result = classify_consensus_date({
            "anilist": "2020",
            "mal": "2020-04-15",
            "bgm": "2021",
        })
        assert result.consensus_flag == "unique_outlier"
        assert result.outlier_sources == ["bgm"]

    def test_slash_format_compatible(self) -> None:
        result = classify_consensus_date({
            "anilist": "2020/04/15",
            "mal": "2020-04-15",
        })
        assert result.consensus_flag == "unanimous"

    def test_english_format_compatible(self) -> None:
        result = classify_consensus_date({
            "anilist": "April 15, 2020",
            "mal": "2020-04-15",
        })
        assert result.consensus_flag == "unanimous"

    def test_empty_sources_unanimous(self) -> None:
        result = classify_consensus_date({})
        assert result.consensus_flag == "unanimous"
        assert result.majority_value is None

    def test_all_null_unanimous(self) -> None:
        result = classify_consensus_date({"anilist": None, "mal": None})
        assert result.consensus_flag == "unanimous"

    def test_null_excluded_from_count(self) -> None:
        result = classify_consensus_date({
            "anilist": "2020",
            "mal": "2020-04-15",
            "bgm": None,
        })
        assert result.consensus_flag == "unanimous"
        assert result.majority_count == 2

    def test_majority_value_is_most_precise(self) -> None:
        """When 3 sources all subset-compatible, pick the most precise."""
        result = classify_consensus_date({
            "anilist": "2020",
            "mal": "2020-04",
            "bgm": "2020-04-15",
        })
        assert result.consensus_flag == "unanimous"
        assert result.majority_value == "2020-04-15"


# ---------------------------------------------------------------------------
# Integration: date columns produce subset-compatible normalized consensus
# ---------------------------------------------------------------------------


@pytest.fixture()
def resolved_conn_with_dates() -> duckdb.DuckDBPyConnection:
    """Resolved DB with a 3-source anime entity for date testing."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE anime (
            canonical_id    VARCHAR PRIMARY KEY,
            source_ids_json VARCHAR NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?)",
        [
            (
                "resolved:anime:date_test",
                json.dumps(["anilist:d10", "mal:d10", "bgm:d10"]),
            ),
        ],
    )
    yield conn
    conn.close()


@pytest.fixture()
def silver_conn_with_dates() -> duckdb.DuckDBPyConnection:
    """Silver DB with start_date values: year-only vs full date."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE anime (
            id          VARCHAR PRIMARY KEY,
            title_ja    VARCHAR,
            title_en    VARCHAR,
            year        VARCHAR,
            start_date  VARCHAR,
            end_date    VARCHAR,
            episodes    VARCHAR,
            format      VARCHAR,
            duration    VARCHAR
        )
        """
    )
    # anilist and mal give year-only; bgm gives full date — all subset-compatible
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("anilist:d10", "テストアニメ", "Test Anime", "2020", "2020", None, "12", "TV", "24"),
            ("mal:d10",     "テストアニメ", "Test Anime", "2020", "2020", None, "12", "TV", "24"),
            ("bgm:d10",     "テストアニメ", "Test Anime", "2020", "2020-04-15", None, "12", "TV", "24"),
        ],
    )
    yield conn
    conn.close()


class TestDateColumnIntegration:
    """Integration tests: date column subset detection in collect_consensus."""

    def test_start_date_year_vs_full_normalized_unanimous(
        self,
        resolved_conn_with_dates: duckdb.DuckDBPyConnection,
        silver_conn_with_dates: duckdb.DuckDBPyConnection,
    ) -> None:
        """start_date: "2020" (×2) vs "2020-04-15" (×1) → normalized=unanimous."""
        records = collect_consensus(
            resolved_conn_with_dates, "anime", silver_conn_with_dates
        )
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:anime:date_test"
            and r["attribute"] == "start_date"
        )
        # Raw flag: 2 distinct values → not raw-unanimous
        assert rec["consensus_flag"] in {"unique_outlier", "majority", "unanimous"}
        # Normalized flag: all subset-compatible → unanimous
        assert rec["normalized_consensus_flag"] == "unanimous", (
            f"Expected normalized=unanimous, got {rec['normalized_consensus_flag']!r}; "
            f"values={rec['values_json']!r}"
        )

    def test_start_date_normalized_majority_value_is_most_precise(
        self,
        resolved_conn_with_dates: duckdb.DuckDBPyConnection,
        silver_conn_with_dates: duckdb.DuckDBPyConnection,
    ) -> None:
        """normalized_majority_value should be the ISO 8601 full date."""
        records = collect_consensus(
            resolved_conn_with_dates, "anime", silver_conn_with_dates
        )
        rec = next(
            r for r in records
            if r["canonical_id"] == "resolved:anime:date_test"
            and r["attribute"] == "start_date"
        )
        # The most precise available date is 2020-04-15
        assert rec["normalized_majority_value"] == "2020-04-15", (
            f"Expected '2020-04-15', got {rec['normalized_majority_value']!r}"
        )
