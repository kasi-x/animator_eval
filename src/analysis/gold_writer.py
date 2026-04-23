"""DuckDB GOLD layer writer/reader (Phase B).

The GOLD layer stores pipeline output (person_scores, score_history, etc.)
in a DuckDB file alongside the SQLite source DB. Writes happen once per
pipeline run (no concurrent writer risk). Reads serve the API and report
generators with vectorized columnar access.

Usage (write):
    from src.analysis.gold_writer import GoldWriter
    with GoldWriter() as gw:
        gw.write_person_scores(score_rows)
        gw.write_score_history(history_rows)

Usage (read):
    from src.analysis.gold_writer import GoldReader
    rows = GoldReader().person_scores()
    row = GoldReader().person_scores_for(person_id)
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Any

import duckdb
import structlog

logger = structlog.get_logger()

# Can be overridden in tests via monkeypatch
DEFAULT_GOLD_DB_PATH: Path = Path(
    os.environ.get(
        "ANIMETOR_GOLD_DB_PATH",
        str(Path(__file__).resolve().parent.parent.parent / "result" / "gold.duckdb"),
    )
)

# DDL — GOLD tables in DuckDB (columnar; no sqlite_master, no WAL)
_DDL = """
CREATE TABLE IF NOT EXISTS person_scores (
    person_id           TEXT PRIMARY KEY,
    person_fe           DOUBLE,
    studio_fe_exposure  DOUBLE,
    birank              DOUBLE,
    patronage           DOUBLE,
    dormancy            DOUBLE,
    awcc                DOUBLE,
    iv_score            DOUBLE,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS score_history (
    person_id           TEXT NOT NULL,
    person_fe           DOUBLE,
    studio_fe_exposure  DOUBLE,
    birank              DOUBLE,
    patronage           DOUBLE,
    dormancy            DOUBLE,
    awcc                DOUBLE,
    iv_score            DOUBLE,
    year                INTEGER,
    quarter             INTEGER
);
"""


def _open(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    path = str(db_path or DEFAULT_GOLD_DB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(path)


# ---------------------------------------------------------------------------
# Writer — used by the pipeline (once per run)
# ---------------------------------------------------------------------------

class GoldWriter:
    """Context manager that writes GOLD tables to DuckDB.

    with GoldWriter(db_path) as gw:
        gw.write_person_scores(score_rows)
        gw.write_score_history(history_rows)
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> "GoldWriter":
        self._conn = _open(self._path)
        self._conn.execute(_DDL)
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def write_person_scores(
        self, rows: list[tuple[Any, ...]]
    ) -> int:
        """Upsert person_scores rows.

        Each row: (person_id, person_fe, studio_fe_exposure, birank,
                   patronage, dormancy, awcc, iv_score)
        Returns number of rows written.
        """
        if not rows:
            return 0
        assert self._conn is not None
        now = datetime.datetime.now(datetime.timezone.utc)
        stamped = [(*r, now) for r in rows]
        self._conn.executemany(
            """
            INSERT INTO person_scores
                (person_id, person_fe, studio_fe_exposure, birank,
                 patronage, dormancy, awcc, iv_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (person_id) DO UPDATE SET
                person_fe          = excluded.person_fe,
                studio_fe_exposure = excluded.studio_fe_exposure,
                birank             = excluded.birank,
                patronage          = excluded.patronage,
                dormancy           = excluded.dormancy,
                awcc               = excluded.awcc,
                iv_score           = excluded.iv_score,
                updated_at         = excluded.updated_at
            """,
            stamped,
        )
        logger.info("gold_person_scores_written", count=len(rows))
        return len(rows)

    def write_score_history(
        self, rows: list[tuple[Any, ...]]
    ) -> int:
        """Append score_history rows.

        Each row: (person_id, person_fe, studio_fe_exposure, birank,
                   patronage, dormancy, awcc, iv_score, year, quarter)
        Returns number of rows written.
        """
        if not rows:
            return 0
        assert self._conn is not None
        self._conn.executemany(
            """
            INSERT INTO score_history
                (person_id, person_fe, studio_fe_exposure, birank,
                 patronage, dormancy, awcc, iv_score, year, quarter)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info("gold_score_history_written", count=len(rows))
        return len(rows)


# ---------------------------------------------------------------------------
# Reader — used by API / report generators
# ---------------------------------------------------------------------------

class GoldReader:
    """Read GOLD tables from DuckDB.

    Stateless (opens/closes connection per call) so it is safe for
    concurrent API reads.
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._path = db_path

    def available(self) -> bool:
        """Return True if the GOLD DuckDB file exists and is readable."""
        path = Path(str(self._path or DEFAULT_GOLD_DB_PATH))
        return path.exists()

    def person_scores(self) -> list[dict[str, Any]]:
        """Return all rows from person_scores ordered by iv_score DESC."""
        if not self.available():
            return []
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM person_scores ORDER BY iv_score DESC NULLS LAST"
            )
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    def person_scores_for(self, person_id: str) -> dict[str, Any] | None:
        """Return one row for person_id, or None if not found."""
        if not self.available():
            return None
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM person_scores WHERE person_id = ?", (person_id,)
            )
            cols = [d[0] for d in rel.description]
            row = rel.fetchone()
            return dict(zip(cols, row)) if row else None
        finally:
            conn.close()

    def score_history_for(self, person_id: str) -> list[dict[str, Any]]:
        """Return score_history rows for a person, most recent first."""
        if not self.available():
            return []
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM score_history WHERE person_id = ?"
                " ORDER BY year DESC, quarter DESC",
                (person_id,),
            )
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    def top_n(self, n: int = 100) -> list[dict[str, Any]]:
        """Return top-n persons by iv_score."""
        if not self.available():
            return []
        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            rel = conn.execute(
                "SELECT * FROM person_scores ORDER BY iv_score DESC NULLS LAST LIMIT ?",
                (n,),
            )
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    def ranking_query(
        self,
        sqlite_path: Path | str,
        *,
        conditions: list[str] | None = None,
        params: list[Any] | None = None,
        sort: str = "iv_score",
        limit: int = 50,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Run the ranking query joining DuckDB person_scores with SQLite SILVER.

        Attaches the SQLite DB (persons, credits) read-only so the entire
        analytical query runs inside DuckDB's vectorized engine.

        Returns (total_count, rows).
        Raises on error — caller should fall back to SQLite.
        """
        conds = ["s.iv_score IS NOT NULL"] + (conditions or [])
        where = " AND ".join(conds)
        bind = list(params or [])

        conn = _open(self._path)
        try:
            conn.execute(_DDL)
            conn.execute(
                f"ATTACH '{sqlite_path}' AS sl (TYPE SQLITE, READ_ONLY TRUE)"
            )

            total = conn.execute(
                f"SELECT COUNT(DISTINCT s.person_id) FROM person_scores s WHERE {where}",
                bind,
            ).fetchone()[0]

            # DuckDB requires all non-aggregated SELECT columns in GROUP BY.
            # The correlated subquery for primary_role is replaced with a JOIN
            # on a pre-aggregated CTE to avoid O(N) subquery executions.
            sql = f"""
            WITH primary_roles AS (
                SELECT person_id,
                       arg_max(role, cnt) AS primary_role
                FROM (
                    SELECT person_id, role, COUNT(*) AS cnt
                    FROM sl.credits
                    GROUP BY person_id, role
                )
                GROUP BY person_id
            )
            SELECT
                s.person_id,
                p.name_ja,
                p.name_en,
                s.iv_score,
                s.birank,
                s.patronage,
                s.person_fe,
                s.awcc,
                s.dormancy,
                MIN(c.credit_year) AS first_year,
                MAX(c.credit_year) AS latest_year,
                pr.primary_role
            FROM person_scores s
            JOIN sl.persons p ON p.id = s.person_id
            LEFT JOIN sl.credits c ON c.person_id = s.person_id
            LEFT JOIN primary_roles pr ON pr.person_id = s.person_id
            WHERE {where}
            GROUP BY
                s.person_id, p.name_ja, p.name_en,
                s.iv_score, s.birank, s.patronage, s.person_fe,
                s.awcc, s.dormancy, pr.primary_role
            ORDER BY s.{sort} DESC NULLS LAST
            LIMIT {limit}
            """
            rel = conn.execute(sql, bind)
            cols = [d[0] for d in rel.description]
            rows = [dict(zip(cols, row)) for row in rel.fetchall()]
            return total, rows
        finally:
            conn.close()
