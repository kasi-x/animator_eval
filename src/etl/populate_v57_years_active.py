"""v57 years_active population: automatic career timeline from credits.

Computes years_active as "YYYY-YYYY" string from min/max credit_year per person.
Applies to SILVER layer after integrate_duckdb and credits are populated.

Represents the earliest and latest credit years observed, allowing gap detection
downstream (e.g., dormancy analysis, career friction detection).
"""
from __future__ import annotations

import duckdb
import structlog

logger = structlog.get_logger()


def populate_persons_years_active(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Populate persons.years_active from credit data.

    For each person with credits, computes earliest and latest credit year
    and stores as "YYYY-YYYY" (e.g., "1998-2023").

    Returns: {"persons_updated": N, "persons_populated": M}
      - persons_updated: rows WHERE years_active was NULL/empty and is now set
      - persons_populated: total distinct persons with derived years_active
    """
    # Step 1: Count before
    count_before = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE years_active IS NULL OR years_active = '[]'"
    ).fetchone()[0]

    # Step 2: Compute and update (only if currently NULL/empty)
    conn.execute("""
        WITH credit_years AS (
            SELECT
                person_id,
                MIN(CAST(credit_year AS INTEGER)) as first_year,
                MAX(CAST(credit_year AS INTEGER)) as last_year
            FROM credits
            WHERE credit_year IS NOT NULL
              AND CAST(credit_year AS INTEGER) > 0
            GROUP BY person_id
        )
        UPDATE persons
        SET years_active = (
            SELECT CONCAT(CAST(first_year AS VARCHAR), '-', CAST(last_year AS VARCHAR))
            FROM credit_years
            WHERE credit_years.person_id = persons.id
        )
        FROM credit_years
        WHERE persons.id = credit_years.person_id
          AND (persons.years_active IS NULL OR persons.years_active = '[]')
    """)

    conn.commit()

    # Step 3: Count after
    updated = count_before - (
        conn.execute(
            "SELECT COUNT(*) FROM persons WHERE years_active IS NULL OR years_active = '[]'"
        ).fetchone()[0]
    )
    populated = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE years_active IS NOT NULL AND years_active != '[]'"
    ).fetchone()[0]

    logger.info("populate_persons_years_active", updated=updated, populated=populated)

    return {"persons_updated": updated, "persons_populated": populated}


def main() -> None:
    import argparse

    from src.analysis.io.gold_writer import gold_connect

    parser = argparse.ArgumentParser(
        description="Populate persons.years_active from credit timeline"
    )
    parser.add_argument("--gold-path", default=None, help="Path to GOLD DuckDB")
    parser.parse_args()

    with gold_connect() as conn:
        result = populate_persons_years_active(conn)
        for key, val in result.items():
            print(f"  {key}: {val}")


if __name__ == "__main__":
    main()
