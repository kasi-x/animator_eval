"""v57 structural metadata population: studio country_of_origin majority vote.

Populates studios.country_of_origin via majority vote from related anime's
country_of_origin. Applies to SILVER layer after integrate_duckdb has run.

Runs in atomicity: read anime_studios + anime, compute majority per studio,
write to studios in a single transaction.
"""
from __future__ import annotations

import duckdb
import structlog

logger = structlog.get_logger()


def populate_studios_country_of_origin(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Populate studios.country_of_origin via majority vote from anime.

    For each studio, count its associated anime by country_of_origin and
    select the most frequent. Ties broken by alphabetical order (ensures determinism).

    Returns: {"studios_updated": N, "studios_populated": M}
      - studios_updated: rows WHERE country_of_origin was NULL/empty and is now set
      - studios_populated: total distinct studios with derived country_of_origin
    """
    # Step 1: Compute majority country per studio
    conn.execute("""
        CREATE TEMP TABLE studio_majority_country AS
        WITH studio_countries AS (
            SELECT
                ast.studio_id,
                a.country_of_origin,
                COUNT(*) as count
            FROM anime_studios ast
            JOIN anime a ON ast.anime_id = a.id
            WHERE a.country_of_origin IS NOT NULL
              AND a.country_of_origin != ''
            GROUP BY ast.studio_id, a.country_of_origin
        ),
        ranked AS (
            SELECT
                studio_id,
                country_of_origin,
                ROW_NUMBER() OVER (
                    PARTITION BY studio_id
                    ORDER BY count DESC, country_of_origin ASC
                ) as rn
            FROM studio_countries
        )
        SELECT studio_id, country_of_origin
        FROM ranked
        WHERE rn = 1
    """)

    # Step 2: Count before
    count_before = conn.execute(
        "SELECT COUNT(*) FROM studios WHERE country_of_origin IS NULL OR country_of_origin = ''"
    ).fetchone()[0]

    # Step 3: Update (only if currently NULL/empty)
    conn.execute("""
        UPDATE studios
        SET country_of_origin = smc.country_of_origin
        FROM studio_majority_country smc
        WHERE studios.id = smc.studio_id
          AND (studios.country_of_origin IS NULL OR studios.country_of_origin = '')
    """)

    conn.commit()

    # Step 4: Count after
    updated = count_before - (
        conn.execute(
            "SELECT COUNT(*) FROM studios WHERE country_of_origin IS NULL OR country_of_origin = ''"
        ).fetchone()[0]
    )
    populated = conn.execute(
        "SELECT COUNT(*) FROM studios WHERE country_of_origin IS NOT NULL AND country_of_origin != ''"
    ).fetchone()[0]

    logger.info("populate_studios_country_of_origin", updated=updated, populated=populated)

    return {"studios_updated": updated, "studios_populated": populated}


def main() -> None:
    import argparse
    from pathlib import Path

    from src.analysis.gold_writer import gold_connect

    parser = argparse.ArgumentParser(
        description="Populate studios.country_of_origin via majority vote from anime"
    )
    parser.add_argument("--gold-path", default=None, help="Path to GOLD DuckDB (default: env ANIMETOR_GOLD_DB_PATH)")
    args = parser.parse_args()

    with gold_connect() as conn:
        result = populate_studios_country_of_origin(conn)
        for key, val in result.items():
            print(f"  {key}: {val}")


if __name__ == "__main__":
    main()
