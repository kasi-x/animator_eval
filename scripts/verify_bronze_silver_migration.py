"""SILVER DuckDB 行数確認 (migration 後の health check)."""
from pathlib import Path
import duckdb

DEFAULT_SILVER = Path("result/silver.duckdb")


def main() -> None:
    if not DEFAULT_SILVER.exists():
        print("SILVER not found:", DEFAULT_SILVER)
        raise SystemExit(1)
    conn = duckdb.connect(str(DEFAULT_SILVER), read_only=True)
    tables = ["anime", "credits", "persons", "studios", "anime_studios"]
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"{t}: {n:,}")
        except duckdb.CatalogException:
            print(f"{t}: (missing)")
    # source 分布
    try:
        src_counts = conn.execute(
            "SELECT evidence_source, COUNT(*) FROM credits GROUP BY evidence_source ORDER BY 2 DESC"
        ).fetchall()
        print("\ncredits by evidence_source:")
        for s, c in src_counts:
            print(f"  {s}: {c:,}")
    except duckdb.CatalogException:
        pass

    # anime by source (from Bronze parquet)
    try:
        anime_src = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE source_mat IS NOT NULL"
        ).fetchone()[0]
        print(f"\nanime with source_mat: {anime_src:,}")
    except Exception:
        pass

    conn.close()


if __name__ == "__main__":
    main()
