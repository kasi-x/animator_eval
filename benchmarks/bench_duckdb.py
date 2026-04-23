"""DuckDB Phase A PoC benchmark: SQLite vs DuckDB read performance.

Runs 3 query pairs (SQLite baseline vs DuckDB) against either the production
DB or a freshly generated synthetic DB, and reports timing + row counts.

Usage:
    pixi run bench-duckdb                # uses result/animetor.db if it exists
    pixi run bench-duckdb --synth        # always use synthetic data
    pixi run bench-duckdb --db path/to/db.db
    pixi run bench-duckdb --repeat 5     # warmup + 5 timed runs per query
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import structlog  # noqa: E402

from src.log import setup_logging  # noqa: E402

setup_logging()
logger = structlog.get_logger()

DEFAULT_PROD_DB = _ROOT / "result" / "animetor.db"
RESULTS_DIR = Path(__file__).resolve().parent / "results"


# ---------------------------------------------------------------------------
# Synthetic DB setup
# ---------------------------------------------------------------------------

def _build_synthetic_db(tmp_path: Path, n_persons: int = 500, n_anime: int = 200) -> Path:
    """Create a moderately sized synthetic SQLite DB for benchmarking."""
    import random
    from src.database import init_db, get_connection
    from src.database import DEFAULT_DB_PATH
    import src.database as db_mod

    db_path = tmp_path / "bench.db"
    db_mod.DEFAULT_DB_PATH = db_path

    conn = get_connection()
    init_db(conn)

    # Insert persons
    person_ids = [f"p{i}" for i in range(n_persons)]
    conn.executemany(
        "INSERT OR IGNORE INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)",
        [(pid, f"人物{i}", f"Person {i}") for i, pid in enumerate(person_ids)],
    )

    # Insert anime
    anime_ids = [f"a{i}" for i in range(n_anime)]
    conn.executemany(
        "INSERT OR IGNORE INTO anime (id, title_ja, title_en, year, episodes) VALUES (?, ?, ?, ?, ?)",
        [(aid, f"アニメ{i}", f"Anime {i}", 2000 + i % 25, random.randint(1, 26))
         for i, aid in enumerate(anime_ids)],
    )

    # Insert studios
    studio_ids = list(range(1, 21))
    conn.executemany(
        "INSERT OR IGNORE INTO studios (id, name) VALUES (?, ?)",
        [(sid, f"スタジオ{sid}") for sid in studio_ids],
    )

    # Insert anime_studios
    conn.executemany(
        "INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main) VALUES (?, ?, ?)",
        [(aid, random.choice(studio_ids), 1) for aid in anime_ids],
    )

    # Insert genres
    genres = ["Action", "Drama", "Fantasy", "Sci-Fi", "Slice of Life", "Romance"]
    genre_rows = []
    for aid in anime_ids:
        for g in random.sample(genres, k=random.randint(1, 3)):
            genre_rows.append((aid, g))
    conn.executemany(
        "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?, ?)",
        genre_rows,
    )

    # Insert credits (~15 credits per anime on average)
    roles = ["director", "key_animator", "animation_director", "episode_director",
             "character_designer", "storyboard", "chief_animation_director"]
    rng = random.Random(42)
    credit_rows = []
    for aid, anime_year in zip(anime_ids, [2000 + i % 25 for i in range(n_anime)]):
        staff_sample = rng.sample(person_ids, k=min(15, len(person_ids)))
        for pid in staff_sample:
            credit_rows.append((
                pid, aid, rng.choice(roles), "", None, "bench", anime_year
            ))
    conn.executemany(
        "INSERT OR IGNORE INTO credits (person_id, anime_id, role, raw_role, episode, evidence_source, credit_year) VALUES (?,?,?,?,?,?,?)",
        credit_rows,
    )

    conn.commit()
    conn.close()
    db_mod.DEFAULT_DB_PATH = DEFAULT_DB_PATH  # restore
    return db_path


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

def _timeit(fn, *, repeat: int = 3) -> tuple[float, int]:
    """Run fn() `repeat` times, return (best_seconds, row_count)."""
    row_count = 0
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - t0
        row_count = len(result)
        best = min(best, elapsed)
    return best, row_count


# ---------------------------------------------------------------------------
# Benchmark targets
# ---------------------------------------------------------------------------

def bench_credits(db_path: Path, repeat: int) -> dict:
    """Query 1: Full credits table scan."""
    import src.analysis.duckdb_io as ddb

    def sqlite_baseline():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM credits").fetchall()
        conn.close()
        return rows

    def duckdb_read():
        return ddb.load_credits_ddb(db_path)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "credits_full_scan",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


def bench_anime_joined(db_path: Path, repeat: int) -> dict:
    """Query 2: Anime with genres/tags/studios (5 SQLite queries → 1 DuckDB)."""
    import src.analysis.duckdb_io as ddb

    def sqlite_baseline():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        anime_rows = conn.execute("SELECT * FROM anime").fetchall()
        by_id = {r["id"]: dict(r) for r in anime_rows}
        for r in conn.execute("SELECT anime_id, genre_name FROM anime_genres").fetchall():
            by_id.get(r[0], {}).setdefault("genres", []).append(r[1])
        for r in conn.execute("SELECT anime_id, tag_name FROM anime_tags").fetchall():
            by_id.get(r[0], {}).setdefault("tags", []).append(r[1])
        for r in conn.execute(
            "SELECT ast.anime_id, s.name FROM anime_studios ast JOIN studios s ON s.id=ast.studio_id"
        ).fetchall():
            by_id.get(r[0], {}).setdefault("studios", []).append(r[1])
        conn.close()
        return list(by_id.values())

    def duckdb_read():
        return ddb.load_anime_joined_ddb(db_path)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "anime_joined",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


def bench_credit_agg(db_path: Path, repeat: int) -> dict:
    """Query 3: GROUP BY person_id / credit_year / role (AKM feed)."""
    import src.analysis.duckdb_io as ddb

    def sqlite_baseline():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT person_id, credit_year, role,
                   COUNT(DISTINCT anime_id) AS n_works, COUNT(*) AS n_credits
            FROM credits
            WHERE credit_year IS NOT NULL
            GROUP BY person_id, credit_year, role
            ORDER BY person_id, credit_year
        """).fetchall()
        conn.close()
        return rows

    def duckdb_read():
        return ddb.agg_credits_per_person_ddb(db_path)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "credit_agg_by_person_year_role",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB Phase A PoC benchmark")
    parser.add_argument("--db", type=Path, default=None, help="Path to SQLite DB")
    parser.add_argument("--synth", action="store_true", help="Use synthetic DB")
    parser.add_argument("--repeat", type=int, default=3, help="Timed runs per query")
    parser.add_argument("--save", action="store_true", help="Save results to benchmarks/results/")
    args = parser.parse_args()

    # Resolve DB path
    if args.synth or (args.db is None and not DEFAULT_PROD_DB.exists()):
        logger.info("using_synthetic_db")
        tmp = tempfile.mkdtemp()
        db_path = _build_synthetic_db(Path(tmp))
        logger.info("synthetic_db_built", path=str(db_path))
    else:
        db_path = args.db or DEFAULT_PROD_DB
        logger.info("using_db", path=str(db_path))

    print(f"\nDB: {db_path}")
    size_mb = db_path.stat().st_size / 1e6 if db_path.exists() else 0
    print(f"Size: {size_mb:.1f} MB\n")
    print(f"{'Query':<35} {'Rows':>8} {'SQLite (s)':>12} {'DuckDB (s)':>12} {'Speedup':>9} {'Match':>6}")
    print("-" * 90)

    results = []
    for bench_fn in [bench_credits, bench_anime_joined, bench_credit_agg]:
        r = bench_fn(db_path, repeat=args.repeat)
        results.append(r)
        speedup_str = f"{r['speedup']}×" if r["speedup"] else "N/A"
        match_str = "✓" if r["rows_match"] else "✗"
        print(
            f"{r['query']:<35} {r['rows']:>8,} {r['sqlite_s']:>12.4f} "
            f"{r['duckdb_s']:>12.4f} {speedup_str:>9} {match_str:>6}"
        )

    print()
    avg_speedup = [r["speedup"] for r in results if r["speedup"]]
    if avg_speedup:
        print(f"Average speedup: {sum(avg_speedup)/len(avg_speedup):.2f}×")

    if args.save:
        RESULTS_DIR.mkdir(exist_ok=True)
        out = RESULTS_DIR / "duckdb_poc.json"
        with open(out, "w") as f:
            json.dump({"db": str(db_path), "results": results}, f, indent=2)
        print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
