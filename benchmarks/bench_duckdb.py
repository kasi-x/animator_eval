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

def _build_synthetic_db(
    tmp_path: Path,
    n_persons: int = 500,
    n_anime: int = 200,
    staff_per_anime: int = 15,
) -> Path:
    """Create a synthetic SQLite DB for benchmarking.

    Total credits ≈ n_anime × staff_per_anime. For aggregation-heavy Phase
    5/6 query patterns (self-joins on credits), use a larger shape — e.g.
    n_persons=5000, n_anime=3000, staff_per_anime=25 gives ~75k credits
    and ~1M collaborator pairs, which is where DuckDB's vectorized execution
    starts to dominate per-row SQLite overhead.
    """
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

    # Insert credits (~staff_per_anime credits per anime on average)
    roles = ["director", "key_animator", "animation_director", "episode_director",
             "character_designer", "storyboard", "chief_animation_director"]
    rng = random.Random(42)
    credit_rows = []
    for aid, anime_year in zip(anime_ids, [2000 + i % 25 for i in range(n_anime)]):
        staff_sample = rng.sample(person_ids, k=min(staff_per_anime, len(person_ids)))
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

def bench_credits(db_path: Path, repeat: int, duckdb_path: Path | None = None) -> dict:
    """Query 1: Full credits table scan."""
    import src.analysis.duckdb_io as ddb

    target = duckdb_path or db_path

    def sqlite_baseline():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM credits").fetchall()
        conn.close()
        return rows

    def duckdb_read():
        return ddb.load_credits_ddb(target)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "credits_full_scan",
        "phase": "1 (data_loading)",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


def bench_anime_joined(db_path: Path, repeat: int, duckdb_path: Path | None = None) -> dict:
    """Query 2: Anime with genres/tags/studios (5 SQLite queries → 1 DuckDB)."""
    import src.analysis.duckdb_io as ddb

    target = duckdb_path or db_path

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
        return ddb.load_anime_joined_ddb(target)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "anime_joined",
        "phase": "1 (data_loading)",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


def bench_credit_agg(db_path: Path, repeat: int, duckdb_path: Path | None = None) -> dict:
    """Query 3: GROUP BY person_id / credit_year / role (AKM feed)."""
    import src.analysis.duckdb_io as ddb

    target = duckdb_path or db_path

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
        return ddb.agg_credits_per_person_ddb(target)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "credit_agg_by_person_year_role",
        "phase": "5 (AKM feed)",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


def bench_collaborator_counts(
    db_path: Path, repeat: int, duckdb_path: Path | None = None
) -> dict:
    """Phase 6 network_density: self-join on credits to count collaborators.

    SQLite baseline mimics the current Python implementation (anime → set of
    persons → collaborator cross-product). This is the O(N²) step that
    dominates Phase 6 for dense graphs.
    """
    import src.analysis.duckdb_io as ddb

    target = duckdb_path or db_path

    def sqlite_baseline():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        # Mirror the Python loop in compute_network_density() — build
        # anime→{persons}, then cross-product per anime, keep unique set
        # per person. Done in Python because that is the current baseline.
        from collections import defaultdict

        anime_persons: dict[str, set[str]] = defaultdict(set)
        for row in conn.execute("SELECT person_id, anime_id FROM credits"):
            anime_persons[row["anime_id"]].add(row["person_id"])
        conn.close()

        collabs: dict[str, set[str]] = defaultdict(set)
        for persons in anime_persons.values():
            for pid in persons:
                collabs[pid].update(persons - {pid})
        return [
            {"person_id": pid, "n_collaborators": len(s)}
            for pid, s in collabs.items()
        ]

    def duckdb_read():
        return ddb.agg_collaborator_counts_ddb(target)

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)

    return {
        "query": "collaborator_counts_selfjoin",
        "phase": "6 (network_density)",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


def bench_patronage_summary(
    db_path: Path, repeat: int, duckdb_path: Path | None = None
) -> dict:
    """Phase 5 patronage_premium: aggregate Π_i = Σ_d log(1+N_id) per person.

    Computes the final per-person patronage score (not the huge intermediate
    pair table), so the measurement reflects what the pipeline actually
    consumes. This is the apples-to-apples DuckDB-vs-Python comparison for
    Phase 5: DuckDB does the aggregation in one pass; Python has to build the
    intermediate pair dict then fold it.
    """
    from math import log1p
    from collections import defaultdict

    import duckdb

    from src.analysis.duckdb_io import _DIRECTOR_ROLE_NAMES

    def sqlite_baseline():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        dir_set = set(_DIRECTOR_ROLE_NAMES)
        anime_dirs: dict[str, set[str]] = defaultdict(set)
        for row in conn.execute("SELECT person_id, anime_id, role FROM credits"):
            if row["role"] in dir_set:
                anime_dirs[row["anime_id"]].add(row["person_id"])
        pair_counts: dict[tuple[str, str], int] = defaultdict(int)
        for row in conn.execute("SELECT person_id, anime_id FROM credits"):
            for did in anime_dirs.get(row["anime_id"], ()):
                if did != row["person_id"]:
                    pair_counts[(row["person_id"], did)] += 1
        conn.close()
        # Fold pair counts into per-person log1p-sum (matches pipeline use)
        per_person: dict[str, float] = defaultdict(float)
        for (pid, _did), n in pair_counts.items():
            per_person[pid] += log1p(n)
        return [{"person_id": p, "patronage": v} for p, v in per_person.items()]

    target = duckdb_path or db_path
    placeholders = ",".join(["?"] * len(_DIRECTOR_ROLE_NAMES))
    sql = f"""
    WITH dirs AS (
        SELECT DISTINCT anime_id, person_id AS director_id
        FROM credits WHERE role IN ({placeholders})
    ),
    pair_counts AS (
        SELECT c.person_id, d.director_id,
               COUNT(DISTINCT c.anime_id) AS n_collabs
        FROM credits c JOIN dirs d ON d.anime_id = c.anime_id
        WHERE c.person_id <> d.director_id
        GROUP BY c.person_id, d.director_id
    )
    SELECT person_id, SUM(ln(1 + n_collabs)) AS patronage
    FROM pair_counts
    GROUP BY person_id
    """

    def duckdb_read():
        conn = duckdb.connect(str(target), read_only=True)
        try:
            conn.execute("SET memory_limit='4GB'")
            rel = conn.execute(sql, list(_DIRECTOR_ROLE_NAMES))
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]
        finally:
            conn.close()

    sq_t, sq_n = _timeit(sqlite_baseline, repeat=repeat)
    dk_t, dk_n = _timeit(duckdb_read, repeat=repeat)
    return {
        "query": "patronage_summary_per_person",
        "phase": "5 (patronage_premium)",
        "rows": sq_n,
        "sqlite_s": round(sq_t, 4),
        "duckdb_s": round(dk_t, 4),
        "speedup": round(sq_t / dk_t, 2) if dk_t > 0 else None,
        "rows_match": sq_n == dk_n,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_SYNTH_SIZES = {
    # name     → (n_persons, n_anime, staff_per_anime)
    # rows = n_anime × staff_per_anime
    "small":   (500,    200,   15),   #   3k rows
    "medium":  (2000,   1000,  20),   #  20k rows
    "large":   (5000,   3000,  25),   #  75k rows
    "prod":    (30000, 10000,  50),   # 500k rows — approximates real SILVER size
}


def _mirror_to_duckdb_native(sqlite_path: Path) -> Path:
    """Copy the SQLite bench DB into a native .duckdb file (ATTACH + CTAS).

    The Phase A target is silver.duckdb in native format. Reading SQLite
    through DuckDB's sqlite_scanner works, but each scan round-trips through
    SQLite's row API, so vectorization wins are muted. Copying once into a
    native DuckDB file matches how silver.duckdb will be populated by the
    Card 05 ETL and is the fairer apples-to-apples measurement.
    """
    import duckdb

    native_path = sqlite_path.with_suffix(".duckdb")
    if native_path.exists():
        native_path.unlink()
    conn = duckdb.connect(str(native_path))
    try:
        conn.execute(f"ATTACH '{sqlite_path}' AS sq (TYPE SQLITE, READ_ONLY)")
        for tbl in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'sq' AND table_type = 'BASE TABLE'"
        ).fetchall():
            name = tbl[0]
            try:
                conn.execute(f'CREATE TABLE "{name}" AS SELECT * FROM sq."{name}"')
            except Exception:
                # Skip tables that have incompatible types (e.g. BLOB).
                continue
        conn.execute("DETACH sq")
    finally:
        conn.close()
    return native_path


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB Phase A PoC benchmark")
    parser.add_argument("--db", type=Path, default=None, help="Path to SQLite DB")
    parser.add_argument("--synth", action="store_true", help="Use synthetic DB")
    parser.add_argument(
        "--size",
        choices=sorted(_SYNTH_SIZES),
        default="prod",
        help="Synthetic DB size — 'prod' approximates real SILVER load (500k rows)",
    )
    parser.add_argument(
        "--no-native",
        action="store_true",
        help="Read DuckDB side via sqlite_scanner instead of a native .duckdb "
             "mirror (shows the Card 05 cutover benefit vs. not).",
    )
    parser.add_argument("--repeat", type=int, default=3, help="Timed runs per query")
    parser.add_argument("--save", action="store_true", help="Save results to benchmarks/results/")
    parser.add_argument(
        "--phase-a-out",
        type=Path,
        default=_ROOT / "result" / "duckdb_phase_a_benchmark.json",
        help="Where to write the Phase A PoC output",
    )
    args = parser.parse_args()

    # Resolve DB path
    if args.synth or (args.db is None and not DEFAULT_PROD_DB.exists()):
        n_persons, n_anime, staff_per_anime = _SYNTH_SIZES[args.size]
        logger.info(
            "using_synthetic_db",
            size=args.size,
            n_persons=n_persons,
            n_anime=n_anime,
            staff_per_anime=staff_per_anime,
        )
        tmp = tempfile.mkdtemp()
        db_path = _build_synthetic_db(
            Path(tmp),
            n_persons=n_persons,
            n_anime=n_anime,
            staff_per_anime=staff_per_anime,
        )
        logger.info("synthetic_db_built", path=str(db_path))
    else:
        db_path = args.db or DEFAULT_PROD_DB
        logger.info("using_db", path=str(db_path))

    # Mirror to native DuckDB for a realistic Phase A read-path measurement.
    duckdb_native_path: Path | None = None
    if not args.no_native:
        try:
            duckdb_native_path = _mirror_to_duckdb_native(db_path)
            logger.info(
                "duckdb_native_mirror_built",
                path=str(duckdb_native_path),
                size_mb=round(duckdb_native_path.stat().st_size / 1e6, 2),
            )
        except Exception as e:
            logger.warning("duckdb_native_mirror_failed", error=str(e))
            duckdb_native_path = None

    print(f"\nDB: {db_path}")
    size_mb = db_path.stat().st_size / 1e6 if db_path.exists() else 0
    print(f"Size: {size_mb:.1f} MB")
    if duckdb_native_path is not None:
        dsz = duckdb_native_path.stat().st_size / 1e6
        print(f"DuckDB (native): {duckdb_native_path} ({dsz:.1f} MB)")
    else:
        print("DuckDB (native): disabled → sqlite_scanner path")
    print()
    print(
        f"{'Query':<38} {'Rows':>9} {'SQLite (s)':>12} {'DuckDB (s)':>12}"
        f" {'Speedup':>9} {'Match':>6}"
    )
    print("-" * 92)

    bench_fns = [
        bench_credits,
        bench_anime_joined,
        bench_credit_agg,
        bench_collaborator_counts,   # Phase 6 network_density
        bench_patronage_summary,     # Phase 5 patronage_premium (per-person)
    ]
    results = []
    for bench_fn in bench_fns:
        try:
            r = bench_fn(db_path, repeat=args.repeat, duckdb_path=duckdb_native_path)
        except Exception as e:
            # Graceful fallback: log and record the error, don't abort the run.
            logger.warning("bench_failed", query=bench_fn.__name__, error=str(e))
            r = {
                "query": bench_fn.__name__,
                "error": str(e),
                "sqlite_s": None,
                "duckdb_s": None,
                "speedup": None,
                "rows_match": False,
            }
        results.append(r)
        speedup_str = f"{r['speedup']}×" if r.get("speedup") else "N/A"
        match_str = "✓" if r.get("rows_match") else "✗"
        rows = r.get("rows", 0) or 0
        sq = r.get("sqlite_s") or 0
        dk = r.get("duckdb_s") or 0
        print(
            f"{r['query']:<38} {rows:>9,} {sq:>12.4f} {dk:>12.4f}"
            f" {speedup_str:>9} {match_str:>6}"
        )

    print()
    avg_speedup = [r["speedup"] for r in results if r.get("speedup")]
    if avg_speedup:
        print(f"Average speedup: {sum(avg_speedup)/len(avg_speedup):.2f}×")

    # Phase 5/6 subset speedup — the task's primary acceptance criterion.
    phase56 = [r for r in results if r.get("phase", "").startswith(("5", "6"))]
    phase56_sp = [r["speedup"] for r in phase56 if r.get("speedup")]
    if phase56_sp:
        print(f"Phase 5/6 average speedup: {sum(phase56_sp)/len(phase56_sp):.2f}×")

    # Phase A output (task spec): always write, regardless of --save
    phase_a_rows = [
        {
            "phase": r.get("phase", "unknown"),
            "query": r["query"],
            "original_seconds": r.get("sqlite_s"),
            "duckdb_seconds": r.get("duckdb_s"),
            "speedup_ratio": r.get("speedup"),
            "rows": r.get("rows"),
            "rows_match": r.get("rows_match", False),
        }
        for r in results
    ]
    args.phase_a_out.parent.mkdir(parents=True, exist_ok=True)
    with args.phase_a_out.open("w") as f:
        json.dump(
            {
                "db": str(db_path),
                "db_size_mb": round(size_mb, 2),
                "duckdb_native": str(duckdb_native_path) if duckdb_native_path else None,
                "repeat": args.repeat,
                "synth_size": args.size if args.synth else None,
                "results": phase_a_rows,
            },
            f,
            indent=2,
        )
    print(f"\nPhase A results → {args.phase_a_out}")

    if args.save:
        RESULTS_DIR.mkdir(exist_ok=True)
        out = RESULTS_DIR / "duckdb_poc.json"
        with open(out, "w") as f:
            json.dump({"db": str(db_path), "results": results}, f, indent=2)
        print(f"Saved legacy bench dump to {out}")


if __name__ == "__main__":
    main()
