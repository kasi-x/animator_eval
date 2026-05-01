"""Union-Find series clustering for SILVER anime table.

Reads ``anime_relations`` SEQUEL/PREQUEL/PARENT/SIDE_STORY/SUMMARY/
ALTERNATIVE/FULL_STORY edges and groups anime into connected components.
Each component is assigned a ``series_cluster_id`` equal to the
lexicographically smallest ``anime_id`` in that component.

Isolated anime (no qualifying relations) receive their own ``id`` as the
cluster ID, so the column is non-NULL for every row after ``backfill``.

Design constraints
------------------
- H1: no anime.score / display_* columns read or written.
- H3: entity_resolution.py is not touched.
- Idempotent: running ``backfill`` multiple times produces identical results.

Usage::

    import duckdb
    from src.etl.cluster.series_cluster import backfill

    conn = duckdb.connect("result/silver.duckdb")
    updated = backfill(conn)
    print(f"{updated} rows updated")

CLI::

    pixi run python -m src.etl.cluster.series_cluster backfill
"""

from __future__ import annotations

import sys
from collections import defaultdict
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Relation types that define a series chain (same IP / franchise)
# Matched case-insensitively against the normalised relation_type column.
# ---------------------------------------------------------------------------

_CHAIN_RELATION_KEYWORDS: frozenset[str] = frozenset(
    {
        "sequel",
        "prequel",
        "parent",
        "side story",
        "summary",
        "alternative",
        "full story",
    }
)


def _is_chain_relation(relation_type: str) -> bool:
    """Return True if *relation_type* belongs to the series-chain set.

    Normalises to lower-case and checks whether any keyword in
    ``_CHAIN_RELATION_KEYWORDS`` is a substring of the normalised value.
    This handles variants such as "Sequel", "sequel of", "Parent Story",
    "Alternative Version", "Alternative Setting", "Full Story" etc.

    Args:
        relation_type: raw value from anime_relations.relation_type column.

    Returns:
        True if the relation should merge the two anime into one cluster.
    """
    normalised = relation_type.lower().strip()
    return any(kw in normalised for kw in _CHAIN_RELATION_KEYWORDS)


# ---------------------------------------------------------------------------
# Union-Find (path-compressed, non-recursive)
# ---------------------------------------------------------------------------


class _UnionFind:
    """Path-compressed Union-Find over string keys.

    All keys are lazily initialised as their own root on first access.
    """

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        """Return the root of x's component with path compression."""
        while self.parent.get(x, x) != x:
            # Path-halving compression
            self.parent[x] = self.parent.get(self.parent[x], self.parent[x])
            x = self.parent[x]
        return x

    def union(self, x: str, y: str) -> None:
        """Merge the components of x and y."""
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            # Union by lex order: smaller id becomes root so cluster_id is stable
            if rx < ry:
                self.parent[ry] = rx
            else:
                self.parent[rx] = ry

    def ensure(self, x: str) -> None:
        """Ensure x is registered (makes itself root if not present)."""
        if x not in self.parent:
            self.parent[x] = x


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------


def compute_clusters(conn: Any) -> dict[str, str]:
    """Compute series_cluster_id for every anime.

    Reads all rows from ``anime`` (for ids) and qualifying rows from
    ``anime_relations``.  Returns a mapping ``{anime_id: cluster_id}``
    where ``cluster_id`` is the lex-min id in the connected component.

    Args:
        conn: DuckDB connection (read access to SILVER anime / anime_relations).

    Returns:
        Dict mapping each anime_id to its cluster_id.  Always contains an
        entry for every id present in the ``anime`` table.
    """
    uf = _UnionFind()

    # Register every anime id so isolated nodes get their own cluster.
    try:
        anime_ids_rows = conn.execute("SELECT id FROM anime").fetchall()
    except Exception as exc:
        log.error("compute_clusters_anime_query_failed", error=str(exc))
        return {}

    for (aid,) in anime_ids_rows:
        uf.ensure(str(aid))

    # Process qualifying relations.
    try:
        relation_rows = conn.execute(
            "SELECT anime_id, related_anime_id, relation_type FROM anime_relations"
        ).fetchall()
    except Exception as exc:
        log.error("compute_clusters_relations_query_failed", error=str(exc))
        # Fall back: return each anime as its own cluster
        return {str(aid): str(aid) for (aid,) in anime_ids_rows}

    merged_count = 0
    for anime_id, related_id, relation_type in relation_rows:
        aid = str(anime_id)
        rid = str(related_id)
        rtype = str(relation_type) if relation_type is not None else ""

        if not _is_chain_relation(rtype):
            continue

        # Only merge if both ends are known anime (guard against dangling refs)
        if aid in uf.parent and rid in uf.parent:
            uf.union(aid, rid)
            merged_count += 1

    log.info(
        "compute_clusters_complete",
        total_anime=len(uf.parent),
        qualifying_edges=merged_count,
    )

    # Build output: each id → lex-min root in its component
    # First pass: collect all members per root
    components: dict[str, list[str]] = defaultdict(list)
    for aid in uf.parent:
        root = uf.find(aid)
        components[root].append(aid)

    # Second pass: assign lex-min id as cluster_id
    result: dict[str, str] = {}
    for root, members in components.items():
        cluster_id = min(members)
        for mid in members:
            result[mid] = cluster_id

    return result


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

_ALTER_SQL = "ALTER TABLE anime ADD COLUMN IF NOT EXISTS series_cluster_id VARCHAR"
_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_anime_series_cluster"
    " ON anime(series_cluster_id)"
)


def _ensure_column(conn: Any) -> None:
    """Add series_cluster_id column and index if absent."""
    conn.execute(_ALTER_SQL)
    conn.execute(_INDEX_SQL)


def backfill(conn: Any) -> int:
    """Write series_cluster_id to every row in the SILVER anime table.

    Idempotent: re-running produces the same result.  Uses PyArrow bulk-load
    + single UPDATE to avoid slow per-row round-trips (562K rows in < 1s).

    Args:
        conn: DuckDB connection with write access.

    Returns:
        Number of rows updated (equal to total anime rows on first run).
    """
    _ensure_column(conn)

    clusters = compute_clusters(conn)
    if not clusters:
        log.warning("backfill_no_clusters_computed")
        return 0

    # Build PyArrow table from the mapping for O(n) bulk load
    try:
        import pyarrow as pa  # available in pixi env
    except ImportError:
        pa = None

    conn.execute("DROP TABLE IF EXISTS _tmp_series_cluster")

    if pa is not None:
        items = list(clusters.items())
        arrow_table = pa.table(  # noqa: F841  — referenced by name in DuckDB SQL below
            {
                "anime_id": [x[0] for x in items],
                "cluster_id": [x[1] for x in items],
            }
        )
        conn.execute(
            "CREATE TEMP TABLE _tmp_series_cluster AS SELECT * FROM arrow_table"
        )
    else:
        # Fallback: executemany in batches (slow but correct)
        conn.execute(
            "CREATE TEMP TABLE _tmp_series_cluster"
            " (anime_id VARCHAR, cluster_id VARCHAR)"
        )
        batch_size = 10_000
        items = list(clusters.items())
        for offset in range(0, len(items), batch_size):
            batch = items[offset : offset + batch_size]
            conn.executemany(
                "INSERT INTO _tmp_series_cluster VALUES (?, ?)", batch
            )

    # Bulk UPDATE via temp table join
    conn.execute(
        """
        UPDATE anime
        SET series_cluster_id = t.cluster_id
        FROM _tmp_series_cluster t
        WHERE anime.id = t.anime_id
        """
    )

    # Count updated rows (rows that now have a cluster_id)
    updated = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE series_cluster_id IS NOT NULL"
    ).fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS _tmp_series_cluster")

    log.info(
        "backfill_complete",
        updated_rows=updated,
        distinct_clusters=len(set(clusters.values())),
    )
    return updated


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _cli_backfill() -> None:
    """CLI: backfill series_cluster_id in silver.duckdb."""
    try:
        import duckdb
    except ImportError as exc:
        print(f"ERROR: duckdb not installed: {exc}", file=sys.stderr)
        sys.exit(1)

    import os

    db_path = os.environ.get(
        "SILVER_DB_PATH",
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "result", "silver.duckdb"),
    )
    db_path = os.path.normpath(db_path)

    print(f"Connecting to {db_path} ...")
    conn = duckdb.connect(db_path)
    updated = backfill(conn)
    conn.close()
    print(f"Done: {updated:,} rows have series_cluster_id set.")


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "backfill":
        _cli_backfill()
    else:
        print("Usage: python -m src.etl.cluster.series_cluster backfill", file=sys.stderr)
        sys.exit(1)
