"""Cross-source ID mapping builder for the Resolved layer (Phase 2b).

Reads BRONZE parquets to extract integer ID links between sources:
  - anilist_int → mal_id_int  (from BRONZE anilist anime, field: mal_id)
  - keyframe_id → anilist_int (from BRONZE keyframe anime, field: anilist_id)

These mappings let resolve_anime.py group conformed rows that represent
the same real-world anime but arrive from different scrapers.

H3 compliance:
  - Only uses existing numeric foreign-key columns; no fuzzy / similarity logic.
  - This is structural identity linking (same anilist_id = same media entry),
    not probabilistic entity resolution.

H1 compliance:
  - No score / popularity values are read or written here.

canonical_id guarantees:
  - **Idempotent**: same input member set + format_suffix → same canonical_id.
  - **Collision-free**: different member sets always produce different canonical_id
    (cluster_key is derived from sorted member IDs, not title+year which can alias).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


def _load_anilist_to_mal_map(bronze_root: Path) -> dict[int, int]:
    """Return {anilist_int: mal_int} from BRONZE anilist anime parquets.

    Uses the latest snapshot (largest date= partition) per anilist ID.
    Only includes rows where both IDs can be cast to INTEGER.
    """
    try:
        import duckdb
    except ImportError:
        logger.warning("duckdb_not_available_cross_source_ids")
        return {}

    al_dir = bronze_root / "source=anilist" / "table=anime"
    if not al_dir.is_dir():
        logger.debug("anilist_anime_bronze_not_found", path=str(al_dir))
        return {}

    parquets = list(al_dir.glob("date=*/*.parquet"))
    if not parquets:
        logger.debug("anilist_anime_parquets_empty", path=str(al_dir))
        return {}

    gl = str(al_dir / "date=*" / "*.parquet")
    conn = duckdb.connect()
    try:
        conn.execute("SET memory_limit='4GB'")
        rows = conn.execute(f"""
            SELECT
                TRY_CAST(REPLACE(id, 'anilist:', '') AS INTEGER) AS anilist_int,
                TRY_CAST(mal_id AS INTEGER) AS mal_int
            FROM (
                SELECT id, mal_id,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS rn
                FROM read_parquet('{gl}', hive_partitioning=true, union_by_name=true)
                WHERE id IS NOT NULL
                  AND mal_id IS NOT NULL
                  AND id LIKE 'anilist:%'
            )
            WHERE rn = 1
              AND TRY_CAST(REPLACE(id, 'anilist:', '') AS INTEGER) IS NOT NULL
              AND TRY_CAST(mal_id AS INTEGER) IS NOT NULL
        """).fetchall()
    except Exception as exc:
        logger.warning("anilist_to_mal_map_failed", error=str(exc))
        return {}
    finally:
        conn.close()

    result = {r[0]: r[1] for r in rows if r[0] is not None and r[1] is not None}
    logger.info("anilist_to_mal_map_loaded", count=len(result))
    return result


def _load_keyframe_to_anilist_map(bronze_root: Path) -> dict[str, int]:
    """Return {keyframe_conformed_id: anilist_int} from BRONZE keyframe anime parquets.

    Args:
        bronze_root: Root of BRONZE parquet tree.

    Returns:
        Dict mapping keyframe conformed IDs (e.g. 'keyframe:p_96870') to anilist integer IDs.
    """
    try:
        import duckdb
    except ImportError:
        return {}

    kf_dir = bronze_root / "source=keyframe" / "table=anime"
    if not kf_dir.is_dir():
        logger.debug("keyframe_anime_bronze_not_found", path=str(kf_dir))
        return {}

    parquets = list(kf_dir.glob("date=*/*.parquet"))
    if not parquets:
        logger.debug("keyframe_anime_parquets_empty", path=str(kf_dir))
        return {}

    gl = str(kf_dir / "date=*" / "*.parquet")
    conn = duckdb.connect()
    try:
        conn.execute("SET memory_limit='4GB'")
        rows = conn.execute(f"""
            SELECT id,
                   TRY_CAST(anilist_id AS INTEGER) AS anilist_int
            FROM (
                SELECT id, anilist_id,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS rn
                FROM read_parquet('{gl}', hive_partitioning=true, union_by_name=true)
                WHERE id IS NOT NULL
                  AND anilist_id IS NOT NULL
            )
            WHERE rn = 1
              AND TRY_CAST(anilist_id AS INTEGER) IS NOT NULL
        """).fetchall()
    except Exception as exc:
        logger.warning("keyframe_to_anilist_map_failed", error=str(exc))
        return {}
    finally:
        conn.close()

    result = {r[0]: r[1] for r in rows if r[0] is not None and r[1] is not None}
    logger.info("keyframe_to_anilist_map_loaded", count=len(result))
    return result


# ---------------------------------------------------------------------------
# Union-Find for cluster building
# ---------------------------------------------------------------------------

def _compute_canonical_id(
    member_rows: list[dict[str, Any]],
    format_suffix: str | None,
) -> str:
    """Deterministic canonical_id from sorted member ids + optional format suffix.

    Idempotent: same input member set + format_suffix → same canonical_id.
    Collision-free: different member sets → different canonical_id.

    The key is built from sorted member IDs joined with ASCII unit-separator
    (\\x1f, which never appears in source IDs) rather than from title+year
    strings, which can alias across independent UF groups when year=None.

    Args:
        member_rows: All conformed rows belonging to this sub-cluster.
        format_suffix: Optional format string (e.g. "TV", "MOVIE") appended
            to the key only when a UF group is split into multiple sub-clusters
            by format.  Pass None when the group is not split.

    Returns:
        canonical_id string of the form "resolved:anime:<12-hex-digest>".

    Note — caveat:
        Adding or removing members from a cluster changes its canonical_id.
        All downstream credits / scores must be re-computed on each full rebuild.
        This is acceptable because the ETL is designed to be run to fixpoint on
        a fixed conformed snapshot (idempotent for fixed input is preserved).
    """
    import hashlib

    member_ids_sorted = sorted(r["id"] for r in member_rows)
    parts = list(member_ids_sorted)
    if format_suffix:
        parts.append(f"__fmt__{format_suffix}")
    key = "\x1f".join(parts)  # ASCII unit separator — absent from source IDs
    digest = hashlib.sha256(key.encode()).hexdigest()[:12]
    return f"resolved:anime:{digest}"


class _UnionFind:
    """Path-compressed Union-Find for anime cluster construction.

    Uses iterative path compression to avoid Python recursion depth limits
    when large clusters (e.g. 1,000+ madb M-rows per C-series) are formed.
    """

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        """Iterative find with two-pass path compression."""
        if x not in self._parent:
            self._parent[x] = x
            return x

        # Step 1: walk up to root
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
            if root not in self._parent:
                self._parent[root] = root
                break

        # Step 2: path compression — point all visited nodes directly to root
        current = x
        while self._parent[current] != root:
            nxt = self._parent[current]
            self._parent[current] = root
            current = nxt

        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra

    def groups(self) -> dict[str, list[str]]:
        """Return {root: [members]} for all non-trivial + trivial groups."""
        groups: dict[str, list[str]] = {}
        for x in self._parent:
            root = self.find(x)
            groups.setdefault(root, []).append(x)
        return groups


def build_cross_source_anime_clusters(
    conformed_rows: list[dict[str, Any]],
    bronze_root: Path | str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Group conformed anime rows into cross-source clusters.

    Clustering strategy (in priority order):

    1. **ID-based links** (when bronze_root provided):
       - anilist:X ↔ mal:Y via BRONZE anilist.mal_id
       - keyframe:X ↔ anilist:Y via BRONZE keyframe.anilist_id
       These are structural identity links — same numeric ID = same real-world entity.

    2. **Title+year fallback** for rows not linked by ID:
       - normalized(title_ja) + str(year) cluster key
       - Rows with empty title_ja each get their own cluster.

    Clusters from step 1 and step 2 are merged transitively: if two rows are
    joined by ID and share a title+year key with a third row, all three are merged.

    Args:
        conformed_rows: All rows from conformed.anime (dicts with 'id', 'title_ja', 'year').
        bronze_root: Root of BRONZE parquet tree. When None, only title+year clustering applies.

    Returns:
        Dict of {cluster_key: [conformed_row, ...]} where cluster_key is the
        canonical_id that will be assigned (or a deterministic surrogate key).
    """
    import unicodedata

    def _norm(t: str) -> str:
        if not t:
            return ""
        return unicodedata.normalize("NFKC", t).strip().lower()

    uf = _UnionFind()

    # Initialize every conformed row as its own node
    for row in conformed_rows:
        uf.find(row["id"])

    # madb M-row → parent C-row linking via schema:isPartOf.
    # M-rows carry episode-level data; their parent C-row represents the series.
    # If a C-row exists in conformed.anime (it was loaded separately), union M to C.
    # If the C-row does NOT appear in conformed rows (orphan M), the M-row forms its
    # own singleton cluster — self-contained and independently identifiable.
    #
    # Note: conformed loader excludes madb:C* from resolve_anime input (line 138-139),
    # so we map M-rows to a synthetic "madb:C{parent_id}" anchor node.  This anchor
    # may not exist as a real conformed row, but the UF still groups all M-rows sharing
    # the same parent C-id together — forming one cluster per series.
    madb_m_to_anchor: dict[str, str] = {}  # conformed_id → anchor_id
    for row in conformed_rows:
        rid = row["id"]
        if not rid.startswith("madb:M"):
            continue
        parent_id = row.get("parent_madb_id") or ""
        if parent_id.startswith("C"):
            anchor = f"madb:{parent_id}"
            madb_m_to_anchor[rid] = anchor
            uf.union(rid, anchor)

    if madb_m_to_anchor:
        logger.info(
            "madb_m_to_c_links",
            count=len(madb_m_to_anchor),
            unique_anchors=len(set(madb_m_to_anchor.values())),
        )

    if bronze_root is not None:
        bronze_root = Path(bronze_root)
        al_to_mal = _load_anilist_to_mal_map(bronze_root)
        kf_to_al = _load_keyframe_to_anilist_map(bronze_root)
    else:
        al_to_mal = {}
        kf_to_al = {}

    # Build index: mal_id_int → conformed id (for MAL rows)
    mal_id_to_row_id: dict[int, str] = {}
    # Build index: anilist numeric id → conformed id (for AniList rows)
    anilist_int_to_row_id: dict[int, str] = {}

    for row in conformed_rows:
        rid = row["id"]
        if rid.startswith("anilist:"):
            try:
                al_int = int(rid.replace("anilist:", ""))
                anilist_int_to_row_id[al_int] = rid
            except ValueError:
                pass
        elif rid.startswith("mal:"):
            mid = row.get("mal_id_int")
            if mid is not None:
                try:
                    mal_id_to_row_id[int(mid)] = rid
                except (ValueError, TypeError):
                    pass

    # Link 1: anilist:X ↔ mal:Y via BRONZE anilist.mal_id
    for al_int, mal_int in al_to_mal.items():
        al_row_id = anilist_int_to_row_id.get(al_int)
        mal_row_id = mal_id_to_row_id.get(mal_int)
        if al_row_id and mal_row_id:
            uf.union(al_row_id, mal_row_id)

    # Link 2: keyframe:X ↔ anilist:Y via BRONZE keyframe.anilist_id
    for kf_row_id, al_int in kf_to_al.items():
        al_row_id = anilist_int_to_row_id.get(al_int)
        if al_row_id and kf_row_id in uf._parent:
            uf.union(kf_row_id, al_row_id)

    logger.info(
        "cross_source_anime_id_links",
        anilist_to_mal=len(al_to_mal),
        keyframe_to_anilist=len(kf_to_al),
    )

    # Title+year secondary clustering:
    # Group rows that share the same (norm title_ja, year) key
    # and union them together (even across source prefixes)
    #
    # 除外 1 — year IS NULL: cluster 形成を抑止 (over-merge 防止)
    # madb の長寿シリーズ (サザエさん 等) や年不明 row が year=None を共有するため。
    # year 不明 row は ID-link 経由でしか cluster されない (singleton fallback)。
    #
    # 除外 2 — madb:M* (M-manifestation rows): 話数 / 個別放送回 は series title を
    # 共有するが本質的に別エンティティ (各話)。M-row は §madb_m_to_c_anchor ステップで
    # 親 C-series 経由の anchor cluster に吸収済み (parent_madb_id あり)。
    # parent_madb_id なし (orphan) の M-row も title+year 集約しない — 孤立 singleton
    # として保持し resolved.episodes に流す (情報保持原則)。
    title_year_index: dict[str, str] = {}  # key → first row_id seen
    for row in conformed_rows:
        rid = row["id"]
        # Skip all madb M-rows: episodes must not be clustered by title+year
        if rid.startswith("madb:M"):
            continue
        title_ja = (row.get("title_ja") or "").strip()
        if not title_ja:
            continue
        year = row.get("year")
        if year is None or year == "":
            continue  # year 不明 row は title fallback の対象外
        ty_key = f"{_norm(title_ja)}|{year}"
        if ty_key in title_year_index:
            uf.union(title_year_index[ty_key], row["id"])
        else:
            title_year_index[ty_key] = row["id"]

    # Collect groups
    raw_groups = uf.groups()

    # Build canonical_id for each group and return dict.
    # Sub-cluster split by `format` (TV/MOVIE/OVA/SPECIAL/PV/ONA/MUSIC):
    # 同 title+year で format 不一致 → 別作品 (本編 vs 特典 PV 等) として分離。
    # LLM 検証で 26 cluster が format split 候補として検出された (anime.format split=26)。
    # `format` 空 (None/'') の row は any-format サブクラスタに同居 (情報不足の保守的扱い)。
    result: dict[str, list[dict[str, Any]]] = {}
    row_by_id = {r["id"]: r for r in conformed_rows}
    format_split_count = 0

    for root, members in raw_groups.items():
        member_rows = [row_by_id[m] for m in members if m in row_by_id]
        if not member_rows:
            continue

        # format 別 subcluster
        by_format: dict[str, list[dict[str, Any]]] = {}
        unknown_format: list[dict[str, Any]] = []
        for r in member_rows:
            f = (r.get("format") or "").strip().upper()
            if not f:
                unknown_format.append(r)
            else:
                by_format.setdefault(f, []).append(r)

        if not by_format:
            # 全 format 不明 → 1 cluster (元挙動)
            subclusters = [member_rows]
        elif len(by_format) == 1:
            # 1 format のみ → unknown_format をそこに合流
            (only_fmt, fmt_rows), = by_format.items()
            subclusters = [fmt_rows + unknown_format]
        else:
            # 複数 format 検出 → format ごとに subcluster、unknown_format は最大の subcluster に合流
            format_split_count += 1
            largest_fmt = max(by_format, key=lambda k: len(by_format[k]))
            by_format[largest_fmt] += unknown_format
            subclusters = list(by_format.values())

        for sc_idx, sc_rows in enumerate(subclusters):
            # Derive format_suffix only when the UF group was split into
            # multiple sub-clusters by format; single-subcluster groups need
            # no suffix because member IDs alone are sufficient for uniqueness.
            if len(subclusters) >= 2:
                rep = sc_rows[0]
                fmt_suffix: str | None = (rep.get("format") or "").strip().upper() or None
            else:
                fmt_suffix = None

            canonical_id = _compute_canonical_id(sc_rows, format_suffix=fmt_suffix)
            result[canonical_id] = sc_rows

    logger.info(
        "cross_source_anime_clusters_built",
        total_conformed=len(conformed_rows),
        total_clusters=len(result),
        format_splits=format_split_count,
    )
    return result
