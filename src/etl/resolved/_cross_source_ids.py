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

class _UnionFind:
    """Path-compressed Union-Find for anime cluster construction."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

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
    import hashlib
    import unicodedata

    def _norm(t: str) -> str:
        if not t:
            return ""
        return unicodedata.normalize("NFKC", t).strip().lower()

    uf = _UnionFind()

    # Initialize every conformed row as its own node
    for row in conformed_rows:
        uf.find(row["id"])

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
    # year IS NULL の場合は cluster 形成を抑止 (over-merge 防止):
    # madb の長寿シリーズ (サザエさん 1,919 件等) や年不明 row が year=None を共有
    # するため、title 一致だけで全部 1 cluster に潰される問題を緩和。
    # year 不明 row は ID-link 経由でしか cluster されない (singleton fallback)。
    title_year_index: dict[str, str] = {}  # key → first row_id seen
    for row in conformed_rows:
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
            rep = sc_rows[0]
            title_ja = (rep.get("title_ja") or "").strip()
            fmt_suffix = (rep.get("format") or "").strip().upper() or "UNK"
            if not title_ja:
                cluster_key = f"__nontitle__|{root}|{fmt_suffix}"
            else:
                cluster_key = f"{_norm(title_ja)}|{rep.get('year') or ''}|{fmt_suffix}"
            # subcluster 数 ≥2 のときのみ key に format を含める (canonical_id 安定性のため)
            if len(subclusters) == 1:
                cluster_key = (
                    f"__nontitle__|{root}" if not title_ja
                    else f"{_norm(title_ja)}|{rep.get('year') or ''}"
                )

            digest = hashlib.sha256(cluster_key.encode()).hexdigest()[:12]
            canonical_id = f"resolved:anime:{digest}"
            result[canonical_id] = sc_rows

    logger.info(
        "cross_source_anime_clusters_built",
        total_conformed=len(conformed_rows),
        total_clusters=len(result),
        format_splits=format_split_count,
    )
    return result
