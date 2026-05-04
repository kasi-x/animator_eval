"""Cross-source consensus aggregation for resolved entities (N-source majority).

For each canonical entity with 3+ contributing sources, aggregates attribute
values across all sources to determine the majority / unanimous / outlier
consensus.

Unlike cross_source_diff.py (24/01) which does pairwise comparisons, this
module collapses N source values per attribute into a single consensus record.

Classification (consensus_flag):
    unanimous      - all sources agree (n_distinct_values == 1)
    majority       - one value has strict majority (> 50%)
    unique_outlier - majority holds AND exactly one source disagrees
    plurality      - one value leads but with ≤ 50% share, no tie
    tie            - two or more values share the top count

A parallel normalized_consensus_flag re-runs classification after applying
NFKC + 旧字体→新字体 + lowercase + punct-strip to each value.

Public API:
    collect_consensus(resolved_conn, entity, silver_conn) -> list[dict]
    classify_consensus(value_counts, n_sources) -> ConsensusResult
    export_consensus(resolved_path, silver_path, output_dir) -> dict[str, int]

Design rules:
    - Read-only queries only (H3 / resolved.duckdb and silver.duckdb immutable).
    - No anime.score / display_* columns are read (H1).
    - All writes go to CSV in result/audit/cross_source_diff/ only.
    - Requires >= 2 sources (works for 2+; unanimous/unique_outlier most useful
      at 3+, but the schema supports all cases).
"""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb
import structlog

from src.etl.normalize.canonical_name import KYU_SHIN_MAP
from src.etl.normalize.column_rules import normalize_for_consensus, to_broad_format
from src.etl.normalize.date_parser import (
    is_date_subset_compatible,
    pick_most_precise_date,
)

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Typing
# ---------------------------------------------------------------------------

Entity = Literal["anime", "persons", "studios"]

ConsensusFlag = Literal[
    "unanimous",
    "majority",
    "unique_outlier",
    "plurality",
    "tie",
]

# Mirror the attribute lists from cross_source_diff.py (single source of truth
# must live there; we reference those lists directly to avoid duplication).
_ANIME_ATTRS: list[str] = [
    "title_ja",
    "title_en",
    "year",
    "start_date",
    "end_date",
    "episodes",
    "format",
    "duration",
]

_PERSONS_ATTRS: list[str] = [
    "name_ja",
    "name_en",
    "birth_date",
    "gender",
]

_STUDIOS_ATTRS: list[str] = [
    "name",
    "country_of_origin",
]

_ENTITY_ATTRS: dict[Entity, list[str]] = {
    "anime": _ANIME_ATTRS,
    "persons": _PERSONS_ATTRS,
    "studios": _STUDIOS_ATTRS,
}

# Punctuation strip regex — same as in cross_source_diff.py.
_PUNCT_RE = re.compile(r"[・．。、，,.\-\s　]+")


# ---------------------------------------------------------------------------
# Date column registry
# ---------------------------------------------------------------------------

# Columns that use the date_iso8601_with_subset normalization rule.
# Subset-compatible detection runs as a second pass for these columns.
_DATE_COLUMNS: frozenset[str] = frozenset({
    "start_date",
    "end_date",
    "aired_from",
    "aired_to",
    "release_date",
    "first_air_date",
    "last_air_date",
    "birth_date",
    "death_date",
})


# ---------------------------------------------------------------------------
# Core result type (defined early so date functions can reference it)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConsensusResult:
    """Result of classifying N source values for one (canonical_id, attribute)."""

    majority_value: str | None
    majority_count: int
    majority_share: float
    consensus_flag: ConsensusFlag
    outlier_sources: list[str]
    outlier_values: list[str]


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _normalize_value(value: str) -> str:
    """Return NFKC + 旧字体→新字体 + lowercase + punct-strip normalized string."""
    s = unicodedata.normalize("NFKC", value)
    s = "".join(KYU_SHIN_MAP.get(ch, ch) for ch in s)
    s = s.lower()
    s = _PUNCT_RE.sub("", s)
    return s


# ---------------------------------------------------------------------------
# Date-specific consensus: subset-compatible grouping
# ---------------------------------------------------------------------------

def classify_consensus_date(
    source_value_map: dict[str, str | None],
) -> ConsensusResult:
    """Classify consensus for a date attribute using subset-compatible grouping.

    Two date values are treated as equivalent if they are subset-compatible
    (e.g. "2020" vs "2020-04-15" — year matches, extra precision is ok).

    When all sources are subset-compatible with each other the result is
    "unanimous", and the most precise ISO 8601 date is used as majority_value.

    When a strict majority (>50%) of sources are mutually subset-compatible,
    the result follows the same majority / unique_outlier / plurality / tie
    rules as classify_consensus, but the majority_value is the most precise
    date in the compatible group.

    Args:
        source_value_map: Mapping of source_name → raw date value.

    Returns:
        ConsensusResult with date-aware majority_value.
    """
    present: dict[str, str] = {
        src: val
        for src, val in source_value_map.items()
        if val is not None and val != ""
    }

    n_sources = len(present)

    if n_sources == 0:
        return ConsensusResult(
            majority_value=None,
            majority_count=0,
            majority_share=0.0,
            consensus_flag="unanimous",
            outlier_sources=[],
            outlier_values=[],
        )

    sources = list(present.keys())
    values = list(present.values())

    # Build subset-compatibility clusters using Union-Find.
    # Each source belongs to exactly one cluster; two sources are in the same
    # cluster iff their values are subset-compatible.
    parent: dict[str, str] = {s: s for s in sources}

    def _find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(a: str, b: str) -> None:
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(len(sources)):
        for j in range(i + 1, len(sources)):
            if is_date_subset_compatible(values[i], values[j]):
                _union(sources[i], sources[j])

    # Group sources by cluster root.
    clusters: dict[str, list[str]] = {}
    for src in sources:
        root = _find(src)
        clusters.setdefault(root, []).append(src)

    # Count cluster sizes and identify the largest.
    cluster_sizes = {root: len(members) for root, members in clusters.items()}
    ranked = sorted(cluster_sizes.items(), key=lambda kv: -kv[1])
    top_root, top_count = ranked[0]
    top_members = clusters[top_root]
    majority_share = top_count / n_sources

    # Pick the most precise date from the winning cluster (source-priority order).
    _SOURCE_PRIORITY = ["anilist", "mal", "bgm", "ann", "madb", "keyframe", "seesaawiki"]
    ordered_values = _priority_ordered_values(top_members, present, _SOURCE_PRIORITY)
    most_precise_iso = pick_most_precise_date(ordered_values)
    majority_value = most_precise_iso or (ordered_values[0] if ordered_values else None)

    # Identify minority (outlier) sources and values.
    minority_sources = [src for src in sources if _find(src) != top_root]
    minority_values = list({present[src] for src in minority_sources})

    # ── unanimous (only one cluster, all compatible) ────────────────────────
    if len(clusters) == 1:
        return ConsensusResult(
            majority_value=majority_value,
            majority_count=top_count,
            majority_share=1.0,
            consensus_flag="unanimous",
            outlier_sources=[],
            outlier_values=[],
        )

    second_count = ranked[1][1] if len(ranked) > 1 else 0

    # ── tie ──────────────────────────────────────────────────────────────────
    if top_count == second_count:
        return ConsensusResult(
            majority_value=majority_value,
            majority_count=top_count,
            majority_share=majority_share,
            consensus_flag="tie",
            outlier_sources=minority_sources,
            outlier_values=minority_values,
        )

    # ── strict majority (>50%) ────────────────────────────────────────────────
    if majority_share > 0.5:
        flag: ConsensusFlag = "unique_outlier" if len(minority_sources) == 1 else "majority"
        return ConsensusResult(
            majority_value=majority_value,
            majority_count=top_count,
            majority_share=majority_share,
            consensus_flag=flag,
            outlier_sources=minority_sources,
            outlier_values=minority_values,
        )

    # ── plurality ─────────────────────────────────────────────────────────────
    return ConsensusResult(
        majority_value=majority_value,
        majority_count=top_count,
        majority_share=majority_share,
        consensus_flag="plurality",
        outlier_sources=minority_sources,
        outlier_values=minority_values,
    )


def _priority_ordered_values(
    sources: list[str],
    present: dict[str, str],
    priority: list[str],
) -> list[str | None]:
    """Return values for given sources ordered by source priority list."""
    ordered: list[str | None] = []
    seen = set()
    for preferred in priority:
        for src in sources:
            if src == preferred and src not in seen:
                ordered.append(present[src])
                seen.add(src)
    # Append any remaining sources alphabetically
    for src in sorted(sources):
        if src not in seen:
            ordered.append(present[src])
            seen.add(src)
    return ordered


# ---------------------------------------------------------------------------
# Core classification
# ---------------------------------------------------------------------------

def _resolve_tie_by_source_priority(
    top_values: list[str],
    source_value_map: dict[str, str | None],
) -> str:
    """Pick a value from equally-tied candidates using source priority ranking.

    Priority order (descending): anilist > mal > bgm > ann > madb > keyframe >
    seesaawiki > other sources (alphabetical fallback).
    """
    _PRIORITY = ["anilist", "mal", "bgm", "ann", "madb", "keyframe", "seesaawiki"]

    top_set = set(top_values)
    # Walk source priority order; return first value that is a top candidate.
    for preferred_source in _PRIORITY:
        for src, val in source_value_map.items():
            if src == preferred_source and val in top_set:
                return val

    # Alphabetical fallback on sources
    for src in sorted(source_value_map.keys()):
        val = source_value_map[src]
        if val in top_set:
            return val  # type: ignore[return-value]

    return top_values[0]


def classify_consensus(
    source_value_map: dict[str, str | None],
) -> ConsensusResult:
    """Classify the consensus among N source values for a single attribute.

    Args:
        source_value_map: Mapping of source_name → value (None / empty means
                          "no data from this source"). NULL/empty sources are
                          excluded from the count, so n_sources reflects only
                          sources that actually provided a value.

    Returns:
        ConsensusResult with all derived fields.
    """
    # Drop NULL / empty values — a source that has no data for an attribute
    # should not influence the consensus count.
    present: dict[str, str] = {
        src: val
        for src, val in source_value_map.items()
        if val is not None and val != ""
    }

    n_sources = len(present)

    if n_sources == 0:
        return ConsensusResult(
            majority_value=None,
            majority_count=0,
            majority_share=0.0,
            consensus_flag="unanimous",  # vacuously — 0 sources, 0 distinct
            outlier_sources=[],
            outlier_values=[],
        )

    value_counts: Counter[str] = Counter(present.values())
    n_distinct = len(value_counts)

    # ── unanimous ────────────────────────────────────────────────────────────
    if n_distinct == 1:
        (only_value, only_count) = next(iter(value_counts.most_common(1)))
        return ConsensusResult(
            majority_value=only_value,
            majority_count=only_count,
            majority_share=1.0,
            consensus_flag="unanimous",
            outlier_sources=[],
            outlier_values=[],
        )

    # Rank by count descending, then by value string for determinism.
    ranked = value_counts.most_common()
    top_count = ranked[0][1]
    second_count = ranked[1][1] if len(ranked) > 1 else 0

    # Collect all values that share the top count.
    top_values = [v for v, c in ranked if c == top_count]
    majority_share = top_count / n_sources

    # Resolve tie-breaker for the "majority_value" field.
    if len(top_values) > 1:
        majority_value = _resolve_tie_by_source_priority(top_values, present)
    else:
        majority_value = top_values[0]

    # Identify minority (outlier) sources + values.
    minority_sources = [src for src, val in present.items() if val != majority_value]
    minority_values = list({present[src] for src in minority_sources})

    # ── tie ──────────────────────────────────────────────────────────────────
    if top_count == second_count:
        return ConsensusResult(
            majority_value=majority_value,
            majority_count=top_count,
            majority_share=majority_share,
            consensus_flag="tie",
            outlier_sources=minority_sources,
            outlier_values=minority_values,
        )

    # ── strict majority (>50%) ────────────────────────────────────────────────
    if majority_share > 0.5:
        # unique_outlier: majority AND exactly one source disagrees.
        if len(minority_sources) == 1:
            flag: ConsensusFlag = "unique_outlier"
        else:
            flag = "majority"

        return ConsensusResult(
            majority_value=majority_value,
            majority_count=top_count,
            majority_share=majority_share,
            consensus_flag=flag,
            outlier_sources=minority_sources,
            outlier_values=minority_values,
        )

    # ── plurality (≤50% but single leader) ────────────────────────────────────
    return ConsensusResult(
        majority_value=majority_value,
        majority_count=top_count,
        majority_share=majority_share,
        consensus_flag="plurality",
        outlier_sources=minority_sources,
        outlier_values=minority_values,
    )


# ---------------------------------------------------------------------------
# Internal: source prefix extraction
# ---------------------------------------------------------------------------

def _source_prefix(conformed_id: str) -> str:
    """Extract the source name from a conformed ID like 'anilist:123' → 'anilist'."""
    return conformed_id.split(":")[0]


# ---------------------------------------------------------------------------
# Internal: load silver rows for a set of conformed IDs
# ---------------------------------------------------------------------------

def _load_silver_rows(
    silver_conn: duckdb.DuckDBPyConnection,
    entity: Entity,
    conformed_ids: list[str],
    attrs: list[str],
) -> dict[str, dict[str, str | None]]:
    """Load attribute values from silver for a list of conformed IDs.

    Returns:
        Dict mapping conformed_id → {attr_name: str_value_or_none}
    """
    if not conformed_ids:
        return {}

    placeholders = ", ".join("?" for _ in conformed_ids)
    select_cols = ", ".join(["id"] + attrs)
    sql = f"SELECT {select_cols} FROM {entity} WHERE id IN ({placeholders})"

    try:
        rows = silver_conn.execute(sql, conformed_ids).fetchall()
    except Exception as exc:
        logger.warning(
            "cross_source_consensus.load_silver_failed",
            entity=entity,
            error=str(exc),
        )
        return {}

    col_names = ["id"] + attrs
    result: dict[str, dict[str, str | None]] = {}
    for row in rows:
        row_dict = dict(zip(col_names, row))
        sid = row_dict.pop("id")
        result[sid] = {k: (str(v) if v is not None else None) for k, v in row_dict.items()}
    return result


# ---------------------------------------------------------------------------
# Internal: build one consensus record
# ---------------------------------------------------------------------------

def _build_broad_format_consensus(
    source_value_map: dict[str, str | None],
) -> ConsensusResult:
    """Classify broad_format consensus by mapping each source value via to_broad_format().

    All superficially different labels that map to the same broad category
    (e.g. "OVA" and "OAV" both → "ova_special") are treated as identical.
    """
    broad_source_map: dict[str, str | None] = {
        src: to_broad_format(val)
        for src, val in source_value_map.items()
    }
    return classify_consensus(broad_source_map)


def _build_consensus_record(
    canonical_id: str,
    attribute: str,
    source_value_map: dict[str, str | None],
) -> dict[str, object]:
    """Build a single consensus CSV row from a (canonical_id, attribute) group.

    Computes both raw consensus and column-rule-normalized consensus flags.
    The normalized view applies column-specific normalization (alias maps,
    kyu→shin, ISO-3166, info_richest, date_iso8601_with_subset, etc.) before
    re-running classify_consensus.

    For date columns, the normalized view uses classify_consensus_date which
    applies subset-compatible grouping ("2020" ⊆ "2020-04-15" → unanimous).

    For the "format" attribute, also computes a broad_format consensus using
    the 8-category taxonomy (ova_special / tv / movie / ona / short / music /
    cm / other).  Fields broad_format_consensus_flag, broad_format_majority_value
    and format_taxonomy_diff are always present in the output; for non-format
    attributes they carry None / False placeholders.
    """
    is_date = attribute in _DATE_COLUMNS
    is_format = attribute == "format"

    # Raw classification always uses exact-match consensus.
    result = classify_consensus(source_value_map)

    # Column-rule-normalized view: apply per-attribute normalization rules.
    col_normalized_source_map: dict[str, str | None] = {
        src: normalize_for_consensus(attribute, val)
        for src, val in source_value_map.items()
    }

    # For date columns, use subset-compatible grouping on the normalized values.
    if is_date:
        norm_result = classify_consensus_date(col_normalized_source_map)
    else:
        norm_result = classify_consensus(col_normalized_source_map)

    # broad_format parallel classification (format attribute only).
    if is_format:
        broad_result = _build_broad_format_consensus(source_value_map)
        broad_format_consensus_flag: str | None = broad_result.consensus_flag
        broad_format_majority_value: str | None = broad_result.majority_value
        # format_taxonomy_diff: True when fine-level (normalized) flag differs from
        # broad-level flag, meaning sources disagree on format even after 8-category
        # normalization — these cases warrant LLM judgment (24/02 pipeline).
        format_taxonomy_diff: bool = broad_result.consensus_flag not in (
            "unanimous", "unique_outlier"
        )
    else:
        broad_format_consensus_flag = None
        broad_format_majority_value = None
        format_taxonomy_diff = False

    n_sources = sum(1 for v in source_value_map.values() if v is not None and v != "")
    n_distinct = len({v for v in source_value_map.values() if v is not None and v != ""})

    return {
        "canonical_id": canonical_id,
        "attribute": attribute,
        "n_sources": n_sources,
        "n_distinct_values": n_distinct,
        "values_json": json.dumps(
            {src: val for src, val in source_value_map.items() if val is not None and val != ""},
            ensure_ascii=False,
        ),
        "majority_value": result.majority_value,
        "majority_count": result.majority_count,
        "majority_share": round(result.majority_share, 4),
        "consensus_flag": result.consensus_flag,
        "outlier_sources": json.dumps(result.outlier_sources, ensure_ascii=False),
        "outlier_values": json.dumps(result.outlier_values, ensure_ascii=False),
        "normalized_consensus_flag": norm_result.consensus_flag,
        "normalized_majority_value": norm_result.majority_value,
        "broad_format_consensus_flag": broad_format_consensus_flag,
        "broad_format_majority_value": broad_format_majority_value,
        "format_taxonomy_diff": format_taxonomy_diff,
    }


# ---------------------------------------------------------------------------
# Public: collect_consensus
# ---------------------------------------------------------------------------

def collect_consensus(
    resolved_conn: duckdb.DuckDBPyConnection,
    entity: Entity,
    silver_conn: duckdb.DuckDBPyConnection,
    *,
    batch_size: int = 2000,
) -> list[dict[str, object]]:
    """Collect N-source consensus records for all multi-source canonical entities.

    For every canonical entity that has source_ids_json with >= 2 entries,
    fetches attribute values from each contributing conformed source row in
    silver.duckdb and produces one consensus row per (canonical_id, attribute).

    Args:
        resolved_conn: Open read-only connection to resolved.duckdb.
        entity: One of "anime", "persons", "studios".
        silver_conn: Open read-only connection to silver.duckdb.
        batch_size: Number of canonical IDs processed per silver query batch.

    Returns:
        List of dicts with keys matching _CSV_FIELDNAMES.
    """
    attrs = _ENTITY_ATTRS[entity]

    sql = (
        f"SELECT canonical_id, source_ids_json FROM {entity} "
        "WHERE json_array_length(source_ids_json) >= 2"
    )
    try:
        canonical_rows = resolved_conn.execute(sql).fetchall()
    except Exception as exc:
        logger.warning(
            "cross_source_consensus.resolved_query_failed",
            entity=entity,
            error=str(exc),
        )
        return []

    logger.info(
        "cross_source_consensus.collect_start",
        entity=entity,
        canonical_count=len(canonical_rows),
    )

    records: list[dict[str, object]] = []

    for batch_start in range(0, len(canonical_rows), batch_size):
        batch = canonical_rows[batch_start : batch_start + batch_size]

        canonical_to_source_ids: dict[str, list[str]] = {}
        all_source_ids: list[str] = []

        for canonical_id, source_ids_json in batch:
            try:
                source_ids: list[str] = json.loads(source_ids_json or "[]")
            except Exception:
                source_ids = []
            if len(source_ids) < 2:
                continue
            canonical_to_source_ids[canonical_id] = source_ids
            all_source_ids.extend(source_ids)

        if not all_source_ids:
            continue

        silver_rows = _load_silver_rows(silver_conn, entity, all_source_ids, attrs)

        for canonical_id, source_ids in canonical_to_source_ids.items():
            available_ids = [sid for sid in source_ids if sid in silver_rows]
            if len(available_ids) < 2:
                continue

            for attr in attrs:
                # Build source → value mapping (only for sources that have a row).
                source_value_map: dict[str, str | None] = {
                    _source_prefix(sid): silver_rows[sid].get(attr)
                    for sid in available_ids
                }

                # Skip if every source is NULL/empty — nothing to aggregate.
                if all(v is None or v == "" for v in source_value_map.values()):
                    continue

                records.append(
                    _build_consensus_record(canonical_id, attr, source_value_map)
                )

        logger.debug(
            "cross_source_consensus.batch_done",
            entity=entity,
            batch_start=batch_start,
            batch_size=len(batch),
            records_so_far=len(records),
        )

    logger.info(
        "cross_source_consensus.collect_done",
        entity=entity,
        record_count=len(records),
    )
    return records


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

_CSV_FIELDNAMES: list[str] = [
    "canonical_id",
    "attribute",
    "n_sources",
    "n_distinct_values",
    "values_json",
    "majority_value",
    "majority_count",
    "majority_share",
    "consensus_flag",
    "outlier_sources",
    "outlier_values",
    "normalized_consensus_flag",
    "normalized_majority_value",
    # format 3-layer taxonomy (24/05) — populated for attribute=="format" only;
    # None for all other attributes.
    "broad_format_consensus_flag",
    "broad_format_majority_value",
    "format_taxonomy_diff",
]


def _write_consensus_csv(path: Path, rows: list[dict[str, object]]) -> None:
    """Write consensus rows to a CSV file.

    Writes an empty CSV with headers if rows is empty so downstream tooling can
    still detect the file exists.
    """
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Public: export_consensus
# ---------------------------------------------------------------------------

def export_consensus(
    resolved_path: Path | str,
    silver_path: Path | str,
    output_dir: Path | str,
) -> dict[str, int]:
    """Collect and export cross-source consensus for all entity types to CSV.

    Reads resolved.duckdb and silver.duckdb in read-only mode; writes
    {anime,persons,studios}_consensus.csv to output_dir.

    Note: Output filenames use the *_consensus.csv suffix to avoid overwriting
    the pairwise diff CSVs produced by cross_source_diff.py (24/01).

    Args:
        resolved_path: Path to resolved.duckdb.
        silver_path: Path to silver/conformed animetor.duckdb.
        output_dir: Directory where output CSVs are written.

    Returns:
        Dict mapping entity name to number of consensus rows written.
        {"anime": 12345, "persons": 4567, "studios": 89}
    """
    resolved_path = Path(resolved_path)
    silver_path = Path(silver_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}

    resolved_conn = duckdb.connect(str(resolved_path), read_only=True)
    silver_conn = duckdb.connect(str(silver_path), read_only=True)
    try:
        resolved_conn.execute("SET memory_limit='4GB'")
        silver_conn.execute("SET memory_limit='4GB'")

        entities: list[Entity] = ["anime", "persons", "studios"]
        for entity in entities:
            records = collect_consensus(resolved_conn, entity, silver_conn)
            out_path = output_dir / f"{entity}_consensus.csv"
            _write_consensus_csv(out_path, records)
            counts[entity] = len(records)
            logger.info(
                "cross_source_consensus.export_done",
                entity=entity,
                rows=len(records),
                path=str(out_path),
            )
    finally:
        resolved_conn.close()
        silver_conn.close()

    return counts


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _cli_main() -> None:
    """CLI: python -m src.etl.audit.cross_source_consensus [--output-dir PATH]"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Cross-source N-source consensus aggregation"
    )
    parser.add_argument(
        "--resolved-db",
        type=Path,
        default=Path("result/resolved.duckdb"),
        help="Path to resolved.duckdb",
    )
    parser.add_argument(
        "--silver-db",
        type=Path,
        default=Path("result/silver.duckdb"),
        help="Path to silver/conformed animetor.duckdb",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("result/audit/cross_source_diff"),
        help="Output directory for consensus CSV files",
    )
    args = parser.parse_args()

    counts = export_consensus(args.resolved_db, args.silver_db, args.output_dir)
    for entity, count in counts.items():
        print(
            f"{entity}: {count:,} consensus rows → "
            f"{args.output_dir / entity}_consensus.csv"
        )


if __name__ == "__main__":
    _cli_main()
