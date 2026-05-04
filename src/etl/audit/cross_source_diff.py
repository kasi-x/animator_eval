"""Cross-source value diff aggregation for resolved entities.

Compares attribute values from different source systems (anilist / mal / ann /
bgm / madb / seesaawiki / keyframe) for entities that share the same
canonical_id in resolved.duckdb.

For each pair of conformed IDs belonging to the same canonical entity, all
differing attribute values are collected, classified, and exported as CSV.

Classification categories (in priority order):
    null_in_one             - one value is NULL/empty, the other is not
    identical_after_normalize - values differ in raw form but match after
                               NFKC + 旧字体→新字体 + lowercase + punct-strip
    digit_count_mismatch    - numeric fields whose string lengths differ (e.g.
                              year "2020" vs "20")
    off_by_year             - year fields that differ by exactly 1
    single_char_diff        - Levenshtein distance == 1, length > 3
    multi_char_diff         - Levenshtein > 1 and relative change ≤ 30%
                              (i.e., not completely different)
    completely_different    - all other differences

Public API:
    collect_diffs(conn, entity, silver_conn) -> list[dict]
    classify_diff(value_a, value_b, attribute)  -> str
    export_audit(resolved_path, silver_path, output_dir) -> dict[str, int]

Design rules:
    - Read-only queries only (H3 / resolved.duckdb and silver.duckdb immutable).
    - No anime.score / display_* columns are read (H1).
    - All writes go to CSV in result/audit/cross_source_diff/ only.
"""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from itertools import combinations
from pathlib import Path
from typing import Literal

import duckdb
import structlog

from src.etl.normalize.canonical_name import KYU_SHIN_MAP

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Typing
# ---------------------------------------------------------------------------

Entity = Literal["anime", "persons", "studios"]

# Attributes per entity that are compared across sources.
# Only structural / factual fields — no display_* or score columns (H1).
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

# Year-type attribute names — triggers digit_count_mismatch / off_by_year logic.
_YEAR_ATTRS: frozenset[str] = frozenset({"year", "start_date", "end_date", "birth_date", "death_date"})

# Punctuation strip regex (applied during normalize step).
_PUNCT_RE = re.compile(r"[・．。、，,.\-\s　]+")


def _normalize_value(value: str) -> str:
    """Return NFKC + 旧字体→新字体 + lowercase + punct-strip normalized string.

    Used for identical_after_normalize classification.
    """
    # 1. NFKC
    s = unicodedata.normalize("NFKC", value)
    # 2. 旧字体→新字体
    s = "".join(KYU_SHIN_MAP.get(ch, ch) for ch in s)
    # 3. Lowercase
    s = s.lower()
    # 4. Strip punctuation / whitespace
    s = _PUNCT_RE.sub("", s)
    return s


# ---------------------------------------------------------------------------
# Levenshtein distance (pure Python, no external dep required)
# ---------------------------------------------------------------------------

def _levenshtein(a: str, b: str) -> int:
    """Return Levenshtein edit distance between two strings."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    # Use two-row DP to keep memory O(min(|a|, |b|))
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(
                min(
                    prev[j] + 1,          # deletion
                    curr[j - 1] + 1,      # insertion
                    prev[j - 1] + (ca != cb),  # substitution
                )
            )
        prev = curr
    return prev[-1]


# ---------------------------------------------------------------------------
# Public: classify_diff
# ---------------------------------------------------------------------------

def classify_diff(
    value_a: str | None,
    value_b: str | None,
    attribute: str,
) -> str:
    """Classify the difference between two attribute values.

    Args:
        value_a: First source value (may be None or empty string).
        value_b: Second source value (may be None or empty string).
        attribute: Name of the attribute being compared (e.g. "year", "name_ja").

    Returns:
        One of the classification strings:
        - "null_in_one"
        - "identical_after_normalize"
        - "digit_count_mismatch"
        - "off_by_year"
        - "single_char_diff"
        - "multi_char_diff"
        - "completely_different"
    """
    # Treat empty string as NULL
    a = value_a if value_a else None
    b = value_b if value_b else None

    if a is None or b is None:
        return "null_in_one"

    # Normalize both values
    na = _normalize_value(a)
    nb = _normalize_value(b)

    if na == nb:
        return "identical_after_normalize"

    # Year-type fields: check digit_count_mismatch and off_by_year
    if attribute in _YEAR_ATTRS:
        # Extract leading 4-digit year if present
        ya_match = re.search(r"\d+", a)
        yb_match = re.search(r"\d+", b)
        if ya_match and yb_match:
            ya_str, yb_str = ya_match.group(), yb_match.group()
            if len(ya_str) != len(yb_str):
                return "digit_count_mismatch"
            try:
                ya_int, yb_int = int(ya_str), int(yb_str)
                if abs(ya_int - yb_int) == 1:
                    return "off_by_year"
            except ValueError:
                pass

    # Levenshtein-based classification
    dist = _levenshtein(na, nb)
    max_len = max(len(na), len(nb))

    if dist == 1 and max_len > 3:
        return "single_char_diff"

    if max_len > 0 and (dist / max_len) <= 0.30:
        return "multi_char_diff"

    return "completely_different"


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

    # Build a safe IN clause with positional params
    placeholders = ", ".join("?" for _ in conformed_ids)
    select_cols = ", ".join(["id"] + attrs)
    sql = f"SELECT {select_cols} FROM {entity} WHERE id IN ({placeholders})"

    try:
        rows = silver_conn.execute(sql, conformed_ids).fetchall()
    except Exception as exc:
        logger.warning(
            "cross_source_diff.load_silver_failed",
            entity=entity,
            error=str(exc),
        )
        return {}

    result: dict[str, dict[str, str | None]] = {}
    col_names = ["id"] + attrs
    for row in rows:
        row_dict = dict(zip(col_names, row))
        sid = row_dict.pop("id")
        # Convert all values to str or None
        result[sid] = {k: (str(v) if v is not None else None) for k, v in row_dict.items()}
    return result


# ---------------------------------------------------------------------------
# Public: collect_diffs
# ---------------------------------------------------------------------------

def collect_diffs(
    resolved_conn: duckdb.DuckDBPyConnection,
    entity: Entity,
    silver_conn: duckdb.DuckDBPyConnection,
    *,
    batch_size: int = 2000,
) -> list[dict[str, str | None]]:
    """Collect cross-source value differences for all multi-source entities.

    For every canonical entity that has source_ids_json with >= 2 entries,
    fetches the attribute values from each contributing conformed source row
    in silver.duckdb, and yields one diff record for every (attribute, source_a,
    source_b) pair where the values differ.

    Args:
        resolved_conn: Open read-only connection to resolved.duckdb.
        entity: One of "anime", "persons", "studios".
        silver_conn: Open read-only connection to silver.duckdb (animetor.duckdb).
        batch_size: Number of canonical IDs processed per silver query batch.

    Returns:
        List of dicts with keys:
            canonical_id, attribute, source_a, value_a, source_b, value_b,
            classification
    """
    attrs = _ENTITY_ATTRS[entity]

    # Fetch all multi-source canonical IDs
    sql = f"SELECT canonical_id, source_ids_json FROM {entity} WHERE json_array_length(source_ids_json) >= 2"
    try:
        canonical_rows = resolved_conn.execute(sql).fetchall()
    except Exception as exc:
        logger.warning("cross_source_diff.resolved_query_failed", entity=entity, error=str(exc))
        return []

    logger.info(
        "cross_source_diff.collect_start",
        entity=entity,
        canonical_count=len(canonical_rows),
    )

    diffs: list[dict[str, str | None]] = []

    # Process in batches for memory efficiency
    # Build: conformed_id → canonical_id mapping for the current batch
    for batch_start in range(0, len(canonical_rows), batch_size):
        batch = canonical_rows[batch_start : batch_start + batch_size]

        # Map: canonical_id → list[conformed_id]
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

        # Bulk load silver rows for this batch
        silver_rows = _load_silver_rows(silver_conn, entity, all_source_ids, attrs)

        # Build conformed_id → canonical_id reverse lookup
        source_to_canonical: dict[str, str] = {}
        for cid, sids in canonical_to_source_ids.items():
            for sid in sids:
                source_to_canonical[sid] = cid

        # For each canonical entity, compare all pairs of source rows
        for canonical_id, source_ids in canonical_to_source_ids.items():
            # Only keep source IDs that actually have silver rows
            available_ids = [sid for sid in source_ids if sid in silver_rows]
            if len(available_ids) < 2:
                continue

            for sid_a, sid_b in combinations(available_ids, 2):
                row_a = silver_rows[sid_a]
                row_b = silver_rows[sid_b]
                src_a = _source_prefix(sid_a)
                src_b = _source_prefix(sid_b)

                for attr in attrs:
                    val_a = row_a.get(attr)
                    val_b = row_b.get(attr)

                    # Skip pairs where both are None/empty (no diff to record)
                    if not val_a and not val_b:
                        continue
                    # Skip identical raw values
                    if val_a == val_b:
                        continue

                    classification = classify_diff(val_a, val_b, attr)

                    diffs.append(
                        {
                            "canonical_id": canonical_id,
                            "attribute": attr,
                            "source_a": src_a,
                            "conformed_id_a": sid_a,
                            "value_a": val_a,
                            "source_b": src_b,
                            "conformed_id_b": sid_b,
                            "value_b": val_b,
                            "classification": classification,
                        }
                    )

        logger.debug(
            "cross_source_diff.batch_done",
            entity=entity,
            batch_start=batch_start,
            batch_size=len(batch),
            diffs_so_far=len(diffs),
        )

    logger.info(
        "cross_source_diff.collect_done",
        entity=entity,
        diff_count=len(diffs),
    )
    return diffs


# ---------------------------------------------------------------------------
# Public: export_audit
# ---------------------------------------------------------------------------

_CSV_FIELDNAMES = [
    "canonical_id",
    "attribute",
    "source_a",
    "conformed_id_a",
    "value_a",
    "source_b",
    "conformed_id_b",
    "value_b",
    "classification",
]


def export_audit(
    resolved_path: Path | str,
    silver_path: Path | str,
    output_dir: Path | str,
) -> dict[str, int]:
    """Collect and export cross-source diffs for all entity types to CSV.

    Reads resolved.duckdb and silver.duckdb in read-only mode; writes CSVs to
    output_dir/{anime,persons,studios}.csv.

    Args:
        resolved_path: Path to resolved.duckdb.
        silver_path: Path to silver/conformed animetor.duckdb.
        output_dir: Directory where output CSVs are written.

    Returns:
        Dict mapping entity name to number of diff rows written, e.g.:
        {"anime": 12345, "persons": 4567, "studios": 0}
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
            diffs = collect_diffs(resolved_conn, entity, silver_conn)
            out_path = output_dir / f"{entity}.csv"
            _write_csv(out_path, diffs)
            counts[entity] = len(diffs)
            logger.info(
                "cross_source_diff.export_done",
                entity=entity,
                rows=len(diffs),
                path=str(out_path),
            )
    finally:
        resolved_conn.close()
        silver_conn.close()

    return counts


def _write_csv(path: Path, rows: list[dict[str, str | None]]) -> None:
    """Write diff rows to a CSV file.

    If rows is empty, writes an empty CSV with headers so downstream tooling
    can still detect the file exists.
    """
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _cli_main() -> None:
    """CLI: python -m src.etl.audit.cross_source_diff [--output-dir PATH]"""
    import argparse

    parser = argparse.ArgumentParser(description="Cross-source value diff audit")
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
        help="Output directory for CSV files",
    )
    args = parser.parse_args()

    counts = export_audit(args.resolved_db, args.silver_db, args.output_dir)
    for entity, count in counts.items():
        print(f"{entity}: {count:,} diffs → {args.output_dir / entity}.csv")


if __name__ == "__main__":
    _cli_main()
