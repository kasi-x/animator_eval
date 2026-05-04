"""LLM enrichment for cross-source diff CSV — pattern classification + best-guess value.

Reads the rule-classified CSV produced by cross_source_diff.py (24/01) and appends
five LLM columns:

    llm_patterns     JSON list of pattern flags
                     (typo / abbreviation / kanji_simplification /
                      romanization_variant / punctuation_normalization /
                      historical_alias / different_naming_convention /
                      year_off_by_one / digit_count_mismatch /
                      different_entity / ambiguous)
    llm_best_guess   "value_a" | "value_b" | "neither"
    llm_best_value   explicit string when best_guess == "neither" (else "")
    llm_confidence   0.0 – 1.0
    llm_rationale    brief explanation

Design rules:
    - LLM results are additive columns; rule_classification is never modified (H3).
    - No anime.score / display_* data enters the prompt (H1).
    - LLM judgment is advisory only — not used for automatic merges (H3).
    - Caching: identical (value_a, value_b, attribute) triples are queried once.
    - Batch processing: BATCH_SIZE rows per LLM call.
    - dry-run mode: build prompts and parse structure; never call LLM.
    - --limit / --sample options cap rows before LLM to control cost.
    - Checkpoint: partial results persisted to JSONL; resume skips done rows.

Public API:
    enrich_csv(input_path, output_path, *, sample, limit, batch_size, dry_run)
    -> dict[str, int]   ({"processed": N, "cached": N, "skipped": N})

CLI:
    python -m src.etl.audit.cross_source_diff_llm \
        --input result/audit/cross_source_diff/anime.csv \
        --output result/audit/cross_source_diff/anime_llm_classified.csv \
        --sample 100
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

import httpx
import structlog

from src.utils.config import (
    LLM_BASE_URL,
    LLM_MAX_TOKENS,
    LLM_MODEL_NAME,
    LLM_TEMPERATURE,
    LLM_TIMEOUT,
)

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Valid pattern labels the LLM may assign.
VALID_PATTERNS: frozenset[str] = frozenset(
    {
        "typo",
        "abbreviation",
        "kanji_simplification",
        "romanization_variant",
        "punctuation_normalization",
        "historical_alias",
        "different_naming_convention",
        "year_off_by_one",
        "digit_count_mismatch",
        "different_entity",
        "ambiguous",
    }
)

VALID_BEST_GUESS: frozenset[str] = frozenset({"value_a", "value_b", "neither"})

# Task key for checkpoint JSONL (one file per output CSV).
_CHECKPOINT_SUFFIX = ".llm_checkpoint.jsonl"

# Default batch size when not specified.
DEFAULT_BATCH_SIZE = 20

# ---------------------------------------------------------------------------
# LLM call (thin wrapper around Ollama's generate endpoint)
# ---------------------------------------------------------------------------


def _ollama_base() -> str:
    """Return the Ollama base URL without the /v1 suffix."""
    return LLM_BASE_URL.rstrip("/").removesuffix("/v1")


def check_llm_available() -> bool:
    """Return True when the Ollama endpoint is reachable."""
    try:
        r = httpx.get(f"{_ollama_base()}/api/tags", timeout=5.0)
        return r.status_code == 200
    except Exception:
        return False


def _call_llm(prompt: str) -> str:
    """Single synchronous Ollama generate call.

    Returns the response text or an empty string on any error.
    Uses think=False to suppress Qwen3 chain-of-thought preamble.
    """
    try:
        r = httpx.post(
            f"{_ollama_base()}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {
                    "temperature": LLM_TEMPERATURE,
                    "num_predict": LLM_MAX_TOKENS * 4,  # structured JSON can be verbose
                },
            },
            timeout=LLM_TIMEOUT * 3,  # diff classification may be slower than norm
        )
        r.raise_for_status()
        data = r.json()
        return data.get("response", "").strip() or data.get("thinking", "").strip()
    except Exception as exc:
        logger.warning("llm_diff_call_failed", error=str(exc))
        return ""


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_PROMPT_TEMPLATE = """\
You are a data quality auditor for an anime credit database.
Classify the discrepancy between two source values for the same entity.

Entity type : {entity_type}
Attribute   : {attribute}
Source A    : {source_a} = "{value_a}"
Source B    : {source_b} = "{value_b}"
Context     : canonical_id={canonical_id}

Choose from these pattern labels (pick all that apply):
  typo, abbreviation, kanji_simplification, romanization_variant,
  punctuation_normalization, historical_alias, different_naming_convention,
  year_off_by_one, digit_count_mismatch, different_entity, ambiguous

Output ONLY a JSON object — no prose, no markdown fences:
{{
  "patterns": ["<label>", ...],
  "best_guess": "value_a" | "value_b" | "neither",
  "best_value": "<explicit string if neither, else empty string>",
  "confidence": <0.0-1.0>,
  "rationale": "<one sentence>"
}}
"""


def build_prompt(
    row: dict[str, Any],
    entity_type: str,
) -> str:
    """Build a single-row LLM prompt from a diff CSV row dict."""
    return _PROMPT_TEMPLATE.format(
        entity_type=entity_type,
        attribute=row.get("attribute", ""),
        source_a=row.get("source_a", ""),
        value_a=row.get("value_a", ""),
        source_b=row.get("source_b", ""),
        value_b=row.get("value_b", ""),
        canonical_id=row.get("canonical_id", ""),
    )


# ---------------------------------------------------------------------------
# JSON extraction + validation
# ---------------------------------------------------------------------------


def _extract_json_object(text: str) -> dict | None:
    """Extract a JSON object from an LLM response (handles markdown fences)."""
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = re.sub(r"```", "", text)
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        obj = json.loads(match.group())
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def _validate_llm_result(raw: dict) -> dict[str, Any]:
    """Validate and normalise a single LLM result dict.

    Returns a dict with keys:
        llm_patterns, llm_best_guess, llm_best_value, llm_confidence, llm_rationale
    Falls back to safe defaults if fields are missing or invalid.
    """
    # patterns — filter to known labels only
    raw_patterns = raw.get("patterns", [])
    if not isinstance(raw_patterns, list):
        raw_patterns = []
    patterns = [p for p in raw_patterns if isinstance(p, str) and p in VALID_PATTERNS]
    if not patterns:
        patterns = ["ambiguous"]

    # best_guess
    best_guess = raw.get("best_guess", "neither")
    if best_guess not in VALID_BEST_GUESS:
        best_guess = "neither"

    # best_value
    best_value = ""
    if best_guess == "neither":
        bv = raw.get("best_value", "")
        best_value = str(bv).strip() if bv else ""

    # confidence
    try:
        conf = float(raw.get("confidence", 0.5))
        conf = max(0.0, min(1.0, conf))
    except (TypeError, ValueError):
        conf = 0.5

    # rationale — one sentence max
    rationale = str(raw.get("rationale", "")).strip()
    if len(rationale) > 300:
        rationale = rationale[:297] + "..."

    return {
        "llm_patterns": json.dumps(patterns, ensure_ascii=False),
        "llm_best_guess": best_guess,
        "llm_best_value": best_value,
        "llm_confidence": round(conf, 3),
        "llm_rationale": rationale,
    }


def _empty_llm_result(reason: str = "llm_unavailable") -> dict[str, Any]:
    """Return a zeroed-out LLM result for rows that could not be classified."""
    return {
        "llm_patterns": json.dumps(["ambiguous"]),
        "llm_best_guess": "neither",
        "llm_best_value": "",
        "llm_confidence": 0.0,
        "llm_rationale": reason,
    }


# ---------------------------------------------------------------------------
# Cache key
# ---------------------------------------------------------------------------


def _cache_key(row: dict[str, Any]) -> str:
    """Return a canonical cache key for a diff row.

    The key is (value_a, value_b, attribute) — identical discrepancies across
    different canonical_ids share one LLM query.
    """
    return json.dumps(
        [
            row.get("value_a", ""),
            row.get("value_b", ""),
            row.get("attribute", ""),
        ],
        ensure_ascii=False,
    )


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _checkpoint_path(output_path: Path) -> Path:
    """Return the JSONL checkpoint file path alongside the output CSV."""
    return output_path.with_suffix("").with_suffix(_CHECKPOINT_SUFFIX)


def _load_checkpoint(output_path: Path) -> dict[str, dict[str, Any]]:
    """Load previously completed results from the checkpoint JSONL.

    Returns:
        Dict mapping cache_key → validated LLM result dict.
    """
    cp = _checkpoint_path(output_path)
    results: dict[str, dict[str, Any]] = {}
    if not cp.exists():
        return results
    try:
        with open(cp, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                key = entry.get("cache_key", "")
                if key:
                    results[key] = entry["result"]
        logger.info("llm_diff.checkpoint_loaded", entries=len(results), path=str(cp))
    except Exception as exc:
        logger.warning("llm_diff.checkpoint_load_failed", error=str(exc))
    return results


def _append_checkpoint(
    output_path: Path,
    cache_key: str,
    result: dict[str, Any],
) -> None:
    """Append a single completed result to the checkpoint JSONL."""
    cp = _checkpoint_path(output_path)
    try:
        with open(cp, "a", encoding="utf-8") as fh:
            fh.write(
                json.dumps({"cache_key": cache_key, "result": result}, ensure_ascii=False)
                + "\n"
            )
    except Exception as exc:
        logger.warning("llm_diff.checkpoint_append_failed", error=str(exc))


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


def _classify_batch(
    rows: list[dict[str, Any]],
    entity_type: str,
    dry_run: bool,
) -> list[dict[str, Any]]:
    """Classify a batch of rows via the LLM (or mock in dry-run mode).

    Each row is classified individually (one prompt per row) to get
    deterministic per-row JSON — batched only for cache deduplication upstream.
    Returns a parallel list of validated LLM result dicts.

    In dry-run mode the prompt is built but no LLM call is made; a stub result
    is returned instead.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        prompt = build_prompt(row, entity_type)

        if dry_run:
            # Dry-run: emit the prompt to stdout and return a stub result.
            print(f"[DRY-RUN] prompt for ({row.get('attribute')}):\n{prompt}\n---")
            result = _empty_llm_result("dry_run")
        else:
            raw_text = _call_llm(prompt)
            raw_obj = _extract_json_object(raw_text)
            if raw_obj is None:
                logger.warning(
                    "llm_diff.parse_failed",
                    attribute=row.get("attribute"),
                    snippet=raw_text[:200],
                )
                result = _empty_llm_result("parse_failed")
            else:
                result = _validate_llm_result(raw_obj)

        out.append(result)
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

LLM_FIELDNAMES: list[str] = [
    "llm_patterns",
    "llm_best_guess",
    "llm_best_value",
    "llm_confidence",
    "llm_rationale",
]


def enrich_csv(
    input_path: Path | str,
    output_path: Path | str,
    *,
    entity_type: str = "anime",
    sample: int | None = None,
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> dict[str, int]:
    """Enrich a cross-source diff CSV with LLM classification columns.

    Reads input_path (produced by 24/01 cross_source_diff.py), appends the five
    LLM columns, and writes to output_path.

    Args:
        input_path: Path to the base CSV (e.g. result/audit/cross_source_diff/anime.csv).
        output_path: Destination CSV (llm_classified variant).
        entity_type: "anime" | "persons" | "studios" — passed into the prompt.
        sample: If set, randomly sample this many rows from the input before
                processing (useful for cost-controlled spot checks). Applied
                before limit.
        limit: Hard cap on the number of rows to process (after sample).
        batch_size: Number of rows per LLM call batch.
        dry_run: If True, build prompts and structure but never call the LLM.

    Returns:
        Dict with keys "processed", "cached", "skipped".
        - processed: rows sent to LLM (not counting cache hits)
        - cached: rows resolved from cache (in-memory or checkpoint)
        - skipped: rows that received an empty/stub result (LLM unavailable etc.)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ---- Load input CSV ----
    with open(input_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        all_rows: list[dict[str, Any]] = list(reader)

    input_fieldnames: list[str] = list(all_rows[0].keys()) if all_rows else []
    output_fieldnames = input_fieldnames + LLM_FIELDNAMES

    logger.info(
        "llm_diff.enrich_start",
        input=str(input_path),
        total_rows=len(all_rows),
        sample=sample,
        limit=limit,
        dry_run=dry_run,
    )

    # ---- Apply sample / limit ----
    if sample is not None and sample < len(all_rows):
        import random

        random.seed(42)
        all_rows = random.sample(all_rows, sample)

    if limit is not None and limit < len(all_rows):
        all_rows = all_rows[:limit]

    rows = all_rows

    # ---- Load checkpoint (resume) ----
    checkpoint: dict[str, dict[str, Any]] = _load_checkpoint(output_path)

    # ---- In-memory dedup cache (value_a, value_b, attribute) → result ----
    in_mem_cache: dict[str, dict[str, Any]] = dict(checkpoint)  # seed from checkpoint

    # ---- LLM availability check ----
    llm_available = dry_run or check_llm_available()
    if not llm_available:
        logger.warning("llm_diff.llm_not_available", action="stub_mode")

    # ---- Process rows ----
    counters = {"processed": 0, "cached": 0, "skipped": 0}
    enriched_rows: list[dict[str, Any]] = []

    # Split into batches; process batches of uncached rows together.
    batch_buf: list[tuple[int, str, dict[str, Any]]] = []  # (row_idx, cache_key, row)

    def _flush_batch() -> None:
        """Process the current batch_buf, updating enriched_rows in-place.

        Deduplicates by cache_key within the batch: identical (value_a, value_b,
        attribute) triples yield exactly one LLM call; the result is fanned out
        to all rows sharing that key.
        """
        if not batch_buf:
            return

        # Deduplicate: collect unique keys and one representative row per key.
        seen_keys: dict[str, dict[str, Any]] = {}  # key → representative row
        key_order: list[str] = []  # unique keys in insertion order
        for _row_idx, key, row in batch_buf:
            if key not in seen_keys:
                seen_keys[key] = row
                key_order.append(key)

        unique_rows = [seen_keys[k] for k in key_order]

        if llm_available:
            unique_results = _classify_batch(unique_rows, entity_type, dry_run)
        else:
            unique_results = [_empty_llm_result("llm_unavailable")] * len(unique_rows)

        key_to_result: dict[str, dict[str, Any]] = dict(zip(key_order, unique_results))

        skipped_in_batch = 0
        for row_idx, key, _row in batch_buf:
            result = key_to_result[key]
            # Persist to in-memory cache and checkpoint only on first encounter
            if key not in in_mem_cache:
                in_mem_cache[key] = result
                _append_checkpoint(output_path, key, result)
                if llm_available:
                    counters["processed"] += 1
                else:
                    skipped_in_batch += 1
            else:
                counters["cached"] += 1
            enriched_rows[row_idx].update(result)

        counters["skipped"] += skipped_in_batch
        batch_buf.clear()

    for row in rows:
        enriched_row = dict(row)
        # Ensure all LLM columns exist (will be overwritten below)
        for col in LLM_FIELDNAMES:
            enriched_row[col] = ""
        enriched_rows.append(enriched_row)
        idx = len(enriched_rows) - 1

        key = _cache_key(row)

        if key in in_mem_cache:
            enriched_rows[idx].update(in_mem_cache[key])
            counters["cached"] += 1
            continue

        # Queue for batch processing
        batch_buf.append((idx, key, row))

        if len(batch_buf) >= batch_size:
            _flush_batch()

    # Flush remaining
    _flush_batch()

    # ---- Write output CSV ----
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=output_fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)

    logger.info(
        "llm_diff.enrich_done",
        output=str(output_path),
        rows=len(enriched_rows),
        **counters,
    )
    return counters


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _detect_entity_type(input_path: Path) -> str:
    """Guess entity type from the filename stem."""
    stem = input_path.stem.lower()
    for entity in ("anime", "persons", "studios"):
        if entity in stem:
            return entity
    return "anime"


def _cli_main() -> None:
    """CLI: python -m src.etl.audit.cross_source_diff_llm [options]"""
    import argparse

    parser = argparse.ArgumentParser(
        description="LLM enrichment for cross-source diff CSV"
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Input CSV (24/01 output). Defaults to result/audit/cross_source_diff/anime.csv",
        default=None,
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output CSV. Defaults to <input_stem>_llm_classified.csv in same dir.",
        default=None,
    )
    parser.add_argument(
        "--entity",
        choices=["anime", "persons", "studios"],
        default=None,
        help="Entity type for prompt context. Auto-detected from filename if omitted.",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=100,
        help="Randomly sample N rows before processing (default: 100). 0 = all rows.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Hard row cap after sampling.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per LLM batch (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build prompts but do not call LLM. Writes stub output CSV.",
    )
    parser.add_argument(
        "--all-entities",
        action="store_true",
        help="Process anime, persons, studios in sequence with the given options.",
    )

    args = parser.parse_args()

    audit_dir = Path("result/audit/cross_source_diff")

    if args.all_entities:
        for entity in ("anime", "persons", "studios"):
            inp = audit_dir / f"{entity}.csv"
            if not inp.exists():
                print(f"Skipping {entity} — {inp} not found", file=sys.stderr)
                continue
            out = audit_dir / f"{entity}_llm_classified.csv"
            sample = args.sample if args.sample > 0 else None
            counts = enrich_csv(
                inp,
                out,
                entity_type=entity,
                sample=sample,
                limit=args.limit,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
            )
            print(
                f"{entity}: processed={counts['processed']} "
                f"cached={counts['cached']} "
                f"skipped={counts['skipped']} → {out}"
            )
        return

    # Single-file mode
    if args.input is None:
        args.input = audit_dir / "anime.csv"

    if not args.input.exists():
        print(f"Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if args.output is None:
        stem = args.input.stem
        args.output = args.input.parent / f"{stem}_llm_classified.csv"

    entity_type = args.entity or _detect_entity_type(args.input)
    sample = args.sample if args.sample > 0 else None

    counts = enrich_csv(
        args.input,
        args.output,
        entity_type=entity_type,
        sample=sample,
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
    print(
        f"Done: processed={counts['processed']} "
        f"cached={counts['cached']} "
        f"skipped={counts['skipped']} → {args.output}"
    )


if __name__ == "__main__":
    _cli_main()
