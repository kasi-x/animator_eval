"""Representative value selection logic for the Resolved layer.

Implements the priority-fallback + majority-vote algorithm declared in
ARCHITECTURE_5_TIER_PROPOSAL.md §確定事項 (E).

Algorithm:
1. Group candidate rows by source prefix.
2. Walk priority list in order.
3. At each tier, collect non-NULL non-empty values from that source's rows.
4. If exactly one value at this tier: accept it (priority_fallback).
5. If multiple rows at same tier with the same value 3+ times: majority_vote.
6. Tie-break: use the first value from the first (highest-ranked) row in tier.
7. If current tier yields nothing: advance to next tier.
8. Return (chosen_value, winning_source, reason) tuple.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.etl.resolved.source_ranking import source_prefix


AuditEntry = dict[str, Any]


def _is_empty(value: Any) -> bool:
    """Return True if a value should be treated as missing."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def select_representative_value(
    field: str,
    candidates: list[dict[str, Any]],
    priority: list[str],
    id_key: str = "id",
    value_key: str | None = None,
) -> tuple[Any, str, str]:
    """Select the best representative value for `field` from a list of candidate rows.

    Args:
        field: Field name to select (used as value_key if value_key is None).
        candidates: List of row dicts, each with at least `id_key` and `field`.
        priority: Ordered list of source prefixes (highest priority first).
        id_key: Name of the ID column in each row dict.
        value_key: Name of the value column (defaults to `field`).

    Returns:
        Tuple of (selected_value, winning_source, reason).
        - winning_source: source prefix of the row that contributed the value.
        - reason: one of 'priority_fallback' | 'majority_vote' | 'tie_break' | 'no_value'.
    """
    vk = value_key or field

    # Group non-empty values by source prefix, preserving insertion order
    by_source: dict[str, list[Any]] = {}
    for row in candidates:
        src = source_prefix(str(row.get(id_key, "")))
        val = row.get(vk)
        if not _is_empty(val):
            by_source.setdefault(src, []).append(val)

    # Walk priority list
    for src in priority:
        values = by_source.get(src)
        if not values:
            continue
        if len(values) == 1:
            return (values[0], src, "priority_fallback")
        # Multiple rows for this source — majority vote
        counter = Counter(values)
        top_val, top_count = counter.most_common(1)[0]
        if top_count >= 3:
            return (top_val, src, "majority_vote")
        # Tie-break: first value (order of candidates list)
        return (values[0], src, "tie_break")

    # No value found in any source
    return (None, "", "no_value")


def build_audit_entries(
    canonical_id: str,
    entity_type: str,
    field: str,
    selected_value: Any,
    winning_source: str,
    reason: str,
) -> AuditEntry:
    """Build a single audit row for meta_resolution_audit."""
    return {
        "canonical_id": canonical_id,
        "entity_type": entity_type,
        "field_name": field,
        "field_value": str(selected_value) if selected_value is not None else None,
        "source_name": winning_source,
        "selection_reason": reason,
    }
