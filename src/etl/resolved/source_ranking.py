"""Source priority rankings — strategy_loader 経由で JSON 駆動。

旧 API (`ANIME_RANKING` 等) は `docs/merge_strategy.json` から動的生成。
新規コードは `strategy_loader.priority_for()` を使う。

Selection algorithm (実装は `_select.py`):
1. Walk priority list in order.
2. Use the first non-NULL, non-empty value found.
3. If multiple conformed rows from the same priority tier exist,
   apply majority vote: pick the value agreed upon by N+ rows
   (N = strategy.selection_rules.majority_vote.threshold).
4. Tie-break: fall back to the value from the first (highest-priority) row.
"""

from __future__ import annotations

from src.etl.resolved.strategy_loader import load_strategy


def _build_ranking(entity_type: str) -> dict[str, list[str]]:
    fields = load_strategy()["entities"][entity_type]["fields"]
    return {f: list(spec["priority"]) for f, spec in fields.items()}


ANIME_RANKING: dict[str, list[str]] = _build_ranking("anime")
PERSONS_RANKING: dict[str, list[str]] = _build_ranking("person")
STUDIOS_RANKING: dict[str, list[str]] = _build_ranking("studio")


def source_prefix(conformed_id: str) -> str:
    """Return the source prefix from a conformed ID (e.g. 'anilist' from 'anilist:a123')."""
    if ":" in conformed_id:
        return conformed_id.split(":", 1)[0]
    return conformed_id


def rank_for_field(
    field: str,
    entity_type: str,  # 'anime' | 'person' | 'studio'
) -> list[str]:
    """Return the priority-ordered source list for a given entity type and field."""
    mapping: dict[str, dict[str, list[str]]] = {
        "anime": ANIME_RANKING,
        "person": PERSONS_RANKING,
        "studio": STUDIOS_RANKING,
    }
    table = mapping.get(entity_type, {})
    return table.get(field, [])
