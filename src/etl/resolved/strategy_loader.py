"""Resolved 層 merge strategy JSON loader.

`docs/merge_strategy.json` を読み込み、`select_representative_value` /
cluster ロジックが参照する priority list / threshold を JSON 駆動で提供。

LLM レビュー後 strategy.json を編集すれば、コード再ビルド不要で挙動変更可能。

H3 互換: cluster algorithm 自体は entity_resolution に委譲、本 module は
priority list と threshold のみ取り扱う。
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

DEFAULT_STRATEGY_PATH = Path(__file__).resolve().parents[3] / "docs" / "merge_strategy.json"


@lru_cache(maxsize=4)
def load_strategy(path: str | Path = DEFAULT_STRATEGY_PATH) -> dict[str, Any]:
    """JSON ロード (キャッシュ付き)。"""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def priority_for(entity_type: str, field: str, *, path: str | Path = DEFAULT_STRATEGY_PATH) -> list[str]:
    """entity (anime/person/studio) × field → priority list を返す。"""
    strategy = load_strategy(path)
    fields = strategy["entities"][entity_type]["fields"]
    spec = fields.get(field)
    if spec is None:
        return []
    return list(spec["priority"])


def selection_rule_for(entity_type: str, field: str, *, path: str | Path = DEFAULT_STRATEGY_PATH) -> str:
    """field の selection_rule (priority_fallback 等) を返す。"""
    strategy = load_strategy(path)
    fields = strategy["entities"][entity_type]["fields"]
    spec = fields.get(field, {})
    return spec.get("selection_rule", "priority_fallback")


def majority_threshold(*, path: str | Path = DEFAULT_STRATEGY_PATH) -> int:
    """majority_vote 発動の同値件数閾値を返す。"""
    strategy = load_strategy(path)
    return int(strategy["selection_rules"]["majority_vote"]["threshold"])


def fields_for(entity_type: str, *, path: str | Path = DEFAULT_STRATEGY_PATH) -> list[str]:
    """entity の field 一覧を strategy 順で返す。"""
    strategy = load_strategy(path)
    return list(strategy["entities"][entity_type]["fields"].keys())


def cluster_spec(entity_type: str, *, path: str | Path = DEFAULT_STRATEGY_PATH) -> dict[str, Any]:
    """entity の cluster strategy 設定を返す。"""
    strategy = load_strategy(path)
    return dict(strategy["entities"][entity_type]["cluster"])
