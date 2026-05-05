"""Resolved 層 merge 個別判断 trace の JSONL writer。

代表値選抜の **個別 case** (cluster × field) を JSONL に append。LLM は
これを読んで `llm_review` を埋める (verdict + reason)。

設計:
- append-only、1 行 = 1 判断 case
- context (cluster_size / value_distinct_count / edit_distance_max 等) を
  事前計算して LLM 判断補助に同梱
- sampling_rate で entire vs random subset を制御 (270K cluster × 多 field
  全件は LLM に投入できないため)
"""

from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path
from typing import Any

DEFAULT_DECISIONS_PATH = Path("result") / "merge_decisions.jsonl"


def _normalize_value(v: Any) -> Any:
    """JSON-serializable 化 (str/None/数値はそのまま、それ以外 str 化)。"""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def _edit_distance(a: str, b: str) -> int:
    """Levenshtein distance (DP, O(len(a)*len(b)))。短文 (人名/題名) 想定。"""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr[j] = min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost)
        prev = curr
    return prev[-1]


def build_decision_record(
    *,
    canonical_id: str,
    entity_type: str,
    field: str,
    candidates: list[dict[str, Any]],
    selected_value: Any,
    winning_source: str,
    selection_rule: str,
    priority: list[str],
    id_key: str = "id",
) -> dict[str, Any]:
    """1 件の決定 record を組み立てる (LLM レビュー対象)。

    Returns:
        {canonical_id, entity_type, field, candidates, selected, context, llm_review}
    """
    cands = []
    str_values: list[str] = []
    for row in candidates:
        rid = str(row.get(id_key, ""))
        src = rid.split(":", 1)[0] if ":" in rid else rid
        val = row.get(field)
        try:
            tier = priority.index(src)
        except ValueError:
            tier = -1
        cands.append({
            "src_id": rid,
            "src": src,
            "value": _normalize_value(val),
            "tier": tier,
        })
        if isinstance(val, str) and val.strip():
            str_values.append(val)

    distinct_values = sorted({c["value"] for c in cands if c["value"] not in (None, "")},
                             key=lambda x: str(x))
    edit_max = 0
    if len(str_values) >= 2:
        for i in range(len(str_values)):
            for j in range(i + 1, len(str_values)):
                d = _edit_distance(str_values[i], str_values[j])
                if d > edit_max:
                    edit_max = d

    context = {
        "cluster_size": len(candidates),
        "value_distinct_count": len(distinct_values),
        "string_edit_distance_max": edit_max,
    }

    return {
        "canonical_id": canonical_id,
        "entity_type": entity_type,
        "field": field,
        "candidates": cands,
        "selected": {
            "value": _normalize_value(selected_value),
            "src": winning_source,
            "rule": selection_rule,
        },
        "context": context,
        "llm_review": None,
    }


def _should_sample(canonical_id: str, field: str, *, sample_rate: float) -> bool:
    """deterministic sampling: hash(canonical_id|field) で決定。

    sample_rate=1.0 で全件、0.001 で 0.1%。同じ ETL 実行間で再現可能。
    """
    if sample_rate >= 1.0:
        return True
    if sample_rate <= 0.0:
        return False
    h = hashlib.sha256(f"{canonical_id}|{field}".encode()).hexdigest()[:8]
    bucket = int(h, 16) / 0xFFFFFFFF
    return bucket < sample_rate


class DecisionsLogger:
    """JSONL append-only writer for merge decisions.

    Usage:
        with DecisionsLogger("result/merge_decisions.jsonl",
                             sample_rate=0.001) as log:
            for record in records:
                log.write(record)
    """

    def __init__(
        self,
        path: str | Path = DEFAULT_DECISIONS_PATH,
        *,
        sample_rate: float = 1.0,
        seed: int | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sample_rate = sample_rate
        self._fh = None
        self._rng = random.Random(seed) if seed is not None else None
        self.written = 0
        self.skipped = 0

    def __enter__(self) -> "DecisionsLogger":
        self._fh = open(self.path, "a", encoding="utf-8")
        return self

    def __exit__(self, *args: Any) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def write(self, record: dict[str, Any]) -> None:
        if not _should_sample(
            record["canonical_id"], record["field"], sample_rate=self.sample_rate
        ):
            self.skipped += 1
            return
        assert self._fh is not None, "DecisionsLogger not entered as context manager"
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.written += 1

    def truncate(self) -> None:
        """既存 jsonl を削除 (full-rebuild 用)。"""
        if self.path.exists():
            self.path.unlink()
