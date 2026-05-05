"""LLM verdict 集計: merge_decisions_reviewed.jsonl → 戦略修正候補レポート。

集計軸:
- verdict 分布 (全体 / entity 別 / field 別 / source 別)
- 'split' 多発 cluster (cluster ロジック弱点)
- 'wrong_value' 多発 (priority list 並び替え候補)
- 'low_confidence' 多発 field (LLM 限界 → 人間レビュー)

Usage:
    pixi run python scripts/maintenance/summarize_merge_verdicts.py \\
        --in result/merge_decisions_reviewed.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--top", type=int, default=20)
    args = p.parse_args()

    inp = Path(args.inp)
    total = 0
    skipped_no_review = 0

    by_verdict: Counter[str] = Counter()
    by_entity_verdict: dict[str, Counter[str]] = defaultdict(Counter)
    by_entity_field_verdict: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    by_winner_verdict: dict[str, Counter[str]] = defaultdict(Counter)
    split_clusters: Counter[str] = Counter()  # canonical_id ごとの 'split' 件数
    wrong_value_examples: list[dict] = []

    with open(inp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            total += 1
            review = r.get("llm_review")
            if not review or not review.get("verdict"):
                skipped_no_review += 1
                continue
            verdict = review["verdict"]
            entity = r["entity_type"]
            field = r["field"]
            winner = r["selected"]["src"]
            by_verdict[verdict] += 1
            by_entity_verdict[entity][verdict] += 1
            by_entity_field_verdict[(entity, field)][verdict] += 1
            by_winner_verdict[winner][verdict] += 1
            if verdict == "split":
                split_clusters[r["canonical_id"]] += 1
            elif verdict == "wrong_value" and len(wrong_value_examples) < args.top:
                wrong_value_examples.append({
                    "canonical_id": r["canonical_id"],
                    "field": field,
                    "selected": r["selected"],
                    "candidates": r["candidates"],
                    "reason": review.get("reason", ""),
                })

    print(f"=== 全体 ===")
    print(f"  total records:    {total:,}")
    print(f"  with llm_review:  {total - skipped_no_review:,}")
    print(f"  no llm_review:    {skipped_no_review:,}")

    print(f"\n=== verdict 分布 ===")
    for v, n in by_verdict.most_common():
        pct = 100 * n / max(1, total - skipped_no_review)
        print(f"  {v:18s}: {n:>6,} ({pct:.1f}%)")

    print(f"\n=== entity × verdict ===")
    for entity, c in by_entity_verdict.items():
        n_e = sum(c.values())
        print(f"  [{entity}] (n={n_e:,})")
        for v, n in c.most_common():
            print(f"    {v:18s}: {n:>6,} ({100*n/n_e:.1f}%)")

    print(f"\n=== entity × field の 'split'/'wrong_value' 上位 (戦略修正候補) ===")
    bad: list[tuple[tuple[str, str], int, int, int]] = []
    for (entity, field), c in by_entity_field_verdict.items():
        bad_n = c.get("split", 0) + c.get("wrong_value", 0)
        if bad_n > 0:
            bad.append(((entity, field), bad_n, c.get("split", 0), c.get("wrong_value", 0)))
    bad.sort(key=lambda x: x[1], reverse=True)
    for (entity, field), bad_n, sp, wv in bad[:args.top]:
        print(f"  {entity}.{field}: bad={bad_n} (split={sp}, wrong_value={wv})")

    print(f"\n=== winning_source × verdict (上位) ===")
    src_bad: list[tuple[str, int]] = []
    for src, c in by_winner_verdict.items():
        bad_n = c.get("split", 0) + c.get("wrong_value", 0)
        src_bad.append((src, bad_n))
    src_bad.sort(key=lambda x: x[1], reverse=True)
    for src, bad_n in src_bad[:args.top]:
        c = by_winner_verdict[src]
        n_total_src = sum(c.values())
        if n_total_src == 0:
            continue
        print(f"  {src}: bad={bad_n}/{n_total_src} ({100*bad_n/n_total_src:.1f}%)")

    if split_clusters:
        print(f"\n=== 'split' 件数上位 cluster (cluster ロジック弱点) ===")
        for cid, n in split_clusters.most_common(args.top):
            print(f"  {cid}: {n} fields flagged split")

    if wrong_value_examples:
        print(f"\n=== 'wrong_value' 例 (priority list 並替候補) ===")
        for ex in wrong_value_examples[:args.top]:
            print(f"  - {ex['canonical_id']}.{ex['field']}: selected={ex['selected']['src']}={ex['selected']['value']!r}")
            print(f"    candidates: {[(c['src'], c['value']) for c in ex['candidates']]}")
            print(f"    reason: {ex['reason']}")


if __name__ == "__main__":
    main()
