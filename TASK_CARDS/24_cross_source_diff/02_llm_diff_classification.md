# Task: ローカル LLM で diff の pattern flag + 正しそうな値判定

**ID**: `24_cross_source_diff/02_llm_diff_classification`
**Priority**: 🟠
**Blocked by**: 24/01 (cross-source diff CSV 生成)
**Status**: pending

---

## Goal

24/01 で生成した cross-source diff CSV (rule-based 自動分類) に対し、**ローカル LLM** で:
1. **複数 pattern flag** を付与 (例: `typo`, `abbreviation`, `different_naming_convention`, `historical_alias`, `different_entity` 等)
2. **どれが正しそうか** judgment (best_guess_value + confidence 0-1)

---

## 必読

1. `/home/user/dev/animetor_eval/CLAUDE.md`
2. `/home/user/dev/animetor_eval/src/analysis/llm_pipeline.py` (既存 LLM 基盤、22/06 で 50 tests 確認)
3. `/home/user/dev/animetor_eval/TASK_CARDS/24_cross_source_diff/01_diff_audit_marimo.md` (24/01 の出力 CSV format)

---

## 設計

### LLM prompt template (差分 1 行に対し 1 query)

```
You are a data quality auditor for an anime credit database.

Entity: {entity_type} (anime / persons / studios)
Attribute: {attribute}
Source A: {source_a} = "{value_a}"
Source B: {source_b} = "{value_b}"
Context: canonical_id={canonical_id}, ja_title={ja_title}, year={year}

Classify this discrepancy. Output JSON:
{
  "patterns": [list of: typo, abbreviation, romanization_variant, kanji_simplification,
                punctuation_normalization, historical_alias, different_naming_convention,
                year_off_by_one, digit_count_mismatch, different_entity, ambiguous],
  "best_guess": "value_a" | "value_b" | "neither",
  "best_value": "explicit string if neither",
  "confidence": 0.0-1.0,
  "rationale": "brief explanation"
}
```

### batch processing

- 数千〜数万 diff をバッチで処理 → cost / time 制御
- caching: 同じ (value_a, value_b, attribute) ペアは 1 回だけ query
- progress checkpoint で resume 可能

### 出力

`result/audit/cross_source_diff/{entity}_llm_classified.csv`:
```
canonical_id, attribute, source_a, value_a, source_b, value_b,
rule_classification (24/01),
llm_patterns (JSON list),
llm_best_guess, llm_best_value, llm_confidence, llm_rationale
```

---

## 範囲

- 新規: `src/etl/audit/cross_source_diff_llm.py` (LLM enrichment)
- 修正: `notebooks/cross_source_diff.py` (24/01 の marimo notebook に LLM column 表示)
- 必要: pixi 環境の LLM 設定 (Ollama / litellm 等、既存 llm_pipeline.py の LLM client 流用)
- 出力: `result/audit/cross_source_diff/{entity}_llm_classified.csv`

---

## 絶対遵守

- H1, H3, H5, H8
- LLM judgment は **判断補助**、自動 merge には使わない (H3 entity_resolution 不変)
- LLM cost 制御: caching + dry-run mode + limit option

---

## 完了条件

- `pixi run lint` clean
- `pixi run test-scoped tests/test_etl/test_cross_source_diff_llm.py` pass (mock LLM 含む)
- 実 LLM で sample 100 件処理 → CSV 出力確認
- marimo notebook に LLM patterns / best_guess / confidence 表示
- commit + bundle 完了
