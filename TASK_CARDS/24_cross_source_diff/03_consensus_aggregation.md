# Task: cross-source consensus 集計 (N source、majority value 検出)

**ID**: `24_cross_source_diff/03_consensus_aggregation`
**Priority**: 🟠
**Blocked by**: 24/01 (canonical_id mapping)

---

## Goal

24/01 の **pairwise diff** (source_a vs source_b) では捉えきれない、**N source 間の consensus** を集計:
- 3 source 中 2 が一致 → majority
- 4 source 中 2 + 2 → tie
- 全員一致 → consensus
- outlier 1 つ → 異常値検出

---

## 出力 column

CSV: `result/audit/cross_source_diff/{entity}_consensus.csv`

| 列 | 内容 |
|----|----|
| `canonical_id` | 対象 entity ID |
| `attribute` | 比較対象列 (title_ja / year / studios / etc.) |
| `n_sources` | 値を提供した source 数 |
| `n_distinct_values` | distinct 値数 (NULL 除外) |
| `values_json` | source → value の mapping (JSON) |
| `majority_value` | 多数派の値 (tie 時は最上位 source ranking) |
| `majority_count` | 多数派 source 数 |
| `majority_share` | majority_count / n_sources |
| `consensus_flag` | `unanimous` / `majority` / `plurality` / `tie` / `unique_outlier` |
| `outlier_sources` | 少数派 source list (JSON) |
| `outlier_values` | 少数派 value list (JSON) |
| `normalized_consensus_flag` | NFKC + 旧字体→新字体 後の consensus_flag |

---

## consensus_flag 分類

| flag | 条件 |
|------|------|
| `unanimous` | n_distinct_values == 1 (全員一致) |
| `majority` | majority_share > 50% (3/4, 4/5, 4/6 等) |
| `plurality` | 最多が 50% 以下だが単独最多 (例 2/4 で他は 1/4 ずつ) |
| `tie` | 同票が 2 種以上 |
| `unique_outlier` | majority + 1 つだけ違う (例 5/6 一致、1 だけ違う = outlier) |

---

## LLM enrichment (24/02 と統合可能)

`unique_outlier` ケースで LLM に「outlier が誤入力か別表記か」を判定させる。

---

## 範囲

- 新規: `src/etl/audit/cross_source_consensus.py` (collect / classify / export)
- 新規: `tests/test_etl/test_cross_source_consensus.py`
- 修正: `notebooks/cross_source_diff.py` (consensus tab 追加 or 別 cell)
- 出力: `result/audit/cross_source_diff/{entity}_consensus.csv`

---

## 完了条件

- `pixi run lint` clean
- `pixi run test-scoped tests/test_etl/test_cross_source_consensus.py` pass
- 実 conformed → CSV 3 件生成 (anime / persons / studios)
- 主要 sample 確認:
  - title_ja 全員一致 → unanimous
  - title_ja 3/4 共通 → majority + outlier_sources 1 件
  - studios リスト → unanimous (同集合) / majority / outlier_outlier
- commit + bundle 完了
