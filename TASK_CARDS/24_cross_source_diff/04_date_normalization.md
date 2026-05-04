# Task: 日付表記揺れ正規化 (年のみ vs 完全日付の階層化)

**ID**: `24_cross_source_diff/04_date_normalization`
**Priority**: 🟠
**Blocked by**: 24/03 (consensus 集計)

---

## 問題

cross-source diff / consensus で日付列 (`start_date` / `end_date` / `release_date` / `aired_*`) に表記揺れあり:

| Source A | Source B | 現状分類 | 期待分類 |
|----------|----------|---------|---------|
| `2020` | `2020-04-15` | completely_different | **subset_compatible** (年だけ ⊆ 完全日付) |
| `2020/04/15` | `2020-04-15` | completely_different | **separator_variant** (区切り違い) |
| `2020-04-15` | `April 15, 2020` | completely_different | **format_variant** (英語表記) |
| `2020` | `2021` | off_by_year | year_off_by_one (現状維持) |
| `{"year": 2025, "month": 4, "day": 8}` | `2025-04-08` | completely_different | **structural_variant** (JSON vs ISO) |

## ゴール

1. 日付列を ISO 8601 (`YYYY-MM-DD`) に正規化
2. **subset 互換** 検出: 「年のみ ⊆ 完全日付」を「整合」と判定
3. consensus 集計で **より詳細な日付を採用**
4. 表記揺れ ≠ データ不整合 を区別

## 対象 column

- `start_date` / `end_date`
- `aired_from` / `aired_to`
- `release_date` / `first_air_date` / `last_air_date`
- `birth_date` / `death_date`
- `airing_schedule_json` 内 day_of_week / etc. (skip)

## 正規化ルール

### type: `date_iso8601_with_subset`

入力フォーマット (parser):
- ISO: `YYYY-MM-DD` / `YYYY-MM` / `YYYY`
- スラッシュ: `YYYY/MM/DD`
- ドット: `YYYY.MM.DD`
- 英語: `April 15, 2020` / `Apr 15 2020` / `2020 Apr 15`
- 和暦: `平成32年4月15日` (low priority、skip 可)
- JSON struct: `{"year": Y, "month": M, "day": D}`

出力: `YYYY-MM-DD` (`YYYY-MM-XX` / `YYYY-XX-XX` で部分既知部分のみ ISO で保持、欠損は `XX`)

### consensus 判定: subset compatible

```python
def is_date_subset_compatible(a: str, b: str) -> bool:
    """e.g. '2020' vs '2020-04-15' → True (年一致 + b は a の specialization)"""
    pa = parse(a)  # (Y, M|None, D|None)
    pb = parse(b)
    # 共通する非 None 部分が一致
    for x, y in zip(pa, pb):
        if x is not None and y is not None and x != y:
            return False
    return True
```

### consensus 集計: より詳細優先

`majority_value` 選択時:
1. 同じ ISO date なら 1 票
2. subset 互換なら **より詳細な日付を採用** (3 source 中 2 が "2020"、1 が "2020-04-15" → "2020-04-15" を採用)
3. 互換 set 中で全体一致票数を majority_count にカウント

---

## 範囲

- 修正: `src/etl/normalize/column_rules.py` に `date_iso8601_with_subset` type 追加
- 修正: `src/etl/audit/cross_source_consensus.py` で date 列の subset 判定を統合
- 新規: `src/etl/normalize/date_parser.py` (multi-format parser、subset compatible check)
- 修正: `tests/test_etl/test_cross_source_consensus.py` に date テスト追加
- 修正: `notebooks/cross_source_diff.py` で date 列の consensus 表示

---

## 完了条件

- `pixi run lint` clean
- `pixi run test-scoped tests/test_etl/test_cross_source_consensus.py` pass
- consensus CSV 再生成
- `2020` vs `2020-04-15` が subset_compatible として unanimous 扱い (期待)
- date 列の `unique_outlier` が大幅減 (期待)
- commit + bundle 完了

---

## 注記

- ISO 8601 統一は **conformed 層 / resolved 層** には書込まず、**audit 用の正規化** のみ (上書き禁止)
- 既存 source 値は保持 (display 用)、`normalized_majority_value` 列のみ ISO 化
