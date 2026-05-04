# Task: cross-source 値違い集計 + 分類 + marimo notebook

**ID**: `24_cross_source_diff/01_diff_audit_marimo`
**Priority**: 🟠
**Status**: pending

---

## Goal

1. 各 source 間で同 entity の値が異なるケースを集合化
2. 各 diff を **表記揺れ** / **誤入力** / **要調査** に自動分類
3. `marimo` notebook で対話的に確認可能な dashboard 提供

---

## 対象 entity / 属性

### anime (resolved 経由 cross-source mapping 利用)
- `title_ja` / `title_en` / `synonyms`
- `year` / `start_date` / `end_date`
- `episodes` / `format` / `duration`
- studio リスト (cross-source set)

### persons (canonical_id 経由)
- `name_ja` / `name_en`
- `birth_date` / `gender`
- `hometown` / `country_of_origin`

### studios
- `name` / `country`

---

## 分類ロジック

### Layer 1: 自動分類

| 分類 | 検出ロジック |
|------|----|
| **identical_after_normalize** | NFKC + 旧字体→新字体 + lowercase + 句読点除去 で一致 → **表記揺れ** |
| **single_char_diff** | Levenshtein distance = 1 + 文字長 > 3 → **誤入力候補** (typo) |
| **digit_count_mismatch** | year 等の数値で桁数が違う → **誤入力候補** (4 桁 vs 2 桁) |
| **off_by_year** | year が ±1 違い → **要調査** (放送開始 vs 放送年表記) |
| **multi_char_diff** | Levenshtein > 1 → **要調査** |
| **completely_different** | normalize しても全く違う → **要調査** |

### Layer 2: confidence score

- `representative_source_pick`: source ranking の最上位値を採用 (現 Phase 2b 実装)
- `is_representative_correct`: ランダムサンプル 100 件で人手 review (notebook 内)

---

## 実装

### Files to create

| File | 内容 |
|------|------|
| `src/etl/audit/cross_source_diff.py` | diff 集計 + 分類 ETL |
| `tests/test_etl/test_cross_source_diff.py` | 分類ロジック unit test |
| `notebooks/cross_source_diff.py` | marimo reactive notebook |
| `result/audit/cross_source_diff/` | 出力 CSV (anime / persons / studios 別) |

### `src/etl/audit/cross_source_diff.py`

```python
def collect_diffs(conn, entity: Literal["anime", "persons", "studios"]) -> DataFrame:
    """各 source の同 canonical_id 行で値が異なる属性を集計。
    Returns columns: canonical_id, attribute, source_a, value_a, source_b, value_b, classification"""

def classify_diff(value_a: str | None, value_b: str | None, attribute: str) -> str:
    """returns one of: identical_after_normalize / single_char_diff / digit_count_mismatch /
       off_by_year / multi_char_diff / completely_different / null_in_one"""

def export_audit(conn, output_dir: Path) -> dict[str, int]:
    """全 entity / 全 attribute で diff 集計 → CSV 出力。Returns counts."""
```

### `notebooks/cross_source_diff.py` (marimo)

```python
import marimo as mo
import pandas as pd
import duckdb

@app.cell
def __():
    df_anime = pd.read_csv("result/audit/cross_source_diff/anime.csv")
    df_persons = pd.read_csv("result/audit/cross_source_diff/persons.csv")
    df_studios = pd.read_csv("result/audit/cross_source_diff/studios.csv")
    return df_anime, df_persons, df_studios

@app.cell
def __(mo, df_anime):
    entity = mo.ui.dropdown(["anime", "persons", "studios"], value="anime")
    classification = mo.ui.dropdown(
        ["all", "identical_after_normalize", "single_char_diff", "digit_count_mismatch",
         "off_by_year", "multi_char_diff", "completely_different"],
        value="all",
    )
    return entity, classification

@app.cell
def __(entity, classification, df_anime, df_persons, df_studios):
    df = {"anime": df_anime, "persons": df_persons, "studios": df_studios}[entity.value]
    if classification.value != "all":
        df = df[df["classification"] == classification.value]
    return df

# ... 統計 / sample 表示 / source matrix heatmap 等
```

依存: `pixi add marimo` (要 toml 編集)。

---

## 範囲

- 新規: `src/etl/audit/cross_source_diff.py`, `tests/test_etl/test_cross_source_diff.py`, `notebooks/cross_source_diff.py`
- 修正: `pixi.toml` に marimo 依存追加
- 出力: `result/audit/cross_source_diff/{anime,persons,studios}.csv`
- **触らない**: src/analysis/scoring (H1)、entity_resolution (H3)

---

## 完了条件

- `pixi run lint` clean
- `pixi run test-scoped tests/test_etl/test_cross_source_diff.py` pass
- 実 conformed (animetor.duckdb) → CSV 3 件生成、各 1000+ 行
- `pixi run marimo run notebooks/cross_source_diff.py` で起動可能
- 主要 sample 確認:
  - 表記揺れ例: 「J.C.STAFF」 vs 「JC STAFF」 → identical_after_normalize
  - 誤入力例: year 「2020」 vs 「2002」 → multi_char_diff
- commit + bundle 完了

---

## 警告

scrape 関連は触らない。本タスクは Conformed 層 (silver) を read-only で集計、Resolved 経由の canonical_id mapping を使用。
