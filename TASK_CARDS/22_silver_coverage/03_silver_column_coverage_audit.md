# Task: SILVER 全列 coverage 包括監査ツール

**ID**: `22_silver_coverage/03_silver_column_coverage_audit`
**Priority**: 🟠
**Estimated changes**: 約 +600 / -0 lines, 3 files
**Requires senior judgment**: yes (取込ギャップの定義)
**Blocks**: 22/04+ (列別修正カード)
**Blocked by**: なし

---

## Goal

SILVER 全表・全 nullable 列について以下を計測する監査ツール:
1. SILVER 側 NULL 率 / 空文字率
2. BRONZE 側に値が存在するか (取込ギャップ判定)
3. source 別の取込率

→ 21/02 O1 gender 95.4% null や 22/01 anime_studios 97.5% miss のような **silent 取込失敗** を網羅的に検出。

---

## Hard constraints

- H5: 既存テスト破壊禁止
- H8
- read-only クエリのみ

---

## Pre-conditions

- [ ] `git status` clean
- [ ] SILVER duckdb 存在
- [ ] BRONZE parquet 存在
- [ ] `pixi run test` baseline pass

---

## 設計

### 計測対象 (28 SILVER 表 × 全 nullable 列)

主要 column 例:
- `persons.gender` / `birth_date` / `death_date` / `country_of_origin` / `name_zh` / `name_ko` / `image_url`
- `anime.title_en` / `synonyms` / `country_of_origin` / `is_adult` / `external_links_json` / `airing_schedule_json` / `trailer_url` / `display_*`
- `characters.name_ja` / `name_en` / `gender` / `date_of_birth` / `description`
- `studios.country` / `name_en` / `established_year` / `website_url`

### 計測ロジック

```python
def measure_column_coverage(conn, table: str, col: str) -> dict:
    """returns {total, non_null, non_empty, null_rate, empty_rate}"""

def find_bronze_source_with_value(bronze_root, source: str, table: str, col: str) -> int:
    """returns count of BRONZE rows where this column has a value (NULL/empty 除外)"""

def gap_analysis(silver_conn, bronze_root) -> DataFrame:
    """For each (silver_table, silver_col), check if BRONZE source has value
    but SILVER doesn't. Returns gap rows."""
```

### 出力

`result/audit/silver_column_coverage.md`:
- per-table summary (各表の NULL 率 top 10 列)
- per-(table × source) gap analysis (BRONZE にあるが SILVER に来ていない列)
- 重大 gap top 20 (NULL 率 > 80% かつ BRONZE で 1000+ rows 存在)
- per-column source distribution (どの source から値が来ているか)

### 使用 column → BRONZE column の mapping (静的宣言)

```python
COLUMN_BRONZE_MAP = {
    ("persons", "gender"): {
        "anilist": ("source=anilist/table=persons", "gender"),
        "mal": ("source=mal/table=people", "gender"),
        "bangumi": ("source=bangumi/table=persons", "gender"),
        ...
    },
    ("persons", "country_of_origin"): {
        "anilist": ("source=anilist/table=persons", "homeTown"),
        ...
    },
    ...
}
```

未マップ列は「未調査」報告。

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/audit/silver_column_coverage.py` | `measure()` + `gap_analysis()` + `generate_report()` |
| `tests/test_etl/test_silver_column_coverage.py` | 検出ロジック unit test |
| `result/audit/silver_column_coverage.md` | 監査レポート (実行で生成) |

## Files to NOT touch

- silver_loaders / silver_reader / etc. (本タスクは監査のみ、修正は別カード)

---

## Steps

### Step 1: SILVER 全表・全列の DESCRIBE 取得

```python
def list_silver_columns(conn) -> list[tuple[str, str, str]]:
    """[(table, column, data_type), ...] for all main schema tables"""
```

### Step 2: COLUMN_BRONZE_MAP 整備

主要 source × 主要列の mapping を宣言。
- 100% 網羅でなくて OK。最低 persons / anime / characters / studios の primary nullable column 全部。

### Step 3: gap_analysis()

各 (silver_table, silver_col) で:
- SILVER NULL/empty rate
- BRONZE source 別に値存在 row 数
- 「BRONZE に値あり、SILVER に来てない」 = gap、source 別に集計

### Step 4: Markdown レポート

```
| silver_table | silver_col | null_rate | bronze_with_value | gap_severity |
|--------------|------------|-----------|-------------------|--------------|
| persons      | gender     | 95.4%     | anilist:120k mal:80k | CRITICAL  |
| anime        | country_of_origin | ... | ...           | HIGH         |
```

severity:
- CRITICAL: null > 80% & BRONZE > 10K
- HIGH: null > 50% & BRONZE > 1K
- MEDIUM: null > 30%

### Step 5: 実行

```bash
pixi run python -m src.etl.audit.silver_column_coverage
```

`result/audit/silver_column_coverage.md` 生成。

### Step 6: 結果サマリ + 22/04+ カード起票候補リスト出力

報告に top 5 CRITICAL gap を含める。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_column_coverage.py
ls result/audit/silver_column_coverage.md
head -50 result/audit/silver_column_coverage.md
```

---

## Stop-if conditions

- [ ] BRONZE parquet 読み込み不可
- [ ] 既存テスト破壊

---

## Rollback

```bash
rm src/etl/audit/silver_column_coverage.py tests/test_etl/test_silver_column_coverage.py result/audit/silver_column_coverage.md
```

---

## Completion signal

- [ ] レポート生成
- [ ] CRITICAL gap top 5 を報告に列挙 (22/04+ 候補)
- [ ] DONE: `22_silver_coverage/03_silver_column_coverage_audit`
