# Task: meta_lineage に 5 レポートの lineage を投入

**ID**: `02_phase4/01_meta_lineage_population`
**Priority**: 🟠 Major
**Estimated changes**: 約 +150 / -10 lines, 5-6 files (レポート 5 本 + 共通ヘルパー)
**Requires senior judgment**: **yes** — 各レポートの式・CI 方法・null model を正確に記述する必要
**Blocks**: `02_phase4/03_method_notes_validation`
**Blocked by**: `01_schema_fix/` 全完了 (v55 で meta_lineage が確実に使える状態)

---

## Goal

以下 5 レポートの末尾で `meta_lineage` テーブルに lineage メタデータ (formula_version, ci_method, null_model, inputs_hash 等) を INSERT する処理を追加する。

対象レポート:
1. `policy_attrition`
2. `policy_monopsony`
3. `policy_gender_bottleneck`
4. `mgmt_studio_benchmark`
5. `biz_genre_whitespace`

---

## Hard constraints

- H1 / H2 / H5 / H8 (共通)
- H3 entity resolution ロジック不変

**本タスク固有**:
- **既存レポートの Findings / Interpretation 本文は変えない**
- lineage 挿入は **レポート生成の最後** に 1 回
- 同一レポートで複数回 INSERT すると監査追跡が乱れるため、**冪等** (`INSERT OR REPLACE`) で書くこと

---

## Pre-conditions

- [ ] `01_schema_fix/` 全カード完了
- [ ] `pixi run test` pass (2161)
- [ ] `sqlite3 result/animetor.db "PRAGMA user_version;"` が 55 以上
- [ ] `meta_lineage` テーブル存在確認:
  ```bash
  sqlite3 result/animetor.db "PRAGMA table_info(meta_lineage);"
  # 期待: 複数カラム (formula_version, ci_method 等)
  ```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/report_generators/reports/policy_attrition.py` | lineage INSERT 追加 |
| `scripts/report_generators/reports/policy_monopsony.py` | 同上 |
| `scripts/report_generators/reports/policy_gender_bottleneck.py` | 同上 |
| `scripts/report_generators/reports/mgmt_studio_benchmark.py` | 同上 |
| `scripts/report_generators/reports/biz_genre_whitespace.py` | 同上 |
| `scripts/report_generators/method_notes.py` | (必要なら) lineage 投入ヘルパーを共通化 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| 他のレポート 32 本 | 本タスク範囲外 |
| `src/analysis/` | 計算ロジック不変 |
| `src/pipeline_phases/` | pipeline 本体を触らない |
| `meta_lineage` テーブル DDL | v54 で確立済み。カラム追加が必要な場合は別タスク化 |

---

## Steps

### Step 0: meta_lineage のスキーマを確認

```bash
# 実 DB で確認
sqlite3 result/animetor.db "PRAGMA table_info(meta_lineage);"
```

代表的なカラム (v54 時点):
- `report_id` TEXT
- `formula_version` TEXT
- `ci_method` TEXT
- `null_model` TEXT
- `holdout_validation` TEXT
- `inputs_hash` TEXT
- `generated_at` TIMESTAMP
- `description` TEXT
- (実 DB で確認すること)

カラム名が異なる場合は実 DB の定義に従う。

### Step 1: 共通ヘルパーの有無確認

```bash
grep -n "def.*lineage\|INSERT.*meta_lineage" scripts/report_generators/method_notes.py scripts/report_generators/_base.py
```

既に `insert_lineage(...)` 相当のヘルパーがあるならそれを使う。なければ Step 2 で作る。

### Step 2: 共通ヘルパーを追加 (ヘルパーがなければ)

`scripts/report_generators/method_notes.py` (または `_base.py`) に以下を追加:

```python
def insert_lineage(
    conn,
    *,
    report_id: str,
    formula_version: str,
    ci_method: str,
    null_model: str,
    holdout_validation: str,
    inputs_hash: str,
    description: str,
) -> None:
    """Idempotently record a report's lineage metadata in meta_lineage.

    Called at the end of each report generator. Using INSERT OR REPLACE
    guarantees that re-running a report overwrites rather than duplicates.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO meta_lineage
            (report_id, formula_version, ci_method, null_model,
             holdout_validation, inputs_hash, generated_at, description)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """,
        (report_id, formula_version, ci_method, null_model,
         holdout_validation, inputs_hash, description),
    )
```

**重要**: 実際のカラム名は Step 0 で確認した DB スキーマに合わせる。

### Step 3: 各レポートに lineage 投入を追加 (5 本)

**パターン** (`policy_attrition.py` の例):

```python
# レポート生成本体の末尾、return / save の直前に追加
from scripts.report_generators.method_notes import insert_lineage
import hashlib, json

# inputs_hash: このレポートが依拠するデータの特徴量を決定論的にハッシュ化
_inputs_fingerprint = json.dumps({
    "cohort_year_range": [2000, 2025],
    "role_filter": ["key_animator", "animation_director"],
    "dormancy_threshold_months": 24,
}, sort_keys=True)
_inputs_hash = hashlib.sha256(_inputs_fingerprint.encode()).hexdigest()[:16]

insert_lineage(
    conn,
    report_id="policy_attrition",
    formula_version="v1.0",
    ci_method="analytical (sigma/sqrt(n), per-person clustered SE)",
    null_model="random credit reassignment (100 draws, seed=42)",
    holdout_validation="leave-one-year-out (last 3 years)",
    inputs_hash=_inputs_hash,
    description=(
        "Attrition = fraction of persons with no credits in the following "
        "calendar year among those credited in the baseline year. Measures "
        "'credit visibility loss', not 'career exit'."
    ),
)
```

**5 レポートそれぞれに固有のメタデータ**:

| report_id | 固有ポイント |
|-----------|-------------|
| `policy_attrition` | inputs: cohort 年、role フィルタ、dormancy 定義 |
| `policy_monopsony` | inputs: HHI 計算ベース、スタジオ数閾値 |
| `policy_gender_bottleneck` | inputs: 推定モデル、時期、CI 計算単位 |
| `mgmt_studio_benchmark` | inputs: スタジオ集合、期間、スケール指標 |
| `biz_genre_whitespace` | inputs: ジャンル空間、空白判定閾値 |

各レポートの本文を読み、**実際に使っている**入力・閾値・式を反映する (推測で埋めない)。分からなければ Stop-if して質問する。

### Step 4: レポートの既存テストが pass するか確認

```bash
pixi run test -- -k "policy_attrition or policy_monopsony or policy_gender or mgmt_studio or biz_genre"
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "policy_attrition or policy_monopsony or policy_gender or mgmt_studio or biz_genre or lineage"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文・Lint
pixi run lint
python -m py_compile scripts/report_generators/reports/policy_attrition.py \
    scripts/report_generators/reports/policy_monopsony.py \
    scripts/report_generators/reports/policy_gender_bottleneck.py \
    scripts/report_generators/reports/mgmt_studio_benchmark.py \
    scripts/report_generators/reports/biz_genre_whitespace.py

# 2. 実際に 5 レポートを生成
pixi run task report-briefs
# または該当レポートを個別に実行

# 3. meta_lineage に 5 行入った
sqlite3 result/animetor.db "
    SELECT report_id, formula_version, ci_method
    FROM meta_lineage
    WHERE report_id IN (
        'policy_attrition', 'policy_monopsony', 'policy_gender_bottleneck',
        'mgmt_studio_benchmark', 'biz_genre_whitespace'
    );
"
# 期待: 5 行

# 4. テスト全件
pixi run test-scoped tests/ -k "policy_attrition or policy_monopsony or policy_gender or mgmt_studio or biz_genre or lineage"
# 期待: 2161+ passed

# 5. vocabulary lint (lineage 追加で禁止語を埋めていないか)
pixi run python scripts/lint_report_vocabulary.py
# 期待: OK, 0 violations

# 6. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] レポートのロジックが不明で lineage 内容が推測になる → **ユーザに質問**
- [ ] `pixi run test` で既存テスト失敗
- [ ] vocabulary lint で禁止語検出 (description で「能力」「優秀」等を書いてしまった)
- [ ] `meta_lineage` のカラム名が想定と違う → DDL を確認してヘルパー修正
- [ ] 5 レポート生成が実環境で失敗 (DB lock 等、`CLAUDE.md` §7.6 参照)

---

## Rollback

```bash
git checkout scripts/report_generators/
pixi run test-scoped tests/ -k "policy_attrition or policy_monopsony or policy_gender or mgmt_studio or biz_genre or lineage"
# 必要なら DB の meta_lineage から追加行を削除:
sqlite3 result/animetor.db "
    DELETE FROM meta_lineage
    WHERE report_id IN (
        'policy_attrition','policy_monopsony','policy_gender_bottleneck',
        'mgmt_studio_benchmark','biz_genre_whitespace'
    );
"
```

---

## Completion signal

- [ ] Verification 全項目 pass (5 行が `meta_lineage` に存在)
- [ ] `git diff --stat` が 5-6 ファイル、±150 lines
- [ ] `git commit`:
  ```
  Populate meta_lineage for 5 briefs

  Adds lineage metadata (formula version, CI method, null model,
  holdout validation, inputs hash, description) for:
  - policy_attrition
  - policy_monopsony
  - policy_gender_bottleneck
  - mgmt_studio_benchmark
  - biz_genre_whitespace

  Uses INSERT OR REPLACE so re-runs are idempotent.
  ```
