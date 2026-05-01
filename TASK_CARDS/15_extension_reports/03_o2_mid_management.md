# Task: O2 mid_management_pipeline (中堅枯渇)

**ID**: `15_extension_reports/03_o2_mid_management`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -0 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

「動画 → 原画 → 作画監督 → 監督」の役職進行年数 cohort 別中央値・分散を Kaplan-Meier で推定し、スタジオ別パイプライン詰まり指標を HR brief / Policy brief に組込む。

---

## Hard constraints

- **H1**: `anime.score` を進行分析に使わない (構造的指標のみ)
- **H2**: 「中堅が枯渇」「離脱」表現は OK、「能力不足で残れない」NG
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] role hierarchy 確認: `src/utils/role_groups.py` の昇進階層 (動画/原画/作監/監督)
- [ ] `feat_career_annual` テーブル row count > 0
- [ ] `pixi run test` baseline pass

---

## Method 設計

### 役職進行年数

```
progression_years[i, role_pair] =
    first_credit_year(i, role_to) - first_credit_year(i, role_from)
```

cohort: debut_year (5 年区切り) で集計。

### Kaplan-Meier survival curve (役職滞留時間)

- イベント: 次の上位役職への進行
- 検閲: データ末端で未進行 / クレジット消失
- `lifelines.KaplanMeierFitter`

### スタジオ別パイプライン詰まり

```
studio_blockage_score[s] =
    median(progression_years for persons primarily affiliated with s)
    - industry_median(progression_years)
```

正値 = 詰まり、負値 = 早期昇進。CI は bootstrap。

### method gate

- KM curve に 95% CI
- スタジオ別 score に bootstrap CI
- cohort 比較に Mann-Whitney U

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o2_mid_management.py` | `O2MidManagementReport(BaseReport)` |
| `src/analysis/career/role_progression.py` | `compute_progression_years()` / `km_role_tenure()` (再利用可能関数) |
| `tests/reports/test_o2_mid_management.py` | smoke + lint_vocab + method gate |

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | 末尾に O2 エントリ追加 |
| HR brief 該当ファイル | 新 section `pipeline_blockage` 追加 |
| Policy brief 該当ファイル | 新 section `mid_career_attrition` 追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/utils/role_groups.py` | 階層定義不変 |
| `scripts/report_generators/reports/base.py` | 共通変更は別タスク |

---

## Steps

### Step 1: progression_years 計算関数

`src/analysis/career/role_progression.py` 新規。

```python
def compute_progression_years(conn, role_from: str, role_to: str) -> DataFrame:
    """各 person の role_from → role_to 進行年数を返す。
    未進行は NaN (検閲扱い)。"""
```

### Step 2: KM curve 関数 + cohort 比較

- `km_role_tenure(progression_df, cohort_col='cohort_5y')` で KM fit
- log-rank test for cohort comparison

### Step 3: スタジオ別 blockage score

- `compute_studio_blockage(conn) -> DataFrame[studio_id, blockage_score, ci_low, ci_high]`
- bootstrap 1000 samples

### Step 4: レポート HTML

- 役職滞留 KM curve (cohort 層別)
- スタジオ別 blockage ヒートマップ (上位/下位 20 studios)
- cohort 昇進ファネル (動画 → 原画 → 作監 → 監督)
- Findings / Interpretation 分離

### Step 5: HR brief + Policy brief 組込み + テスト

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o2_mid_management.py

# 3. レポート生成
pixi run python -m scripts.generate_reports --only o2_mid_management

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o2_mid_management.py   # 0 件
rg '\b(ability|skill|talent|competence|capability)\b' scripts/report_generators/reports/o2_mid_management.py   # 0 件
```

---

## Stop-if conditions

- [ ] `lifelines` 依存未追加かつ追加不可 → 自前 KM 実装は範囲外、Stop
- [ ] role hierarchy が credits に十分マップされていない (各役職 row count < 1000)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm scripts/report_generators/reports/o2_mid_management.py
rm src/analysis/career/role_progression.py
rm tests/reports/test_o2_mid_management.py
git checkout docs/REPORT_INVENTORY.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] HR + Policy brief に section 組込み
- [ ] DONE: `15_extension_reports/03_o2_mid_management`

---

## ステークホルダー

- スタジオ HR、厚労省雇用環境均等局、労組、業界団体 (アニメ制作者連盟)
