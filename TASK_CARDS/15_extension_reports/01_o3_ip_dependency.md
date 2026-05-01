# Task: O3 ip_person_dependency (IP 人的依存リスク)

**ID**: `15_extension_reports/01_o3_ip_dependency`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -0 lines, 3 files (新規 report / 新規 test / REPORT_INVENTORY 追記)
**Requires senior judgment**: yes (counterfactual 設計)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

シリーズ単位で個人の貢献集中度を可視化し、key person 離脱時の counterfactual 影響予測を投資家向け Business brief に組込む。

---

## Hard constraints

(`_hard_constraints.md` 事前読込必須)

- **H1**: `anime.score` を寄与計算に使わない。寄与重みは `production_scale` (staff_count × episodes × duration_mult) のみ
- **H2**: 「key person」表現は許容、「優秀な人材」「優れた creator」は禁止
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] SILVER `anime` / `credits` / `persons` row count 確認
- [ ] シリーズ識別子の存在確認 (`relations_json` で SEQUEL/PREQUEL 集約 or `franchise_id` 列)
- [ ] `pixi run test` baseline pass

---

## Method 設計

### 寄与比率算出

```
contribution_share[i, series s] =
    Σ (role_weight × production_scale_credit) for credits of person i in series s
    / Σ (role_weight × production_scale_credit) for all credits in series s
```

- `role_weight`: 既存 `src/utils/role_groups.py` の重み (構造的)
- `production_scale`: 既存 AKM 結果変数と同一定義

### Counterfactual 下落

```
predicted_scale_without_i[s] = predicted_scale[s] - sum_of_credits_attributable_to_i
```

- person FE (`theta_i`) を AKM から引用、対象シリーズの remaining production_scale を再推定
- CI: bootstrap (1000 samples)

### Null model

- random removal baseline: 同シリーズで同役職分布のランダム person 群を 1000 回抜いて null 分布
- 観測下落幅が null distribution の何 percentile か報告

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o3_ip_dependency.py` | `O3IpDependencyReport(BaseReport)` |
| `tests/reports/test_o3_ip_dependency.py` | smoke + lint_vocab + method gate |

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | 末尾に O3 エントリ追加 (Business brief 組込み) |
| `scripts/report_generators/briefs/business_brief.py` (or 該当) | 新 section `key_person_concentration` 追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `scripts/report_generators/reports/base.py` | 共通変更は別タスク |
| `src/analysis/scoring/*` | 寄与計算は report 層で完結 |

---

## Steps

### Step 1: シリーズ識別子戦略確定

```bash
grep -rn "franchise_id\|series_id" src/db/schema.py
grep -rn "SEQUEL\|PREQUEL" src/etl/silver_loaders/ src/etl/integrate_duckdb.py
```

- `relations_json` から SEQUEL/PREQUEL クラスタリングして `series_cluster_id` を report 内で生成
- 単発作品はそれ自体を 1 シリーズとして扱う

### Step 2: contribution_share + counterfactual 関数

`o3_ip_dependency.py` に:
- `compute_series_contribution_shares(conn, series_id) -> DataFrame`
- `compute_counterfactual_drop(conn, series_id, person_id) -> tuple[float, tuple[float, float]]`  # (drop, CI)
- `compute_null_distribution(conn, series_id, n_iter=1000) -> ndarray`

### Step 3: レポート HTML 構成

- シリーズ別寄与 sunburst (上位 10 series)
- counterfactual 下落幅 forest plot (CI 付)
- null vs observed percentile annotation
- Findings / Interpretation 分離

### Step 4: Business brief 組込み

新 section `key_person_concentration_risk` を追加。1 brief = 4 section × 3 method gate ルール遵守。

### Step 5: テスト

- 合成 fixture (5 series × 30 person) で寄与比率検証
- lint_vocab 通過
- method note 含有確認
- `anime.score` SELECT 不在確認

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト (本カード)
pixi run test-scoped tests/reports/test_o3_ip_dependency.py

# 3. レポート生成 (実 SILVER で)
pixi run python -m scripts.generate_reports --only o3_ip_dependency

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o3_ip_dependency.py   # 0 件
rg '\b(ability|skill|talent|competence|capability)\b' scripts/report_generators/reports/o3_ip_dependency.py   # 0 件
```

---

## Stop-if conditions

- [ ] シリーズ識別子戦略が設計時点で確定できない (Step 1 で blocker)
- [ ] counterfactual の CI が異常に広い (bootstrap 設計見直し要)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm scripts/report_generators/reports/o3_ip_dependency.py
rm tests/reports/test_o3_ip_dependency.py
git checkout docs/REPORT_INVENTORY.md scripts/report_generators/briefs/
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] REPORT_INVENTORY 登録
- [ ] Business brief に section 組込み
- [ ] DONE: `15_extension_reports/01_o3_ip_dependency`

---

## ステークホルダー / 参考文献

- ステークホルダー: 制作委員会、出資者、配信プラットフォーム
- 参考: Page (2007) *The Difference*、Lazear (1986) firm-specific human capital
