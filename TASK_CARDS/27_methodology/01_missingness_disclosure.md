# Task: missingness honest 開示 (source 別 coverage 行列)

**ID**: `27_methodology/01_missingness_disclosure`
**Priority**: 🟠
**Estimated changes**: +300 / -0 lines, 3 files
**Requires senior judgment**: medium
**Blocks**: 全 report の信頼性
**Blocked by**: なし

---

## Goal

source × role × year の credit coverage 行列を生成し、Findings 層に必須開示。
特に「動画」「第二原画」「仕上げ」等の under-credited role の bias を可視化。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Publication** | 査読耐性 (limitation section) |
| **政策** | データに基づく主張の honest reporting |
| **Business** | 顧客 (スタジオ HR) からの「うちのデータ抜け過ぎ」反論への先回り |

---

## Hard constraints

- 全レポート Findings 層に「coverage caveat block」挿入 (テンプレート化)
- 「データ不足のため過小推定」と明記、推定値の補正は行わない (補正したら source 透明性失う)

---

## Method

### Coverage matrix

```
coverage[source, role_group, year] =
    n_credits / expected_n_credits

expected = ANN を upper bound (最も網羅的) として参考値
```

### Output

- HTML / parquet で coverage 行列 (`mart.meta_coverage_matrix`)
- 各 report の冒頭に「本レポートが依拠する source の role × year coverage」block 自動挿入

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/quality/coverage_matrix.py` | 行列生成 |
| `scripts/report_generators/_coverage_block.py` | 共通 block (base.py から呼出) |
| `tests/analysis/quality/test_coverage_matrix.py` | toy データ |

---

## Pre-conditions

- [ ] 全 source の credits 統合完了
- [ ] role_groups single source (`src/utils/role_groups.py`)

---

## Verification

```bash
pixi run test-scoped tests/analysis/quality/test_coverage_matrix.py
# 全 v3 report に coverage block が含まれるか
rg 'coverage_block' scripts/report_generators/reports/ | wc -l
```
