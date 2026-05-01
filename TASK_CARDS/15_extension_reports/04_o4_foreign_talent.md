# Task: O4 foreign_talent_position (海外人材)

**ID**: `15_extension_reports/04_o4_foreign_talent`
**Priority**: 🟡
**Estimated changes**: 約 +400 / -0 lines, 3 files
**Requires senior judgment**: yes (limited mobility bias 注記)
**Blocks**: `05_o6_cross_border` (国籍識別ロジック共有)
**Blocked by**: `14_silver_extend/01_anilist_extend` + 関連 SILVER 列 (`name_zh`, `name_ko`, `country_of_origin`) 充実

---

## Goal

国籍別 person FE 分布、国籍 × 役職進行、studio FE 帰属パターンを可視化し、海外人材の業界内位置を Policy / Business brief に組込む。

---

## Hard constraints

- **H1**: `anime.score` を国籍分析に使わない
- **H2**: 「海外人材は劣る」「日本人より能力低い」NG。「役職進行が遅延しているか」「FE 分布の差」のみ
- **H3**: entity_resolution ロジック不変
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] **依存カード**: `14_silver_extend/01_anilist_extend` 完了確認
- [ ] SILVER `persons.country_of_origin` row coverage 確認 (> 30% 推奨)
- [ ] AKM theta_i 結果 (`feat_person_scores` 等) 存在確認
- [ ] `pixi run test` baseline pass

---

## Method 設計

### 国籍判定戦略

優先順位:
1. `persons.country_of_origin` (AniList GraphQL `homeTown` 由来)
2. `name_zh` / `name_ko` 存在 → 推定 (false positive リスク注記)
3. fallback: 「不明」カテゴリ

### 国籍別 person FE 分布

- AKM 既存結果から `theta_i` を国籍別に集計
- violin plot + Mann-Whitney U で分布差検定
- limited mobility bias 注記 (Andrews et al. 2008): 海外人材はスタジオ間移動が少ない → FE 推定が劣化

### 国籍 × 役職進行

- O2 の `progression_years` 関数を流用
- 国籍別 KM curve + log-rank test

### studio FE 帰属パターン

- 海外人材を多く起用する studio top 20 を抽出
- studio FE (`psi_j`) と海外人材比率の散布図 + 相関

### method gate

- 全プロット 95% CI
- limited mobility bias 警告 (method note 必須)
- 国籍カテゴリ別 sample size 表示

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o4_foreign_talent.py` | `O4ForeignTalentReport(BaseReport)` |
| `src/analysis/network/nationality_resolver.py` | `resolve_nationality()` (CN/KR/SE Asia 抽出ロジック) |
| `tests/reports/test_o4_foreign_talent.py` | smoke + lint_vocab + method gate |

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | 末尾に O4 エントリ追加 |
| Policy / Business brief | 新 section `foreign_talent_integration` 追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/structural_estimation.py` | AKM ロジック不変 |
| `src/analysis/entity_resolution.py` | H3 |

---

## Steps

### Step 1: SILVER 国籍カバレッジ確認

```bash
pixi run python -c "
import duckdb
c = duckdb.connect('result/silver.duckdb', read_only=True)
print(c.execute('SELECT country_of_origin, COUNT(*) FROM persons GROUP BY 1 ORDER BY 2 DESC LIMIT 20').fetchall())
print(c.execute('SELECT COUNT(*) FILTER (WHERE name_zh IS NOT NULL) AS zh, COUNT(*) FILTER (WHERE name_ko IS NOT NULL) AS ko, COUNT(*) AS total FROM persons').fetchall())
"
```

カバレッジ < 30% なら本カード Stop、SILVER enrichment 先行。

### Step 2: nationality_resolver 実装

優先順位ロジック + 推定信頼度フィールド:

```python
def resolve_nationality(person_row) -> tuple[str, str]:
    """returns (country_code, confidence: 'high'|'medium'|'low')"""
```

### Step 3: FE 分布 + 役職進行 + studio FE

- 既存 AKM 結果テーブル読込
- O2 関数流用 (Card 03 完了後なら import 可、未完なら本カードで一時実装)

### Step 4: レポート HTML

- 国籍別 FE 分布 violin (CI 付)
- 役職進行 cohort curve (国籍層別)
- studio × 海外人材比率 散布図
- limited mobility bias 警告 banner

### Step 5: brief 組込み + テスト

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o4_foreign_talent.py

# 3. レポート生成
pixi run python -m scripts.generate_reports --only o4_foreign_talent

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o4_foreign_talent.py   # 0 件
rg '\b(ability|skill|talent|competence|capability)\b' scripts/report_generators/reports/o4_foreign_talent.py   # 0 件
```

---

## Stop-if conditions

- [ ] SILVER `country_of_origin` カバレッジ < 30% → Card 14 完了待ち
- [ ] AKM 結果テーブル存在せず → Phase 5 (core_scoring) 先行
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm scripts/report_generators/reports/o4_foreign_talent.py
rm src/analysis/network/nationality_resolver.py
rm tests/reports/test_o4_foreign_talent.py
git checkout docs/REPORT_INVENTORY.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] Policy + Business brief に section 組込み
- [ ] DONE: `15_extension_reports/04_o4_foreign_talent`

---

## ステークホルダー / 参考文献

- ステークホルダー: 経産省コンテンツ産業課、文化庁、KOCCA、海外スタジオ (Studio Mir / Mir Animation / Toei Phils 等)
- 参考: Andrews et al. (2008) "limited mobility bias"、Borjas (2014) immigrant earnings
