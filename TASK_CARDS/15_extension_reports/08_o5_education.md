# Task: O5 education_outcome_tracking (教育機関キャリア追跡)

**ID**: `15_extension_reports/08_o5_education`
**Priority**: 🟢 (最も上流の前提整備が必要、推奨順 8/8)
**Estimated changes**: 約 +500 / -0 lines, 4 files (上流 scraper 設計含む)
**Requires senior judgment**: yes (selection bias 注記 / 教育機関データ source 設計)
**Blocks**: なし
**Blocked by**: 出身校データ取得経路確定 (現状 SILVER 未保有)

---

## Goal

出身校 × クレジット獲得時期 × 役職進行 × 5 年離脱率を追跡し、propensity score で入学時の不可観測能力差を control する。教育機関向け独立 brief 採否を検討する。

---

## Hard constraints

- **H1**: `anime.score` を出身校評価に使わない
- **H2**: 「○○学校出身者は能力高い」NG。「クレジット獲得時期の差」「ネットワーク参入経路」のみ
- **H3**: entity_resolution ロジック不変
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] **依存**: 出身校データ取得経路確定 (本カード冒頭で決定)
- [ ] AKM 結果テーブル存在
- [ ] `pixi run test` baseline pass

---

## データ取得経路 (本カードの最大難所)

候補:

| 経路 | 取得性 | 倫理 |
|------|------|------|
| アニメ専門学校公式提供 | 要交渉、各校別 | 入学許諾範囲内なら OK |
| 学校公式 alumni ページ scraper | 公開部分のみ可 | ToS 確認 |
| 業界誌 (アニメージュ等) インタビュー記事 NLP | 半自動、精度低 | 公開情報利用 |
| LinkedIn / Twitter プロフィール | OK だが不完全 | API ToS、scraper 不可 |
| 直接アンケート (creators 自身) | 高品質、低カバレッジ | 同意必要 |

`docs/method_notes/o5_education_data_sources.md` に source 比較記録、ユーザ承認後に着手。

---

## Method 設計

### コホート軌跡

- 出身校別に同年卒 cohort で集計
- 初クレジット獲得までの median years
- 5 年生存率 (継続クレジット保有率)

### propensity score matching

- 入学時の不可観測能力差を間接 control
- 共変量: 卒業年、年齢、初クレジット時の役職、studio 規模
- IPW (inverse propensity weighting) or NN matching

### 役職進行

- O2 の `progression_years` 関数流用 (Card 03 完了後)
- 出身校層別 KM curve

### method gate

- selection bias 警告 banner (入学時能力差は不可観測)
- propensity score balance 表示 (matched cohort)
- bootstrap CI (5 年離脱率)

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o5_education.py` | `O5EducationOutcomeReport(BaseReport)` |
| `src/etl/silver_loaders/education_history.py` | (データ source 確定後) BRONZE → SILVER `education_history` 表 |
| `src/scrapers/education_scraper.py` | (公開 alumni ページ scraper、ToS 確認後) |
| `tests/reports/test_o5_education.py` | smoke + lint_vocab + method gate |
| `docs/method_notes/o5_education_data_sources.md` | データ source 比較 + 倫理記録 |

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | `education_history` 表追加 (person_id, school_id, enroll_year, graduate_year, source) |
| `docs/REPORT_INVENTORY.md` | 末尾に O5 エントリ追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| `src/analysis/structural_estimation.py` | AKM 不変 |

---

## Steps

### Step 1: データ source 確定 (本カード Stop-if 条件)

`docs/method_notes/o5_education_data_sources.md` に source 比較を記録、ユーザ承認得る。

承認なし → 本カード Stop。

### Step 2: schema 変更 + scraper 実装

- `education_history` テーブル DDL 追加 + Atlas migration
- 承認された source の scraper を `src/scrapers/education_scraper.py` に実装

### Step 3: silver_loaders/education_history.py

BRONZE → SILVER 統合 loader (Card 14 と同様パターン)。

### Step 4: propensity score matching

- `statsmodels` or `sklearn` で logistic regression による propensity score
- IPW 適用、balance 確認 (Standardized Mean Difference)

### Step 5: レポート HTML

- 学校別 cohort 軌跡 (timeline、初クレジット年中央値)
- 5 年離脱率比較 (matched cohort)
- 役職進行 KM curve (出身校層別)
- selection bias warning banner

### Step 6: 教育機関向け独立 brief 検討 (横断タスク連携)

`x_cross_cutting` で「教育機関 brief」採否決定。

### Step 7: テスト

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o5_education.py

# 3. レポート生成 (出身校データ整備後)
pixi run python -m scripts.generate_reports --only o5_education

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o5_education.py   # 0 件
rg '\b(ability|skill|talent|competence|capability)\b' scripts/report_generators/reports/o5_education.py   # 0 件
```

---

## Stop-if conditions

- [ ] データ source 確定不可 → 本カード Stop
- [ ] 出身校 row coverage < 5% (分析可能サンプル不足) → Stop
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
git checkout src/db/schema.py docs/REPORT_INVENTORY.md
rm -rf src/etl/silver_loaders/education_history.py
rm src/scrapers/education_scraper.py
rm scripts/report_generators/reports/o5_education.py
rm tests/reports/test_o5_education.py
rm docs/method_notes/o5_education_data_sources.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] DONE: `15_extension_reports/08_o5_education`

---

## ステークホルダー / 参考文献

- ステークホルダー: アニメ専門学校 (代々木アニメーション学院 / アミューズメントメディア総合学院 / 大阪アニメーションスクール他)、大学アニメ学科 (京都精華 / 東京工芸 / 武蔵野美術他)、文科省高等教育局
- 参考: Dale & Krueger (2002, 2014) college effect with propensity matching、Card-Krueger (1992) school quality
