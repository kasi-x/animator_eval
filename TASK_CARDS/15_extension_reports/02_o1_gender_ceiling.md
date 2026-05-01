# Task: O1 gender_ceiling_analysis (ジェンダー天井効果)

**ID**: `15_extension_reports/02_o1_gender_ceiling`
**Priority**: 🟠
**Estimated changes**: 約 +500 / -0 lines, 4 files
**Requires senior judgment**: yes (Cox 回帰 / DID 制度イベント特定)
**Blocks**: なし
**Blocked by**: gender enrichment scraper (Stop-if 確認済 2026-05-02: null 率 95.4% / 259,684 中 247,038)。`TODO.md §15` 参照

---

## Goal

役職進行ハザード率の性別差を Cox 回帰で推定し、共クレジット ego-network の性別構成を null model と比較する。Policy brief に組込む。

---

## Hard constraints

- **H1**: `anime.score` を性別差分析に使わない (構造的指標のみ)
- **H2**: 「女性は劣る」「能力差」等の framing 厳禁。「役職進行ハザード率の差」「ネットワーク位置の差」のみ
- **H3**: entity_resolution ロジック不変
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] **gender カバレッジ確認**: `SELECT gender, COUNT(*) FROM persons GROUP BY 1` で `M/F/NB/null` 分布把握。null 比率 > 70% なら Stop-if
- [ ] AKM 結果テーブル (`feat_person_scores` または同等) 存在確認
- [ ] `pixi run test` baseline pass

---

## Method 設計

### 4 method gate

1. **Cox 回帰** (役職進行ハザード)
   - 進行イベント: 動画 → 原画 / 原画 → 作画監督 / 作画監督 → 監督
   - 共変量: gender, cohort (debut_year), studio_fixed_effects
   - Output: HR (gender) と 95% CI

2. **Mann-Whitney U** (同コホート内昇進タイミング差)
   - 同年デビュー組内で初昇進までの年数を性別比較
   - non-parametric、効果量 r

3. **ego-network 性別構成 vs null model**
   - 各 person の共クレジット ego-net (1-hop) で `same_gender_share`
   - null: 同役職分布で random pairing 1000 回
   - 観測 share が null distribution の何 percentile か

4. **DID** (制度変更前後の昇進率変化)
   - 制度イベント要調査 (例: 男女雇用機会均等法改正、業界団体ガイドライン)
   - 業界外 control group が無いため、cohort × gender の treatment effect として推定

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o1_gender_ceiling.py` | `O1GenderCeilingReport(BaseReport)` |
| `src/analysis/causal/gender_progression.py` | `cox_progression_hazard()` / `did_policy_event()` (再利用可能関数) |
| `tests/reports/test_o1_gender_ceiling.py` | smoke + lint_vocab + method gate |

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | 末尾に O1 エントリ追加 |
| Policy brief 該当ファイル | 新 section `gender_progression_disparity` 追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/structural_estimation.py` | AKM ロジック不変 |
| `scripts/report_generators/reports/base.py` | 共通変更は別タスク |

---

## Steps

### Step 1: gender カバレッジ確認 + 制度イベント調査

```bash
pixi run python -c "
import duckdb
c = duckdb.connect('result/silver.duckdb', read_only=True)
print(c.execute('SELECT gender, COUNT(*) FROM persons GROUP BY 1').fetchall())
"
```

- 制度イベント候補: 1985 男女雇用機会均等法、1999 改正、2007 改正、業界団体宣言など。`docs/method_notes/o1_did_events.md` に列挙

### Step 2: Cox 回帰実装 (`gender_progression.py`)

- `lifelines.CoxPHFitter` 使用、依存追加要なら `pixi.toml` 編集
- 進行イベント定義は `src/utils/role_groups.py` の階層を使う

### Step 3: Mann-Whitney U + ego-network 性別構成

- `scipy.stats.mannwhitneyu` (既存依存)
- ego-network は `networkx` で 1-hop 抽出、null model は permutation

### Step 4: レポート HTML

- 役職別性別比 violin
- cohort 昇進 KM curve (gender 層別)
- ego-net 性別構成 sankey
- DID coefficient plot (制度イベント別)

### Step 5: Policy brief 組込み + テスト

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o1_gender_ceiling.py

# 3. レポート生成
pixi run python -m scripts.generate_reports --only o1_gender_ceiling

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o1_gender_ceiling.py   # 0 件
rg '\b(ability|skill|talent|competence|capability)\b' scripts/report_generators/reports/o1_gender_ceiling.py   # 0 件
rg '能力|実力|優秀|劣る' scripts/report_generators/reports/o1_gender_ceiling.py   # 0 件
```

---

## Stop-if conditions

- [ ] gender null 率 > 70% → 別途 gender enrichment スクレイパー要、本カード Stop
- [ ] DID 制度イベントが特定できない → Cox + ego-network のみで実装、DID は次回 PR に分離
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm scripts/report_generators/reports/o1_gender_ceiling.py
rm src/analysis/causal/gender_progression.py
rm tests/reports/test_o1_gender_ceiling.py
git checkout docs/REPORT_INVENTORY.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] Policy brief に section 組込み
- [ ] DONE: `15_extension_reports/02_o1_gender_ceiling`

---

## ステークホルダー / 参考文献

- ステークホルダー: 内閣府男女共同参画局、厚労省雇用環境均等局、労組
- 参考: Lutter (2015 *American Sociological Review*)、Card-Heining-Kline 系列、Bertrand (2018)
