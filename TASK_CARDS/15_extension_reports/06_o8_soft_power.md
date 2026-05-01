# Task: O8 soft_power_index (ソフトパワー指標)

**ID**: `15_extension_reports/06_o8_soft_power`
**Priority**: 🟢
**Estimated changes**: 約 +500 / -0 lines, 4 files (新規 brief 含む可能性)
**Requires senior judgment**: yes (海外配信メタ取得経路)
**Blocks**: なし
**Blocked by**: 海外配信メタデータ取得経路整備 (Card 16 新ソース or 公式提供契約)

---

## Goal

海外配信プラットフォーム掲載作品 + 国際賞受賞 + 海外売上比率 × 関与人材分布を可視化し、内閣府クールジャパン向け独立 brief に組込む (新 audience 候補)。

---

## Hard constraints

- **H1**: `anime.score` を soft power 指標に**絶対**入れない (lint pass 必須、本カード重点)
- **H2**: 「日本のアニメは優秀」NG。「国際展開の構造的測定」「関与人材の地理分布」のみ
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] **依存**: 海外配信メタデータ source 確定 (Netflix/Crunchyroll メタ scraper or 公式提供)
- [ ] 国際賞データ存在確認 (Annecy / Anima Mundi / Asia Pacific Screen Awards)
- [ ] `pixi run test` baseline pass

---

## Method 設計

### データ要件 (本カード Stop-if 条件)

| データ | 取得経路候補 |
|--------|------------|
| Netflix/Crunchyroll メタ (作品 → 配信国) | 公式 API (要契約) / scraper (ToS 確認) / 業界レポート |
| 国際賞受賞 | Annecy 公式サイト / Anima Mundi / IMDb 賞ページ |
| 海外売上比率 | 業界統計 (日本動画協会 / VIPO / JETRO) |

### 指標構成

```
soft_power_index[anime] =
    f(配信カバー国数, 受賞重み, 海外売上比率)
```

ただし `f` は **重み固定** (method gate で宣言)、`anime.score` 経由禁止。

### 関与人材分布

- 国際展開作品 vs 国内専作品で関与人材のネットワーク位置 (theta_i / PageRank) 分布比較
- 国際展開作品に多く関与する person を上位 50 抽出

### method gate

- 配信カバー国数 / 受賞重みの算出根拠を method note に明記
- `anime.score` 不参入 lint
- bootstrap CI (関与人材比較)

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o8_soft_power.py` | `O8SoftPowerReport(BaseReport)` |
| `src/etl/silver_loaders/international_distribution.py` | (海外配信データ source 確定後) BRONZE → SILVER |
| `tests/reports/test_o8_soft_power.py` | smoke + lint_vocab + method gate |

新 audience brief 採否は `15_extension_reports/x_cross_cutting` で決定。

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | 末尾に O8 エントリ追加 |
| `src/db/schema.py` | (海外配信データ取得後) `anime_international_distribution` 新表追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/scoring/*` | スコアリング核心ロジック不変、本レポートは可視化のみ |

---

## Steps

### Step 1: 海外配信メタデータ source 決定

候補:
1. JustWatch API (商用) — 国別配信プラットフォーム情報あり
2. Netflix Top 10 (公開) — 限定的
3. AniList anime extras `external_links_json` (`14_silver_extend/01` 完了後利用可) — 配信プラットフォームリンクあり

`docs/method_notes/o8_data_sources.md` に source 比較を記録。決定後 BRONZE 取得設計は別カード化 (Card 16 候補)。

### Step 2: 国際賞データ取得

- Annecy 公式 (1960-) / Asia Pacific Screen Awards (2007-) / Anima Mundi (1993-)
- BRONZE `awards_international` テーブル新設
- 受賞重み: 賞ごとに固定 (Annecy Cristal / Special Award の階層)

### Step 3: soft_power_index 算出関数

```python
def compute_soft_power_index(conn) -> DataFrame[anime_id, index, components_json, ci_low, ci_high]:
    """重み固定式での集計。anime.score 不使用。"""
```

### Step 4: 関与人材分布

- 国際展開 anime に関与した person の theta_i / PageRank 分布 vs 国内専
- Mann-Whitney U + 効果量 r

### Step 5: レポート HTML

- 国際展開 vs 関与クリエイター散布図
- 年次トレンド (1980 → 2025)
- 受賞重み内訳 sunburst

### Step 6: 新 audience 検討 (横断タスク連携)

`x_cross_cutting` で「クールジャパン向け独立 brief」採否決定。当面は Business brief に section 組込み。

### Step 7: テスト

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o8_soft_power.py

# 3. レポート生成 (海外配信データ整備後)
pixi run python -m scripts.generate_reports --only o8_soft_power

# 4. invariant (本カード最重要)
rg 'anime\.score\b' scripts/report_generators/reports/o8_soft_power.py   # 0 件
rg 'display_score|score' scripts/report_generators/reports/o8_soft_power.py | rg -v '^.*#'   # 0 件
```

---

## Stop-if conditions

- [ ] 海外配信メタ source 確定不可 → 本カード Stop、Card 16 で新ソース整備優先
- [ ] 国際賞データ scraper 構築不可 → 賞データ抜きで Tier1 (配信のみ) で着地
- [ ] `anime.score` が lint で検出 → 即修正

---

## Rollback

```bash
rm scripts/report_generators/reports/o8_soft_power.py
rm src/etl/silver_loaders/international_distribution.py
rm tests/reports/test_o8_soft_power.py
git checkout docs/REPORT_INVENTORY.md src/db/schema.py
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] Business brief or 新 audience brief に section 組込み
- [ ] DONE: `15_extension_reports/06_o8_soft_power`

---

## ステークホルダー

- 内閣府クールジャパン戦略推進会議、経産省コンテンツ産業課、JETRO、日本動画協会
