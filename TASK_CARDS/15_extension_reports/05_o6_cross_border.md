# Task: O6 cross_border_collaboration (国際共同制作)

**ID**: `15_extension_reports/05_o6_cross_border`
**Priority**: 🟡
**Estimated changes**: 約 +400 / -0 lines, 3 files
**Requires senior judgment**: yes (community detection 安定性)
**Blocks**: なし
**Blocked by**: `04_o4_foreign_talent` (`nationality_resolver` 共有)

---

## Goal

国境跨ぎコラボエッジを抽出し、国別人物中心性 / 共同制作作品クラスタリング / null model 比較を Business brief + 海外パートナー向け extract に組込む。

---

## Hard constraints

- **H1**: `anime.score` を国際協業評価に使わない
- **H2**: 「日本主導」「下請け」表現は事実記述のみ可、価値判断 NG
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] **依存カード**: `04_o4_foreign_talent` 完了 (`nationality_resolver` 利用)
- [ ] SILVER `studios.country` 列 row coverage 確認
- [ ] `pixi run test` baseline pass

---

## Method 設計

### 国境跨ぎコラボエッジ抽出

```
cross_border_edge = {
    edge: (person_a, person_b),
    same_anime: True,
    person_a.country != person_b.country
}
```

studio 単位:
```
cross_border_studio_edge = (studio_a, studio_b) where studio_a.country != studio_b.country
```

### 国別人物中心性

- weighted PageRank where edge weight scaled by international participation count
- 国別 hub person の top 10 を抽出

### 共同制作作品クラスタリング

- bipartite anime × person network → community detection (`networkx.community.louvain_communities`)
- 各クラスタの dominant country pair / role 分布

### null model 比較

- 同役職分布で random pairing 1000 回
- 観測 cross_border_edge 数 / community 構造との比較

### method gate

- PageRank に boot strap CI
- community detection に modularity score
- null distribution percentile 注記

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o6_cross_border.py` | `O6CrossBorderReport(BaseReport)` |
| `src/analysis/network/cross_border.py` | `extract_cross_border_edges()` / `country_centrality()` / `cross_border_community()` |
| `tests/reports/test_o6_cross_border.py` | smoke + lint_vocab + method gate |

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | 末尾に O6 エントリ追加 |
| Business brief | 新 section `international_collaboration_network` 追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/network/__init__.py` 既存 | 追加のみ、既存関数不変 |

---

## Steps

### Step 1: studio.country カバレッジ確認 + nationality_resolver 取得

```bash
pixi run python -c "
import duckdb
c = duckdb.connect('result/silver.duckdb', read_only=True)
print(c.execute('SELECT country, COUNT(*) FROM studios GROUP BY 1 ORDER BY 2 DESC').fetchall())
"
```

### Step 2: cross_border edge 抽出

`extract_cross_border_edges(conn) -> DataFrame[a_id, b_id, anime_id, country_a, country_b]`

### Step 3: PageRank + community detection

- `networkx` グラフ構築 (weight = 国際協業重み)
- `louvain_communities` または `girvan_newman` で community
- per-community: modularity, dominant country pair, member count

### Step 4: null model

- shuffle person.country labels (within role distribution) 1000 回
- 観測値 percentile

### Step 5: レポート HTML

- 国際協業ネットワーク (force-directed graph、国別カラー)
- 国別 hub person 表 (top 10 / country)
- クラスタ概要 (modularity / dominant pair / role 分布)
- null vs observed annotation

### Step 6: Business brief 組込み + テスト

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o6_cross_border.py

# 3. レポート生成
pixi run python -m scripts.generate_reports --only o6_cross_border

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o6_cross_border.py   # 0 件
```

---

## Stop-if conditions

- [ ] `04_o4_foreign_talent` 未完了 (`nationality_resolver` 不在)
- [ ] cross_border edge 数 < 1000 (分析可能データ不足)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm scripts/report_generators/reports/o6_cross_border.py
rm src/analysis/network/cross_border.py
rm tests/reports/test_o6_cross_border.py
git checkout docs/REPORT_INVENTORY.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] Business brief に section 組込み
- [ ] DONE: `15_extension_reports/05_o6_cross_border`

---

## ステークホルダー

- 仏 CNC、韓 KOCCA、日中合作スタジオ、文化庁国際交流課、JETRO
