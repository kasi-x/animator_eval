# Task: anime_relations Union-Find クラスタを SILVER 列化

**ID**: `21_silver_enrichment/04_relations_cluster_id`
**Priority**: 🟡
**Estimated changes**: 約 +200 / -20 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

`anime_relations` の SEQUEL/PREQUEL/PARENT/SIDE_STORY エッジを Union-Find でクラスタリングし、`anime` 表に `series_cluster_id` 列を追加。15/01 O3 で計算済のロジックを SILVER post-hoc ETL として SILVER に保存し、レポート間で再利用可能にする。

---

## Hard constraints

- **H3**: entity_resolution 不変、cluster ID は新規列のみ
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- 並列衝突回避: schema.py 末尾の `anime` 拡張セクションを末尾追記のみ (Card 03 と衝突しないよう注意)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] anime_relations row > 25,000 確認
- [ ] 15/01 O3 の cluster ロジック存在: `grep -rn "Union" scripts/report_generators/reports/o3_ip_dependency.py src/analysis/graph/`
- [ ] `pixi run test` baseline pass

---

## 設計

### Union-Find クラスタリング

```python
class UnionFind:
    def __init__(self):
        self.parent = {}
    def find(self, x):
        while self.parent.get(x, x) != x:
            self.parent[x] = self.parent.get(self.parent[x], self.parent[x])
            x = self.parent[x]
        return x
    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[rx] = ry
```

### 対象 relation_type

- SEQUEL / PREQUEL / PARENT / SIDE_STORY / SUMMARY / ALTERNATIVE / FULL_STORY

= 同一 IP / シリーズと考えられる関係。SPIN_OFF / CHARACTER は別シリーズの可能性 → 除外。

### cluster_id 生成

各 anime に対し:
- 連結成分内で **最若 anime_id** (lex min) を `series_cluster_id` とする
- 連結なし anime は自身の id を cluster_id とする

### SILVER 列追加

```sql
ALTER TABLE anime ADD COLUMN IF NOT EXISTS series_cluster_id VARCHAR;
CREATE INDEX IF NOT EXISTS idx_anime_series_cluster ON anime(series_cluster_id);
```

post-hoc ETL で全 anime に backfill。

---

## Files to create / modify

| File | 変更内容 |
|------|---------|
| `src/etl/cluster/__init__.py` | パッケージ init |
| `src/etl/cluster/series_cluster.py` | `compute_clusters(conn) -> dict[anime_id, cluster_id]` + `backfill(conn) -> int` |
| `tests/test_etl/test_series_cluster.py` | Union-Find ロジック検証 |
| `src/db/schema.py` | `anime.series_cluster_id` 列追加 (末尾) |
| `scripts/report_generators/reports/o3_ip_dependency.py` | (任意) 既存 cluster 関数を `src/etl/cluster/series_cluster.py` から import に変更、重複削減 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| `src/etl/silver_loaders/*` | post-hoc ETL なので既存 loader 不変 |

---

## Steps

### Step 1: 既存 cluster ロジック確認

```bash
grep -rn "UnionFind\|union_find\|series_cluster" src/ scripts/ | head -10
```

15/01 O3 (`o3_ip_dependency.py`) と既存 `src/analysis/graph/synergy_score.py` を確認。

### Step 2: 共通モジュール化

`src/etl/cluster/series_cluster.py` に Union-Find + cluster 計算関数。

```python
def compute_clusters(conn) -> dict[str, str]:
    """Returns {anime_id: cluster_id}.
    cluster_id = lex min anime_id in connected component."""
```

### Step 3: schema.py 末尾追記

```sql
ALTER TABLE anime ADD COLUMN IF NOT EXISTS series_cluster_id VARCHAR;
```

### Step 4: backfill

```python
def backfill(conn) -> int:
    """全 anime に series_cluster_id を書込。idempotent。
    Returns: 更新行数。"""
    clusters = compute_clusters(conn)
    # bulk UPDATE via temp table
```

CLI entry point: `pixi run python -m src.etl.cluster.series_cluster backfill`

### Step 5: テスト

合成 anime_relations (1-2-3 の SEQUEL chain、4-5 の独立) でクラスタ検証。

### Step 6: 実行 + 検証

```bash
pixi run python -m src.etl.cluster.series_cluster backfill
duckdb result/silver.duckdb -c "
SELECT series_cluster_id, COUNT(*) AS members
FROM anime
WHERE series_cluster_id IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY members DESC
LIMIT 20
"
```

主要シリーズ (例: ガンダム / ジョジョ / プリキュア) のクラスタが大きいこと確認。

### Step 7: O3 レポート refactor (任意)

`o3_ip_dependency.py` の cluster 関数を `src/etl/cluster/series_cluster.compute_clusters` に置換。テスト pass 維持。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_series_cluster.py
duckdb result/silver.duckdb -c "
SELECT 
  COUNT(*) FILTER (WHERE series_cluster_id IS NOT NULL) AS with_cluster,
  COUNT(DISTINCT series_cluster_id) AS distinct_clusters,
  MAX(member_count) AS largest_cluster
FROM (
  SELECT series_cluster_id, COUNT(*) AS member_count
  FROM anime WHERE series_cluster_id IS NOT NULL GROUP BY 1
)
"
```

期待: 全 anime に cluster_id、distinct_clusters < anime 行数 (連結成分多数)、largest_cluster > 50 (主要 IP)。

---

## Stop-if conditions

- [ ] anime_relations が relation_type 列を持たない
- [ ] cluster サイズが 1 個に収束 (Union-Find バグ)
- [ ] 既存テスト破壊 (新規)

---

## Rollback

```bash
git checkout src/db/schema.py scripts/report_generators/reports/o3_ip_dependency.py
rm -rf src/etl/cluster/
rm tests/test_etl/test_series_cluster.py
duckdb result/silver.duckdb -c "ALTER TABLE anime DROP COLUMN series_cluster_id"
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] anime 全行 cluster_id 設定
- [ ] 主要シリーズが正しく集約 (sample 検証)
- [ ] DONE: `21_silver_enrichment/04_relations_cluster_id`
