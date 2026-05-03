# Task: anime_studios 覆い率 2.5% 問題の調査と修正

**ID**: `22_silver_coverage/01_anime_studios_coverage`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -50 lines, 4-6 files
**Requires senior judgment**: yes (silver_loaders 改修方針)
**Blocks**: AKM 精度向上 (現 connected set 52K のみ)
**Blocked by**: なし

---

## Goal

SILVER `anime` 562,191 行のうち、`anime_studios` 紐付けが存在するのは 14,069 行 (2.5%) のみという覆い率不足を調査・修正する。BRONZE 側に studio 情報がある分は SILVER に取り込まれるべき。

---

## Hard constraints

- **H1**: scoring 経路に score / popularity 流入禁止
- **H3**: entity_resolution ロジック不変
- **H4**: anime_studios の `source` 列維持 (20/04 で追加済)
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- silver.duckdb backup 必須

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 現状確認:
```bash
duckdb result/silver.duckdb -c "
SELECT 'anime' AS t, COUNT(*) FROM anime
UNION ALL SELECT 'anime_studios', COUNT(*) FROM anime_studios
UNION ALL SELECT 'studios', COUNT(*) FROM studios
UNION ALL SELECT 'anime with studios', COUNT(DISTINCT a.id)
  FROM anime a JOIN anime_studios x ON x.anime_id = a.id
"
```
- [ ] `pixi run test` baseline pass

---

## 調査戦略

### Step 1: source 別 coverage 計測

各 source (anilist / mal / ann / mediaarts / seesaawiki / keyframe / bangumi) で:
- BRONZE 側の anime 数
- BRONZE 側で studio 情報を含む anime 数
- SILVER `anime_studios` に取り込まれた anime 数 (source 別)
- 取込率

```bash
duckdb result/silver.duckdb -c "
SELECT source, COUNT(DISTINCT anime_id) FROM anime_studios GROUP BY 1 ORDER BY 2 DESC
"
```

### Step 2: BRONZE での studio 情報存在確認

- anilist: `anime.studios_main` / `studios_co` (JSON 配列)
- mal: `anime_studios` BRONZE 表
- ann: `company` BRONZE 表 (role='Animation Production' 等)
- mediaarts: `production_companies` BRONZE 表
- seesaawiki: `studios` / `gross_studios` BRONZE 表
- keyframe: `anime_studios` BRONZE 表
- bangumi: `subjects.relations.subject_relation` (動画制作 等)

### Step 3: 各 silver_loader の INSERT 経路 tracing

`grep -rn "anime_studios" src/etl/silver_loaders/`

各 loader が:
1. studio name → studio_id 解決ができているか
2. anime_id 形式が SILVER の `anime.id` と一致するか (`anilist:a123` 等)
3. INSERT OR IGNORE で silent fail していないか
4. studio name 正規化 (NFKC + 略称) で match 失敗していないか

### Step 4: 修正

判明した不備を loader 別に修正:
- studio name resolution の補強
- ID prefix 整合
- INSERT 条件の緩和 (NULL 許容しすぎていないか確認)

### Step 5: 再 ETL + 再計測

```bash
pixi run python -m src.etl.integrate_duckdb
duckdb result/silver.duckdb -c "
SELECT COUNT(DISTINCT anime_id) FROM anime_studios
"
```

期待: 14K → 100K+ (anime の半分以上が studio 紐付けあり)

---

## Files to investigate / modify

| File | 変更内容 |
|------|---------|
| `src/etl/silver_loaders/anilist.py` | `studios_main` / `studios_co` JSON parse → anime_studios INSERT 確認 |
| `src/etl/silver_loaders/mal.py` | anime_studios BRONZE → SILVER mapping 確認 |
| `src/etl/silver_loaders/ann.py` | company 表の role filter 確認 (Animation Production / Studio / Production 等) |
| `src/etl/silver_loaders/mediaarts.py` | production_companies → anime_studios mapping |
| `src/etl/silver_loaders/seesaawiki.py` | studios / anime_studios INSERT 確認 |
| `src/etl/silver_loaders/keyframe.py` | anime_studios INSERT 経路確認 |
| `src/etl/silver_loaders/bangumi.py` | subject_relations から studio 抽出経路 |
| `src/etl/audit/anime_studios_coverage.py` (新規) | source 別 coverage 計測ツール |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/io/silver_reader.py` | 21/01 で修正済、本タスクは loader 側 |
| `src/analysis/entity_resolution.py` | H3 |
| `src/analysis/scoring/akm.py` | scoring 不変 |

---

## Steps

### Step 1: 計測ツール作成

`src/etl/audit/anime_studios_coverage.py` 新規:
```python
def measure(silver_db, bronze_root) -> DataFrame:
    """Returns per-source coverage: bronze_anime / bronze_with_studio / silver_in_anime_studios."""
```

`result/audit/anime_studios_coverage.md` 出力。

### Step 2: 計測実行 + 上位欠損 source 特定

最も覆い率低い source を 1-2 個特定 → Step 3 で深堀。

### Step 3: 該当 source の loader 修正

例 anilist:
- BRONZE `anime.studios_main` の JSON parse が機能しているか
- studio name → studio_id 解決ロジック
- 改善: `studios_co` (協力 studio) も統合、現状 main のみなら main+co 両方

### Step 4: 再 ETL

```bash
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -m src.etl.integrate_duckdb
```

### Step 5: 再計測 + AKM 影響確認

```bash
pixi run pipeline 2>&1 | grep -E "silver_anime_loaded|akm_estimated"
```

期待: with_studios 大幅改善、AKM r² 微増 + n_observations 増加。

### Step 6: テスト追加

各 loader で anime_studios INSERT が機能する unit test 追加。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_*.py tests/test_etl/test_anime_studios_coverage.py
duckdb result/silver.duckdb -c "
SELECT source, COUNT(DISTINCT anime_id) AS anime_count
FROM anime_studios GROUP BY 1 ORDER BY 2 DESC
"
# 期待: 各 source で 1,000+ anime に紐付け
```

---

## Stop-if conditions

- [ ] BRONZE 側に studio 情報が本当に存在しない (= scrape 不足) → scrape 系タスク化、本カード Stop
- [ ] silver_loader の修正が広範囲に及び、他 SILVER 表に副作用
- [ ] `pixi run test` 既存テスト破壊

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/silver_loaders/
rm -f src/etl/audit/anime_studios_coverage.py tests/test_etl/test_anime_studios_coverage.py
```

---

## Completion signal

- [ ] `result/audit/anime_studios_coverage.md` 生成
- [ ] anime_studios row 大幅増加 (現 56K → 100K+ 期待)
- [ ] AKM `n_observations` 微増 (silent regression なし)
- [ ] DONE: `22_silver_coverage/01_anime_studios_coverage`
