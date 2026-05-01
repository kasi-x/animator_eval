# Task: anime_studios PK 衝突修正 (93edd3d WIP)

**ID**: `20_silver_quality/04_anime_studios_pk_fix`
**Priority**: 🟠
**Estimated changes**: 約 +100 / -30 lines, 2-3 files
**Requires senior judgment**: yes (PK 設計判断)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

`93edd3d feat(silver): 8 source loader 並列追加` で WIP 残存と記録された `anime_studios PK 衝突` を解消する。

---

## Hard constraints

- **H4**: 各 source 区別を維持 (`evidence_source` or 専用列)
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- silver.duckdb backup 必須

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 現状確認:
```bash
duckdb result/silver.duckdb -c "
DESCRIBE anime_studios;
SELECT COUNT(*), COUNT(DISTINCT (anime_id, studio_id, role)) FROM anime_studios
"
```
- [ ] 各 silver_loader の anime_studios INSERT 文確認
- [ ] `pixi run test` baseline pass

---

## 衝突原因 (推定)

### 候補 1: 同 anime × 同 studio が複数 source で衝突

例: AniList と MAL が「Studio Ghibli が となりのトトロ を制作」の重複行を試みる → PK `(anime_id, studio_id)` で 2 行目失敗。

→ PK を `(anime_id, studio_id, role, source)` に拡張、`source` 列で source 並存可。

### 候補 2: 同 source 内で role 違いの重複

例: 同 anime で「制作」と「協力」が違う row → role 列を PK に含めれば解決。

### 候補 3: studio name 違いで同 ID

`kf:n:<name>` ベースで studio を生成しているため、同 anime で複数 studio 名同義 → 別 ID で別 row になる (これは衝突でなく重複)。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | `anime_studios` PK 修正 (末尾 ALTER TABLE で migration) |
| `src/etl/silver_loaders/<source>.py` | INSERT 文に `source` 列追加 (各 source) |
| `tests/test_etl/test_silver_<source>.py` | PK 衝突テスト追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |

---

## Steps

### Step 1: 現状の anime_studios DDL 確認

```bash
grep -B 2 -A 20 "CREATE TABLE.*anime_studios" src/db/schema.py
```

PK の定義を把握。

### Step 2: 衝突再現確認

```bash
# integrate_duckdb 実行ログで PK 衝突 警告/エラーを確認
pixi run python -m src.etl.integrate_duckdb 2>&1 | grep -i "anime_studios\|primary key\|conflict"
```

### Step 3: PK 拡張 + source 列追加

PK: `(anime_id, studio_id, role, source)` に変更。

```sql
ALTER TABLE anime_studios ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT '';
-- DuckDB は ALTER PK 直接不可、新表作成 + データ移行 + RENAME
```

DuckDB の制約上、PK 変更は新表作成経路で対応:
```sql
CREATE TABLE anime_studios_new AS SELECT * FROM anime_studios;
ALTER TABLE anime_studios_new ADD CONSTRAINT pk PRIMARY KEY (anime_id, studio_id, role, source);
-- ON CONFLICT DO NOTHING で重複 skip
```

### Step 4: 各 silver_loader の anime_studios INSERT 文修正

mal / ann / anilist / mediaarts / seesaawiki / keyframe / bangumi の anime_studios INSERT に `source` 列追加。

### Step 5: 既存データ migration

```sql
-- 既存行に source backfill (ID prefix から推定)
UPDATE anime_studios SET source = 
    CASE 
        WHEN anime_id LIKE 'anilist:%' THEN 'anilist'
        WHEN anime_id LIKE 'mal:%' THEN 'mal'
        WHEN anime_id LIKE 'ann:%' THEN 'ann'
        WHEN anime_id LIKE 'bgm:%' THEN 'bangumi'
        WHEN anime_id LIKE 'mediaarts:%' THEN 'mediaarts'
        WHEN anime_id LIKE 'kf:%' THEN 'keyframe'
        ELSE 'unknown'
    END
WHERE source = '' OR source IS NULL
```

### Step 6: pipeline 再実行 + 衝突解消確認

```bash
pixi run python -m src.etl.integrate_duckdb 2>&1 | grep -i "primary key" | head -5  # 0 件期待
```

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_anilist.py tests/test_etl/test_silver_mal.py tests/test_etl/test_silver_ann.py
duckdb result/silver.duckdb -c "
SELECT source, COUNT(*) FROM anime_studios GROUP BY 1 ORDER BY 2 DESC
"
```

---

## Stop-if conditions

- [ ] PK 拡張で既存テスト破壊
- [ ] DuckDB が ALTER TABLE PRIMARY KEY を未サポート → 新表作成経路必須、その他は問題なし
- [ ] backfill SQL で source = 'unknown' が大量 (> 5%)

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/db/schema.py src/etl/silver_loaders/ tests/test_etl/
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] anime_studios INSERT 衝突 0 件
- [ ] DONE: `20_silver_quality/04_anime_studios_pk_fix`
