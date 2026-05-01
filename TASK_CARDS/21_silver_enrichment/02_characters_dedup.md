# Task: characters cross-source 重複検出 + safe merge

**ID**: `21_silver_enrichment/02_characters_dedup`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -20 lines, 3 files
**Requires senior judgment**: yes (キャラ同定基準)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

SILVER `characters` 135K 行の cross-source 重複候補を検出し、高信頼度ペア (similarity > 0.99 + 同 anilist_id or 同 anime + 同 actor 一致) を safe merge する。

---

## Hard constraints

- **H1**: characters の `favourites` は scoring 経路不参入
- **H3**: entity_resolution ロジック不変、本タスクは検出 + 特例 merge ETL
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- silver.duckdb backup 必須

---

## Pre-conditions

- [ ] `git status` clean
- [ ] characters 行数: `duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM characters"` (期待: 135,375)
- [ ] character_voice_actors 行数確認
- [ ] `pixi run test` baseline pass

---

## 検出戦略

### 同定基準 (高信頼度)

1. **同 anilist_id** (cross-source: anilist と他 source で同 anilist_id 持つ場合) → automatic merge
2. **同 name (NFKC + 句読点除去) + 同 actor 一致** (同じ person_id が character_voice_actors で両 character に紐づく)
3. **同 name + 同 anime_id** (同 anime に同名 character は同一)

### 検出 SQL 例

```sql
WITH normalized AS (
    SELECT id, COALESCE(name_ja, name_en) AS name,
           lower(regexp_replace(coalesce(name_ja, name_en), '[\s\.\-]', '', 'g')) AS norm_name,
           anilist_id
    FROM characters
)
SELECT a.id AS id_a, b.id AS id_b, a.name, b.name, a.anilist_id
FROM normalized a JOIN normalized b 
  ON a.norm_name = b.norm_name AND a.id < b.id
  AND (a.anilist_id IS NOT NULL AND a.anilist_id = b.anilist_id)
```

### Safe merge

canonical = lex min ID。CVA / credits 触らず characters 行のみ統合。

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/audit/characters_dedup.py` | `audit(conn, output_dir) -> dict` |
| `src/etl/dedup/characters_safe_merge.py` | `merge(conn, audit_csv, dry_run=False) -> dict` |
| `tests/test_etl/test_characters_dedup.py` | 検出 + merge 合成 fixture テスト |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| `src/etl/silver_loaders/*` | 既存 ETL 不変 |

---

## Steps

### Step 1: audit 関数

`characters_dedup.py`:
- `find_dup_by_anilist_id(conn) -> DataFrame`
- `find_dup_by_name_and_actor(conn) -> DataFrame`
- `find_dup_by_name_and_anime(conn) -> DataFrame`
- `audit(conn, output_dir) -> dict[criterion, count]`

CSV 出力: `result/audit/characters_dedup.csv`

### Step 2: safe merge 関数

`characters_safe_merge.py`:
- `merge(conn, audit_csv, dry_run=False) -> dict[before, after, audit_logged]`
- transaction 内で character_voice_actors の `character_id` を canonical に UPDATE → characters の非 canonical 行 DELETE

### Step 3: テスト

合成 fixture (anilist_id 一致 + name 一致 + actor 一致パターン) で検証。

### Step 4: 実行

```bash
pixi run python -c "
import duckdb
from pathlib import Path
from src.etl.audit.characters_dedup import audit
conn = duckdb.connect('result/silver.duckdb', read_only=True)
print(audit(conn, Path('result/audit')))
"
# Dry-run merge
pixi run python -c "
from src.etl.dedup.characters_safe_merge import merge
import duckdb
conn = duckdb.connect('result/silver.duckdb')
print(merge(conn, 'result/audit/characters_dedup.csv', dry_run=True))
"
# Backup + actual merge
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -c "...(dry_run=False)..."
```

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_characters_dedup.py
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM characters"  # 微減確認
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM character_voice_actors"  # 不変
```

---

## Stop-if conditions

- [ ] merge 件数が characters 全体の 5% 超 → 基準厳格化、Stop
- [ ] character_voice_actors row が変化 (不変が期待) → Rollback

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/audit/characters_dedup.py src/etl/dedup/characters_safe_merge.py tests/
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] characters 行数微減 (期待 < 5%)
- [ ] `meta_entity_resolution_audit` に characters merge 記録
- [ ] DONE: `21_silver_enrichment/02_characters_dedup`
