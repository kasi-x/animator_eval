# Task: persons cluster TMDb 同名 over-merge 修正

**Status**: ✅ 完了 (2026-05-13、commit f0d4547、Jonas 47 cluster → 各 tmdb_id ごと分離、tests 6/6 pass)

**ID**: `19_resolved_cluster_fix/02_persons_tmdb_homonym`
**Priority**: 🟠 High
**Estimated changes**: ~+80 / -10 lines, 2 files
**Requires senior judgment**: yes (homonym guard 強化は誤分離リスク評価必要)
**Blocks**: persons 由来 scoring (over-merge による theta_i 汚染)
**Blocked by**: なし

---

## Goal

`exact_match_cluster` の en_name_groups 経路で TMDb 同名異人 (`'Jonas'` 47 件 / `'David'` 25 件) が 1 cluster に潰される問題を修正。

**情報量原則**: TMDb の 293K name_ja + 99K name_en + 109K gender + 12K death_date は重要 source。row 自体は捨てない。`tmdb_id` を numeric ID キーに加えて **異 tmdb_id = 別人** と判定。

---

## Hard constraints

- **H3**: entity_resolution interface 不変 (関数シグネチャ + 戻り値型)
- 過剰分離リスク: 同一人の TMDb 重複 page (e.g. 別 tmdb_id だが同一実在人物) があれば誤分離する → 検証ループ必須
- **再 scrape 不要**: 既 tmdb conformed に tmdb_id 保持済 (要確認)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] tmdb conformed の id 構造確認: `SELECT id, name_en FROM conformed.persons WHERE id LIKE 'tmdb:%' LIMIT 5;`
- [ ] `result/resolved.duckdb` バックアップ取得
- [ ] baseline persons cluster 統計記録:
  ```sql
  SELECT json_array_length(source_ids_json) AS n, COUNT(*) FROM persons
  GROUP BY n ORDER BY n DESC LIMIT 20;
  ```

---

## Files to modify

| File | 変更内容 |
|------|----------|
| `src/analysis/entity/entity_resolution.py` | (1) `Person` dataclass に `tmdb_id` 列追加 / (2) `_numeric_id_key` を `(ann_id, anilist_id, mal_id, tmdb_id)` に拡張 / (3) `_definitely_different` で tmdb_id 比較追加 / (4) en_name_groups merge 前の guard: 同 source 内 numeric_id 全異なる場合は merge 拒否 |
| `src/etl/resolved/_persons_cluster.py` | tmdb_id を Person 生成時に流入させる (現状は anilist_id/mal_id/ann_id のみ) |

## Files to create

| File | 内容 |
|------|------|
| `tests/test_entity/test_tmdb_homonym_guard.py` | Jonas 47 件 fixture で別 cluster 化検証 |

---

## Implementation outline

### Step 1: Person dataclass 拡張
```python
@dataclass
class Person:
    id: str
    name_ja: str
    name_en: str
    ...
    ann_id: int | None = None
    anilist_id: int | None = None
    mal_id: int | None = None
    tmdb_id: int | None = None  # 新規
```

### Step 2: numeric ID key 拡張
```python
def _numeric_id_key(p: Person) -> tuple:
    return (p.ann_id, p.anilist_id, p.mal_id, p.tmdb_id)
```

### Step 3: `_definitely_different` で tmdb_id 比較追加
既存 logic に追記:
```python
if p.tmdb_id is not None and rep.tmdb_id is not None and p.tmdb_id != rep.tmdb_id:
    return True  # different persons
```

### Step 4: en_name_groups 経路の guard 追加
en で merge 候補になった ids を `_merge_group` に渡す前に追加 filter:
```python
# 全員 name_ja 空 + name_en 同一 + 同一 source prefix の場合、
# numeric ID が全員存在し全員異なれば merge 拒否
def _should_block_en_merge(ids: list[str], persons_by_id) -> bool:
    if len({pid.split(":", 1)[0] for pid in ids}) > 1:
        return False  # 異 source 同名は merge 検討余地あり
    keys = [_numeric_id_key(persons_by_id[pid]) for pid in ids]
    non_null_keys = [k for k in keys if any(v is not None for v in k)]
    return len(set(non_null_keys)) == len(non_null_keys) and len(non_null_keys) == len(ids)
```

### Step 5: persons cluster ETL に tmdb_id 流入
`_persons_cluster.py` の Person 生成 (conformed.persons → Person) で `tmdb_id` 列を読み出し設定。

---

## Audit / verification

### Step 6: Jonas / David / Elmer 検証
```sql
-- BEFORE: 1 cluster (47 件)
-- AFTER:  47 cluster (各 1 件) を期待
SELECT canonical_id, name_en, json_array_length(source_ids_json) AS n
FROM persons WHERE name_en = 'Jonas' AND source_ids_json LIKE '%tmdb%'
ORDER BY n DESC LIMIT 10;
```

### Step 7: 全体分布
```sql
-- src=tmdb 単独 + size > 5 の cluster 数
SELECT COUNT(*) FROM persons
WHERE source_ids_json LIKE '[%tmdb%]'  -- 全 src tmdb
  AND json_array_length(source_ids_json) > 5;
-- BEFORE: ~10 件以上 / AFTER: ≤ 1 件期待
```

### Step 8: 過剰分離リスク確認
LLM 検証 (qwen2.5:14b で sample 100 cluster):
- BEFORE/AFTER 両 cluster を sample → 「同一人物だが分離されている」ケース報告 → 該当 0 か手動確認
- `merge_strategy.json` 1.2 で確立した LLM 検証フローを再利用 (`scripts/maintenance/llm_review_clusters.py` 等が既存ならそれ)

---

## Open questions

- **TMDb 重複 page 問題**: 同一人物が tmdb 内で 2 page 持つケースあるか? あれば guard が誤分離 → stage 4 (similarity_based) で再統合する経路を残すか検討
- **他 source 同名問題**: tmdb 以外で同様の over-merge ある? `seesaa` 内連番同名は確認済? 他 source も同 logic 適用すべきか
- **fast_only=True 維持か?**: stage 3-5 を有効化すれば similarity-based で別経路救済も可能。ETL コストとのトレードオフ

---

## Rollback

```bash
mv result/resolved.duckdb.bak.before-tmdb-homonym result/resolved.duckdb
git revert HEAD~N..HEAD
```

---

## Done criteria

- [ ] 'Jonas' / 'David' / 'Elmer' / 'Ryan Cooper' / 'Hoog' 各 cluster size = 1 (各 tmdb_id ごと別 cluster)
- [ ] 全 src tmdb + size > 5 の cluster 数 ≤ 1
- [ ] LLM sample 検証で過剰分離 0 件
- [ ] persons 行数 = before fix + (over-merge 解消分の増加、推定 +500 件程度)
- [ ] `pixi run test-scoped tests/test_entity/test_tmdb_homonym_guard.py` pass
