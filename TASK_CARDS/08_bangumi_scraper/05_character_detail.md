# Task: bangumi characters jsonlines → BRONZE parquet (最後回し)

**ID**: `08_bangumi_scraper/05_character_detail`
**Priority**: 🟢
**Estimated changes**: 約 +100 lines, 1 file 新規
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `03_subject_relations`

---

## Goal

`character.jsonlines` → `src_bangumi_characters` parquet。anime subject に登場する character のみ filter。**優先度最低**、他 card すべて完了してから着手。

---

## Hard constraints

- character は架空人物なので entity resolution の文脈外。person と混ぜない
- infobox は raw JSON 保存

---

## Pre-conditions

- [ ] `03_subject_relations` 完了
- [ ] `01..04` の card が全完了 (優先度運用)

---

## Step 0: character jsonlines 構造確認

```bash
head -1 data/bangumi/dump/latest/character.jsonlines | python -m json.tool
# 期待 key: id, name, role, infobox, summary, img, last_modified
```

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/migrate_bangumi_characters_to_parquet.py` | filter + parquet |

---

## Steps

### Step 1: 参照 character_id 集合

```python
referenced = {r[0] for r in con.execute("""
    SELECT DISTINCT character_id FROM read_parquet('result/bronze/source=bangumi/table=subject_characters/**/*.parquet')
""").fetchall()}
```

### Step 2: jsonlines stream filter → parquet

`04_person_detail` と同構造。infobox json.dumps、出力パス `table=characters/date=<release>/`。

---

## Verification

```bash
pixi run python scripts/migrate_bangumi_characters_to_parquet.py
pixi run python -c "
import duckdb
n = duckdb.connect().execute(\"SELECT count(*) FROM read_parquet('result/bronze/source=bangumi/table=characters/**/*.parquet')\").fetchone()[0]
print('characters:', n)
"
pixi run lint
```

---

## Stop-if conditions

- [ ] row count が referenced 集合サイズと不一致 (差 > 10%)
- [ ] schema エラー

---

## Rollback

```bash
git checkout scripts/
rm -rf result/bronze/source=bangumi/table=characters/date=<今回>/
```

---

## Completion signal

- [ ] parquet 出力済
- [ ] DONE 記録
