# Task: bangumi subject × persons/characters 関係 → BRONZE parquet

**ID**: `08_bangumi_scraper/03_subject_relations`
**Priority**: 🔴
**Estimated changes**: 約 +180 lines, 1 file 新規
**Requires senior judgment**: no
**Blocks**: `04_person_detail`, `05_character_detail`
**Blocked by**: `01_archive_dl`, `02_subjects_parquet`

---

## Goal

dump の `subject-persons.jsonlines` / `subject-characters.jsonlines` / `person-characters.jsonlines` を BRONZE parquet 化。anime (type=2) subject 限定で filter。

出力 3 table:

- `src_bangumi_subject_persons` — staff 関係 (subject_id × person_id × position)
- `src_bangumi_subject_characters` — cast 関係 (subject_id × character_id × type)
- `src_bangumi_person_characters` — 声優 (person_id × character_id × subject_id)

---

## Hard constraints

- H1 score 不使用 (この table には来ないが念のため)
- **position / role label は raw 中文** のまま column に保存。正規化 mapping はこの card で**実装しない**
- anime subject のみ filter (`02_subjects_parquet` 出力の subject_id 集合で inner join)

---

## Pre-conditions

- [ ] `01_archive_dl` 完了
- [ ] `02_subjects_parquet` 完了 → anime subject_id 集合取得可能

---

## Step 0: 3 jsonlines の構造確認

```bash
for f in subject-persons subject-characters person-characters; do
  echo "=== $f ==="
  head -1 data/bangumi/dump/latest/${f}.jsonlines | python -m json.tool
done
```

推定 schema (確認必須):

- **subject-persons**: `{person_id, subject_id, position}` — position は int code (1=原作, 2=导演, …) か string か release 次第
- **subject-characters**: `{character_id, subject_id, type}` — type は主人公/配角
- **person-characters**: `{person_id, character_id, subject_id}` — 声優 link

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/migrate_bangumi_relations_to_parquet.py` | 3 relation を 1 script で同時処理 |

---

## Steps

### Step 1: anime subject_id 集合取得

```python
import duckdb
con = duckdb.connect()
anime_ids: set[int] = {
    r[0] for r in con.execute(
        "SELECT id FROM read_parquet('result/bronze/source=bangumi/table=subjects/**/*.parquet')"
    ).fetchall()
}
```

### Step 2: 3 jsonlines それぞれ stream 処理

- 1 行ずつ json.loads
- `subject_id in anime_ids` で filter
- parquet writer に append (row_group 単位で flush)
- position code 一覧は別途 distinct 取得して manifest に記録 (後の正規化タスク用)

### Step 3: 出力パス

```
result/bronze/source=bangumi/table=subject_persons/date=<release>/part-0.parquet
result/bronze/source=bangumi/table=subject_characters/date=<release>/part-0.parquet
result/bronze/source=bangumi/table=person_characters/date=<release>/part-0.parquet
```

### Step 4: position code 辞書 dump

```
result/bronze/source=bangumi/table=subject_persons/date=<release>/position_codes.json
```

`{code: sample_subject_id}` のような形で観測値を保存 (デバッグ用、SILVER 化で label 解決する)。

---

## Verification

```bash
pixi run python scripts/migrate_bangumi_relations_to_parquet.py

pixi run python -c "
import duckdb
con = duckdb.connect()
for t in ['subject_persons', 'subject_characters', 'person_characters']:
    n = con.execute(f\"SELECT count(*) FROM read_parquet('result/bronze/source=bangumi/table={t}/**/*.parquet')\").fetchone()[0]
    print(f'{t}: {n}')
# subject_persons: ~100k-1M, subject_characters: ~100k-500k, person_characters: ~200k-1M 想定
"

pixi run lint
```

---

## Stop-if conditions

- [ ] 各 table 行数が 10k 未満 → anime filter が過剰
- [ ] 各 table 行数が 10M 超 → anime filter が機能していない
- [ ] 未知の position code が 100 種超 → jsonlines schema 変化疑い

---

## Rollback

```bash
git checkout scripts/
rm -rf result/bronze/source=bangumi/table={subject_persons,subject_characters,person_characters}/date=<今回>/
```

---

## Completion signal

- [ ] 3 table parquet 出力済
- [ ] position code manifest 出力済
- [ ] row count 妥当
- [ ] DONE 記録
