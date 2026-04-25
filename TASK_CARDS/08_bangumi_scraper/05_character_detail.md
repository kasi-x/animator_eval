# Task: bangumi characters (API scrape) → BRONZE parquet (最後回し)

**ID**: `08_bangumi_scraper/05_character_detail`
**Priority**: 🟢
**Estimated changes**: 約 +120 lines, 1 file 新規
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `03_subject_relations`

---

## Goal (2026-04-25 改訂: API 方式)

`/v0/characters/{id}` で API scrape。対象は Card 03 で集めた character_id 集合 (subject_characters + person_characters の和集合)。**優先度最低**、他 card 完了後に着手。

---

## Hard constraints

- character は架空人物 → entity resolution の対象外、person と混ぜない
- infobox は raw 保存
- rate limit 1 req/sec

---

## Pre-conditions

- [x] `03_subject_relations` 完了
- [x] `04_person_detail` 完了 (運用順)

---

## Step 0: API レスポンス確認

```bash
curl -sH 'User-Agent: animetor_eval/0.1 (https://github.com/kashi-x)' \
  https://api.bgm.tv/v0/characters/1 | python -m json.tool | head -30
```

期待 key: `id, name, role, images, summary, locked, last_modified, stat, infobox, gender, blood_type, birth_year, birth_mon, birth_day`

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/scrape_bangumi_characters.py` | orchestrator CLI (`BangumiClient` 流用) |

---

## Steps

### Step 1: 参照 character_id 集合

```python
referenced = {r[0] for r in con.execute("""
    SELECT DISTINCT character_id FROM read_parquet('result/bronze/source=bangumi/table=subject_characters/**/*.parquet')
    UNION
    SELECT DISTINCT character_id FROM read_parquet('result/bronze/source=bangumi/table=person_characters/**/*.parquet')
""").fetchall()}
```

### Step 2: `BangumiClient.fetch_character(id)` 逐次、checkpoint resume、parquet append

### Step 3: schema

```
id: int64
name: string
role: int32              # 1=角色, 2=機体, 3=組織...
summary: string | null
infobox: string          # raw wiki
gender: string | null
blood_type: int32 | null
birth_year: int32 | null
birth_mon: int32 | null
birth_day: int32 | null
images: string           # json.dumps
stat_comments: int32
stat_collects: int32
last_modified: timestamp
fetched_at: timestamp
```

出力: `result/bronze/source=bangumi/table=characters/date=YYYYMMDD/part-N.parquet`

---

## Verification

```bash
pixi run python scripts/scrape_bangumi_characters.py --limit 10
pixi run python -c "
import duckdb
n = duckdb.connect().execute(\"SELECT count(*) FROM read_parquet('result/bronze/source=bangumi/table=characters/**/*.parquet')\").fetchone()[0]
print('characters:', n)
"
pixi run lint
```

---

## Stop-if conditions

- [ ] 429 連続 → sleep 増
- [ ] schema 不一致

---

## Rollback

```bash
git checkout scripts/scrape_bangumi_characters.py
rm -rf result/bronze/source=bangumi/table=characters/date=<今回>/
rm -f data/bangumi/checkpoint_characters.json
```

---

## Completion signal

- [x] 10 件 dry-run + 実 run 成功
- [x] full run 完走 (user 承認後)
- [x] DONE 記録

**DONE: 2026-04-25 — commit 0d121b6**
