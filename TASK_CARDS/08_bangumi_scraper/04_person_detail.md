# Task: bangumi persons (API scrape) → BRONZE parquet

**ID**: `08_bangumi_scraper/04_person_detail`
**Priority**: 🟠
**Estimated changes**: 約 +180 lines, 1 file 新規
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `03_subject_relations`

---

## Goal (2026-04-25 改訂: API 方式)

dump に person.jsonlines が含まれていないため `/v0/persons/{id}` で API scrape。対象は Card 03 で収集した person_id 集合 (subject_persons + person_characters の和集合)。

---

## Hard constraints

- H3 entity resolution 不変 (bangumi → 既存 AniList/MAL/ANN との統合は SILVER 化タスクで別起票)
- rate limit **1 req/sec** (Card 03 クライアント流用)
- infobox (wiki template string) は raw 保存、展開しない
- fetch 失敗 (404) は skip + checkpoint に記録

---

## Pre-conditions

- [x] `03_subject_relations` 完了 → relation parquet 存在
- [x] person_id 集合 ~10-20k 推定

---

## Step 0: API レスポンス確認

```bash
curl -sH 'User-Agent: animetor_eval/0.1 (https://github.com/kashi-x)' \
  https://api.bgm.tv/v0/persons/1 | python -m json.tool | head -40
```

期待 key: `id, name, type, career, images, summary, locked, last_modified, stat{comments, collects}, img, infobox, gender, blood_type, birth_year, birth_mon, birth_day`

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/scrape_bangumi_persons.py` | orchestrator CLI (client は Card 03 で作った `BangumiClient` 流用) |

---

## Steps

### Step 1: 参照 person_id 集合取得

```python
import duckdb
con = duckdb.connect()
referenced = {r[0] for r in con.execute("""
    SELECT DISTINCT person_id FROM read_parquet('result/bronze/source=bangumi/table=subject_persons/**/*.parquet')
    UNION
    SELECT DISTINCT person_id FROM read_parquet('result/bronze/source=bangumi/table=person_characters/**/*.parquet')
""").fetchall()}
```

### Step 2: `BangumiClient.fetch_person(person_id)` で逐次取得

- rate limit 1 req/sec (Card 03 と共有の limiter)
- checkpoint `data/bangumi/checkpoint_persons.json` で resume
- 各 100 件ごと parquet append
- 404 は skip + failed_ids に記録

### Step 3: 出力 parquet schema

```
id: int64
name: string
type: int32              # 1=個人, 2=公司, 3=組合
career: string           # json.dumps(list)
summary: string | null
infobox: string          # raw wiki template
gender: string | null
blood_type: int32 | null
birth_year: int32 | null
birth_mon: int32 | null
birth_day: int32 | null
images: string           # json.dumps(dict with small/medium/large)
stat_comments: int32
stat_collects: int32
last_modified: timestamp
fetched_at: timestamp
```

出力: `result/bronze/source=bangumi/table=persons/date=YYYYMMDD/part-N.parquet`

---

## Verification

```bash
pixi run python scripts/scrape_bangumi_persons.py --limit 10
pixi run python -c "
import duckdb
n = duckdb.connect().execute(\"SELECT count(*) FROM read_parquet('result/bronze/source=bangumi/table=persons/**/*.parquet')\").fetchone()[0]
print('persons:', n)
"
pixi run lint
```

full run は user 承認後 (~10-20k req → ~3-6 時間)。

---

## Stop-if conditions

- [ ] 429 連続 → sleep 2sec に増やして再起動
- [ ] schema 不一致 (想定 key 欠落) → parser 修正
- [ ] referenced 集合サイズ > 50k → anime filter 漏れ、Card 03 見直し

---

## Rollback

```bash
git checkout scripts/scrape_bangumi_persons.py
rm -rf result/bronze/source=bangumi/table=persons/date=<今回>/
rm -f data/bangumi/checkpoint_persons.json
```

---

## Completion signal

- [x] 10 件 dry-run + 実 run 成功
- [x] resume 動作確認
- [x] full run 完走 (user 承認後、別実行)
- [x] DONE 記録

**DONE: 2026-04-25 — commit 960323d**
