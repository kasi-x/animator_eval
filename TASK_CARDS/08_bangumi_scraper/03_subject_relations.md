# Task: bangumi subject × persons/characters 関係 (API scrape) → BRONZE parquet

**ID**: `08_bangumi_scraper/03_subject_relations`
**Priority**: 🔴
**Estimated changes**: 約 +400 lines, 3 files 新規
**Requires senior judgment**: yes (rate limit 設計)
**Blocks**: `04_person_detail`, `05_character_detail`
**Blocked by**: `01_archive_dl`, `02_subjects_parquet`

---

## Goal (2026-04-25 改訂: dump→API 方式転換)

**当初計画変更**: `bangumi/Archive` weekly zip は subject.jsonlines のみで relation dump 無し (local file header scan で確認済)。→ **`/v0` API で relation を取得する方式に変更**。

Card 02 出力の anime subject_id 集合 (~3-5k) を起点に以下 2 endpoint を rate limit 1 req/sec で順次 fetch:

- `GET /v0/subjects/{id}/persons` → staff 関係 (list of {id, name, type, career, relation, eps, image})
- `GET /v0/subjects/{id}/characters` → cast 関係 + 声優ネスト (list of {id, name, type, relation, actors:[{id, name, type}]})

出力 3 BRONZE parquet:

- `src_bangumi_subject_persons` — (subject_id, person_id, position(=relation), career[], eps, name_raw) ※ position は raw 中文のまま
- `src_bangumi_subject_characters` — (subject_id, character_id, type, relation, name_raw)
- `src_bangumi_person_characters` — (subject_id, person_id, character_id, actor_type) ※ characters endpoint の actors ネストを分解

---

## Hard constraints

- H1 score 不使用 (この table には混入しない)
- **position / relation label は raw 中文** のまま column に保存。正規化はこの card で**実装しない**
- rate limit **1 req/sec** 厳守 (bangumi ガイド曖昧 → 安全側)
- **User-Agent 必須**: `animetor_eval/<version> (https://github.com/kashi-x)` 形式 (bangumi 規約準拠)
- checkpoint resume 必須 (~6-10k req、途中落ちたら最初からやり直しはコスト大)

---

## Pre-conditions

- [x] `01_archive_dl` 完了 (commit 9d3578e)
- [x] `02_subjects_parquet` 完了 (commit 84dda39)
- [ ] `data/bangumi/dump/latest/manifest.json` 経由で subject_id 集合取得可能
- [ ] `result/bronze/source=bangumi/table=subjects/**/*.parquet` 読める

---

## Step 0: API レスポンス仕様確認

```bash
# 著名 anime id で 2 endpoint を試打ち
curl -sH 'User-Agent: animetor_eval/0.1 (https://github.com/kashi-x)' \
  https://api.bgm.tv/v0/subjects/1/persons | python -m json.tool | head -40

curl -sH 'User-Agent: animetor_eval/0.1 (https://github.com/kashi-x)' \
  https://api.bgm.tv/v0/subjects/1/characters | python -m json.tool | head -60
```

期待: list of dict。characters の `actors` 配列が person-character link を内包。Step 0 の実レスポンスを見てから schema 決定。

---

## Files to create

| File | 責務 |
|------|------|
| `src/scrapers/bangumi_scraper.py` | async httpx client、`fetch_subject_persons(id)` / `fetch_subject_characters(id)`、retry、rate limiter |
| `src/scrapers/queries/bangumi.py` | endpoint 定数 + URL builder (既存 `queries/anilist.py` 同構造) |
| `scripts/scrape_bangumi_relations.py` | orchestrator CLI (typer + Rich progress、checkpoint resume) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/bangumi_dump.py` | Card 01 成果、dump 用途で独立 |
| `src/analysis/`, `src/pipeline_phases/` | SILVER 化は別タスク |

---

## Steps

### Step 1: `src/scrapers/queries/bangumi.py`

- `BANGUMI_API_BASE = "https://api.bgm.tv"`
- `DEFAULT_USER_AGENT = "animetor_eval/0.1 (https://github.com/kashi-x)"`
- `subject_persons_url(subject_id)` / `subject_characters_url(subject_id)` / `person_url(person_id)` / `character_url(character_id)`

### Step 2: `src/scrapers/bangumi_scraper.py`

async httpx client。責務:

- `BangumiClient(user_agent, rate_limit_per_sec=1.0)` — `__aenter__/__aexit__` で httpx.AsyncClient 管理
- 内部 `_rate_limiter`: `asyncio.Semaphore(1) + last_request_at = monotonic()` で全 req 間 >= 1.0s を保証 (sleep で埋める)
- `fetch_subject_persons(subject_id)` / `fetch_subject_characters(subject_id)` / `fetch_person(person_id)` / `fetch_character(character_id)` (後 2 つは Card 04/05 で使うが同クライアントに定義)
- retry: 既存 `src/scrapers/retry.py` / `retrying_http_client.py` を流用 (429 / 5xx で指数 backoff、最大 5 回)
- 404 は retry せず `None` 返却、そのまま checkpoint に記録して skip
- structlog で `bangumi_api_fetch_done subject_id=... endpoint=... count=...`

### Step 3: `scripts/scrape_bangumi_relations.py`

CLI (typer):

```
pixi run python scripts/scrape_bangumi_relations.py \
  [--since <checkpoint>] [--limit N] [--resume] [--dry-run]
```

処理:

1. Card 02 parquet から anime subject_id 集合取得 (DuckDB)
2. checkpoint (`data/bangumi/checkpoint_relations.json`) 読み込み — `{completed_ids: [...], last_run_at: ..., failed_ids: [...]}`
3. 未完了 id のみ queue 化
4. Rich progress bar (tqdm 相当) で N/total 表示、ETA
5. 各 subject で 2 endpoint 並行 (asyncio.gather、ただし rate limiter 経由で実質直列)
6. 各 100 件ごとに checkpoint flush + parquet append (row_group 単位)
7. 完走後 3 parquet を `result/bronze/source=bangumi/table={subject_persons,subject_characters,person_characters}/date=YYYYMMDD/part-N.parquet` に最終書き出し
8. date は実行日 (dump release_date と異なる → API 取得日を真実源とする)

### Step 4: parquet schema

#### `subject_persons`
```
subject_id: int64
person_id: int64
position: string           # raw 中文 relation ("导演", "原画", ...) — mapping は SILVER で
position_code: int32 | null # bangumi/common yaml に integer code があれば保存、無ければ null
career: string             # json.dumps(list) 圧縮
eps: string | null         # 参加 episode range 文字列
name_raw: string           # API レスポンスの name をそのまま
fetched_at: timestamp
```

#### `subject_characters`
```
subject_id: int64
character_id: int64
relation: string           # raw 中文 ("主角", "配角", "客串")
type: int32                # character type (1/2/3...)
name_raw: string
fetched_at: timestamp
```

#### `person_characters` (actors ネストを分解)
```
subject_id: int64
character_id: int64
person_id: int64           # 声優
actor_type: int32 | null
fetched_at: timestamp
```

compression: zstd、row_group_size: 10000、明示 `pa.schema()`。

### Step 5: 起動テスト (小規模 dry-run)

```bash
pixi run python scripts/scrape_bangumi_relations.py --limit 10 --dry-run
# → 10 subject 分の予測 req 数・ETA のみ表示、書き込みなし
pixi run python scripts/scrape_bangumi_relations.py --limit 10
# → 実書き込み、~20 秒 (10 subject × 2 endpoint × 1 sec + overhead)
```

---

## Verification

```bash
# 小規模 (10 件) 成功確認後、本実行は user 明示承認後に着手
pixi run python scripts/scrape_bangumi_relations.py --limit 10

pixi run python -c "
import duckdb
con = duckdb.connect()
for t in ['subject_persons', 'subject_characters', 'person_characters']:
    n = con.execute(f\"SELECT count(*) FROM read_parquet('result/bronze/source=bangumi/table={t}/**/*.parquet')\").fetchone()[0]
    print(f'{t}: {n}')
# 10 subject 時点で: subject_persons ~100-500, subject_characters ~100-500, person_characters ~50-500
"

pixi run lint

# checkpoint idempotent
cat data/bangumi/checkpoint_relations.json | python -m json.tool | head
pixi run python scripts/scrape_bangumi_relations.py --limit 10 --resume
# → completed 10 件 skip、追加取得 0 or 次の未完了に進む
```

---

## Stop-if conditions

- [ ] 429 / 5xx が 3 回連続 → rate limit 1sec で足りない、sleep 2sec に上げて再起動
- [ ] User-Agent 拒絶 (403) → bangumi にブロック済、form 変更して再試行
- [ ] schema 想定外 (`actors` キー欠落 / 型不一致) → Step 0 の spec 確認漏れ、parser 修正
- [ ] `git diff --stat` が 600 lines 超 → 分解 or 設計見直し

---

## Rollback

```bash
git checkout src/scrapers/ scripts/
rm -rf result/bronze/source=bangumi/table={subject_persons,subject_characters,person_characters}/date=<今回>/
rm -f data/bangumi/checkpoint_relations.json
pixi run lint
```

---

## Completion signal

- [ ] 10 件 dry-run + 実 run で 3 parquet 生成確認
- [ ] checkpoint resume 動作確認
- [ ] rate limit 遵守ログ (1 req/sec) 確認
- [ ] lint pass
- [ ] full run (~3-5k subject) は **別タスク** として user 承認後に実行 (実行時間 ~2 時間)
- [ ] DONE 記録
