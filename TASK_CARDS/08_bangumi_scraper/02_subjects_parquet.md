# Task: bangumi subjects jsonlines → BRONZE parquet

**ID**: `08_bangumi_scraper/02_subjects_parquet`
**Priority**: 🔴
**Estimated changes**: 約 +150 lines, 1 file 新規
**Requires senior judgment**: no
**Blocks**: `03_subject_relations`, `04_person_detail`
**Blocked by**: `01_archive_dl`

---

## Goal

`data/bangumi/dump/latest/subject.jsonlines` → `result/bronze/source=bangumi/table=subjects/date=YYYYMMDD/*.parquet`。type=2 (anime) のみ抽出、他 type (book/game/music/real) は除外。

---

## Hard constraints

- H1 `score`, `rank`, `rating` は raw column 保全、scoring 流入禁止
- dump 由来の全 column を保全 (drop しない、string/int/dict のまま parquet に)
- `date=YYYYMMDD` は dump release 日付 (manifest から取得、実行日ではない)

---

## Pre-conditions

- [x] `01_archive_dl` 完了 → `data/bangumi/dump/latest/subject.jsonlines` 存在
- [x] `pixi run lint` baseline pass

---

## Step 0: subject jsonlines 構造確認

```bash
head -1 data/bangumi/dump/latest/subject.jsonlines | python -m json.tool
# 期待 key (バージョンで変化): id, type, name, name_cn, infobox, summary, score, rank, rating, platform, image, tags, date, ...
# type 分布
awk -F'"type":' '{split($2, a, ","); print a[1]}' data/bangumi/dump/latest/subject.jsonlines | sort | uniq -c
# type=2 が anime
```

実際の key 一覧を見てから parser column 決定。

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/migrate_bangumi_subjects_to_parquet.py` | jsonlines → parquet 変換 CLI |

## Files to NOT touch

| File | 理由 |
|------|------|
| `data/bangumi/dump/**` | 入力 read-only |
| `src/scrapers/parsers/` | parser 統一は SILVER 移行タスクで別起票 |

---

## Steps

### Step 1: script skeleton

`scripts/migrate_allcinema_to_parquet.py` のパターン準拠。差分:

- 入力: jsonlines (1 行 1 subject)
- filter: `doc.get("type") == 2` のみ
- infobox は list-of-dict 形式 → json.dumps して string column に
- tags は list[dict{name, count}] → 同様 json.dumps
- rating は dict{rank, total, count:{1..10}, score} → json.dumps
- date column は `release_date` に rename (SQL 予約語回避)

### Step 2: BRONZE writer 統合

既存 `src/scrapers/bronze_writer.py` の API があればそれを使用 (allcinema card 参照)。無ければ直接 `pyarrow.parquet.write_table`。

- compression: `zstd`
- row_group_size: 10000
- schema は first pass で推定 → 明示 `pa.schema()` で固定 (column 順序安定化)

### Step 3: 出力パス

```
result/bronze/source=bangumi/table=subjects/date=<dump_release_date>/part-0.parquet
```

`<dump_release_date>` は `data/bangumi/dump/latest/manifest.json` の `release_tag` (YYYY-MM-DD 形式) → `YYYYMMDD` に変換。

---

## Verification

```bash
pixi run python scripts/migrate_bangumi_subjects_to_parquet.py
ls result/bronze/source=bangumi/table=subjects/date=*/

pixi run python -c "
import duckdb
con = duckdb.connect()
df = con.execute('SELECT count(*) FROM read_parquet(\"result/bronze/source=bangumi/table=subjects/**/*.parquet\")').fetchone()
print('row count:', df[0])
# anime は ~40k 想定。桁違いなら type filter 漏れ
"

pixi run lint
```

---

## Stop-if conditions

- [ ] 出力 parquet 行数 < 10000 or > 100000 → filter 誤り疑い
- [ ] schema inference エラー (dict column の型混合) → `json.dumps` 漏れ
- [ ] `git diff --stat` が 300 lines 超

---

## Rollback

```bash
git checkout scripts/
rm -rf result/bronze/source=bangumi/table=subjects/date=<今回>/
pixi run lint
```

---

## Completion signal

- [x] type=2 anime のみ parquet 出力済
- [x] row count が 妥当範囲 (20k-60k)
- [x] DuckDB から read_parquet 可能
- [x] DONE 記録

**DONE: 2026-04-25 — commit 84dda39**
