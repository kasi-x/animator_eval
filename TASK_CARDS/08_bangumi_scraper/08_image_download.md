# Task: bangumi 画像 DL → data/bangumi/images/ + image_manifest BRONZE parquet

**ID**: `08_bangumi_scraper/08_image_download`
**Priority**: 🟢
**Estimated changes**: 約 +400 lines, 1 file 新規 (script 既存: `scripts/download_bangumi_images.py`)
**Requires senior judgment**: no
**Blocks**: (なし — display 専用)
**Blocked by**: `01..05` 全完了、**全 backfill 完走後のみ着手**

---

## Goal

BRONZE parquets (`persons`, `characters`) に格納済みの画像 URL から `large` バリアントのみを
ダウンロード → `data/bangumi/images/{persons,characters}/{id}.{ext}` に保存 →
内容アドレス可能な manifest を BRONZE parquet (`table=image_manifest`) に書き出す。
画像は **display 専用** — scoring path に一切入れない (H1)。

---

## Hard constraints

- **H1**: 画像は UI 表示のみ。scoring、edge weight、任意の analysis module からは参照しない
- **破壊的操作禁止**: 既存 `data/bangumi/images/` を上書きしない (ローカルパス + SHA256 一致でスキップ)
- **rate limit**: bangumi CDN (`lain.bgm.tv`) に対して **1 req/sec 厳守**。BangumiClient の
  throttle とは独立した `asyncio.Lock` ベースのリミッターを使用すること
- **User-Agent**: `animetor_eval/0.1 (https://github.com/kashi-x)` をすべての CDN リクエストに付ける
- **書込み先 ALLOWED_SOURCES**: `bangumi` はすでに `ALLOWED_SOURCES` に含まれる。
  `BronzeWriter("bangumi", table="image_manifest")` はそのまま動く — `bronze_writer.py` を変更しない

---

## Pre-conditions

- [ ] `01_archive_dl` ✅ 完了
- [ ] `02_subjects_parquet` ✅ 完了
- [ ] `03_subject_relations` ✅ 完了
- [ ] `04_person_detail` ✅ 完了 — `table=persons` parquet に全 person 画像 URL 格納済み
- [ ] `05_character_detail` ✅ 完了 — `table=characters` parquet に全 character 画像 URL 格納済み
- [ ] **全 backfill 完走確認** (`checkpoint_persons.json` + `checkpoint_characters.json` の `pending` が 0)
- [ ] `data/bangumi/images/` 書込み可 (ディレクトリなければ script が作成)
- [ ] 空きディスク 5GB 以上 (推定 3-4GB + バッファ)
- [ ] rate budget に余裕 (scraper/backfill が走っていないこと)

---

## URL ソース一覧

| テーブル | 列 | 優先度 | 備考 |
|---|---|---|---|
| `table=persons` | `images` (JSON: `small/grid/medium/large`) | **最高** | person portrait、正規 |
| `table=characters` | `images` (JSON: `small/grid/medium/large`) | **最高** | character portrait、正規 |
| `table=subject_persons` | `images` (JSON: `small/grid/medium/large`) | 補助 | per-subject サムネ。persons と同一 URL が多い。DL 後 dedup で自動排除 |
| `table=subject_characters` | `images` (JSON: `small/grid/medium/large`) | 補助 | 同上。characters と同一 URL が多い |

**推奨**: `large` バリアントのみ DL (デフォルト `--variant large`)。
`small/grid/medium` はインライン表示用でサーバー側リサイズ版。マスター画像は `large` のみ保存して十分。

---

## コスト見積もり

| 項目 | 推定値 | 根拠 |
|---|---|---|
| ユニーク URL (persons large) | ~30-50k | 全 person の ~60-80% が非空 URL |
| ユニーク URL (characters large) | ~5-10k | 全 character の ~40-60% が非空 URL |
| 合計ユニーク URL | **~35-60k** | persons + characters dedup後 |
| DL 時間 (1 req/sec) | **10-17 時間** | 35k÷1 ≈ 9.7h 〜 60k÷1 ≈ 16.7h |
| 想定ファイルサイズ | 平均 100KB / 枚 | bangumi CDN 実測値 |
| 合計ディスク | **3.5-6 GB** | 35-60k × 100KB |

> バックフィル完走後に `--dry-run` で正確な数値を確認してから実行開始すること。

---

## 出力レイアウト

```
data/bangumi/images/
├── persons/
│   ├── 953.jpg        # id.ext (ext は URL から推定)
│   ├── 958.jpg
│   └── ...
└── characters/
    ├── 1.jpg
    └── ...

result/bronze/source=bangumi/table=image_manifest/
└── date=YYYYMMDD/
    └── {uuid}.parquet   # manifest (1 UUID file per run)

data/bangumi/checkpoint_images.json
```

### manifest parquet schema

| 列 | 型 | 説明 |
|---|---|---|
| `kind` | string | `"person"` または `"character"` |
| `id` | int64 | bangumi 内部 ID |
| `variant` | string | `"large"` (または `"medium"` 等) |
| `url` | string | 元 CDN URL |
| `local_path` | string | `data/bangumi/images/persons/953.jpg` |
| `bytes` | int64 | ダウンロード済みファイルサイズ |
| `sha256` | string | 16 進文字列 (64 char) |
| `content_type` | string | `image/jpeg` 等 |
| `fetched_at` | timestamp[us, UTC] | ダウンロード完了時刻 |

---

## Files to create

| File | 内容 |
|---|---|
| `scripts/download_bangumi_images.py` | typer CLI スケルトン (既存 script、カード作成時点で生成済み) |

## Files to NOT touch

| File | 理由 |
|---|---|
| `src/scrapers/bronze_writer.py` | `bangumi` 既に ALLOWED_SOURCES に含まれる |
| `src/scrapers/bangumi_scraper.py` | CDN ホストは別ホスト。新規 `asyncio.Lock` ベースのリミッターを script 内で宣言 |
| `src/scrapers/**` | scraper 改修不要 |
| `scripts/scrape_bangumi_*.py` | 完了済み scraper |
| `TASK_CARDS/08_bangumi_scraper/01..06_*.md` | 完了済みカード |

---

## Steps

### Step 1: dry-run で規模確認

```bash
pixi run python scripts/download_bangumi_images.py --dry-run --variant large
```

期待出力例:
```
Unique URLs  persons=42,318  characters=7,104  total=49,422
Sample URLs:
  [person  953] https://lain.bgm.tv/pic/crt/l/92/32/953_prsn_anidb.jpg
  ...
Estimated wall time @ 1 req/s : 13h 44m 42s
Estimated disk (100 KB/img)   : 4.9 GB
```

### Step 2: 少量テスト (`--limit 5`)

rate budget に余裕があることを確認してから:

```bash
pixi run python scripts/download_bangumi_images.py --limit 5 --variant large
```

確認:
- `data/bangumi/images/persons/` に 5 ファイル生成
- manifest parquet 1 ファイル生成 → DuckDB で内容検証

```bash
python -c "
import duckdb
rows = duckdb.connect().execute(
    \"SELECT kind, id, bytes, sha256 FROM read_parquet('result/bronze/source=bangumi/table=image_manifest/**/*.parquet')\"
).fetchall()
for r in rows: print(r)
"
```

### Step 3: 全件実行 (ユーザー承認必須)

```bash
# tmux / screen 内で実行推奨 (10-17h)
pixi run python scripts/download_bangumi_images.py --variant large --resume
```

中断再開は `--resume` (デフォルト) で安全。checkpoint は 100 件ごとに atomic write。

### Step 4: manifest 統合確認

```bash
python -c "
import duckdb
stats = duckdb.connect().execute(\"\"\"
    SELECT kind, COUNT(*) AS n, SUM(bytes)/1024/1024 AS mb
    FROM read_parquet('result/bronze/source=bangumi/table=image_manifest/**/*.parquet')
    GROUP BY kind
\"\"\").fetchall()
for r in stats: print(r)
"
```

---

## Verification

```bash
# 1. dry-run (ネットワーク不使用、URL 数カウントのみ)
pixi run python scripts/download_bangumi_images.py --dry-run --limit 5

# 2. 少量 DL (5 件)
pixi run python scripts/download_bangumi_images.py --limit 5 --variant large

# 3. lint
pixi run lint

# 4. manifest 検証
python -c "
import duckdb
n = duckdb.connect().execute(
    'SELECT COUNT(*) FROM read_parquet(\"result/bronze/source=bangumi/table=image_manifest/**/*.parquet\")'
).fetchone()[0]
print('manifest rows:', n)
assert n >= 5, 'manifest too small'
print('OK')
"

# 5. ファイル存在確認
ls data/bangumi/images/persons/ | head -10
```

---

## Stop-if conditions

- [ ] CDN から **429 レスポンス** → 5 分 cooldown 後にレートを 0.5 req/sec に下げて再試行。2 回連続 429 で停止しユーザーに報告
- [ ] `df -h data/bangumi/images/` が **50GB 超** → 即停止 (設定誤りの可能性)
- [ ] SHA256 検証失敗 (partial download) → 該当ファイル削除 → checkpoint から除外して次回再試行
- [ ] `pixi run lint` 失敗
- [ ] `data/bangumi/images/` の空きが 500MB を切る

---

## Rollback

```bash
# DL 済みファイルの削除 (全件)
rm -rf data/bangumi/images/persons/ data/bangumi/images/characters/
rm -f data/bangumi/checkpoint_images.json
# manifest parquet 削除
rm -rf result/bronze/source=bangumi/table=image_manifest/

# git で script を戻す場合
git checkout scripts/download_bangumi_images.py
```

---

## Completion signal

- [ ] `--dry-run` で URL 数・ETA 正常表示
- [ ] `--limit 5` で manifest 5 行 + ファイル 5 個確認
- [ ] 全件完走 (ユーザー承認後) または checkpoint resume 動作確認
- [ ] `pixi run lint` pass
- [ ] DONE 記録
