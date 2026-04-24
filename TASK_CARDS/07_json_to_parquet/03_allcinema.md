# Task: allcinema checkpoint JSON → BRONZE Parquet

**ID**: `07_json_to_parquet/03_allcinema`
**Priority**: 🟡
**Estimated changes**: 約 +150 lines, 1 file (新規 script)
**Requires senior judgment**: no
**Blocks**: `06_e2e_verify`
**Blocked by**: `01_common_utils`

---

## Goal

`data/allcinema/checkpoint_cinema.json` (1.2M) から anime + credits + persons を抽出し、`result/bronze/source=allcinema/table={anime,credits,persons}/date=YYYYMMDD/*.parquet` に書き出す。

---

## Hard constraints

- H3 entity resolution 不変
- **破壊的操作禁止**: `data/allcinema/` 削除しない

---

## Pre-conditions

- [ ] `01_common_utils` 完了
- [ ] `data/allcinema/checkpoint_cinema.json` 存在

---

## Step 0: 入力スキーマ調査 (Haiku へ最初の指示)

`checkpoint_cinema.json` の構造は `src/scrapers/allcinema_scraper.py` の `_save_checkpoint` 呼び出し箇所 (line 297, 313, 333, 360, 373) と `scrape_allcinema` 関数を読むと推測できる。

まず以下で実データ構造を確認:

```bash
pixi run python -c "
import json
d = json.load(open('data/allcinema/checkpoint_cinema.json'))
print('type:', type(d).__name__)
print('keys:', list(d.keys())[:10] if isinstance(d, dict) else len(d))
# 代表的な 1 件を dump
if isinstance(d, dict):
    for k, v in list(d.items())[:3]:
        print(f'  [{k}]:', type(v).__name__, (list(v.keys())[:5] if isinstance(v, dict) else v[:100] if isinstance(v, (list,str)) else v))
"
```

**注意**: この Step 0 結果に基づいて Step 1 の `_extract_*` 実装を調整すること。スキーマが想定と異なる場合は迷わず `src/scrapers/allcinema_scraper.py` の `scrape_allcinema` / `scrape_persons` / `AllcinemaCredit` / `AllcinemaPerson` を読んで参照すること。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/migrate_allcinema_to_parquet.py` | 新規作成 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/allcinema_scraper.py` | 既存動作中 |
| `data/allcinema/**` | 入力データ、read-only |

---

## Steps

### Step 1: script skeleton

`scripts/migrate_allcinema_to_parquet.py` の雛形は `02_seesaawiki.md` を参照。以下の差分のみ注意:

- source = `"allcinema"`
- tables = `["anime", "credits", "persons"]`
- `_extract_anime_row(doc)` / `_extract_credit_rows(doc)` / `_extract_person_rows(doc)` を `checkpoint_cinema.json` の構造に合わせて実装
- `allcinema_scraper.py` 内の `AllcinemaAnime` / `AllcinemaCredit` / `AllcinemaPerson` dataclass がそのまま BronzeWriter の入力になる (line 272, 276, 282 参照) → 同じ dict 形式を再現

checkpoint_cinema.json が dict of dicts (anime_id → anime_data) 形式の場合:

```python
def _iter_records(checkpoint: dict) -> Iterator[tuple[str, dict]]:
    """(anime_id, record) を yield. スキーマは Step 0 で確認."""
    for aid, rec in checkpoint.items():
        yield aid, rec
```

### Step 2: dry-run + 件数確認

```bash
pixi run python scripts/migrate_allcinema_to_parquet.py --dry-run
```

### Step 3: 本実行

```bash
pixi run python scripts/migrate_allcinema_to_parquet.py
```

### Step 4: 出力確認

```bash
find result/bronze/source=allcinema -name "*.parquet"
pixi run python -c "
import pyarrow.parquet as pq, glob
for tbl in ['anime', 'credits', 'persons']:
    paths = glob.glob(f'result/bronze/source=allcinema/table={tbl}/date=*/*.parquet')
    total = sum(pq.read_metadata(p).num_rows for p in paths)
    print(f'{tbl}: {len(paths)} files, {total} rows')
"
```

---

## Verification

```bash
pixi run lint
test -n "$(find result/bronze/source=allcinema -name '*.parquet' 2>/dev/null)" && echo OK
```

---

## Stop-if conditions

- [ ] `checkpoint_cinema.json` が空または構造が想定と全く違う (→ user 確認)
- [ ] parquet 書き込み失敗

---

## Rollback

```bash
rm -rf result/bronze/source=allcinema/
rm scripts/migrate_allcinema_to_parquet.py
```

---

## Completion signal

- [ ] parquet ファイル生成
- [ ] 作業ログに `DONE: 07_json_to_parquet/03_allcinema` 記録
