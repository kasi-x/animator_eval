# Task: MADB (文化庁メディア芸術DB) metadata → BRONZE Parquet

**ID**: `07_json_to_parquet/05_madb`
**Priority**: 🟡
**Estimated changes**: 約 +180 lines, 1 file (新規 script)
**Requires senior judgment**: yes (JSON-LD 構造解析必要)
**Blocks**: `06_e2e_verify`
**Blocked by**: `01_common_utils`

---

## Goal

`data/madb/metadata{201,...,504}.json` (16 ファイル, 603M) を parse し、`result/bronze/source=madb/table={anime,credits,persons}/date=YYYYMMDD/*.parquet` に書き出す。

---

## Input データ構造

MADB (文化庁メディア芸術データベース) の JSON-LD 形式:

```json
{
  "@context": {...},
  "@graph": [
    {
      "@id": "...",
      "@type": "schema:CreativeWork",
      "name": "...",
      "creator": [...],
      "datePublished": "...",
      ...
    },
    ...
  ]
}
```

`metadata<class>.json` の `<class>` は作品分類コード (201=TVアニメ, 202=OVA, 等)。詳細は `src/scrapers/mediaarts_scraper.py` の parse ロジックを参照。

---

## Pre-condition 調査 (最初に必ずやる)

```bash
pixi run python -c "
import json
d = json.load(open('data/madb/metadata201.json'))
print('top keys:', list(d.keys()))
graph = d.get('@graph', [])
print('graph entries:', len(graph))
if graph:
    print('first entry keys:', list(graph[0].keys())[:20])
    print('first entry @type:', graph[0].get('@type'))
"
ls data/madb/metadata*.json | wc -l
```

`mediaarts_scraper.py` に既存 parse ロジックがある場合はそれを import して再利用:

```bash
grep -n "def parse\|def extract" src/scrapers/mediaarts_scraper.py | head
grep -n "@graph\|jsonld" src/scrapers/mediaarts_scraper.py | head
```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/migrate_madb_to_parquet.py` | 新規作成 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/mediaarts_scraper.py` | 既存動作中、import 専用 |
| `data/madb/**` | 入力データ、read-only |

---

## Steps

### Step 1: mediaarts_scraper parse ロジック再利用調査

```bash
grep -n "def .*parse\|extract_anime\|extract_credit\|BronzeAnime" src/scrapers/mediaarts_scraper.py | head -20
```

既存 parse 関数があれば `from src.scrapers.mediaarts_scraper import parse_graph_entry` 等で import 再利用。なければ Step 2 で新規実装。

### Step 2: script 実装

雛形は `02_seesaawiki.md` 参照。差分:

- source = `"mediaarts"` (bronze_writer の ALLOWED_SOURCES に `mediaarts` あり、line 33)
- tables = `["anime", "credits", "persons"]`
- 入力 = `data/madb/metadata*.json` の `@graph` をイテレート
- `@type` が作品 → anime テーブル、人物 → persons、creator 関係 → credits

```python
def _iter_graph_entries(metadata_dir: Path) -> Iterator[tuple[Path, dict]]:
    """各 JSON-LD ファイルの @graph を yield."""
    for p in sorted(metadata_dir.glob("metadata*.json")):
        doc = load_json(p)
        if doc is None:
            continue
        for entry in doc.get("@graph", []):
            yield p, entry


def _classify(entry: dict) -> str:
    """'anime' | 'person' | 'credit' | 'skip' を返す."""
    t = entry.get("@type", "")
    if "CreativeWork" in t or "TVEpisode" in t or "AnimationSeries" in t:
        return "anime"
    if "Person" in t:
        return "person"
    return "skip"
```

credit は anime entry の `creator` / `contributor` フィールドから展開。

### Step 3: dry-run

```bash
pixi run python scripts/migrate_madb_to_parquet.py --dry-run
```

期待: anime_rows / person_rows / credit_rows が桁違いに 0 でないこと。

### Step 4: 本実行 + 確認

```bash
pixi run python scripts/migrate_madb_to_parquet.py
find result/bronze/source=mediaarts -name "*.parquet" | wc -l
```

---

## Verification

```bash
pixi run lint
pixi run python -c "
import pyarrow.parquet as pq, glob
for tbl in ['anime', 'credits', 'persons']:
    paths = glob.glob(f'result/bronze/source=mediaarts/table={tbl}/date=*/*.parquet')
    total = sum(pq.read_metadata(p).num_rows for p in paths)
    print(f'{tbl}: {len(paths)} files, {total} rows')
"
```

---

## Stop-if conditions

- [ ] `@graph` 構造が mediaarts_scraper の想定と全く違う → user 確認
- [ ] `@type` 分類で全て skip になる → classifier 要見直し

---

## Rollback

```bash
rm -rf result/bronze/source=mediaarts/
rm scripts/migrate_madb_to_parquet.py
```

---

## Completion signal

- [ ] parquet 生成 (anime / credits / persons 全 table)
- [ ] lint pass
- [ ] 作業ログに `DONE: 07_json_to_parquet/05_madb` 記録
