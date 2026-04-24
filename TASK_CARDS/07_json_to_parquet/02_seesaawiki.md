# Task: seesaawiki parsed JSON → BRONZE Parquet

**ID**: `07_json_to_parquet/02_seesaawiki`
**Priority**: 🟠 (最大ボリューム)
**Estimated changes**: 約 +180 lines, 1 file (新規 script)
**Requires senior judgment**: no
**Blocks**: `06_e2e_verify`
**Blocked by**: `01_common_utils`

---

## Goal

`data/seesaawiki/parsed/*.json` (8712件, 573M) を読み、`result/bronze/source=seesaawiki/table={anime,credits}/date=YYYYMMDD/*.parquet` に書き出す。HTTP 再取得ゼロ。

---

## Hard constraints

- H3 entity resolution 不変: 本 script は BRONZE のみ、SILVER 以降の pipeline に触れない
- **破壊的操作禁止**: `data/seesaawiki/parsed/` を削除しない
- **role normalization しない**: BRONZE は生データ、role 文字列はそのまま parquet に入れる

---

## Pre-conditions

- [ ] `07_json_to_parquet/01_common_utils` 完了
- [ ] `ls data/seesaawiki/parsed/*.json | wc -l` が 0 より大きい (スキップ判断)

---

## Input データ構造

`data/seesaawiki/parsed/<safe_title>_<anime_id>.json` の schema:

```json
{
  "title": "モンキーターンV",
  "anime_id": "seesaa:c489efc1eed623f2",
  "parser_used": "structured",
  "body_text_length": 12345,
  "body_text": "...",
  "llm_validation": null,
  "episodes": [
    {
      "episode": null,
      "credits": [
        {"role": "原作", "name": "河合克敏", "position": 0,
         "is_known_role": true, "affiliation": "...", "is_company": false}
      ]
    }
  ],
  "series_staff": [
    {"role": "原作", "name": "河合克敏", "position": 0, "is_known_role": true}
  ],
  "llm_records": []
}
```

注: `parsed/*.json` の多くは `episodes=[]`, `series_staff=[]`, `llm_records=[]` (空) の可能性あり。その場合は `anime` 行のみ出力し `credits` は skip。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/migrate_seesaawiki_to_parquet.py` | 新規作成 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/seesaawiki_scraper.py` | 新規 scrape 経路、既に parquet 対応済 |
| `data/seesaawiki/**` | 入力データ、read-only |

---

## Steps

### Step 1: script skeleton

`scripts/migrate_seesaawiki_to_parquet.py`:

```python
"""One-shot migration: data/seesaawiki/parsed/*.json → BRONZE parquet.

Reads parsed JSON intermediates (saved by seesaawiki_scraper.save_parsed_intermediate),
converts to BronzeAnime / Credit rows, writes partitioned parquet.

Usage:
    pixi run python scripts/migrate_seesaawiki_to_parquet.py
    pixi run python scripts/migrate_seesaawiki_to_parquet.py --dry-run
"""
from __future__ import annotations

import datetime as _dt
from collections import defaultdict
from pathlib import Path

import structlog
import typer

from scripts._migrate_common import (
    group_files_by_date,
    iter_json_files,
    load_json,
    report_summary,
)
from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

log = structlog.get_logger()
app = typer.Typer()

DEFAULT_PARSED_DIR = Path("data/seesaawiki/parsed")


def _extract_anime_row(doc: dict) -> dict | None:
    """parsed JSON → BronzeAnime dict. required: title + anime_id."""
    anime_id = doc.get("anime_id")
    title = doc.get("title") or ""
    if not anime_id:
        return None
    return {
        "id": anime_id,
        "title_ja": title,
        "title_en": "",
    }


def _extract_credit_rows(doc: dict) -> list[dict]:
    """parsed JSON → Credit dicts. episodes + series_staff を統合、dedup."""
    anime_id = doc.get("anime_id")
    if not anime_id:
        return []

    rows: list[dict] = []
    # 1. series_staff (全話共通)
    for credit in doc.get("series_staff", []) or []:
        rows.append(
            {
                "anime_id": anime_id,
                "role": credit.get("role", ""),
                "name": credit.get("name", ""),
                "position": credit.get("position", 0),
                "episode": None,
                "is_company": credit.get("is_company", False),
                "affiliation": credit.get("affiliation"),
            }
        )
    # 2. episodes[].credits (話別)
    for ep in doc.get("episodes", []) or []:
        ep_no = ep.get("episode")
        for credit in ep.get("credits", []) or []:
            rows.append(
                {
                    "anime_id": anime_id,
                    "role": credit.get("role", ""),
                    "name": credit.get("name", ""),
                    "position": credit.get("position", 0),
                    "episode": ep_no,
                    "is_company": credit.get("is_company", False),
                    "affiliation": credit.get("affiliation"),
                }
            )
    return rows


@app.command()
def main(
    parsed_dir: Path = typer.Option(DEFAULT_PARSED_DIR),
    bronze_root: Path = typer.Option(DEFAULT_BRONZE_ROOT),
    dry_run: bool = typer.Option(False),
    scrape_date: str | None = typer.Option(None, help="YYYY-MM-DD override"),
) -> None:
    if not parsed_dir.exists():
        log.error("migrate_seesaa_parsed_dir_missing", path=str(parsed_dir))
        raise typer.Exit(1)

    files = list(iter_json_files(parsed_dir, pattern="*.json"))
    log.info("migrate_seesaa_scanning", count=len(files))

    override = _dt.date.fromisoformat(scrape_date) if scrape_date else None
    buckets = group_files_by_date(files, override_date=override)

    counts = defaultdict(int)

    for d, paths in buckets.items():
        anime_bw = BronzeWriter("seesaawiki", table="anime", root=bronze_root, date=d)
        credits_bw = BronzeWriter("seesaawiki", table="credits", root=bronze_root, date=d)

        for p in paths:
            doc = load_json(p)
            if doc is None:
                counts["json_failed"] += 1
                continue
            anime_row = _extract_anime_row(doc)
            if anime_row is None:
                counts["anime_skipped"] += 1
                continue
            if not dry_run:
                anime_bw.append(anime_row)
            counts["anime_rows"] += 1

            credit_rows = _extract_credit_rows(doc)
            if not dry_run:
                credits_bw.extend(credit_rows)
            counts["credit_rows"] += len(credit_rows)

        if not dry_run:
            anime_bw.flush()
            credits_bw.flush()

    report_summary("seesaawiki", dict(counts), dry_run)


if __name__ == "__main__":
    app()
```

### Step 2: dry-run で件数確認

```bash
pixi run python scripts/migrate_seesaawiki_to_parquet.py --dry-run 2>&1 | tail
```

期待: `anime_rows=<N>, credit_rows=<M>` が出る。`json_failed` が全体の 1% 未満。

### Step 3: 本実行

```bash
pixi run python scripts/migrate_seesaawiki_to_parquet.py
```

### Step 4: 出力確認

```bash
find result/bronze/source=seesaawiki -name "*.parquet" | head
du -sh result/bronze/source=seesaawiki/
pixi run python -c "
import pyarrow.parquet as pq
import glob
# anime
paths = glob.glob('result/bronze/source=seesaawiki/table=anime/date=*/*.parquet')
tbl = pq.read_table(paths[0]) if paths else None
print('anime parquet:', len(paths), 'files, first row:', tbl.slice(0,1).to_pylist() if tbl else 'none')
# credits
paths = glob.glob('result/bronze/source=seesaawiki/table=credits/date=*/*.parquet')
tbl = pq.read_table(paths[0]) if paths else None
print('credits parquet:', len(paths), 'files, first row:', tbl.slice(0,1).to_pylist() if tbl else 'none')
"
```

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. parquet ファイル生成確認
test -n "$(find result/bronze/source=seesaawiki -name '*.parquet' 2>/dev/null)" && echo "PARQUET OK" || echo "PARQUET MISSING"

# 3. SILVER 側の影響なし (テスト不変)
pixi run test-scoped tests/ -k "seesaawiki"
```

---

## Stop-if conditions

- [ ] `json_failed` が全体の 10% を超える (parse 不能 JSON 多発 → 要調査)
- [ ] `anime_rows` が 8712 の 50% 未満 (`anime_id` 欠損多発 → 要調査)
- [ ] parquet 書き込み中に disk full

---

## Rollback

```bash
rm -rf result/bronze/source=seesaawiki/
rm scripts/migrate_seesaawiki_to_parquet.py
```

---

## Completion signal

- [ ] `result/bronze/source=seesaawiki/table=anime/date=*/*.parquet` 存在
- [ ] `result/bronze/source=seesaawiki/table=credits/date=*/*.parquet` 存在
- [ ] 作業ログに `DONE: 07_json_to_parquet/02_seesaawiki` 記録
