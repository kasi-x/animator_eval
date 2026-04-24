# 07_json_to_parquet — 既存 JSON スクレイプデータ → BRONZE Parquet 移行

**作成日**: 2026-04-24
**目的**: `data/{seesaawiki,madb,ann,allcinema}/**/*.json` に蓄積済みの生データを、新 BRONZE Parquet 経路 (`result/bronze/source=*/table=*/date=*/*.parquet`) に取り込む。HTTP 再取得せず、既存 JSON を読むだけで parquet 出力する一度きり script 群。

---

## 背景

- 新 scraper は `src/scrapers/bronze_writer.BronzeWriter` で parquet 直書き (設計済 / 動作済)
- 既存 scrape 済データは 1.5GB 超の JSON で保存されている (`data/seesaawiki/parsed/` 8712件、`data/madb/metadata*.json` 603M、他)
- checkpoint.json に記載の完了 URL は HTTP skip される (再取得なし)
- しかし Parquet には**新規 scrape 分のみ**しか入らない → 既存 JSON は ETL (`src/etl/integrate_duckdb.py`) から見えない
- 結果として SILVER DuckDB が部分データしか持てない

## ゴール

- [ ] `result/bronze/source=seesaawiki/table={anime,credits,persons,studios,anime_studios}/date=YYYYMMDD/*.parquet` 生成
- [ ] `result/bronze/source=madb/table={anime,credits,persons}/date=YYYYMMDD/*.parquet` 生成
- [ ] `result/bronze/source=ann/table={anime,credits,persons}/date=YYYYMMDD/*.parquet` 生成
- [ ] `result/bronze/source=allcinema/table={anime,credits,persons}/date=YYYYMMDD/*.parquet` 生成
- [ ] `pixi run python -m src.etl.integrate_duckdb --refresh` が parquet を読み SILVER に投入できる
- [ ] 既存 JSON は**削除しない** (ロールバック保険)

## Non-goals

- scraper 本体の変更 (新規 scrape 経路は既に parquet 書き出し済)
- SQLite `src_*` テーブルの扱い (本カードでは触らない、後続カードで廃止)
- seesaawiki raw HTML (`data/seesaawiki/raw/*.html` 888M) の変換 (BLOB 不要、parse 済 JSON 優先)

---

## カード一覧と依存関係

```
01 共通 utils (bronze_writer 確認 + date 推定 + dry-run)
  ↓
02 seesaawiki (parsed/*.json → parquet)    ← 最大ボリューム
03 allcinema (checkpoint_cinema.json → parquet)
04 ann (JSON 不在時は no-op)
05 madb (metadata*.json → parquet)
  ↓
06 E2E 検証 (integrate_duckdb.py refresh + SILVER 行数確認)
```

各カードは **独立 script** (`scripts/migrate_<source>_to_parquet.py`)。全体 orchestrator は作らない (Haiku が 1 ファイルずつ処理)。

---

## Hard constraints (_hard_constraints.md 参照)

- H1 anime.score: BRONZE parquet に score フィールドを保持してよい (BRONZE は生データ)。SILVER 段階で除去済
- H3 entity resolution: 本タスクは BRONZE のみ、entity resolution は SILVER 層 → 不変
- **破壊的操作禁止**: JSON ソースを削除・移動しない。parquet 出力のみ
- **冪等性**: 複数回実行しても parquet ファイルが UUID で分離される (bronze_writer 仕様)
- **日付推定**: JSON ファイル `mtime` を `date=` partition に使う (scrape 日時を保持)

---

## 実装パターン (全カード共通)

```python
# scripts/migrate_<source>_to_parquet.py
from pathlib import Path
from datetime import date
import json, typer
from src.scrapers.bronze_writer import BronzeWriter, DEFAULT_BRONZE_ROOT

app = typer.Typer()

@app.command()
def main(
    source_dir: Path = typer.Option("data/<source>", help="JSON 入力ディレクトリ"),
    bronze_root: Path = typer.Option(DEFAULT_BRONZE_ROOT, help="Parquet 出力 root"),
    dry_run: bool = typer.Option(False, help="parquet 書き込まずログのみ"),
    scrape_date: str | None = typer.Option(None, help="YYYY-MM-DD, default=ファイル mtime"),
) -> None:
    # 1. JSON 走査
    # 2. 各 table 用 BronzeWriter 開く
    # 3. JSON → Bronze* モデル変換 → .append()
    # 4. flush() で parquet 書き出し
    # 5. 件数レポート
```

---

## Completion signal

- 全 5 カード完了 (01-05) → TODO.md にチェック
- 06 E2E script が SILVER 行数を期待値と比較して pass
- `git log --oneline -6` に各 migration script commit が並ぶ
