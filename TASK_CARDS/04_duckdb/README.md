# 04_duckdb — DuckDB 全面移行 (BRONZE Parquet + SILVER/GOLD DuckDB)

**優先度**: 🟠 Major (`01` → `02` → `03` → `04` → `05` → `06` 順)
**前提**: `01_schema_fix/` 全完了、`02_phase4/` と `03_consistency/` の他カード完了

---

## 設計方針 (確定)

```
scrapers ──→ bronze/source=X/date=Y/*.parquet   (per-source, append-only, 競合なし)
                       │
                       ▼ (単一プロセス ETL = pixi task)
                 silver.duckdb                  (consolidated, atomic swap で更新)
                       │
                       ▼ (pipeline 1 回/run、atomic swap)
                 gold.duckdb                    (consolidated, atomic swap で更新)
```

**根拠** (前回設計議論で確定):

1. **per-source Parquet** にする理由: 並列スクレイピングで write 競合を完全に消す、scraper 単位のクラッシュ分離、選択的再実行 (`rm -rf bronze/source=X/date=Y/`)
2. **per-source DuckDB は使わない**: ATTACH 跨ぎで分析 query が 2-5x 遅い
3. **silver/gold は単一 DuckDB**: 分析クエリは consolidated に対して走るので速度劣化なし
4. **atomic swap**: スクレイピング/ETL と分析が同時実行される運用想定 (`os.replace()` で writer block を消す)

却下案:
- ❌ scraper が直接 silver.duckdb に書く: writer 1 本制約で並列不可
- ❌ Queue + writer thread 単一プロセス: クラッシュ分離・選択的再実行の実装コストが per-source Parquet より重い

---

## カード構成

```
04_duckdb/
├── README.md                       本ファイル
├── 01_bronze_writer.md             src/scrapers/bronze_writer.py 新設 (~40 行)
├── 02_scraper_migration.md         6 scraper を bronze_writer 経由に置換
│                                     (03_consistency/01 を吸収)
├── 03_integrate_etl.md             parquet → silver.duckdb + atomic swap
├── 04_gold_atomic_swap.md          gold_writer.py に atomic swap を組み込み
├── 05_analysis_cutover.md          src/analysis/ の SQLite 経路を silver.duckdb に切替
└── 06_sqlite_decommission.md       database.py から SQLite 撤去
```

**01-04 は順序厳密**。05 は 03 完了後に並行可。06 は 05 完了が必須。

---

## 03_consistency/01 との関係

`03_consistency/01_scraper_unification.md` は **本セクション 02 が吸収** する。理由:

- 03_consistency/01 は scraper を `upsert_canonical_anime` (SQLite ラッパー) 経由に統一する案
- だが本セクションでは scraper を Parquet 出力に切替えるため、SQLite ラッパーへの一時的迂回は無駄
- **scraper 改修は 1 回で済ませる** → 04_duckdb/02 で直接 `bronze_writer.append()` 化

→ 03_consistency/01 は本セクション着手時に **`SUPERSEDED.md` にリネーム** (削除はしない、判断履歴として保持)

---

## メモリ・ディスク予算

| Resource | 予算 | 対策 |
|---|---|---|
| RAM (integrate process) | 2-4 GB | `PRAGMA memory_limit='2GB'` を connection 開設時に明示 |
| RAM (analysis process) | 2-4 GB | 同上、independent connection ごとに設定 |
| Disk: silver.duckdb 容量 | < 5 GB 想定 | atomic swap 中の peak は old + new + bronze parquet で **3x** |
| Disk: bronze parquet 累積 | 日次 partition、~6 ソース x 日数 | 月次で古い partition を圧縮 or アーカイブ (将来) |
| Spill | `temp_directory` | `PRAGMA temp_directory='/tmp/duckdb_spill'` |

**RAM の見落とし**: integrate と analysis が同時に DuckDB を開くと、各 connection が default で `0.8 * total_RAM` を取りに行く → OOM リスク。**memory_limit の明示は必須** (各カードの "Hard constraints" に記載)。

---

## 成功判定 (本セクション全完了の条件)

- [ ] BRONZE は scraper が parquet 出力するのみ。SQLite に書かない
- [ ] integrate ETL が parquet → silver.duckdb を atomic swap で更新
- [ ] pipeline は silver.duckdb 読み + gold.duckdb 書き (atomic swap)
- [ ] analysis / API / report は silver.duckdb / gold.duckdb のみ参照
- [ ] スクレイピング走行中も分析クエリが block されない (atomic swap が機能している)
- [ ] SQLite 依存コード削除、`pixi run test` 全 pass
- [ ] benchmarks: Phase 5/6 が SQLite 比 2x 以上高速 (現状の `benchmarks/bench_duckdb.py` ベース)

---

## 参考

- `TODO.md §4` 全文
- `benchmarks/bench_duckdb.py` (Phase A PoC ベンチ)
- `src/analysis/duckdb_io.py` (Phase A 実装、ATTACH パターン)
- `src/analysis/gold_writer.py` (Phase B 実装、本セクション 04 で atomic swap 拡張)
