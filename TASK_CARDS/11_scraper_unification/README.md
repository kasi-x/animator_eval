# 11_scraper_unification — scraper 共通化第 2 弾

**Owner**: Sonnet
**Started**: 2026-04-25
**Goal**: 残りの scraper 共通化を完了し、HTTP client / progress / CLI を全 scraper で揃える。

## 背景

これまでに完了済 (DONE):

- `src/scrapers/bronze_writer.py` — `BronzeWriter` (`compact_on_exit=True`) + `BronzeWriterGroup`
- `src/scrapers/bronze_compaction.py` — atomic merge + CLI (`--all` / `--dry-run`)
- `src/scrapers/checkpoint.py` — `Checkpoint` クラス + `atomic_write_json` / `load_json_or`
- `src/scrapers/cli_common.py` — `LimitOpt` / `DryRunOpt` / `ResumeOpt` / `ForceOpt` / `QuietOpt` / `ProgressOpt` / `DelayOpt` / `DataDirOpt` / `CheckpointIntervalOpt`
- `src/scrapers/progress.py` — `scrape_progress(total, description, enabled)` (TTY 自動 / `--quiet` / `--progress` / env)

各 scraper の対応:

| scraper | progress 統合 | `--limit` alias | `--quiet/--progress` | 共通基底 (Bronze/Checkpoint) |
|---|---|---|---|---|
| ANN | ✅ | ✅ (元から) | ✅ | ✅ |
| allcinema | ✅ | ✅ (元から) | ✅ | ✅ |
| mal | ✅ | ✅ (alias) | ✅ | ✅ |
| mediaarts | ✅ | ✅ (alias) | ✅ | ✅ |
| keyframe | ✅ | ✅ (alias) | ✅ | ✅ |
| jvmg | ✅ | ✅ (alias) | ✅ | ✅ |
| seesaawiki | ❌ (CLI のみ、内部 loop 未統合) | ✅ (alias) | ✅ (CLI のみ) | ✅ |
| anilist | ❌ (元から Rich Progress 利用、共通化未着手) | ❌ | ❌ | △ (一部) |
| bangumi 3 script | ✅ | ✅ (元から) | ✅ | ✅ |

## 残タスク

| Card | Title | Priority | Risk |
|---|---|---|---|
| `01_anilist_cli_unify` | anilist_scraper を共通 CLI / progress に統合 | 🟠 | 中 (ファイル巨大、closure 多い) |
| `02_seesaawiki_progress` | seesaawiki 内部 loop に scrape_progress 統合 | 🟡 | 中 (3 分岐 + 並列 for ループ) |
| `03_http_client_base` | rate-limit + retry の共通基底クラス抽出 | 🔴 | 高 (テスト無、挙動繊細) |
| `04_verification` | 全 scraper の `--limit 1 --quiet` 実走 + 共通機能 verify | 🟢 | 低 |

## 実行順

1. `01_anilist_cli_unify` (独立、verify 容易)
2. `02_seesaawiki_progress` (独立)
3. `03_http_client_base` (1 + 2 後、最大の構造変更)
4. `04_verification` (最後の総合検証)

## 共通の Hard Rules

- **`anime.score` を scoring に使わない** (`_hard_constraints.md` H1)
- **既存 CLI flag 名を破壊しない**: 既存 option (`--count` / `--max-records` 等) は残し、`--limit` 等は alias として追加
- **既存挙動を変えない**: 進捗表示の見た目は変わるが、scrape の throughput / retry / rate limit は変えない
- **本番 scrape 実行中**: `pixi run python -m src.scrapers.ann_scraper` 等が並列で動いている可能性があるため、CLI flag 削除は禁止 (alias 追加のみ)
