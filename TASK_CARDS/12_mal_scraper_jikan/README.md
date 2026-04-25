# 12 MAL scraper / Jikan v4 全 endpoint 網羅

**Objective**: Jikan v4 API (`https://api.jikan.moe/v4`) から取得可能な全フィールドを raw 近い形で BRONZE parquet へ保存する。スタジオ・アニメーター・アニメ詳細・原作 manga まで網羅。

**方針** (ユーザ確定 2026-04-25):

1. **Option A 採択**: 取得可能な全情報 (anime / persons / characters / **producers (= スタジオ)** / **manga (= 原作)** / news / schedules / magazines) を 28 BRONZE テーブルへ raw 保存。
2. **画像は URL のみ**: `picture_url` 列で文字列保存、binary DL は scope 外 (将来別カード化)。
3. **fetch 追加呼び出し最小**: 既存 `/anime/{id}/full` レスポンスに含まれる relations / themes / external / streaming は full 1 回で取れるが、**完全性検証のため個別 endpoint も叩く** (Jikan の full 内 sub-object が省略されるケースが報告されている)。
4. **3 Phase 構造**: Phase A (anime + 13 sub-endpoint) → Phase B (persons + characters) → Phase C (producers + manga + masters)。各 phase 独立 checkpoint。
5. **rate limit 厳密化**: Jikan 公式 = **3 req/s + 60 req/min**。現 `REQUEST_INTERVAL=0.4` (秒間のみ) → 二重 semaphore へ拡張。
6. **完走見積 ~7.6 日 (~183h)**: Phase A 60h / Phase B 33h / Phase C 90h (manga 主因)。

## 既存資産

- `src/scrapers/mal_scraper.py` (364行) — 基本骨格 + `JikanClient` + `BronzeWriterGroup` 統合済 (`anime/persons/credits` 3 テーブルのみ)
- `src/scrapers/parsers/mal.py` (82行) — `parse_anime_data` / `parse_staff_data` のみ
- `src/scrapers/bronze_writer.py` — `mal` source 登録済 (`ALLOWED_SOURCES`)
- `src/etl/sources.py` — `mal` source prefix `mal-` 登録済
- 実 scrape **未実行** (`data/mal/` / `result/bronze/source=mal/` 不在)

## カード構成

| ID | 内容 | Priority | Estimated |
|----|------|---------|-----------|
| [01_schema_design](01_schema_design.md) | BRONZE 28 テーブル dataclass 確定 (Hard Rule H1: `display_*` prefix 全列洗い出し) | 🟠 | +500 / 1 file |
| [02_parser_extend](02_parser_extend.md) | `parsers/mal.py` に 18+ parser 関数追加 (anime sub / persons / characters / producers / manga / news / schedules / magazines) | 🟠 | +900 / 1 file + tests |
| [03_scraper_phases](03_scraper_phases.md) | `mal_scraper.py` 多 BronzeWriter (28 table) + 3 Phase 構造 + checkpoint 拡張 | 🟠 | +600 / 1 file + integration |
| [04_rate_limit_strict](04_rate_limit_strict.md) | 3 req/s + 60 req/min 二重 semaphore + 429/503 Retry-After 尊重 + backoff | 🟠 | +200 / `http_base.py` 拡張 |
| [05_rescrape](05_rescrape.md) | 全件 scrape (anime ~26000 / persons / characters / producers / manga ~70000) ~183h 完走 | 🟠 | data ~15GB |

## 実施順

`01 → 02 → 03 → 04 → 05` 順守。schema 確定なしに parser 書かない、parser なしに scraper 結合しない、rate limit 強化なしに大量 scrape 開始しない。

## Hard Rules

- **H1 (No viewer ratings in scoring)**: `display_*` prefix 必須列群:
  - `mal_anime`: `display_score`, `display_scored_by`, `display_rank`, `display_popularity`, `display_members`, `display_favorites`
  - `mal_anime_statistics.*` 全列
  - `mal_anime_episodes.display_score`
  - `mal_anime_characters.display_favorites`
  - `mal_persons.display_favorites`
  - `mal_characters.display_favorites`
  - `mal_producers.display_favorites`
  - `mal_manga.display_score`, `display_scored_by`, `display_rank`, `display_popularity`, `display_members`, `display_favorites`
  - 各 dataclass docstring に「scoring path 不参入」明記
- **H3 (Entity resolution accuracy)**: person_id = `mal:p{mal_person_id}` 維持。既存ロジック不変。
- **raw 保存原則**: position 文字列 / role 名 / language 名 / relation_type は **正規化なし** で BRONZE。

## 完了判定

- BRONZE `source=mal/table=*` に 28 parquet partition 存在
- Hard Rule H1 遵守 (rating / score / popularity / favorites / statistics 全部 `display_*` prefix)
- 既存 `parse_anime_data` / `parse_staff_data` の互換維持 (regression なし)
- entity_resolution に渡せる person_id format 維持 (`mal:p{N}`)

## TODO.md 更新先

§12.3 を:
```
🟠 High | TASK_CARDS/12_mal_scraper_jikan/ (5 カード) — Jikan v4 全 endpoint 網羅、28 BRONZE テーブル、3 Phase、~183h 完走
```
