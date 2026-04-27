# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-27 (allcinema 削除反映)

本書はプロジェクト内のすべての**未完了**項目を一元管理するファイルです。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟠 High | ローカル再 parse | seesaawiki raw HTML (8688) / madb raw JSON (603MB) の parser 拡張 — 再 scrape 不要でオプション情報回収 (§10) |
| ✅ Done | ANN scraper 改修 | Card 01-04 全完了 (2026-04-26) — anime 27,000 / persons 36,350 / 9 BRONZE テーブル生成済 → `TASK_CARDS/10_ann_scraper_extend/` |
| 🟠 High | MAL/Jikan scraper | Jikan v4 全 endpoint 網羅 (anime/persons/characters/producers/manga + news/schedules/magazines)、28 BRONZE テーブル、3 Phase → `TASK_CARDS/12_mal_scraper_jikan/` (§12.3 から起票) |
| 🟡 Maintenance | スキーマ後続 | v56 既存データ再スクレイプ (name_ja 誤入り修正)、v57 title.native |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ |
| ✅ Done | DuckDB | Card 05 era_fe / era_deflated_iv / opportunity_residual 実装 — era_fe + era_deflated_iv 完了 (export_and_viz.py)、opportunity_residual も individual_profiles から読み込み済み (2026-04-24) |
| 🟡 Medium | Report methodology | Temporal foresight Section 3.3 holdout validation 実装 (Option A、feat_career_annual / feat_person_scores データ投入後)。Option B (記述的分析rename) 完了 2026-04-24 |
| ✅ Done | Test coverage | scraper E2E: anilist_scraper / seesaawiki_scraper / ann_scraper integration tests (18 cases, 6 each) — 2026-04-24 |
| ✅ Done | Test coverage | llm_pipeline.py (574 lines): 50 unit + integration tests (eb3837a) — 2026-04-24 |
| ✅ Done | Test coverage | seesaawiki parser unit tests: 82 cases (_split_names_paren_aware, _is_company_name, _clean_name, parse_credit_line, _parse_episode_ranges, parse_series_staff, parse_episodes) — 2c77147 — 2026-04-24 |
| ✅ Done | Test coverage | structural_estimation.py (AKM / causal): 19 unit tests (FE recovery, SE scaling, order invariance, DID contract, parallel trends, placebo, edge cases) — 2026-04-24 |

**完了済み大項目** (→ `DONE.md`): anime.score 汚染除去、Phase 1-4 基盤、DuckDB §4 全フェーズ、Hamilton H-1〜H-7 (PipelineContext 完全削除)、レポート統廃合 §8、アーキテクチャ §9/11、ドキュメント §12、テストカバレッジ §6、feat_* 層別分離 §13、scraper queries/parsers 分離 §3、§7.1 差分更新 (hash比較フィルタ + E2E)、§7.3 retry refactor、§9 similarity/recommendation スタブ化

---

## SECTION 1: スキーマ後続タスク

### v56 多言語名対応

- [x] ANN / allcinema スクレイパーの `name_ko`/`name_zh` 対応 (3c45ab6 + bdba63f)
- [x] `backfill_anilist_hometown.py` script + tests 実装 (573fed0, 13 tests pass)
- [x] dry-run 実行確認 (2026-04-24): persons テーブル空 → 対象ゼロ、将来データ入投入時用 script として待機

### v57 構造的メタデータ

- [x] `title.native` を `country_of_origin` 分岐で `titles_alt` JSON へ格納 (4ec1003 実装完了、assign_native_title_fields + parser フィールド追加)

---

## SECTION 3: データ修正 残務 ✅

- [x] WIKIDATA_ROLE_MAP 修正済 (813d684)
- ~~既存 JVMG credits 再マップ~~: **不要** (JVMG データは SILVER 未統合 → 旧マッピング汚染なし)
- [x] **オプション**: JVMG 初回統合試行 → 見送り
      - 理由: Wikidata SPARQL エンドポイント持続的 rate limit (429/504)
      - 再試行: Wikidata API quota 解放時に `jvmg_fetcher` を再実行可能 (scraper_cache は整備済)

---

## SECTION 7: スクレイパー強化残務

### 7.1 差分更新 — Parquet + DuckDB ベース ✅
- [x] `hash_utils.py`, anilist/ann/allcinema/seesaawiki hash 計算
- [x] integrate_duckdb.py REPLACE upsert, anilist `--since YYYY-MM-DD` mode
- [x] hash 比較フィルタリング (UPDATE skip) — ecd6477
- [x] E2E テスト (hash差分検出) — 1a8dfcd

### 7.3 anilist_scraper retry refactor ✅ (3cf8ad1)

### 7.4 ANN scraper 再実行 ✅ DONE
- [x] Card 10/04 にて完遂 — anime 27,000 / persons 36,350、9 BRONZE parquet 生成完了 (2026-04-26)

---

## SECTION 9: アーキテクチャ整理 ✅

similarity.py / recommendation.py はスタブ化済 (2行)、重複整理完了。

---

## SECTION 10: ローカル raw データ再 parse (オプション情報回収)

再 scrape 不要、ローカル HTML/JSON を parser 拡張で再抽出。

### 10.1 seesaawiki raw HTML 再 parse

- **対象**: `data/seesaawiki/raw/*.html` (8,688 ファイル / 1.5GB)
- **parser**: `src/scrapers/parsers/seesaawiki.py` 拡張
- **追加抽出候補**:
  - [ ] 各話クレジット詳細 (作画監督/原画/動画/背景/撮影/編集)
  - [ ] グロス請けスタジオ (「制作協力」表記)
  - [ ] 主題歌アーティスト情報 (作詞/作曲/編曲/歌手、OP/ED/挿入歌)
  - [ ] 製作委員会構成
  - [ ] 各話タイトル
  - [ ] 原作情報 (出版社/レーベル/連載誌)
  - [ ] credit 記載順を `source_listing_position` で保持 (ED 順 proxy)
- **手順**:
  - [ ] 追加フィールドを拾う parser 関数追加 + unit test
  - [ ] `BronzeWriter` 用 dataclass に列追加 (schema v58)
  - [ ] 全 HTML 再 parse → BRONZE parquet 書き直し
  - [ ] SILVER 再統合

### 10.2 madb raw JSON 再 parse ✅ BRONZE 完了 (2026-04-27)

- **対象**: `data/madb/metadata*.json` (603MB、SPARQL 結果生データ)
- **parser**: madb integrate 経路を拡張
- **追加抽出候補**:
  - [x] broadcaster (放送局リスト、ネット局数) → BRONZE `broadcasters` 332,492 行
  - [x] 放送時間帯 / 放送枠 → BRONZE `broadcast_schedule` 4,626 行
  - [x] 製作委員会メンバー (`producedBy` 複数) → BRONZE `production_committee` 15,352 行
  - [x] 製作会社群 (main + 協力分離) → BRONZE `production_companies` 29,434 行
  - [x] 映像ソフト (DVD/BD) 発売情報 → BRONZE `video_releases` 292,148 行
  - [x] 原作情報 (manga/LN マスタへの link) → BRONZE `original_work_links` 3,576 行
- **手順**:
  - [x] SPARQL レスポンスに含まれるプロパティの網羅調査 (metadata*.json の JSON schema 確認)
  - [x] 追加フィールド extraction 関数 + test (96 tests PASS)
  - [x] BRONZE 書き直し (`result/bronze/source=mediaarts/table=*/date=2026-04-27/`)
  - [ ] SILVER 再統合 (別タスク)

---

## SECTION 11: ANN scraper / parser 改修

→ `TASK_CARDS/10_ann_scraper_extend/` に全面移管 (2026-04-24 起票)。

**方針確定** (2026-04-24):
- Option B (今回再 scrape、raw cache 層は将来化)
- BRONZE parquet は複雑でも raw に近い形式で保存 (8 テーブル構成)
- SILVER 移行設計は後続で再検討

**カード**:
- `10/01_schema_design` ✅ DONE — BRONZE 8 テーブル dataclass 確定
- `10/02_parser_extend` ✅ DONE — `parsers/ann.py` 拡張 (XML + HTML 全フィールド)
- `10/03_scraper_integration` ✅ DONE — `ann_scraper.py` BronzeWriterGroup 8 テーブル書き分け
- `10/04_rescrape` ✅ DONE — anime 27,000/27,000 (2026-04-25), persons 36,350/36,350 (2026-04-26)、9 BRONZE parquet 生成完 (anime 11,009 / credits 305,174 / persons 36,235 / cast 250,048 / company 33,857 / episodes 169,115 / releases 22,603 / news 90,234 / related 15,198)

旧 §7.4 「ANN 再 scrape」は本カード 04 に統合済。

---

## SECTION 12: 他ソース拡張 (将来)

### 12.1 AniList GraphQL query 拡張 🟡 部分実装

**Query 本体 + BRONZE 完了** (`src/scrapers/queries/anilist.py` 全フィールド実装、BronzeAnime/Person/Character/CVA model 対応済)

**残務 — SILVER 統合側**:
- [ ] `characters` / `character_voice_actors` SILVER ローダー実装 (`integrate_duckdb.py:integrate()` — DDL は `:213,232` に存在、parquet ロード SQL 未接続)
- [ ] anime 拡張列を SILVER `_ANIME_SQL_INSERT_TMPL` にマップ: `external_links_json` / `airing_schedule_json` / `trailer_url` / `trailer_site` / `rankings_json`
- [ ] staff `homeTown` v56 backfill 起動 (persons データ投入時、§1 参照)

**完了済**:
- anime: `source` / `season` / `seasonYear` / `relations` / `studios` (main+協力) / `tags` (rank 付き) ✅ SILVER 統合済
- staff: `yearsActive` / `primaryOccupations` / `dateOfBirth` ✅ 全層実装済

### 12.2 ~~allcinema parser 拡張~~ ❌ 廃止
allcinema scraper 削除済 (commit 300345e)。再導入予定なし (規制強・情報量少でコスパ悪い)。

### 12.3 MAL / Jikan 本格実装 → TASK_CARDS/12_mal_scraper_jikan/

🟠 **Cards 01-04 実装完了 (2026-04-25)** / Card 05 (全件 scrape) のみ未実行

| Card | 状態 | 内容 |
|------|------|------|
| 01_schema_design | ✅ done | 30 dataclass 実装 (MalAnimeRecord + 29)、display_* prefix 全列 |
| 02_parser_extend | ✅ done | 22 parser 関数 + 50 unit test (tests/unit/test_mal_parsers_extended.py) |
| 03_scraper_phases | ✅ done | mal_scraper.py 全置換 — 3 Phase + 30 BronzeWriter + checkpoint |
| 04_rate_limit_strict | ✅ done | DualWindowRateLimiter (http_base.py) + 8 unit test (tests/scrapers/test_http_rate_limit.py) |
| 05_rescrape | 🟠 pending | 全件 ~9.4 日完走 — `pixi run python -m src.scrapers.mal_scraper` で開始可 |

旧予定 (characters + VA AniList 補完) は Card 02-03 に内包。

### 12.4 新ソース (優先度低)
- LiveChart.me (放送スケジュール精密化)
- TMDb (劇場アニメ国際配信)
- Wikidata 受賞データ (rate limit 解放待ち)

---

## SECTION 13: bangumi.tv BRONZE 統合 ✅

- ✅ 13.1 Archive dump DL + 展開 (`bangumi_dump.py`)
- ✅ 13.2 subject.jsonlines → subjects parquet (type=2 filter)
- ✅ 13.3 subject × persons/characters/person-characters → 3 parquet
  - GraphQL batch (persons) + REST per-subject `/v0/subjects/{id}/characters` (chars+actors)
  - actors を subject 単位で取得、per-character `fetch_character_actors` 廃止 (~32K req 削減)
- ✅ 13.4 persons: REST `/v0/persons/{id}` (gender/blood_type/birth_year 含む)
- ✅ 13.5 characters: REST `/v0/characters/{id}` (gender 含む、actors は relations で取得済み)
- ✅ ANIME_POSITION_LABELS: bangumi/server 公式ソース準拠 (50+ codes)
- 🟡 13.6 日次差分 API cron — 待機中 (dump 安定運用後)

---

## 実施順序

```
即時 (再 scrape 不要):
  §10.1  seesaawiki raw HTML 再 parse
  §10.2  madb raw JSON 再 parse

短期 (parser 拡張 + 再 scrape):
  TASK_CARDS/10  ANN scraper/parser 改修 + 再 scrape (旧 §11) ✅ 2026-04-26
  §12.1           AniList SILVER 統合残務 (characters loader / 拡張列 INSERT)

中期:
  §12.3  MAL/Jikan 本格実装 (Card 05 全件 scrape ~9.4 日)
  §1     v56 既存データ再スクレイプ (backfill_anilist_hometown.py)
  §3     JVMG credits 再マップ (WIKIDATA_ROLE_MAP 確定後)

長期:
  §12.4  新ソース (LiveChart / TMDb)
  §13    bangumi.tv BRONZE 統合 ✅ (dump + GraphQL + REST 完了、差分 cron のみ残)
  §1 v57 title.native → titles_alt (v58 実施時)

scraper 内部設計 (リファクタ):
  TASK_CARDS/13_scraper_runner_refactor/  ✅ 完了
    01_http_client_dedupe        ✅ RetryingHttpClient 統合 + tenacity 導入
    02_cache_store_expansion     ✅ HtmlFetcher/XmlBatchFetcher/JsonFetcher (fetchers.py)
    03_bronze_sink               ✅ BronzeSink (sinks.py) + BronzeWriterGroup
    04_runner_abstraction        ✅ ScrapeRunner (runner.py) + allcinema/ann 移行
    ann anime batch phase        ✅ _AnimeBronzeWriters 削除 → BronzeWriterGroup context manager
    keyframe phase2/3            ✅ BronzeWriter ×5/4 → BronzeWriterGroup 統合 + dict checkpoint → Checkpoint クラス移行
    http_base.py 削除            ✅ DualWindowRateLimiter → http_client.py、RateLimitedHttpClient 継承除去
    retry_if_result バグ修正     ✅ context manager → callable パターン (AsyncRetrying(...)(_once))
    keyframe cli_common 統一     ✅ DelayOpt/LimitOpt/ForceOpt/DataDirOpt 適用
    keyframe RetryingHttpClient  ✅ KeyframeApiClient 独自 retry → RetryingHttpClient 委譲
    keyframe Phase3 ScrapeRunner ✅ person ループ → ScrapeRunner + cache_store (gzip 廃止)
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
