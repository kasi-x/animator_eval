# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-23 (Card 05 step 3 完了)

本書はプロジェクト内のすべての未完了項目を一元管理するファイルです。完了済みサマリーは `DONE.md`、設計原則は `CLAUDE.md`。

## 実行指示書は `TASK_CARDS/`

弱いモデル・初見エンジニアが実際に作業する際は `TASK_CARDS/` 配下の個別カードを読んでください。各カードは自己完結 (前提条件・制約・手順・検証・ロールバック) です。

```
TASK_CARDS/
├── README.md                # エントリポイント、実行順序、ルール
├── _hard_constraints.md     # 全タスク共通の絶対遵守事項
├── 01_schema_fix/           # 🔴 Critical (5 cards)
├── 02_phase4/               # 🟠 Major (5 cards)
├── 03_consistency/          # 🟠 Major (5 cards)
├── 04_duckdb/               # ⚠️ SENIOR ONLY
├── 05_hamilton/             # ⚠️ SENIOR ONLY
└── 06_tests/                # 🟡 Minor
```

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🔴 Critical | スキーマ整合 | v55 migration 未登録、DDL 残存、`scores` 物理リネーム未実施 |
| 🟠 Major | Phase 4 残務 | pipeline smoke test、method notes validation gate |
| 🟠 Major | コード一貫性 | scraper 統一、episode sentinel、etl 公開 |
| 🟠 Major | DuckDB 全面移行 | Phase A 完了 (silver_reader + 15 module 移行済)。次: Card 06 GOLD DuckDB 化 |
| 🟠 Major | Hamilton 導入 | `PipelineContext` 解消、DAG 化 (H-1 ~ H-5) |
| 🟠 Major | レポートシステム統廃合 | 3 系統 → 1 系統、v1 monolith 解体、FastAPI 2 系統 → 1 系統 |
| 🟡 Minor | テストカバレッジ | pipeline_phases 未テスト、VA 7 モジュール、モノリスファイル分割 |
| 🟡 Minor | スクレイパー強化残務 | 差分更新、ANN HTML切替、テストカバレッジ |
| 🟡 Maintenance | Schema baseline 固定 | v55 安定後に v1-v55 legacy migration 削除 |
| 🟡 Maintenance | アーキテクチャ整理 | 孤立モジュール削除、Taskfile 圧縮、monitoring リネーム |
| 🟢 Future | レイアウト・命名 | `src/` 平置き解消、`analysis/` 69本平置き → subpackage、docs 整理 |
| 🟢 Future | feat_* 層別分離 | `feat_career` / `feat_network` の L2/L3 分割 |

---

## 🔴 SECTION 1: スキーマ整合性修復

### 根本問題
- `src/database.py:27` に `SCHEMA_VERSION = 54`
- `_migrate_v54_to_v55()` (line 8924) が定義済みだが `migrations` dict に未登録
- `_migrate_v55_to_v56()` (line 9012) はコメントアウト
- `ensure_phase1_schema()` (line 9088) は定義のみで呼び出しなし

### 1.1 `sources` テーブル DDL 衝突解消

**問題**: DDL が 2 系統に分岐
- `init_db()` line 261: PK=`code TEXT`、カラム `name_ja, base_url, license, description`
- `_migrate_v54_to_v55()` line 8938: PK=`id TEXT`、カラム `name TEXT, description TEXT` (別物)

フレッシュインストールは `init_db()` の PK=`code` を作る。v55 が後から `CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY ...)` を流すと `IF NOT EXISTS` でスキップされ壊れる。

**修正** (`src/database.py:8936-8952`):
- [ ] `_migrate_v54_to_v55()` の `sources` DDL を削除
- [ ] seed INSERT のみに変更 (code, name_ja, base_url, license, description の 5 行)

### 1.2 v55 migration の正規登録

- [ ] 1.1 完了後、`_migrate_v54_to_v55` を `migrations[55]` に登録
- [ ] `SCHEMA_VERSION = 55` に更新
- [ ] `ensure_phase1_schema()` を削除

### 1.3 `scores` → `person_scores` 物理リネーム

**参照残存箇所**: `src/api.py` 6箇所、`src/cli.py` 6箇所、`src/database.py` 2箇所、tests 複数

- [ ] v55 migration に追加: `ALTER TABLE scores RENAME TO person_scores` (VIEW 先削除後)
- [ ] 一括置換: `rg -l 'FROM scores\b|INTO scores\b' src/ tests/ | xargs sed -i ...`

### 1.5 `anime_analysis` DDL 除去

**現状**: v50 で `anime` にリネーム済みにもかかわらず `init_db()` に DDL が残存 (line 289, 414, 7457)、インデックス 3 本も残存 (line 440-442)。

- [ ] `init_db()` から `anime_analysis` DDL とインデックス (`idx_anime_analysis_*` 3本) を削除

### 1.6 `_migrate_v55_to_v56` の扱い

ジャンル正規化 (anime_genres/anime_tags JSON 展開) を含むため実行コスト高。本作業では保留。
- [ ] 関数上部に `# STATUS: deferred — high execution cost` コメント付記
- [ ] `migrations` dict に未登録のまま残す

### 🟡 Maintenance: v55 安定後の legacy migration 削除

`src/database.py` が 8,925 行に膨れている主因は v1-v54 の migration 関数群 (~7,000 行)。production DB が v55 で安定したら:
- [ ] `src/db/schema.py` = `init_db()` + 最新 DDL (single source of truth)
- [ ] `src/db/dao.py` = upsert/query ヘルパー群
- [ ] v1-v54 の migration 関数をまとめて削除 (git history で参照可)

### 検証

```bash
pixi run test
sqlite3 result/animetor.db "PRAGMA user_version;"                # 55
sqlite3 result/animetor.db "SELECT COUNT(*) FROM sources;"       # 5
sqlite3 result/animetor.db "SELECT type, sql FROM sqlite_master WHERE name='person_scores';"  # type=table
rg 'FROM scores\b|INTO scores\b' src/ tests/                     # 0 件
rg 'anime_analysis' src/                                         # 0 件
```

---

## 🟠 SECTION 2: Phase 4 残務

### 2.2 Pipeline smoke test

- [ ] フレッシュ v55 schema でパイプラインが完走することを確認する test を `tests/test_pipeline_v55_smoke.py` として追加
- [ ] CI (`.github/workflows/`) に組み込む

### 2.3 Method Notes validation gate

- [ ] レポート生成時に `meta_lineage` 参照の有無を CI でチェック
- [ ] 参照なしは FAIL

---

## 🟠 SECTION 3: コード一貫性

### 3.1 Scraper 統一

**現状**: 6 scraper が `upsert_anime()` を直接呼び、`integrate.py` の dual-write パターンを bypass している。

**対象**:
```
src/scrapers/seesaawiki_scraper.py:3282, 3658
src/scrapers/anilist_scraper.py:239
src/scrapers/keyframe_scraper.py:416
src/scrapers/jvmg_fetcher.py:310
src/scrapers/mal_scraper.py:398
src/scrapers/mediaarts_scraper.py:475
```

- [ ] 各 scraper はモデルを組み立て、`integrate_*()` 関数 (or 新ラッパー `upsert_canonical_anime()`) 経由に統一
- [ ] `upsert_anime()` を bronze upsert に改名 or 内部で dual-write
- [ ] GraphQL クエリ文字列を `src/scrapers/queries/` に分離 (`PERSON_DETAILS_QUERY` 等)
- [ ] パース関数を `src/scrapers/parsers/` に分離 (`_parse_anime_staff` 等)
- [ ] scraper 本体は「fetch → parse → write」の orchestration だけに

### 3.2 `anime_display` 書き込み停止

- [ ] `upsert_anime_display()` 呼び出し箇所を全削除
- [ ] display は `src/utils/display_lookup.py` 経由で bronze から読む設計に統一

### 3.4 `credits.episode` sentinel 除去 ✅ DONE (2026-04-23)

- [x] init_db() DDL の `DEFAULT -1` 削除 → `episode INTEGER` (NULL 意味)
- [x] v50 migration に既存の UPDATE あり (`UPDATE credits SET episode = NULL WHERE episode = -1`)
- [x] `insert_credit()` は NULL-aware dedup 済み (`episode IS NULL` チェック)
- [x] コードレベルの `-1` チェックなし (grep で確認済み)

### 3.5 `src/etl/__init__.py` が空 ✅ DONE

- [x] `__all__` に 7 関数を export (upsert_canonical_anime, integrate_*, run_integration)

### 3.6 テストの `Anime(score=..., studios=...)` 移行

**対象** (15+ ファイル): `test_akm.py`, `test_decade_analysis.py`, `test_genre_affinity.py` 他

- [ ] 短期: `BronzeAnime` シム維持 (破壊しない)
- [ ] 中長期: `AnimeAnalysis(...)` に段階移行 (`rg 'Anime\(.*score=' tests/ --count` でスコープ確認)

### 3.7 `jvmg_fetcher.py` の `WIKIDATA_ROLE_MAP` バグ修正

**現状** (`src/scrapers/jvmg_fetcher.py:45-50`): プロパティ → role の対応が 3 件誤り。

| Prop | 現在の map (誤) | Wikidata 実定義 (2026-04-23 確認) |
|---|---|---|
| P58 | `episode director` | `screenwriter` |
| P1040 | `animation director` | `film editor` |
| P3174 | `episode_director` | `art director` |
| P57 | `director` | `director` (これは正しい) |

**影響**: BRONZE `credits` テーブルに誤ったロールが混入している可能性。JVMG 由来 credits は既に投入済みなら遡って再分類が必要。

- [ ] 正しい map に修正 (`P57→director, P58→screenwriter, P1040→editor, P3174→art_director`)
- [ ] `parse_role()` 側が `screenwriter`/`editor`/`art_director` を 24 種の role_groups に正しく落とせるか検証
- [ ] P10800 (animation director) の存在確認 — あれば追加
- [ ] 既存 JVMG-source の credits を再スクレイプ or 再マップ (どちらが現実的か判断)
- [ ] 新規 `wikidata_world_scraper.py` と map を共有 — `src/scrapers/wikidata_role_map.py` に定数を切り出して 2 scraper で import

**関連**: 新規 `wikidata_world_scraper.py` (世界アニメ DB 拡張) の設計で発覚。新 scraper 側は正した map で実装するので追加バグは生まない。

---

## 🟠 SECTION 4: DuckDB 全面移行

詳細カード: **`TASK_CARDS/04_duckdb/` (README + 6 cards)**

### 確定方針 (2026-04-23 設計レビュー)

```
scrapers ──→ bronze/source=X/date=Y/*.parquet   (per-source append-only、競合なし)
                     ▼ (単一 ETL)
               silver.duckdb                  (consolidated、atomic swap で更新)
                     ▼ (pipeline)
               gold.duckdb                    (consolidated、atomic swap で更新)
```

- per-source Parquet で並列スクレイピング時の write 競合を消す
- atomic swap (`os.replace()`) で「スクレイプしながら分析」を成立させる
- memory 暴走対策に `PRAGMA memory_limit` を全 connection で明示

### 期待効果
- 分析クエリ 5-50x 高速化 (columnar + vectorized)
- BRONZE を Parquet snapshot 化 → immutable audit trail
- `src/analysis/` の aggregation を SQL 化 → 2-3 割コード削減

### 注意点
- 並行書き込みに弱い (single writer + multi reader は OK)
- WAL mode 相当なし → パイプライン走行中の API 読み取りは別ファイルに書いて atomic swap
- OLTP は非推奨 (API 高頻度小クエリは SQLite より遅い可能性)

### 4.1 Phase A: 読み取りだけ DuckDB (PoC) ✅ DONE (2026-04-23)

- [x] `pixi add duckdb` — pixi.toml 済み
- [x] `src/analysis/silver_reader.py` 新設 — `silver_connect()` + typed loaders (`load_persons/anime/credits_silver()`)
- [x] `src/analysis/duckdb_io.py` ATTACH パターン廃止 → `silver_connect()` 直接接続に書き換え
- [x] `src/analysis/` 15 モジュールの standalone `main()` を silver.duckdb 直読に移行 (Card 05 step 3)
  - network: community_detection, path_finding, multilayer, core_periphery, structural_holes, temporal_bridge, temporal_influence
  - scoring: potential_value, pagerank (write path のみ legacy get_connection 残存)
  - graph, anime_value, contribution_attribution, genre/specialization, growth_acceleration, studio/bias_correction
- [ ] ベンチマーク: Phase 5/6 の時間比較

**Card 05 で積み越し** (pipeline_phases + attrition/gender は GOLD テーブル依存のため Card 06 に):
- `pipeline_phases/{data_loading,validation,entity_resolution,result_assembly}.py` — GOLD 書き込みと conn 共用
- `analysis/attrition/*.py`, `analysis/gender/bottleneck.py` — `feat_career`/`feat_career_gaps` GOLD テーブルを読む
- `analysis/{method_notes,person_parameters,llm_pipeline}.py` — GOLD 書き込み or LLM キャッシュ

### 4.2 Phase B: GOLD 層を DuckDB 化 (= Card 06)

- [ ] GOLD テーブル (person_scores, score_history, meta_*, agg_*, feat_*) を gold.duckdb へ
- [ ] パイプライン最終 Phase (`export_and_viz.py`) が gold.duckdb に書く
- [ ] API 側の GOLD 読み取りを DuckDB に切替
- [ ] attrition/gender/method_notes/person_parameters の conn を gold_connect() に切替
- [ ] `pipeline_phases/` の読み取り経路を silver_reader に、書き込み経路を gold_connect() に切替

### 4.3 Phase C: BRONZE を Parquet + DuckDB

- [ ] Scraper 出力を `src_*` テーブル → Parquet ファイル (日付パーティション) に
- [ ] `display_lookup.py` の読み取り先を Parquet に切替

### 4.4 Phase D: SQLite 完全撤去

- [ ] Entity resolution の書き込み経路を DuckDB に切替
- [ ] `src/database.py` を廃止 (DAO 群を `src/db/` に移管)
- [ ] `database_v2.py` / `models_v2.py` — DuckDB 移行で活かす計画がないなら削除
- [ ] `migrate_to_v2.py` は使い捨て script。`01_schema_fix/01_one_shot_copy.md` 実行後に削除
- [ ] Atlas migration を DuckDB 環境で再生成

### 事前確認

- [ ] `duckdb-engine` が SQLModel 機能 (computed_field, 外部キー, index 種別) に対応しているか検証

### 成功判定

- [ ] 全パイプラインが DuckDB 単独で完走
- [ ] テスト 2300+ 件 pass
- [ ] Phase 5/6 で 5x 以上の高速化
- [ ] API レスポンス時間が劣化していない

---

## 🟠 SECTION 5: Hamilton 導入

詳細カード: **`TASK_CARDS/05_hamilton/` (H1-H5)**

### 解消したい痛み

1. テストの `PipelineContext` 偽装 monkeypatch 地獄
2. 依存関係が暗黙的 (`PipelineContext` フィールド使用箇所を読まないと依存が分からない)
3. 部分再実行が面倒
4. 観測性が弱い (どの phase が何秒か grep 頼み)

### 5.1 Phase H-1: PoC (analysis_modules だけ) ✅ DONE (2026-04-23)

- [x] `pixi add sf-hamilton`
- [x] `src/pipeline_phases/hamilton_modules/` (core/studio/genre/network/causal) 5 モジュール、49 nodes
- [x] node 署名修正完了 (commit 9ecfef8)、49/49 non-None
- [x] Benchmark: Hamilton 3.36s vs ThreadPoolExecutor 16.42s (-79.5%) → **PASS**
- [x] 既存テスト 18 件 pass

**判断結果**: 20% 閾値を大きく下回った → H-2 進行。

### 5.2 Phase H-2: Phase 5-8 を Hamilton 化

- [ ] `core_scoring.py` (AKM / IV / PageRank / BiRank)
- [ ] `supplementary_metrics.py` / `result_assembly.py` / `post_processing.py`

### 5.3 Phase H-3: Phase 1-4 を Hamilton 化

- [ ] `data_loading.py` / `validation.py` / `entity_resolution.py` / `graph_construction.py`

### 5.4 Phase H-4: `PipelineContext` 完全削除

- [ ] `PipelineContext` dataclass を削除
- [ ] `src/pipeline.py` を Hamilton `Driver` の薄いラッパーに

### 5.5 Phase H-5: 観測・運用機能

- [ ] `@tag(stage="phase5", cost="expensive")` を各 node に付与
- [ ] 実行時間計測 adapter (Hamilton lifecycle hook)

### 中止判定

H-1 終了時に以下のいずれかなら H-2 以降中止:
- Hamilton overhead で Phase 9 並列実行が 20% 以上遅くなる
- 型ヒント + decorator の可読性が `PipelineContext` より悪い

---

## 🟠 SECTION 6: テストカバレッジ

### 6.1 pipeline_phases ユニットテスト (13/15 未テスト)

重要度順:
- [ ] Phase 5 `core_scoring.py` — 補償根拠の中核
- [ ] Phase 8 `post_processing.py` — パーセンタイル・CI 計算
- [ ] Phase 9 `analysis_modules.py` — 並列実行 (Hamilton 化で一部解消見込み)

### 6.2 `patronage_dormancy.py` 直接テスト

~~IV テストではモック使用。実際の dormancy penalty 計算ロジック (指数減衰、猶予期間) が未検証。~~
- [x] 完了 (2026-04-23): `tests/test_patronage_dormancy_direct.py` 12 tests

### 6.3 VA パイプライン (7 モジュール全て未テスト)

- [x] 完了 (2026-04-23): `tests/test_va_modules.py` 38 tests (va_akm, va_graph, ensemble_synergy)
- [x] 完了 (2026-04-23): `tests/test_va_pipeline_phases.py` 10 smoke tests (graph_construction / core_scoring / supplementary_metrics / result_assembly)
- [ ] `va_integrated_value`, `va_career_friction`, `va_character_diversity` 等残り 4 モジュール

### 6.4 テストファイル分割

モノリス test ファイルは保守コストが高い:

```
tests/test_scraper_coverage.py    1856 行  # "coverage" 命名は散在の自白
tests/test_analysis_coverage.py   1440 行
tests/test_va_studio_genre.py      591 行  # 3 ドメイン混在
tests/test_api.py                 1181 行
tests/test_akm.py                 1102 行
```

- [ ] `test_scraper_coverage.py` を scraper 別に分割 (`test_anilist_scraper.py`, `test_mal_scraper.py` 等)
- [ ] `test_analysis_coverage.py` は submodule 単位に分割
- [ ] `test_va_studio_genre.py` → `test_va.py` + `test_studio.py` + `test_genre.py`
- [ ] fixture は `tests/conftest.py` + `tests/fixtures/` に寄せる (test ファイル内定義を避ける)
- [ ] `tests/unit/` / `tests/integration/` の最低限分離

---

## 🟡 SECTION 7: スクレイパー強化残務

セッション `7e4081c` + `107cbfe` で着手したが未完了の項目。

### 7.1 差分更新 (incremental update) — ブロック中 (スキーマ変更待ち)

- **やること**:
  1. `src_*_anime` テーブルに `fetched_at` / `content_hash` カラム追加
  2. upsert 時に hash 比較して変更時のみ update + `meta_scrape_changes` に差分記録
  3. scraper 側に `--since YYYY-MM-DD` mode 実装
     - ANN: masterlist の `lastModified` 利用
     - AniList: GraphQL `updatedAt` フィルタ
     - MAL/Jikan: `updated_at` でフィルタ
- **先行実装可**: `content_hash` 算出 (sha256 of canonical JSON of relevant fields)。スキーマ側 agent 完了後に配線

### 7.2 ANN Phase 3 を HTML スクレイプに書き換え ✅ 完了 (2026-04-23)

- **背景**: ANN `?people=ID` API が `<warning>ignored</warning>` を返す (2026-04-23 確認)。Phase 3 (scrape-persons) が完全に空振り
- **完了**:
  - `ann_scraper.py` に `PEOPLE_HTML_BASE`, `parse_person_html`, `_parse_dob_html` 追加
  - `_run_scrape_persons` は per-ID HTML fetch loop (`fetch_person_html`) 実装済み、`AnnClient._throttle()` で rate limit (1.5s)、checkpoint 保存、`BronzeWriter("ann", table="persons")` 経由で BRONZE 書き込み
  - `tests/test_ann_scraper_parse.py` 46 件 (parse / _parse_dob_html / DOB 形式 / Cloudflare ブロック / minimal field) green

### 7.3 anilist_scraper retry refactor (任意)

- **現状**: 220 行のカスタム `X-RateLimit-*` 監視 + token refresh + probe query 実装
- [ ] 共通部分のみ `RetryingHttpClient` に委譲、X-RateLimit-* 専用 callback hook を追加
- **価値**: 中 (重複削減 50 行)、**リスク**: 中 (rate-limit 動作が壊れると本番ペイン)

### 7.4 未テストの scraper

| source | parse 関数 | テスト | 注 |
|---|---|---|---|
| jvmg / wikidata | `parse_wikidata_results` | 未 | SPARQL JSON fixture → 3-5 件で足りる |
| keyframe | `extract_preload_data` 等 | 未 | HTML fixture 1 件で OK |
| seesaawiki | `parse_*` (3864 行内) | 未 | 旧来から quality audit メモ済 |

### 7.5 確認済みの壊れた endpoint

| endpoint | 症状 | 対応 |
|---|---|---|
| ANN `cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all` | HTML を返す | fallback `_probe_max_id` で動作中。本来 nlist 正規パラメータ調査要 |
| ANN `?people=ID` API | `<warning>ignored</warning>` | 7.2 で HTML scrape (people.php?id=N) に切替済 ✅ |

### 7.6 lint 残債

- `src/scrapers/anilist_scraper.py:2603` F841 `existing_person_ids` 未使用
- `src/scrapers/ann_scraper.py:515` E402 mid-file `import dataclasses`
- `src/scrapers/allcinema_scraper.py:403` E402 同上

---

## 🟠 SECTION 8: レポートシステム統廃合

### 8.1 3 系統の統一

| 系統 | 入口 | 行数 | ステータス |
|------|------|------|---------|
| v1 monolith | `scripts/generate_all_reports.py` | 24,983 | `pixi run reports` の現行エントリ |
| v2 orchestrator | `scripts/generate_reports_v2.py` + `report_generators/reports/*.py` | 413 + 多数 | 現行並行 |
| v3 class-based | `src/reporting/` | 2,725 | 使われていない (`pixi run reports-new`) |

- [ ] v1 か v3 のどちらかを即座に消す (v2 は移行過渡期の束なので最終形に寄せて閉じる)
- [ ] `pixi.toml` の `reports` / `reports-new` を 1 つにまとめる

### 8.2 `generate_all_reports.py` の 24,983 行分解

63 の関数が単一ファイル。35 本は `report_generators/reports/` に分離済み。

**未分離**: `generate_ml_clustering_report`, `generate_industry_overview`, `generate_network_analysis_report`, `generate_team_report`, `generate_career_report`, `generate_temporal_report`, `generate_network_evolution_report`, `generate_growth_score_report`, `generate_person_ranking_report`, `generate_compensation_report`, `generate_bias_report`, `generate_genre_report`, `generate_studio_impact_report`, `generate_credit_statistics_report` 他。

- [ ] 分離済みの関数を本体から削除 (v1/v2 でダブっている分を潰す)
- [ ] 本体を「v2/v3 にない暫定的な関数のみ」の薄いファイルに縮める (目標 < 2,000 行)

### 8.3 FastAPI 2 系統の統合

| | path | 役割 |
|---|------|------|
| メイン | `src/api.py` 1,322 行 42+ endpoint | persons/scores 照会、i18n、WebSocket |
| レポート | `scripts/report_api.py` ~250 行 12 endpoint | brief 生成、versioning |

**問題**: URL 衝突 (両方 `/api/...`)、認証・デプロイが 2 プロセス。

- [ ] `scripts/report_api.py` を `src/api/report_routes.py` に移し、メイン app に `include_router()` で合流
- [ ] Taskfile `report-api` は `pixi run serve` に統合
- [ ] `src/api.py` 自体を `src/api/{persons,reports,i18n}.py` の router 単位に分割 (1,322 行の分解)

---

## 🟡 SECTION 9: アーキテクチャ整理

### 9.1 孤立した analysis モジュール (外部参照 1-3 件)

```
1  src/analysis/batch_compare.py
2  src/analysis/anime_prediction.py
2  src/analysis/anime_title_matching.py
2  src/analysis/compatibility.py
2  src/analysis/compensation_analyzer.py
2  src/analysis/graphml_export.py
2  src/analysis/insights_report.py
2  src/analysis/ml_homonym_split.py
2  src/analysis/synergy_score.py
3  src/analysis/comparison_matrix.py
3  src/analysis/neo4j_direct.py
3  src/analysis/person_tags.py
3  src/analysis/recommendation.py
3  src/analysis/similarity.py
```

- [ ] `batch_compare`, `comparison_matrix`, `compatibility`, `similarity`, `recommendation` — 機能重複。`similarity.py` に一本化検討
- [ ] `anime_prediction`, `anime_title_matching` — 呼び出し元を確認して削除判定
- [ ] `insights_report`, `compensation_analyzer` — `report_generators/reports/` に後継があるなら削除
- [ ] `neo4j_direct` + `graphml_export` — `docs/NEO4J_MIGRATION.md` があるが実稼働していないなら削除

### 9.2 VA パイプラインの平行配線

`src/pipeline_phases/` に VA 専用 4 ファイル (340 行、本家と構造同一):
```
va_core_scoring.py / va_graph_construction.py / va_result_assembly.py / va_supplementary_metrics.py
```

- [ ] 共通ロジックを `src/pipeline_phases/common/` に抽出して DRY
- [ ] または VA パイプラインを `src/analysis/va/` の module 集合として扱い `pipeline_phases/` から分離

### 9.3 Julia 視覚化層の要否確認

```
julia_viz/               Julia project
scripts/setup_julia_env.py 70 行
pixi.toml [feature.viz]   juliacall 依存
result/makie/             生成物 3.6MB
```

- [ ] `juliacall` を使う利用箇所を確認 (setup script のみなら削除)。matplotlib/Plotly と役割重複

### 9.4 Taskfile 81 タスクの圧縮

```
scrape         vs  scrape-old
scrape-update  vs  scrape-update-old
scrape-all     vs  scrape-all-old
fix-retry, fix-all, fix-backfill, fix-cleanup, fix-status
```

- [ ] `-old` variants は `--reverse` フラグ違いのみ。`task scrape ORDER=reverse` にパラメタ化
- [ ] `fix-*` が参照している `scripts/fix_scraping_failures.sh` を確認し不要なら削除
- [ ] Taskfile と `pixi.toml tasks` で重複定義があるものは Taskfile に寄せる

### 9.5 `src/monitoring.py` → `src/freshness.py` リネーム

- `src/monitoring.py` = データ鮮度 (scraper が最新か)
- `scripts/monitoring/` = データ品質 (quality snapshot)
- 同名・別概念で認知負荷が高い。
- [ ] `src/monitoring.py` を `src/freshness.py` にリネーム (import 10 箇所更新)

### 9.6 `scripts/` の subdir 整理

```
scripts/                  # 18 top-level scripts が散在
scripts/report_generators/
scripts/monitoring/
scripts/maintenance/
```

- [ ] `scripts/analysis/` に `analyze_*.py` を寄せる
- [ ] `scripts/ci/` に `ci_check_*.py` を寄せる
- [ ] Top-level は entry-point のみ

---

## 🟡 SECTION 10: 関数分解候補

セッション `944e32d` + `415aa3e` の続き。`ast` 抽出済みの 40 行以上の関数 (agent territory 除外)。

| 行数 | 場所 | 関数 | 注 |
|---:|---|---|---|
| 106 | `scripts/report_generators/html_templates.py:778` | `wrap_html_v2` | report 系の最頻 caller、効果大 |
| 87 | `scripts/report_generators/html_templates.py:454` | `wrap_html` | 旧版だがまだ使用中 |
| 86 | `src/utils/performance.py:268` | `print_report` | utils stable、テストもある |
| 72 | `scripts/report_generators/report_brief.py:187` | `ReportBrief.validate` | vocab 検査含む validation chain |
| 65 | `scripts/report_generators/html_templates.py:907` | `plotly_div_safe` | Plotly HTML 埋め込み |
| 50 | `src/utils/json_io.py:500` | `save_pipeline_json_if_data_present` | 保存判定+書き込み混在 |
| 49 | `scripts/report_generators/section_builder.py:145` | `validate_findings` | regex 検査 chain |
| 46 | `src/utils/episode_parser.py:20` | `parse_episodes` | 文字列パース、case 分岐多 |
| 42 | `src/utils/display_lookup.py:178` | `get_display_description` | bronze 経由の説明取得 |
| 42 | `scripts/report_generators/section_builder.py:239` | `build_section` | section 組立 |
| 40 | `src/utils/performance.py:208` | `generate_report` | レポート生成 |

**進め方**: Read → 概念の塊を identify → `_verb_noun` 名で extract → 元関数を recipe に → `pixi run pytest`

**避けるべき** (agent が触る可能性): `src/validation.py:222 validate_data_freshness`, `src/monitoring.py:36 check_data_freshness` (DuckDB agent が触る)

---

## 🟢 SECTION 11: レイアウト・命名整理

### 11.1 `src/` 直下 16 本の平置き解消

```
api.py  api_validators.py  cli.py  database.py  database_v2.py  db_rows.py
log.py  models.py  models_v2.py  monitoring.py  pipeline.py  report.py
synthetic.py  validation.py  websocket_manager.py
```

提案レイアウト (DuckDB 移行後が自然なタイミング):
```
src/db/        # database.py, models.py, db_rows.py → schema.py, dao.py, rows.py, models.py
src/runtime/   # api/, cli.py, pipeline.py
src/infra/     # log.py, websocket_manager.py, freshness.py (旧 monitoring.py)
src/testing/   # synthetic.py → fixtures.py
```

- [ ] 単発ファイルはなくす。整理前 16 → 整理後 5 パッケージ + 1 ファイル

### 11.2 `src/analysis/` 69 本平置き + 12 subdir の統合

矛盾: `mentor/` subdir と top-level `mentorship.py` が共存。`network/` subdir があるのに top-level `graph.py` / `collab_diversity.py` もある。

提案: 平置き 0 本、全員 subpackage 所属。新設:
```
analysis/graph/   analysis/career/   analysis/entity/   analysis/credits/
analysis/compat/  analysis/viz/      analysis/io/       analysis/quality/
```

- [ ] 移動は `__init__.py` で再エクスポートして後方互換を保つ
- [ ] 先に D-1 (孤立モジュール) を統廃合してから

### 11.3 命名ゆらぎの解消

- [ ] `_v2` suffix 廃止: `database_v2`/`models_v2` は破棄 or 正式名昇格 (S-5 参照)
- [ ] `generate_reports_v2.py` → v1 を消した後に `generate_reports.py`
- [ ] 同一概念を同じ名前に統一: `person_fe` で統一するなら `theta_i` はコメントで説明
- [ ] `src/log.py` → `src/infra/logging.py`、`src/synthetic.py` → `src/testing/fixtures.py`

---

## 🟢 SECTION 12: ドキュメント整理

### 12.1 完了済み戦略文書を `docs/archive/` に移動

現役ではなく完了報告の文書:
```
docs/REFACTORING_SUMMARY.md             ← 2026-02-10 Phase 1-4 完了報告
docs/PHASE1_DATA_LAYER_REFACTOR.md      ← Phase 1 完了
docs/BUG_FIXES_PHASE_B.md               ← Phase B 完了
docs/REPORT_STRATEGY.md                 ← v2 検討メモ
docs/TEST_OPTIMIZATION_STRATEGY.md      ← 戦略提案
docs/COMMUNITY_DETECTION_ENHANCEMENTS.md
docs/COVERAGE_REPORT.md
docs/SCHEMA_VERSIONING_STATUS.md
docs/TEST_AND_AUDIT.md
```

- [ ] 上記を `docs/archive/` に移動 (git history で参照可)
- [ ] `docs/` 直下に残すのは: ARCHITECTURE, CALCULATION_COMPENDIUM, REPORT_PHILOSOPHY, DATA_DICTIONARY, REPORT_INVENTORY, schema.dbml のみ

### 12.2 CLAUDE.md スリム化 (827 行 → ~300 行目標)

Phase 2C/2D/2E/2F/2G の詳細 (~400 行) は毎回ロードされる必要はない。

退避先:
```
docs/report-briefs.md          ← 現 CLAUDE.md の Phase 4 (Report Briefs Architecture) 104 行
docs/technical-appendix.md     ← Phase 2C 120 行
docs/ci-gates.md               ← Phase 2D (CI/CD Integration) 80 行
docs/export-system.md          ← Phase 2E 88 行
docs/report-versioning.md      ← Phase 2F 32 行
docs/report-api.md             ← Phase 2G 63 行
```

- [ ] CLAUDE.md はプロジェクト原則・禁止事項・ディレクトリ案内に絞る

### 12.3 README.md スリム化 (512 行 → ~150 行目標)

```
現状の過剰部分:
  API Endpoints    67 行 → docs/api.md
  CLI Commands     37 行 → docs/cli.md
  Architecture     31 行 → docs/architecture.md (CLAUDE と重複)
  Tech Stack       (CLAUDE と重複)
  Directory Structure  (CLAUDE と重複)
```

- [ ] README.md = 「何をやっていてどう始めるか」(外部向け) に限定
- [ ] CLAUDE.md = 「原則と禁止事項」(エージェント向け)

### 12.4 CLAUDE.md ドリフト修正 (随時)

- [ ] 「1394 tests」→「2300+ tests」
- [ ] 「SQLite WAL mode (storage)」→ DuckDB 移行後に更新 (Section 4 Phase D 完了時)
- [ ] Testing patterns の `monkeypatch DEFAULT_DB_PATH` → DuckDB 切替後に更新
- [ ] `src/models.py` の `AnimeAnalysis` に付いた DEPRECATED コメントを削除 (現在 active な canonical 型)

---

## 🟢 SECTION 13: 将来タスク (feat_* 層別分離)

現状 `feat_career` と `feat_network` は L2 (集約数値) と L3 (独自計算) が混在。

- [ ] `agg_person_career` (L2: `first_year`, `active_years`, `total_credits` 等) と `feat_career_scores` (L3: `growth_trend` 等) に分割
- [ ] `agg_person_network` (L2: `n_collaborators`, `n_unique_anime`) と `feat_network_scores` (L3: centrality 等) に分割
- [ ] `corrections_*` テーブル: クレジット年補正・ロール正規化などの修正差分を生データから分離して追跡

---

## 実施順序

```
Phase α: スキーマ整合修復 (ブロッキング — 先に倒す)
  1.1 → 1.2 → 1.3 → 1.5 → 1.6

Phase β: Phase 4 残務 + DuckDB/Hamilton PoC (並行可)
  2.2, 2.3              (Phase 4 残務)
  3.1, 3.2, 3.4, 3.5   (コード一貫性)
  7.2                   (ANN HTML切替、ブロッカーなし)
  4.1 ✅ DONE           (DuckDB Phase A PoC — silver_reader + 15 module 移行)
  5.1 ✅ DONE           (Hamilton H-1 PoC)

Phase γ: 移行本格化 (β 効果確認後)
  4.2 DuckDB Phase B = Card 06 (GOLD DuckDB 化 + pipeline_phases/attrition 切替)
  5.2 Hamilton H-2 (Phase 5-8)
  6.1, 6.2, 6.3   (テストカバレッジ)
  8.1, 8.2, 8.3   (レポート統廃合 — S-1/S-2/S-4 完了で Taskfile も整理)

Phase δ: 完全移行
  4.3 DuckDB Phase C (BRONZE Parquet)
  5.3 Hamilton H-3 (Phase 1-4)
  4.4 DuckDB Phase D (SILVER, SQLite 撤去)
  5.4 Hamilton H-4 (PipelineContext 削除)
  5.5 Hamilton H-5 (観測)

Phase ε: 余裕時
  3.6 テスト AnimeAnalysis 移行
  6.4 テストファイル分割
  7.1, 7.3, 7.4, 7.6   (スクレイパー強化)
  9.1-9.6   (アーキテクチャ整理)
  10   (関数分解)
  11-12   (レイアウト・命名・ドキュメント)
  13   (feat_* 層別分離)
```

### 並行・逐次の注意
- **α は必ず先**: schema 整合性が取れていない状態で DuckDB / Hamilton 移行はデバッグ困難
- **β の PoC 2 つは並行可**: DuckDB A と Hamilton H-1 は互いに独立
- **DuckDB Phase D と Hamilton H-4 は同時適用しない**: マージ競合が荒れる

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: 方法論的パラメータは method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF / GPU Polars)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
