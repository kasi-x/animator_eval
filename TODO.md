# TODO.md — 未完了作業 (人間レビュー用オーバービュー)

作成日: 2026-04-22 / 最終更新: 2026-04-22

本書は **人間レビュー・進捗把握用のオーバービュー** です。完了済みサマリーは `DONE.md`、プロジェクト設計原則は `CLAUDE.md`。

## 実行指示書は `TASK_CARDS/`

弱いモデル・初見エンジニアが実際に作業する際は、本書ではなく **`TASK_CARDS/` 配下の個別カード** を読んでください。各カードは自己完結 (前提条件・制約・手順・検証・ロールバック) で 1 タスク = 1 ファイルに分解されています。

```
TASK_CARDS/
├── README.md                # エントリポイント、実行順序、ルール
├── _hard_constraints.md     # 全タスク共通の絶対遵守事項
├── _card_template.md        # カード書式
├── 01_schema_fix/           # 🔴 Critical (6 cards) — 必ず最初
├── 02_phase4/               # 🟠 Major (5 cards)
├── 03_consistency/          # 🟠 Major (5 cards)
├── 04_duckdb/               # ⚠️ SENIOR ONLY
├── 05_hamilton/             # ⚠️ SENIOR ONLY
└── 06_tests/                # 🟡 Minor (senior が必要に応じて細分化)
```

**推奨**: 弱いモデルに振る場合は `TASK_CARDS/README.md` と `_hard_constraints.md` を事前に読ませる。

---

以下は各 Section の概要。詳細・手順は必ず `TASK_CARDS/` のカードを参照してください。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🔴 Critical | スキーマ整合 | v55 migration 未登録、`anime_display`/`anime_analysis` DDL 残存、`scores` 物理リネーム未実施 |
| 🟠 Major | Phase 4 残務 | meta_lineage population 5 件、pipeline smoke test、method notes validation |
| 🟠 Major | コード一貫性 | scraper 統一、entity_resolution_audit 書き込み、episode sentinel |
| 🟠 Major | DuckDB 全面移行 | SQLite → DuckDB (BRONZE Parquet + SILVER/GOLD DuckDB + atomic swap)。詳細: `TASK_CARDS/04_duckdb/` (6 cards) |
| 🟠 Major | Hamilton 導入 | `PipelineContext` 解消、DAG 化 (H-1 ~ H-5) |
| 🟡 Minor | テストカバレッジ | pipeline_phases 13/15 未テスト、VA 7 モジュール未テスト |
| 🟡 Maintenance | Schema baseline 固定 | production DB が v55 で安定後、v1-v55 の legacy migration 関数を削除し `init_db()` のみを single source of truth にする |
| 🟢 Future | 層別分離 | `feat_career` / `feat_network` の L2/L3 分割 |

---

## 🔴 SECTION 1: スキーマ整合性修復

### 根本問題
- `src/database.py:27` に `SCHEMA_VERSION = 54`
- `_migrate_v54_to_v55()` (line 8924) が定義済みだが `migrations` dict に未登録
- `_migrate_v55_to_v56()` (line 9012) はコメントアウト
- `ensure_phase1_schema()` (line 9088) は定義のみで呼び出しなし

結果として v55 以降の変更 (sources lookup, roles lookup, person_aliases) はどの DB にも適用されていない。

### 1.1 `sources` テーブル DDL 衝突解消

**問題**: DDL が 2 系統に分岐している
- `init_db()` line 261 / `_migrate_v50_canonical_silver()` line 8291: PK=`code TEXT`、カラム `name_ja, base_url, license, description`
- `_migrate_v54_to_v55()` line 8938: PK=`id TEXT`、カラム `name TEXT, description TEXT` (別物)

フレッシュインストールは `init_db()` の PK=`code` を作る。v55 が後から `CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY ...)` を流すと `IF NOT EXISTS` でスキップされ、`id` カラム前提のコードが壊れる。

**修正** (`src/database.py:8936-8952`):
- [ ] `_migrate_v54_to_v55()` の `sources` DDL を削除
- [ ] seed INSERT のみに変更:
  ```python
  SOURCE_SEEDS = [
      ('anilist',    'AniList',                'https://anilist.co',               'proprietary', 'GraphQL で structured staff 情報が最も豊富'),
      ('ann',        'Anime News Network',     'https://www.animenewsnetwork.com', 'proprietary', 'historical depth と職種粒度'),
      ('allcinema',  'allcinema',              'https://www.allcinema.net',        'proprietary', '邦画・OVA の網羅性'),
      ('seesaawiki', 'SeesaaWiki',             'https://seesaawiki.jp',            'CC-BY-SA',    'fan-curated 詳細エピソード情報'),
      ('keyframe',   'Sakugabooru/Keyframe',   'https://www.sakugabooru.com',      'CC',          'sakuga コミュニティ別名情報'),
  ]
  for code, name_ja, base_url, license_, desc in SOURCE_SEEDS:
      conn.execute(
          "INSERT OR IGNORE INTO sources (code, name_ja, base_url, license, description) VALUES (?,?,?,?,?)",
          (code, name_ja, base_url, license_, desc),
      )
  ```

### 1.2 v55 migration の正規登録

**修正**:
- [ ] 1.1 完了後、`_migrate_v54_to_v55` を `migrations[55]` に登録
- [ ] `SCHEMA_VERSION = 55` に更新
- [ ] `ensure_phase1_schema()` を削除

### 1.3 `scores` → `person_scores` 物理リネーム

**現状**: VIEW `person_scores` が `scores` のエイリアス (`src/database.py:8639`)。物理リネーム未実施。

**参照残存箇所** (確認済み):
- `src/api.py` 6 箇所: `line 239, 307, 365, 437, 450, 758`
- `src/cli.py` 6 箇所: `line 46, 203, 272, 437, 602, 1685`
- `src/database.py`: `line 5347, 5605`
- `tests/`: `test_api.py`, `test_cli.py`, `test_monitoring.py`, `test_integration.py`, `test_db_schema.py`

**修正**:
- [ ] v55 migration に以下を追加:
  ```python
  if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scores'").fetchone():
      conn.execute("DROP VIEW IF EXISTS person_scores")
      conn.execute("ALTER TABLE scores RENAME TO person_scores")
  ```
- [ ] 一括置換:
  ```bash
  rg -l 'FROM scores\b|INTO scores\b|TABLE scores\b' src/ tests/ | xargs sed -i 's/FROM scores/FROM person_scores/g; s/INTO scores/INTO person_scores/g'
  ```

### 1.4 `anime_display` 廃止 ✅ 完了 (2026-04-23 確認)

監査結果:
- ✅ Step 1: `init_db()` の DDL → コメントアウト済 (`src/database.py:416-421`)
- ✅ Step 2: `validation.py` / `cli.py` / `api.py` の参照 → **0 件** (既にクリーン)
- ✅ Step 3: v54→v55 migration に `DROP TABLE IF EXISTS anime_display` あり (`src/database.py:8802`)、`16cffa0` で migration 登録済

残存する `anime_display` 参照は legacy migration 関数 (v49 等) と「v55 後に anime_display が消えていることを検証する test」のみ。問題なし。

### 1.5 `anime_analysis` DDL 除去

**現状**: v50 で `anime` にリネーム済みにもかかわらず `init_db()` に DDL が残存 (`src/database.py:289, 414, 7457`)。インデックス 3 本も残存 (`line 440-442`)。

**修正**:
- [ ] `init_db()` から `anime_analysis` DDL を削除 (`anime` テーブルが canonical)
- [ ] 関連インデックス (`idx_anime_analysis_*`) 3 本を削除
- [ ] `init_db()` の役割を「最終スキーマを直接 CREATE する」に統一

### 1.6 `_migrate_v55_to_v56` の扱い

ジャンル正規化 (anime_genres/anime_tags JSON 展開) を含むため実行コスト高。本作業では保留。
- [ ] 関数上部に `# STATUS: deferred — high execution cost, schedule separately` コメント付記
- [ ] `migrations` dict に未登録のまま残す

### 検証

```bash
pixi run test
sqlite3 result/animetor.db "PRAGMA user_version;"                # 55
sqlite3 result/animetor.db "SELECT COUNT(*) FROM sources;"       # 5
sqlite3 result/animetor.db "SELECT type, sql FROM sqlite_master WHERE name='person_scores';"  # type=table
rg 'FROM scores\b|INTO scores\b' src/ tests/                     # 0 件
rg 'anime_display' src/ tests/                                   # 0 件
rg 'anime_analysis' src/                                         # 0 件 (migration 内の履歴記述を除く)
```

---

## 🟠 SECTION 2: Phase 4 残務

### 2.1 meta_lineage population (5 レポート)

meta_lineage テーブルに記録を投入していないレポート:
- [ ] `policy_attrition`
- [ ] `policy_monopsony`
- [ ] `policy_gender_bottleneck`
- [ ] `mgmt_studio_benchmark`
- [ ] `biz_genre_whitespace`

各レポートの末尾で `meta_lineage` に formula_version / CI method / null_model / inputs_hash を insert する。

**ブロッカー**: `init_db()` の DB lock (現状 timeout 30s で緩和中)。完全解決するまでは既存 DB 環境で手動投入。

### 2.2 Pipeline smoke test

- [ ] フレッシュ v55 schema でパイプラインが完走することを確認する test を `tests/test_pipeline_v55_smoke.py` として追加
- [ ] CI (`.github/workflows/`) に組み込む

### 2.3 Method Notes validation gate

- [ ] レポート生成時に `meta_lineage` 参照の有無を CI でチェック
- [ ] 参照なしは FAIL

### 2.4 Full lineage check 実装

- [ ] `scripts/report_generators/ci_check_lineage.py` を骨格から完全実装に (入力 hash、formula version、CI method の全検証)

### 2.5 Technical appendix vocabulary audit

- [ ] 15 technical reports の禁止語使用を棚卸し
- [ ] documented exception として許容するものを `scripts/report_generators/forbidden_vocab_exceptions.yaml` に記録

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

### 3.2 `anime_display` 書き込み停止

- [ ] `upsert_anime_display()` 呼び出し箇所を全削除
- [ ] display は `src/utils/display_lookup.py` 経由で bronze から読む設計に統一
- [ ] 1.4 Step 1 と連動

### 3.3 `meta_entity_resolution_audit` への書き込み追加 ✅ 完了 (2026-04-23 確認)

監査結果 (TODO 文面はテーブル名 rename 前のもの):
- 現在のテーブル名は `ops_entity_resolution_audit` (init_db_v2 で作成、`12_meta_prefix_split` の方針通り)
- 書き込み関数 `upsert_meta_entity_resolution_audit` 実装済 (`src/database.py:5040`)
- `pipeline_phases/entity_resolution.py:422-471` で audit_rows を生成・upsert
- `pipeline.py:210` で `conn` を渡しているため本番 pipeline で書き込まれる
- merge_method は `cross_source` / `exact_match` (ML resolver の挙動を変えず、生成済み canonical_map を分類記録するのみ — H3 違反なし)
- `tests/test_database.py::TestMetaLineageAndAudit` (2 tests) pass

### 3.4 `credits.episode` sentinel 除去

**現状**: `DEFAULT -1` (sentinel = 全話通し) の設計。NULL 意味付けに変更すべき。

- [ ] v55 migration に追加: `UPDATE credits SET episode = NULL WHERE episode = -1`
- [ ] DDL の `DEFAULT -1` を削除
- [ ] 既存の `-1` チェックを `IS NULL` に一括置換:
  ```bash
  rg 'episode.*-1|episode == -1|episode < 0' src/
  ```

### 3.5 `src/etl/__init__.py` が空

- [ ] `integrate.py` の公開 API (`integrate_anilist`, `integrate_ann` 等) を export するか、private 設計を docstring で明示

### 3.6 テストの `Anime(score=..., studios=...)` 移行

**対象** (15+ ファイル): `test_akm.py`, `test_decade_analysis.py`, `test_genre_affinity.py`, `test_expected_ability.py`, 他

- [ ] 短期: `BronzeAnime` シム維持 (破壊しない)
- [ ] 中長期: `AnimeAnalysis(...)` に段階移行
- [ ] スコープ把握: `rg 'Anime\(.*score=' tests/ --count`

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
- silver/gold は単一 DuckDB で query 速度を維持 (per-source DuckDB は ATTACH コストで却下)
- atomic swap (`os.replace()`) で「スクレイプしながら分析」を成立させる
- memory 暴走対策に `PRAGMA memory_limit` を全 connection で明示

### 方針
SQLite → DuckDB 全面移行。規模感 (credits 数百万 / anime 数万 / persons 10万) は DuckDB の得意領域。out-of-core 実行により memory 問題は発生しない。

### 期待効果
- 分析クエリ 5-50x 高速化 (columnar + vectorized)
- `duckdb.sql(...).df()` で pandas zero-copy
- BRONZE を Parquet snapshot 化 → immutable audit trail
- `src/analysis/` の aggregation を SQL 化 → 2-3 割コード削減

### 残すもの
- SQLModel (`duckdb-engine` dialect 経由で継続利用)
- Atlas migrations (DuckDB 対応済み)
- 3層 medallion (BRONZE / SILVER / GOLD) 構造
- `display_lookup` の SILVER↔BRONZE 唯一経路原則

### 失うもの / 注意点
- 並行書き込みに弱い (single writer + multi reader は OK)
- WAL mode 相当なし → パイプライン走行中の API 読み取りは別ファイルに書いて atomic swap
- OLTP は非推奨 (API 高頻度小クエリは SQLite より遅い可能性)
- エッジケース差異 (NULL 比較、日付フォーマット) に注意

### 4.1 Phase A: 読み取りだけ DuckDB (PoC)

- [ ] `pixi add duckdb duckdb-engine`
- [ ] `src/analysis/` の重いクエリ 2-3 箇所を DuckDB 経由 (SQLite を `ATTACH`) で書き直し
- [ ] ベンチマーク: Phase 5 (core_scoring) / Phase 6 (supplementary_metrics) の時間比較
- [ ] 書き込みは SQLite のまま (後戻りコスト最小)

### 4.2 Phase B: GOLD 層を DuckDB 化

- [ ] GOLD テーブル (scores/person_scores, score_history, meta_*, agg_*, feat_*) を DuckDB へ
- [ ] パイプライン最終 Phase (`src/pipeline_phases/export_and_viz.py`) が DuckDB に書く
- [ ] API 側の GOLD 読み取りを DuckDB に切替
- [ ] 書き込みはパイプライン 1 回のみなので競合リスクなし

### 4.3 Phase C: BRONZE を Parquet + DuckDB

- [ ] Scraper 出力を `src_*` テーブル → Parquet ファイル (日付パーティション) に
- [ ] DuckDB はクエリ時に Parquet を直読
- [ ] `display_lookup.py` の読み取り先を Parquet に切替
- [ ] 過去スナップショットの保持コストが大幅減 (圧縮率)

### 4.4 Phase D: SILVER を DuckDB 化 + SQLite 完全撤去

- [ ] SILVER テーブル (anime, persons, credits, roles, etc.) を DuckDB へ
- [ ] Entity resolution の書き込み経路を DuckDB に切替
- [ ] SQLite 依存コードを削除、`src/database.py` を DuckDB ベースに書き換え
- [ ] `DEFAULT_DB_PATH` 等のテストパッチポイントを DuckDB 用に更新
- [ ] Atlas migration を DuckDB 環境で再生成

### 事前確認

- [ ] Scraper の並行書き込み状況 (writer 1 本化の要否)
- [ ] API とパイプラインの同時実行パターン (atomic swap の要否)
- [ ] `duckdb-engine` が SQLModel 機能 (computed_field, 外部キー, index 種別) に対応しているか検証

### 成功判定

- [ ] 全パイプラインが DuckDB 単独で完走
- [ ] テスト 2161 件 pass
- [ ] Phase 5/6 で 5x 以上の高速化
- [ ] API レスポンス時間が劣化していない

---

## 🟠 SECTION 5: Hamilton 導入

### 解消したい痛み
1. テストの `PipelineContext` 偽装 monkeypatch 地獄 (`DEFAULT_DB_PATH` / 各モジュールの `JSON_DIR` を個別パッチ)
2. 依存関係が暗黙的 (Phase 6 が Phase 4 のグラフと Phase 5 の scores に依存することが `PipelineContext` フィールド使用箇所を読まないと分からない)
3. 部分再実行が面倒 (中間成果物を手動で保存/復元)
4. 観測性が弱い (どの phase が何秒、どこで落ちたか grep 頼み)

### 期待効果
- `PipelineContext` dataclass 解消 → テスト monkeypatch 激減
- 関数シグネチャから DAG 自動構築 (宣言的・可視化可能)
- 部分再実行: `driver.execute(["person_fixed_effects"])`
- 各 node が単独でユニットテスト可能 (fixture 引数のみ)

### スコープ境界 (触らない)
- 分析アルゴリズム (AKM/IV/BiRank/PageRank のロジック不変)
- DB スキーマ
- Rust 拡張 (Hamilton node から呼び出す形)
- API / CLI / report generator

### 5.1 Phase H-1: PoC (analysis_modules だけ)

- [ ] `pixi add sf-hamilton`
- [ ] `src/pipeline_phases/analysis_modules.py` の 20+ モジュール呼び出しを Hamilton module に変換
- [ ] 既存の `ThreadPoolExecutor` 並列実行と同等以上の性能を確認 (`executors.MultiProcessingExecutor` or `ThreadPoolExecutor`)
- [ ] 既存テスト (`tests/test_pipeline_phases/`) pass 確認
- [ ] `PipelineContext` は残したまま、Phase 9 だけ Hamilton 担当の橋渡し構成

**判断ポイント**: H-1 終了時に「DAG 可視化・部分再実行・テスト容易性」が効果を出すか評価。効かなければ H-2 以降中止。

### 5.2 Phase H-2: Phase 5-8 を Hamilton 化

- [ ] `core_scoring.py` (AKM / IV / PageRank / BiRank)
- [ ] `supplementary_metrics.py` (centrality, decay, career stage)
- [ ] `result_assembly.py`
- [ ] `post_processing.py` (percentile, CI)

### 5.3 Phase H-3: Phase 1-4 を Hamilton 化

- [ ] `data_loading.py`
- [ ] `validation.py`
- [ ] `entity_resolution.py` (5-step resolution を node 分解)
- [ ] `graph_construction.py`

### 5.4 Phase H-4: `PipelineContext` 完全削除

- [ ] `PipelineContext` dataclass を削除
- [ ] `src/pipeline.py` を Hamilton `Driver` の薄いラッパーに:
  ```python
  from hamilton import driver
  dr = driver.Builder().with_modules(data_loading, core_scoring, ...).build()
  results = dr.execute(final_outputs, inputs={"db_path": ...})
  ```
- [ ] Phase 10 (`export_and_viz.py`) の `ExportSpec` registry を Hamilton output node として再配線
- [ ] CLI `pixi run pipeline` エントリーポイント切替

### 5.5 Phase H-5: 観測・運用機能

- [ ] `@tag(stage="phase5", cost="expensive")` を各 node に付与
- [ ] 実行時間計測 adapter (Hamilton lifecycle hook or 自作)
- [ ] Hamilton UI 導入検討 (optional、別プロセス、CI 非組込)
- [ ] 部分再実行 CLI: `pixi run pipeline-node <node_name>`

### 中止判定

H-1 終了時に以下のいずれかなら H-2 以降中止、H-1 ロールバック:
- Hamilton overhead で Phase 9 並列実行が 20% 以上遅くなる
- 型ヒント + decorator の可読性が `PipelineContext` より悪い
- Rust 拡張との統合で顕著な複雑さが出る

### 成功判定

- [ ] 全 2161 テスト pass
- [ ] パイプライン実行時間が現状と同等以上
- [ ] `PipelineContext` が削除されている
- [ ] CI に DAG 可視化 (SVG 出力) 組み込み

### テスト戦略の変化

```python
# Before (monkeypatch 地獄)
def test_akm(tmp_path, monkeypatch):
    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(src.pipeline, "JSON_DIR", tmp_path)
    ctx = PipelineContext(...)  # 全フィールド埋める
    run_akm_phase(ctx)

# After (Hamilton)
def test_person_fixed_effects():
    result = core_scoring.person_fixed_effects(
        credits_df=synthetic_credits(),
        anime_df=synthetic_anime(),
    )
    assert result.theta_i.shape == (100,)
```

---

## 🟡 SECTION 6: テストカバレッジ

### 6.1 pipeline_phases ユニットテスト (13/15 未テスト)

重要度順:
- [ ] Phase 5 `core_scoring.py` — 補償根拠の中核
- [ ] Phase 8 `post_processing.py` — パーセンタイル・CI 計算
- [ ] Phase 9 `analysis_modules.py` — 並列実行 (Hamilton 化で一部解消見込み)

### 6.2 `patronage_dormancy.py` 直接テスト

IV テストではモック使用。実際の dormancy penalty 計算ロジック (指数減衰、猶予期間) が未検証。
- [ ] 直接テスト追加

### 6.3 VA パイプライン (7 モジュール全て未テスト)

- [ ] `va_akm`, `va_integrated_value`, `va_graph` 等の 7 モジュール

### 6.4 `generate_all_reports.py` 分割後ヘルパー

- [ ] `fmt_num`, `name_clusters_by_rank` 等の単体テスト

---

## 🟢 SECTION 7: 将来タスク (feat_* 層別分離)

現状 `feat_career` と `feat_network` は L2 (集約数値) と L3 (独自計算) が混在。修正頻度が異なるため分離が望ましい。

### 7.1 `feat_career` 分離
- [ ] `agg_person_career` (L2: `first_year`, `active_years`, `total_credits` 等) と `feat_career_scores` (L3: `growth_trend`, `activity_ratio` 等) に分割

### 7.2 `feat_network` 分離
- [ ] `agg_person_network` (L2: `n_collaborators`, `n_unique_anime`) と `feat_network_scores` (L3: `centrality`, `bridge_score` 等) に分割

### 7.3 `corrections_*` テーブル
- [ ] クレジット年補正・ロール正規化などの修正差分を生データから分離して追跡

---

## 実施順序

```
Phase α: スキーマ整合修復 (ブロッキング — 先に倒す)
  1.1 → 1.2 → 1.3 → 1.4 → 1.5 → 1.6

Phase β: Phase 4 残務 + DuckDB/Hamilton PoC (並行可)
  2.1, 2.2, 2.3, 2.4, 2.5  (Phase 4 残務)
  3.1, 3.2, 3.3, 3.4, 3.5  (コード一貫性)
  4.1 DuckDB Phase A PoC
  5.1 Hamilton H-1 PoC

Phase γ: 移行本格化 (β 効果確認後)
  4.2 DuckDB Phase B (GOLD)
  5.2 Hamilton H-2 (Phase 5-8)
  6.1, 6.2, 6.3, 6.4 (テストカバレッジ)

Phase δ: 完全移行
  4.3 DuckDB Phase C (BRONZE Parquet)
  5.3 Hamilton H-3 (Phase 1-4)
  4.4 DuckDB Phase D (SILVER, SQLite 撤去)
  5.4 Hamilton H-4 (PipelineContext 削除)
  5.5 Hamilton H-5 (観測)

Phase ε: 余裕時
  3.6 テスト AnimeAnalysis 移行
  7.1, 7.2, 7.3 feat_* 層別分離
```

### 並行・逐次の注意
- **α は必ず先**: schema 整合性が取れていない状態で DuckDB / Hamilton 移行はデバッグ困難
- **β の PoC 2 つは並行可**: DuckDB A と Hamilton H-1 は互いに独立
- **DuckDB Phase D と Hamilton H-4 は同時適用しない**: マージ競合が荒れる。どちらか先に完了させる

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: 方法論的パラメータは method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF / GPU Polars)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
