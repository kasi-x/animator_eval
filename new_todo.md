# new_todo.md — リファクタリング作業の続き

作成: 2026-04-23 / セッション: updater
本書は **このセッションで途中まで進めたリファクタリング作業** の続きを次回以降が拾えるようにした覚書。`TODO.md` 本体には載せない (機能タスクではないため)。

---

## このセッションで完了したこと

| commit | 内容 |
|---|---|
| `1dd4a90` | §1.4 anime_display 廃止 / §3.3 entity_resolution_audit を DONE 化 (両方とも先行作業で完了済だった、TODO が stale だった) |
| `811aa18` | dead code 削除: `AnimeDisplay` class + F401 27 件 + F841 9 件 (-67 行、15 files) |
| `944e32d` | `scripts/report_generators/export.py:BriefHTMLRenderer.render` 分解 (118 → 15 + 11 helpers) |
| `415aa3e` | `scripts/report_generators/section_builder.py:method_note_from_lineage` 分解 (103 → 18 + 7 helpers) |
| `7e4081c` | scraper 強化 ANN focus: file logging (logging_utils), retry 拡張 (500/502/504 + httpx 例外 3 種), `_ANN_TYPE_MAP` case-insensitive 化 (OAV→OVA バグ修正), 4 フィクスチャ + 40 件のネット非依存テスト |
| `107cbfe` | scraper retry 統一: `src/scrapers/http_client.py:RetryingHttpClient` 新設 → mal/jvmg/allcinema を移行 (60+ 行重複削減)。anilist/mal/allcinema のフィクスチャ + parse テスト追加。全 scraper の typer command に file_logging hook |

ruff F401/F841/F811 は処理した安定領域で **0 件**。

---

## ユーザ preference (作業方針)

新規メモリ保存済み:
- [feedback_function_decomposition.md](~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_function_decomposition.md): **過剰なくらい関数化されたコードを好む**。3-5 行の private helper でも名前があれば OK。Claude default の「不要な抽象化を避ける」より優先
- [feedback_dead_code_aggressive.md](~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_dead_code_aggressive.md): **dead code は見つけ次第削除**。git history があるので延命無用。ただし agent 領域は除外

---

## 残っている関数分解候補 (安定領域、優先度順)

40 行以上の関数を `ast` で抽出。agent territory (`src/scrapers/`, `src/etl/`, `src/analysis/`, `src/pipeline_phases/`, `src/database.py`, `scripts/generate_all_reports.py`) は除外済み。

| 行数 | 場所 | 関数 | 注 |
|---:|---|---|---|
| 106 | `scripts/report_generators/html_templates.py:778` | `wrap_html_v2` | report 系の最頻 caller、効果大 |
| 87 | `scripts/report_generators/html_templates.py:454` | `wrap_html` | 旧版だがまだ使用中、対応推奨 |
| 86 | `src/utils/performance.py:268` | `print_report` | utils stable、テストもある |
| 72 | `scripts/report_generators/report_brief.py:187` | `ReportBrief.validate` | vocab 検査含む validation chain |
| 65 | `scripts/report_generators/html_templates.py:907` | `plotly_div_safe` | Plotly HTML 埋め込みヘルパ |
| 50 | `src/utils/json_io.py:500` | `save_pipeline_json_if_data_present` | 保存判定+書き込み混在 |
| 49 | `scripts/report_generators/section_builder.py:145` | `validate_findings` | regex 検査 chain |
| 46 | `src/utils/episode_parser.py:20` | `parse_episodes` | 文字列パース、case 分岐多 |
| 42 | `src/utils/display_lookup.py:178` | `get_display_description` | bronze 経由の説明取得 |
| 42 | `scripts/report_generators/section_builder.py:239` | `build_section` | section 組立 |
| 40 | `src/utils/performance.py:208` | `generate_report` | レポート生成 |

**避けるべき** (agent 触る可能性あり):
- `src/validation.py:222 validate_data_freshness` (97 行)
- `src/monitoring.py:36 check_data_freshness` (91 行)
- 上 2 つは `05_analysis_cutover` (silver.duckdb 切替) で touch される

### 進め方テンプレ

1. 関数を Read
2. 「概念上の塊」を identify (例: header / body / footer、validation chain の各 step)
3. 各塊を `_render_X` / `_validate_X` / `_load_X` 等の **動詞 + 対象** 名で extract
4. 元の関数は **塊を順に呼ぶだけの recipe** にする
5. 既存テストを `pixi run pytest` で回し、HTML 系は smoke render で構造保持を確認
6. commit message は decomposition tree を ASCII で描く (上記 commit 参照)

---

## 残っている dead code 候補 (未調査)

agent territory も含むので、調査時は git status を確認して衝突を避ける。

### 私が手を付けなかった F401/F841 (agent territory)

| ファイル | 件数 | 理由 |
|---|---:|---|
| `scripts/generate_all_reports.py` | 69 | §6.4 agent territory |
| `src/scrapers/anilist_scraper.py` 他 | 多数 | scraper agent territory |
| `src/models_v2.py` | 5 | DuckDB agent が触る予定 |
| `src/database.py` | 2 | 同上 |
| `src/database_v2.py` | 2 | 同上 |

### 構造的 dead code 候補 (調査要)

- **`src/database_phase1_plan.md`**: 「✅ anime_display table: HAS score/popularity/description」と書いてある planning doc。anime_display は v55 で削除済なので **stale**。削除可能
- **`src/database.py` の legacy migration 関数群** (~7000 行): TODO §1 Maintenance に「production DB が v55 で安定後、v1-v55 の legacy migration 関数を削除し `init_db()` のみを single source of truth にする」とある。本セッションでは触れず
- **`scripts/lint_report_vocabulary.py`**: agent が削除済 (`git status` で `D`)
- **`src/models.py`** の `AnimeAnalysis`: 「DEPRECATED」コメントが付いているが **実際には active** (`pipeline_phases/context.py`, `pipeline_phases/entity_resolution.py`, `src/utils/time_utils.py` で `Anime` として alias)。**コメントが misleading なだけ**、コードは消さない
  - 改善案: `AnimeAnalysis` の docstring から DEPRECATED 行を削除し、現実 (= canonical analysis 型) を反映する 1 行修正だけしておく

---

## 関連する別エージェントの作業状況 (2026-04-23 時点)

並列エージェントが触っている領域、衝突回避の参考。

- **04_duckdb agent** (worktree `agent-ad5e1958`): scraper migration 中、`src/etl/atomic_swap.py` が新規追加済
- **§6.4 agent**: `scripts/generate_all_reports.py`, `scripts/report_generators/helpers.py`, `tests/test_report_helpers.py` を触る
- **scraper 系 agents** (#13-#20 タスク): `src/scrapers/`, `tests/fixtures/scrapers/`, `src/scrapers/http_client.py` (新規) を触る
- **05_hamilton カード書き agent**: `TASK_CARDS/05_hamilton/H1-H5*.md` を新規作成済
- **06_tests カード書き agent**: `TASK_CARDS/06_tests/T01a-T03*.md` を新規作成済

→ 上記領域に入る場合は **必ず `git status` と `git log -10` で最新を確認**。

---

## 検出済みの真のテスト状態 (F audit 結果)

`pixi run test` を 2026-04-23 12:48 頃に実行:
- **2301 passed / 6 failed / 4 skipped** (405.70s)
- 6 failures は全て scraper 系 — 別 agent の作業中タスクと一致 (#14 ANN bug, #16 smoke, #18 fixture)
- CLAUDE.md は「1394 tests」と書いており **大幅に乖離** → CLAUDE.md 更新候補

```
FAILED tests/test_madb_scraper.py::TestMADBIntegration::test_scrape_with_mock_dump
FAILED tests/test_madb_scraper.py::TestMADBIntegration::test_scrape_multiple_contributor_fields
FAILED tests/test_madb_scraper.py::TestMADBIntegration::test_no_files_returns_empty_stats
FAILED tests/test_scraper_coverage.py::TestAniListBatchSave::test_save_anime_batch_to_database
FAILED tests/test_scraper_coverage.py::TestAniListBatchSave::test_save_persons_batch_to_database
FAILED tests/test_scraper_coverage.py::TestAniListBatchSave::test_save_credits_batch_to_database
```

→ 本セッションでは触らず (agent 担当領域)。

---

## invariant 健診結果 (G audit、2026-04-23)

CLAUDE.md 核心原則の機械的検証、全て pass:

```bash
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # コメント/docstring のみ
rg 'display_lookup' src/analysis/ src/pipeline_phases/   # __init__.py の boundary guard のみ
rg -n 'src_anilist_anime|src_mal_anime|...' src/analysis/ src/pipeline_phases/   # 0 件
rg 'anime\.popularity|anime\.favourites' src/analysis/ src/pipeline_phases/   # 0 件
rg 'anime_display' src/ tests/ | grep -v "src/database.py" | grep -v "_legacy\|_migrate_v"
  # 不在検証 test と planning doc のみ
```

特筆: `src/utils/import_guard.py` の `install_display_lookup_boundary_guard` が **runtime レベルで** display_lookup の analysis/pipeline_phases からの import を遮断。grep + import 二重防御。

---

## 2026-04-23 スクレイパー強化セッション残務

`7e4081c` + `107cbfe` で着手したが完了していない / 未着手の項目。

### A. ブロック中 (依存待ち)

#### A.1 差分更新 (incremental update) — TODO §17 対応
- **状態**: 保存側 (`src/database.py` / schema) 更新中につきブロック
- **やる必要があること**:
  1. `src_*_anime` テーブルに `fetched_at` / `content_hash` カラム追加 (schema 変更)
  2. upsert 時に hash 比較して変更時のみ update + `meta_scrape_changes` (source, entity_type, entity_id, change_type, before_hash, after_hash, changed_at) に差分記録
  3. scraper 側に `--since YYYY-MM-DD` mode 実装
     - ANN: masterlist の `lastModified` 利用 (現状 CDN endpoint 自体が壊れている問題あり)
     - AniList: GraphQL `updatedAt` フィルタ
     - MAL/Jikan: `updated_at` でフィルタ
- **scraper 側で先行実装可能なもの**: `content_hash` 算出 (sha256 of canonical JSON of relevant fields)。受け取り API が決まれば配線するだけ
- **担当**: 保存側 agent 完了後、scraper 担当が拾う

### B. 部分実装 / 引き継ぎ可

#### B.1 ANN Phase 3 を HTML スクレイプに書き換え — TODO §20 対応
- **状態**: user 側で着手の形跡あり (`ann_scraper.py` に `PEOPLE_HTML_BASE`, `parse_person_html`, `_parse_dob_html` が追加済、`tests/test_ann_scraper_parse.py` に 9 件のテスト追加済)
- **背景**: ANN の `?people=ID` API が `<warning>ignored</warning>` を返す状態 (2026-04-23 確認)。Phase 3 (scrape-persons) が完全に空振り
- **残り**: typer `cmd_scrape_persons` 内の `_run_scrape_persons` を XML batch fetch から HTML page fetch に切り替え (1 person = 1 GET、レート制限注意)
- **fixture**: `tests/fixtures/scrapers/ann/person_260.html` 取得済

### C. 意図的に skip した refactor (拡張余地あり)

#### C.1 anilist_scraper.query() の retry refactor
- **理由**: 220 行のカスタム `X-RateLimit-*` 監視 + token refresh + probe query 実装あり、機能差なし
- **やるなら**: 共通部分 (httpx 例外 catch + 指数 backoff) のみ `RetryingHttpClient` に委譲、X-RateLimit-* 専用 callback hook を追加。**価値**: 中 (重複削減 50 行程度)、**リスク**: 中 (rate-limit 動作が壊れると本番ペイン)

#### C.2 keyframe_scraper / mediaarts_scraper の HTTP 統一
- **理由**: HTTP layer が呼び出し側から `httpx.AsyncClient` を渡される構造で、client class なし。refactor は呼び出し階層全体を触る必要
- **やるなら**: `RetryingHttpClient` をモジュール内 factory で生成して内部で使う形に統一。**価値**: 小 (現状動いてる)、**リスク**: 中 (caller 側の影響範囲)

### D. テストカバレッジ未追加の scraper

| source | parse 関数 | テスト | 注 |
|---|---|---|---|
| jvmg / wikidata | `parse_wikidata_results` | 未 | SPARQL JSON fixture 取得 → 1 ファイル + 3-5 件のテストで足りる |
| keyframe | `extract_preload_data` 等 | 未 | HTML fixture 1 件で OK |
| seesaawiki | `parse_*` (3864 行内) | 未 | 旧来から quality audit メモ済 (`seesaawiki_parser_quality.md`)、parse 関数の特定が要 |

### E. 確認済の壊れた endpoint (要対応)

| endpoint | 症状 | 対応 |
|---|---|---|
| ANN `cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all` | HTML を返す (URL 廃止 or block) | fallback `_probe_max_id` で動作中。本来 nlist の正規パラメータ調査要 |
| ANN `?people=ID` API | `<warning>ignored</warning>` を返す | B.1 で対応中 |

### F. lint debt (user territory)

私が触らなかった lint 残:
- `src/scrapers/anilist_scraper.py:2603` F841 `existing_person_ids` 未使用
- `src/scrapers/ann_scraper.py:515` E402 mid-file `import dataclasses` (bronze writer 領域)
- `src/scrapers/allcinema_scraper.py:403` E402 同上

---

## CLAUDE.md drift 候補 (まだ未対応)

将来 CLAUDE.md 更新する際の修正対象:

- 「1394 tests」→ 「2300+ tests」 (2026-04-23 時点)
- 「SQLite WAL mode (storage)」→ DuckDB 移行後に更新 (`04_duckdb/06_sqlite_decommission` 完了時)
- Testing patterns の `monkeypatch DEFAULT_DB_PATH` → silver.duckdb / gold.duckdb 切替後に更新 (`04_duckdb/05_analysis_cutover` 完了時)
- 「Comprehensive refactoring complete (Phases 1-4 + parallelization)」 → さらに進んだ Phase の状況反映

**注**: CLAUDE.md 編集は影響大。1 commit に 1 ドリフト修正くらいの粒度で。
