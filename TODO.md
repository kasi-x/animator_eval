# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-05-20 (Session 3 完了: 3 analysis v2 化 + 10 SPEC 強化)

完了済みは `DONE.md`、設計原則は `CLAUDE.md`、変更履歴は `CHANGELOG.md`。

---

## Session 3 (2026-05-20) 成果

Session 2 累積に加え、本 session で:
- **3 新規 v2 report** (survival_curves / structural_break / spillover_effects) を実装、registered count **42 → 45**
- **10 SPEC を充実化** (Bottom 5 + 新 Bottom 5 + o-series 5) で alternative_interpretations ≥ 2 + sensitivity_grid 3 axis 追加
- **scorecard mean 81.1 → 85.5** (+4.4)、目標 85+ 達成
- Top 5: temporal_foresight 100 / mgmt_attrition_risk 95 / policy_monopsony 92 / o2_mid_management 90 / o3_ip_dependency 90
- 全 quality-gates audit pass、`tests/integration/test_quality_infrastructure.py` 25/25 pass
- 3 新規 report に cross_reference エントリ追加 (mentor_effect ↔ spillover_effects ほか)

実 data 経路: feat_career.latest_year / person_scores.person_fe / credits.credit_year に schema 適合済。spillover real 経路は cap 強化 (600 person / 6000 credit) で 5-8 秒 / report。

---

## Session 2 全 5 ラウンド累積成果 (再開時の起点)

15 commit (`df2debb` → `62f033e`)、**349 tests pass**、**quality scorecard mean 81.1/100**、**全 audit 0 issues**。

| 観点 | 値 |
|------|----|
| v2 reports | 38 → **42 registered** |
| Analysis modules | **13 新規** (resilience, oaxaca, cohort_inequality, cox_visibility, mentor_effect, heterogeneous_effects, did_robustness, credit_anomaly, power_analysis, multiple_testing, survival_curves, structural_break, spillover_effects) |
| Method notes | **7 新規** (`docs/method_notes/`) |
| Mart DDL tables | **9 新規** (feat_did_hte, feat_mentor_pairs/event_study/did_matched, feat_credit_anomaly_flags, feat_did_robustness, feat_network_resilience, feat_cohort_inequality, feat_oaxaca_decomposition) |
| post_processing drivers | **7 skeleton + 2 実 driver** (cohort_inequality / anomaly_flag) |
| forbidden_vocab | 4 → **0 violations / 56 files** |
| Findings/Interpretation lint | 14 → **0 warnings** |
| SPEC coverage | **55/55** (assert_valid pass) |
| Method gate audit | **55/55 pass** (CI + null + holdout/sensitivity) |
| Cross-reference | 13 → **56/56 reports linked** |
| Quality scorecard | mean **81.1** (top: temporal_foresight 100 / mgmt_attrition_risk 95) |
| brief sections | policy 5→10 / hr 6→8 / business 6→7 (executive_summary auto-inject) |
| Taskfile | `task quality-gates` で 6 audit を 1 コマンド一括実行 |

### 再開時の動作確認コマンド

```bash
pixi run task quality-gates           # 6 audit 一括 (vocab / lint / spec / method-gate / scorecard / lineage)
pixi run python scripts/report_generators/quality_scorecard.py   # scorecard mean 確認
pixi run test-scoped tests/integration/test_quality_infrastructure.py   # 15 tests
pixi run test-scoped tests/integration/test_new_modules_integration.py  # 10 tests
```

---

## 優先度マトリクス (残務)

| 優先度 | カテゴリ | 内容 | Claude |
|--------|---------|------|:------:|
| 🟠 Block | MAL/Jikan scraper | Card 05 全件 scrape (~9.4 日完走) → `TASK_CARDS/12_mal_scraper_jikan/05_rescrape.md` | ❌ |
| 🟠 Block | gender enrichment | `persons.gender` null 80.9% → 70% 閾値達成には MAL Card 05 + AniList orphan backfill 必要 (§14) | ❌ |
| 🟠 Block | AniList orphan backfill | カード起票済 (`TASK_CARDS/36_anilist_orphan_backfill/01_orphan_backfill.md`)、実装 scrape 律速 (~16 時間) | ❌ |
| 🟠 High | 残 7 driver の実 data 投入 | feat_did_hte / feat_mentor_* / feat_did_robustness / feat_oaxaca → upstream feat_did_studio_transfer + theta_i panel + gender 充足が前提 | ⚠️ 依存待 |
| ✅ 完了 | 3 新規 analysis の v2 report 化 | survival_curves / structural_break / spillover_effects を v2 report 実装 (42 → 45 reports)。Session 3 で完了 | — |
| ✅ 完了 | quality scorecard 底上げ | mean **81.1 → 85.5** (+4.4)、10 SPEC 強化 (Bottom 5 + 新 Bottom 5 + o-series 5)。Session 3 で完了 | — |
| 🟡 Medium | network_resilience 実 generate | 4,912 nodes / 2.6M edges 実 graph で HTML 生成 (現状 cap k=20、~分単位の所要)。perf 改善 or partial generation | ✅ |
| 🟡 Medium | Temporal foresight 真 holdout | 現状 concept-only、feat_career_annual / feat_person_scores データ投入後に真 holdout validation 実装 | ⚠️ 依存待 |
| 🟡 Medium | brief HTML 実生成 | `generate_briefs_v2.py` で JSON 出力済、HTML wrap 出力 (export.py) は別 path | ✅ |
| 🟡 Medium | 既存 reports の data-driven 化 | causal_studio_transfer の HTE section / mentor_effect が feat_did_hte / feat_mentor_event_study 読込 (driver 投入後に有効) | ⚠️ 依存待 |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ (Wikidata rate limit 解放待ち) | ❌ |
| 🟢 Future | 新ソース | LiveChart / Wikidata 受賞 → `TASK_CARDS/16_new_sources/` | ❌ |
| 🟢 Future | bangumi cron | 日次差分 API cron → `TASK_CARDS/17_bangumi_diff_cron.md` (dump 安定運用後) | ❌ |
| 🟢 Future | Atlas migration | DB v63 物理 schema 反映、9 新規 Mart テーブルを SQLModel `src/db/schema.py` 登録 | ✅ |
| 🟢 Future | CI workflow GitHub Actions | `.github/workflows/quality.yml` で `task quality-gates` 自動実行 | ✅ |
| ⛔ 除外 | 法的・倫理 | `TASK_CARDS/29_legal/` (3) — 法務 review | ❌ |
| ⛔ 除外 | ステークホルダー | `TASK_CARDS/30_stakeholder/` (3) — JAniCA outreach | ❌ |
| ⛔ 除外 | Business (startup) | `TASK_CARDS/31_business/` (3) — 法人形態 / funding | ❌ |
| ⛔ 除外 | 政策提言経路 | `TASK_CARDS/33_policy/` (3) — 議連 / 省庁 | ❌ |

---

## Phase 進捗スナップショット

| Phase | 内容 | 状態 |
|------:|------|------|
| A | 待機系 scrape (MAL Card 05 + AniList orphan) | 🟠 Claude 不可、外部依存 |
| B | pipeline driver 実装 | ✅ 2 実 driver + 5 skeleton (graceful) |
| C | HTML 実生成 + smoke | 🟡 brief JSON 4 件出力済、HTML wrap 未、42 report 全 smoke 未 |
| D | 新規 3 analysis (survival/break/spillover) | ✅ analysis module 完成 + tests、v2 report 化未 |
| E | インフラ (CI / Atlas / holdout) | 🟡 Taskfile quality-gates 完了、GitHub Actions / Atlas / holdout 未 |

呼出例: `pixi run python -c "from src.pipeline_phases.post_processing import run_all_session2_drivers; print(run_all_session2_drivers())"`

---

## Session 3 着手順 (再開時、上から順に推奨)

各タスクに **再開コマンド** / **依存** / **期待結果** を明記。

### 1. 残 3 analysis を v2 report 化 (推奨最優先、依存なし、Claude 可)

| analysis | report file (新規作成) | audience | 推定難度 |
|----------|---------------------|----------|---------|
| `src/analysis/career/survival_curves.py` | `scripts/report_generators/reports/survival_curves_report.py` | technical_appendix | 中 (既存 KM パターン流用) |
| `src/analysis/quality/structural_break.py` | `scripts/report_generators/reports/structural_break_report.py` | policy / technical_appendix | 中 (forest_plot + break candidate) |
| `src/analysis/causal/spillover_effects.py` | `scripts/report_generators/reports/spillover_effects_report.py` | publication / technical_appendix | 高 (2SLS 結果 + weak IV flag) |

```bash
# 着手前 baseline
pixi run task quality-gates
pixi run python scripts/report_generators/quality_scorecard.py
# 期待: mean 81.1、v2 42 reports

# 各 report 実装後
pixi run python scripts/generate_reports.py --only <new_report>
pixi run python scripts/report_generators/ci_check_spec_coverage.py
pixi run python scripts/report_generators/ci_check_method_gate.py
# 期待: v2 45 reports、scorecard mean 上昇
```

### 2. 次 Bottom 5 SPEC 充実化 (scorecard 81.1 → 85+ 目標)

対象: `network_analysis` / `network_graph` / `madb_coverage` / `cohort_animation` / `knowledge_network`

各 SPEC に追加:
- `alternative_interpretations`: ≥ 2 件
- `sensitivity_grid`: 2-3 axis (`SensitivityAxis(name=..., values=...)`)
- `identifying_assumption`: ≥ 100 char (現状 30-50 程度)

```bash
# 修正前
pixi run python scripts/report_generators/quality_scorecard.py 2>&1 | grep "Bottom"

# 修正後
pixi run python scripts/report_generators/quality_scorecard.py 2>&1 | grep "Mean"
# 期待: mean ≥ 83
```

### 3. brief HTML wrap 出力経路実装

```bash
# 現状: JSON 出力済 (4 件、29-39 KB)
ls result/json/{policy,hr,workers,business}_brief.json

# export.py の brief HTML renderer を確認、必要なら CLI flag 追加
grep -n "def\|render_brief\|to_html" scripts/report_generators/export.py | head

# 期待: result/reports/{policy,hr,business}_brief.html 出力
```

### 4. 42 v2 report 全 generate smoke

```bash
# 個別 only 指定なしで全 report generate
pixi run python scripts/generate_reports.py 2>&1 | tee /tmp/all_reports.log | tail -20
# 期待: "Done in Xs — 42 OK"
# graceful skip 可、失敗は --only <name> で再現
```

### 5. GitHub Actions workflow 実装

```bash
# .github/workflows/quality.yml 新規作成、task quality-gates + pixi run test を実行
ls .github/workflows/ 2>&1
# 期待: quality.yml が PR / push で自動 run
```

### 6. プロジェクト目的経路 → カード対応 (参考)

全 主要 cards 完了済。実 data 動作待ちは §14 gender enrichment 依存のみ。

| カード | 状態 |
|--------|------|
| `25/01-04` 補償公正 | ✅ 全完了 (25/04 は §14 待ち) |
| `26/01-03` 業界構造 | ✅ 全完了 |
| `27/01-03` 方法論強化 | ✅ 全完了 |
| `15/01-08` 拡張目的 | ✅ 7 完了 / 15/08 stop-if |
| `34/01-04` レポート再構築 | ✅ 全完了 |
| `28/01` 品質監視 | ✅ 完了 |
| `35/01` nationality 流入路 | ✅ 完了 (3.48% → 12.26%) |
| `36/01` AniList orphan backfill | 🟠 起票済 (scrape 律速、Claude 不可) |
| 新規 mentor_effect / cohort_inequality / network_resilience / credit_anomaly_audit | ✅ 実装完了 |
| 新規 survival_curves / structural_break / spillover_effects | ✅ analysis module 実装、v2 report 化は Session 3 候補 |

---

## SECTION 12: 他ソース / scraping 残務 (Claude 不可、長時間 scrape)

- **12.1 AniList orphan backfill** — credits 由来 anilist:p 90K に Staff GraphQL batch fetch (gender / hometown / birthday / image)。カード起票済 (`TASK_CARDS/36_anilist_orphan_backfill/01_orphan_backfill.md`)、実装 scrape 律速 (~16 時間)
- **12.3 MAL Card 05** — `pixi run python -m src.scrapers.mal_scraper` 全件 ~9.4 日完走。MAL gender / persons 大幅増 → §14 アンロック
- **12.4 新ソース** — LiveChart 放送スケジュール、Wikidata 受賞 (rate limit 解放待ち)
- **13.6 bangumi 日次差分 cron** — `TASK_CARDS/17_bangumi_diff_cron.md` 待機 (dump 安定運用後)

完了済: AniList characters/voice_actors/staff/anime 主要 + 拡張列、bangumi BRONZE/Conformed、tmdb 14/09。

---

## SECTION 14: gender enrichment 充足現状

`equity_oaxaca` / `o1_gender_ceiling` の Pre-condition: SILVER `persons.gender` null 率 **80.9%**、閾値 70% 未達。

| Source | persons | gender | 率 | 状態 |
|--------|--------:|-------:|---:|------|
| anilist | 97,596 | 5,894 | 6.0% | ✅ 統合済、orphan 90K 未着手 |
| bangumi | 21,125 | 12,646 | 59.9% | ✅ 統合済 |
| tmdb | 293,115 | 109,040 | 37.2% | ✅ 統合済 |
| ann | 36,350 | 0 | 0.0% | ❌ source 制約 |
| keyframe | 35,395 | 0 | 0.0% | ❌ source 制約 |
| mal | 40,551 | 0 | 0.0% | 🟡 Card 05 待ち |
| seesaawiki | 137,014 | 0 | 0.0% | ❌ source 制約 |

**全体**: persons 732,460 / gender 140,226 / null **80.9%**

**改善経路**: MAL Card 05 (+大幅) → AniList orphan backfill (+数千) → 70% 達成。

---

## SECTION 15: 完了済セクション → DONE.md / CHANGELOG.md 参照

詳細は `DONE.md` / `CHANGELOG.md` の該当エントリ。Session 2 全 5 ラウンドの累積成果は冒頭サマリ参照。

- v3 Reports & Visualization (旧 §16)
- 補償公正の核 25/01-04
- 業界構造観察 26/01-03
- 方法論強化 27/01-03
- 品質監視 28/01
- レポート再構築 34/01-04
- データ流入路修復 35/01
- 13 新規 analysis module + 4 新規 v2 report + quality 基盤 (Session 2 全 5 ラウンド、`[v3.1] CHANGELOG`)

---

## SECTION 18: コード全体整備・改善案 (2026-05-20 深堀考察)

Session 2 完了後の repo 全体 audit から抽出。優先度別、各タスク **着手手順** / **Claude 可否** / **依存** を明記。

### 18.1 🟠 即効・低リスク (disk / housekeeping)

| # | 内容 | 影響 | 状態 |
|---|------|------|------|
| 18.1.1 | `.tmp/` 227 GB = DuckDB temp 残骸 / 中断 pipeline 痕跡 | disk -227 GB | **✅ 完了 2026-05-20** `.gitignore` に `.tmp/` 追加 + `rm -rf .tmp/`、disk 602 G 空きに改善 |
| 18.1.2 | `src/result/gold.duckdb` 誤 path = 相対 path `result/silver.duckdb` (CWD 依存) | data 一貫性 | **✅ 完了 2026-05-20** `src/result/gold.duckdb` 削除 + `src/etl/lineage/collect.py` の `DEFAULT_SILVER_PATH` / `DEFAULT_BRONZE_ROOT` を `Path(__file__).resolve().parents[3]` 起点絶対 path に修正 |
| 18.1.3 | `grants.zip` / `grants/` repo commit 状況 | repo size | **✅ 確認済** `.gitignore` 既登録 (54-60 行)、tracked なし |
| 18.1.4 | 小文字 `todo.md` (20 KB) = Wikidata 設計書 | docs 整合 | **✅ 完了 2026-05-20** `TASK_CARDS/16_new_sources/03_wikidata_world.md` へ整形移管、`todo.md` 削除 |
| 18.1.5 | `refactor.md` (258 行) T1-T7 委託タスク 1 ヶ月放置 | tech debt | **✅ 完了 2026-05-20** 内容を 18.2.1-18.2.8 へ昇格、`refactor.md` 削除 |
| 18.1.6 | `tests/__pycache__` 残留 (6 dir) | clean | **✅ 完了 2026-05-20** 削除実行 (0 dir 残)。pre-commit hook 検討は 18.4.3 |

### 18.2 🟠 refactor.md タスク audit 結果 (2026-05-20)

`refactor.md` 2026-04-23 起票分の現状 audit。**T1-T7 = 全件既消化** (DONE.md 未記載だが実態は適用済)、**T8 のみ未消化、むしろ膨張**。

| # | 旧 ID | 内容 | 状態 |
|---|-------|------|------|
| 18.2.1 | T1 | SQL `COALESCE(NULLIF(p.name_ja,''))` 集約 | **✅ 完了** `sql_fragments.py` 存在、scripts/ 内 46 → **2 occurrences** (95% 削減) |
| 18.2.2 | T2 | VA `core_scoring.py` 工程分解 | **✅ 完了** `_run_va_{akm,birank,trust,patronage,dormancy,awcc_placeholder,iv}` 全 helper 実装済 + `_va_credits_to_pseudo_credits` + `_build_sd_assignments` 追加 |
| 18.2.3 | T3 | VA `result_assembly.py` record builder 分解 | **✅ 完了** `_build_pid_to_name_map` / `_build_va_base_record` / `_enrich_with_diversity` / `_sort_results_by_iv` 全実装済 |
| 18.2.4 | T4 | `src/api_reports.py` error_boundary + dead code | **✅ 構造変更で消滅** 旧 file 削除、`src/routers/{i18n,persons,reports,validators}.py` に分割移行済。同等指摘の再 audit は別 issue 化 |
| 18.2.5 | T5 | `name_utils.py` 分解 + 3 重 `resp.json()` バグ | **✅ 完了** `_atomic_write_json` / `_build_llm_prompt` / `_call_ollama_generate` / `_parse_country_code` / `_from_script_direct` / `_resolve_zh_or_ja` / `_resolve_arabic` 全実装済。`payload = resp.json()` 1 回化、`except (httpx.HTTPError, ValueError)` 絞り込み済 |
| 18.2.6 | T6 | `enrich_hometown_nationality.py` 分解 + dead var | **✅ 完了** `_print_cache` / `_ensure_llm_reachable_or_exit` / `_classify_candidate` / `_process_row` / `_print_summary` 全実装済 |
| 18.2.7 | T7 | `section_builder.py` `_load_lineage_row` 二重クエリ | **✅ 消滅** `_load_lineage_row` 関数自体が削除済 (別実装に置換) |
| 18.2.8 | T8 | 巨大 reports 段階分解 — **唯一未消化、膨張中** | **🟠 残務** `score_layers_analysis.py` 1491→**1663 行** (+11%) / `bridge_analysis.py` 1028 行 / `network_analysis.py` 1054 行 / `network_graph.py` 468 行。総 4213 行。`_build_*_section` helper 未検出 |

### 18.3 🟡 構造的改善 (中規模、低-中リスク)

| # | 内容 | 動機 | 着手 | Claude |
|---|------|------|------|:------:|
| 18.3.1 | **物理 file rename**: `silver.duckdb` → `conformed.duckdb`、`gold.duckdb` → `mart.duckdb`、`bronze/` → `source/` (CLAUDE.md の「当面維持」を解除する時機。Source/Conformed/Mart 移行完了済) | 命名整合 | (a) `DEFAULT_*_PATH` 定数 grep 列挙 (b) atomic rename + symlink fallback で段階移行 (c) 全 test pass 確認 (d) symlink 削除 | ✅ |
| 18.3.2 | **`bare except Exception:` 595 件のリスク区域絞り込み** — `src/etl/` (ETL 中の silent fail = 旧 SILVER 21/01 系問題)、`src/scrapers/` (network error mask) を最優先で具体例外型に絞る | silent fail 防止 | `rg -n "except Exception" src/etl/ src/scrapers/` → ファイル単位で `(httpx.HTTPError, ValueError, json.JSONDecodeError)` 等に絞る | ✅ |
| 18.3.3 | **`typing.List/Dict/Optional` → 3.12 builtin** (15 ファイル) | code style | `ruff` rule `UP006` / `UP007` を `pyproject.toml` に有効化 → `pixi run ruff check --fix` | ✅ |
| 18.3.4 | **docs/ 40+ MD 整理** — 中間設計文書 `ARCHITECTURE_5_TIER_PROPOSAL.md` / `ARCHITECTURE_CLEANUP.md` / `ETL_SCHEMA_V55_INTEGRATION.md` 等を `docs/archive/` へ移動 (既に `docs/archive/` 9 ファイル先例あり) | 可読性 | 各 MD を最新化 vs archive 判定 → mv + cross-ref 修正 | ✅ |
| 18.3.5 | **`PROJECT.md` 797 行 と `CLAUDE.md` 261 行 の整合性** — `PROJECT.md` は CLAUDE.md と重複する Overview / Architecture を含む可能性。役割分担: `CLAUDE.md`=AI 向け指針、`PROJECT.md`=人間向け詳細 → 重複セクション削除 | docs 単純化 | diff 取って重複削減 | ✅ |
| 18.3.6 | **`TASK_CARDS/` 整理** — 31+ サブディレクトリ、完了済 (01-04, 06, 07, 14, 25-28, 34, 35) を `TASK_CARDS/archive/` へ移動 | 可読性 | DONE.md の card 完了 entry を crosswalk → 移動 | ✅ |
| 18.3.7 | **`jvmg_fetcher.py` role map バグ修正** (`todo.md` / `refactor.md` で指摘済、未消化) | data 正確性 | Wikidata prop P57/P58/P162/P1431 mapping を `src/utils/role_groups.py` 24 種に正しく寄せる + JVMG credits 再投入 | ✅ |
| 18.3.8 | **`PipelineContext` 完全削除の痕跡 audit** (CLAUDE.md 主張済) | 整合性 | **🟠 部分完了 2026-05-20**: `src/` 配下は全て docstring/comment 言及のみ (実 class 使用なし、CLAUDE.md 主張正しい)。但し **tests 2 ファイル collection ERROR**: `tests/unit/test_analysis_modules.py` + `tests/test_va_pipeline_phases.py` が `from src.pipeline_phases.context import PipelineContext` で **module 不在**、テスト走らず → 18.3.10 で個別対応 | ✅ |
| 18.3.9 | **v2 report registry 数の乖離調査** — `scripts/report_generators/reports/` に 64 ファイル、TODO は 45 registered (Session 3 後)。差は `_base.py` / `derived_params.py` / 3 brief_index ほか helper | 数字の信頼性 | **🟠 部分判明 2026-05-20**: `registry` module 存在せず (`scripts.report_generators.registry` import error)。実 registry は `generate_reports.py` 内 dispatch 経由。実数列挙手段確立 → CI で自動 check | ✅ |
| 18.3.10 | **壊れテスト 2 ファイル修復** (18.3.8 派生) — `tests/unit/test_analysis_modules.py` + `tests/test_va_pipeline_phases.py` が `from src.pipeline_phases.context import PipelineContext` で collection error。CLAUDE.md「完全削除済」なので **テスト側を新 typed dataclass に書き換え** or **削除** (該当機能の他テストでカバー済か確認必要) | テスト網羅 | ✅ |

### 18.4 🟡 CI / quality gate (Session 3 既存項目との合流)

| # | 内容 | 状態 | Claude |
|---|------|------|:------:|
| 18.4.1 | `.github/workflows/quality.yml` で `task quality-gates` + `pixi run test` 自動実行 (Session 3 #5) | TODO 既存 | ✅ |
| 18.4.2 | `forbidden_vocab` lint を pre-commit hook 化 (`scripts/report_generators/lint_vocab.py` を git hook で実行) | 提案新規 | ✅ |
| 18.4.3 | `tests/__pycache__` cleanup を pre-commit hook 化 | 提案新規 | ✅ |
| 18.4.4 | DB v63 物理 schema を `src/db/schema.py` SQLModel に反映 (Atlas migration、9 新規 Mart テーブル) | TODO 既存 | ✅ |

### 18.5 🟢 観測・運用 (中長期)

| # | 内容 | 動機 | Claude |
|---|------|------|:------:|
| 18.5.1 | **Rust extension 利用率測定** — `src/analysis/graph_rust.py` の fallback ログ集計、graceful fallback が常時発火していないか確認 | perf 確証 | ✅ |
| 18.5.2 | **DuckDB temp storage 自動 cleanup** — pipeline 終了 / 中断 hook で `.tmp/duckdb_temp_storage_*` 削除 | disk 安定 | ✅ |
| 18.5.3 | **`htmlcov/` 自動再生成 task** — 古い coverage 残骸 (6 MB)、`pixi run test --cov` 都度再生成、commit 対象外確認 (`.gitignore` 済) | 鮮度 | ✅ |
| 18.5.4 | **`docs/method_notes/` 18 → SPEC との 1:1 同期 audit** — 各 SPEC の `method_note_id` 参照先が実在するか CI check | 整合性 | ✅ |

### 18.6 推奨着手順 (再開時)

1. **18.1 全 6 件** (即効、disk + housekeeping、1 セッションで完了)
2. **18.3.1** 物理 rename (1 PR、影響範囲明確)
3. **18.2.1 (T1)** SQL 断片集約 (1 PR、mechanical)
4. **18.2.4 (T4) / 18.2.5 (T5) / 18.2.7 (T7)** dead code + bug fix (3 PR 並列可)
5. **18.2.2 / 18.2.3 / 18.2.6 (T2/T3/T6)** 工程分解 (3 PR)
6. **18.2.8 (T8)** section 単位段階分解 (N PR、最後)
7. **18.3 / 18.4 / 18.5** 余力で

---

## SECTION 17: 除外スコープ (Claude 範囲外)

- `TASK_CARDS/29_legal/` (3) 法務 review
- `TASK_CARDS/30_stakeholder/` (3) JAniCA / SNS / press kit outreach
- `TASK_CARDS/31_business/` (3) 法人形態 / B2C 設計 / funding plan
- `TASK_CARDS/33_policy/` (3) 議連 / 省庁 outreach

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
