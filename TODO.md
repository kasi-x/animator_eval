# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-05-20 (Session 2 ラウンド 5 完了)

完了済みは `DONE.md`、設計原則は `CLAUDE.md`、変更履歴は `CHANGELOG.md`。

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
| 🟠 High | 3 新規 analysis の v2 report 化 | survival_curves / structural_break / spillover_effects を v2 report HTML として実装 (42 → 45 reports) | ✅ |
| 🟠 High | quality scorecard 底上げ | mean 81.1 → 85+ 目標。次 Bottom 5 (network_analysis / network_graph / madb_coverage / cohort_animation / knowledge_network) の SPEC 充実化 | ✅ |
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
