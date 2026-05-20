# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-05-20 (Session 2 ラウンド 4 完了: レポート品質最高水準化、quality scorecard mean 79.5/100、315 tests pass、全 audit 0 issues)

未完了項目のみ。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス (残務)

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟠 Block | MAL/Jikan scraper | Card 05 全件 scrape (~9.4 日完走) → `TASK_CARDS/12_mal_scraper_jikan/05_rescrape.md`。Claude 単独不可 (rate-limited 長時間 scrape) |
| 🟠 Block | gender enrichment | SILVER `persons.gender` null 80.9% → 70% 閾値達成には MAL Card 05 + AniList orphan backfill 必要 (§15)。`equity_oaxaca` / `o1_gender_ceiling` 実データ動作の前提 |
| 🟠 Block | AniList orphan backfill | カード起票済 (`TASK_CARDS/36_anilist_orphan_backfill/01_orphan_backfill.md`)、実装 scrape 律速 (~16 時間)、Claude 単独不可 |
| 🟠 High | post_processing driver | 9 新規 Mart テーブル (feat_did_hte / feat_mentor_* / feat_credit_anomaly_flags / feat_did_robustness / feat_network_resilience / feat_cohort_inequality / feat_oaxaca_decomposition) への計算 driver 実装。DDL 済、計算経路は skeleton |
| 🟠 High | report HTML 実生成 | network_resilience 実 graph (4,912 nodes / 2.6M edges) で実 HTML 生成。現状 cap k=20 + auth skip で動作確認のみ |
| 🟠 High | brief HTML smoke | generate_briefs_v2.py で policy/hr/business brief HTML 実生成 (現状 dict 出力まで確認) |
| 🟡 Medium | quality scorecard 底上げ | mean 79.5 → 85+ 目標。Bottom 5 (network_evolution / derived_params / temporal_foresight / bridge_analysis / individual_view) の SPEC に alternative_interpretations + sensitivity_grid 追加 |
| 🟡 Medium | 残 3 analysis module | survival_curves (KM + log-rank wrap) / structural_break (CUSUM / Bai-Perron) / spillover_effects (peer IV proxy) — 概念定義のみ、実装未着手 |
| 🟡 Medium | Report methodology | Temporal foresight Section 3.3 holdout validation 実装 (feat_career_annual / feat_person_scores データ投入後) |
| 🟡 Medium | 既存 reports の data-driven 化 | causal_studio_transfer の HTE section / mentor_effect の event-study が feat_did_hte / feat_mentor_event_study を読込 (driver 投入後に有効) |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ (Wikidata rate limit 解放待ち) |
| 🟢 Future | 新ソース | LiveChart / Wikidata 受賞 → `TASK_CARDS/16_new_sources/` (TMDb は 14/09 で完了済) |
| 🟢 Future | bangumi cron | 日次差分 API cron → `TASK_CARDS/17_bangumi_diff_cron.md` (dump 安定運用後) |
| 🟢 Future | Atlas migration apply | DB v63 物理 schema 反映、9 新規 Mart テーブルを SQLModel `src/db/schema.py` に登録 |
| 🟢 Future | CI workflow | quality_scorecard / spec_coverage / method_gate / lineage_register / lint_findings_separation を CI に統合 |
| ⛔ 除外 | 法的・倫理 | `TASK_CARDS/29_legal/` (3 cards) — 法務 review、人間判断 |
| ⛔ 除外 | ステークホルダー | `TASK_CARDS/30_stakeholder/` (3 cards) — JAniCA outreach 等 |
| ⛔ 除外 | Business (startup) | `TASK_CARDS/31_business/` (3 cards) — 法人形態 / 法務 |
| ⛔ 除外 | 政策提言経路 | `TASK_CARDS/33_policy/` (3 cards) — 議連 / 省庁 outreach |

---

## 推奨着手順 (Session 3 候補)

### Phase A: 待機系の解放 (Claude 不可、長時間 scrape)

```
1. MAL Card 05 全件 scrape (~9.4 日、放置可)
   → 完了後 §15 gender enrichment 約 40-50% カバー
2. AniList orphan backfill 実走 (~16 時間、rate-limited)
   → §15 70% 達成 → equity_oaxaca / o1_gender_ceiling が本格動作
```

### Phase B: pipeline driver 実装 (Claude 可、コード)

```
3. post_processing.run_oaxaca() driver — equity_oaxaca を実 data 駆動
4. post_processing.run_resilience() driver — network_resilience を実 graph 駆動
5. post_processing.run_cohort_inequality() — cohort_inequality を駆動
6. post_processing.run_mentor_audit() — mentor pair + event-study + matched DiD
7. post_processing.run_hte() — feat_did_hte 投入
8. post_processing.run_did_robustness() — placebo + E-value + joint leads 投入
9. post_processing.run_anomaly_flag() — credit_anomaly 3 detector 投入
```

### Phase C: HTML 実生成 + 検証

```
10. brief HTML 実生成 (generate_briefs_v2.py)
11. 42 v2 report 全 generate smoke
12. quality scorecard mean 79.5 → 85+ 底上げ
    (Bottom 5: network_evolution / derived_params / temporal_foresight /
     bridge_analysis / individual_view の SPEC 充実化)
```

### Phase D: 新規 analysis 3 module

```
13. survival_curves (Kaplan-Meier + log-rank wrap、role_progression と統合)
14. structural_break (CUSUM / Bai-Perron、時系列構造変化点検出)
15. spillover_effects (peer effect IV proxy、mentor の延長)
```

### Phase E: インフラ整備

```
16. CI workflow に 4 audit (scorecard / spec / method_gate / lineage) 統合
17. Atlas migration apply (DB v63 + 9 新規テーブル → SQLModel 登録)
18. temporal_foresight の真 holdout validation (現状 concept のみ)
```

---

## プロジェクト目的経路 → カード対応 (参考)

| カード | publication | business | 政策 | 状態 |
|--------|:-----------:|:--------:|:----:|------|
| `25/01_did_studio_transfer` | ◎ | ○ | ◎ | ✅ 実装完了、HTE section 追加済、SPEC 完備 |
| `25/02_opportunity_residual_null` | ○ | ◎ | ◎ | ✅ 完了 (commit `d03d57c`) |
| `25/03_visibility_loss_holdout` | ○ | ◎ | ○ | ✅ 完了 + Cox PH 並設可能化 |
| `25/04_pay_equity_decomp` | ◎ | ○ | ◎ | ✅ equity_oaxaca 実装、§15 gender 充足待ち |
| `26/01_committee_influence` | ◎ |  | ◎ | ✅ 完了 |
| `26/02_international_collab` | ○ | ◎ |  | ✅ 完了、nationality 流入路修復済 |
| `26/03_studio_pipeline_strength` |  | ◎ | ○ | ✅ 完了 |
| `27/01_missingness_disclosure` | ◎ | ◎ | ◎ | ✅ 完了 (全 report で auto-inject) |
| `27/02_career_trajectory_typology` | ◎ | ○ |  | ✅ 完了 |
| `27/03_iv_xai` | ○ | ◎ |  | ✅ 完了 |
| 新規 `mentor_effect` | ○ | ◎ |  | ✅ skeleton、pipeline driver 待ち |
| 新規 `cohort_inequality` |  | ○ | ◎ | ✅ 実装完了 |
| 新規 `network_resilience` |  |  | ◎ | ✅ 実装完了、実 graph 生成 perf 課題 |
| 新規 `credit_anomaly_audit` |  |  |  | ✅ 完了 (技術監査) |

---

## SECTION 12: 他ソース / scraping 残務

- **12.1 AniList orphan backfill** — credits 由来 anilist:p 90K に Staff GraphQL batch fetch (gender / hometown / birthday / image)。カード起票済 (`TASK_CARDS/36_anilist_orphan_backfill/01_orphan_backfill.md`)、実装 scrape 律速 (~16 時間)
- **12.3 MAL Card 05** — `pixi run python -m src.scrapers.mal_scraper` 全件 ~9.4 日完走。MAL gender / persons 大幅増 → §14 アンロック
- **12.4 新ソース** — LiveChart 放送スケジュール、Wikidata 受賞 (rate limit 解放待ち)
- **13.6 bangumi 日次差分 cron** — `TASK_CARDS/17_bangumi_diff_cron.md` 待機 (dump 安定運用後)

完了済: AniList characters/voice_actors/staff/anime 主要 + 拡張列、bangumi BRONZE/Conformed、tmdb 14/09。

---

## SECTION 13: 拡張目的レポート群 → `TASK_CARDS/15_extension_reports/`

8 種拡張目的レポート + cross-cutting。

| ID | 状態 |
|----|------|
| `15/01_o3_ip_dependency` | ✅ 完了 |
| `15/02_o1_gender_ceiling` | 🟡 §14 gender 70% 待ち (skeleton 動作) |
| `15/03_o2_mid_management` | ✅ 完了 |
| `15/04_o4_foreign_talent` | ✅ 完了 (35/01 完了で実データ動作) |
| `15/05_o6_cross_border` | ✅ 完了 (Session 2 ラウンド 1) |
| `15/06_o8_soft_power` | ✅ Tier2 拡張済 |
| `15/07_o7_historical` | ✅ 実装済 |
| `15/08_o5_education` | 🟢 Stop-if (出身校データ経路不在) |
| `15/x_cross_cutting` | ✅ 完了 |

共通実装規約: `_base.py` 継承 / lint_vocab pass / method gate / disclaimer / 単体 tests。

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

## SECTION 15: Session 3 候補タスク (Claude 可)

### A. pipeline data driver 実装 (post_processing/*)

9 新規 Mart テーブルへの計算 driver:

- `run_oaxaca_audit()` → feat_oaxaca_decomposition
- `run_resilience_audit()` → feat_network_resilience (sample top-N graph)
- `run_cohort_inequality()` → feat_cohort_inequality
- `run_mentor_audit()` → feat_mentor_pairs + event_study + matched_did
- `run_did_hte()` → feat_did_hte (DiD subgroup CATE)
- `run_did_robustness()` → feat_did_robustness (placebo + E-value + leads)
- `run_anomaly_flag()` → feat_credit_anomaly_flags (top-N 5K)
- `run_power_audit()` ✅ 既実装

各 driver: graceful (依存データ不在時 skip)、idempotent、test 必須。

### B. HTML 実生成 + smoke 検証

- `generate_briefs_v2.py` で 3 brief HTML 実出力
- 42 v2 report 全 generate (network_resilience は cap 適用で実走可)
- pixi run test (フル 2450+ tests) PR 直前

### C. quality scorecard 底上げ (mean 79.5 → 85+)

Bottom 5 報告 (network_evolution 72 / derived_params 72 / temporal_foresight 75 /
bridge_analysis 75 / individual_view 75) の SPEC に:

- `alternative_interpretations`: ≥ 2 件
- `sensitivity_grid`: ≥ 2 axis
- `identifying_assumption`: ≥ 100 char に拡張

### D. 新規 3 analysis module

- `survival_curves.py`: KM + log-rank wrap (`role_progression` と統合、CI band)
- `structural_break.py`: CUSUM + Bai-Perron (時系列構造変化検出)
- `spillover_effects.py`: peer effect IV proxy (mentor 延長)

### E. インフラ整備

- `temporal_foresight` の真 holdout validation
- Atlas migration apply (9 新規テーブル → `src/db/schema.py` 登録)
- CI workflow に quality_scorecard / spec_coverage / method_gate / lineage 統合
- DATA_DICTIONARY 自動再生成パイプ (`scripts/export_data_dictionary.py` 拡張)

---

## SECTION 16: 完了済セクション → DONE.md 参照

以下は Session 1 + Session 2 で完了。詳細は `DONE.md` の該当セクション。

- v3 Reports & Visualization (旧 §16)
- 補償公正の核 25/01-04 (旧 §17)
- 業界構造観察 26/01-03 (旧 §18)
- 方法論強化 27/01-03 (旧 §19)
- 品質監視 28/01 (旧 §20)
- レポート再構築 34/01-04 (旧 §26)
- データ流入路修復 35/01 (Session 2)
- 9 新規 analysis module + 4 新規 v2 report + 品質基盤 (Session 2 全 4 ラウンド)

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
