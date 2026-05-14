# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-05-15 (4 カード並列消化: 27/01, 15/01, 15/03, 25/02 完了 → DONE.md)

未完了項目のみ。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟠 High | Resolved cluster | madb 話数行 over-merge (サザエさん 1089 件等) + TMDb 同名 over-merge (Jonas 47 件等) → `TASK_CARDS/19_resolved_cluster_fix/` |
| 🟠 High | MAL/Jikan scraper | Card 05 全件 scrape (~9.4 日完走) → `TASK_CARDS/12_mal_scraper_jikan/05_rescrape.md` |
| 🟡 Medium | Report methodology | Temporal foresight Section 3.3 holdout validation 実装 (Option A、feat_career_annual / feat_person_scores データ投入後) |
| 🟡 Medium | gender enrichment | SILVER `persons.gender` null 80.9% (2026-05-05、TMDb 修正後) → O1 70% 閾値達成には MAL Card 05 + AniList orphan backfill 必要 (§15) |
| 🟡 Medium | AniList orphan backfill | credits 由来 90K orphan persons に Staff query batch fetch (gender + hometown 追加) — 新規 Card 候補 |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ (Wikidata rate limit 解放待ち) |
| 🟢 Strategy | 拡張目的レポート | 8 種拡張目的レポート → `TASK_CARDS/15_extension_reports/` (順: O3 → O1 → O2 → O4 → O6 → O8 → O7 → O5、05/06/07/08 は部分着手済) |
| 🟢 Future | 新ソース | LiveChart / Wikidata 受賞 → `TASK_CARDS/16_new_sources/` (TMDb は 14/09 で完了済) |
| 🟢 Future | bangumi cron | 日次差分 API cron → `TASK_CARDS/17_bangumi_diff_cron.md` (dump 安定運用後) |
| 🟠 High | 補償公正の核 | DiD 移籍 / opportunity null model / 可視性喪失 holdout / Oaxaca 分解 → `TASK_CARDS/25_compensation_fairness/` (4 cards) |
| 🟡 Medium | 業界構造観察 | 制作委員会 centrality / 国際協業 / studio pipeline strength → `TASK_CARDS/26_industry_structure/` (3 cards) |
| 🟠 High | 方法論強化 | missingness 開示 / career typology / IV XAI → `TASK_CARDS/27_methodology/` (3 cards、`01_missingness_disclosure` は基盤) |
| 🟡 Medium | 品質監視 | entity resolution drift 週次 snapshot → `TASK_CARDS/28_monitoring/` (1 card) |
| 🟠 High | 法的・倫理 | full public + opt-out 法務 review / publication ethics / opt-out 機構 → `TASK_CARDS/29_legal/` (3 cards) |
| 🟡 Medium | ステークホルダー | JAniCA outreach / 個別 SNS / press kit → `TASK_CARDS/30_stakeholder/` (3 cards) |
| 🟠 High | Business (startup) | startup 形態整理 / B2C 設計 / funding plan → `TASK_CARDS/31_business/` (3 cards) |
| 🟠 High | Publication 戦略 | 経済+情報DS 両建て / replication snapshot / 第 1 弾 anchor → `TASK_CARDS/32_publication/` (3 cards) |
| 🟡 Medium | 政策提言経路 | 2 ページ短縮 brief / 議連 / 省庁 outreach → `TASK_CARDS/33_policy/` (3 cards) |
| 🟠 High | レポート再構築 | labor-first audit / brief restructure / SNS short-form / B2C view → `TASK_CARDS/34_report_rebuild/` (5 cards) |

---

## プロジェクト目的経路 → カード対応

3 オプション (publication / business / 政策) すべてを意識したカード設計。

| カード | publication | business | 政策 |
|--------|:-----------:|:--------:|:----:|
| `25/01_did_studio_transfer` | ◎ | ○ | ◎ |
| `25/02_opportunity_residual_null` | ○ | ◎ | ◎ |
| `25/03_visibility_loss_holdout` | ○ | ◎ | ○ |
| `25/04_pay_equity_decomp` | ◎ | ○ | ◎ |
| `26/01_committee_influence` | ◎ |  | ◎ |
| `26/02_international_collab` | ○ | ◎ |  |
| `26/03_studio_pipeline_strength` |  | ◎ | ○ |
| `27/01_missingness_disclosure` | ◎ | ◎ | ◎ |
| `27/02_career_trajectory_typology` | ◎ | ○ |  |
| `27/03_iv_xai` | ○ | ◎ |  |

推奨着手順:
1. `27/01` (基盤、全 report の信頼性向上)
2. `25/02` (補償根拠 = プロジェクト目的の核)
3. `25/03` (HR brief 早期警告で business 経路すぐに供給)
4. `25/01` + `25/04` (publication / 政策へ向けた causal evidence、`§15` gender 充足が前提)

---

## SECTION 12: 他ソース拡張

### 12.1 AniList GraphQL query 拡張 — 残務

- [ ] staff `homeTown` v56 backfill 起動 (`scripts/maintenance/backfill_anilist_hometown.py`、SQLite 前提なので Conformed 対応に書き換え要)
- [ ] orphan persons backfill (新規): credits 由来 anilist:p 90K に Staff GraphQL batch (gender / hometown / birthday / image) fetch、BRONZE 追記 → re-integrate

完了済:
- characters / character_voice_actors SILVER 統合 (148K + 633K row)
- anime 主要列 (`source` / `season` / `seasonYear` / `relations` / `studios` / `tags`)
- staff (`yearsActive` / `primaryOccupations` / `dateOfBirth` / `gender` / `hometown` ← BRONZE 5,894/2,239 充足分は統合済)
- anime 拡張列 5 列 (external_links_json / airing_schedule_json / trailer_url / trailer_site / display_rankings_json)

### 12.3 MAL / Jikan — Card 05 のみ未実行

`TASK_CARDS/12_mal_scraper_jikan/05_rescrape.md`

`pixi run python -m src.scrapers.mal_scraper` で開始。~9.4 日完走。完了後:
- mal persons: 40,551 (現状 = staff_credits + va_credits union 上限) → 大幅増の見込み
- gender enrichment (§15) の MAL 経路がアンロック

### 12.4 新ソース → `TASK_CARDS/16_new_sources/`

- `01_livechart` 🟡 LiveChart.me (放送スケジュール精密化)
- Wikidata 受賞データ (rate limit 解放待ち、未カード化)

完了: `02_tmdb` (14/09 で Conformed 統合済、anime 79K + persons 293K)

---

## SECTION 13: bangumi.tv — 残務

- 🟡 13.6 日次差分 API cron — `TASK_CARDS/17_bangumi_diff_cron.md` (待機中、dump 安定運用後)

BRONZE / Conformed 統合は完了 (anime 3,715 / persons 21,125 / credits 232K)。3,715 は BRONZE subjects type=2 の真の上限と確認済 (2026-05-05)。

---

## SECTION 14: 拡張目的レポート群 → `TASK_CARDS/15_extension_reports/`

PROJECT.md §6.14 で宣言した 8 種の拡張目的レポートを 8 カード + 横断タスクに分割。

### カード一覧 (実施推奨順)

| ID | 拡張目的 | 内容 | 依存 | Priority | 状態 |
|----|--------|------|------|---------|------|
| `15/01_o3_ip_dependency` | O3 | IP 人的依存リスク (counterfactual + null model) | 既存 SILVER のみ | 🟠 | ✅ 完了 (2026-05-15) |
| `15/02_o1_gender_ceiling` | O1 | ジェンダー天井効果 (Cox + ego-net + DID) | gender カバレッジ (§15 完了後) | 🟠 | ブロック中 |
| `15/03_o2_mid_management` | O2 | 中堅枯渇 (KM + スタジオ別 blockage) | 既存 SILVER のみ | 🟠 | ✅ 完了 (2026-05-15) |
| `15/04_o4_foreign_talent` | O4 | 海外人材 (国籍別 FE / 役職進行) | gender / nationality カバレッジ | 🟡 | 待機 |
| `15/05_o6_cross_border` | O6 | 国際共同制作 (PageRank / community) | `04_o4` 完了 | 🟡 | 部分実装 |
| `15/06_o8_soft_power` | O8 | ソフトパワー指標 | 海外配信メタ (Card 16 / 別経路) | 🟢 | Tier2 拡張済 |
| `15/07_o7_historical` | O7 | 失われたクレジット復元 | schema 変更 (`confidence_tier`) | 🟢 | 実装済 (10dd2f3) |
| `15/08_o5_education` | O5 | 教育機関キャリア追跡 | 出身校データ取得経路 (現状不在) | 🟢 | Stop-if 報告済 |
| `15/x_cross_cutting` | — | brief マッピング / 新 audience / lint_vocab / method_note template | 個別 O カードと並行可 | 🟡 | 未着手 |

### 共通実装規約

- `scripts/report_generators/reports/base.py` 継承
- lint_vocab 通過 (`ability/skill/talent/competence/capability` + 日本語「能力」「実力」「優秀」「劣る」)
- method gate 表示 (CI / null model / holdout)
- `anime.score` SELECT 禁止 (lint で検証)
- disclaimer (JA + EN) を `build_disclaimer()` で挿入
- テスト: `tests/reports/test_<name>.py` で smoke + lint_vocab + method gate

---

## SECTION 15: gender enrichment scraper

`15_extension_reports/02_o1_gender_ceiling` の Pre-condition: SILVER `persons.gender` null 率 **80.9%** (2026-05-05、TMDb 修正後)、閾値 70% に未達。

### Conformed gender 充足現状 (2026-05-05)

| Source | persons | gender | 充足率 | 状態 |
|--------|--------:|-------:|-------:|------|
| anilist | 97,596 | 5,894 | 6.0% | ✅ 統合済 (BRONZE 7,528 中 5,894 = AniList Staff query 結果上限。orphan 90K は credits 由来 id-only) |
| bangumi | 21,125 | 12,646 | 59.9% | ✅ 統合済 |
| tmdb | 293,115 | 109,040 | 37.2% | ✅ 統合済 (2026-05-05 loader 拡張) |
| ann | 36,350 | 0 | 0.0% | ❌ ANN HTML person ページに gender label 無し = データソース側制約、scrape 不可 |
| keyframe | 35,395 | 0 | 0.0% | ❌ keyframe API/HTML に gender field 無し = データソース側制約、scrape 不可 |
| mal | 40,551 | 0 | 0.0% | 🟡 BRONZE persons table 不在 (Card 05 待ち) → scrape 後 統合可 |
| seesaawiki | 137,014 | 0 | 0.0% | ❌ seesaawiki が staff gender を出さない設計 |

**全体**: persons 732,460 / gender 140,226 / **null 率 80.9%**

### 改善経路

- **MAL Card 05 完了** → gender 大幅追加見込み (Jikan `/v4/people/{id}` で gender 取得可)
- **AniList orphan backfill** (新規 Card 起票): credits 由来の anilist:p 90K に Staff query batch fetch → 数千 gender 追加見込み
- **gender-guesser** (名前推定、補助のみ、信頼度別管理要)
- **ANN bio NLP** (description から pronoun 抽出、低精度)

依存: 完了後に `15_extension_reports/02_o1_gender_ceiling` 再起動 (閾値 70% 確認)。

---

## 実施順序

```
即時着手可:
  TASK_CARDS/15  拡張目的レポート (O3 / O2 から、SILVER のみで完結する 2 カード)

中期:
  §12.3 Card 05      MAL/Jikan 全件 scrape (~9.4 日、放置可)
  §12.1              AniList anime 拡張列 SILVER mapping + homeTown backfill
  §15                gender enrichment scraper 設計レビュー → 実装
  §14 O1             gender カバレッジ確保後に再起動

長期:
  TASK_CARDS/16  LiveChart / Wikidata 受賞
  TASK_CARDS/17  bangumi 日次差分 cron
  §3 JVMG 再マップ (Wikidata quota 解放待ち)
```

---

## SECTION 16: v3 Reports & Visualization — 完了 (2026-05-06)

§16 全 4 項目完了。`commit 45a6435` で残務消化、詳細は `DONE.md` 参照。

### 16.1 BC alias 削除 ✅

- [x] `BizUndervaluedTalentReport` alias を削除 (biz_exposure_gap.py 末尾 + __init__.py)
- [x] 外部参照ゼロ確認、V2_REPORT_CLASSES 件数 46 維持

### 16.2 P11 ChoroplethJP GeoJSON 統合 ✅

- [x] `scripts/maintenance/fetch_jp_geojson.py` で 47 features 取得 → `data/geo/japan_prefectures.geojson` (gitignore)
- [x] `src/viz/primitives/choropleth_jp.py` を真 render に実装 (geojson + featureidkey="properties.nam_ja")
- [x] `tests/unit/test_viz_choropleth_jp.py` 6 tests pass (graceful fallback / unknown 名 / missing file)

### 16.3 DB migration — ReportSpec 永続化 ✅

- [x] `mart.meta_report_spec` DDL 追加 (`src/analysis/io/mart_writer.py`)
- [x] `write_report_specs()` upsert (SHA-256 spec_hash, idempotent)
- [x] `src/pipeline_phases/post_processing.py` に upsert step 追加 (46 SPEC, non-fatal)
- [x] `tests/test_meta_report_spec.py` 11 tests pass
- 注: Atlas CLI 未 install 環境では `CREATE TABLE IF NOT EXISTS` で auto-apply

### 16.4 テストカバレッジ追加 ✅

- [x] `tests/viz/test_primitives_graceful_fallback.py` 21 tests (P1-P11 各 graceful fallback)
- [x] `tests/reports/test_spec_gate.py` 18 tests (strict mode toggle / make_default_spec / BriefArc.to_html)

---

## SECTION 17: 補償公正の核 → `TASK_CARDS/25_compensation_fairness/`

プロジェクト目的「個人の貢献を可視化して公正な報酬につなげる」の核。
analytical CI / null model / holdout の method gate を満たした **causal evidence** 提供。

| Card | 内容 | 経路 |
|------|------|------|
| `01_did_studio_transfer` | スタジオ移籍 DiD: theta_i / opportunity 因果効果 (event-study 含) | publication / 政策 |
| `02_opportunity_residual_null` | ✅ 完了 (2026-05-15) panel + analytical CI + permutation null | business / 政策 |
| `03_visibility_loss_holdout` | 翌年クレジット可視性喪失 早期警告 (LightGBM + temporal holdout + calibration) | business (HR) |
| `04_pay_equity_decomp` | Oaxaca-Blinder 分解 (gender / cohort / 所属): endowment vs structural | publication / 政策 |

依存: `04` は `§15` gender 充足 + `01` 完了が望ましい。

---

## SECTION 18: 業界構造観察 → `TASK_CARDS/26_industry_structure/`

Resolved 層完成と新ソース統合を活用した構造変化の可視化。

| Card | 内容 | 経路 |
|------|------|------|
| `01_committee_influence` | 制作委員会 bipartite centrality / Netflix 前後比較 | publication / 政策 |
| `02_international_collab` | 中韓・東南アジアスタジオとの edge 構造、role 別海外比率 | business / publication |
| `03_studio_pipeline_strength` | スタジオ別 若手育成 / 中堅滞留 / bus factor | business (投資家・HR) |

依存: `01` は制作委員会 source 統合 (新規)、`02` は `19_resolved_cluster_fix` (CJK 名寄せ精度)。

---

## SECTION 19: 方法論強化 → `TASK_CARDS/27_methodology/`

honest reporting / 説明可能性 / 軌跡分類。

| Card | 内容 | 経路 |
|------|------|------|
| `01_missingness_disclosure` | ✅ 完了 (2026-05-15) coverage 行列 + caveat block + base hook | 全経路 (基盤) |
| `02_career_trajectory_typology` | 役職遷移 sequence の Optimal Matching + cluster | publication / business |
| `03_iv_xai` | IV 5 成分 + dormancy 透明分解、個人向け API 出力 | business (個人 SaaS) |

`01` は他カード全ての信頼性基盤、最優先。

---

## SECTION 20: 品質監視 → `TASK_CARDS/28_monitoring/`

| Card | 内容 | 経路 |
|------|------|------|
| `01_entity_resolution_drift` | cross-source disagreement 週次 snapshot + CUSUM drift 検出 | 基盤 |

---

## SECTION 21: 法的・倫理的設計 → `TASK_CARDS/29_legal/`

`docs/STANCE.md §3` (個人情報・公開設計) を法的にも整合させる。

| Card | 内容 |
|------|------|
| `01_data_protection_review` | full public + opt-out 設計の法務 review (B2B / B2C 化前 必須) |
| `02_ethics_review_for_publication` | 査読誌投稿時の ethics statement 設計 |
| `03_optout_mechanism` | 削除フォーム + 7 日 SLA 実装 |

---

## SECTION 22: ステークホルダー戦略 → `TASK_CARDS/30_stakeholder/`

| Card | 内容 |
|------|------|
| `01_janica_outreach` | JAniCA への接触 / データ共有 / 政策共同提言 |
| `02_individual_outreach_sns` | 個別アニメーター向け SNS 発信運営 |
| `03_media_press_kit` | 取材対応 press kit (reactive) |

---

## SECTION 23: Business (startup) → `TASK_CARDS/31_business/`

ユーザーは startup として運営中。labor-first スタンス維持しつつ business 軸を整理。

| Card | 内容 |
|------|------|
| `01_startup_form` | 法人形態 / co-founder / IP 所有関係の整理 (機密 doc) |
| `02_b2c_design` | 個別アニメーター向け B2C SaaS 設計 |
| `03_funding_plan` | 学振 / 助成 / VC 優先順位設計 |

---

## SECTION 24: Publication 戦略 → `TASK_CARDS/32_publication/`

| Card | 内容 |
|------|------|
| `00_paper_strategy` | 経済 + 情報DS 両建て (a/b/c/d) 確定 |
| `01_replication_snapshot_exception` | 高位 venue 投稿時のみ snapshot + Zenodo DOI |
| `02_first_paper_anchor` | 最初の論文 anchor 選定 (DiD / opportunity / typology) |

---

## SECTION 25: 政策提言経路 → `TASK_CARDS/33_policy/`

| Card | 内容 |
|------|------|
| `01_short_form_brief` | 政策担当向け 2 ページ短縮 brief |
| `02_giin_renkei_protocol` | 議連 / 政策研究会経由のアクセス設計 |
| `03_ministry_outreach` | 経産省・文化庁・厚労省・内閣府 接触 |

---

## SECTION 26: レポート再構築 → `TASK_CARDS/34_report_rebuild/`

`docs/STANCE.md` (2026-05-06 確定) に基づく既存 37 v3 reports + 3 brief の labor-first 再構築。

実装済 (2026-05-06):
- ✅ `docs/STANCE.md` 起草
- ✅ `helpers.py`: `build_stance_block()` / `build_disclaimer()` 追加
- ✅ `html_templates.py`: `STANCE_BLOCK` 追加、`wrap_html_v2` で全 v2 report に自動注入、`DISCLAIMER` 文言改訂
- ✅ `export.py`: brief HTML renderer の disclaimer / stance block 追加
- ✅ `forbidden_vocab.yaml`: `ranking_framing` / `hiring_framing` カテゴリ追加

| Card | 内容 |
|------|------|
| `01_audit_existing_reports` | 全 37 v3 report を新カテゴリ含む lint で audit、違反洗い出し |
| `02_brief_restructure` | 3 brief 構成改訂: HR brief → Workers Brief、Business brief → 労働者寄り業界観察 |
| `03_sns_outreach_layer` | X / note 用 short-form 自動生成 |
| `04_b2c_individual_view` | 個別 person view (B2C) の labor-first 設計 |
| `05_policy_brief_short` | 33_policy/01 と統合 (重複カード) |

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
