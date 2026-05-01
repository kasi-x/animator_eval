# Report Inventory — Audience Assignment (Task 3-1)

> Phase 3-1 deliverable. **CHECKPOINT 3**: User must approve audience assignments
> before archive / consolidation (Task 3-5) proceeds.
>
> Source of truth: `scripts/report_generators/reports/` (class-based v2 generators)
> plus `scripts/generate_reports_v2.py` (V2_REPORT_CLASSES registry).

---

## Scope

- Files audited under `scripts/report_generators/reports/`: **52 modules** (3 already under `archived/` + 49 live).
- Live classes registered in `V2_REPORT_CLASSES`: **49**.
- Non-generator infrastructure (not counted): `_base.py`, `__init__.py`.

---

## Audience key

| Code | Meaning | Target reader |
|------|---------|---------------|
| `common` | 全 audience 共通基盤 | 全員 |
| `policy` | 政策提言 Brief | 政府・業界団体 |
| `hr` | 人材評価 / 現場効率化 Brief | スタジオ HR・制作デスク |
| `biz` | 新たな試み提案 Brief | 投資家・新規企画者 |
| `technical_appendix` | 手法・診断レポート | 監査・研究者 |
| `archived` | `archived/` へ退避 | — |

---

## common — 全 audience 共通入口

| report_file | class_name | filename | title | action | consolidate_into | rationale |
|---|---|---|---|---|---|---|
| index_page.py | IndexPageReport | index.html | 全体目次 | keep | — | トップレベル landing |
| industry_overview.py | IndustryOverviewReport | industry_overview.html | 業界概観ダッシュボード | **rewrite** | — | Chart D "期待能力×実際能力 4ティア" は禁止語を含むため書換必須 |
| person_parameter_card.py | PersonParameterCardReport | person_parameter_card.html | 個人パラメータカード | keep | — | 全 audience 共通カード |
| credit_statistics.py | CreditStatisticsReport | credit_statistics.html | クレジット統計 | merge | industry_overview | Data Statement 章へ吸収 |
| bias_detection.py | BiasDetectionReport | bias_detection.html | スコア差異分析 | keep | — | 横断バイアス検査 |
| policy_brief_index.py | PolicyBriefIndexReport | policy_brief_index.html | 政策提言 Brief index | keep | — | Task 3-2 済 |
| hr_brief_index.py | HrBriefIndexReport | hr_brief_index.html | 現場 Workflow Brief index | keep | — | Task 3-2 済 |
| biz_brief_index.py | BizBriefIndexReport | biz_brief_index.html | 新たな試み Brief index | keep | — | Task 3-2 済 |

---

## policy — 政策提言 Brief

| report_file | class_name | title | action | consolidate_into | rationale |
|---|---|---|---|---|---|
| policy_attrition.py | PolicyAttritionReport | 新卒離職の因果分解 | keep | — | meta_policy_attrition |
| policy_monopsony.py | PolicyMonopsonyReport | 人材市場流動性・独占度 | keep | — | meta_policy_monopsony |
| policy_gender_bottleneck.py | PolicyGenderBottleneckReport | ジェンダー・ボトルネック | keep | — | meta_policy_gender |
| policy_generational_health.py | PolicyGenerationalHealthReport | 世代交代健全性指標 | keep | — | meta_policy_generation |
| compensation_fairness.py | CompensationFairnessReport | スコア分散公平性 | keep | — | 独立章 |
| career_friction_report.py | CareerFrictionReport | キャリア摩擦分析 | merge | policy_attrition | 離職章として吸収 |
| exit_analysis.py | ExitAnalysisReport | 退職・復職分析 | merge | policy_attrition | KM/Cox を付録へ |
| industry_analysis.py | IndustryAnalysisReport | 業界分析ダッシュボード | merge | industry_overview | common 版と重複大 |
| career_transitions.py | CareerTransitionsReport | キャリア遷移分析 | merge | policy_generational_health | 段階遷移の章に吸収 |

> 統合後 policy = **5 本**。

---

## hr — 人材評価 / 現場効率化 Brief

| report_file | class_name | title | action | consolidate_into | rationale |
|---|---|---|---|---|---|
| mgmt_studio_benchmark.py | MgmtStudioBenchmarkReport | スタジオ・ベンチマーク・カード | keep | — | meta_hr_studio_benchmark |
| mgmt_director_mentor.py | MgmtDirectorMentorReport | 監督育成力ランキング | **rewrite** | — | 「育成力ランキング」は evaluative framing → 「育成実績プロファイル」 |
| mgmt_attrition_risk.py | MgmtAttritionRiskReport | 離職リスクスコア分析 | keep | — | 認証必須 |
| mgmt_succession.py | MgmtSuccessionReport | 後継計画マトリクス | keep | — | meta_hr_succession |
| mgmt_team_chemistry.py | MgmtTeamChemistryReport | チーム化学反応分析 | keep | — | 協業適合度 |
| studio_impact.py | StudioImpactReport | スタジオインパクト | merge | mgmt_studio_benchmark | v1 版。benchmark に統合 |
| studio_timeseries.py | StudioTimeseriesReport | スタジオ時系列 | merge | mgmt_studio_benchmark | 時系列章へ |
| team_analysis.py | TeamAnalysisReport | 制作チーム構造 | merge | mgmt_team_chemistry | 統合 |
| compatibility.py | CompatibilityReport | コラボレーション相性 | merge | mgmt_team_chemistry | 相性指標統合 |
| growth_scores.py | GrowthScoresReport | 成長スコア分析 | keep | — | キャリア軌跡 |
| structural_career.py | StructuralCareerReport | 構造的キャリア分析 | merge | growth_scores | 吸収 |
| career_dynamics.py | CareerDynamicsReport | キャリアダイナミクス | merge | growth_scores | 統合 |

> 統合後 hr = **6 本** (上限)。

---

## biz — 新たな試み提案 Brief

| report_file | class_name | title | action | consolidate_into | rationale |
|---|---|---|---|---|---|
| biz_genre_whitespace.py | BizGenreWhitespaceReport | ジャンル空白地分析 | keep | — | meta_biz_whitespace |
| biz_undervalued_talent.py | BizUndervaluedTalentReport | 過小評価タレント・プール | **rewrite** | — | 「タレント」= talent の含意。「露出機会ギャップ人材プール」等へ |
| biz_trust_entry.py | BizTrustEntryReport | 信頼ネット参入経路 | keep | — | bridge_analysis 概要を吸収 |
| biz_team_template.py | BizTeamTemplateReport | チーム組成テンプレート | keep | — | meta_biz_team_template |
| biz_independent_unit.py | BizIndependentUnitReport | 独立ユニット形成可能性 | keep | — | meta_biz_independent_unit |
| genre_analysis.py | GenreAnalysisReport | ジャンル・スコア親和性 | merge | biz_genre_whitespace | whitespace の章に吸収 |

> 統合後 biz = **5 本**。

---

## technical_appendix — 監査・研究者向け

| report_file | class_name | title | action | rationale |
|---|---|---|---|---|
| akm_diagnostics.py | AKMDiagnosticsReport | AKM 固定効果診断 | keep | 推定診断 |
| dml_causal_inference.py | DMLCausalInferenceReport | DML 因果推定 | keep | メソッド説明 |
| score_layers_analysis.py | ScoreLayersAnalysisReport | スコア層別分解 | keep | 多層構造 |
| shap_explanation.py | SHAPExplanationReport | SHAP 特徴量重要度 | keep | 説明性裏付け |
| longitudinal_analysis.py | LongitudinalAnalysisReport | 縦断分析 (41 charts) | keep | 研究者向け |
| ml_clustering.py | MLClusteringReport | ML クラスタリング | keep | 詳細 |
| network_analysis.py | NetworkAnalysisReport | ネットワーク分析 | rewrite | 「能力の過小評価ではなく」を narrow label へ |
| network_graph.py | NetworkGraphReport | ネットワーク可視化 | keep | 可視化 heavy |
| network_evolution.py | NetworkEvolutionReport | ネットワーク時系列変化 | keep | 時系列 |
| cooccurrence_groups.py | CooccurrenceGroupsReport | 共同制作集団分析 | keep | 共起詳細 |
| madb_coverage.py | MADBCoverageReport | データカバレッジ分析 | keep | Data Statement 隣接 |
| derived_params.py | DerivedParamsReport | 導出パラメータ透明性 | keep | v2 transparency gate |
| cohort_animation.py | CohortAnimationReport | デビューコホート分析 | keep | 可視化 |
| knowledge_network.py | KnowledgeNetworkReport | 知識伝達ネットワーク | keep | ネットワーク |
| temporal_foresight.py | TemporalForesightReport | キャリア軌跡予測 | rewrite | predictive claim → holdout validation 未実装なら archived 候補 |
| bridge_analysis.py | BridgeAnalysisReport | ネットワークブリッジ | merge | 概要を biz_trust_entry 吸収、詳細 keep |

> 統合後 technical_appendix = **15 本** (上限)。

---

## archived

| report_file | title | rationale |
|---|---|---|
| archived/anime_value_report.py | 作品価値指標分析 | anime.score 依存 |
| archived/person_ranking.py | 人物ランキング | evaluative framing |
| archived/expected_ability_report.py | 期待値・実績乖離分析 | ability/能力 残存 |

---

## Summary — count per audience

| audience | 現状 | 統合後 | target | status |
|---|---|---|---|---|
| common | 8 | 7 | 2–3 | over (brief index 3 本が audience 必須) |
| policy | 9 | 5 | 4–6 | ok |
| hr | 12 | 6 | 4–6 | ok (上限) |
| biz | 6 | 5 | 4–6 | ok |
| technical_appendix | 16 | 15 | 10–15 | ok (上限) |
| archived | 3 | 3 | 3–5 | ok |
| **audience brief 本体のみ** | **27** | **16** | ≤20 | **ok** |

---

## Vocabulary concerns — v2 禁止語の出現箇所

### High severity (書換必須)

| report_file | location | context | action |
|---|---|---|---|
| industry_overview.py | 89 | `("優秀確定", "#F72585")` ティアラベル | 「高期待・高実績群」へ |
| industry_overview.py | 91 | `("隠れた実力", ...)` | 「低期待・高実績群」へ |
| industry_overview.py | 1243/1245/1257 | ティア分岐 | ラベル刷新に連動 |
| industry_overview.py | 1282 | chart title "D-2. 期待能力 vs 実際能力" | 「協業者 IV 平均 vs 個人 IV スコア」 |
| industry_overview.py | 1346/1349 | axis "期待能力パーセンタイル" | narrow label へ |
| industry_overview.py | 1385–1396 | Chart D 説明文 | 全面書換 |
| industry_overview.py | 1391 | title "期待能力 × 実際能力 4ティア" | 書換 |
| industry_overview.py | 11/1180 | コメント "Expected x Actual Ability" | 書換 |
| index_page.py | 23 | desc "期待能力分析" | 書換 |
| index_page.py | 47 | archived エントリ "期待能力・タレントギャップ" | index から除外 |

### Keep (disclaimer 内否定文脈, v2 準拠)

- index_page.py:279 `本スコアは「能力の測定」ではなく...`
- network_analysis.py:810 `能力の過小評価ではなく...` (narrow label がベター)
- biz_brief_index.py:56, hr_brief_index.py:54/70, policy_brief_index.py:65 — 各 disclaimer
- hr_brief_index.py:56/73-74 `do not assess individual ability`
- policy_brief_index.py:68 `does not assess individual ability`
- biz_brief_index.py:58 `do not assess individual ability`

### Low–medium severity (rename 検討)

- mgmt_director_mentor.py — 「育成力ランキング」→「育成実績プロファイル」
- biz_undervalued_talent.py — 「過小評価タレント」→「露出機会ギャップ人材プール」(file/class rename 伴う)

---

## Appendix: meta_* table routing

- `meta_policy_attrition` → policy_attrition
- `meta_policy_monopsony` → policy_monopsony
- `meta_policy_gender` → policy_gender_bottleneck
- `meta_policy_generation` → policy_generational_health
- `meta_hr_studio_benchmark` → mgmt_studio_benchmark (+ studio_impact/studio_timeseries 統合)
- `meta_hr_mentor_card` → mgmt_director_mentor
- `meta_hr_attrition_risk` → mgmt_attrition_risk
- `meta_hr_succession` → mgmt_succession
- `meta_hr_team_chemistry` → mgmt_team_chemistry (+ team_analysis/compatibility 統合)
- `meta_biz_whitespace` → biz_genre_whitespace (+ genre_analysis 統合)
- `meta_biz_undervalued` → biz_undervalued_talent
- `meta_biz_trust_entry` → biz_trust_entry (+ bridge_analysis 概要)
- `meta_biz_team_template` → biz_team_template
- `meta_biz_independent_unit` → biz_independent_unit
- `meta_common_person_parameters` → person_parameter_card

---

## Decisions pending user approval (CHECKPOINT 3)

1. **industry_overview Chart D rename** (high severity)
2. **biz_undervalued_talent の title/file rename** (file/class rename 伴う)
3. **career_dynamics / structural_career の統合先** (現案 `growth_scores`)
4. **common の本数上限** (brief index 3 本の扱い)
5. **archived 追加候補**: `temporal_foresight`, `cohort_animation`

Task 3-5 (archive/consolidation 実施) は本インベントリ承認後に着手。

---

## 拡張目的レポート (TASK_CARDS/15_extension_reports)

| ID | report_file | class_name | filename | title | audience | meta_lineage_table |
|----|---|---|---|---|---|---|
| O2 | o2_mid_management.py | O2MidManagementReport | o2_mid_management.html | 中堅枯渇パイプライン分析 | hr + policy | meta_o2_mid_management |

### O2 meta_lineage_table 詳細

- **table_name**: `meta_o2_mid_management`
- **audience**: hr (primary), policy (via brief section)
- **source_silver_tables**: credits, persons, anime
- **formula_version**: v1.0
- **ci_method**: Greenwood formula (KM 95% CI) + bootstrap percentile (studio blockage, n=1000, seed=42)
- **null_model**: industry median reference (blockage score) + multivariate log-rank test (cohort comparison)
- **sections**: KM survival curves (cohort-stratified) / studio blockage heatmap / promotion funnel
- **brief_integration**: HR brief `pipeline_blockage` section, Policy brief `mid_career_attrition` section

---

## 拡張目的レポート群 (O1–O8) brief マッピング確定表
### X1 — TASK_CARDS/15_extension_reports/x_cross_cutting 実施結果 (2026-05-02)

| O | レポートスラグ | 第一 brief | セカンダリ | 状態 |
|---|--------------|-----------|----------|------|
| O1 | gender_ceiling | policy | hr | カード実施待ち |
| O2 | mid_management | hr | policy | カード実施待ち |
| O3 | ip_dependency | biz | policy | カード実施待ち |
| O4 | foreign_talent | policy | biz | カード実施待ち |
| O5 | education_outcome | policy | technical_appendix | 新 audience 見送り (→ X2 参照) |
| O6 | cross_border | biz | policy | カード実施待ち |
| O7 | historical_restoration | technical_appendix | policy | 新 audience 見送り (→ X2 参照) |
| O8 | soft_power | biz | policy | 新 audience 見送り (→ X2 参照) |

### マッピング根拠

- **O1 (gender_ceiling)**: ジェンダー格差・ボトルネックは政策立案者の優先議題。HR では
  後継・チーム構成への応用が second use case。
- **O2 (mid_management)**: 中堅枯渇は現場ワークフロー・後継計画への直接影響が大きく HR
  が主。産業構造問題として policy にも交差。
- **O3 (ip_dependency)**: IP 人的依存リスクは投資判断・新規企画に直結するため biz が主。
  市場集中指標として policy にも交差。
- **O4 (foreign_talent)**: 海外人材の参加分布・経路分析は労働政策の直接素材。biz では
  チーム組成・ホワイトスペースとして参照。
- **O5 (education_outcome)**: キャリア追跡データは policy (教育制度評価) に帰属。
  新 audience (教育機関 brief) は当面見送り (→ X2)。
- **O6 (cross_border)**: 国際共同制作は新規企画・提携先評価として biz が主。
  産業外交・文化輸出として policy にも交差。
- **O7 (historical_restoration)**: 失われたクレジット復元はデータ信頼性・研究基盤として
  technical_appendix が主。policy では文化財保護議論に交差。
  新 audience (文化財 brief) は当面見送り (→ X2)。
- **O8 (soft_power)**: ソフトパワー指標は海外展開・配信戦略として biz が主。
  政策では文化外交議論と交差。新 audience (クールジャパン brief) は当面見送り (→ X2)。

---

## 15_extension_reports — 拡張目的レポート群 (TASK_CARDS/15_extension_reports)

| ID | report_file | class_name | filename | title | audience | status |
|----|-------------|------------|----------|-------|----------|--------|
| O3 | o3_ip_dependency.py | O3IpDependencyReport | o3_ip_dependency.html | IP 人的依存リスク分析 | biz | ✅ 実装済 (2026-05-02) |

### O3 — IP 人的依存リスク分析

- **Method**: contribution_share (role_weight × production_scale 加重比率) + counterfactual_drop + null model (random removal 1000 iter) + bootstrap 95% CI
- **Series ID strategy**: relations_json の SEQUEL/PREQUEL/PARENT/SIDE_STORY 関係を Union-Find でクラスタリング。単発作品は単独シリーズ。
- **Brief 組込み**: `scripts/report_generators/briefs/business_brief.py` section `key_person_concentration_risk`
- **Hard constraints**: H1 遵守 (外部視聴者評価を寄与計算から完全除外)、H2 遵守 (lint_vocab clean)
- **Tests**: `tests/reports/test_o3_ip_dependency.py` (38 tests: series clustering / contribution share / counterfactual / null model / smoke / lint / method gate)
