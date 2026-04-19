# Report Inventory — Audience Assignment

> Phase 3-1 draft. **CHECKPOINT 3**: User must approve audience assignments before
> archive/consolidation (Task 3-5) proceeds.
>
> Columns: report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason

---

## common (全 audience 共通)

| report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason |
|-------------|-----------|---------|------------------|-----------------|----------------|
| index_page.py | IndexPageReport | common | — | — | 全体目次、全 audience の入口 |
| industry_overview.py | IndustryOverviewReport | common | meta_policy_*, meta_hr_*, meta_biz_* | — | Executive summary (2-4p)。各 brief の要約を集約 |
| person_parameter_card.py | PersonParameterCardReport | common | meta_common_person_parameters | — | 全 audience 共通の個人基本カード |
| credit_statistics.py | CreditStatisticsReport | common | — | — | データ全体の概要。Data Statement に隣接 |
| bias_detection.py | BiasDetectionReport | common | — | — | 横断的バイアス検査。全 audience に関係 |

## policy (政策提言 Brief)

| report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason |
|-------------|-----------|---------|------------------|-----------------|----------------|
| policy_attrition.py | PolicyAttritionReport | policy | meta_policy_attrition | — | DML 離職分析の主レポート |
| policy_monopsony.py | PolicyMonopsonyReport | policy | meta_policy_monopsony | — | 労働市場集中度分析 |
| policy_gender_bottleneck.py | PolicyGenderBottleneckReport | policy | meta_policy_gender | — | ジェンダー生存分析 |
| policy_generational_health.py | PolicyGenerationalHealthReport | policy | meta_policy_generation | — | 世代別キャリア生存曲線 |
| career_friction_report.py | CareerFrictionReport | policy | — | policy_attrition | 離職まわり指標。policy_attrition に吸収予定 |
| exit_analysis.py | ExitAnalysisReport | policy | meta_policy_attrition | policy_attrition | policy_attrition の視覚化補完。統合検討 |
| compensation_fairness.py | CompensationFairnessReport | policy | — | — | 報酬格差分析。政策立案者向け |
| industry_analysis.py | IndustryAnalysisReport | policy | — | — | 業界全体トレンド。政策背景データ |
| career_transitions.py | CareerTransitionsReport | policy | — | — | キャリア段階遷移。policy 背景データ |

## hr (人材評価 / 現場効率化 Brief)

| report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason |
|-------------|-----------|---------|------------------|-----------------|----------------|
| mgmt_studio_benchmark.py | MgmtStudioBenchmarkReport | hr | meta_hr_studio_benchmark | — | スタジオ定着率・VA・H_s の主レポート |
| mgmt_director_mentor.py | MgmtDirectorMentorReport | hr | meta_hr_mentor_card | — | 監督育成貢献プロファイル |
| mgmt_attrition_risk.py | MgmtAttritionRiskReport | hr | meta_hr_attrition_risk | — | 離職リスクプロファイル (認証必須) |
| mgmt_succession.py | MgmtSuccessionReport | hr | meta_hr_succession | — | 後継者候補 (aggregate 公開) |
| mgmt_team_chemistry.py | MgmtTeamChemistryReport | hr | — | — | チーム相性・協業適合度 |
| studio_impact.py | StudioImpactReport | hr | meta_hr_studio_benchmark | mgmt_studio_benchmark | スタジオ影響力。mgmt_studio_benchmark に統合 |
| studio_timeseries.py | StudioTimeseriesReport | hr | meta_hr_studio_benchmark | mgmt_studio_benchmark | スタジオ時系列。mgmt_studio_benchmark に統合 |
| team_analysis.py | TeamAnalysisReport | hr | — | mgmt_team_chemistry | チーム分析。team_chemistry に統合 |
| compatibility.py | CompatibilityReport | hr | — | mgmt_team_chemistry | 相性指標。team_chemistry に統合 |
| growth_scores.py | GrowthScoresReport | hr | — | — | キャリア成長軌跡。hr または common |
| structural_career.py | StructuralCareerReport | hr | — | — | 構造的キャリア指標。hr 向け解釈 |
| career_dynamics.py | CareerDynamicsReport | hr | — | mgmt_studio_benchmark | キャリアダイナミクス。hr に統合またはアーカイブ |

## biz (新たな試み提案 Brief)

| report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason |
|-------------|-----------|---------|------------------|-----------------|----------------|
| biz_genre_whitespace.py | BizGenreWhitespaceReport | biz | meta_biz_whitespace | — | ジャンル空白地図の主レポート |
| biz_undervalued_talent.py | BizUndervaluedTalentReport | biz | meta_biz_undervalued | — | 過小露出人材発掘 |
| biz_trust_entry.py | BizTrustEntryReport | biz | meta_biz_trust_entry | — | 信頼ネットワーク参入 |
| biz_team_template.py | BizTeamTemplateReport | biz | meta_biz_team_template | — | チームテンプレート提案 |
| biz_independent_unit.py | BizIndependentUnitReport | biz | meta_biz_independent_unit | — | 独立制作ユニット分析 |
| genre_analysis.py | GenreAnalysisReport | biz | meta_biz_whitespace | biz_genre_whitespace | ジャンル分析。whitespace に吸収または並存 |

## technical_appendix (監査・研究者向け)

| report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason |
|-------------|-----------|---------|------------------|-----------------|----------------|
| akm_diagnostics.py | AKMDiagnosticsReport | technical_appendix | — | — | AKM 推定の診断。研究者向け |
| dml_causal_inference.py | DMLCausalInferenceReport | technical_appendix | — | — | DML メソッド説明。本体は policy に吸収 |
| score_layers_analysis.py | ScoreLayersAnalysisReport | technical_appendix | — | — | スコア多層構造の解説 |
| shap_explanation.py | SHAPExplanationReport | technical_appendix | — | — | SHAP 特徴量重要度 |
| longitudinal_analysis.py | LongitudinalAnalysisReport | technical_appendix | — | — | 41 charts 巨大レポート。研究者向け |
| ml_clustering.py | MLClusteringReport | technical_appendix | — | — | ML クラスタリング詳細 |
| network_analysis.py | NetworkAnalysisReport | technical_appendix | — | — | ネットワーク構造詳細 |
| network_graph.py | NetworkGraphReport | technical_appendix | — | — | ネットワーク可視化 |
| network_evolution.py | NetworkEvolutionReport | technical_appendix | — | — | ネットワーク時間変化 |
| cooccurrence_groups.py | CooccurrenceGroupsReport | technical_appendix | — | — | 共起グループ詳細 |
| madb_coverage.py | MADBCoverageReport | technical_appendix | — | — | MADB カバレッジ (Data Statement 補完) |
| derived_params.py | DerivedParamsReport | technical_appendix | — | — | 導出パラメータ定義と検証 |
| cohort_animation.py | CohortAnimationReport | technical_appendix | — | — | コホートアニメーション。可視化 heavy |
| knowledge_network.py | KnowledgeNetworkReport | technical_appendix | — | — | 知識ネットワーク構造 |
| temporal_foresight.py | TemporalForesightReport | technical_appendix | — | — | 時系列予測。研究者向け |
| bridge_analysis.py | BridgeAnalysisReport | technical_appendix | — | — | ブリッジ人材詳細。概要は biz_trust_entry に吸収 |

## archived (アーカイブ)

| report_file | class_name | audience | reads_meta_tables | consolidate_into | decision_reason |
|-------------|-----------|---------|------------------|-----------------|----------------|
| anime_value_report.py | AnimeValueReport | archived | — | — | anime.score 依存の名残。法的リスク (score = ability の含意) |
| person_ranking.py | PersonRankingReport | archived | — | — | 順位付けは evaluative framing リスク大。アーカイブ |
| expected_ability_report.py | ExpectedAbilityReport | archived | — | — | "ability" framing が v2 哲学に違反 |

---

## Summary

| audience | 本数 (統合前) | 目標 (統合後) |
|---------|------------|------------|
| common | 5 | 4-5 |
| policy | 9 | 4-6 (exit + career_friction → policy_attrition 等に統合) |
| hr | 12 | 5-7 (studio_impact/timeseries → benchmark、team/compat → chemistry に統合) |
| biz | 6 | 5-6 |
| technical_appendix | 16 | 12-15 |
| archived | 3 | 3 |
| **合計** | **51** | **34-43** |

---

## 統合待ちアクション (Task 3-5 で実施、CHECKPOINT 3 承認後)

1. `studio_impact.py` + `studio_timeseries.py` → `mgmt_studio_benchmark.py` に統合
2. `team_analysis.py` + `compatibility.py` → `mgmt_team_chemistry.py` に統合
3. `exit_analysis.py` + `career_friction_report.py` → `policy_attrition.py` に吸収または独立維持
4. `genre_analysis.py` → `biz_genre_whitespace.py` に吸収または並存
5. `career_dynamics.py` → hr に統合または archived に移動
6. `anime_value_report.py`, `person_ranking.py`, `expected_ability_report.py` → `archived/` に git mv
