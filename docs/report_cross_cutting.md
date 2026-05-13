# Report Cross-Cutting Architecture (Extension Reports O1–O8)

**Purpose**: Coordinate brief mapping, audience brief adoption, method gates, vocabulary enforcement, and method note templating for the 8-report expansion series (O1–O8) spanning policy / HR / business briefs and technical appendix.

**Scope**: TASK_CARDS/15_extension_reports/x_cross_cutting (§X1–§X4)

**Status**: Normative document (live configuration, not historical record)

**Last updated**: 2026-05-13

---

## §1. Report → Brief Mapping (X1)

All extension reports (O1–O8) plus existing 37 v3 reports map to one primary audience brief with optional secondary integration. This table is source of truth for brief inclusion logic.

### Extension Reports (O1–O8) Audience Assignment

| O | report_slug | brief_primary | brief_secondary | status |
|---|---|---|---|---|
| O1 | gender_ceiling | policy | hr | ✅ 実装済 (2026-05-04) |
| O2 | mid_management | hr | policy | ✅ 実装済 (2026-05-02) |
| O3 | ip_dependency | biz | policy | ✅ 実装済 (2026-05-02) |
| O4 | foreign_talent | policy | biz | ✅ 実装済 (2026-05-02) |
| O5 | education_outcome | policy | technical_appendix | 実装予定 |
| O6 | cross_border | biz | policy | 実装予定 |
| O7 | historical_restoration | technical_appendix | policy | 実装予定 |
| O8 | soft_power | biz | policy | ✅ Tier1 実装済 (2026-05-02) |

### Mapping Rationale

**O1 (gender_ceiling)**: ジェンダー格差・ボトルネックは政策立案者の優先議題 (labor force participation, opportunity disparity)。HR では後継・チーム構成への応用が二次的。

**O2 (mid_management)**: 中堅枯渇は現場ワークフロー・後継計画への直接影響が大きく HR 主。産業構造問題として policy にも交差。

**O3 (ip_dependency)**: IP 人的依存リスクは投資判断・新規企画に直結するため biz 主。市場集中・提携リスク指標として policy にも交差。

**O4 (foreign_talent)**: 海外人材の参加分布・経路分析は労働政策の直接素材。チーム組成・ホワイトスペース採用として biz にも交差。

**O5 (education_outcome)**: キャリア追跡データは政策に帰属。新 audience (教育機関 brief) は当面見送り。

**O6 (cross_border)**: 国際共同制作は新規企画・提携先選定 (biz 判断) に直結。産業外交として policy にも交差。

**O7 (historical_restoration)**: 失われたクレジット復元は DB 信頼性・研究基盤に帰属。文化財保護議論として policy に交差。新 audience 見送り。

**O8 (soft_power)**: ソフトパワー指標は海外展開・配信戦略に直結。文化外交として policy に交差。クールジャパン brief 新設は見送り。

---

## §2. New Audience Brief Adoption Status (X2)

### Adoption Decision: 12-Month Deferral (2026-05-02)

**結論**: 3 候補 (教育機関 / 文化財 / クールジャパン) すべて、当面は新 audience brief として設置せず、既存 3 brief (policy / hr / biz) + technical_appendix への section 追加で運用する。新 audience brief 新設は **12 ヶ月後 review** (2027-05 目安) まで延期。

**判断根拠**:

1. **構造的差異の未確認**: 各読者層は既存 policy brief 読者層と重複が大きい。別 brief が必要な固有の表現様式・語彙が確定していない。

2. **最低レポート数未達**: 新 audience brief は対象向けの最低 3 本レポート群が確定してから。現在各候補は 1 本のみ。

3. **維持コストとリスク**: audience が増えると lint_vocabulary の個別管理、disclaimer 言語バリエーション増加。既存 3 brief 品質を先に安定させることを優先。

4. **代替案の十分性**: policy brief の section / technical_appendix へ収容可能。

### 12 ヶ月後 Review Conditions

以下のいずれかが満たされた場合、audience brief 新設を再検討:

- 対象 audience 向けレポートが **3 本以上** 確定
- 既存 brief に収まらない読者固有の **表現規範** が 2 件以上特定
- 実際の外部ステークホルダーから audience-specific brief の需要確認

---

## §3. Method Gate Cross-Cutting Perspective (X3)

### Confidence Interval (CI) 要件

**硬い要件** (docs/REPORT_PHILOSOPHY.md §3.1):

個人レベルの推定値は**必ず信頼区間を伴う**。区間なしの公表を禁ずる。

| O-report | 推定対象 | CI 手法 |
|---------|--------|-------|
| O1 | hazard ratio (gender) | 解析的 SE (Cox) |
| O2 | KM survival curve | Greenwood formula + bootstrap |
| O3 | counterfactual contribution | bootstrap percentile |
| O4 | person FE distribution | bootstrap percentile |
| O7 | restoration coverage % | Wilson score interval |
| O8 | soft_power_index | analytical (sum variance) |

### Null Model 一覧

**硬い要件** (docs/REPORT_PHILOSOPHY.md §3.2):

集団レベルの主張は**必ずランダム化ベースラインとの対比**で提示。

| O-report | null model type | n_iterations | seed |
|---------|-----------------|--------------|------|
| O1 | log-rank (Cox partial residuals) | — | — |
| O2 | industry median blockage reference | — | — |
| O3 | random person drop | 1000 | 42 |
| O4 | label permutation (nationality) | 1000 | 42 |
| O6 | Louvain permutation | 199 | 42 |
| O7 | binomial random recovery | 1000 | 42 |
| O8 | platform assignment randomization | 1000 | 42 |

### Holdout Validation (予測的主張)

**硬い要件** (docs/REPORT_PHILOSOPHY.md §3.3):

「先見」「予測」含む主張は**必ず時間分割ホールドアウト検証を伴う**。

| O-report | 予測対象 | train/test split |
|---------|--------|------------------|
| O5 | キャリア継続性 | T < 2019 / T ≥ 2019 |
| O7 | 未復元クレジット発見 | chronological fold |

---

## §4. Cross-Cutting Metric Usage Table (X4)

| Metric | O1 | O2 | O3 | O4 | O5 | O6 | O7 | O8 |
|--------|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| theta_i (person FE) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| opportunity_residual | — | ✓ | ✓ | — | — | — | — | — |
| dormancy_multiplier | — | ✓ | — | — | ✓ | — | — | — |
| studio_fe | ✓ | ✓ | — | ✓ | — | ✓ | — | — |
| birank | — | — | — | — | — | ✓ | — | ✓ |
| collaboration_density | ✓ | ✓ | — | — | — | — | — | — |
| visibility_loss_rate | ✓ | ✓ | — | ✓ | ✓ | — | — | — |
| role_rank_progression | ✓ | ✓ | — | ✓ | ✓ | — | — | — |
| platform_distribution | — | — | — | — | — | ✓ | — | ✓ |
| series_dependency_share | — | — | ✓ | — | — | — | — | — |
| restoration_coverage | — | — | — | — | — | — | ✓ | — |

**使用ルール**: 各 metric の定義・計算方法は `docs/method_notes/` に専用 method note として記載。O1–O8 が参照する場合、当該 method note へのリンクを報告書内に埋め込む。

---

## §5. Lint Vocabulary 拡張案 (X3)

### 現状 (2026-05-13 実装済)

#### CONTEXTUAL_BIGRAMS (文脈条件付 2-gram)

すでに `scripts/report_generators/lint_vocab.py` に実装済。4 patterns に対して文脈条件付き検出:

1. **Pattern A** (lost + personnel): 喪失系 term と 人材系 term が 60 文字以内に並ぶ → 人員喪失を暗示 → suggest "next-year credit absence rate"
2. **Pattern B** (absent + proficiency): 不在系 term と proficiency系 term が近接 → absent proficiency を暗示 → suggest "credit absence period"
3. **Pattern C** (hidden + talent): 埋もれ系 term と talent系 term が近接 → hidden merit を暗示 → suggest "exposure opportunity gap pool"
4. **Pattern D** (dormant + capability): 眠る系 term と capability系 term が近接 → dormant merit を暗示 → suggest "low-credit-frequency, high-network-position group"

**設計原理**: 各 term 単独では合法。歴史的背景の「喪失クレジット」は O7 context で使用可。「人材」は neutral 構造用語。ただし両者 60 文字以内に並ぶと ability framing を暗示するため flag。

**実装詳細**: `scripts/report_generators/lint_vocab.py` lines 97–142 (CONTEXTUAL_BIGRAMS list 定義)

**違反検出**:
```bash
pixi run python scripts/report_generators/lint_vocab.py scripts/report_generators/reports/
```

### 12 ヶ月後拡張候補 (2027-05 レビュー)

| 候補パターン | term_a | term_b | 判断 |
|-----------|--------|--------|------|
| 日本 exclusivity | 「独自」 | 「技術」 | defer (O6 review) |
| 海外 subcontracting stigma | 「下請け」 | 「海外」 | defer (O6 review) |
| 教育制度ランキング | 「優秀」 | 「専門学校」 | defer (O5 review) |
| 文化遺産価値化 | 「希少」 | 「クレジット」 | defer (O7 review) |

---

## §6. Method Note Template Enumeration (X4)

### 実装済 Patterns

`docs/method_notes/_template.md` (2026-05-13 初版) が以下 8 手法をカバー:

| 手法 | 関連 O-report | 状態 |
|------|-------------|------|
| Cox PH 回帰 | O1 | ✅ 実装済 |
| Mann-Whitney U | O1, O4, O8 | 実装準備中 |
| Kaplan-Meier | O1, O2 | 実装準備中 |
| Counterfactual + Bootstrap | O3 | ✅ 実装済 |
| Louvain Community Detection | O6 | 実装準備中 |
| Propensity Score Matching / IPW | O5 | 実装準備中 |
| Difference-in-Differences (DID) | O7 | 実装準備中 |
| Weighted PageRank | O6 | 実装準備中 |

### Method Note Instantiation Rules

各 O-report カード実施時:

1. **§3 Method Gate を先に読む**: CI method, null model, holdout validation 要件確認
2. **_template.md 構造を複写**: 8 section 準守
3. **narrow labeling を明示**: 「Do NOT interpret as」リスト作成
4. **Tests を作成**: 別途テストファイル作成
5. **Brief 統合時にリンク**: HTML brief section に参照追加

---

## §7. Implementation Roadmap (X1–X4)

### Step 1: Brief Mapping 確定 ✅ (2026-05-13 完了)

- [x] O1–O8 brief assignment table (§1) 確定
- [x] `docs/REPORT_INVENTORY.md` §15_extension_reports 表記録
- [x] 既存 37 v3 report との整合性確認

### Step 2: New Audience Brief 判断 ✅ (2026-05-13 完了)

- [x] 3 候補検討完了
- [x] 12 ヶ月後 deferral 判断確定 (§2)
- [x] `docs/REPORT_PHILOSOPHY.md` §11 記録完了

### Step 3: Lint Vocab 拡張 ✅ (2026-05-13 完了)

- [x] CONTEXTUAL_BIGRAMS 4 patterns 実装済
- [x] O1–O4 報告書 lint pass 確認
- [x] 12 ヶ月後候補リストアップ (§5)

### Step 4: Method Note Template ✅ (2026-05-13 完了)

- [x] 8-section template 初版作成 (docs/method_notes/_template.md)
- [x] 例示インスタンス (Cox PH O1) 作成
- [x] 8 method ↔ report mapping 確定

### Step 5: Compliance Verification (PR merge 条件)

**Before merging O1–O8 PRs:**

```bash
# 1. Lint check
pixi run python scripts/report_generators/lint_vocab.py \
    scripts/report_generators/reports/o*.py
# Expected: return 0

# 2. Method note exists
test -f docs/method_notes/[method_name]_[o_id].md

# 3. Brief section is linked
grep -A 5 "meta_lineage_table: meta_o[1-8]" scripts/report_generators/briefs/*.py

# 4. Test pass (scoped)
pixi run test-scoped tests/reports/test_o[1-8]_*.py
```

---

## Appendix A: Brief Section Naming Convention

各 brief ファイルで extension report (O1–O8) section を以下のように命名:

- Policy Brief: section_gender_progression_disparity (O1), section_foreign_talent_policy_dimensions (O4), section_labor_pipeline_education (O5), section_historical_credit_preservation (O7)
- HR Brief: section_pipeline_blockage_analysis (O2), section_gender_role_progression (O1)
- Business Brief: section_key_person_concentration_risk (O3), section_international_collaboration_structure (O6), section_soft_power_export_readiness (O8)
- Technical Appendix: section_longitudinal_career_outcomes (O5), section_historical_coverage_audit (O7)

各 section 内: Findings 1–2 段落 + visualization + Method note link + Interpretation (optional)

---

## References

- **REPORT_PHILOSOPHY.md**: v2 認識論的立場、禁止語彙、method gate 要件
- **STANCE.md**: Labor-first framing、ステークホルダー関係、学術スタンス
- **REPORT_INVENTORY.md**: 全 52 報告書の brief 帰属、consolidation plan
- **method_notes/_template.md**: 標準 method note 構造
- **TASK_CARDS/15_extension_reports/x_cross_cutting.md**: 原本カード

---

**Document Status**: Normative (v1.0)
**Last Updated**: 2026-05-13
**Maintained by**: Animetor Eval Documentation
**Related**: CLAUDE.md §5 (Report System), TASK_CARDS/15_extension_reports/
