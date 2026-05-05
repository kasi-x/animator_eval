# Report Design v3 — Method-Driven Reconstruction

本文書は `REPORT_PHILOSOPHY.md` (認識論的立場 / Findings·Interpretation 分離 /
方法論最低要件) と `REPORT_INVENTORY.md` (audience 分配と本数上限) を実装層に
落とす設計文書である。v3 の中核は次の二点に集約される。

1. **方法宣言の一元化**: 各 report を 5 タプル `(claim, identifying assumption,
   null model, method gate, sensitivity grid, interpretation guard)` で宣言する。
   レポート本体は宣言を参照し、再発明しない。
2. **Brief = narrative arc**: brief は section 列挙ではなく、現象提示 → null
   との対比 → 解釈の限界 → 代替視点の 4 段で構成する。section は arc を支える
   素材として位置付ける。

---

## 1. なぜ method-driven か

v2 の現状では、各 report が独立に method note を記述し、CI 算出方法、null
model、感度分析の扱い、用語選択が report 間で揺れている。これは
`REPORT_PHILOSOPHY.md` §3 (方法論最低要件) を強制する gate を持たないことの
帰結である。

v3 では report ごとに **`ReportSpec` データクラス** を宣言する。`ReportSpec`
は次を保持する。

| field | 内容 | 強制 |
|-------|------|------|
| `claim` | 1 文の主張 (狭い名前で) | 必須 |
| `identifying_assumption` | 主張が成立する前提 (例: クレジット可視性 ≈ 雇用実態) | 必須 |
| `null_model` | 帰無モデル定義 (configuration / degree-preserving / cohort-matched) | 集団主張で必須 |
| `method_gate` | CI / 縮約 / holdout / 感度の最低要件 | 必須 |
| `sensitivity_grid` | window / threshold / 集計単位の代替選択 | 必須 (1 軸以上) |
| `interpretation_guard` | 禁止 framing と必須代替解釈の数 | 必須 |
| `data_lineage` | source 層 → mart 層の経路と meta_lineage table | 必須 |

`ReportSpec` を満たさないレポートは CI でブロックする (`ci_check_report_spec.py`
を新設、既存の lineage / lint_vocab に並ぶ)。

---

## 2. claim の狭い名前

`REPORT_PHILOSOPHY.md` §2.1 で「最も狭い説明」を要求しているが、現状の
report title は過去の慣性で広い名前を残している。v3 で次のように改める。

| 現行 (広い) | v3 (狭い) | 根拠 |
|-------------|----------|------|
| 新卒離職の因果分解 | 翌年クレジット可視性喪失率の cohort 比較 | 「離職」は雇用実態を含意 |
| 育成力ランキング (mgmt_director_mentor) | 監督下デビュー人数と 5 年後可視性プロファイル | 「育成力」は能力評価 |
| 過小評価タレント (biz_undervalued_talent) | theta_i 高 / 露出機会低 ペア候補リスト | 評価語回避 |
| 業界規模 | データベース上のクレジット記録密度 | 「業界規模」は外延が曖昧 |
| 信頼ネット参入経路 | 高 PageRank ノードへの shortest-path 長分布 | 比喩を指標名に出さない |

claim 書換は `REPORT_INVENTORY.md` の rewrite 項目を v3 に取り込む形で実施する。

---

## 3. null model の標準カタログ

v3 では集団主張に対し、次の null model のいずれかを **必ず** 適用する。
新規定義はカタログ追加 PR を経由する。

| ID | model | 用途 |
|----|-------|------|
| `N1` | configuration model (Newman 2003) | 次数保存ランダム化、ネットワーク主張 |
| `N2` | degree-preserving rewiring (double edge swap, 1000 iter) | 局所構造保存比較 |
| `N3` | cohort-matched permutation | コホート効果除去 |
| `N4` | role-matched bootstrap | 役職効果除去 |
| `N5` | era-window matched resample | 時代窓固有効果除去 |
| `N6` | uniform random (情報量ゼロベースライン) | HHI / 集中指標の床 |
| `N7` | naive activity baseline (出演数のみ) | 予測指標の素朴ベンチ |

各 report は `null_model` に `[N3, N5]` 等の組合せを宣言する。

---

## 4. method gate の最低要件 (実装可能形式)

`MethodGate` クラスを `scripts/report_generators/_spec.py` に新設する。
既存の `report_brief.MethodGate` を継承し、次の field を追加する。

```python
@dataclass
class MethodGate:
    name: str
    estimator: str                      # e.g. "Kaplan-Meier"
    ci: CIMethod                        # Greenwood / bootstrap / delta
    n_resamples: int | None             # bootstrap 時必須
    rng_seed: int                       # 必須 (再現性)
    holdout: HoldoutSpec | None         # 予測主張で必須
    null: list[str]                     # null model ID の組合せ
    shrinkage: ShrinkageSpec | None     # 個人ランキングで必須
    sensitivity_grid: list[SensitivityAxis]
    limitations: list[str]              # 既知の限界 (3 件以上)
```

レポート生成時に `MethodGate.validate()` が呼ばれ、欠落があれば
`MethodGateViolation` を raise する。CI で `pixi run check-method-gates` を
追加し、全 49 report の gate 完備を保証する。

---

## 5. Brief narrative arc

brief は次の 4 段構成を取る。各段の役割を明示し、節タイトルにも反映する。

### 段 1: 現象提示 (Findings only)

データ上の観察を狭い名前で提示する。null model との比較や因果含意は **書かない**。
読者がそこに意味を見出すかは、段 2 以降で展開する。

### 段 2: null model との対比

各主張について、宣言された null との差を提示する。「観測値 X、null 95% 区間
[a, b]、外れているか / 外れていないか」を chart で示す。Null との差が小さい
場合、その事実をそのまま記す (ファイルドロワー回避)。

### 段 3: 解釈の限界 (Method note 群の集約)

3 つを明示する: `identifying_assumption` の妥当性、`sensitivity_grid` での
結論揺れ、`shrinkage` 後の順序変化。読者が自分で limitation を判断できる
情報を一箇所に集める。

### 段 4: 代替視点 (Interpretation)

著者の解釈を主語付きで提示し、最低 1 つの代替解釈を併走させる。代替を
採らなかった理由を明記する。推奨を行う場合は、別の価値観からの異なる
推奨を 1 つ並走させる。

### Brief 構造の宣言

```python
@dataclass
class BriefArc:
    audience: Literal["policy", "hr", "biz"]
    presenting_phenomena: list[str]   # section_id 列
    null_contrast: list[NullContrast] # 各主張と null の比較
    limitation_block: LimitationBlock
    interpretation: Interpretation    # 主語明示, alternatives ≥ 1
```

`BriefArc.render()` が 4 段の HTML を生成する。section の差し込みは
`presenting_phenomena` で参照される report id に従う。

---

## 6. Cross-report 一貫性

### 6.1 共通 baseline figure

全 audience で参照される baseline を `result/json/baseline_figures.json` に
集約する。各 report は自分の主張を baseline figure と並べて提示する。

| baseline | 内容 |
|---------|------|
| `industry_baseline` | 全期間 / 全役職での産業平均 (HHI / median tenure / median theta_i) |
| `cohort_baseline` | 5 年デビューコホート別の標準軌跡 |
| `role_baseline` | 24 役職別の協業密度中央値 |

### 6.2 共通指標の scale 統一

`theta_i` (person FE) は report 間で z-score 化して提示する。生 logit 値は
technical appendix のみで提示する。**brief では z-score とパーセンタイルの併記**
を標準とする。

### 6.3 用語固定

`docs/GLOSSARY_v3.md` (新設) に v3 用語を集約し、`SectionBuilder` の glossary
inject から参照する。「離職」「能力」「実績」は禁止語、代替語は単一化する。

---

## 7. report 5 タプル例 (3 件)

### 例 A: `policy_attrition`

```yaml
claim: "デビューコホート別に翌年クレジット可視性喪失率が 5 年窓で単調減少する"
identifying_assumption: "クレジット可視性喪失 = 雇用離脱 を仮定しない。
  クレジット可視性のみを観察対象とする。"
null_model: ["N3 (cohort-matched permutation)", "N5 (era-window resample)"]
method_gate:
  estimator: "Kaplan-Meier (Greenwood CI 95%) + Cox PH (Breslow)"
  ci: "Greenwood (KM), delta method (Cox HR)"
  n_resamples: null
  rng_seed: 42
  holdout: "leave-last-3-years-out (2022-2024)"
  shrinkage: null
  limitations:
    - "右打切り (観測末年)"
    - "海外下請け / 無名義参加 / 産休 を可視性喪失が混在で吸収"
    - "クレジット粒度の時代差 (1980s vs 2010s) が hazard 推定に bias"
sensitivity_grid:
  - axis: "exit definition"
    values: ["1年空白", "3年空白", "5年空白"]
  - axis: "cohort cut"
    values: ["5年", "10年"]
interpretation_guard:
  forbidden_framing: ["離職率の悪化", "若手定着の課題"]
  required_alternatives: 2
data_lineage:
  source: ["credits", "persons", "anime"]
  meta_table: "meta_policy_attrition"
```

### 例 B: `mgmt_director_mentor` (rename 後)

```yaml
claim: "監督ノード A の下でデビューした人物の 5 年後 theta_i 分布が
  全監督下デビュー者分布と異なる"
identifying_assumption: "監督下デビュー = 監督が機会を割り当てた と
  仮定しない。共起は機会割当の必要条件ではあるが十分条件ではない。"
null_model: ["N4 (role-matched bootstrap)", "N5 (era-window resample)"]
method_gate:
  estimator: "permutation test (10000 iter, two-sided)"
  ci: "bootstrap percentile (n=1000)"
  rng_seed: 42
  shrinkage: "James-Stein (監督下デビュー人数 < 30 で適用)"
  limitations:
    - "「監督下デビュー」の定義: 初クレジット作品の監督との共起"
    - "監督個人の選好と機会の混在"
    - "縮約後でも n < 10 の監督は提示しない"
sensitivity_grid:
  - axis: "デビュー定義"
    values: ["初クレジット", "初メイン役職"]
  - axis: "5年後 vs 10年後"
    values: [5, 10]
interpretation_guard:
  forbidden_framing: ["育成力", "弟子の質"]
  required_alternatives: 2
```

### 例 C: `biz_undervalued_talent` (rename 後 = `biz_exposure_gap`)

```yaml
claim: "高 theta_i / 低 露出 ペアが industry baseline を上回る密度で存在する"
identifying_assumption: "露出 = 主要スタジオでの mainstream クレジット。
  別経路 (sakuga / SNS / 海外) の露出は露出ゼロとして扱われる。"
null_model: ["N4 (role-matched bootstrap)", "N7 (activity baseline)"]
method_gate:
  estimator: "joint distribution test (theta_i, exposure) vs null product"
  ci: "bootstrap percentile (n=1000)"
  rng_seed: 42
  shrinkage: "Empirical Bayes (theta_i, beta prior)"
  limitations:
    - "個人提示は theta_i CI 区間幅 < 1.0 のみ"
    - "露出 = 主要 5 スタジオ + メイン役職"
    - "海外下請け露出は捕捉できない"
sensitivity_grid:
  - axis: "露出定義"
    values: ["mainstream studio", "mainstream + sakuga 引用", "全クレジット"]
  - axis: "theta_i 閾値"
    values: ["P75", "P90", "P95"]
interpretation_guard:
  forbidden_framing: ["過小評価", "発掘", "原石"]
  required_alternatives: 2
```

---

## 8. 移行戦略 (49 reports)

v3 への移行は段階的に行う。一度に全 report を書換えると CI 不能になる。

### Phase 0 — 雛形 + 1 report 実証 (本 PR スコープ)

- `_spec.py` に `ReportSpec` / `MethodGate` / `BriefArc` を実装。
- `ci_check_report_spec.py` を opt-in (環境変数 `STRICT_REPORT_SPEC=1` のみ強制)。
- `policy_attrition` を v3 仕様で書換、forest plot を `viz` primitive に乗せる。

### Phase 1 — Policy brief (5 reports)

- `policy_attrition` / `policy_monopsony` / `policy_gender_bottleneck` /
  `policy_generational_health` / `compensation_fairness` を v3 化。
- `BriefArc` で policy brief を 4 段構造に再編。

### Phase 2 — HR brief (6 reports)

- 同手順。`mgmt_director_mentor` の rename を含む。

### Phase 3 — Biz brief (5 reports)

- 同手順。`biz_undervalued_talent` → `biz_exposure_gap` の rename を含む。

### Phase 4 — Technical appendix (15 reports)

- `akm_diagnostics` / `dml_causal_inference` / `score_layers_analysis` 等。
- predictive 主張を持つ `temporal_foresight` は holdout が無ければ archive 候補。

### Phase 5 — Strict mode 有効化

- `STRICT_REPORT_SPEC=1` を CI default に昇格。`pixi run check-method-gates`
  を `pixi run lint` 同列で blocking 化。

---

## 9. 受け入れ基準 (Phase 0)

本文書を承認 gate として、以下を Phase 0 完了の条件とする。

1. `_spec.py` に `ReportSpec` / `MethodGate` / `BriefArc` の skeleton 実装。
2. `policy_attrition` が `ReportSpec` を返す。
3. `policy_attrition` の Cox forest plot が新 `viz` primitive 経由で描画される。
4. 既存テスト (`tests/reports/test_policy_attrition.py`) が壊れない。
5. `lint_vocab` clean (0 violations)。
6. design doc (本文書) に対する review コメント解消。

---

## 10. 用語

- **claim**: 1 文の狭い主張。proposition と互換。
- **identifying assumption**: 主張が成立する前提。因果推論文献の identification
  assumption と同義。
- **null model**: ランダム化ベースライン。`REPORT_PHILOSOPHY.md` §3.2 を実装。
- **sensitivity grid**: 主張が依存する選択 (window / threshold / 集計単位)
  の代替値の集合。
- **interpretation guard**: Findings から Interpretation への越境を防ぐ規則。
  禁止 framing と必須代替解釈の数を保持する。

---

## 改訂履歴

- **v3.0 (2026-05-05)**: 初版。`REPORT_PHILOSOPHY.md` v2.1 の方法論最低要件を
  実装可能な dataclass / brief arc / null model カタログとして具体化。
  Phase 0 (雛形 + policy_attrition 実証) を本 PR スコープと定める。
