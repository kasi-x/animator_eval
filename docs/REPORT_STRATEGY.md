# レポート戦略: Audience-Driven 再編計画

**作成日**: 2026-04-17
**目的**: 現状の method-driven レポート群 (AKM / DML / bridges …) を、
**マクロ政策 / 企業経営 / 新規事業** の3 audience に向けた価値ある分析に再編する。
本書は「何を分析すべきか」「どう達成するか」「どこまで有効か」を整理した計画書。

---

## 0. 方針と前提

### 0.1 基本方針
- **Audience 起点**: 分析の始点は「誰のどの意思決定に役立つか」。手法ではない。
- **既存34レポートは技術付録として温存**: 全面 rewrite ではなく、その上に 3 audience brief + 共通 Parameter Card を積む。
- **v2 Philosophy 準拠**: Findings / Interpretation 分離、CI/null-model/holdout 義務、スコアを能力と呼ばない。
- **"ability" framing 禁止**: 全分析で「ネットワーク位置/構造的特性」としてのみ記述。

### 0.2 データ制約
- anime.score 禁止 (16 contamination pathways 除去中)
- 使用可: クレジット / 役割 / 作品メタ / スタジオ / 制作規模 (staff数ベース) / タイムライン / 共同クレジット / ネットワーク位置
- 能動的に「ability」を主張する表現は避ける

### 0.3 成果物の型
| 型 | 対象 | 分量 | 頻度 |
|---|---|---|---|
| **Audience Brief** | 政策 / 経営 / 新規 | 4-8 page executive brief + web | 四半期更新 |
| **Person Parameter Card** | 全個人 | 1 card / person, JSON + HTML | 月次更新 |
| **Technical Appendix** | 研究者/監査 | 既存34レポート | 現行維持 |

---

## 1. マクロ政策 Brief

### 1.1 新卒離職の因果分解 【優先度: 最高】
**目的**: 新人が3-5年以内に業界を去る原因を特定し、具体的な政策提言を可能にする。

**問い/仮説**:
- H1: 初回参加作品のスタジオ規模 (tier) が離職ハザードに因果的に影響するか
- H2: 初回の現場監督の経験/規模が離職ハザードに因果的に影響するか
- H3: 初回参加作品の「制作混乱」指標 (staff急拡大・役割異常) が離職ハザードに因果的に影響するか
- H4: 初回役割 (1話数 vs 多話数、主要ポジ vs 補助) が離職ハザードに因果的に影響するか

**手法**:

1. **サンプル定義**
   - 対象: `first_year ∈ [2010, 2018]` (6年の観察窓を確保)
   - 右打ち切り: 最終観測年 = 2020 (RELIABLE_MAX_YEAR-3)
   - 除外: 初回作品が特定できないケース (co-credit が 2 人未満)

2. **処置変数 (Treatments)**
   - **T1 (初回スタジオ tier)**: `feat_work_context.scale_tier ∈ {1,...,5}` — カテゴリカル
   - **T2 (初回監督の経験)**: 初回作品の director(s) の当時の person_fe 分位 (Q1-Q5)
   - **T3 (制作混乱指標)**: 初回作品の staff 異動率 = `unique_persons_credited / unique_persons_scheduled` の proxy
     - 実装: `feat_credit_activity` から役割変更件数 / 作品スタッフ数
   - **T4 (初回負荷)**: 初回作品での episode_coverage (1話のみ vs 複数話)

3. **アウトカム (Y)**
   - 生存時間 `t = latest_year - first_year + 1`
   - Event: `t < observation_window` (定義あり)
   - **主要定義**: 5年以上クレジットなし → exit
   - **感度定義**: 3年 / 7年

4. **交絡 (X)**
   - `gender`, `debut_era` (10年 bin), `initial_role_category`, `primary_genre`, `early_co_worker_density`
   - 構造的: 業界年次 (マクロ環境), 先行 5年の業界 entry rate

5. **推定器**

   **(A) Cox Proportional Hazard (記述的)**:
   ```
   h(t | X, T) = h₀(t) × exp(β_T × T + β_X × X)
   ```
   - `lifelines.CoxPHFitter` を使用
   - PH 仮定の確認: Schoenfeld residuals test, 違反時は stratified Cox

   **(B) Double Machine Learning (因果)**:
   ```
   E[Y | X]     ← RSF (Random Survival Forest) で推定
   E[T | X]     ← LightGBM (multinomial) で推定
   θ̂_ATE       ← cross-fit residual-on-residual regression (K=5 folds)
   ```
   - `econml.DML` を使用
   - 各 T について個別に ATE を推定 (離散処置)
   - CI: asymptotic SE × 1.96 (Chernozhukov et al.)

   **(C) 異質性 (Heterogeneous Treatment Effects)**:
   - `econml.dml.CausalForestDML` で τ(X) を推定
   - 層: gender × debut_era × genre で τ の分布を可視化
   - 有意な層差の検定: Wald test on group coefficients

6. **Validation / Sensitivity**
   - **Placebo test**: 初回クレジット year の career-year=0 時点の co-worker count は T と関連しないはず
   - **Rosenbaum bounds**: Γ-sensitivity で未観測交絡に対する頑健性を数値化
   - **Definition sensitivity**: exit = 3/5/7年 それぞれで再推定
   - **Attrition vs right-censoring**: 2015+ debuts は観察窓が短いため別分析

7. **出力**
   - Forest plot: 各処置レベルの HR with 95% CI
   - Heterogeneity heatmap: gender × era で τ̂ を表示
   - 処置効果の ranking: 「最も効く要因」を findings レベルで明示

**必要データ**:
- `feat_career.first_year`, `latest_year`
- `feat_credit_contribution` (初回作品の role × episode_coverage)
- `feat_work_context.scale_tier` (初回作品の tier)
- 初回の co-credit から director を特定 → その director の当時の person_fe
- `persons.gender`
- `feat_anime_genre` (primary genre)
- `feat_credit_activity` (staff変動の proxy)

**必要な新規ライブラリ**: `econml`, `lifelines`, `scikit-survival`

**有効性**: 🟢 **極めて高い**
- 「具体的に何が効くか」を示せる (例: 初回が tier 1 なら離職確率 N% 上昇)
- 政策提言に直結 (新人のアサインメント guideline、初回監督の minimum experience)
- 既存 DML コードの延長で実装可能

**リスク/限界**:
- 未観測交絡: 家族事情・居住地・健康など個人要因は捕捉不可 → 方法注記で明記
- 初回 director の特定: 「共同クレジット」から推定するため誤認混入可能性 → 複数定義で sensitivity
- 離職 = 「5年以上クレジットなし」は proxy。真の離職ではない可能性 → 方法注記

**見出し findings 例 (v2 compliant)**:
- 「初回作品の tier が 1-2 の新人グループは tier 4-5 の新人グループに比べ、3年以内クレジット可視性喪失率が X±Y ポイント高い (n=…, 95% CI)」

---

### 1.2 人材市場流動性・monopsony 指標
**目的**: アニメ業界の労働市場が競争的か独占的か定量化する。

**問い/仮説**:
- H1: スタジオ集中度 (person-year ベース HHI) が過去20年でどう推移したか
- H2: 個人のスタジオ間移動率は他産業と比べて低いか
- H3: person_fe が高い個人ほど所属スタジオを変えにくい (lock-in) か

**手法**:

1. **HHI (Herfindahl-Hirschman Index) の算出**
   ```
   share_{s,y} = person_years_{s,y} / Σ_s person_years_{s,y}
   HHI_y       = Σ_s (share_{s,y})² × 10000
   HHI*_y      = (HHI_y − 1/N_y) / (1 − 1/N_y)   // 正規化 (0=完全分散, 1=独占)
   ```
   - person_years: 各個人のクレジット存在年数をスタジオに按分
     - Alternative 1: 作品関与年のみで按分 (発表作品起点)
     - Alternative 2: first_year〜last_year の連続期間で按分
   - 両定義で結果を並記 (sensitivity)

2. **集中度の時系列とベンチマーク**
   - 1980–2025 の年次推移をプロット
   - 米/韓/中 のゲーム業界 HHI (既出論文値) と比較して相対的位置を論じる
   - 構造変化検定: Chow test で 2010年代前後の断絶を検定

3. **個人レベル流動性**
   - 転職率 `mobility_{p,5y}`:
     ```
     mobility = 1  if len(unique_studios[p, y−5:y]) ≥ 2 else 0
     ```
   - 層別集計: career stage × era × gender × person_fe 分位
   - 年代比較: 1990s debut vs 2010s debut cohorts の 10年累積転職率

4. **Lock-in 回帰 (monopsony 検定)**
   - Panel: person × year
   - Outcome: `same_studio_next_year ∈ {0, 1}`
   - 主要説明変数: `log(person_fe_rank)`
   - Controls: age_proxy, role, genre, studio size, year FE, studio FE
   - モデル:
     ```
     logit(P(stay)) = α_y + α_j + β × log(pfe_rank) + γ × X
     ```
   - 仮説: 高 person_fe ほど stay 確率が高い (β > 0) ⇒ lock-in の証拠
   - 可能であれば外側選択肢 (outside option) を instrument として用いる

5. **Outside option proxy**
   - 「類似プロフィール (同役割・同career year ± 2) の他者を credit したスタジオ数」
   - この値が低いほど「移れる先が少ない」= lock-in 強
   - Manning (2003) のモデル枠組みで解釈

6. **Monopsony 弾力性の近似**
   - アニメ業界は賃金データがないため、「転職時の person_fe 変化」を wage proxy として扱う
   - Post-switch person_fe change の分布が右偏 ⇒ 「離れた方が良かった」人が離れている = lock-in 強の証拠

**必要データ**: `feat_studio_affiliation`, `feat_career`, `feat_credit_contribution`

**有効性**: 🟢 **高い**
- アニメ業界の monopsony は長年疑われてきたが定量的証拠が乏しい
- 政策議論 (公正取引委員会的視点) で価値
- 既存の studio affiliation データで即実装可能

**リスク/限界**:
- 制作委員会方式 = 「スタジオ所属」の概念が曖昧 → フリーランス扱いの補正が必要
- 定義sensitivity が結果を大きく変えうる → 複数定義の結果を並記

---

### 1.3 ジェンダー・ボトルネック分析
**目的**: キャリアのどの段階で女性代表性が低下するかを特定する。

**問い/仮説**:
- H1: 性別比は入職時 vs 5年後 vs 10年後 vs 監督昇進時でどう変化するか
- H2: 特定の役割経路でジェンダー格差が集中するか (作画 → 作監 → 監督 パス vs 他)
- H3: スタジオ別に女性の定着率・昇進率が大きく分散しているか

**手法**:

1. **コーホート定義**
   - Debut cohorts: 2000, 2005, 2010, 2015 (5年窓)
   - 最小観測: 各 cohort 5年以上
   - 欠損 gender のサンプル: data statement で報告、分析からは除外

2. **ステージ遷移の定義**
   - Stage 0 (新人) → Stage 1 (動画/原画) → Stage 2 (作画監督系) → Stage 3 (演出) → Stage 4 (監督) → Stage 5 (総監督/chief)
   - 遷移イベント: 初めて該当 role_category で credit された年
   - `feat_person_role_progression` から抽出

3. **Kaplan-Meier 生存推定**
   - 各遷移 (k, k+1) について:
     ```
     Ŝ_g(t) = Π_{t_i ≤ t} (1 − d_i / n_i)
     ```
   - 各 t_i で: d_i = 遷移数, n_i = at-risk 人数
   - 性別 g ∈ {F, M} 別に推定
   - Log-rank test: χ² で性別間の有意差
   - Peto-Peto variant: 早期の差を感度高く検定

4. **競合リスク分析 (Fine-Gray sub-distribution hazard)**
   - 競合イベント: {昇進, 離職, 継続}
   - Sub-distribution hazard:
     ```
     λ_k^sub(t) = lim dt→0 P(T ≤ t+dt, event=k | T > t または (T ≤ t かつ event ≠ k)) / dt
     ```
   - `lifelines.CRCSplineFitter` または R の `cmprsk`
   - Outcome: 昇進 vs 離職 の相対リスクを gender 間で比較

5. **スタジオ FE × 性別 交互作用**
   - Cox モデル:
     ```
     h_{ij}(t) = h₀(t) × exp(α_j + β × female + γ_j × female + δ × X)
     ```
   - α_j: スタジオ j の基底 FE
   - γ_j: スタジオ特有の gender 差 (本分析の主要パラメータ)
   - Bayesian hierarchical model:
     ```
     γ_j ~ Normal(μ_γ, τ²_γ)
     ```
   - τ²_γ (shrinkage posterior) で「スタジオ間で gender gap が異なる程度」を推定
   - PyMC または Stan で推定

6. **Oaxaca-Blinder 分解**
   - 昇進率の性別 gap:
     ```
     R_M − R_F = [β_M × (X̄_M − X̄_F)]   // 説明可能 (分布差)
                 + [(β_M − β_F) × X̄_F]     // 説明不能 (構造差)
     ```
   - X: 役割分布, スタジオ分布, 初回 tier, career years
   - 説明不能部分 > 説明可能部分 ⇒ 構造的要因の示唆

7. **出力**
   - Survival curves stacked by transition × gender
   - Scatter of α_j vs γ_j (各スタジオを点で表示)
   - Decomposition bar chart
   - ジェンダー gap が集中する段階 (どの遷移で最も差が開くか) を明示

**必要データ**: `persons.gender`, `feat_person_role_progression`, `feat_studio_affiliation`, `feat_credit_contribution`

**有効性**: 🟢 **高い**
- 政策・企業両方に響く (政治的トラクション強)
- 具体的レバー (どの段階に介入すべきか) を示せる
- 既存 `career_friction_report` をジェンダー視点で特化可能

**リスク/限界**:
- gender フィールドの欠損率が高いと bias 混入 → 欠損率を data statement に明記
- 「能力差」解釈への予防線を findings レベルで徹底 → 構造的要因の記述に限定

---

### 1.4 世代交代健全性指標
**目的**: 業界が「高齢化」しているか、新陳代謝が機能しているかを診断する。

**問い/仮説**:
- H1: ベテラン (キャリア20年+) の絶対数/比率の推移
- H2: コーホート別 5年/10年/15年生存率は改善しているか悪化しているか
- H3: 参入数 / 退職数 比率の推移

**手法**:

1. **コーホート生存**
   - 5 cohorts: debut decade ∈ {1980s, 90s, 00s, 10s, 20s}
   - 各 cohort の survival S(k) at career year k = 5, 10, 15, 20
   - cohort 間の S(k) を比較、傾向検定 (Jonckheere-Terpstra)

2. **世代ピラミッド**
   - 各年 y の active population を career-year bin で分布表示
   - Shape: bottom-heavy (若年多) / top-heavy (高齢多) / narrowing (全体縮小)
   - 時系列アニメーション

3. **Flow accounting**
   - 各年: entry, exit, net_flow
   - entry_rate = new / active
   - exit_rate = exit / active (5y-cutoff 定義)
   - dependency_ratio = senior(≥20y) / junior(≤3y)

4. **Productivity 変化 (Baumol's cost disease の検証)**
   - veteran share と industry-wide production scale の関係
   - veteran share が増加しても production scale が維持されているか
   - 逆ならば「高齢化で産業が縮小」の示唆

**有効性**: 🟡 **中程度**
- 既存 `industry_overview` で部分的に実装済み → 政策視点での再フレーミングが主作業
- 既存結果の再解釈が多く、新規性は限定的

---

### 1.5 地域/国際パイプライン【データ依存】
**目的**: 地域・国別の人材供給源・流出先を把握する。

**データ依存度が高く優先度は低い**。`persons.country` の coverage を確認してから判断。

---

## 2. 企業経営 Brief

### 2.1 スタジオ・ベンチマーク・カード 【優先度: 最高】
**目的**: 各スタジオが業界平均比でどの位置にいるかを可視化する。

**問い/提供指標**:
- 定着率 (5年継続率): そのスタジオで初回クレジットした新人の N 年後残存率
- 育成 value-add: そこで育った人材のキャリア後半 person_fe 平均 (他スタジオ移籍後含む) 対 業界ベースライン
- タレント吸引力: 他スタジオから移籍してきた人数/期間
- 役割多様性: スタジオ内の役割分布エントロピー
- 規模ティア推移: 関わった作品の tier 分布の経年変化

**手法**:

1. **定着率 R_5 (5年継続率)**
   ```
   R_5_s = |{ p : first_studio(p)=s ∧ active_at_year(p, t+5) }| / |{ p : first_studio(p)=s }|
   ```
   - Cohort: first_year ∈ [2010, 2015] のスタジオ別新人集合
   - active_at_year: ±1 credit 定義 / ±3 credit 定義 (sensitivity)

2. **育成 value-add**
   - スタジオ s を経由した人 p について:
     ```
     Y_p,post = person_fe(p, post-exit period)
     Ŷ_p,post = GBM予測 (features: p の初期状態 + s 到着時の role/tier)
     ε_p      = Y_p,post − Ŷ_p,post
     VA_s     = mean(ε_p) over p ∈ alumni(s)
     ```
   - 「そこで育って他に移った人が、予測を超えて伸びたか」を測定

3. **Empirical Bayes shrinkage** (小 n 対策)
   ```
   μ̂, τ̂² ← {R_5_s, n_s} から推定 (moment method または REML)
   R̂_5_s  = (n_s × τ² × R_raw + σ²_raw × μ̂) / (n_s × τ² + σ²_raw)
   ```
   - n_s < 5 のスタジオは "insufficient n" フラグを立てるが値は出す
   - 同様の shrinkage を VA_s, 吸引力, 多様性 にも適用

4. **吸引力 (Attraction)**
   - inflow_s = Σ_p 1{joined(p, s) in [y−3, y]}
   - Normalized: inflow_s / industry_total_inflow
   - 流入/流出 ratio = inflow_s / outflow_s

5. **役割多様性 (Role diversity)**
   - Shannon entropy:
     ```
     H_s = − Σ_r p_{s,r} × log(p_{s,r})
     ```
   - p_{s,r}: スタジオ s の credit に占める役割 r の比率
   - 24 role types を 5-8 role categories に集約してから計算

6. **規模ティア推移**
   - Past 5y で関与した作品の tier 分布
   - 移動平均 trajectory を chart

7. **Composite percentile card**
   - 各指標を業界 percentile (0-99) に変換
   - Radar / parallel coordinates で 6 軸表示
   - Shrinkage 済み値を表示、raw 値は hover で参照

8. **CI 付与**
   - R_5: Wilson score interval (二項)
   - VA_s: Bootstrap (n_boot=1000) percentile interval
   - H_s: Bootstrap
   - 多指標の同時比較時は Bonferroni または BH 補正

9. **Year-on-year delta**
   - 各指標の前年比変化を示す
   - トレンドの向き (改善/悪化) を Kendall's τ で検定

**必要データ**: `feat_studio_affiliation`, `feat_career_annual`, `feat_work_context`, `feat_scores`

**有効性**: 🟢 **極めて高い**
- スタジオ経営層が喉から手が出るほど欲しい情報 (Glassdoor for studios)
- 投資家・取材メディアも potential consumer
- 既存 `studio_impact`, `studio_timeseries` の再編で実装可能

**リスク/限界**:
- 名誉毀損リスク: 「悪いスタジオ」ランキング化への誘惑を methodology 的に抑える必要
- 規模差の補正を丁寧に (大手有利の bias を避ける)

**見せ方**: v1 の「順位付け」ではなく、multi-axis parallel coordinates + 自社を hover highlight

---

### 2.2 監督育成力ランキング (Mentor Value-Add)
**目的**: 「新人を育てている監督」を定量的に特定する。

**問い/仮説**:
- H1: 監督 X の下で初回クレジットした人材の、5年後 person_fe は業界平均からどれだけ上振れるか
- H2: その上振れは監督の特性 (本人のperson_fe, 作風, 規模) で説明可能か
- H3: 「育てる監督」と「使い潰す監督」のパターン差はあるか

**手法**:

1. **Mentee 定義** (2つの代替定義で sensitivity)
   - **定義A**: 初回 credit の director (co-credit から特定)
   - **定義B**: 初回 3 作品中で最も同席回数が多い director
   - 両定義で結果を並記。一致率 > 70% ならば単一化、未満なら両方維持

2. **Mentee outcome Y_p**
   - `Y_p = person_fe_percentile(p) at career_year=5`
   - Alternative: career_year=10 (より長期効果、sample 減少)

3. **Expected outcome Ŷ_p** (counterfactual baseline)
   - 説明変数 (career year 1 時点で観測可能):
     - initial role category (動画 / 原画 / other)
     - first anime tier (1-5)
     - first studio size
     - debut year
     - gender
     - first genre
   - Model: `LightGBM Regressor` with 5-fold CV
   - Training: 全 mentee サンプル (director 無視) で Ŷ を fit
   - 目的: 「mentor 効果を除いた baseline」を近似

4. **Mentor effect M_d**
   ```
   M_d = (1/n_d) × Σ_{p ∈ mentees(d)} (Y_p − Ŷ_p)
   ```
   - n_d ≥ 5 の director のみ対象

5. **Empirical Bayes shrinkage**
   ```
   τ²_M ← Σ (M_d − M̄)² / (N_directors − 1)  (cross-director variance の推定)
   σ²_d ← var(ε_p) / n_d                    (director 内 sampling variance)
   k    = σ̄²/τ²                              (global shrinkage constant)
   M̂_d  = (n_d / (n_d + k)) × M_d
   ```
   - 少ないメンティーの director は大きく中央に縮む

6. **Null model**
   - Permutation: mentee-director 関係をランダムに入れ替え、M を再計算
   - n_perm = 1000 回 simulation で null distribution を構築
   - 各 director の M̂_d が null 95% CI を外れるか検定

7. **Selection bias 対処**
   - **Within-studio comparison**: studio FE をコントロールに追加して M̂_d を再推定
     - 「優秀 director が良いスタジオにいる」効果を除去
   - **Instrumental variable (可能なら)**:
     - Instrument: director の「直前プロジェクト完了時期」 ← 並行可能性に関する外生変動
     - 2SLS で M̂_d の IV 版を推定
   - IV 版と OLS 版の乖離が大きいなら selection bias の存在を示唆

8. **Caveats & 出力**
   - Findings: M̂_d の点推定 + 95% CI (bootstrap)
   - Interpretation: 「これは育成効果の proxy であり、selection を完全には除外できない」を明示
   - 公開: ranking top-N のみ (ranking への異議申立てフォームを用意)

**必要データ**: 初回クレジットの director を特定する共同クレジット分析

**有効性**: 🟢 **高い**
- 可視化されていない貢献を浮き彫りにする (mentor は報酬を受け取りにくい)
- 業界賞/表彰への基礎資料として価値
- 育成の良いスタジオを間接的に可視化

**リスク/限界**:
- Attribution 問題: メンティー成長は初回監督だけでなく次のアサイン・スタジオ環境も影響
- Selection bias: 伸びる新人を見抜いて採用する監督 vs 誰でも伸ばす監督の区別がつかない → 明示的な caveat が必要
- 小 n の監督は高variance → shrinkage 必須

---

### 2.3 離職リスクスコア (個人 × 現場組合せ)
**目的**: 新人個人 × 配属チームの組合せで、N年以内離職確率を予測する。

**手法**:

1. **サンプル & イベント定義**
   - 対象: `first_year ∈ [2010, 2018]`, n ≈ 数千
   - Event: `t_exit = last_credit_year + 1` (censored at `RELIABLE_MAX_YEAR - 3`)
   - 打ち切り率が高い場合は inverse probability of censoring weighting (IPCW)

2. **特徴量 (feature engineering)**
   - **個人特徴** (year=1 時点で観測可能):
     - gender, debut role, primary initial genre, debut region
   - **初回作品特徴**:
     - tier (1-5), total staff count, episode count, format (TV/OVA/Movie)
     - director(s) の当時の person_fe (Q1-Q5)
     - 作品の production length (制作期間 proxy)
   - **チーム特徴**:
     - 初回作品の team size, 役割分布の entropy
     - 同作品 co-worker の平均 career years
     - director の過去 mentee retention rate (data leakage を避けるため過去のみ)
   - **マクロ**:
     - debut year の業界 entry rate
     - 業界全体の exit rate baseline

3. **モデル**
   - **主モデル**: `scikit-survival.RandomSurvivalForest` or `xgbse.XGBSEDebiasedBCE`
   - **比較**: Cox PH (線形baseline), DeepSurv (非線形比較)
   - 各モデルで同じ feature set を使用

4. **Temporal split (data leakage 防止)**
   - Train: debut year ∈ [2010, 2015]
   - Val: debut year ∈ 2016
   - Test: debut year ∈ [2017, 2018]
   - Features は debut year + 1 時点のスナップショットのみ (未来情報混入禁止)

5. **評価指標**
   - **C-index** (concordance index): target ≥ 0.70 を publication gate
   - **Time-dependent AUC** at t=1, 3, 5
   - **Brier score** (IPCW-weighted)
   - **Calibration plot**: 予測リスク 5 分位 × 実際の KM 生存率

6. **Feature importance**
   - SHAP values on test set
   - 重要特徴を brief に提示
   - 個人スコアは出さず、「パターン」を提示

7. **公開 vs 内部利用の分離**
   - 公開版: top-5 feature importance のみ、個人 prediction は出さない
   - 内部 API (認証付き): 個人 prediction score 提供 (studio HR 利用想定)

8. **Fairness audit**
   - gender 別, 地域別, career path 別に C-index を分割評価
   - グループ間で性能が大きく異なる場合は Warning
   - 需要性能差を data statement で開示

**有効性**: 🟢 **高い (予測精度が出れば)**
- HR 的に直接 actionable
- スタジオは「辞めそうな新人」の早期発見に支払う意思あり

**リスク/限界**:
- 予測精度が低ければ公開に値しない → 厳格な gate (C-index > 0.7 を最低基準)
- 個人スコア公開の倫理問題 → スタジオ内部利用を想定、公開版は aggregate 統計のみ

---

### 2.4 後継計画マトリクス
**目的**: 引退が近いベテランに対し、内部で後継候補となりうる人材を特定する。

**手法**:

1. **引退リスクスコア**
   - career_years: `RELIABLE_MAX_YEAR − first_year`
   - recent_credit_trend: 直近 3 年の credit count slope (LOESS または OLS)
   - **RetireRisk_p = sigmoid(career_years − 25) × (1 − normalized_slope)**
   - Q90 以上をフラグ

2. **後継候補の特定**
   - 候補プール: 同スタジオ × 同 role_category × career_year ∈ [10, 20]
   - **特徴ベクトル v_p = [role_dist, tier_dist, genre_affinity, person_fe, centrality]**
   - **Similarity score**: `cosine(v_veteran, v_candidate)`
   - **Weighted score** = similarity × 0.5 + same_studio × 0.2 + co-credit_frequency × 0.3

3. **Ranking per veteran**
   - 各ベテラン v について top-10 候補をリスト
   - 公開版: aggregate (「N 人のベテラン中 M 人に後継候補が存在」) のみ
   - 内部版: 個別 candidate list

4. **Network-based alternative**
   - Random walk from veteran in collaboration graph
   - Stationary probability on same-role persons = implicit successor score

**有効性**: 🟡 **中〜高**
- 高齢化が進む業界で価値あるが、具体的な「年齢」データが限られる
- キャリア年数だけだと粗い

---

### 2.5 チーム化学反応 (Team Chemistry)
**目的**: 想定を超える成果を出すチーム組合せの特定。

**手法**:

1. **Pair outcome の算出**
   - 作品 a で person_i, person_j が共に credit された場合:
     ```
     Y_{a,ij} = log(production_scale_a) − expected(a)
     expected(a) = f(tier, studio, episode_count, year)  // GBM で推定
     ```

2. **Pair-level aggregation**
   ```
   mean_res_{ij} = (1/n_{ij}) × Σ_a Y_{a,ij}
   SE_{ij}       = sd / sqrt(n_{ij})
   ```
   - n_{ij} ≥ 3 作品を条件

3. **期待外成果の検定**
   - H0: mean_res_{ij} = 0
   - 片側 t-test; 多重比較で BH 補正

4. **可視化**
   - 共作ネットワーク上で edge weight = mean_res_{ij}
   - Positive chemistry の上位エッジをハイライト

**有効性**: 🟡 **中程度**。既存機能の特化で比較的低コスト実装可。

---

## 3. 新規事業 Brief

### 3.1 過小評価タレント・プール 【優先度: 高】
**目的**: 実力 (構造指標) は高いが最近の露出/アサインが少ない人材を特定する。

**問い**: 新規スタジオが比較的 accessible なハイクオリティ人材を発見できるか？

**手法**:

1. **Under-exposure score**
   ```
   U_p = percentile(person_fe_p) − percentile(recent_3y_credit_count_p)
   ```
   - Threshold: U_p ≥ 30 ポイント

2. **Stability check**
   - Under-exposure が長期で持続している (5年中 3年以上で U_p ≥ 30) 人を「構造的過小露出」とラベル
   - 一時的な dip (1年のみ) は除外

3. **理由クラスタリング** (K=5 想定)
   - 特徴ベクトル per person:
     - gap pattern: 連続クレジット空白年数の分布
     - age proxy: career years
     - gender × age interaction (育児期間 proxy)
     - former studio status: 初期スタジオが現在も稼働しているか
     - cross-industry proxy: 同名同定が他業界で検出されるか (external data 必要)
   - K-means (K=5) で archetype を抽出:
     - 育児・介護休業型 (F, career 3-10y, gap 3-5y, 復帰兆候あり)
     - スタジオ倒産型 (初期所属崩壊 + 再就職遅延)
     - 他業界移行型 (他業界での credit 検出)
     - 意図的セーブ型 (低頻度だが継続)
     - 引退移行型 (career 20y+ の gradual decline)

4. **Recruiting feasibility score**
   - 「戻ってくる可能性」を proxy:
     - 近年の partial return signals (1年 1-credit)
     - SNS 可視性 (外部データ、optional)
     - mentor/元同僚との active network

5. **出力**
   - 公開版: archetype 別の aggregate counts, 代表的 career pattern
   - 内部版 (認証付): 個人リスト with feasibility score
   - 言葉遣い: 「過小評価」ではなく「直近露出が少ない高構造スコア群」

**必要データ**: `feat_career_annual`, `feat_scores`

**有効性**: 🟢 **高い**
- 直接 actionable (「このリストから声かけてみろ」で終わる)
- 新規事業・小規模スタジオが最も欲しい情報

**リスク/限界**:
- 誤った印象を与える可能性 (「過小評価」言葉が評価的) → 「直近露出が少ない高構造スコア群」と表現
- 名誉毀損リスクの慎重な扱い

---

### 3.2 ジャンル空白地 (Genre Whitespace)
**目的**: 需要成長があるのに熟練スタッフ供給が少ないジャンルを特定する。

**問い**:
- H1: 過去5年で作品数が増加しているジャンル (需要の proxy)
- H2: そのジャンルに 3+ 作品参加した熟練者 (supply) は十分か
- H3: 需要/供給ギャップが最も大きいジャンルは何か

**手法**:

1. **需要 proxy**
   ```
   count_{g,y}      = そのジャンル g の y 年の作品数
   penetration_{g,y} = count_{g,y} / total_anime_y
   CAGR_{g,5y}      = (count_{g,y} / count_{g,y−5})^(1/5) − 1
   ```
   - CAGR と penetration の両方を demand proxy として使用
   - Alternative: total episodes per genre (episode 数ベース)

2. **供給**
   ```
   specialist_g = |{ p : genre_share(p, g) > θ ∧ person_fe_percentile(p) > 75 }|
   θ ∈ {0.5, 0.6, 0.7}  // sensitivity
   ```
   - 「ジャンル g でキャリアの大半を積んだ上位スタッフ」

3. **Whitespace score**
   ```
   W_g = CAGR_{g,5y} × penetration_{g,current} / log(specialist_g + 1)
   ```
   - 正規化: 全ジャンルの W_g を 0-99 percentile に

4. **ジャンル間関係分析**
   - 共起 clustering: 同一作品内でタグ付けされることが多いジャンルをグループ化
   - 「隣接ジャンル」で specialist を供給できる可能性を評価
   - Transition matrix: ジャンル g1 の specialist が g2 に参入する確率

5. **スタジオ overlay**
   - 各 whitespace genre に対し、既存スタジオの specialization 分布を overlay
   - 「空白地 × 既存参入スタジオ少ない」セルを抽出

6. **出力**
   - Genre grid: demand CAGR × supply count, whitespace セルを highlight
   - Recommendation table: top 5 whitespace genre × 隣接供給候補

**必要データ**: `feat_anime_genre`, `feat_career`

**有効性**: 🟢 **高い**
- コンテンツ企画の根拠資料として直接価値
- 新規参入者の戦略立案材料

**リスク/限界**:
- 需要の proxy が弱い (作品数は供給側指標でもある)
- anime.score を使わないため「人気ジャンル」は直接測れない → 前提明示

---

### 3.3 チーム組成テンプレート (Tier別成功パターン)
**目的**: tier ごとの「成功するチーム構造」の標準パターンを抽出する。

**手法**:

1. **Success 定義**
   - 主要基準: production_scale tier ≥ 4 AND post-project team retention ≥ 0.80
   - retention = 同メンバーの N 年後の industry active 率
   - Sensitivity: tier 3+ / retention 0.70+

2. **Team feature vector**
   - Team size
   - Role distribution (24 roles → 8 category percentages)
   - Career year distribution (mean, std, quartiles)
   - Gender composition
   - person_fe distribution (mean, std, max)
   - Intra-team network density (co-credit edges / possible edges)
   - Studio experience heterogeneity (studio diversity entropy)

3. **Clustering**
   - Standardize all features (z-score)
   - K-means K=5 (elbow method で K 選定)
   - 各クラスタの centroid を feat_specs で ranking → archetype 命名
     (例: "veteran-heavy", "small-elite", "large-mixed", "balanced-mid", "newcomer-dominant")

4. **Validation**
   - Hold-out teams (20%): どのクラスタに分類され、success 率が cluster baseline と整合するか
   - Silhouette score > 0.3 を gate

5. **Recommendation output**
   - 各 tier に対して「この構成パターンが最も high-retention」の例
   - 新規スタジオ setup 指針として使用

**有効性**: 🟡 **中〜高**
- 新規スタジオ設立時の「何人雇えば回るか」指針として価値
- ただし「成功」の定義に依存

---

### 3.4 信頼ネット参入経路
**目的**: 新規スタジオが既存信頼ネットに組み込まれるための最短 hiring パス。

**手法**:

1. **Gatekeeper score**
   ```
   G_p = z(betweenness_p) + z(distinct_studios_p) + z(person_fe_percentile_p) + z(bridge_score_p)
   ```
   - z: z-score standardization
   - 上位 top-100 をプール化

2. **Reach metric**
   - 2-hop reachability: person p から共演グラフで 2 hop 以内にたどり着く unique person 数
     ```
     Reach_p = |N²(p)| / |V|
     ```
   - Weighted variant: edge weight でスコアリング

3. **Hiring path optimization**
   - 新規スタジオが k 人雇う場合の最適化:
     ```
     maximize  |N²(S)|      // 雇用セット S の 2-hop reach
     s.t.      |S| ≤ k
               Σ_{p ∈ S} cost_p ≤ budget
     cost_p = f(person_fe_percentile_p, career_years_p)
     ```
   - Greedy submodular optimization (Nemhauser-Wolsey近似) で解く

4. **Network evolution simulation**
   - 「雇用された gatekeeper が N 年後にどの程度信頼 network を拡張するか」を可視化
   - Historical data から expansion rate を推定

5. **出力**
   - Top gatekeeper profile summary (aggregate; 個別開示せず roles/career stages のみ)
   - Reach × cost のペア frontier
   - Case study: 過去 5 年で新規参入したスタジオの実際の hiring pattern 分析

**有効性**: 🟢 **高い**
- Anime業界特有の信頼ネット論理を捉える
- 実際の hiring 意思決定に使える
- 既存 bridges/structural_holes 分析の延長

**リスク/限界**:
- 特定個人のネット価値を公にすることの倫理的配慮
- 公開版は aggregate パターンのみ、個人特定版はプライベート

---

### 3.5 独立ユニット形成可能性
**目的**: 既存スタジオから独立して動けるグループを特定する。

**手法**:

1. **Candidate group 生成**
   - 共演グラフから密結合部分グラフ (community detection: Louvain / Leiden) を抽出
   - サイズ {10, 20, 50} の候補グループ群

2. **Coverage score**
   ```
   coverage_G = |{ role r : ∃ p ∈ G with role r in past 3y }| / total_required_roles
   ```
   - 必要 roles リスト: 制作進行、監督、作画監督、美術監督、音響監督、... (24 roles 全部または essential subset 12)

3. **Trust density**
   ```
   density_G = Σ_{i,j ∈ G} edge_weight_{ij} / (|G| × (|G|−1) / 2)
   ```

4. **Viability score**
   ```
   V_G = coverage_G × density_G × mean(person_fe_G)
   ```
   - Top candidate groups を ranking

5. **Stress test**
   - 特定 role を 1人欠いた場合に coverage を保てるか (redundancy check)
   - 外部コラボでカバー可能な role を識別

**有効性**: 🟡 **中程度**。業界構造変化 (制作委員会 → 独立ユニット) の文脈で価値。

---

## 4. 共通基盤: Person Parameter Card

### 4.1 目的
個人を 7-10 個の「わかりやすい日本語 parameter」で表現し、全レポートの参照基盤にする。

### 4.2 パラメータ設計
| 日本語名 | 内訳 | 正規化 |
|---------|------|--------|
| 規模到達力 | person_fe (AKM) | 0-99 percentile |
| 協業幅 | versatility score | 0-99 percentile |
| 継続力 | normalized CV (safety) | 0-99 percentile |
| 育成貢献 | mentor residual | 0-99 percentile |
| 中心性 | weighted PageRank | 0-99 percentile |
| 信頼蓄積 | cumulative edge weight | 0-99 percentile |
| 役割進化 | role progression slope | 0-99 percentile |
| ジャンル特化 | top-genre affinity | 0-99 percentile |
| 直近活発度 | 直近3年 weighted credits | 0-99 percentile |
| 相性指標 | high-compatibility partners count | 0-99 percentile |

### 4.3 各 parameter の詳細手法

| パラメータ | ソース指標 | 計算式 | 正規化 | CI |
|-----------|-----------|-------|-------|----|
| 規模到達力 | AKM person_fe θ_i | AKM 推定 (既存) | `rank(θ_i) / N × 99` | analytical SE = σ/√n_credits |
| 協業幅 | versatility | `entropy(role_distribution_p)` | percentile | bootstrap n=1000 |
| 継続力 | consistency | `1 − CV(annual_score_p)` | percentile | bootstrap |
| 育成貢献 | mentor residual | 2.2 節の M̂_d | percentile | bootstrap |
| 中心性 | weighted PageRank | 既存 pagerank | percentile | network null model |
| 信頼蓄積 | cumulative edge weight | `Σ edge_weight_p` | percentile | analytical |
| 役割進化 | role progression | stage transition count / career_years | percentile | analytical |
| ジャンル特化 | genre_affinity max | `max_g(share_g × quality_g)` | percentile | bootstrap |
| 直近活発度 | recent weighted credits | `Σ_{y ∈ last 3y} weight_{p,y}` | percentile | analytical |
| 相性指標 | high-compat partners | `|{q : mean_res_{pq} > Q75}|` | percentile | bootstrap |

### 4.4 Archetype labeling

- 10-vector を K-means (K=6) で clustering
- Centroids を feat_specs ranking で archetype 命名:
  - "構造中核型" (中心性・信頼蓄積 高)
  - "育成型" (育成貢献 高)
  - "スペシャリスト型" (ジャンル特化 高, 協業幅 低)
  - "広域ジェネラリスト型" (協業幅 高, 継続力 中)
  - "現役トップ型" (直近活発度 + 規模到達力 高)
  - "レガシー型" (継続力 + 信頼蓄積 高, 直近活発度 低)
- ユーザは archetype ラベル + 10-axis radar の両方を見られる

### 4.5 表示
- Radar chart (10-axis, Japanese labels) with CI as shaded band around the polygon
- 各 parameter を click で expand → raw value + CI + method note + 同じ archetype の他者リスト
- Archetype transition 可視化: career year ごとの archetype 推移

### 4.6 v2 compliance
- 各 parameter の findings は数値のみ (CI 付き)
- Interpretation (archetype 分類) は明示的に「解釈」と labeling
- 能力・優劣を暗示する語彙禁止

### 4.7 有効性
🟢 **極めて高い**: 全分析の基盤、一般ユーザーと analyst の共通語彙。初期投資は大きいが ROI が最も高い。

---

## 5. 方法論的必須事項 (全分析共通)

### 5.1 v2 ゲート
- [ ] 個人レベル推定値に **必ず analytical CI** (SE = σ/√n)
- [ ] グループレベル主張に **必ず null model 比較**
- [ ] 予測主張に **必ず holdout validation** (C-index, AUC, RMSE)
- [ ] Findings 層に評価形容詞・因果動詞ゼロ
- [ ] Interpretation 層は明示 label + 代替解釈を最低1つ

### 5.2 感度分析
- 閾値依存の結果 (exit = 5年 vs 3年 など) は必ず複数定義で出す
- 時代補正の重み選択で結果が変わる可能性を明記

### 5.3 データ声明
全レポート末尾に:
- Source coverage
- Bias (どの年代・どの役割・どの地域で疎か)
- Name resolution confidence
- Missing values の扱い

---

## 6. 優先順位マトリクス

| # | 分析 | audience | 有効性 | 実現性 | 新規性 | 総合優先度 |
|---|------|---------|-------|-------|-------|-----------|
| 4 | **Person Parameter Card** | 全 | 🟢 | 🟢 | 🟡 | ⭐⭐⭐⭐⭐ |
| 1.1 | **新卒離職因果分解** | 政策 | 🟢 | 🟢 | 🟢 | ⭐⭐⭐⭐⭐ |
| 2.1 | **スタジオ Benchmark Card** | 経営 | 🟢 | 🟢 | 🟡 | ⭐⭐⭐⭐⭐ |
| 3.1 | **過小評価タレント・プール** | 新規 | 🟢 | 🟢 | 🟢 | ⭐⭐⭐⭐⭐ |
| 1.2 | 人材市場流動性 | 政策 | 🟢 | 🟢 | 🟢 | ⭐⭐⭐⭐ |
| 2.2 | 監督育成力ランキング | 経営 | 🟢 | 🟡 | 🟢 | ⭐⭐⭐⭐ |
| 3.4 | 信頼ネット参入経路 | 新規 | 🟢 | 🟢 | 🟡 | ⭐⭐⭐⭐ |
| 1.3 | ジェンダー・ボトルネック | 政策 | 🟢 | 🟢 | 🟡 | ⭐⭐⭐⭐ |
| 3.2 | ジャンル空白地 | 新規 | 🟢 | 🟡 | 🟡 | ⭐⭐⭐ |
| 2.3 | 離職リスクスコア (予測) | 経営 | 🟢 | 🟡 | 🟡 | ⭐⭐⭐ |
| 3.3 | チーム組成テンプレート | 新規 | 🟡 | 🟡 | 🟡 | ⭐⭐⭐ |
| 2.4 | 後継計画マトリクス | 経営 | 🟡 | 🟡 | 🟡 | ⭐⭐⭐ |
| 1.4 | 世代交代健全性 | 政策 | 🟡 | 🟢 | 🔴 | ⭐⭐ |
| 2.5 | チーム化学反応 | 経営 | 🟡 | 🟢 | 🔴 | ⭐⭐ |
| 3.5 | 独立ユニット形成 | 新規 | 🟡 | 🟡 | 🟡 | ⭐⭐ |
| 1.5 | 地域/国際パイプライン | 政策 | 🟡 | 🔴 | 🟡 | ⭐ (data依存) |

凡例: 🟢 高 / 🟡 中 / 🔴 低

---

## 7. 段階的ロードマップ

### Phase A: 基盤構築 (Person Parameter Card)
- `src/analysis/person_parameters.py` 新設
- 10 parameter の計算 + 正規化 + CI
- HTML radar chart template
- 全 person 分の JSON export
- **Gate**: 単体テスト + サンプル出力の人間レビュー

### Phase B: マクロ政策 Brief
1. 新卒離職因果分解 (1.1) - メイン
2. 人材市場流動性 (1.2) - 補助
3. ジェンダー・ボトルネック (1.3) - 補助
- **Gate**: DML の holdout validation, null model 比較, sensitivity, data statement

### Phase C: 企業経営 Brief
1. スタジオ Benchmark Card (2.1) - メイン
2. 監督育成力 (2.2) - 補助
3. 離職リスクスコア (2.3) - オプション (予測精度 gate を通過した場合のみ)
- **Gate**: empirical Bayes shrinkage, CI, C-index thresh

### Phase D: 新規事業 Brief
1. 過小評価タレント・プール (3.1) - メイン
2. 信頼ネット参入経路 (3.4) - 補助
3. ジャンル空白地 (3.2) - 補助
- **Gate**: 個人識別リスクレビュー, 倫理 check

### Phase E: 連携強化
- 3 Brief 間の cross-reference (例: 経営 Brief から Mentor ランキングの政策 Brief セクションへリンク)
- Person Parameter Card から全分析への deep link

---

## 8. 既存レポート (34本) の位置付け

| 分類 | 扱い |
|------|------|
| 技術付録として維持 | bridge_analysis, structural_holes, temporal_pagerank, dml 各種, akm_diagnostics, shap_explanation |
| 3 Brief に吸収/再編 | industry_overview (政策Brief), studio_impact (経営Brief), career_friction (政策Brief), compensation_fairness (政策+経営Brief) |
| 縮小/統合 | 重複度の高い細分レポート群は該当 audience brief 内の sub-section に |
| 削除検討 | test/legacy レポート |

---

## 9. リスクと倫理

### 9.1 法的リスク
- 個人の「能力」評価と誤読される表現は全面禁止
- Undervalued/離職予測 の個人名出力は公開版から除外 (aggregate のみ)
- 「このスタジオは悪い」という読解を誘発する ranking 化を避ける

### 9.2 方法論的リスク
- Selection bias (生き残った個人しかデータにない) → すべての出力に注記
- Attribution の曖昧さ (monitor 効果 vs selection 効果) → Interpretation 層で明示
- Confounding (未観測要因) → DML で一部対応、caveat 明記

### 9.3 データ品質リスク
- 名前解決誤認 → entity resolution eval を公開し confidence を開示
- 時代カバレッジ偏り → data statement 必須

---

## 10. 成否の判定基準

各 audience Brief に対し:

| 基準 | 測定方法 |
|------|---------|
| 読まれるか | セッション時間 / 到達率 |
| 使われるか | 引用・言及される回数 (policy paper, 業界メディア) |
| 信頼されるか | 独立 reviewer からの指摘 / 訂正要請の少なさ |
| 裏目に出ないか | 名誉毀損クレーム / 炎上 ゼロ |

---

**次ステップ候補**:
1. Person Parameter Card の骨組み実装 (Phase A) を先行
2. または 新卒離職因果分解 (1.1) のパイロット (既存 DML 流用で素早く結果を見る)
3. スタジオ Benchmark Card のデータ層整備 (feat_studio_affiliation のメトリクス追加)

推奨: **Phase A → 1.1 の順**。Phase A は全分析の基盤、1.1 は社会的インパクトが最大で既存コード流用度も高い。
