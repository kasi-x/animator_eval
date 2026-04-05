# 計算ロジック監査 TODO

本文書は、プロジェクト全体の計算ロジックを監査し、問題点と理想の姿を定義したものである。

---

## 第1部: 理想の姿

### 設計原則 (CLAUDE.md より)

> "scores reflect network position and collaboration density, never subjective 'ability' judgments"

> "When presenting as compensation evidence, confidence intervals are required"

### 現状の根本問題: anime.score 汚染

**anime.score (AniList/MAL 視聴者評価) がシステムの事実上の ground truth として全構成要素に浸透している。** これは視聴者の主観的人気評価であり、制作スタッフの貢献度とは独立した要因 (原作人気、マーケティング、配信プラットフォーム、放送時期) に大きく左右される。「ネットワーク位置と協業密度のみを反映」という設計原則と根本的に矛盾する。

#### anime.score 汚染マップ (現状)

```
anime.score (視聴者評価)
│
├─→ AKM アウトカム変数 y                    [person_fe の基盤: IV の 30%]
│     akm.py:274,290
│
├─→ グラフエッジ重み (_work_importance)       [Authority, Trust, BiRank の基盤]
│     graph.py:182-185
│
├─→ Skill (OpenSkill) の試合結果             [Layer 1 の第3軸]
│     skill.py:45-47,86
│
├─→ Patronage 品質 (Quality_id)             [IV の 20%]
│     patronage_dormancy.py:78-90
│
├─→ IV ラムダ最適化ターゲット                [全5構成要素の重み決定]
│     integrated_value.py:129-131,172-176
│
├─→ Temporal PageRank エッジ重み             [時間的権威指標]
│     temporal_pagerank.py:140
│
├─→ Independent value の因果推定アウトカム    [Layer 2 補償根拠]
│     individual_contribution.py:433-434,486-495
│
├─→ Opportunity residual の統制変数          [Layer 2 補償根拠]
│     individual_contribution.py:120-122,276
│
├─→ Consistency フォールバック               [Layer 2 補償根拠]
│     individual_contribution.py:367-368
│
├─→ Work impact の 30/100 点                [作品影響力評価]
│     work_impact.py:56-57
│
├─→ Anime value (商業価値 50%, 批評価値 60%) [作品価値評価]
│     anime_value.py:90-92,124-126
│
├─→ Team composition の「成功」定義          [チーム構成分析]
│     team_composition.py:84-88
│
├─→ Genre affinity のティア分類              [ジャンル親和性]
│     genre_affinity.py:16-24
│
├─→ Milestones の「最高評価作品」            [キャリアマイルストーン]
│     milestones.py:51-58
│
├─→ Expected ability の重み                  [期待能力]
│     expected_ability.py:113
│
└─→ Anime prediction の予測対象              [スコア予測モジュール]
      anime_prediction.py:60-97
```

anime.score を使っていない「クリーン」なモジュール:
- `trust.py` — 監督の作品数のみ
- `circles.py` — 共有作品数のみ
- `bridges.py` — コミュニティサイズ×グラフトポロジー
- `mentorship.py` — キャリアステージ差×共同クレジット数
- `network_evolution.py` — エッジ・ノード数の時系列
- `collab_diversity.py` — 共同クレジット頻度のシャノンエントロピー
- `career.py` — 役割ステージ×活動年
- `normalize.py` — 数学的スケーリング
- `clusters.py` — Louvain コミュニティ検出
- `knowledge_spanners.py` (AWCC/NDI) — グラフトポロジーのみ

### 理想の計算体系

anime.score を完全に排除し、**観測可能な構造データのみ**からスコアを構築する。

#### 使用可能なデータソース

| データ | 性質 | 例 |
|--------|------|---|
| **クレジット記録** | 客観的事実 | 人物 A が作品 X で key_animator としてクレジット |
| **役割** | 客観的事実 | 24種の役職分類 |
| **作品メタデータ** | 客観的事実 | エピソード数、放送期間、フォーマット (TV/映画/OVA) |
| **スタジオ情報** | 客観的事実 | 制作スタジオ、共同制作関係 |
| **時系列** | 客観的事実 | 活動年、キャリア期間、ブランク |
| **共演関係** | 構造データ | 誰と誰が同じ作品でクレジット |
| **ネットワーク位置** | 導出データ | 中心性、ブリッジ、コミュニティ構造 |

#### 使用してはいけないデータ

| データ | 理由 |
|--------|------|
| anime.score (視聴者評価) | 主観的、制作貢献と無関係な要因に左右される |
| anime.popularity | 同上 |
| 外部レビュー/批評点 | 主観的判断 |

#### 理想の Layer 1: Network Profile

| 軸 | 現状 | 理想 |
|---|---|---|
| **Authority** | PageRank (anime.score 重み付きグラフ) | PageRank (構造重みのみ: `role_weight × episode_coverage × duration_mult`) |
| **Trust** | 累積エッジ重み (anime.score 汚染) | 累積エッジ重み (構造重みのみ) |
| **Skill** | OpenSkill (anime.score ランク) | **廃止** → クレジット密度×役割進行度メトリクスに置換。または OpenSkill の「試合結果」をスタッフ規模・エピソード数ベースの制作規模ランキングに変更 |

#### 理想の AKM

| 要素 | 現状 | 理想 |
|---|---|---|
| **アウトカム y** | anime.score | **制作規模指標**: `log(staff_count) × log(episodes) × duration_mult` — 大規模制作に呼ばれること自体が業界での評価を反映 |
| **統制変数** | experience, role_weight | experience, role_weight, **ジャンル FE**, **フォーマット FE**, **年度 FE** |
| **person_fe の解釈** | 「同じスタジオで同じ役割の同僚と比べてどれだけ高評価作品に出るか」 | 「同じスタジオで同じ条件の同僚と比べてどれだけ大規模制作に呼ばれるか」 |

#### 理想の Patronage

| 要素 | 現状 | 理想 |
|---|---|---|
| **式** | `PR_d × log(1+N) × Quality` | `PR_d × log(1+N)` (Quality 項を削除。頻度と監督プレスティッジのみで十分) |
| **監督プレスティッジ** | BiRank (循環的) | person_fe (AKM 固定効果) または 監督の指名回数 |

#### 理想の IV ラムダ最適化

| 要素 | 現状 | 理想 |
|---|---|---|
| **ターゲット** | mean(anime.score) | **外部検証不要**: prior 重みをそのまま使用し CV を廃止。または held-out person_fe の予測精度を使用 |
| **代替案** | — | ターゲットを「翌年の制作規模指標」にして時間的外部性を確保 |

#### 理想の Layer 2: Individual Contribution Profile

| 指標 | 現状 | 理想 |
|---|---|---|
| **peer_percentile** | iv_score のコホート内パーセンタイル | person_fe のコホート内パーセンタイル (iv_score からの循環を断つ) |
| **opportunity_residual** | OLS で avg_anime_score を統制 | OLS で **avg_staff_count**, **avg_studio_fe**, **career_years**, **role** のみを統制 |
| **consistency** | AKM 残差の exp(-std) / CV | AKM 残差の **正規化 CV** (`std / |mean|`) に統一 |
| **independent_value** | anime.score の with/without 比較 | **構造指標の with/without 比較**: コラボレーターの BiRank やクレジット密度が pid の有無で変化するか |

#### 理想の信頼区間

| 要素 | 現状 | 理想 |
|---|---|---|
| **方法** | ヒューリスティック幅 `scale × 0.5 × (1-conf)` | **解析的 SE**: `person_fe_se = σ_resid / sqrt(n_obs)` → `CI = θ ± t_{α/2} × SE` |
| **ブートストラップ** | 残差平均の CI (≈ 0) | **クラスタブートストラップ**: スタジオ単位でリサンプルし AKM を再推定 |

---

## 第2部: 実装エラー・バグ

anime.score 排除とは独立して存在する、コードレベルのバグ。

---

### B01. [実装エラー] Phase 5/6/7 で studio_exposure が3回異なる式で計算

**TAG:** `STUDIO-EXPOSURE-INCONSISTENCY`
**File:**
- Phase 5: `src/analysis/integrated_value.py:84-86` (年数加重平均)
- Phase 6: `src/pipeline_phases/supplementary_metrics.py:90-96` (ユニークスタジオ単純平均)
- Phase 7: `src/pipeline_phases/result_assembly.py:55-60` (年数加重平均)

Phase 6 が最終 `context.iv_scores` を上書きするため、スコア出力に直接影響。

**修正:** Phase 6 でも `compute_studio_exposure()` を呼び出す。

---

### B02. [実装エラー] Phase 6 の IV 再計算が component_std/mean を未適用

**TAG:** `IV-RENORMALIZATION-MISSING`
**File:** `src/pipeline_phases/supplementary_metrics.py:102-110`

Phase 5 で正規化 (z-score) して重み付けしたコンポーネントを、Phase 6 では正規化なしで再計算。異なる変換空間のスコアが混在。

**修正:** `context` に `component_std`/`component_mean` を保持し、Phase 6 でも渡す。

---

### B03. [実装エラー] AKM `_build_panel` で `anime.studios[0]` を使うが `studio_assignments` と不一致

**TAG:** `AKM-STUDIO-MISMATCH`
**File:** `src/analysis/akm.py:282-283`

パネル回帰のスタジオラベルと `_redistribute_studio_fe` のスタジオラベルが共同制作で乖離。

**修正:** `_build_panel` でも `studio_assignments` を参照。

---

### B04. [実装エラー] BiRank の更新順序が Gauss-Seidel 型

**TAG:** `BIRANK-UPDATE-ORDER`
**File:** `src/analysis/birank.py:139-142`

`u_new` に `p_new` (更新済み) を使用。収束点が理論値と異なる。

```python
p_new = alpha * (T @ u) + (1 - alpha) * p_0
u_new = beta * (S.T @ p_new) + (1 - beta) * u_0  # ← p_new を使用
```

**修正:** `u_new = beta * (S.T @ p) + (1 - beta) * u_0`

---

### B05. [実装エラー] closeness_centrality に類似度重みを距離として渡している

**TAG:** `CLOSENESS-WEIGHT-INVERTED`
**File:** `src/analysis/graph.py:758-765`

`distance="weight"` → 強い関係ほど「遠い」と判定。全 closeness 値が逆転。

**修正:** `distance` を削除するか、`1/weight` 変換を追加。

---

### B06. [実装エラー] `opportunity_residual` のダミー変数: 1ロール時に零列が残りランク欠損

**TAG:** `OLS-ZERO-DUMMY`
**File:** `src/analysis/individual_contribution.py:270-271`

`n_roles = max(len(roles) - 1, 1)` → 1ロール時に全零ダミー列 → 自由度を消費し studentized residual が歪む。

**修正:** `max(len(roles) - 1, 0)` に変更。

---

### B07. [実装エラー] `compute_independent_value` で pid 自身の IV がプロジェクト品質から未除外

**TAG:** `INDEPENDENT-VALUE-SELF-INCLUSION`
**File:** `src/analysis/individual_contribution.py:480-499`

`anime_iv_sum - collab_iv` で collab のみ除外、pid 自身が残存。with-X 作品の residual が pid の貢献を過小評価。

**修正:** `total` から pid の IV も減算、`count` も -1。

---

### B08. [実装エラー] 信頼区間のスケール前提不一致

**TAG:** `CONFIDENCE-SCALE`
**File:** `src/analysis/confidence.py:54-81`

`scale=100.0` 前提だが iv_score は ~-1〜+2。margin=50 → CI が [0, 100] にクランプ。法的要件違反。

**修正:** パーセンタイル変換後のスコアに対して計算するか、各軸の実測値域を渡す。

---

### B09. [実装エラー] ブートストラップ CI が残差平均 (≈0) の CI を計算

**TAG:** `BOOTSTRAP-WRONG-TARGET`
**File:** `src/analysis/confidence.py:84-118`

`person_fe` の CI と称しているが、AKM 残差の平均をブートストラップしている。OLS 構造上 ≈0。

**修正:** `person_fe_se = σ_resid / sqrt(n_obs)` による解析的 SE に変更。

---

### B10. [実装エラー] `compute_consistency` の AKM パスと CV パスでスケール非互換

**TAG:** `CONSISTENCY-SCALE-MISMATCH`
**File:** `src/analysis/individual_contribution.py:381-383`

AKM: `exp(-std)` (非正規化)、フォールバック: `1 - std/|mean|` (正規化済み)。切り替え時に不連続ジャンプ。

**修正:** 両パスを統一定義 (正規化 CV) に揃える。

---

### B11. [実装エラー] `_episode_weight_for_pair` の Jaccard 前提不整合

**TAG:** `EPISODE-WEIGHT-JACCARD`
**File:** `src/analysis/graph.py:241-243`

一方が固定推定値 `min(26/N, 1.0)` で集合独立性前提を満たさない。`total_episodes is None` のフォールスルー (`return 1.0`) もオーバーカウント。

**修正:** 期待重複率 `known_frac × unknown_frac` を直接返す。

---

### B12. [実装エラー] Studio retention に Jaccard を使用 — 拡大と縮小が同値

**TAG:** `STUDIO-RETENTION-JACCARD`
**File:** `src/analysis/studio_timeseries.py:122-128`

`|A∩B|/|A∪B|` は retention 率ではない。前年全員継続+新規90人でも retention=0.1。

**修正:** `len(intersection) / len(prev_staff)` に変更。

---

### B13. [実装エラー] Milestones の `top_anime` がグローバル最高評価を使用 — 個人別でない

**TAG:** `MILESTONES-GLOBAL-MAX`
**File:** `src/analysis/milestones.py:51-58`

`best_anime_id` が全 anime_map でグローバルに決定されるため、その作品にクレジットされた1人のみが milestone を得る。個人ごとの最高作品であるべき。

**修正:** 個人ごとのクレジット作品内で最高を選択するよう変更。

---

### B14. [実装エラー] `compute_retrospective_potential` が常に current_score を返す (no-op)

**TAG:** `RETROSPECTIVE-NOOP`
**File:** `src/analysis/community_detection.py:535-545`

`future_peak = current_score` → `score_gap = 0` → 常に `return current_score`。`avg_retrospective_potential` が `avg_ability_at_formation` と同一になる。

**修正:** 実装するか、フィールドを削除。

---

### B15. [実装エラー] `compute_studio_exposure` の `akm_result` パラメータがデッドコード

**TAG:** `STUDIO-EXPOSURE-DEAD-CODE`
**File:** `src/analysis/integrated_value.py:64-69`

`akm_result` が渡されても `assignments = {}` (空dict) が設定され、実際のデータは抽出されない。

**修正:** `akm_result` から抽出するか、パラメータを削除。

---

### B16. [実装エラー] Mentorship の `_compute_confidence` で年数スパンがカウントに

**TAG:** `MENTORSHIP-YEAR-COUNT`
**File:** `src/analysis/mentorship.py:111-119`

`year_count = len(years)` (共演年の数) をスパン代理に使うが、10年間で2年だけ共演しても2年間で2年共演しても同値。

**修正:** `max(years) - min(years)` をスパンとして渡す。

---

## 第3部: 設計上の疑念・正当化困難

---

### D01. [正当化困難] ロール重み (`COMMITMENT_MULTIPLIERS`) の根拠不在

**TAG:** `ROLE-WEIGHT-JUSTIFICATION`
**File:** `src/utils/config.py:40-93`

`direction=3.0`, `animation_supervision=2.8`, `in_between=0.5` 等の数値に参照文献・業界調査が存在しない。公益目的の補償根拠として証拠能力を持たない。

**修正:** 業界給与データ・クレジット統計の根拠を記録する。

---

### D02. [正当化困難] `contribution_attribution.py` のロール貢献度も根拠不在

**TAG:** `CONTRIBUTION-WEIGHT-JUSTIFICATION`
**File:** `src/analysis/contribution_attribution.py:48-64`

`DIRECTOR=0.20`, `KEY_ANIMATOR=0.06`, `IN_BETWEEN=0.01` 等。

---

### D03. [正当化困難] エッジ重み `commit_a × commit_b` の乗算が二次膨張

**TAG:** `EDGE-WEIGHT-QUADRATIC`
**File:** `src/analysis/graph.py:354, 413`

監督同士のエッジが監督×動画の9倍。`create_director_animator_network` は算術平均使用で非一貫。

**修正:** 幾何平均 `sqrt(a×b)` への統一を検討。

---

### D04. [正当化困難] 休眠ペナルティ `decay_rate=0.5` はドキュメントの `0.3` と不一致かつ急速すぎ

**TAG:** `DORMANCY-DECAY-RATE`
**File:** `src/analysis/patronage_dormancy.py:97-143`
**参照:** `CALCULATION_COMPENDIUM.md` §2.4 (`δ=0.3`)

5年ブランクで 78% 減 (code) vs 59% 減 (doc)。

---

### D05. [正当化困難] AKM κ 下限=2.0, 上限=50.0 に理論的根拠なし

**TAG:** `AKM-KAPPA-BOUNDS`
**File:** `src/analysis/akm.py:647`

---

### D06. [正当化困難] AKM studio FE redistribution の α 推定に形式的同定論証なし

**TAG:** `AKM-REDISTRIBUTION-ALPHA`
**File:** `src/analysis/akm.py:527-557`

---

### D07. [正当化困難] IV の L2 正則化 `l2_alpha=0.5` が強すぎて CV が形骸化

**TAG:** `IV-LAMBDA-REGULARIZATION`
**File:** `src/analysis/integrated_value.py:243-269`

Prior の ±10-15% しか動かせない。事実上 prior 固定。

---

### D08. [疑念] AKM 処理順序: shrinkage → debias → redistribution の非可換チェーン

**TAG:** `AKM-PROCESSING-ORDER`
**File:** `src/analysis/akm.py:890-929`

Shrinkage 後に redistribution で studio FE 分を追加すると shrinkage 効果が部分的に無効化。

---

### D09. [疑念] `_debias_by_obs_count` が任意の負の slope で発動 — 有意性検定なし

**TAG:** `DEBIAS-NO-SIGNIFICANCE`
**File:** `src/analysis/akm.py:399-410`

slope=-0.001 でも発動。真の経済現象と機械的アーティファクトの判別がない。

---

### D10. [疑念] `career_capital` の乗法的形式で保護閾値 0.7 がほぼ到達不能

**TAG:** `CAREER-CAPITAL-THRESHOLD`
**File:** `src/analysis/patronage_dormancy.py:253-260`

10年 stage3 の中堅で `0.8 × 0.33 × 0.50 = 0.133`。保護が事実上機能しない。

**修正:** 加算的形式 (加重平均) か閾値引き下げ。

---

### D11. [疑念] Synergy score で n=2 → n=3 間に 4.6 倍の不連続ジャンプ

**TAG:** `SYNERGY-DISCONTINUITY`
**File:** `src/analysis/synergy_score.py:241-243`

**修正:** `log1p(n) * quality` に統一。

---

### D12. [疑念] ロールグループ分類不整合 — CHIEF_ANIMATION_DIRECTOR

**TAG:** `ROLE-GROUP-INCONSISTENCY`
**File:** `src/utils/role_groups.py:21-27`

`DIRECTOR_ROLES` に作画監督が含まれるが `ROLE_CATEGORY` では `animation_supervision`。

---

### D13. [疑念] Compatibility でスタッフ50人超の作品を完全除外 — 大作バイアス

**TAG:** `COMPATIBILITY-LARGE-WORK-EXCLUSION`
**File:** `src/analysis/compatibility.py:97-98`

**修正:** `CORE_TEAM_ROLES` でフィルタして処理。

---

### D14. [疑念] peer_percentile は iv_score の単なるパーセンタイル変換

**TAG:** `PEER-PERCENTILE-CIRCULAR`
**File:** `src/analysis/individual_contribution.py:155-214`

Layer 2 が Layer 1 の派生のみで構成され、「独立した評価軸」になっていない。

---

### D15. [疑念] 低 mover 率フォールバックで studio_exposure が暗黙ゼロ化

**TAG:** `AKM-LOW-MOVER-FALLBACK`
**File:** `src/analysis/akm.py:809-822`

`studio_fe = {}` → IV の5構成要素のうち1つが消失するが重みは未調整。

---

### D16. [疑念] `expected_ability` で collaborator IV を説明変数に使うが IV は person_fe の関数

**TAG:** `EXPECTED-ABILITY-SIMULTANEITY`
**File:** `src/analysis/expected_ability.py:106-152`

同時性バイアス。OLS 係数が因果的に解釈不能。

---

### D17. [疑念] Director は Patronage = 0 (構造的非対称、文書化なし)

**TAG:** `DIRECTOR-ZERO-PATRONAGE`
**File:** `src/analysis/patronage_dormancy.py:72-80`

---

### D18. [疑念] Patronage に BiRank を使うことで循環依存

**TAG:** `PATRONAGE-BIRANK-CIRCULAR`
**File:** `src/pipeline_phases/core_scoring.py:81-83`

BiRank → Patronage → IV → (BiRank に間接影響)。

---

### D19. [要説明] パーセンタイル計算のタイ処理と1人コホート

**TAG:** `PERCENTILE-TIES`
**File:** `src/pipeline_phases/post_processing.py:33-37`

1人コホートに 100.0 を付与。最小コホートサイズの閾値が必要。

---

### D20. [要説明] `opportunity_residual` の `unique_studios` が機会の代理指標として曖昧

**TAG:** `OPPORTUNITY-RESIDUAL-PROXY`
**File:** `src/analysis/individual_contribution.py:126-129`

---

### D21. [要説明] AKM の mover_fraction < 10% 閾値の根拠

**TAG:** `AKM-MOVER-THRESHOLD`
**File:** `src/analysis/akm.py:809-822`

---

### D22. [要調査] Label Propagation にシード固定なし — 大規模グラフで再現性喪失

**TAG:** `LABEL-PROPAGATION-SEED`
**File:** `src/pipeline_phases/graph_construction.py:44-52`

---

### D23. [要調査] `_run_tags` が並列スレッドから `context.results` の dict に書き込み

**TAG:** `TAGS-THREAD-SAFETY`
**File:** `src/pipeline_phases/analysis_modules.py:183-188`

---

### D24. [要調査] IV 最適化の時系列 CV でターゲットリーク

**TAG:** `IV-CV-TARGET-LEAK`
**File:** `src/analysis/integrated_value.py:196-213`

人物単位 fold だが y と特徴量が全年データを含む。

---

### D25. [要説明] API が生スコアを文脈・免責事項なしで公開

**TAG:** `API-RAW-SCORE-EXPOSURE`
**File:** `src/api.py:273-280`

---

### D26. [要調査] `VOICE_ACTOR`, `THEME_SONG`, `ADR` が `ROLE_CATEGORY` 未登録

**TAG:** `ROLE-CATEGORY-MISSING`
**File:** `src/utils/role_groups.py:117-176`

---

### D27. [要説明] `anime_value.py` のスケールバグ疑惑: score/100 vs score/10

**TAG:** `ANIME-VALUE-SCALE`
**File:** `src/analysis/anime_value.py:90-92, 124-126`

`anime.score / 100` だが他モジュールは `anime.score / 10.0`。Anime.score が 0-10 スケールなら commercial_value の external_score が常に ≤ 0.1 に崩壊。

---

## 第4部: 修正ロードマップ

### Phase A: anime.score 排除 (最優先)

1. **`_work_importance` からスコア乗数を削除** → 構造重みのみ (`duration_mult × role_weight`)
2. **AKM のアウトカム変数を構造指標に変更** → `log(staff_count) × log(episodes) × duration_mult`
3. **Patronage から Quality 項を削除** → `PR_d × log(1+N)` のみ
4. **Skill を構造メトリクスに置換** → 制作規模ランキングまたはクレジット密度×役割進行度
5. **IV ラムダ最適化のターゲット変更** → prior 固定化または構造指標ターゲット
6. **Layer 2 の全指標から anime.score 依存を除去**
7. **表示用モジュール (anime_stats, growth, time_series) の anime.score は情報として残しても可** — スコアリングに使わない限り問題なし

### Phase B: 実装エラー修正

1. B01-B02 (studio_exposure + IV 再計算) — スコア正確性に直結
2. B04 (BiRank 更新順序)
3. B05 (closeness 重み逆転)
4. B08-B09 (信頼区間) — 法的要件
5. 残りの B03, B06, B07, B10-B16

### Phase C: 設計改善

1. D01-D02 (重み根拠の文書化)
2. D03-D07 (マジックナンバーの正当化・感度分析)
3. D08-D24 (疑念・要調査項目の検証)
