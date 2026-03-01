# 計算ロジック監査 TODO (詳細版)

本文書は、プロジェクト全体の計算ロジックを監査し、問題点と理想の姿を定義したものである。
各項目にはコード引用、影響範囲、具体的修正案を含む。

カテゴリ凡例:
- `[実装エラー]` — コードのバグ・不整合。修正が明確
- `[疑念]` — 設計意図は理解できるが、結果の妥当性に疑問
- `[正当化困難]` — マジックナンバーや恣意的選択で根拠が不在
- `[要説明]` — ドキュメント不足。根拠を明記すべき
- `[要調査]` — 実データでの影響を検証してから判断すべき

---

# 第1部: 理想の姿

## 設計原則 (CLAUDE.md)

> "scores reflect network position and collaboration density, never subjective 'ability' judgments"
> "When presenting as compensation evidence, confidence intervals are required"

## 根本問題: anime.score 汚染

**anime.score (AniList/MAL 視聴者評価) がシステムの事実上の ground truth として全構成要素に浸透している。**
視聴者評価は制作スタッフの貢献度とは独立した要因 (原作人気、マーケティング、配信プラットフォーム、放送時期) に大きく左右される。
CLAUDE.md の設計原則と根本的に矛盾する。

### anime.score 汚染マップ

以下に、anime.score がシステムに入り込む全16経路を示す。

#### 経路 1: AKM アウトカム変数 — person_fe の基盤

`src/analysis/akm.py:274,290`

AKM は `anime.score` を従属変数 `y` として回帰する。`person_fe` (個人固定効果) は
「同じスタジオの同僚と比べてどれだけ**高評価作品**に出るか」を測定しており、
ネットワーク構造ではなく視聴者人気への貢献度を測定している。

`person_fe` は IV の 30% を占める最重要構成要素であり、汚染の影響が最も大きい。

```python
# akm.py:274 — スコアなし作品を除外 (系統的バイアス)
if not anime or not anime.year or anime.score is None:
    continue

# akm.py:290 — anime.score をアウトカム y として使用
pa_data[key] = (anime.score, w, anime.year, studio, cw)

# akm.py:327 — パネル回帰の y ベクトルに格納
obs_y.append(score)  # score = anime.score
```

**影響範囲:** `person_fe` → `iv_score` (30% weight) → `peer_percentile`, `opportunity_residual`, `consistency` → 全ての最終スコア

**除外時の代替:** 制作規模指標 `log(staff_count) × log(episodes) × duration_mult` を y として使用。大規模制作に呼ばれること自体が業界での評価を反映する。

---

#### 経路 2: グラフエッジ重み — Authority, Trust, BiRank の基盤

`src/analysis/graph.py:182-185`

`_work_importance()` が anime.score をエッジ重みの乗数として使用。
コラボレーショングラフの全エッジに影響し、そこから計算される PageRank (Authority)、
累積エッジ重み (Trust)、BiRank の全てに波及する。

スコア 9.0 の作品でのコラボは、スコア 1.0 の作品の **9倍** の重みを持つ。

```python
# graph.py:182-185
if anime is None or anime.score is None:
    score_mult = 0.5                       # スコア不明 → 0.5 (根拠なし)
else:
    score_mult = max(anime.score / 10.0, 0.1)  # 0.1-1.0 の範囲

# graph.py:354 — エッジ重み計算 (全ペアに適用)
new_weight += commit_a * commit_b * ep_w * importance  # importance に score_mult を含む
```

**影響範囲:** グラフ構造 → PageRank, BiRank (15% weight), betweenness, closeness → 全ての中心性指標

**除外時の代替:** `importance = duration_mult` のみ使用。品質ではなく制作規模のみで重み付け。

---

#### 経路 3: Skill (OpenSkill) の試合結果 — Layer 1 第3軸

`src/analysis/skill.py:45-47,86`

anime.score で年ごとにアニメをランキングし、OpenSkill の「試合結果」として使用。
高評価作品のスタッフは「勝者」、低評価作品のスタッフは「敗者」として扱われる。

モジュール docstring が明言: `"高評価作品に多く参加しているアニメーターほど高スキルと推定"`

```python
# skill.py:1-8 (docstring)
"""
作品のスコア（MAL/AniList 評点）を「試合結果」と見立て、
各アニメーターのスキルレーティングを算出する。
"""

# skill.py:45-47 — anime.score を記録
if anime.score and anime.score > 0:
    record = ScoredAnimeRecord(anime_id=anime_id, score=anime.score, year=year)

# skill.py:86 — 年ごとにスコア降順でランク付け → OpenSkill の試合順位
yearly_anime_records.sort(key=lambda record: record.score, reverse=True)
```

**除外時の代替:** Skill 軸を廃止し、「クレジット密度 × 役割進行度」メトリクスに置換。
または OpenSkill の試合結果を制作規模 (staff_count × episodes) ランキングに変更。

---

#### 経路 4: Patronage Premium の Quality 項 — IV の 20%

`src/analysis/patronage_dormancy.py:78-91`

`Π_i = Σ_d (PR_d × log(1+N_id) × Quality_id)` の `Quality_id` が
監督との共同作品の **平均 anime.score**。

低評価作品で高名な監督と働いても patronage がほぼゼロになる。
関係の「質」を視聴者人気で定義している。

```python
# patronage_dormancy.py:78 — anime.score を品質として蓄積
anime_score = anime.score if anime.score is not None else 0.0
for dir_id in anime_directors.get(c.anime_id, set()):
    person_director_collabs[c.person_id][dir_id].append(anime_score)

# patronage_dormancy.py:88-91 — patronage 計算
quality = sum(scores) / len(scores) if scores else 0.0
total += pr_d * log1p(n_collabs) * quality  # quality = mean(anime.score)
```

**除外時の代替:** `quality` 項を削除し `Π_i = Σ_d (PR_d × log(1+N_id))` とする。
頻度と監督プレスティッジのみで十分。

---

#### 経路 5: IV ラムダ最適化ターゲット — 全5構成要素の重み決定

`src/analysis/integrated_value.py:129-131,172-176`

CV 最適化が `mean(anime.score per person)` を最小化対象にしている。
全5構成要素の重みが「アニメの視聴者評価をどれだけ予測できるか」で調整される。

```python
# integrated_value.py:129-131 — anime.score を収集
if anime and anime.score is not None and anime.year is not None:
    person_anime_scores[c.person_id].append(
        (c.anime_id, anime.year, anime.score)
    )

# integrated_value.py:172-176 — y = 人物ごとの平均 anime.score
y_raw = np.array([
    np.mean([s for _, _, s in person_anime_scores[pid]])
    for pid in person_list
], dtype=np.float64)
```

**除外時の代替案:**
(a) prior 重みをそのまま使用し CV を廃止
(b) held-out person_fe の予測精度をターゲットに
(c) 翌年の制作規模指標をターゲットに (時間的外部性を確保)

---

#### 経路 6: Temporal PageRank のエッジ重み

`src/analysis/temporal_pagerank.py:140`

同僚間のピアエッジが anime.score で重み付けされる。

```python
# temporal_pagerank.py:140
importance = max(anime.score / 10.0, 0.1) if (anime and anime.score) else 0.5
w = peer_edge_weight * importance
```

---

#### 経路 7: Independent value のアウトカム — Layer 2 補償根拠

`src/analysis/individual_contribution.py:433-434,486-495`

「person X がいるときの方がコラボレーターの成績が良いか」を測定するが、
「成績」が anime.score。Layer 2 は「compensation basis」として設計されており、
CLAUDE.md の法的要件に直結する。

```python
# individual_contribution.py:433-434
if anime and anime.score:
    person_anime_scores[c.person_id][c.anime_id] = anime.score

# individual_contribution.py:492-495 — with/without 比較
resid = work_score - proj_quality  # work_score = anime.score
if aid in pid_anime:
    with_x_resids.append(resid)
else:
    without_x_resids.append(resid)
```

---

#### 経路 8: Opportunity residual の統制変数

`src/analysis/individual_contribution.py:120-122,276`

OLS 回帰の統制変数 `avg_anime_score` が anime.score の平均。
被説明変数 `iv_score` 自体が anime.score に依存しているため二重循環。

```python
# individual_contribution.py:276
X[i, 1] = f["avg_anime_score"]  # 統制変数として使用
```

---

#### 経路 9: Consistency フォールバック

`src/analysis/individual_contribution.py:367-368`

AKM 残差が利用できない場合、生の anime.score で一貫性を計算。

```python
# individual_contribution.py:367-368
if anime.score:
    person_work_values[pid].append(anime.score)
```

---

#### 経路 10: Work impact の 30/100 点

`src/analysis/work_impact.py:56-57`

4要素の影響度スコアのうち最大要素 (30点) が anime.score。

```python
# work_impact.py:56-57
score_factor = (anime.score / 10 * 30) if anime.score else 10
# max: 30 + 25 + 25 + 20 = 100
```

---

#### 経路 11: Anime value (商業価値 50%, 批評価値 60%)

`src/analysis/anime_value.py:90-92,124-126`

`compute_commercial_value` で `external_score = anime.score / 100` を 50% の重みで使用。
`compute_critical_value` で `critic_score = anime.score / 100` を 60% の重みで使用。

**スケールバグ疑惑:** 他モジュールは `anime.score / 10.0` だが本モジュールは `/100`。
Anime.score が 0-10 スケールなら external_score は常に ≤ 0.1 に崩壊。

```python
# anime_value.py:90-92
if anime.score and anime.score > 0:
    external_score = min(1.0, anime.score / 100)   # ← /100 だが score は 0-10?
```

---

#### 経路 12: Team composition の「成功」定義

`src/analysis/team_composition.py:84-88`

`min_score=7.0` で「成功作品」を定義。「推奨ペア」が視聴者人気で決定される。

---

#### 経路 13: Genre affinity のティア分類

`src/analysis/genre_affinity.py:16-24`

`score >= 8.0` → `"high_rated"`, `score >= 6.5` → `"mid_rated"`, else `"low_rated"`。
プロファイルに保存される。コード内コメントで「ジャンルタグ利用可能まで代用」と明言。

---

#### 経路 14: Milestones の「最高評価作品」

`src/analysis/milestones.py:51-58`

`"最高評価作品参加"` というキャリアマイルストーンが anime.score のグローバル最大値で定義。

---

#### 経路 15: Expected ability の重み

`src/analysis/expected_ability.py:110-112`

協業者 IV の加重平均で、重みが `anime.score`。

```python
# expected_ability.py:110-112
w = anime.score if anime.score else 1.0
collab_ivs.append(avg_collab * w)
```

---

#### 経路 16: Anime prediction の予測対象

`src/analysis/anime_prediction.py:60-97`

モジュール全体が「チーム構成から視聴者評価を予測」する設計。能力判断そのもの。

---

### anime.score を使っていないクリーンなモジュール

| モジュール | 使用するデータ |
|-----------|-------------|
| `trust.py` | 監督の作品数のみ |
| `circles.py` | 共有作品数のみ |
| `bridges.py` | コミュニティサイズ × グラフトポロジー |
| `mentorship.py` | キャリアステージ差 × 共同クレジット数 |
| `network_evolution.py` | エッジ・ノード数の時系列 |
| `collab_diversity.py` | 共同クレジット頻度のシャノンエントロピー |
| `career.py` | 役割ステージ × 活動年 |
| `normalize.py` | 純粋な数学的スケーリング |
| `clusters.py` | Louvain コミュニティ検出 |
| `knowledge_spanners.py` (AWCC/NDI) | グラフトポロジーのみ |

---

## 理想の計算体系

### 使用可能なデータソース

| データ | 性質 | 例 |
|--------|------|---|
| **クレジット記録** | 客観的事実 | 人物 A が作品 X で key_animator としてクレジット |
| **役割** | 客観的事実 | 24種の職種分類 |
| **作品メタデータ** | 客観的事実 | エピソード数、放送期間、フォーマット (TV/映画/OVA) |
| **スタジオ情報** | 客観的事実 | 制作スタジオ、共同制作関係 |
| **制作規模** | 客観的事実 | スタッフ数 (クレジットから計算可能) |
| **時系列** | 客観的事実 | 活動年、キャリア期間、ブランク |
| **共演関係** | 構造データ | 誰と誰が同じ作品でクレジット |
| **ネットワーク位置** | 導出データ | 中心性、ブリッジ、コミュニティ構造 |

### 使用してはいけないデータ

| データ | 理由 |
|--------|------|
| anime.score (視聴者評価) | 主観的、制作貢献と無関係な要因に左右される |
| anime.popularity | 同上 |
| 外部レビュー・批評点 | 主観的判断 |

### 理想のスコア構成

#### Layer 1: Network Profile (3軸)

| 軸 | 現状 | 理想 |
|---|---|---|
| **Authority** | PageRank (anime.score 重み付きグラフ) | PageRank (構造重みのみ: `role_weight × episode_coverage × duration_mult`) |
| **Trust** | 累積エッジ重み (anime.score 汚染) | 累積エッジ重み (構造重みのみ) |
| **Skill** | OpenSkill (anime.score ランク) | 廃止 → クレジット密度 × 役割進行度メトリクス |

#### AKM (個人固定効果)

| 要素 | 現状 | 理想 |
|---|---|---|
| **アウトカム y** | anime.score | 制作規模指標: `log(staff_count) × log(episodes) × duration_mult` |
| **統制変数** | experience, role_weight | + ジャンル FE, フォーマット FE, 年度 FE |
| **解釈** | 「高評価作品への貢献度」 | 「大規模制作への指名度」 |

#### Patronage

| 要素 | 現状 | 理想 |
|---|---|---|
| **式** | `PR_d × log(1+N) × Quality` | `PR_d × log(1+N)` (Quality 削除) |
| **監督プレスティッジ** | BiRank (循環的) | person_fe または指名回数 |

#### IV ラムダ最適化

| 要素 | 現状 | 理想 |
|---|---|---|
| **ターゲット** | mean(anime.score) | prior 重み固定 (CV 廃止) |

#### Layer 2: Individual Contribution Profile

| 指標 | 現状 | 理想 |
|---|---|---|
| **peer_percentile** | iv_score のコホート内順位 | person_fe のコホート内順位 |
| **opportunity_residual** | OLS で avg_anime_score を統制 | OLS で avg_staff_count, avg_studio_fe のみ統制 |
| **consistency** | exp(-std) / CV (非互換) | 正規化 CV に統一 |
| **independent_value** | anime.score の with/without | BiRank/クレジット密度の with/without |

#### 信頼区間

| 要素 | 現状 | 理想 |
|---|---|---|
| **方法** | ヒューリスティック幅 | 解析的 SE: `θ ± t × σ/√n` |
| **ブートストラップ** | 残差平均 CI (≈0) | クラスタブートストラップで AKM 再推定 |

---

# 第2部: 実装エラー・バグ

anime.score 排除とは独立して存在する、コードレベルのバグ。

---

### B01. [実装エラー] Phase 5/6/7 で studio_exposure が3回異なる式で計算

**TAG:** `STUDIO-EXPOSURE-INCONSISTENCY`
**深刻度:** HIGH

**概要:**
`studio_exposure` (スタジオ固定効果への暴露度) が3つのフェーズでそれぞれ独立に計算されるが、
Phase 6 のみ異なる計算式を使用しており、最終スコアに不整合が生じる。

**Phase 5 (`core_scoring.py:98-102`) — 正規の計算:**
```python
studio_exposure = compute_studio_exposure(
    context.person_fe, context.studio_fe,
    studio_assignments=context.studio_assignments,
)
```
`compute_studio_exposure()` (`integrated_value.py:40-90`) は **年数加重平均** を使用:
```python
# integrated_value.py:84-86
exp_val = sum(
    studio_fe.get(studio, 0.0) * (years / total_years)
    for studio, years in studio_years.items()
)
```
10年間スタジオ A (FE=0.5) で5年、スタジオ B (FE=0.2) で5年 → `0.5×0.5 + 0.2×0.5 = 0.35`

**Phase 6 (`supplementary_metrics.py:90-96`) — 簡略版 (バグ):**
```python
studio_exposure = {
    pid: sum(
        context.studio_fe.get(s, 0.0)
        for s in set(context.studio_assignments.get(pid, {}).values())
    ) / max(len(set(context.studio_assignments.get(pid, {}).values())), 1)
    for pid in context.iv_scores_historical
    if context.studio_assignments.get(pid)
}
```
**ユニークスタジオの単純平均** — 時間重みなし。
9年間スタジオ A + 1年間スタジオ B → `(0.5 + 0.2) / 2 = 0.35` (偶然一致するが、一般には異なる)

**Phase 7 (`result_assembly.py:55-60`) — 正規の計算:**
```python
studio_exposure = compute_studio_exposure(
    context.person_fe, context.studio_fe,
    studio_assignments=context.studio_assignments,
)
```

**問題:**
Phase 6 の `compute_integrated_value()` 呼び出し (`supplementary_metrics.py:102-110`) が
`context.iv_scores` を上書きする。この値が最終出力に使用される。
Phase 7 の `studio_exposure` は `score.studio_fe_exposure` に格納されるが、IV には影響しない。
つまり **最終 IV スコアには Phase 6 の簡略版 studio_exposure が使われている**。

**修正:**
Phase 6 でも `compute_studio_exposure()` を呼び出す。`context` にキャッシュして再利用するのが理想。

---

### B02. [実装エラー] Phase 6 の IV 再計算が component_std / component_mean を未適用

**TAG:** `IV-RENORMALIZATION-MISSING`
**深刻度:** HIGH

**概要:**
Phase 5 の IV 計算では各コンポーネントを z-score 正規化してから重み付けするが、
Phase 6 のドーマンシー適用後の再計算では正規化パラメータが渡されず、
生スコアにラムダ重みが掛けられる。

**Phase 5 のフロー:**
```python
# core_scoring.py:108-119 — compute_integrated_value_full が正規化パラメータを計算
iv_result = compute_integrated_value_full(...)
# iv_result.component_std = {"person_fe": 0.42, "birank": 0.15, ...}
# iv_result.component_mean = {"person_fe": -0.03, "birank": 0.002, ...}

# core_scoring.py:127-136 — 正規化パラメータを渡して IV 計算
context.iv_scores = compute_integrated_value(
    ...,
    component_std=iv_component_std,      # ← 渡している
    component_mean=iv_component_mean,    # ← 渡している
)
```

**Phase 6 のフロー:**
```python
# supplementary_metrics.py:102-110 — 正規化パラメータを渡していない
context.iv_scores = compute_integrated_value(
    context.person_fe,
    context.birank_person_scores,
    studio_exposure,                     # ← Phase 6 の簡略版
    awcc_scores,
    context.patronage_scores,
    career_dormancy,
    context.iv_lambda_weights,
    # component_std=???    ← 未渡し!
    # component_mean=???   ← 未渡し!
)
```

**`compute_integrated_value` の内部動作:**
```python
# integrated_value.py:357-358 — component_std が None のとき除算をスキップ
for name, scores in components.items():
    val = scores.get(pid, 0.0)
    if component_std:
        val = val / component_std.get(name, 1.0)   # z-score 正規化
    if component_mean:
        val = val - component_mean.get(name, 0.0)   # センタリング
    raw += lambdas.get(name, 0.2) * val
```

Phase 5: `person_fe=0.5` → 正規化 → `0.5/0.42 - (-0.03) = 1.22` → ラムダ 0.3 適用 → 0.366
Phase 6: `person_fe=0.5` → **正規化なし** → `0.5` → ラムダ 0.3 適用 → 0.15

同じ person_fe=0.5 に対して **2.4倍の差** が生じる。

**修正:**
`context` に `iv_component_std`, `iv_component_mean` を保持し、Phase 6 で渡す。
```python
# core_scoring.py に追加
context.iv_component_std = iv_result.component_std
context.iv_component_mean = iv_result.component_mean

# supplementary_metrics.py を修正
context.iv_scores = compute_integrated_value(
    ...,
    component_std=context.iv_component_std,
    component_mean=context.iv_component_mean,
)
```

---

### B03. [実装エラー] AKM `_build_panel` で `anime.studios[0]` を使うが `studio_assignments` と不一致

**TAG:** `AKM-STUDIO-MISMATCH`
**深刻度:** HIGH

**概要:**
パネルデータ構築 (`_build_panel`) と、その後の studio FE redistribution (`_redistribute_studio_fe`)
で異なるスタジオラベルが使われている。

`infer_studio_assignment()` (`akm.py:64-105`) は加重投票で人物ごとに主スタジオを決定:
```python
# akm.py:87-88 — 共同制作スタジオに均等分配
per_studio_w = w / len(anime.studios)
for studio in anime.studios:
    weight_accum[(c.person_id, anime.year, studio)] += per_studio_w
```

しかし `_build_panel()` は先頭スタジオをハードコード:
```python
# akm.py:283 — 常に studios[0]
studio = anime.studios[0]
```

**具体例:**
アニメ X がスタジオ A (studios[0]) とスタジオ B の共同制作。
人物 P がスタジオ B のアニメーターとして多くのクレジットを持つ場合:
- `infer_studio_assignment` → P はスタジオ B に割り当て
- `_build_panel` → P の観測はスタジオ A に帰属
- `_redistribute_studio_fe` → P はスタジオ B として redistribution 計算

回帰とリディストリビューションのスタジオラベルが不一致。

**修正:**
`_build_panel` でも `studio_assignments` を参照:
```python
studio = studio_assignments.get(c.person_id, {}).get(anime.year, anime.studios[0])
```

---

### B04. [実装エラー] BiRank の更新順序が Gauss-Seidel 型

**TAG:** `BIRANK-UPDATE-ORDER`
**深刻度:** HIGH

**概要:**
BiRank (He et al. 2017) は Gauss-Jacobi 型 (同時更新) を前提とするが、
実装では `u_new` の計算に `p_new` (同一イテレーションで更新済み) を使用。
収束点が理論値と異なる。

```python
# birank.py:139-142
# Person scores: p = α·T·u + (1-α)·p₀
p_new = alpha * (T @ u) + (1 - alpha) * p_0

# Anime scores: u = β·Sᵀ·p + (1-β)·u₀
u_new = beta * (S.T @ p_new) + (1 - beta) * u_0
#                      ^^^^^ ← p_new (更新済み) を使用
```

**正しい実装:**
```python
p_new = alpha * (T @ u) + (1 - alpha) * p_0
u_new = beta * (S.T @ p) + (1 - beta) * u_0   # ← 古い p を使用
```

**影響:**
Gauss-Seidel 型は一般に収束が速いが、理論的に異なる固定点に収束する。
非対称な更新により person スコアが anime スコアに相対的に過大/過小評価される可能性がある。

---

### B05. [実装エラー] closeness_centrality に類似度重みを距離として渡している

**TAG:** `CLOSENESS-WEIGHT-INVERTED`
**深刻度:** HIGH

**概要:**
NetworkX の `closeness_centrality(distance="weight")` はエッジ重みを「距離」として扱う
(大きいほど遠い)。しかしこのグラフのエッジ重みは `commit_a × commit_b × importance`
で計算された「強い関係 = 大きい値」の類似度量。

```python
# graph.py:758-765
c = nx.closeness_centrality(subg, distance="weight")
```

結果: 最も強くつながっている人物が「最も遠い」と判定される。
closeness centrality の値が **全員逆転** している。

**修正案:**
(a) `distance="weight"` を削除 (無重みで計算)
(b) エッジに `distance = 1/weight` 属性を追加:
```python
for u, v, d in subg.edges(data=True):
    d["distance"] = 1.0 / max(d.get("weight", 1.0), 0.001)
c = nx.closeness_centrality(subg, distance="distance")
```

---

### B06. [実装エラー] OLS ダミー変数: 1ロール時に零列が残りランク欠損

**TAG:** `OLS-ZERO-DUMMY`
**深刻度:** MEDIUM

**概要:**
`compute_opportunity_residual()` のダミー変数行列で、ロールが1種類でも
ダミー列が1つ確保されるが、基底カテゴリ (ridx=0) はスキップされるため全零列になる。

```python
# individual_contribution.py:270-271
n_roles = max(len(roles) - 1, 1)  # ← 1ロール時に 1 列確保
X = np.zeros((n, 3 + n_roles))     # 4列: career_years, avg_anime_score, unique_studios, + 零列

# individual_contribution.py:278-280
ridx = role_to_idx.get(f["primary_role"], 0)
if ridx > 0 and ridx <= n_roles:   # ridx=0 はスキップ → 零列が残る
    X[i, 2 + ridx] = 1.0
```

零列はリッジ正則化 `1e-8 × I` で数値的には動くが、
自由度 p を1つ消費し、hat matrix diagonal `h_ii` が歪む。
studentized residual の算出 (`residuals[i] / (s × √(1 - h_ii))`) に影響。

**修正:**
```python
n_roles = max(len(roles) - 1, 0)  # 1ロール時は 0 列
```

---

### B07. [実装エラー] `compute_independent_value` で pid 自身の IV が未除外

**TAG:** `INDEPENDENT-VALUE-SELF-INCLUSION`
**深刻度:** MEDIUM

**概要:**
「コラボレーターの成績を、person X がいるときといないときで比較する」際に、
`anime_iv_sum` から collab の IV は引くが、pid (対象者) 自身の IV が残っている。

```python
# individual_contribution.py:444-451 — 全参加者の IV を合計 (pid も含む)
for c in credits:
    pid_c = c.person_id
    if pid_c in features:
        iv = features[pid_c]["iv_score"]
        anime_iv_sum[c.anime_id] += iv
        anime_iv_count[c.anime_id] += 1

# individual_contribution.py:487-489 — collab のみ除外、pid は残存
total = anime_iv_sum.get(aid, 0.0) - collab_iv
count = anime_iv_count.get(aid, 0) - 1
proj_quality = total / count if count > 0 else 0.0
# ↑ proj_quality に pid 自身の IV が含まれたまま
```

with-X 作品で `proj_quality` が pid の IV 分だけ膨らみ、`resid = work_score - proj_quality` が
pid の貢献を過小評価する方向にバイアスがかかる。

**修正:**
```python
total = anime_iv_sum.get(aid, 0.0) - collab_iv
if aid in pid_anime:
    total -= pid_iv  # pid 自身も除外
    count = anime_iv_count.get(aid, 0) - 2  # collab + pid
else:
    count = anime_iv_count.get(aid, 0) - 1  # collab のみ
```

---

### B08. [実装エラー] 信頼区間のスケール前提不一致

**TAG:** `CONFIDENCE-SCALE`
**深刻度:** HIGH (法的要件)

**概要:**
`compute_score_range()` は `scale=100.0` を前提とするが、
実際の各スコアのスケールは全く異なる。

```python
# confidence.py:75-77
max_margin = scale * 0.5     # = 50.0
margin = max_margin * (1.0 - confidence)
lower = max(0.0, score - margin)
upper = min(scale, score + margin)
```

各スコアの実際のスケール:
| スコア | 典型的範囲 | scale=100 での margin (conf=0.5) |
|--------|-----------|------|
| iv_score | -1 〜 +2 | ±25 → CI は [0, 27] — 無意味 |
| person_fe | -2 〜 +2 | ±25 → CI は [0, 27] — 無意味 |
| birank | 0 〜 0.01 | ±25 → CI は [0, 25.01] — 無意味 |
| patronage | 0 〜 5 | ±25 → CI は [0, 30] — 無意味 |

CLAUDE.md: 「補償根拠に使う場合は信頼区間が必須」→ 壊れた CI は要件違反。

呼び出し側 (`confidence.py:170-180`):
```python
r["score_range"] = {
    "iv_score": compute_score_range(r["iv_score"], conf),      # scale=100 (暗黙)
    "birank": compute_score_range(r["birank"], conf),          # scale=100 (暗黙)
    "patronage": compute_score_range(r["patronage"], conf),    # scale=100 (暗黙)
}
```

**修正:**
パーセンタイル変換後 (0-100) のスコアに対して計算するか、各軸の実測値域を渡す:
```python
"iv_score": compute_score_range(r["iv_score_pct"], conf, scale=100.0),
```

---

### B09. [実装エラー] ブートストラップ CI が残差平均 (≈0) の CI を計算

**TAG:** `BOOTSTRAP-WRONG-TARGET`
**深刻度:** HIGH (法的要件)

**概要:**
person_fe の CI と称しているが、AKM 残差 `ε_ij` の平均をブートストラップしている。
OLS の構造上、残差の平均はほぼ 0 であり、この CI は「person_fe がどれだけ不確実か」を
測定していない。

```python
# confidence.py:110-113 — 残差の平均をブートストラップ
boot_means = np.empty(n_bootstrap)
for b in range(n_bootstrap):
    sample = rng.choice(arr, size=n, replace=True)    # arr = AKM 残差のリスト
    boot_means[b] = np.mean(sample)                   # 残差の平均 ≈ 0

# confidence.py:115-118
lower = float(np.percentile(boot_means, alpha * 100))    # ≈ -0.01
upper = float(np.percentile(boot_means, (1 - alpha) * 100))  # ≈ +0.01
```

結果は `r["score_range"]["person_fe"] = (-0.01, 0.01)` のような小さな区間になり、
person_fe の実際の不確実性を過小評価する。

**正しい person_fe の CI:**
```python
# 解析的 SE (推奨)
se_i = sigma_resid / sqrt(n_obs_i)
ci_lower = person_fe_i - 1.96 * se_i
ci_upper = person_fe_i + 1.96 * se_i
```

---

### B10. [実装エラー] `compute_consistency` の AKM パスと CV パスでスケール非互換

**TAG:** `CONSISTENCY-SCALE-MISMATCH`
**深刻度:** MEDIUM

**概要:**
AKM 残差パスは `exp(-std)` (非正規化)、フォールバックパスは `1 - CV` (正規化済み)。
同じ人物がデータ量の変化でパスが切り替わると不連続ジャンプが発生する。

```python
# individual_contribution.py:381-383 — AKM パス
if akm_residuals:
    consistency = float(np.exp(-std))
    # std=0.5 → 0.607, std=1.0 → 0.368, std=2.0 → 0.135

# individual_contribution.py:386-389 — フォールバック
else:
    cv = std / abs(mean) if abs(mean) > 1e-10 else 0.0
    consistency = max(0.0, 1.0 - cv)
```

AKM 残差の std は anime.score のスケール (0-10) に依存し、
`exp(-std)` は std=3 で 0.05 まで落ちる (非常に厳しい)。
一方 CV パスは相対的指標で、mean が大きいと std=3 でも consistency が高くなりうる。

**修正:**
両パスを統一: `consistency = max(0.0, 1.0 - std / reference_scale)` で
`reference_scale` を残差の標準偏差の母集団平均として設定。

---

### B11. [実装エラー] `_episode_weight_for_pair` の Jaccard 前提不整合

**TAG:** `EPISODE-WEIGHT-JACCARD`
**深刻度:** LOW

**概要:**
一方がエピソードデータなし (推定値 `min(26/N, 1.0)`) の場合に
Jaccard 式 `overlap / union` を適用するが、推定値は集合の独立性前提を満たさない。

```python
# graph.py:241-243
return (known_frac * unknown_frac) / max(
    known_frac + unknown_frac - known_frac * unknown_frac, 0.001
)
```

また `total_episodes is None` のフォールスルー (`return 1.0`) は、
片方がデータを持つ場合でも1.0を返し、オーバーカウントになる。

---

### B12. [実装エラー] Studio retention に Jaccard を使用

**TAG:** `STUDIO-RETENTION-JACCARD`
**深刻度:** MEDIUM

```python
# studio_timeseries.py:126-128
intersection = staff & prev_staff
union = staff | prev_staff
retention = len(intersection) / len(union) if union else 0.0
```

前年10人 → 今年100人 (全員継続+90人新規): `10/100 = 0.1` (retention 10%)
実際は前年スタッフ全員継続しており retention 100% であるべき。

**修正:** `retention = len(intersection) / len(prev_staff)`

---

### B13. [実装エラー] Milestones の `top_anime` がグローバル最高評価を使用

**TAG:** `MILESTONES-GLOBAL-MAX`
**深刻度:** LOW

```python
# milestones.py:51-57 — anime_map 全体でグローバル最大を探索
best_score = 0.0
best_anime_id = None
for aid, anime in anime_map.items():
    if anime.score and anime.score > best_score:
        best_score = anime.score
        best_anime_id = aid

# milestones.py:118 — その作品にクレジットされた人だけがマイルストーンを得る
if credit.anime_id == best_anime_id and best_anime_id is not None:
```

**修正:** 個人ごとのクレジット作品内で最高を選択。

---

### B14. [実装エラー] `compute_retrospective_potential` が常に current_score を返す

**TAG:** `RETROSPECTIVE-NOOP`
**深刻度:** LOW

```python
# community_detection.py:535-536
future_peak = current_score          # ← current = future に設定
retrospective = compute_retrospective_potential(
    ..., ability_at_time, future_peak,
)

# community_detection.py:201-205 (関数本体)
score_gap = future_peak_score - current_score    # = 0
if score_gap <= 0:
    return current_score   # ← 常にここに到達
```

`avg_retrospective_potential` は `avg_ability_at_formation` と同一。

---

### B15. [実装エラー] `compute_studio_exposure` の `akm_result` がデッドコード

**TAG:** `STUDIO-EXPOSURE-DEAD-CODE`
**深刻度:** LOW

```python
# integrated_value.py:66-67
if assignments is None and akm_result is not None:
    assignments = {}  # ← akm_result を使わず空 dict
```

---

### B16. [実装エラー] Mentorship の year_count がスパンでなくカウント

**TAG:** `MENTORSHIP-YEAR-COUNT`
**深刻度:** LOW

```python
# mentorship.py:118
span_score = min(30, year_count * 6)  # year_count = len(years), not max-min
```

10年間で2年共演: `2 * 6 = 12`。2年間で2年共演: `2 * 6 = 12`。同じスコア。

**修正:** `max(years) - min(years)` をスパンとして渡す。

---

# 第3部: 設計上の疑念・正当化困難

---

### D01. [正当化困難] ロール重み (`COMMITMENT_MULTIPLIERS`) の根拠不在

**TAG:** `ROLE-WEIGHT-JUSTIFICATION`
**File:** `src/utils/config.py:40-93`

```python
COMMITMENT_MULTIPLIERS = {
    "direction": 3.0,              # なぜ 3.0?
    "animation_supervision": 2.8,  # なぜ direction の 93%?
    "design": 2.3,                 # なぜ animation (2.0) より上?
    "art": 1.3,                    # なぜ technical (2.0) より下?
}
```

最終エッジ重みは `COMMITMENT_MULTIPLIERS[category] × ROLE_RANK[role]` で計算:
- `director`: 3.0 × 1.0 = **3.0**
- `key_animator`: 2.0 × 1.0 = **2.0**
- `in_between`: 2.0 × 0.5 = **1.0**

監督のエッジ重みは動画の **3倍**。この比率に業界調査の裏付けがない。

---

### D02. [正当化困難] `ROLE_CONTRIBUTION_WEIGHTS` も根拠不在

**TAG:** `CONTRIBUTION-WEIGHT-JUSTIFICATION`
**File:** `src/analysis/contribution_attribution.py:48-64`

```python
ROLE_CONTRIBUTION_WEIGHTS = {
    Role.DIRECTOR: 0.20,          # 監督 20%
    Role.KEY_ANIMATOR: 0.06,      # 原画 6%
    Role.IN_BETWEEN: 0.01,        # 動画 1%
}
```

---

### D03. [正当化困難] エッジ重み `commit_a × commit_b` の二次膨張

**TAG:** `EDGE-WEIGHT-QUADRATIC`
**File:** `src/analysis/graph.py:354,413`

```python
# graph.py:354
new_weight += commit_a * commit_b * ep_w * importance
```

監督 (3.0) 同士: `3.0 × 3.0 = 9.0`
監督 × 動画: `3.0 × 1.0 = 3.0`

一方 `create_director_animator_network` は算術平均 `(dir_w + anim_w) / 2.0` を使用。
同一プロジェクト内で異なる重み関数を使う非一貫性。

---

### D04. [正当化困難] 休眠 `decay_rate=0.5` はドキュメントの `0.3` と不一致

**TAG:** `DORMANCY-DECAY-RATE`
**File:** `src/utils/config.py:132` vs `CALCULATION_COMPENDIUM.md` §2.4

| ブランク年数 | effective_gap | code (δ=0.5) | doc (δ=0.3) | 差 |
|-------------|---------------|------------|------------|-----|
| 3年 | 1 | 0.607 | 0.741 | 1.22x |
| 5年 | 3 | 0.223 | 0.407 | 1.82x |
| 8年 | 6 | 0.050 | 0.165 | 3.30x |
| 10年 | 8 | 0.018 | 0.091 | 5.06x |

5年ブランクで成果の 78% を失う (code) vs 59% (doc)。

---

### D05. [正当化困難] AKM κ のフロア=2.0, キャップ=50.0

**TAG:** `AKM-KAPPA-BOUNDS`
**File:** `src/analysis/akm.py:664`

```python
kappa = float(np.clip(kappa, 2.0, 50.0))
```

| n_obs | κ=2 | κ=50 |
|-------|-----|------|
| 1 | 0.33 (67%縮小) | 0.02 (98%縮小) |
| 5 | 0.71 | 0.09 |
| 10 | 0.83 | 0.17 |
| 50 | 0.96 | 0.50 |

下限 2.0: 「シグナル分散 ≥ 残差分散/2」という暗黙の仮定。
`sigma2_signal = max(..., sigma2_person_raw * 0.1)` のフロアも恣意的。

---

### D06. [正当化困難] AKM redistribution の α に形式的同定論証なし

**TAG:** `AKM-REDISTRIBUTION-ALPHA`
**File:** `src/analysis/akm.py:558-560`

```python
alpha = max(0.0, slope_mover - slope_stayer)
```

movers の person_fe がスタジオ品質に独立であるという仮定は、
movers 自身がスタジオ選択に内生的な場合に崩壊する (操作変数なし)。

---

### D07. [正当化困難] IV の L2 正則化 `l2_alpha=0.5` で CV が形骸化

**TAG:** `IV-LAMBDA-REGULARIZATION`
**File:** `src/analysis/integrated_value.py:253`

prior からの ±10-15% しか動かせない。事実上 prior 固定。

---

### D08. [疑念] AKM 処理順序: shrinkage → debias → redistribution

**TAG:** `AKM-PROCESSING-ORDER`
**File:** `src/analysis/akm.py:903-949`

shrinkage で縮小した後に redistribution で studio FE 分を追加すると、
shrinkage 効果が部分的に無効化される。
理論的には redistribution → shrinkage → debias が一貫。

---

### D09. [疑念] `_debias_by_obs_count` が任意の負 slope で発動

**TAG:** `DEBIAS-NO-SIGNIFICANCE`
**File:** `src/analysis/akm.py:401-403`

```python
if slope >= 0:
    log.info("akm_debias_skipped", ...)
    return person_fe_arr
# slope < 0 なら無条件で debias 適用
```

slope=-0.001 (実質無相関) でも発動。有意性検定なし。

---

### D10. [疑念] `career_capital` の乗法的形式で閾値 0.7 が到達困難

**TAG:** `CAREER-CAPITAL-THRESHOLD`
**File:** `src/analysis/patronage_dormancy.py:255-260`

```python
career_capital = iv_pctile * years_norm * stage_norm
# iv_pctile=0.8, years=10(→0.33), stage=3(→0.50): 0.8 × 0.33 × 0.50 = 0.133
# 閾値 0.7 に到達するには全要素 ≈ 0.9 以上が必要
```

保護対象: iv 上位10% × 27年超 × stage5以上 → 極少数のみ。

---

### D11. [疑念] Synergy score で n=2 → n=3 間に 4.6 倍ジャンプ

**TAG:** `SYNERGY-DISCONTINUITY`
**File:** `src/analysis/synergy_score.py:240-243`

| n | スコア | 増加率 |
|---|-------|--------|
| 1 | 0 | — |
| 2 | 0.3×q | — |
| 3 | 1.386×q | **4.62x** |
| 4 | 1.609×q | 1.16x |

---

### D12. [疑念] CHIEF_ANIMATION_DIRECTOR が DIRECTOR_ROLES に含まれる

**TAG:** `ROLE-GROUP-INCONSISTENCY`
**File:** `src/utils/role_groups.py:21-27`

```python
DIRECTOR_ROLES = frozenset({
    Role.DIRECTOR,
    Role.EPISODE_DIRECTOR,
    Role.CHIEF_ANIMATION_DIRECTOR,  # ← 作画監督は監督ではない
})
```

`ROLE_CATEGORY` では `animation_supervision` に分類。
patronage 計算で「監督」扱い、コミットメント計算で別カテゴリという二重定義。

---

### D13. [疑念] Compatibility でスタッフ50人超の作品を除外

**TAG:** `COMPATIBILITY-LARGE-WORK-EXCLUSION`
**File:** `src/analysis/compatibility.py:96-97`

```python
if len(staff_list) > 50:
    continue
```

大作・高予算作品 (劇場版、長期TVシリーズ) での共演が完全無視される。

---

### D14. [疑念] peer_percentile は iv_score の循環的パーセンタイル変換

**TAG:** `PEER-PERCENTILE-CIRCULAR`
**File:** `src/analysis/individual_contribution.py:155-218`

Layer 2 の peer_percentile は iv_score (Layer 1 の最終産物) のコホート内順位。
独立した評価軸にならない。

---

### D15. [疑念] 低 mover 率で studio_exposure が暗黙ゼロ化

**TAG:** `AKM-LOW-MOVER-FALLBACK`
**File:** `src/analysis/akm.py:809-822`

`mover_fraction < 0.10` → `studio_fe = {}` → IV 5構成要素中1つが消失。
重みは未調整のまま。

---

### D16. [疑念] `expected_ability` で collaborator IV に同時性バイアス

**TAG:** `EXPECTED-ABILITY-SIMULTANEITY`
**File:** `src/analysis/expected_ability.py:103-112`

```python
# expected_ability.py:110 — anime.score で重み付け
w = anime.score if anime.score else 1.0
collab_ivs.append(avg_collab * w)
```

Feature 0 (collaborator IV) は person_fe の関数。
Target y も person_fe。同時方程式バイアス。

---

### D17. [疑念] Director は Patronage = 0 (構造的非対称)

**TAG:** `DIRECTOR-ZERO-PATRONAGE`
**File:** `src/analysis/patronage_dormancy.py:73`

```python
if c.role in DIRECTOR_ROLES:
    continue  # Director のクレジットは全スキップ
```

---

### D18. [疑念] Patronage に BiRank → IV → BiRank の循環

**TAG:** `PATRONAGE-BIRANK-CIRCULAR`
**File:** `src/pipeline_phases/core_scoring.py:81-83`

---

### D19. [要説明] パーセンタイルのタイ処理と1人コホート

**TAG:** `PERCENTILE-TIES`
**File:** `src/pipeline_phases/post_processing.py:33-41`

```python
# post_processing.py:33-37 — bisect_right で同スコア者が全員同値
sorted_vals = sorted(r.get(axis, 0) for r in context.results)
rank = bisect.bisect_right(sorted_vals, r.get(axis, 0))
r[f"{axis}_pct"] = round(rank / n * 100, 1)

# post_processing.py:40-41 — 1人の場合は 100.0
elif n == 1:
    for r in context.results:
        r[f"{axis}_pct"] = 100.0
```

---

### D20. [要説明] `unique_studios` が機会の代理指標として曖昧

**TAG:** `OPPORTUNITY-RESIDUAL-PROXY`

---

### D21. [要説明] AKM の mover_fraction < 10% 閾値の根拠

**TAG:** `AKM-MOVER-THRESHOLD`

---

### D22. [要調査] Label Propagation にシード固定なし

**TAG:** `LABEL-PROPAGATION-SEED`
**File:** `src/pipeline_phases/graph_construction.py:44-51`

```python
if n_edges <= 1_000_000:
    communities = nx.community.louvain_communities(..., seed=42)  # 再現可能
else:
    communities = nx.community.label_propagation_communities(...)  # シードなし
```

---

### D23. [要調査] `_run_tags` が並列スレッドから dict に書き込み

**TAG:** `TAGS-THREAD-SAFETY`
**File:** `src/pipeline_phases/analysis_modules.py:184-187`

```python
# コメント: "thread-safe since results is read-only here"
# 実際: dict に書き込んでいる
r["tags"] = person_tag_assignments[pid]
```

---

### D24. [要調査] IV 最適化の CV でターゲットリーク

**TAG:** `IV-CV-TARGET-LEAK`
**File:** `src/analysis/integrated_value.py:196-213`

人物の中央活動年で fold 分割するが、y (全キャリア平均 anime.score) と
特徴量 (全データから計算) が全年を含む。

---

### D25. [要説明] API が生スコアを文脈なしで公開

**TAG:** `API-RAW-SCORE-EXPOSURE`
**File:** `src/api.py:273-280`

---

### D26. [要調査] `VOICE_ACTOR` 等が `ROLE_CATEGORY` 未登録

**TAG:** `ROLE-CATEGORY-MISSING`
**File:** `src/utils/role_groups.py:117-176`

---

### D27. [要説明] `anime_value.py` のスケール: score/100 vs score/10

**TAG:** `ANIME-VALUE-SCALE`
**File:** `src/analysis/anime_value.py:90-92,124-126`

他モジュールは `anime.score / 10.0` (0-10 スケール前提) だが、
本モジュールは `/100` (0-100 スケール前提)。
Anime model の `score` フィールドのスケール定義が曖昧。

---

# 第4部: 修正ロードマップ

### Phase A: anime.score 排除 (最優先)

| 順序 | 対象 | 影響範囲 | 作業量 |
|------|------|---------|--------|
| A1 | `_work_importance` からスコア乗数削除 | グラフ全体 → BiRank, Authority, Trust | 小 |
| A2 | AKM のアウトカムを構造指標に変更 | person_fe → IV 全体 | 大 |
| A3 | Patronage から Quality 項削除 | Patronage → IV 20% | 小 |
| A4 | Skill を構造メトリクスに置換 | Skill 軸全体 | 中 |
| A5 | IV ラムダ最適化ターゲット変更 | 全5構成要素の重み | 中 |
| A6 | Layer 2 から anime.score 依存除去 | 全4サブ指標 | 中 |
| A7 | 表示用は情報として残す | anime_stats, growth 等 | 小 |

### Phase B: 実装エラー修正

| 順序 | TAG | 影響 |
|------|-----|------|
| B1 | `STUDIO-EXPOSURE-INCONSISTENCY` + `IV-RENORMALIZATION-MISSING` | スコア正確性 |
| B2 | `BIRANK-UPDATE-ORDER` | BiRank 全値 |
| B3 | `CLOSENESS-WEIGHT-INVERTED` | closeness 全値 |
| B4 | `CONFIDENCE-SCALE` + `BOOTSTRAP-WRONG-TARGET` | 法的要件 |
| B5 | `AKM-STUDIO-MISMATCH` | person_fe |
| B6 | `INDEPENDENT-VALUE-SELF-INCLUSION` | Layer 2 |
| B7 | `OLS-ZERO-DUMMY` + `CONSISTENCY-SCALE-MISMATCH` | Layer 2 |
| B8 | 残りの B11-B16 | 軽微 |

### Phase C: 設計改善

| 順序 | 対象 |
|------|------|
| C1 | 重み根拠の文書化 (D01, D02) |
| C2 | マジックナンバーの感度分析 (D04, D05, D07, D10) |
| C3 | 設計疑念の検証 (D08, D09, D14, D18) |
| C4 | ドキュメント整備 (D19-D27) |
