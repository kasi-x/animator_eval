# Glossary v3 — 用語集

本プロジェクトで使用する用語の正規定義と、評価的含意を持つ語の代替リスト。
`scripts/report_generators/section_builder.py` の glossary inject から
参照される。

---

## 1. 構造的指標 (使用可)

| 用語 | 定義 | 算出 |
|------|------|------|
| **person FE (θ_i)** | AKM 二要素固定効果モデルの個人固定効果 | `log(production_scale_ij) = θ_i + ψ_j + ε_ij` |
| **studio FE (ψ_j)** | AKM のスタジオ固定効果 | 同上 |
| **production_scale** | 作品規模 | `staff_count × episodes × duration_mult` |
| **role_weight** | 役職別エッジ重み | `src/utils/role_groups.py` 定数 |
| **birank** | bipartite ネットワーク中心性 | person × anime の bipartite PageRank |
| **clustering coefficient** | local triangle density | NetworkX 標準実装 |
| **PageRank (重み付き)** | エッジ重み考慮の中心性 | tol=1e-6, max_iter=100 |
| **HHI** | 集中度 | `Σ share_s² × 10000` |
| **R5 retention** | デビュー後 5 年同一スタジオ可視継続率 | EB 縮小推定 |
| **U_p (露出機会ギャップ)** | `pct(θ_i) - pct(total_credits)` | 閾値 30pt |
| **G_p (ゲートキーパー)** | `Σ deg_centrality(first-degree neighbors)` | top-K PageRank ノード |
| **W_g (whitespace)** | `(1-penetration) × CAGR × specialist_supply` | ジャンル × 年 |
| **V_G (独立ユニット)** | `role_coverage × density × scale_compatibility` | Louvain コミュニティ |
| **mean_res (ペア残差)** | 線形予測モデルからの残差平均 | BH 補正 q<0.05 |
| **M̂_d (メンティー M̂ シフト)** | 監督下メンティーの 5 年後 θ 平均変化量 | EB 縮小推定 |
| **soft_power_index** | `anime_count × mean_θ_proxy` | 配信プラットフォーム別 |

---

## 2. 統計手法 (使用可)

| 用語 | 用途 | 既知の限界 |
|------|------|-----------|
| **Cox PH 回帰** | 生存解析 / hazard 比 | 比例ハザード仮定 (Schoenfeld 検定で確認) |
| **Kaplan-Meier (KM)** | 生存関数 | 共変量を調整しない記述統計 |
| **Greenwood CI** | KM の解析的 CI | 小サンプルで保守的 |
| **Mann-Whitney U** | 非パラメトリック中央値検定 | tied ranks の影響 |
| **DML (Double ML)** | 因果推定 | 非交絡性仮定、観測されない交絡には無対処 |
| **AKM (Abowd-Kramarz-Margolis)** | 個人 × スタジオ FE 分解 | limited mobility bias (Andrews et al. 2008) |
| **Empirical Bayes (EB) shrinkage** | 推定値の縮小 | 事前分布の選択依存 |
| **bootstrap 95% CI** | リサンプリング推定 | n_resamples ≥ 1000 推奨 |
| **BH 補正** | 多重検定 FDR 制御 | family-wise 制御ではない |
| **Louvain コミュニティ** | ネットワーク分割 | 確率的 (seed 依存)、resolution limit |
| **permutation null** | ランダム化ベースライン | 帰無分布の choice が結論に影響 |

---

## 3. 禁止語 → 代替語マップ

REPORT_PHILOSOPHY v2.1 §2.1 + REPORT_DESIGN_v3.md §2 で禁止される
評価的 framing と、対応する構造的代替語。

### 能力 framing → 構造的記述

| 禁止 | 代替 (狭い名前) |
|------|---------------|
| 能力 / ability | クレジット密度 / ネットワーク位置 / 構造スコア (θ_i) |
| 実力 / skill | 構造指標値 / ネットワーク位置 |
| 才能 / talent | クレジット密度 / 協業回数 |
| 優秀 / excellent | 高スコア群 / 上位パーセンタイル |
| 一流 / top-tier | 上位パーセンタイル群 |
| 凡庸 / mediocre | 中央値帯 / 中位パーセンタイル |
| 低水準 / weak | 下位パーセンタイル |

### 因果 framing → 関連 framing

| 禁止 | 代替 |
|------|------|
| 〜の原因 / cause | 〜と共起する / is associated with |
| 〜の結果として / result in | 観察上の関連 / co-occurrence |
| 〜のせいで / due to | 〜の前後で / before-after |
| 〜を引き起こす / trigger | 〜と関連する / correlate |

### 規範 framing → 観察 framing

| 禁止 | 代替 |
|------|------|
| べき / should | 観察する / 記述する |
| 必要 / need to | 観察される / 計上される |
| 重要 / important | 高頻度 / 上位パーセンタイル |
| 健全 / healthy | 集中度低 / 分散度高 (HHI<1500) |
| 危機 / crisis | 〜の急変 / discontinuity |

### 過去の慣性表現 (rename)

| v2 までの呼称 | v3 での呼称 | 根拠 |
|--------------|------------|------|
| 育成力 / 育成実績 | メンティー M̂ シフト | mentor outcomes は構造的測定 |
| 過小評価タレント | 露出機会ギャップ人材プール | 評価語を排除 |
| 離職率 | 翌年クレジット可視性喪失率 | 雇用実態とクレジットを区別 |
| 出世 | 役職進行 | 評価的含意を排除 |
| 成功率 | observed achievement rate | 主観評価を排除 |
| 失敗 | observed gap / discontinuity | 因果含意を排除 |
| 業界平均より上 | 業界中央値の P50 超 | 順序的記述に統一 |

---

## 4. 対象範囲 / 除外

### 対象 (使用可)

- 公開クレジットレコード (anime / persons / credits / studios / roles)
- ネットワーク構造指標 (degree / PageRank / clustering / community)
- 時系列メタデータ (year / season / format)

### 除外 (禁止)

- `anime.score` (視聴者評価) — Source 層に display 用に保持するが
  scoring path 一切不使用
- 個人のキャラクター / 性格 / 動機 評価
- 主観的な作品の質的評価
- 賃金 / 報酬データ (本プロジェクトは保有せず)

---

## 5. v3 用語の更新ガイド

新規用語追加 / 既存用語 rename の手順:

1. 本ファイル (`docs/GLOSSARY_v3.md`) に登録。狭い名前を採用。
2. `scripts/report_generators/section_builder.COMMON_GLOSSARY_TERMS` に
   定義を追加 (HTML render 時に glossary tooltip 表示)。
3. 禁止語の場合は `forbidden_vocab.yaml` に追加し、
   `vocab_replacements.yaml` に代替語を併記。
4. 関連 reports の SPEC `interpretation_guard.forbidden_framing` に
   curate 時に明示。

---

## 改訂履歴

- **v3.0 (2026-05-05)**: 初版。REPORT_PHILOSOPHY v2.1 + REPORT_DESIGN_v3
  の禁止語規範を集約。各 SPEC が参照する単一の glossary source。
