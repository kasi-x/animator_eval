# Event Study Guide (イベントスタディ使用ガイド)

## 概要

Event Studyは、**処置の前後で効果がどう変化するか**を時系列で追跡する手法です。大手スタジオ入所の前後で、個人のスキルスコアがどのように推移するかを分析します。

---

## なぜEvent Studyが必要か？

### 問題: DIDの並行トレンド仮定は検証不可能

DID（差分の差分法）は強力ですが、**並行トレンド仮定**に依存します：

```
仮定: 処置がなければ、処理群と対照群のトレンドは同じだったはず
```

しかし、この仮定は**反実仮想**（counterfactual）なので、直接検証できません。

### 解決策: Event Studyで視覚的に検証

Event Studyは処置前期間の係数を推定することで、並行トレンド仮定を**間接的に検証**できます：

```
もし並行トレンドが成立しているなら:
  → 処置前係数（k < 0）は全て ≈ 0 のはず

もし並行トレンドが成立していないなら:
  → 処置前係数が有意にゼロと異なる
  → 処置前から既に差がある（選択効果の証拠）
```

---

## Event Studyの数学的定式化

### モデル

```
Y_it = α_i + Σ_{k=-3}^{+3} β_k · 1{t - t_entry = k} + γ·X_it + δ_t + ε_it
```

**変数の意味**:
- `Y_it`: 人物 i の時点 t におけるスキルスコア
- `α_i`: 個人固定効果（個人ごとの平均的な能力）
- `β_k`: 入所時点から k 年後の処置効果（**推定対象**）
- `t_entry`: 大手スタジオ入所年
- `1{条件}`: 条件が真なら1、偽なら0
- `X_it`: 共変量（経験年数、ポテンシャル等）
- `δ_t`: 年次固定効果（業界全体のトレンド）

### 推定される係数

| k | 時期 | 意味 | 期待される値 |
|---|------|------|-------------|
| k = -3 | 入所3年前 | 処置前トレンド | ≈ 0（並行トレンドなら） |
| k = -2 | 入所2年前 | 処置前トレンド | ≈ 0（並行トレンドなら） |
| k = -1 | 入所1年前 | 処置直前 | ≈ 0（予期効果なし） |
| **k = 0** | **入所年** | **即時効果** | **> 0（処置効果あり）** |
| k = +1 | 入所1年後 | 短期効果 | > β_0（学習効果） |
| k = +2 | 入所2年後 | 中期効果 | > β_+1（累積効果） |
| k = +3 | 入所3年後 | 長期効果 | > β_+2（継続的成長） |

---

## 実装詳細

### 使い方（コード例）

```python
from src.analysis.structural_estimation import (
    estimate_event_study,
    test_parallel_trends,
    build_panel_data
)
from src.analysis.event_study_viz import (
    plot_event_study,
    plot_event_study_with_annotation
)

# Step 1: パネルデータ構築
panel_data = build_panel_data(
    credits=credits,
    anime_map=anime_map,
    person_scores=person_scores,
    major_studios=major_studios,
)

# Step 2: Event Study 推定
event_study_results = estimate_event_study(
    panel_data=panel_data,
    pre_periods=3,   # 入所前3年間
    post_periods=3,  # 入所後3年間
)

# Step 3: 並行トレンド検定
parallel_trends_check = test_parallel_trends(event_study_results)

print(f"Parallel Trends: {parallel_trends_check.result}")
# Output: "passed" / "failed" / "warning"

# Step 4: 可視化
plot_event_study_with_annotation(
    event_study_results=event_study_results,
    parallel_trends_test=parallel_trends_check.evidence,
    output_path="event_study.png"
)
```

### 出力例（JSON）

```json
{
  "event_study": {
    "available": true,
    "n_periods": 7,
    "coefficients": {
      "-3": {
        "beta": 0.5,
        "se": 1.2,
        "p_value": 0.68,
        "ci": [-1.9, 2.9],
        "is_pre_treatment": true,
        "interpretation": "Pre-treatment period (k=-3): No significant effect (β=0.50, p=0.680). Parallel trends supported."
      },
      "0": {
        "beta": 10.2,
        "se": 2.3,
        "p_value": 0.001,
        "ci": [5.7, 14.7],
        "is_treatment_year": true,
        "interpretation": "Treatment year (k=0): Immediate effect of 10.20 points (p=0.001)."
      },
      "+3": {
        "beta": 18.5,
        "se": 3.1,
        "p_value": 0.002,
        "ci": [12.4, 24.6],
        "is_post_treatment": true,
        "interpretation": "Post-treatment period (k=3): Cumulative effect of 18.50 points (p=0.002)."
      }
    }
  },
  "parallel_trends_test": {
    "result": "passed",
    "detail": "Parallel trends assumption appears satisfied. Pre-treatment coefficients: avg |β|=0.67, max |β|=1.20. All p-values > 0.10.",
    "evidence": {
      "pre_treatment_betas": [0.5, 1.2, -0.3],
      "avg_abs_beta": 0.67,
      "max_abs_beta": 1.20,
      "has_trend": false
    }
  }
}
```

---

## 結果の解釈

### ケース1: 並行トレンド成立（理想的）

```
効果サイズ (β_k)
    │
+20 │                        ●●●
    │                      ●●
+15 │                    ●●      ← 入所後: 効果が累積
    │                  ●●
+10 │                ●●
    │              ●●
 +5 │            ●
    │          ●
  0 │━━━━━━━●━━━━━━━━━━━━━━━━━  ← 入所前: 効果なし
    │    ●   ↑
 -5 │  ●     入所時点 (k=0)
    │●
    └─────────────────────────> 相対時点 (k)
      -3 -2 -1  0 +1 +2 +3
```

**解釈**:
- ✅ 処置前係数（k < 0）がゼロ付近 → 並行トレンド成立
- ✅ 入所年（k = 0）で急激に上昇 → 即時効果あり
- ✅ 入所後（k > 0）も継続的に上昇 → 累積的学習効果

**結論**: 大手スタジオの**教育効果（treatment effect）が確認**された。

---

### ケース2: 並行トレンド不成立（問題あり）

```
効果サイズ (β_k)
    │
+20 │              ●●●●●●●●   入所後も変化なし
    │            ●●
+15 │          ●●
    │        ●●              入所前から既に上昇
+10 │      ●●                → 選択効果の可能性
    │    ●●
 +5 │  ●●
    │●●
  0 │━━━━━━━●━━━━━━━━━━━━━━
    │        ↑
    └────────────────────────> 相対時点 (k)
```

**解釈**:
- ❌ 処置前係数（k < 0）が有意にプラス → 並行トレンド不成立
- ❌ 入所前から既に成長中 → 選択効果（元から優秀）
- ❌ 入所後の追加的上昇が小さい → 処置効果は限定的

**結論**: 大手スタジオは既に成長中の人材を**選択**している（selection bias）。因果効果は過大評価の可能性。

---

### ケース3: 予期効果（Anticipation Effect）

```
効果サイズ (β_k)
    │
+20 │                      ●●●
    │                    ●●
+15 │                  ●●
    │                ●●
+10 │              ●●
    │            ●●          入所前から上昇開始
 +5 │          ●●            → 予期効果
    │      ●●
  0 │━━●●●━━━━━━━━━━━━━━━━━
    │        ↑
    └────────────────────────> 相対時点 (k)
```

**解釈**:
- ⚠️ k = -1, -2 で係数が上昇開始 → 予期効果（anticipation）
- これは、入所が決まった時点で既にモチベーション上昇や準備開始
- 処置のタイミングが実際より早い可能性

**対処法**: 処置時点を再定義（採用決定時点に変更）

---

## 並行トレンド検定

### 検定方法

`test_parallel_trends()` 関数は3つのテストを実施：

#### 1. 個別有意性検定
```
H0: β_k = 0  for all k < 0
```

全ての処置前係数の p値 > 0.10 なら合格。

#### 2. 平均絶対値検定
```
avg |β_pre| < 3.0 かつ max |β_pre| < 5.0
```

処置前係数の絶対値が小さいことを確認。

#### 3. トレンド検定
```
処置前係数に線形トレンドがあるか？
slope ≈ 0 かつ p_trend > 0.10 なら合格
```

### 検定結果の解釈

| 結果 | 意味 | 推奨アクション |
|------|------|---------------|
| `"passed"` | ✅ 並行トレンド成立 | 因果推論を信頼してよい |
| `"warning"` | ⚠️ 一部の係数が大きい | 結果を慎重に解釈、追加分析推奨 |
| `"failed"` | ❌ 明確なトレンドあり | DID/Event Studyは不適切、別手法を検討 |
| `"inconclusive"` | ❓ データ不足 | サンプルサイズ増加が必要 |

---

## 可視化

### 基本プロット

```python
from src.analysis.event_study_viz import plot_event_study

plot_event_study(
    event_study_results=event_results,
    output_path="event_study_basic.png",
    title="Event Study: Dynamic Treatment Effects",
)
```

**特徴**:
- 係数の点推定値 + 95%信頼区間
- ゼロ参照線（並行トレンド確認用）
- 入所年の垂直線
- 処置前期間のシェーディング

### アノテーション付きプロット

```python
from src.analysis.event_study_viz import plot_event_study_with_annotation

plot_event_study_with_annotation(
    event_study_results=event_results,
    parallel_trends_test=parallel_trends_dict,
    output_path="event_study_annotated.png",
)
```

**特徴**:
- 並行トレンド検定結果をテキスト表示
- 処置前期間を色分け（緑=合格、赤=不合格、黄=警告）
- 検定統計量（avg |β|, max |β|）を表示

### 分解プロット（3パネル）

```python
from src.analysis.event_study_viz import plot_event_study_decomposition

plot_event_study_decomposition(
    event_study_results=event_results,
    output_path="event_study_decomposition.png",
)
```

**特徴**:
- Panel 1: 処置前（並行トレンドチェック）
- Panel 2: 入所年（即時効果）
- Panel 3: 処置後（累積効果）

---

## 技術的詳細

### Within Transformation（個人内変換）

Event Studyは固定効果推定と同様、個人内変換を使用：

```
(Y_it - Ȳ_i) = Σ_k β_k · (1{t - t_entry = k} - 1̄{k}_i) + γ·(X_it - X̄_i) + (ε_it - ε̄_i)
```

これにより、時間不変の個人特性 α_i（才能、性格等）を除去。

### サンプル選択

Event Studyに含まれるのは：

1. **大手スタジオに入所した人**のみ（処理群）
2. **入所前後に十分なデータ**がある人（pre_periods + post_periods）

最低要件:
```
入所前3年間 + 入所年 + 入所後3年間 = 計7年間のデータ
```

### 標準誤差

現在の実装:
- Homoskedastic SE（等分散仮定）
- 個人内で残差が独立

将来の拡張:
- Cluster-robust SE（個人でクラスタリング）
- Newey-West SE（時系列自己相関に対処）

---

## 使用上の注意

### 1. サンプルサイズ

Event Studyは**データ要求が高い**：

- 最低でも有効な人物数 ≥ 5人
- 推奨：≥ 20人（統計的検出力のため）
- 各相対時点 k に十分な観測数

サンプルが小さいと：
- 標準誤差が大きくなる
- 統計的有意性が低下
- 並行トレンド検定の検出力不足

### 2. 処置時点の定義

「大手スタジオ入所」の定義が重要：

- 初めて大手スタジオで働いた年？
- 大手スタジオの作品が多数を占めた年？
- 契約社員から正社員になった年？

定義が曖昧だと、推定値が不安定になる。

### 3. 動的選択（Dynamic Selection）

時間とともに選択が変わる場合、Event Studyでも対処困難：

例: 実力が上がった人から順に大手に移る
→ 処置前係数が右上がり（並行トレンド不成立）

対処法:
- Callaway & Sant'Anna (2021) の異質的処置効果手法
- Staggered adoption のDID

---

## まとめ

### Event Studyの強み

✅ **視覚的説得力**: グラフ一枚で並行トレンドを確認
✅ **動的効果**: 効果がいつ、どう変化するかを追跡
✅ **頑健性**: 複数時点での検証により、偶然を排除
✅ **透明性**: 仮定の成否が一目瞭然

### Event Studyの弱み

❌ **データ要求**: 長期パネルデータが必要
❌ **複数時点**: 処置が一度きりでない場合、複雑化
❌ **外挿不可**: 閾値から離れた期間の効果は不明

### 推奨される使用場面

| 状況 | Event Study適用 | 理由 |
|------|----------------|------|
| 並行トレンド検証 | ✅ 必須 | DIDの前提条件確認 |
| 動的効果の解明 | ✅ 推奨 | 効果の時間変化を可視化 |
| 即時効果のみ | ⚠️ オプション | FEやDIDで十分 |
| サンプル<10人 | ❌ 非推奨 | 統計的検出力不足 |

---

**作成日**: 2026-02-10
**バージョン**: 1.0.0
**著者**: Claude Opus 4.6
