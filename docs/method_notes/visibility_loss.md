# Method Note: 翌年クレジット可視性喪失 早期警告 (Visibility Loss Forecasting)

**Status**: implemented (2026-05-15)
**Module**: `src/analysis/career/visibility_loss.py`
**Report**: `scripts/report_generators/reports/career_visibility_warning.py`
**TASK_CARDS**: `25_compensation_fairness/03_visibility_loss_holdout.md`
**Hard constraints**: H1 (`anime.score` excluded from features), H2 (no individual-attribute framing), H4 (holdout AUC + calibration error required for individual-level prediction)

---

## Purpose

人物 i について「翌年 (ref_year + 1) に本データセット上でクレジットが出現しない」確率を推定する。狭い名前で呼ぶ:

> **翌年クレジット可視性喪失率** (one-year-ahead credit visibility loss rate)

**「離職率」「離脱率」「業界引退率」とは呼ばない**。クレジットの不在は離職と等価ではなく、スタジオ移動・無名義参加・海外下請け参加・データ source 側の欠落・休業など複数の事象を含む複合観測である。本指標は構造的ネットワーク観測量であり、個人の属性 (心理的志向・健康状態・キャリア意図) を反映するものではない。

労働分配 (Workers brand) 経路での用途:

- スタジオ HR の介入トリガー (面談・契約条件再交渉)
- 業界全体の可視性低下時系列 → 政策対話の根拠
- 個別人事判断の単独根拠としては使えない (確率的指標、誤分類コスト存在)

---

## Specification

### Label definition

```
visibility_loss[i, ref_year+1] = 1
    if credit_count[i, ref_year+1] == 0
    AND max(credit_count[i, ref_year-2 .. ref_year]) >= 1
```

「直近 3 年以内にクレジットが存在し (= active)、翌年に出現しない」person のみを対象とする。Never-active や long-dormant な person は対象外。

### Features (structural only)

| Feature | Description | Source |
|---|---|---|
| `theta_i` | AKM person fixed effect (production scale) | `mart.akm_person_fe` |
| `pagerank` | weighted PageRank in co-credit graph | `mart.network_centrality` |
| `betweenness` | betweenness centrality | `mart.network_centrality` |
| `credit_slope` | linear slope of credit_count over last 3 years | computed from credits panel |
| `credit_variance` | variance of credit_count over last 3 years | computed from credits panel |
| `studio_entropy` | Shannon entropy of studio distribution, last 3 years | `credits × anime` join |
| `role_stall_years` | consecutive years in dominant role, last 5 years | computed from credits panel |
| `peer_loss_rate` | fraction of co-credit peers who lost visibility at ref_year | co-credit edges |
| `cohort_age` | `ref_year - debut_year` | min credit year per person |
| `role_diversity` | unique role count over last 3 years | computed from credits panel |

**anime.score / popularity / favourites は一切使用しない** (Hard rule H1)。

### Model

- **Base learner**: LightGBM `LGBMClassifier`
  - `n_estimators=300`, `learning_rate=0.05`, `max_depth=5` (default)
  - `scale_pos_weight = N_negative / N_positive` でクラス不均衡を補正
  - `random_state=42`
- **Calibration**: `sklearn.calibration.CalibratedClassifierCV(method="isotonic", cv=5)`
  - 5-fold で base learner を再学習し isotonic regression で確率を再較正
  - Platt scaling より柔軟な単調変換、N が十分な panel に向く

### Validation: temporal year split (leakage 防止)

```
Train:   ref_year < holdout_year
Holdout: ref_year == holdout_year
```

- **Person split ではなく year split** を採用。person split は同一 person の過去-未来情報が train と holdout に分散する逆 leakage を起こすが、本タスクは person の "翌年" を予測するため year で切れば leakage は無い。
- `run_leakage_check()` で `train.ref_year < holdout_year` および `holdout.ref_year == holdout_year` を機械的に検証 (CI 必須項目)。

### Evaluation metrics

| Metric | Formula | Pass criterion |
|---|---|---|
| ROC-AUC | `sklearn.metrics.roc_auc_score` | ≥ 0.65 (公開ゲート) |
| Brier score | `mean((y_true - y_score)^2)` | 報告のみ (低いほど良) |
| ECE (Expected Calibration Error) | `Σ_b (n_b/N) · | mean_pred_b - frac_pos_b |` (10 等幅ビン) | < 0.10 で個別予測公開可 |
| Reliability curve | (bin_center, mean_pred, frac_pos, count) | calibration plot 用 |
| Baseline AUC | last-3-year-per-person mean loss rate を holdout で評価 | モデル vs ナイーブ比較 |
| Subgroup AUC | gender / role_group / cohort_band 別 AUC | 最大差 ≤ 0.10 |

---

## Decision criteria (gate logic)

| Condition | Action |
|---|---|
| `auc_roc < 0.65` | **Stop-if**: 個別予測スコアを report 化しない (TASK_CARDS Stop-if) |
| `n_holdout < 30` | **Stop-if**: ベース数不足、report 化しない |
| `subgroup_max_diff > 0.10` | **Stop-if**: fairness 修正を先行、個別予測公開を保留 |
| `ece >= 0.10` | 個別確率値の表示を抑制し集計値のみ表示 (確率の意味解釈不可) |
| `auc_roc >= 0.65 AND ece < 0.10 AND subgroup_max_diff <= 0.10` | 個別 + 集計を HR brief に公開可 |

---

## Known limitations

1. **Label ≠ 業界離脱**: 本データから「離脱」「廃業」を区別できない。スタジオ移動・無名義・海外案件・データ欠落・育休/介護休業・長期休暇など複数の事象が混在する。
2. **Source coverage bias**: in-between animator やレイアウトなど低クレジット可視性 role は構造的に label=1 になりやすい。データ source (AniList / MAL / ANN / SeesaaWiki / allcinema) のカバレッジ差にも依存する。レポートでは role group ごとの予測精度を必ず subgroup AUC で開示する。
3. **Temporal autocorrelation の影響**: AUC が高くても、それが「過去の可視性が低い人は将来も低い」という単純な時間的自己相関を学習した結果である可能性が残る。Counterfactual / DML を別途実施しない限り、構造的原因への解釈には踏み込まない。
4. **Holdout 1 年のみ**: 単年 holdout は year-specific shock (例: 2020 COVID-19、2011 震災) に強く影響される。multi-year temporal rolling validation を将来加える必要がある (sensitivity grid に holdout_year を追加すること)。
5. **共変量の同期 leakage 懸念**: `peer_loss_rate` は `ref_year - 1` の peer の可視性喪失を観測しているが、その peer の喪失が ref_year - 1 末以降に観測される場合、micro-level leakage が起こりうる。本実装では「peer が ref_year - 1 に active で ref_year に不在」を条件としているため理論上は ref_year のフルラベル情報を使わないが、エッジケースで稀に発火しうる。`run_leakage_check()` は year level の粗い検査のみで、この件は cover していない。
6. **AKM / PageRank が未計算の場合は 0.0 補完**: feature 品質が下がる可能性あり (ゼロ補完は MAR 仮定)。

---

## Interpretation guide

**「高 score = 翌年クレジット可視性喪失リスクが高い」と読む**。これは:

- 業界からの離脱を意味しない
- 個人の心理状態・職業意欲・健康状態を意味しない
- 「翌年クレジットが観測される」「観測されない」の二項に対する確率的予測である

**禁止される使い方**:

- 個別人事判断 (契約終了・条件不利化) の単独根拠
- 個人を名指しした defamatory な評価表現
- スコアを順序リストとして晒し、特定人物の閲覧可能な掲載

**推奨される使い方**:

- スタジオ HR の継続クレジット公開推進トリガー (スタジオ側のクレジット記載漏れの再点検依頼など)
- 業界団体の attrition 構造分析の参考指標 (集計値のみ)
- 政策対話 (継続教育支援、再就職支援、health insurance) の事実根拠

---

## 実装上の判断 (保守的選択 + 代替案)

| 判断項目 | 採用案 | 代替案 / 検討メモ |
|---|---|---|
| Calibration 方式 | isotonic regression (5-fold CV) | Platt scaling は単調シグモイドで N 少時に安定だが本データの calibration shape は非単調になりうるため柔軟性を優先 |
| Holdout 期間 | 単年 (最終年 - 1) | rolling 3-year holdout は将来課題。当面 multi-year は sensitivity grid (`SensitivityAxis(name="holdout_year")`) で粒度を上げる |
| Class imbalance | `scale_pos_weight` のみ | SMOTE 等 oversampling は時系列で人工データを生成し leakage を入れる懸念のため見送り |
| Person split vs year split | year split | 本タスクは「翌年の個人」を予測するため year split が自然。person split は同じ人物の過去-未来情報を分散させるため不適 |
| AUC ゲート | 0.65 | 0.60 / 0.70 と sensitivity 比較を `_spec.SensitivityAxis` に追加済み |
| ECE ゲート | 0.10 | 0.05 / 0.15 を sensitivity に加えるかは公開時の運用判断で決定 |
| 検証 ECE のビン数 | 10 等幅 | quantile bins (等頻度) も実装可能だが解釈容易性 (0.0-0.1, 0.1-0.2 ...) を優先 |

---

## References

- Code: `src/analysis/career/visibility_loss.py`
- Report: `scripts/report_generators/reports/career_visibility_warning.py`
- Tests: `tests/analysis/career/test_visibility_loss.py`
- Task card: `TASK_CARDS/25_compensation_fairness/03_visibility_loss_holdout.md`
- Hard constraints: `TASK_CARDS/_hard_constraints.md`
- Report philosophy: `docs/REPORT_PHILOSOPHY.md`
- LightGBM: Ke et al. (2017) "LightGBM: A Highly Efficient Gradient Boosting Decision Tree" (NeurIPS)
- Isotonic calibration: Zadrozny & Elkan (2002) "Transforming classifier scores into accurate multiclass probability estimates" (KDD)
- ECE definition: Guo et al. (2017) "On Calibration of Modern Neural Networks" (ICML)
