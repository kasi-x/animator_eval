# Method Note — Credit Attribution Anomaly Detection

## 目的

統計的に異常な credit 出現パターンを検出し review priority を flag する。
**自動修復はしない**。entity resolution audit (28/01 drift snapshot) と相補:

| layer | 検出対象 |
|-------|---------|
| ER audit (28/01) | 同一人物が複数 source で違う ID で merge 失敗 |
| credit_anomaly (本 module) | クレジット数 / 役職分布が統計的に外れる person/anime |

## 3 detector

### 1. Poisson outlier

`detect_poisson_outliers()`:
ある cohort/period 内で credit 数 ~ Poisson(μ) を仮定。z = (obs - μ) / √μ。
|z| >= 3.0 で flag。

- min_expected = 5.0 未満は noisy estimate → skip。
- direction: "high" / "low" 両方向検出 (high = 過剰クレジット疑い、low = under-credit)。

### 2. Role distribution divergence

`detect_role_divergence()`:
各 person の役職比率 vs cohort marginal の KL divergence。

```
KL(p_person || q_cohort) >= 1.5 で flag
```

役職構成が cohort norm から大きく逸脱 = 特殊キャリア or データ不整合。

- min_credits = 10 未満は noise。
- dominant_role + share を併記。

### 3. Multi-source disagreement

`detect_source_disagreement()`:
同 canonical id の source 間 credit 数 spread:

```
spread = max_count / max(min_count, 1) >= 4.0  かつ
|z_max| >= 2.5  (per-source cohort z 比較)
```

→ 「片方の source が誤マッチしている可能性」。

- min_total = 10 で noise filter。

## H1 制約

- anime.score 非依存。
- 統計的 outlier flag であり、誤マッチ確定ではない。report 表現に注意 (review priority のみ)。

## Caveats

- 産業特有の biopolar 分布 (極少数の大量クレジット director vs 大量の少量クレジット
  animator) を Poisson 単峰仮定で扱うと false positive。**cohort 内** 分布で局所化が必要。
- role distribution は role taxonomy 整備度に依存。`role_groups.py` の改訂で結果変動。
- multi-source disagreement は **source 自体のカバレッジ差** (anilist は新作偏重 vs
  bangumi は古作偏重) で false positive。

## 関連

- `src/analysis/quality/credit_anomaly.py` (14 tests pass)
- `src/analysis/quality/resolution_drift.py` (28/01 既存)
- `src/etl/resolved/_value_validators.py` (上流 ER 検証)
