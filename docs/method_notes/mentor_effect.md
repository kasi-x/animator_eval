# Method Note — Mentor Effect Event-Study

## 目的

`mentorship.py` の `infer_mentorships()` で推定済 mentor–mentee pair に対し:

```
Δθ_mentee = mean(θ at +1..+5) - mean(θ at -3..-1)
```

mentor 初協業 event year を 0 とする event-study。さらに非 mentor control 群との
**matched DiD** で confounding を部分除去。

## Confounding 問題

selection on observables: 「経験豊富な mentor は構造的に有利な mentee を選ぶ」
逆因果。本 module は完全除去できない。観察可能 confounder (cohort / role) のみ
matching → unobserved confounder は E-value 等で sensitivity 検討すべき。

## Spec

### Pair event-study

```
pre_window  = event_year + (-3, -1)   # default
post_window = event_year + (1, 5)
pre_mean  = mean(θ_mentee[year ∈ pre_window])
post_mean = mean(θ_mentee[year ∈ post_window])
delta = post_mean - pre_mean
```

両 window で少なくとも 1 観測必要、不足は None スキップ。

### Aggregate effect

`aggregate_mentor_effects()`:
- mean_delta, sd_delta, median_delta
- bootstrap CI (1000 回、pair 単位置換抽出)

### Matched DiD

`estimate_matched_did(treated_pairs, candidate_controls)`:
- treated mentee: pair_rows の delta
- control: 非 mentor person の同 event_year ± window での delta
  (event_year を **仮想 event** として control に流用)
- DiD = mean(treated_delta) - mean(control_delta)

bootstrap CI: 両 arm を独立に置換抽出。

## H1/H2 制約

- H1: anime.score 非依存。
- H2: 「mentor の優秀さ」frame NG → 「協業経験あり/なし person の構造的位置の差」のみ。

## Caveats

- candidate_controls は entity resolution 済 person のみ。mentor 関係に **無い** こと
  を upstream で保証する必要。
- event_year の流用は control の selection bias を完全除去しない。propensity score
  matching が将来拡張候補。
- mentor relationship 推定そのものに精度限界 (`mentorship.py` の `min_shared_works`,
  `min_stage_gap` 閾値設定で結果変動)。
- theta_i panel が年次で揃ってない場合、window 内観測数不均衡 → mean が薄い year に
  影響受ける。

## 代替 spec (拡張候補)

- IPW で propensity matching
- Synthetic control (mentor 関係を擬似コントロールから構築)
- mentor 効果の persistence test (post-window を +6..+10 年に拡張)

## 関連

- `src/analysis/career/mentor_effect.py` (11 tests pass)
- `src/analysis/career/mentorship.py` (pair 推定の上流)
- `src/analysis/causal/did_studio_transfer.py` (matched DiD の比較先)
