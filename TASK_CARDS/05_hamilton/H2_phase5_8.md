# Task: H-2 — Phase 5-8 を Hamilton 化

**ID**: `05_hamilton/H2_phase5_8`
**Priority**: 🟠 Major
**Estimated changes**: +600 / -400 lines
**Requires senior judgment**: YES — 関数シグネチャ設計、PipelineContext フィールド分解
**Blocks**: `05_hamilton/H3_phase1_4`
**Blocked by**: `05_hamilton/H1_poc_analysis_modules` (成功判定済み)

---

## Goal

`core_scoring.py` / `supplementary_metrics.py` / `result_assembly.py` / `post_processing.py`
の 4 Phase (5/6/7/8) を Hamilton node に変換する。

Phase 9 (H-1) が Hamilton 化済みであることが前提。`PipelineContext` はまだ残す (H-4 で削除)。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score scoring 禁止
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- AKM / IV / BiRank / PageRank のアルゴリズムを変更しない
- Node 入出力を変更したら `tests/test_akm.py` 等の既存テストを更新する
- 各 Phase の出力は型付き dataclass または TypedDict で明示する

---

## Pre-conditions

- [ ] H-1 完了 (成功判定済み)
- [ ] `pixi run test` pass
- [ ] `git status` clean

---

## 着手前 Senior 決定事項

1. **Node 境界**: 各 Phase を 1 node にするか、関数単位 (AKM / IV / BiRank / PageRank を個別) にするか
   - 推奨: 関数単位 (部分再実行の粒度が細かい方が価値が高い)
2. **`PipelineContext` の扱い**: H-2 では ctx をそのまま引数に渡す。H-4 で分解する
3. **出力型**: `AKMResult`, `IVResult` 等の dataclass を新設するか既存を使うか

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/core_scoring.py` | 各関数を Hamilton node 化 (`@node` または直接関数定義) |
| `src/pipeline_phases/supplementary_metrics.py` | 同上 |
| `src/pipeline_phases/result_assembly.py` | 同上 |
| `src/pipeline_phases/post_processing.py` | 同上 |
| `src/pipeline_phases/hamilton_modules/scoring.py` | **新規** — scoring node 群 |
| `src/pipeline_phases/hamilton_modules/metrics.py` | **新規** — supplementary metrics node 群 |
| `tests/test_hamilton_phase5_8.py` | **新規** — Phase 5-8 node ユニットテスト |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/pipeline_phases/context.py` | H-4 で削除 |
| `src/analysis/scoring/akm.py` 等 | アルゴリズム不変 |
| `src/pipeline_phases/data_loading.py` 等 | H-3 で対処 |

---

## Steps

### Step 1: core_scoring.py の node 化

```python
# src/pipeline_phases/hamilton_modules/scoring.py

def person_fixed_effects(
    credits_df: pd.DataFrame,
    anime_df: pd.DataFrame,
) -> AKMResult:
    """AKM decomposition: theta_i (person FE) + psi_j (studio FE)."""
    from src.analysis.scoring.akm import run_akm
    return run_akm(credits_df, anime_df)

def integrated_value(
    person_fixed_effects: AKMResult,
    birank_scores: pd.Series,
    studio_exposure: pd.Series,
    awcc_scores: pd.Series,
    patronage_scores: pd.Series,
    dormancy_map: dict[int, float],
) -> pd.Series:
    """IV = (λ1·θ + λ2·birank + λ3·studio + λ4·awcc + λ5·patronage) × D."""
    from src.analysis.scoring.integrated_value import compute_iv
    return compute_iv(person_fixed_effects, birank_scores, ...)
```

### Step 2: supplementary_metrics.py の node 化

中心性指標 / 減衰 / キャリアステージを個別 node に:

```python
def degree_centrality(graph: nx.Graph) -> dict[int, float]: ...
def betweenness_centrality(graph: nx.Graph, betweenness_cache: dict) -> dict[int, float]: ...
def career_stage(credits_df: pd.DataFrame) -> dict[int, int]: ...
```

### Step 3: result_assembly.py / post_processing.py の node 化

```python
def person_result_rows(
    person_fixed_effects: AKMResult,
    integrated_value: pd.Series,
    degree_centrality: dict[int, float],
    ...
) -> list[dict]: ...

def percentile_ranks(person_result_rows: list[dict]) -> list[dict]: ...
def confidence_intervals(person_result_rows: list[dict]) -> list[dict]: ...
```

### Step 4: Driver 更新

`src/pipeline.py` で H-1 の Driver に Phase 5-8 module を追加:

```python
dr = driver.Builder().with_modules(
    hamilton_modules.scoring,
    hamilton_modules.metrics,
    hamilton_modules.analysis,  # H-1 追加済み
).build()
```

### Step 5: Node ユニットテスト

各 node は `PipelineContext` なしで単独テスト可能:

```python
def test_person_fixed_effects():
    credits = pd.DataFrame(...)  # synthetic
    anime = pd.DataFrame(...)
    result = scoring.person_fixed_effects(credits, anime)
    assert result.theta_i.shape[0] > 0
    assert result.theta_i.dtype == float
```

---

## Verification

```bash
pixi run test
pixi run bench   # Phase 5-8 の時間比較
# DAG 可視化 (Phase 5-9 全体)
pixi run python -c "
from hamilton import driver
from src.pipeline_phases.hamilton_modules import scoring, metrics, analysis
dr = driver.Builder().with_modules(scoring, metrics, analysis).build()
dr.display_all_functions('/tmp/h2_dag.png')
"
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
```

---

## Stop-if conditions

- [ ] `pixi run test` で AKM / IV / BiRank 関連テストが壊れる
- [ ] `pixi run bench` で Phase 5-6 が 20% 以上遅くなる
- [ ] node 間の型不整合でデバッグコストが高騰する

---

## Rollback

```bash
git checkout src/pipeline_phases/core_scoring.py \
    src/pipeline_phases/supplementary_metrics.py \
    src/pipeline_phases/result_assembly.py \
    src/pipeline_phases/post_processing.py
rm -f src/pipeline_phases/hamilton_modules/scoring.py \
      src/pipeline_phases/hamilton_modules/metrics.py \
      tests/test_hamilton_phase5_8.py
pixi run test
```

---

## Completion signal

- [ ] `pixi run test` pass
- [ ] `pixi run bench` Phase 5-8 が baseline 比 -20% 以内
- [ ] DAG 可視化で Phase 5-9 の依存関係が表示される
- [ ] `tests/test_hamilton_phase5_8.py` の node ユニットテストが pass
- [ ] commit:
  ```
  H-2: Hamilton — Phase 5-8 (core_scoring, supplementary, result, postproc)

  Node granularity: function-level. PipelineContext still passed as-is (H-4 removes it).
  All existing AKM/IV/BiRank tests pass.
  ```
