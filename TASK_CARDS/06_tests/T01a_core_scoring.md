# Task: T01a — core_scoring.py ユニットテスト

**ID**: `06_tests/T01a_core_scoring`
**Priority**: 🟡 Minor
**Estimated changes**: +200 / -0 lines (`tests/test_core_scoring_phase.py` 新規)
**Requires senior judgment**: no (既存テストパターン踏襲)
**Blocks**: なし
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

`src/pipeline_phases/core_scoring.py:compute_core_scores_phase()` の内部で呼ばれる
個別計算 (AKM / BiRank / IV / patronage / dormancy) を **合成データで直接テスト** する。

現在のカバレッジ状況:
- `tests/test_akm.py`: AKM 単独は OK、ただし PipelineContext 経由
- `tests/test_integrated_value.py`: IV は OK
- `compute_core_scores_phase()` 全体フロー: **未テスト**

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score を assertion に使わない
- H5 既存テスト green 維持

---

## Pre-conditions

- [ ] `pixi run test` pass (baseline)
- [ ] `git status` clean

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `tests/test_core_scoring_phase.py` | **新規作成** |

---

## Test Cases

### 1. `compute_core_scores_phase()` が PipelineContext を更新する

```python
def test_core_scores_phase_updates_context(minimal_pipeline_context):
    """compute_core_scores_phase() sets person_fe, iv_scores, birank on ctx."""
    from src.pipeline_phases.core_scoring import compute_core_scores_phase
    compute_core_scores_phase(minimal_pipeline_context)
    ctx = minimal_pipeline_context
    assert ctx.person_fe is not None
    assert len(ctx.person_fe) > 0
    # IV scores populated
    assert hasattr(ctx, "iv_scores") and len(ctx.iv_scores) > 0
```

### 2. AKM: person FE の値域 sanity check

```python
def test_akm_person_fe_range(synthetic_credits, synthetic_anime):
    """AKM person FE values should be within reasonable log-scale range."""
    from src.analysis.scoring.akm import estimate_akm
    result = estimate_akm(synthetic_credits, synthetic_anime)
    fe_values = list(result.person_fe.values())
    assert all(-15 < v < 15 for v in fe_values), "FE values outside expected log range"
```

### 3. dormancy: grace_period 以内はペナルティ 0

```python
def test_dormancy_within_grace_no_penalty(minimal_credits, minimal_anime_map):
    """Person active 1 year ago → dormancy = 1.0 (no penalty, grace_period=2)."""
    from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
    penalties = compute_dormancy_penalty(
        minimal_credits,
        minimal_anime_map,
        current_year=2025,
        decay_rate=0.5,
        grace_period=2.0,
    )
    for pid, d in penalties.items():
        assert d == pytest.approx(1.0), f"Person {pid}: expected 1.0, got {d}"
```

### 4. dormancy: 長期ブランクは指数減衰

```python
def test_dormancy_long_gap_decays(credits_last_active_2010, minimal_anime_map):
    """Person last active 2010, current 2025 → gap=15, grace=2 → exp(-0.5×13)."""
    from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
    import math
    penalties = compute_dormancy_penalty(
        credits_last_active_2010,
        minimal_anime_map,
        current_year=2025,
        decay_rate=0.5,
        grace_period=2.0,
    )
    expected = math.exp(-0.5 * 13)
    for pid, d in penalties.items():
        assert d == pytest.approx(expected, rel=0.01)
```

### 5. BiRank: scores が [0, 1] の範囲

```python
def test_birank_scores_in_range(minimal_bipartite_graph):
    """BiRank scores must be in [0, 1] after normalization."""
    from src.analysis.scoring.birank import compute_birank
    result = compute_birank(minimal_bipartite_graph)
    for pid, score in result.person_scores.items():
        assert 0.0 <= score <= 1.0, f"{pid}: {score}"
```

### 6. Integrated Value: dormancy D=0 のとき IV=0

```python
def test_iv_zero_when_fully_dormant(synthetic_iv_inputs):
    """When D=0 for all persons, IV should be 0 for all."""
    from src.analysis.scoring.integrated_value import compute_integrated_value_full
    dormancy = {pid: 0.0 for pid in synthetic_iv_inputs.person_fe}
    result = compute_integrated_value_full(**synthetic_iv_inputs._asdict(), dormancy=dormancy)
    for pid, iv in result.items():
        assert iv == pytest.approx(0.0)
```

---

## Fixtures

```python
# tests/conftest.py に追加 (または test ファイル内で定義)

@pytest.fixture
def minimal_pipeline_context(tmp_path):
    """PipelineContext seeded with 3 persons, 5 anime, 15 credits."""
    from tests.helpers.synthetic import make_synthetic_context
    return make_synthetic_context(tmp_path, n_persons=3, n_anime=5, n_credits=15)

@pytest.fixture
def credits_last_active_2010():
    """One person, all credits in year 2010."""
    ...

@pytest.fixture
def minimal_bipartite_graph():
    """Small bipartite graph: 3 persons × 3 anime."""
    ...
```

---

## Steps

```bash
# 1. 新規ファイル作成 (上記テストを実装)
touch tests/test_core_scoring_phase.py

# 2. 単体確認
pixi run python -m pytest tests/test_core_scoring_phase.py -v

# 3. 全テスト
pixi run test

# 4. lint
pixi run lint
```

---

## Verification

```bash
pixi run python -m pytest tests/test_core_scoring_phase.py -v
# 期待: 6+ passed

pixi run test --tb=no -q 2>&1 | tail -3
# 期待: baseline + 6 以上

rg 'anime\.score\b' tests/test_core_scoring_phase.py   # 0 件
```

---

## Stop-if conditions

- [ ] `PipelineContext` fixture の構築が困難 → `tests/helpers/synthetic.py` を先に整備する
- [ ] `estimate_akm` がメモリ不足 → n_persons / n_anime を削減

---

## Completion signal

- [ ] `tests/test_core_scoring_phase.py` が 6 件以上 pass
- [ ] 既存テストに影響なし
- [ ] commit:
  ```
  T01a: Add core_scoring phase unit tests (AKM, dormancy, BiRank, IV)
  ```
