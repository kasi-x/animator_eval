# Task: T03 — VA モジュール テスト (7 analysis + 4 pipeline phases)

**ID**: `06_tests/T03_va_modules`
**Priority**: 🟡 Minor
**Estimated changes**: +350 / -0 lines (複数テストファイル新規)
**Requires senior judgment**: no (既存 test_akm.py パターン踏襲)
**Blocks**: なし
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

VA (Voice Actor) 関連モジュールの全 11 ファイルにユニットテストを追加する。
現状: `tests/test_va_modules.py` が存在するが空 or 最小限。

### 対象ファイル (11 件)

**analysis モジュール** (`src/analysis/va/`):
| ファイル | 主要関数 |
|---------|---------|
| `akm.py` | `estimate_va_akm(va_credits, char_va_map, sound_directors)` → `VAAKMResult` |
| `character_diversity.py` | `compute_character_diversity(va_credits, anime_map)` → `dict[str, float]` |
| `ensemble_synergy.py` | `compute_ensemble_synergy(va_credits, anime_map)` → `dict[str, float]` |
| `graph.py` | `build_va_collaboration_graph(va_credits, anime_map)` → `nx.Graph` |
| `integrated_value.py` | `compute_va_iv(akm_result, ...)` → `dict[str, float]` |
| `replacement_difficulty.py` | `compute_replacement_difficulty(va_credits, char_map)` → `dict[str, float]` |
| `trust.py` | `compute_va_trust(va_credits, sound_directors)` → `dict[str, float]` |

**pipeline phases** (`src/pipeline_phases/`):
| ファイル | 主要関数 |
|---------|---------|
| `va_core_scoring.py` | `compute_va_core_scores(ctx)` |
| `va_graph_construction.py` | `build_va_graphs(ctx)` |
| `va_result_assembly.py` | `assemble_va_results(ctx)` |
| `va_supplementary_metrics.py` | `compute_va_supplementary(ctx)` |

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score を使わない
- H5 既存テスト green 維持

---

## Pre-conditions

- [ ] `pixi run test` pass (baseline)
- [ ] `git status` clean

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `tests/test_va_modules.py` | 拡充 (または新規) |
| `tests/test_va_pipeline_phases.py` | **新規** — pipeline phase テスト |

---

## Test Cases (優先度順)

### VA AKM (最重要 — 補償根拠)

```python
class TestVaAkm:
    def _make_va_data(self):
        """Synthetic: 3 VAs, 2 sound directors, 3 anime, 10 character_va records."""
        ...

    def test_va_akm_returns_result(self):
        from src.analysis.va.akm import estimate_va_akm
        data = self._make_va_data()
        result = estimate_va_akm(**data)
        assert hasattr(result, "person_fe")
        assert hasattr(result, "studio_fe")  # sound director FE
        assert len(result.person_fe) > 0

    def test_va_person_fe_range(self):
        from src.analysis.va.akm import estimate_va_akm
        result = estimate_va_akm(**self._make_va_data())
        for pid, fe in result.person_fe.items():
            assert -15 < fe < 15

    def test_va_akm_single_va_does_not_crash(self):
        """Edge case: only 1 VA in dataset."""
        from src.analysis.va.akm import estimate_va_akm
        ...
```

### VA Graph

```python
class TestVaGraph:
    def test_graph_has_va_nodes(self):
        from src.analysis.va.graph import build_va_collaboration_graph
        g = build_va_collaboration_graph(synthetic_va_credits, synthetic_anime_map)
        assert g.number_of_nodes() > 0

    def test_graph_edge_weight_positive(self):
        g = build_va_collaboration_graph(...)
        for _, _, d in g.edges(data=True):
            assert d.get("weight", 0) > 0
```

### VA Integrated Value

```python
class TestVaIntegratedValue:
    def test_iv_non_negative(self):
        from src.analysis.va.integrated_value import compute_va_iv
        akm_result = make_va_akm_result(n_vas=5)
        iv = compute_va_iv(akm_result, ...)
        assert all(v >= 0 for v in iv.values())

    def test_iv_zero_when_dormant(self):
        """D=0 → IV=0 for all VAs."""
        iv = compute_va_iv(..., dormancy={pid: 0.0 for pid in pids})
        assert all(v == pytest.approx(0.0) for v in iv.values())
```

### Replacement Difficulty

```python
def test_replacement_difficulty_unique_roles_higher():
    """VA specializing in rare character types has higher replacement difficulty."""
    ...

def test_replacement_difficulty_range():
    """Scores should be in [0, 1] range after normalization."""
    from src.analysis.va.replacement_difficulty import compute_replacement_difficulty
    result = compute_replacement_difficulty(...)
    for pid, score in result.items():
        assert 0.0 <= score <= 1.0
```

### Character Diversity & Ensemble Synergy

```python
def test_character_diversity_single_type_low():
    """VA who only voices one character type has low diversity."""
    ...

def test_ensemble_synergy_repeated_cast_higher():
    """VAs who repeatedly work together have higher synergy."""
    ...
```

### Pipeline Phases

```python
def test_va_core_scores_runs_without_error(minimal_va_context):
    from src.pipeline_phases.va_core_scoring import compute_va_core_scores
    compute_va_core_scores(minimal_va_context)  # should not raise

def test_va_graphs_populated(minimal_va_context):
    from src.pipeline_phases.va_graph_construction import build_va_graphs
    build_va_graphs(minimal_va_context)
    assert minimal_va_context.va_graph is not None
```

---

## Fixtures

```python
@pytest.fixture
def synthetic_va_credits():
    """3 VAs, 5 anime, 15 character_voice_actor records."""
    from src.models import CharacterVoiceActor
    return [
        CharacterVoiceActor(
            character_id=f"c{i}",
            person_id=f"va{i % 3}",
            anime_id=f"a{i % 5}",
            role="main",
        )
        for i in range(15)
    ]

@pytest.fixture
def minimal_va_context(tmp_path, synthetic_va_credits):
    """PipelineContext with VA data seeded."""
    ...
```

---

## Steps

```bash
# 既存ファイルを確認してから実装
cat tests/test_va_modules.py

# VA モジュールの関数シグネチャを確認
grep -n "^def \|^class " src/analysis/va/*.py

# テスト実装
pixi run python -m pytest tests/test_va_modules.py tests/test_va_pipeline_phases.py -v

# 全テスト
pixi run test && pixi run lint
```

---

## Verification

```bash
pixi run python -m pytest tests/test_va_modules.py -v
# 期待: 12+ passed

pixi run python -m pytest tests/test_va_pipeline_phases.py -v
# 期待: 4+ passed (各 pipeline phase × 1 smoke test)

rg 'anime\.score\b' tests/test_va_modules.py tests/test_va_pipeline_phases.py   # 0 件
```

---

## Stop-if conditions

- [ ] `CharacterVoiceActor` / `VAAKMResult` のフィールドが変わっている → `src/models.py` と `src/analysis/va/akm.py` を参照して修正
- [ ] VA pipeline phases が `PipelineContext` を大幅に変更している → H-2/H-3 と衝突。順序を調整

---

## Completion signal

- [ ] `tests/test_va_modules.py` が 12 件以上 pass
- [ ] `tests/test_va_pipeline_phases.py` が 4 件以上 pass
- [ ] 既存テストに影響なし
- [ ] commit:
  ```
  T03: Add VA module tests (AKM, graph, IV, diversity, synergy, pipeline phases)
  ```
