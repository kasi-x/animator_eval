# Task: T02 — patronage_dormancy.py 直接テスト

**ID**: `06_tests/T02_patronage_dormancy`
**Priority**: 🟡 Minor
**Estimated changes**: +200 / -0 lines (`tests/test_patronage_dormancy_direct.py` 新規)
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

`src/analysis/scoring/patronage_dormancy.py` の計算ロジックを **純粋関数レベルで直接テスト** する。

現状の問題:
- `tests/test_integrated_value.py` は IV テストでモックを使って dormancy=1.0 に固定している
- 指数減衰 `exp(-δ × max(0, gap - τ))` と猶予期間のロジックが **未検証**
- `compute_patronage_premium()` の段階ウェイト計算が **未検証**

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H5 既存テスト green 維持
- **本タスク固有**: anime.score に依存しない合成データのみ使用

---

## Pre-conditions

- [ ] `pixi run test` pass (baseline)
- [ ] `git status` clean

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `tests/test_patronage_dormancy_direct.py` | **新規作成** |

---

## 対象関数

```python
# src/analysis/scoring/patronage_dormancy.py

compute_dormancy_penalty(
    credits, anime_map, current_year, decay_rate=0.5, grace_period=2.0
) -> dict[str, float]

compute_patronage_premium(
    credits, anime_map, person_roles, ...
) -> dict[str, float]

compute_patronage_and_dormancy(credits, anime_map, ...) -> PatronageDormancyResult

compute_career_aware_dormancy(credits, anime_map, ...) -> dict[str, float]
```

---

## Test Cases

### 1. grace_period 内 → penalty なし (D=1.0)

```python
def test_dormancy_within_grace_period():
    """Active 1 year ago, grace_period=2 → D = exp(0) = 1.0."""
    import math
    credits = [make_credit(person_id="p1", anime_id="a1")]
    anime_map = {"a1": make_anime(anime_id="a1", year=2024)}  # last active 2024
    result = compute_dormancy_penalty(credits, anime_map, current_year=2025, grace_period=2.0)
    assert result["p1"] == pytest.approx(1.0)
```

### 2. gap = grace_period → penalty なし (ちょうど境界)

```python
def test_dormancy_at_grace_boundary():
    """Gap exactly equal to grace_period → D = exp(0) = 1.0."""
    credits = [make_credit("p1", "a1")]
    anime_map = {"a1": make_anime("a1", year=2023)}  # gap = 2 years
    result = compute_dormancy_penalty(credits, anime_map, current_year=2025, grace_period=2.0)
    assert result["p1"] == pytest.approx(1.0, abs=0.01)
```

### 3. gap > grace_period → 指数減衰

```python
def test_dormancy_exponential_decay():
    """gap=5, grace=2, rate=0.5 → D = exp(-0.5 × 3) ≈ 0.223."""
    import math
    credits = [make_credit("p1", "a1")]
    anime_map = {"a1": make_anime("a1", year=2020)}  # gap ≈ 5
    result = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                      decay_rate=0.5, grace_period=2.0)
    expected = math.exp(-0.5 * 3)
    assert result["p1"] == pytest.approx(expected, rel=0.05)
```

### 4. decay_rate が高いほど D が低い

```python
def test_higher_decay_rate_lowers_d():
    credits = [make_credit("p1", "a1")]
    anime_map = {"a1": make_anime("a1", year=2015)}
    d_low = compute_dormancy_penalty(credits, anime_map, current_year=2025, decay_rate=0.1)
    d_high = compute_dormancy_penalty(credits, anime_map, current_year=2025, decay_rate=1.0)
    assert d_low["p1"] > d_high["p1"]
```

### 5. 複数クレジットは最新を使う

```python
def test_dormancy_uses_most_recent_credit():
    """Person has credits in 2010 and 2023 — dormancy based on 2023."""
    credits = [make_credit("p1", "a1"), make_credit("p1", "a2")]
    anime_map = {
        "a1": make_anime("a1", year=2010),
        "a2": make_anime("a2", year=2023),
    }
    result = compute_dormancy_penalty(credits, anime_map, current_year=2025, grace_period=2.0)
    assert result["p1"] == pytest.approx(1.0)  # recent credit within grace
```

### 6. クレジットのない person は結果に含まれない

```python
def test_dormancy_excludes_persons_without_valid_credits():
    """Person with credits on anime with no year are excluded."""
    credits = [make_credit("p1", "a1")]
    anime_map = {"a1": make_anime("a1", year=None)}  # no year
    result = compute_dormancy_penalty(credits, anime_map, current_year=2025)
    assert "p1" not in result
```

### 7. patronage: 繰り返し collaborator がいる場合にスコアが高い

```python
def test_patronage_increases_with_repeat_collaborators():
    """Person worked with same director 3 times → higher patronage than 1 time."""
    single = compute_patronage_premium(
        [make_credit("p1", "a1")],
        {f"a{i}": make_anime(f"a{i}", year=2020+i, director_ids=["d1"]) for i in range(1)},
        ...
    )
    repeat = compute_patronage_premium(
        [make_credit("p1", f"a{i}") for i in range(3)],
        {f"a{i}": make_anime(f"a{i}", year=2020+i, director_ids=["d1"]) for i in range(3)},
        ...
    )
    assert repeat["p1"] >= single["p1"]
```

---

## Helpers

```python
# tests/test_patronage_dormancy_direct.py 内に定義

from src.models import Credit, Role

def make_credit(person_id: str, anime_id: str, role: str = "key_animator") -> Credit:
    return Credit(
        person_id=person_id,
        anime_id=anime_id,
        role=Role(role),
        episode=None,
        evidence_source="anilist",
    )

def make_anime(anime_id: str, year: int | None, episodes: int = 12):
    from src.models import AnimeAnalysis
    return AnimeAnalysis(
        anime_id=anime_id,
        title=f"Anime {anime_id}",
        year=year,
        episodes=episodes,
        format="TV",
        duration=24,
    )
```

---

## Steps

```bash
touch tests/test_patronage_dormancy_direct.py
pixi run python -m pytest tests/test_patronage_dormancy_direct.py -v
pixi run test
pixi run lint
```

---

## Verification

```bash
pixi run python -m pytest tests/test_patronage_dormancy_direct.py -v
# 期待: 7+ passed (全 test case)

rg 'anime\.score\b' tests/test_patronage_dormancy_direct.py   # 0 件
```

---

## Stop-if conditions

- [ ] `PatronageDormancyResult` のフィールド名が変わっている → ファイルを読んで確認
- [ ] `AnimeAnalysis` のフィールドが異なる → `src/models.py` を参照

---

## Completion signal

- [ ] `tests/test_patronage_dormancy_direct.py` が 7 件以上 pass
- [ ] 既存 IV テストが壊れていない
- [ ] commit:
  ```
  T02: Add patronage_dormancy direct tests (exponential decay, grace period, patronage)
  ```
