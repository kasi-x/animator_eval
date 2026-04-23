# Task: H-4 — PipelineContext 完全削除

**ID**: `05_hamilton/H4_pipeline_context_delete`
**Priority**: 🟠 Major
**Estimated changes**: -300 lines (削除中心)
**Requires senior judgment**: YES — PipelineContext フィールドの全使用箇所を追跡
**Blocks**: `05_hamilton/H5_observability`
**Blocked by**: `05_hamilton/H3_phase1_4`

---

## Goal

`src/pipeline_phases/context.py` の `PipelineContext` dataclass を削除し、
`src/pipeline.py` を Hamilton `Driver` の薄いラッパーに書き換える。

H-3 完了時点で全 node の入出力が明示的になっているため、
`PipelineContext` は単なる「グローバル状態バッグ」として残っているだけ。本カードで撤去する。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score scoring 禁止
- H5 既存テスト green 維持

**本タスク固有**:
- `PipelineContext` を削除する前に、全フィールドが Hamilton node の引数/戻り値に配線されていることを確認する
- CLI の `pixi run pipeline` エントリーポイントの動作を変えない
- `src/pipeline.py` は 50 行以下の薄いラッパーになるはず

---

## Pre-conditions

- [ ] H-3 完了
- [ ] `pixi run test` pass
- [ ] `PipelineContext` の全フィールドが node 入出力に反映済み (H-2/H-3 で確認)
- [ ] `grep -r "PipelineContext" src/ tests/` の結果一覧を取得しておく

```bash
grep -r "PipelineContext" src/ tests/ | wc -l   # 削除前の参照数を記録
```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/context.py` | **削除** |
| `src/pipeline.py` | `Driver` ラッパーに書き換え (50 行以下) |
| `src/pipeline_phases/__init__.py` | `PipelineContext` の export を削除 |
| `tests/conftest.py` | `PipelineContext` fixture を削除/更新 |
| 全 `tests/` | `PipelineContext(...)` の生成箇所を削除 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/**` | アルゴリズム不変 |
| `src/api.py`, `src/cli.py` | パイプラインとは別経路 |

---

## Steps

### Step 1: PipelineContext フィールド追跡

```bash
# 全フィールドを列挙
grep -A 50 "class PipelineContext" src/pipeline_phases/context.py | grep "^\s*\w.*:"

# 各フィールドの使用箇所
grep -rn "ctx\.\w\+" src/pipeline_phases/ src/analysis/
```

各フィールドが Hamilton DAG のどの node に対応するかを確認:
- `ctx.persons` → `raw_persons` node
- `ctx.credits` → `raw_credits` node
- `ctx.graphs` → `collaboration_graph` node
- `ctx.betweenness_cache` → `betweenness_centrality` node
- ... etc.

### Step 2: pipeline.py を Driver ラッパーに書き換え

```python
# src/pipeline.py
"""Pipeline entry point — thin Hamilton Driver wrapper."""
from __future__ import annotations

from pathlib import Path

from hamilton import driver
from hamilton.execution import executors

from src.pipeline_phases.hamilton_modules import (
    loading, resolution, scoring, metrics, analysis
)

JSON_DIR = Path("result/json")   # still needed for export


def run_pipeline(db_path: Path = ..., json_dir: Path = JSON_DIR, ...) -> dict:
    dr = (
        driver.Builder()
        .with_modules(loading, resolution, scoring, metrics, analysis)
        .with_executor(executors.MultiThreadingExecutor(max_workers=...))
        .build()
    )
    return dr.execute(
        final_vars=[...],   # JSON export targets
        inputs={"db_path": db_path, "json_dir": json_dir, ...},
    )
```

### Step 3: context.py 削除

```bash
rm src/pipeline_phases/context.py
```

`ImportError` が出る箇所を全て修正:

```bash
pixi run python -c "from src.pipeline_phases import *"
pixi run test --co -q 2>&1 | grep "ERROR"   # collection error を全修正
```

### Step 4: テストの PipelineContext 生成を削除

```python
# Before:
def test_xxx(tmp_path, monkeypatch):
    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_path / "test.db")
    ctx = PipelineContext(db_path=tmp_path / "test.db", ...)
    run_xxx(ctx)

# After:
def test_xxx(tmp_path):
    result = scoring.person_fixed_effects(
        credits_df=synthetic_credits(),
        anime_df=synthetic_anime(),
    )
    assert ...
```

### Step 5: Phase 10 (export_and_viz) の配線確認

`export_and_viz.py` の `ExportSpec` registry が Driver output node として正しく動くことを確認。

---

## Verification

```bash
# PipelineContext が完全に消えていること
grep -r "PipelineContext" src/ tests/   # 0 件

# パイプライン完走
pixi run pipeline --dry-run 2>&1 | tail -5   # エラーなし

# 全テスト
pixi run test

# monkeypatch 残存確認
grep -r "DEFAULT_DB_PATH" tests/   # 0 件

rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
```

---

## Stop-if conditions

- [ ] `PipelineContext` フィールドが Hamilton node に未対応のものが残る
- [ ] `pixi run pipeline` が起動できない
- [ ] テスト pass 数が大幅減少 (設計漏れがある)

---

## Rollback

```bash
git checkout src/pipeline_phases/context.py src/pipeline.py
git checkout tests/conftest.py tests/
pixi run test
```

---

## Completion signal

- [ ] `grep -r "PipelineContext" src/ tests/` → 0 件
- [ ] `grep -r "DEFAULT_DB_PATH" tests/` → 0 件 (monkeypatch 完全解消)
- [ ] `pixi run pipeline` (small test DB) で完走
- [ ] `pixi run test` pass
- [ ] `src/pipeline.py` が 50 行以下
- [ ] commit:
  ```
  H-4: Delete PipelineContext — pipeline is now a pure Hamilton DAG

  src/pipeline.py reduced to thin Driver wrapper.
  All DEFAULT_DB_PATH monkeypatches eliminated.
  ```
