# Task: H-1 PoC — analysis_modules を Hamilton 化

**ID**: `05_hamilton/H1_poc_analysis_modules`
**Priority**: 🟠 Major
**Estimated changes**: +400 / -200 lines (src/pipeline_phases/analysis_modules.py + tests)
**Requires senior judgment**: YES — node 分解粒度、executor 選択
**Blocks**: `05_hamilton/H2_phase5_8` (H-1 の中止判定次第)
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

`src/pipeline_phases/analysis_modules.py` の 20+ モジュール呼び出しを **Hamilton DAG node** に変換する。
本カードは **Phase 9 だけ** が対象。他フェーズと `PipelineContext` はそのまま残す。

判断ポイント: H-1 終了時に「DAG 可視化・部分再実行・テスト容易性」が効果を出すか評価。
**効果が出なければ H-2 以降中止、H-1 をロールバックする。**

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score を scoring に使わない
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- `PipelineContext` を削除しない (H-4 で対処)
- 分析アルゴリズムのロジックを変更しない (入出力の配線のみ)
- 現行の `ThreadPoolExecutor` 並列実行と同等以上の性能を維持する
- Rust 拡張 (`animetor_eval_core`) の呼び出しは変更しない

---

## Pre-conditions

- [ ] `01_schema_fix/` 全完了
- [ ] `pixi run test` pass (baseline — 現在の pass 数を記録)
- [ ] `git status` clean

```bash
pixi run test --tb=no -q 2>&1 | tail -3   # baseline pass 数記録
```

---

## 着手前 Senior 決定事項

以下を決定してからカード実装に入ること:

1. **Node 分解粒度**: 1 分析モジュール = 1 node か、モジュール群をまとめるか
   - 推奨: 1 モジュール = 1 node (ユニットテスト容易、再実行粒度最細)
2. **Node 入出力型**: `PipelineContext` を渡すか、フィールドを個別引数に分解するか
   - 推奨: H-1 は `PipelineContext` をそのまま受け取る (移行コスト最小)
3. **Executor**: `ThreadPoolExecutor` をそのまま使う / Hamilton の `executors.ThreadPoolExecutor` に切替
   - 推奨: Hamilton の `executors.ThreadPoolExecutor(max_workers=min(32, cpu_count+4))`
4. **キャッシュ**: `CachingGraphAdapter` は H-1 では使わない (複雑さ増大を避ける)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `pixi.toml` | `sf-hamilton` を `[dependencies]` に追加 |
| `src/pipeline_phases/analysis_modules.py` | Hamilton driver + module 関数化 |
| `src/pipeline_phases/hamilton_modules/` | **新規ディレクトリ** — 各分析モジュールの node 定義 |
| `tests/test_hamilton_poc.py` | **新規** — H-1 専用テスト |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/pipeline_phases/context.py` | `PipelineContext` は H-4 まで残す |
| `src/pipeline_phases/core_scoring.py` | H-2 で対処 |
| `src/analysis/**` | アルゴリズムは変更しない |
| `src/pipeline.py` | H-4 で `Driver` ラッパーに変更 |

---

## Steps

### Step 1: sf-hamilton 依存追加

```bash
pixi add sf-hamilton
pixi run python -c "import hamilton; print(hamilton.__version__)"
```

### Step 2: hamilton_modules ディレクトリ新設

```
src/pipeline_phases/hamilton_modules/
├── __init__.py
├── career.py           # career 系モジュール node
├── network.py          # graph 系モジュール node
├── studio.py           # studio 系モジュール node
└── ...
```

各 node は純粋関数:

```python
# src/pipeline_phases/hamilton_modules/career.py
from src.pipeline_phases.context import PipelineContext
import src.analysis.career as _career

def career_analysis(ctx: PipelineContext) -> dict:
    """Career trajectory analysis node."""
    return _career.analyze(ctx.persons, ctx.credits, ctx.anime_stats)
```

### Step 3: analysis_modules.py を Hamilton Driver 化

```python
# src/pipeline_phases/analysis_modules.py (改訂版)
from hamilton import driver
from hamilton.execution import executors
from src.pipeline_phases import hamilton_modules

def run_analysis_modules(ctx: PipelineContext) -> dict:
    dr = (
        driver.Builder()
        .with_modules(hamilton_modules.career, hamilton_modules.network, ...)
        .with_executor(executors.SynchronousLocalTaskExecutor())  # まず直列で動作確認
        .build()
    )
    results = dr.execute(
        final_vars=["career_analysis", "network_analysis", ...],
        inputs={"ctx": ctx},
    )
    return results
```

直列で動いたら `ThreadPoolExecutor` に切替:

```python
.with_executor(executors.MultiThreadingExecutor(max_workers=min(32, cpu_count + 4)))
```

### Step 4: DAG 可視化確認

```python
dr.display_all_functions("dag_phase9.png")  # SVG/PNG 出力
```

### Step 5: 既存テスト通過確認

```bash
pixi run test --tb=short -q
```

### Step 6: 性能ベンチマーク

```bash
pixi run bench   # H-1 前後比較
```

現行 `ThreadPoolExecutor` より **20% 以上遅くなったら中止判定**。

---

## Verification

```bash
# 1. import 確認
pixi run python -c "from src.pipeline_phases.analysis_modules import run_analysis_modules; print('OK')"

# 2. DAG 可視化
pixi run python -c "
from hamilton import driver
from src.pipeline_phases import hamilton_modules
dr = driver.Builder().with_modules(hamilton_modules.career).build()
dr.display_all_functions('/tmp/h1_dag.png')
print('DAG saved')
"

# 3. テスト
pixi run test

# 4. 性能
pixi run bench

# 5. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
rg 'display_lookup' src/analysis/ src/pipeline_phases/   # 0 件
```

---

## 中止判定 (H-1 終了時に Senior が評価)

以下のいずれかに該当したら **H-2 以降を中止してロールバック**:

- [ ] Hamilton overhead で Phase 9 並列実行が 20% 以上遅くなる
- [ ] 型ヒント + `@node` decorator の可読性が旧 `PipelineContext` より悪い
- [ ] Rust 拡張との統合で顕著な複雑さが出る
- [ ] 既存テストが壊れ、修正コストが高い

---

## Rollback

```bash
git checkout src/pipeline_phases/analysis_modules.py
rm -rf src/pipeline_phases/hamilton_modules/ tests/test_hamilton_poc.py
git checkout pixi.toml pixi.lock
pixi install
pixi run test
```

---

## Completion signal

- [ ] `pixi run test` pass (baseline 以上)
- [ ] `pixi run bench` で現行比 -20% 以内
- [ ] DAG PNG が生成される
- [ ] commit message:
  ```
  H-1: Hamilton PoC — analysis_modules (Phase 9 only)

  PipelineContext and all other phases unchanged. Node granularity: 1 module = 1 node.
  Executor: ThreadPoolExecutor(min(32, cpu+4)). Performance validated vs baseline.
  ```
