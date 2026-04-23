# Task: H-5 — 観測・運用機能

**ID**: `05_hamilton/H5_observability`
**Priority**: 🟡 Minor
**Estimated changes**: +150 / -0 lines (追加のみ)
**Requires senior judgment**: partial (tag 設計 / Hamilton UI 採否)
**Blocks**: なし
**Blocked by**: `05_hamilton/H4_pipeline_context_delete`

---

## Goal

Hamilton DAG に observability (タグ付け / 実行時間計測 / 部分再実行 CLI) を追加する。
`PipelineContext` の頃に grep 頼みだった「どのフェーズが何秒かかったか」を
宣言的・可視的にする。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H5 既存テスト green 維持
- **本タスク固有**: Hamilton UI はオプション (別プロセス、CI 非組込)

---

## Pre-conditions

- [ ] H-4 完了
- [ ] `pixi run test` pass
- [ ] `git status` clean

---

## Scope

### 必須 (このカードで実装)
1. `@tag` 付与 — 各 node に `stage` / `cost` タグ
2. 実行時間計測 adapter — Hamilton lifecycle hook で各 node の実行時間を structlog に記録
3. 部分再実行 CLI — `pixi run pipeline-node <node_name>`

### オプション (Senior 判断で採否)
4. Hamilton UI — `hamilton-ui` パッケージ導入 (localhost:8241 でDAG可視化)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/hamilton_modules/*.py` | 各 node に `@tag` 追加 |
| `src/pipeline_phases/lifecycle.py` | **新規** — timing lifecycle hook |
| `src/pipeline.py` | Driver に lifecycle hook を登録 |
| `src/cli.py` | `pipeline-node` サブコマンド追加 |
| `pixi.toml` | `hamilton-ui` (optional feature) |

---

## Steps

### Step 1: @tag 付与

```python
# src/pipeline_phases/hamilton_modules/scoring.py
from hamilton.function_modifiers import tag

@tag(stage="phase5", cost="expensive", domain="scoring")
def person_fixed_effects(credits_df: pd.DataFrame, anime_df: pd.DataFrame) -> AKMResult:
    ...

@tag(stage="phase5", cost="moderate", domain="scoring")
def integrated_value(...) -> pd.Series:
    ...
```

タグ設計:
- `stage`: `phase1`...`phase9`
- `cost`: `cheap` / `moderate` / `expensive`
- `domain`: `loading` / `resolution` / `scoring` / `metrics` / `analysis`

### Step 2: 実行時間計測 lifecycle hook

```python
# src/pipeline_phases/lifecycle.py
import time
import structlog
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook

log = structlog.get_logger()

class TimingHook(NodeExecutionHook):
    def run_before_node_execution(self, *, node_name: str, node_tags: dict, **kw):
        self._start = time.perf_counter()

    def run_after_node_execution(self, *, node_name: str, node_tags: dict, **kw):
        elapsed = time.perf_counter() - self._start
        log.info(
            "node_executed",
            node=node_name,
            stage=node_tags.get("stage"),
            cost=node_tags.get("cost"),
            elapsed_s=round(elapsed, 3),
        )
```

```python
# src/pipeline.py
from src.pipeline_phases.lifecycle import TimingHook

dr = driver.Builder()
    ...
    .with_adapters(TimingHook())
    .build()
```

### Step 3: 部分再実行 CLI

```python
# src/cli.py (追加)
@app.command("pipeline-node")
def pipeline_node(
    node_name: str = typer.Argument(..., help="Hamilton node name to execute"),
    db: Path = typer.Option(DEFAULT_DB_PATH),
):
    """Execute a single Hamilton node and print its output."""
    from src.pipeline import build_driver
    dr = build_driver(db_path=db)
    result = dr.execute([node_name], inputs={"db_path": db})
    typer.echo(f"{node_name}: {result[node_name]}")
```

```bash
# 使用例
pixi run python -m src.cli pipeline-node person_fixed_effects --db result/animetor.db
pixi run python -m src.cli pipeline-node collaboration_graph
```

`pixi.toml` に alias 追加:
```toml
[feature.pipeline.tasks.pipeline-node]
cmd = "python -m src.cli pipeline-node"
```

### Step 4: Hamilton UI (オプション)

Senior が採用を決めた場合:

```bash
pixi add hamilton-ui --feature=dev
hamilton-ui &   # localhost:8241 でブラウザ可視化
```

```python
# src/pipeline.py (条件付き)
from hamilton.plugins import h_ray  # or hamilton_ui
```

---

## Verification

```bash
# タグ確認
pixi run python -c "
from hamilton import driver
from src.pipeline_phases.hamilton_modules import scoring
dr = driver.Builder().with_modules(scoring).build()
for name, node in dr.graph.get_nodes().items():
    print(name, node.tags)
"

# 実行時間ログ確認 (小さい DB で)
pixi run pipeline 2>&1 | grep "node_executed" | head -10

# 部分再実行
pixi run pipeline-node collaboration_graph

# 全テスト
pixi run test

rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
```

---

## Stop-if conditions

- [ ] lifecycle hook が全体の性能を 5% 以上低下させる
- [ ] `pipeline-node` CLI がエラーなく動かない

---

## Completion signal

- [ ] 全 node に `stage` / `cost` / `domain` タグが付いている
- [ ] `pixi run pipeline` のログに `node_executed` エントリーが出る
- [ ] `pixi run pipeline-node <node>` が動作する
- [ ] `pixi run test` pass
- [ ] commit:
  ```
  H-5: Observability — @tag, TimingHook, pipeline-node CLI

  Each node now carries stage/cost/domain tags.
  TimingHook logs execution time per node via structlog.
  New `pixi run pipeline-node <name>` for partial re-execution.
  ```
