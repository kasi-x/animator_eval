# Task: H-3 — Phase 1-4 を Hamilton 化

**ID**: `05_hamilton/H3_phase1_4`
**Priority**: 🟠 Major
**Estimated changes**: +500 / -350 lines
**Requires senior judgment**: YES — entity_resolution の 5-step を node 分解する粒度
**Blocks**: `05_hamilton/H4_pipeline_context_delete`
**Blocked by**: `05_hamilton/H2_phase5_8`

---

## Goal

`data_loading.py` / `validation.py` / `entity_resolution.py` / `graph_construction.py`
の 4 Phase (1/2/3/4) を Hamilton node に変換する。

H-3 完了時点でパイプライン全体 (Phase 1-9) が Hamilton DAG になる。
`PipelineContext` はまだ残す (H-4 で削除)。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score scoring 禁止
- H3 entity_resolution の false positive は欠陥 — 名前統合ロジックを変えない
- H5 既存テスト green 維持

**本タスク固有**:
- entity_resolution の 5-step (exact / cross-source / romaji / similarity / AI) 分解は
  **アルゴリズム変更なし** — 各 step を node に配線するだけ
- graph 構築の edge weight 計算を変えない (`role_weight × episode_coverage × duration_mult`)
- `DEFAULT_DB_PATH` のテストパッチポイントを `data_loading` node の `db_path: Path` 引数に移す

---

## Pre-conditions

- [ ] H-2 完了
- [ ] `pixi run test` pass
- [ ] `git status` clean

---

## 着手前 Senior 決定事項

1. **entity_resolution node 粒度**: 5-step 全体を 1 node にするか、step ごとに分解するか
   - 推奨: step ごと (各 step が独立テスト可能、AI step を条件付き実行しやすい)
2. **DB 接続 node**: `data_loading` node が `db_path: Path` を受け取り connection を返す設計
   - テスト時は `tmp_path / "test.db"` を渡すだけ → monkeypatch 不要
3. **graph node**: `graph_construction` node は `nx.Graph` を返す

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/data_loading.py` | `load_data(db_path: Path) -> DataBundle` node 化 |
| `src/pipeline_phases/validation.py` | `validate_data(bundle: DataBundle) -> DataBundle` node 化 |
| `src/pipeline_phases/entity_resolution.py` | 5-step を個別 node 化 |
| `src/pipeline_phases/graph_construction.py` | `build_graph(bundle: DataBundle) -> nx.Graph` node 化 |
| `src/pipeline_phases/hamilton_modules/loading.py` | **新規** |
| `src/pipeline_phases/hamilton_modules/resolution.py` | **新規** — entity resolution nodes |
| `tests/test_hamilton_phase1_4.py` | **新規** |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/database.py` | DB スキーマは別タスク |
| `src/analysis/` 内の entity resolution ロジック | アルゴリズム不変 |
| `src/pipeline_phases/context.py` | H-4 で削除 |

---

## Steps

### Step 1: data_loading node

```python
# hamilton_modules/loading.py
from pathlib import Path
import sqlite3

def db_connection(db_path: Path) -> sqlite3.Connection:
    """Open DB connection. Tests pass tmp_path/test.db directly."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def raw_credits(db_connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM credits", db_connection)

def raw_persons(db_connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM persons", db_connection)

def raw_anime(db_connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM anime", db_connection)
```

### Step 2: validation node

```python
def validated_credits(raw_credits: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing person_id or anime_id."""
    ...

def validated_anime(raw_anime: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing title or year."""
    ...
```

### Step 3: entity_resolution nodes (5-step)

```python
def exact_match_clusters(raw_persons: pd.DataFrame) -> dict[int, int]: ...
def cross_source_clusters(exact_match_clusters: dict, ...) -> dict[int, int]: ...
def romaji_clusters(cross_source_clusters: dict, ...) -> dict[int, int]: ...
def similarity_clusters(romaji_clusters: dict, ...) -> dict[int, int]: ...
def canonical_person_map(similarity_clusters: dict) -> dict[int, int]:
    """Final: person_id → canonical_id mapping."""
    ...
```

AI step は optional — `hamilton.htypes.Optional` か config フラグで制御:

```python
def ai_clusters(similarity_clusters: dict, use_ai: bool = False) -> dict[int, int]:
    if not use_ai:
        return similarity_clusters
    ...
```

### Step 4: graph_construction node

```python
def collaboration_graph(
    validated_credits: pd.DataFrame,
    canonical_person_map: dict[int, int],
    validated_anime: pd.DataFrame,
) -> nx.Graph:
    """Build collaboration graph with role_weight × episode_coverage × duration_mult edges."""
    ...
```

### Step 5: テスト移行の確認

```python
# Before (monkeypatch地獄):
def test_data_loading(tmp_path, monkeypatch):
    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_path / "test.db")
    ctx = PipelineContext(...)
    run_data_loading(ctx)

# After (node 直接テスト):
def test_raw_credits(tmp_path):
    conn = loading.db_connection(tmp_path / "test.db")
    # seed test data into conn
    result = loading.raw_credits(conn)
    assert isinstance(result, pd.DataFrame)
```

---

## Verification

```bash
pixi run test

# monkeypatch count 減少確認
grep -r "monkeypatch.setattr.*DEFAULT_DB_PATH" tests/ | wc -l   # 減っているはず

# 全 DAG 可視化
pixi run python -c "
from hamilton import driver
from src.pipeline_phases.hamilton_modules import loading, resolution, scoring, metrics, analysis
dr = driver.Builder().with_modules(loading, resolution, scoring, metrics, analysis).build()
dr.display_all_functions('/tmp/h3_full_dag.png')
print('Full DAG (Phase 1-9) saved')
"

rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
```

---

## Stop-if conditions

- [ ] entity_resolution の false positive/negative が変化する (テストで検出)
- [ ] graph edge weights が変化する
- [ ] monkeypatch の削減が期待より少ない (設計を再考)

---

## Rollback

```bash
git checkout src/pipeline_phases/data_loading.py \
    src/pipeline_phases/validation.py \
    src/pipeline_phases/entity_resolution.py \
    src/pipeline_phases/graph_construction.py
rm -f src/pipeline_phases/hamilton_modules/loading.py \
      src/pipeline_phases/hamilton_modules/resolution.py \
      tests/test_hamilton_phase1_4.py
pixi run test
```

---

## Completion signal

- [ ] `pixi run test` pass
- [ ] Phase 1-9 の全 DAG PNG が生成される
- [ ] `DEFAULT_DB_PATH` monkeypatch の使用箇所が H-3 前の 50% 以下に減少
- [ ] `tests/test_hamilton_phase1_4.py` で entity_resolution の各 step が独立テスト可能
- [ ] commit:
  ```
  H-3: Hamilton — Phase 1-4 (loading, validation, entity_resolution, graph)

  db_connection node eliminates DEFAULT_DB_PATH monkeypatch.
  entity_resolution 5-step split into individual nodes.
  Full DAG (Phase 1-9) now visualizable.
  ```
