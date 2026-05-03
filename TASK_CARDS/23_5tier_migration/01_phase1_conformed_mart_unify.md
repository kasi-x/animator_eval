# Task: Phase 1 — Conformed + Mart 統合 file 移行 + rename

**ID**: `23_5tier_migration/01_phase1_conformed_mart_unify`
**Priority**: 🟠
**Estimated changes**: 約 +500 / -300 lines, 30+ files (200+ import path 変更)
**Requires senior judgment**: yes (大規模 rename、merge 競合リスク)
**Blocks**: Phase 2-5 (Resolved 層実装)
**Blocked by**: なし (worktree 隔離で 22/02 / 22/03 と並列可)

---

## Goal

5層 architecture Phase 1 を完遂:
- 旧 `silver.duckdb` / `gold.duckdb` 削除
- 新 `result/animetor.duckdb` に schema 分離で Conformed + Mart 同居
- `silver_loaders/` → `conformed_loaders/`、`silver_reader.py` → `conformed_reader.py`、`gold_writer.py` → `mart_writer.py`
- 全 import / path / config 更新
- tests 全 green 維持

---

## Hard constraints

- H1, H3, H4, H5, H8
- 機能不変 (rename のみ、ロジック変更なし)
- 既存 2161+ tests green 維持

---

## Pre-conditions

- [ ] `git status` clean
- [ ] worktree 隔離で起動 (本 prompt は agent worktree で実行される)
- [ ] `pixi run test` baseline pass (本 worktree 内)

---

## 並列衝突 (注意)

22/02 (anime_studios 残取込) / 22/03 (column coverage 監査) と並走中。これらは:
- `src/etl/silver_loaders/{bangumi,seesaawiki,madb,keyframe}.py` 編集
- `src/etl/audit/silver_column_coverage.py` 新規

本タスクは:
- `src/etl/silver_loaders/*` 全部 rename
- competing changes 必至 → Opus が merge 担当

→ 本 worktree 内で完結させ、bundle 提出。 merge 衝突解決は Opus が main で実施。

---

## 実装範囲

### 1. animetor.duckdb 新規作成

`result/animetor.duckdb` に schema 分離:

```sql
CREATE SCHEMA IF NOT EXISTS conformed;
CREATE SCHEMA IF NOT EXISTS mart;
```

### 2. ETL 書き込み先変更

- `src/etl/conformed_loaders/*` (= 旧 silver_loaders) の全 INSERT 先を `conformed.<table>` に
- `src/analysis/io/mart_writer.py` (= 旧 gold_writer) の全 INSERT 先を `mart.<table>` に
- `src/etl/integrate_duckdb.py` の DDL を schema-qualified に

### 3. ETL 読み込み元変更

- `src/analysis/io/conformed_reader.py` (= 旧 silver_reader) を `animetor.duckdb` の `conformed.*` schema 読み
- 既存 `silver_connect()` → `conformed_connect()` rename
- 既存 `DEFAULT_SILVER_PATH` → `DEFAULT_DB_PATH` 単一化

### 4. config 変更

- `pixi.toml` の DB path 変数
- `Taskfile.yml` 同上
- `src/utils/config.py` の `DEFAULT_*_PATH` 定数

### 5. ファイル rename

- `src/etl/silver_loaders/` → `src/etl/conformed_loaders/`
- `src/analysis/io/silver_reader.py` → `src/analysis/io/conformed_reader.py`
- `src/analysis/io/gold_writer.py` → `src/analysis/io/mart_writer.py`
- `tests/test_silver_reader.py` → `tests/test_conformed_reader.py`
- `tests/test_gold_writer.py` → `tests/test_mart_writer.py`

### 6. import path 一括更新 (200+ 箇所)

```bash
rg -l "silver_loaders\." src/ tests/ scripts/ | xargs sed -i 's|silver_loaders|conformed_loaders|g'
rg -l "silver_reader" src/ tests/ scripts/ | xargs sed -i 's|silver_reader|conformed_reader|g'
rg -l "gold_writer" src/ tests/ scripts/ | xargs sed -i 's|gold_writer|mart_writer|g'
rg -l "silver_connect" src/ tests/ scripts/ | xargs sed -i 's|silver_connect|conformed_connect|g'
```

### 7. 旧 file 削除 (worktree 内では実 file 操作不要、import path 更新のみで OK)

- `git mv silver.duckdb` 不要 (.gitignore 対象、commit 対象外)
- DEFAULT_*_PATH を新 path に更新するだけ

---

## Files to modify

| Pattern | 内容 |
|---------|------|
| `src/etl/silver_loaders/*` | rename + INSERT 先 schema-qualified |
| `src/analysis/io/silver_reader.py` | rename + connect path 更新 |
| `src/analysis/io/gold_writer.py` | rename + connect path 更新 |
| `src/etl/integrate_duckdb.py` | DDL schema-qualified、target file 変更 |
| `pixi.toml` / `Taskfile.yml` | DB path 環境変数 |
| `src/utils/config.py` | `DEFAULT_*_PATH` 定数 |
| `tests/test_*.py` | 全 import path 更新 |

## Files to NOT touch

- `src/analysis/scoring/*` (Phase 3 対象、本タスクは触らない)
- `src/analysis/entity_resolution.py` (H3)
- BRONZE 関連 (`src/scrapers/`, `result/bronze/`)

---

## Steps

### Step 1: 新 schema 定義 (空 file で動作確認)

```python
# 試行
conn = duckdb.connect("result/animetor.duckdb")
conn.execute("CREATE SCHEMA IF NOT EXISTS conformed")
conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
```

### Step 2: ファイル rename

`git mv` で履歴保持:
```bash
git mv src/etl/silver_loaders src/etl/conformed_loaders
git mv src/analysis/io/silver_reader.py src/analysis/io/conformed_reader.py
git mv src/analysis/io/gold_writer.py src/analysis/io/mart_writer.py
```

### Step 3: 各 loader の INSERT 文に schema prefix 追加

`INSERT INTO anime` → `INSERT INTO conformed.anime`
`INSERT INTO scores` → `INSERT INTO mart.scores`

(注: schema-qualified 全置換、search_path 設定でも OK)

### Step 4: 全 import path 更新

```bash
rg -l "silver_loaders" src/ tests/ scripts/ | xargs sed -i 's|silver_loaders|conformed_loaders|g'
rg -l "silver_reader" src/ tests/ scripts/ | xargs sed -i 's|silver_reader|conformed_reader|g'
rg -l "gold_writer" src/ tests/ scripts/ | xargs sed -i 's|gold_writer|mart_writer|g'
rg -l "from src.analysis.io.silver" src/ tests/ scripts/ | xargs sed -i 's|from src.analysis.io.silver|from src.analysis.io.conformed|g'
```

### Step 5: DEFAULT_*_PATH 統一

```python
# src/utils/config.py
DEFAULT_DB_PATH = Path("result/animetor.duckdb")
# 旧 DEFAULT_SILVER_PATH / DEFAULT_GOLD_DB_PATH も DEFAULT_DB_PATH 参照に
```

connect 経路も schema 指定で:
```python
def conformed_connect(read_only=True):
    return duckdb.connect(DEFAULT_DB_PATH, read_only=read_only)
    # クエリ時に conformed.<table> で参照
```

### Step 6: silver.duckdb / gold.duckdb path → animetor.duckdb path 全置換

```bash
rg -l "silver\.duckdb" src/ tests/ scripts/ pixi.toml Taskfile.yml | xargs sed -i 's|silver\.duckdb|animetor.duckdb|g'
rg -l "gold\.duckdb" src/ tests/ scripts/ pixi.toml Taskfile.yml | xargs sed -i 's|gold\.duckdb|animetor.duckdb|g'
```

(注: schema 区別は SQL 内 `conformed.` / `mart.` prefix で行う)

### Step 7: pipeline 動作確認 (worktree 内)

```bash
pixi run python -m src.etl.integrate_duckdb  # 新 animetor.duckdb 構築
pixi run pipeline                              # mart pipeline 実行
```

### Step 8: テスト全 pass 確認

```bash
pixi run lint
pixi run test
```

### Step 9: ARCHITECTURE_5_TIER_PROPOSAL.md の Phase 1 を ✅ に更新

---

## 成果物保全プロトコル (必須)

完了後:
```bash
git add -A
git commit -m "refactor(architecture): Phase 1 — silver/gold → animetor.duckdb conformed+mart"
git bundle create /tmp/agent-bundles/23-01-phase1-rename.bundle HEAD ^main
```

報告に bundle path + HEAD SHA + 主要 rename 一覧 + 22/02/03 との competing changes 警告を含める。

---

## Verification

```bash
pixi run lint
pixi run test
pixi run pipeline  # 新 animetor.duckdb で完走
duckdb result/animetor.duckdb -c "SHOW SCHEMAS"  # conformed / mart 両方存在
```

---

## Stop-if conditions

- [ ] DuckDB schema 機能が想定通り動かない (cross-schema constraint 等)
- [ ] 既存テスト 5+ 件破壊
- [ ] worktree 内 build で animetor.duckdb 生成失敗

---

## Rollback

```bash
git checkout main -- .
# bundle 不要、commit せずに撤退
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] worktree 内で `pixi run pipeline` 完走、新 animetor.duckdb 生成
- [ ] commit + bundle 完了、報告に bundle path + SHA
- [ ] DONE: `23_5tier_migration/01_phase1_conformed_mart_unify`
