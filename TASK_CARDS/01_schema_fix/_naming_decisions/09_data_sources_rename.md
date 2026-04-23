# Task: `data_sources` テーブルを役割明示名に改名

**ID**: `01_schema_fix/09_data_sources_rename`
**Priority**: 🔴 Critical (`sources` との命名衝突解消)
**Estimated changes**: 約 +10 / -5 lines (migration) + 参照置換 ~10 lines, 複数 file
**Requires senior judgment**: **minor** (新名の選定だけ)
**Blocks**: (なし)
**Blocked by**: `01_schema_fix/06_v56_defer_comment` (01-06 完了後、07 / 08 と並行可)

---

## Goal

`data_sources` テーブルを `source_scrape_status` に物理リネーム。`sources` (canonical lookup、PK=code) との役割混同を解消し、運用状態テーブルであることを名前で表す。

---

## 背景

現状 2 テーブルが並存し混乱:

| テーブル | 内容 | 性質 |
|---------|------|-----|
| `sources` (PK=code) | code / name_ja / base_url / license / description | **lookup** (canonical、静的) |
| `data_sources` (PK=source) | source / last_scraped_at / item_count / status | **運用状態** (scraper 履歴) |

両者は**目的が違う**が名前が似通っており、新規読者が誤読する。`data_sources` の中身は scraper の同期状態なので `source_scrape_status` が意味的に正確。

**参照箇所**: `src/api.py:43, 1027-1028`, `src/monitoring.py:9, 53`, `src/database.py` 内の `get_data_sources()` 関数。

---

## Hard constraints

- H5 全テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **`sources` テーブルは一切触らない** (canonical lookup、01/02 で既に整理済み)
- 関数名 `get_data_sources` → `get_source_scrape_status` にリネーム
- API レスポンスで `{"data_sources": [...]}` として返している箇所があれば、**後方互換のため key はそのまま** or 新 key `source_scrape_status` と両方返す(判断要)

---

## Pre-conditions

- [ ] `01_schema_fix/01`〜`06` 完了
- [ ] `git status` clean
- [ ] `SCHEMA_VERSION = 55`
- [ ] `sources` テーブルが正常存在(`01/02` 完了確認):
  ```bash
  pixi run python -c "
  import sqlite3
  conn = sqlite3.connect('result/animetor.db')
  print(conn.execute(\"SELECT COUNT(*) FROM sources\").fetchone())
  print(conn.execute(\"SELECT COUNT(*) FROM data_sources\").fetchone())
  "
  # 期待: sources count >= 5, data_sources count = 実スクレイプ数
  ```

---

## Files to modify

### グループ A: migration (v55 に追加)
| File | 変更内容 |
|------|---------|
| `src/database.py` (`_migrate_v54_to_v55` 関数) | `ALTER TABLE data_sources RENAME TO source_scrape_status` を追加 |

### グループ B: DDL
| File | 変更内容 |
|------|---------|
| `src/database.py` (`init_db` 内) | `CREATE TABLE IF NOT EXISTS data_sources` → `source_scrape_status` |

### グループ C: 関数名 + 参照
| File | 変更内容 |
|------|---------|
| `src/database.py` | `def get_data_sources(conn)` → `def get_source_scrape_status(conn)` |
| `src/api.py` (line 43, 1027) | `from ... import get_data_sources` + 呼び出し |
| `src/monitoring.py` (line 9, 53) | 同上 |

### グループ D (判断要): API レスポンス key
| File | 変更内容 |
|------|---------|
| `src/api.py:1028` | `{"stats": stats, "data_sources": sources}` の key 名をどうするか決定 |

---

## Files to NOT touch

| File / 場所 | 理由 |
|------|------|
| `sources` テーブル | 別概念、別タスク(01/02)で整理済み |
| 過去 migration 関数 (`_migrate_v*` の v ≤ 54) | 歴史的記録 |
| 外部 API consumer(フロント等) | Group D で後方互換 key を残すことで影響なし |

---

## Steps

### Step 0: 対象棚卸し

```bash
# テーブル参照
rg -n '\bdata_sources\b' src/ tests/

# 関数
grep -n 'def get_data_sources\|get_data_sources(' src/

# API レスポンス
grep -n '"data_sources"' src/
```

出力を作業ログに保存。

### Step 1: migration に RENAME を追加

`_migrate_v54_to_v55` 関数の末尾付近に追加:

```python
    # 7. Rename data_sources → source_scrape_status (disambiguate from sources lookup)
    existing_data_sources = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='data_sources'"
    ).fetchone()
    if existing_data_sources:
        cursor.execute("ALTER TABLE data_sources RENAME TO source_scrape_status")
        logger.info("data_sources_renamed_to_source_scrape_status")
```

### Step 2: `init_db()` の DDL 更新

`CREATE TABLE IF NOT EXISTS data_sources (...)` を `source_scrape_status` にリネーム。スキーマ内容は一切変えない。

### Step 3: `get_data_sources` 関数のリネーム

```bash
grep -n 'def get_data_sources' src/database.py
```

Edit ツールで:
```python
def get_data_sources(conn) -> list[dict]:
    ...
    rows = conn.execute("SELECT * FROM data_sources").fetchall()
    ...
```
を:
```python
def get_source_scrape_status(conn) -> list[dict]:
    """Return scrape sync state per source (last_scraped_at, item_count, status)."""
    ...
    rows = conn.execute("SELECT * FROM source_scrape_status").fetchall()
    ...
```
に変更。

### Step 4: 呼び出し側の import と呼び出しを更新

`src/api.py`, `src/monitoring.py` で:

**Before**:
```python
from src.database import get_data_sources
...
sources = get_data_sources(conn)
```

**After**:
```python
from src.database import get_source_scrape_status
...
scrape_status = get_source_scrape_status(conn)
```

### Step 5: API レスポンス key の判断

`src/api.py:1028` 付近:

```python
return {"stats": stats, "data_sources": sources}
```

**選択肢**:
1. **後方互換重視**: key `data_sources` はそのまま維持(外部フロント依存を壊さない)
2. **きれいに**: key を `source_scrape_status` に変更

**推奨は選択肢 1** (破壊的変更を避ける)。理由を **コメントで明示**:
```python
return {
    "stats": stats,
    # NOTE: key kept as 'data_sources' for backward compatibility with
    # existing API consumers. Underlying table was renamed to
    # source_scrape_status in schema v55.
    "data_sources": scrape_status,
}
```

### Step 6: 変数名の整合

Step 4 で `sources = ...` という変数名になっていた箇所が `sources` (canonical lookup) と紛らわしいので、`scrape_status` など別名に:

```python
scrape_status = get_source_scrape_status(conn)
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact`
- **T2 (失敗直後)**: `pixi run test-quick`
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "sources or monitoring or api"`
- **T4 (commit 直前 1 回)**: `pixi run test`

```bash
# 1. 構文
python -m py_compile src/database.py src/api.py src/monitoring.py

# 2. data_sources 参照が非 migration コードから消えた
rg -n '\bdata_sources\b' src/ --type py | grep -v '_migrate_v' | grep -v '# NOTE' | grep -v 'data_sources".*for backward'
# 期待: 0 件
# (API response の後方互換 key "data_sources" はコメント付きで残存可)

# 3. 新テーブル名が存在
rg -n 'source_scrape_status' src/
# 期待: init_db DDL + migration + 関数定義 + 呼び出し = 4+ 箇所

# 4. 旧関数名が消えた (API response key 以外)
rg -n 'def get_data_sources\b|get_data_sources\(' src/
# 期待: 0 件

# 5. 新関数名が import / 呼ばれる
rg -n 'get_source_scrape_status' src/
# 期待: 3+ 件 (定義 + api.py + monitoring.py)

# 6. テスト
pixi run test-scoped tests/ -k "sources or monitoring or api"

# 7. Lint
pixi run lint

# 8. フレッシュ DB 確認
pixi run python -c "
import tempfile, pathlib
from src.database import get_connection, init_db, run_migrations
p = pathlib.Path(tempfile.mktemp(suffix='.db'))
conn = get_connection(p)
init_db(conn)
run_migrations(conn)
rows = conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('sources','data_sources','source_scrape_status')\").fetchall()
print('tables found:', sorted(r[0] for r in rows))
# 期待: ['source_scrape_status', 'sources']
"

# 9. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] `sources` テーブルが誤って変更された(verification 8 で `sources` が missing)
- [ ] テスト失敗
- [ ] Verification 8 で `data_sources` が残存 or `source_scrape_status` が作られていない
- [ ] 外部フロントのテスト(もしあれば)が `data_sources` key を期待して失敗 → Group D で key を元に戻す

---

## Rollback

```bash
git checkout src/
pixi run test-scoped tests/ -k "sources or monitoring"
```

---

## Completion signal

- [ ] Verification 全項目 pass
- [ ] `git diff --stat`: 4-6 files、約 ±30 lines
- [ ] `git commit`:
  ```
  Rename data_sources → source_scrape_status (disambiguate from sources)

  The `sources` lookup (canonical, PK=code) and `data_sources`
  (scrape sync state) had collidingly similar names despite serving
  different purposes. The latter is renamed to express its role.

  Python helper renamed: get_data_sources → get_source_scrape_status.
  API response key kept as 'data_sources' for backward compatibility
  (documented via comment).
  ```
