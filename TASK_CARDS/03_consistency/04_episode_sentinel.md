# Task: `credits.episode` sentinel `-1` を NULL に正規化

**ID**: `03_consistency/04_episode_sentinel`
**Priority**: 🟠 Major
**Estimated changes**: 約 +10 / -15 lines (code) + migration 1 行, 複数 file
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `01_schema_fix/02_register_v55_migration` (v55 migration が登録されている前提)

---

## Goal

`credits.episode` の sentinel 値 `-1` (= 全話通しクレジット) を `NULL` に置き換える。
- DDL の `DEFAULT -1` を削除
- v55 migration で既存データを一括 UPDATE
- `-1` をチェックしているコードを `IS NULL` に置換

---

## Hard constraints

- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **意味論は変えない**: `episode IS NULL` = 全話通し、`episode > 0` = 特定話数指定、という解釈は既存 `-1` と同じ
- migration は **冪等** (何度走っても結果が同じ) に書く

---

## Pre-conditions

- [ ] `01_schema_fix/` 完了
- [ ] `pixi run test` pass
- [ ] `SCHEMA_VERSION = 55`

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/database.py` (`_migrate_v54_to_v55` 関数) | `UPDATE credits SET episode = NULL WHERE episode = -1` を追加 |
| `src/database.py` (credits テーブル DDL, `init_db()` 内) | `episode INTEGER NOT NULL DEFAULT -1` → `episode INTEGER` に変更 |
| `src/**/*.py` | `episode == -1`, `episode = -1`, `episode < 0` のチェックを `episode IS NULL` に置換 |

---

## Files to NOT touch

- 過去 migration 関数 (`_migrate_v*` の 54 以前) 内の `-1` 参照 (歴史的記録)

---

## Steps

### Step 0: 棚卸し

```bash
# DDL の現状
grep -n 'episode.*DEFAULT.*-1\|episode.*-1' src/database.py | head -10

# コード側の -1 チェック
rg -n 'episode.*==\s*-1|episode.*=\s*-1|episode\s*<\s*0' src/ tests/
```

出力を作業ログに保存。

### Step 1: migration に UPDATE を追加

`_migrate_v54_to_v55` 関数の末尾付近 (既に scores rename, anime_display drop を書いた後) に追加:

```python
    # 5. Normalize credits.episode sentinel -1 → NULL
    cursor.execute("UPDATE credits SET episode = NULL WHERE episode = -1")
    affected = cursor.rowcount
    logger.info("credits_episode_normalized_to_null", rows_updated=affected)
```

### Step 2: DDL を修正

`init_db()` 内の credits テーブル DDL で、episode カラム定義を変更:

**Before**:
```sql
CREATE TABLE IF NOT EXISTS credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL,
    episode INTEGER NOT NULL DEFAULT -1,
    ...
);
```

**After**:
```sql
CREATE TABLE IF NOT EXISTS credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL,
    episode INTEGER,                     -- NULL = 全話通しクレジット (作品レベル)
    ...
);
```

**注意**:
- `NOT NULL` を削除し、`DEFAULT -1` も削除
- UNIQUE 制約 (`UNIQUE(person_id, anime_id, role, episode)` 等) に `episode` が含まれている場合、NULL を含む複合 UNIQUE の SQLite 挙動は「NULL は他の NULL と等しくない」ため、NULL 同士では UNIQUE 違反が起きない点に注意
- これが問題になるテストがあれば `episode` を含めない UNIQUE 制約への移行を検討 (別タスク)

### Step 3: コード側の `-1` チェックを置換

Step 0 で列挙した箇所を Edit ツールで個別に置換:

**Before**:
```python
if credit.episode == -1:
    # all episodes
    ...
```

**After**:
```python
if credit.episode is None:
    # all episodes
    ...
```

SQL 文字列内では:

**Before**:
```sql
WHERE episode = -1
```

**After**:
```sql
WHERE episode IS NULL
```

**Before**:
```sql
WHERE episode != -1
```

**After**:
```sql
WHERE episode IS NOT NULL
```

### Step 4: モデル側 (Pydantic) の更新

`src/models.py` で `Credit` モデルの `episode` フィールド:

**Before**:
```python
episode: int = -1
```

**After**:
```python
episode: int | None = None
```

既に `Optional[int]` や `int | None` になっている場合は変更不要。

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "credits or episode"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile src/database.py src/models.py

# 2. -1 チェックが残っていないこと
rg -n 'episode.*==\s*-1|episode.*=\s*-1|episode\s*<\s*0' src/ tests/ | grep -v '_migrate_v'
# 期待: 0 件 (過去 migration 内は許容)

# 3. DDL の DEFAULT -1 が消えた
rg -n 'episode.*DEFAULT.*-1' src/database.py
# 期待: 0 件

# 4. migration に UPDATE が追加された
rg -n 'UPDATE credits SET episode = NULL' src/database.py
# 期待: 1 件 (_migrate_v54_to_v55 内)

# 5. テスト全件
pixi run test-scoped tests/ -k "credits or episode"

# 6. Lint
pixi run lint

# 7. フレッシュ DB で動作確認
pixi run python -c "
import tempfile, pathlib
from src.database import get_connection, init_db, run_migrations
p = pathlib.Path(tempfile.mktemp(suffix='.db'))
conn = get_connection(p)
init_db(conn)
run_migrations(conn)
# 新規 INSERT で episode 省略 → NULL になること
conn.execute('INSERT INTO persons(id, canonical_name) VALUES(?, ?)', ('p1', 'Test'))
conn.execute('INSERT INTO anime(id, title_ja, title_en) VALUES(?, ?, ?)', ('a1', 'テスト', 'Test'))
conn.execute(\"INSERT INTO credits(person_id, anime_id, role, evidence_source) VALUES('p1','a1','director','anilist')\")
row = conn.execute('SELECT episode FROM credits LIMIT 1').fetchone()
print('episode default:', row[0])
# 期待: None
"

# 8. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] `pixi run test` 失敗
- [ ] UNIQUE 制約違反で INSERT が増える (NULL 挙動の違いで重複扱いが変わった)
- [ ] Verification 7 で `episode default:` が `None` 以外
- [ ] Pydantic モデル変更でテスト失敗

---

## Rollback

```bash
git checkout src/ tests/
pixi run test-scoped tests/ -k "credits or episode"
```

---

## Completion signal

- [ ] Verification 全項目 pass
- [ ] `git diff --stat` が 5-10 files、±30 lines
- [ ] `git commit`:
  ```
  Normalize credits.episode sentinel -1 → NULL

  - v55 migration: UPDATE credits SET episode = NULL WHERE episode = -1
  - DDL: drop NOT NULL + DEFAULT -1 for episode (NULL = 全話通し)
  - Replace `episode == -1` / `episode < 0` with `episode IS NULL`
  - Pydantic: episode: int | None = None
  ```
