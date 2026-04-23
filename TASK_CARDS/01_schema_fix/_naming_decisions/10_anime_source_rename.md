# Task: `anime.source` → `anime.original_work_type` 列リネーム

**ID**: `01_schema_fix/10_anime_source_rename`
**Priority**: 🟡 Medium
**Estimated changes**: 約 -3 / +5 lines migration + 全 caller 置換
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: `01_schema_fix/02_register_v55_migration` (v55 完了後に v56 で実施)

---

## Goal

`anime.source` 列は「原作種別」(original_work = manga/novel/original/game/…) を保持しているが、
`credits.evidence_source`（クレジットのデータソース）と紛らわしい。
`anime.original_work_type` に物理リネームして曖昧さを解消する。

---

## Hard constraints

- H1 anime.score を scoring に使わない
- H5 全テスト green 維持
- H8 行番号を信じず **関数名で探す**

**本タスク固有**: migration は v56 として登録する（v55 は既に確定済み）。
`anime.source` と `credits.evidence_source` は**別物**— リネーム対象は `anime.source` のみ。

---

## Pre-conditions

- [ ] `git status` が clean（前タスクコミット済み）
- [ ] SCHEMA_VERSION = 55

---

## Changes

### 変更 A: v56 migration を定義・登録

`src/database.py` に `_migrate_v55_to_v56_anime_source_rename` 関数を新規追加
（既存 `_migrate_v55_to_v56` は genres JSON 展開用でそのまま保留のまま変更しない）:

```python
def _migrate_v55_to_v56_anime_source_rename(conn: sqlite3.Connection) -> None:
    """v56: anime.source → anime.original_work_type 物理リネーム."""
    cursor = conn.cursor()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(anime)").fetchall()]
    if "source" in cols and "original_work_type" not in cols:
        conn.executescript("""
            ALTER TABLE anime ADD COLUMN original_work_type TEXT;
            UPDATE anime SET original_work_type = source;
        """)
        # SQLite は DROP COLUMN を v3.35+ でのみサポート; version check:
        import sqlite3 as _sqlite3
        if _sqlite3.sqlite_version_info >= (3, 35, 0):
            conn.execute("ALTER TABLE anime DROP COLUMN source")
        logger.info("anime_source_renamed_to_original_work_type")
    conn.commit()
    _set_schema_version(conn, 56)
```

`migrations` dict に追加:
```python
56: _migrate_v55_to_v56_anime_source_rename,
```

`SCHEMA_VERSION = 56`

### 変更 B: 非 migration コードの `anime.source` 参照を置換

```bash
# init_db() の DDL (CREATE TABLE anime) — source 列を original_work_type に直接リネーム
# upsert_anime() / load_all_anime() — フィールドマッピング更新
grep -rn "anime\.source\b\|a\.source\b\|\"source\"\s*:\s*anime\b" src/ --include="*.py"
```

各箇所を `original_work_type` に変更。`credits.evidence_source` は絶対に触らない。

### 変更 C: テスト更新

```bash
grep -rn "anime\.source\b\|a\.source\b" tests/ --include="*.py"
```

---

## Verification

```bash
pixi run python -c "
import pathlib, tempfile
from src.database import get_connection, init_db, _run_migrations
p = pathlib.Path(tempfile.mktemp(suffix='.db'))
conn = get_connection(p)
init_db(conn)
_run_migrations(conn)
cols = [r[1] for r in conn.execute('PRAGMA table_info(anime)').fetchall()]
assert 'original_work_type' in cols, 'original_work_type missing'
assert 'source' not in cols, 'source still present'
print('OK')
"
pixi run test
```

---

## Commit message

```
Rename anime.source → anime.original_work_type (v56 migration)
```
