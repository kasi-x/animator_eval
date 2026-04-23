# Task: `va_scores` → `voice_actor_scores` 物理リネーム

**ID**: `01_schema_fix/08_va_scores_rename`
**Priority**: 🔴 Critical (命名対称性、略語濫用の解消)
**Estimated changes**: 約 +10 / -5 lines (migration) + 参照置換 ~15 lines, 複数 file
**Requires senior judgment**: no (機械的置換、`03_scores_rename` と同じパターン)
**Blocks**: (なし)
**Blocked by**: `01_schema_fix/06_v56_defer_comment` (01-06 完了後、07 と並行実行可)

---

## Goal

VA (voice actor) 専用 score テーブル `va_scores` を `voice_actor_scores` に物理リネーム。
- `person_scores` (animator/director 用) と対称な命名にする
- "va" という略語の初見読者不親切さを解消

---

## Hard constraints

- H5 全テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- `03_scores_rename` と同じ**物理リネームパターン**を使う
- `VAScoreResult` (Pydantic クラス、`src/models.py`) は `VoiceActorScoreResult` に改名してもよい(別タスクでも可)。本タスクでは **DB テーブル名と SQL 文字列参照のみ**対象
- 変数名 `va_score`, `va_scores_dict` 等は本タスクではスコープ外 (命名の一貫性としては別タスクで)

---

## Pre-conditions

- [ ] `01_schema_fix/01`〜`06` 完了・コミット済み
- [ ] `01_schema_fix/07` と並行実行可(互いに独立)
- [ ] `git status` clean
- [ ] `SCHEMA_VERSION = 55`
- [ ] `pixi run test-scoped tests/ -k "migration or schema or va"` pass

---

## Files to modify

### グループ A: migration (v55 に追加)
| File | 変更内容 |
|------|---------|
| `src/database.py` (`_migrate_v54_to_v55` 関数) | `ALTER TABLE va_scores RENAME TO voice_actor_scores` を追加 |

### グループ B: DDL
| File | 変更内容 |
|------|---------|
| `src/database.py` (`init_db` 内) | `CREATE TABLE IF NOT EXISTS va_scores` → `voice_actor_scores` に変更 |

### グループ C: SQL 文字列参照
| File | 対象 |
|------|-----|
| `src/` 配下 | `FROM va_scores`, `INTO va_scores`, `TABLE va_scores` を `voice_actor_scores` に置換 |
| `tests/` 配下 | 同上 |

---

## Files to NOT touch

| File / パターン | 理由 |
|------|------|
| `src/database.py` の `_migrate_v*` (v54 以前) | 過去 migration 内の `va_scores` は歴史的記録として残す |
| `VAScoreResult` クラス | 本タスクでは Pydantic モデル名を変えない(別タスクで) |
| Python 変数名 `va_score`, `va_scores_*` | 本タスクではコード内変数名を変えない |

---

## Steps

### Step 0: 対象棚卸し

```bash
# SQL 文字列内の va_scores テーブル参照を列挙
rg -n '\bFROM va_scores\b|\bINTO va_scores\b|\bTABLE va_scores\b' src/ tests/
rg -n "'va_scores'" src/ tests/

# DDL も確認
grep -n 'CREATE TABLE IF NOT EXISTS va_scores' src/database.py
```

出力を作業ログに保存。

### Step 1: migration に RENAME を追加

`_migrate_v54_to_v55` 関数の末尾付近(scores→person_scores リネームの後、anime_display drop の後)に追加:

```python
    # 6. Rename va_scores → voice_actor_scores for naming symmetry with person_scores
    existing_va_scores = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='va_scores'"
    ).fetchone()
    if existing_va_scores:
        cursor.execute("ALTER TABLE va_scores RENAME TO voice_actor_scores")
        logger.info("va_scores_renamed_to_voice_actor_scores")
```

### Step 2: `init_db()` 内の DDL を更新

```bash
grep -n 'CREATE TABLE IF NOT EXISTS va_scores' src/database.py
```

該当 DDL のテーブル名を変更:

**Before**:
```sql
CREATE TABLE IF NOT EXISTS va_scores (
    person_id TEXT PRIMARY KEY,
    ...
);
```

**After**:
```sql
CREATE TABLE IF NOT EXISTS voice_actor_scores (
    person_id TEXT PRIMARY KEY,
    ...
);
```

関連する `idx_va_scores_*` インデックスも同様に `idx_voice_actor_scores_*` に改名。

### Step 3: SQL 文字列の置換 (src/)

```bash
# dry-run
rg -n '\bFROM va_scores\b|\bINTO va_scores\b' src/

# 置換
sed -i 's/\bFROM va_scores\b/FROM voice_actor_scores/g' $(rg -l '\bFROM va_scores\b' src/)
sed -i 's/\bINTO va_scores\b/INTO voice_actor_scores/g' $(rg -l '\bINTO va_scores\b' src/)

# 確認
rg -n '\bFROM va_scores\b|\bINTO va_scores\b' src/
# 期待: 0 件 (非 migration コード)
```

**注意**: migration 関数 (`_migrate_v*`、v54 以前) の `va_scores` 参照は**残す**。grep で前後数行を見て判別。

### Step 4: tests/ の SQL 文字列置換

```bash
rg -n '\bFROM va_scores\b|\bINTO va_scores\b|\bTABLE va_scores\b' tests/

sed -i 's/\bFROM va_scores\b/FROM voice_actor_scores/g' $(rg -l '\bFROM va_scores\b' tests/)
sed -i 's/\bINTO va_scores\b/INTO voice_actor_scores/g' $(rg -l '\bINTO va_scores\b' tests/)
sed -i 's/\bTABLE va_scores\b/TABLE voice_actor_scores/g' $(rg -l '\bTABLE va_scores\b' tests/)

# 確認
rg -n '\bFROM va_scores\b|\bINTO va_scores\b|\bTABLE va_scores\b' tests/
# 期待: 0 件
```

### Step 5: インデックス名の更新

```bash
rg -n "idx_va_scores" src/database.py
```

`idx_va_scores_*` を `idx_voice_actor_scores_*` に置換:
- `init_db()` の DDL 内
- 必要なら v55 migration 内で `DROP INDEX IF EXISTS idx_va_scores_*` + `CREATE INDEX ... idx_voice_actor_scores_*` (既存 DB 対応)

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact`
- **T2 (失敗直後)**: `pixi run test-quick`
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "va or voice_actor or score"`
- **T4 (commit 直前 1 回)**: `pixi run test`

```bash
# 1. 構文
python -m py_compile src/database.py src/api.py src/cli.py 2>/dev/null

# 2. テーブル参照が全て置換された
rg -n '\bFROM va_scores\b|\bINTO va_scores\b|\bTABLE va_scores\b' src/ tests/
# 期待: _migrate_v* 関数内にのみ残存、非 migration コードでは 0 件

# 3. migration に RENAME が入った
rg -n 'ALTER TABLE va_scores RENAME TO voice_actor_scores' src/database.py
# 期待: 1 件 (_migrate_v54_to_v55 内)

# 4. DDL が新名
rg -n 'CREATE TABLE IF NOT EXISTS voice_actor_scores' src/database.py
# 期待: 1+ 件

rg -n 'CREATE TABLE IF NOT EXISTS va_scores' src/database.py
# 期待: 0 件 (init_db 側)。migration 関数内にはあっても OK

# 5. テスト
pixi run test-scoped tests/ -k "va or voice_actor or score"

# 6. Lint
pixi run lint

# 7. フレッシュ DB で物理テーブルが voice_actor_scores になっているか
pixi run python -c "
import tempfile, pathlib
from src.database import get_connection, init_db, run_migrations
p = pathlib.Path(tempfile.mktemp(suffix='.db'))
conn = get_connection(p)
init_db(conn)
run_migrations(conn)
row = conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('va_scores','voice_actor_scores')\").fetchall()
print('tables found:', [r[0] for r in row])
# 期待: ['voice_actor_scores'] のみ
"

# 8. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] Step 0 で予想外に大量の参照が見つかる → スコープ過大、要分割
- [ ] `pixi run test` で 1 件でも失敗
- [ ] Verification 7 で `va_scores` が残存 or `voice_actor_scores` が作られない
- [ ] sed が想定外のファイル (バイナリ、ドキュメント、migration 内) を変更

---

## Rollback

```bash
git checkout src/ tests/
pixi run test-scoped tests/ -k "va or voice_actor or score"
```

---

## Completion signal

- [ ] Verification 全項目 pass
- [ ] `git diff --stat`: 5-10 files、約 ±30 lines
- [ ] `git commit`:
  ```
  Physical rename va_scores → voice_actor_scores (v55 migration)

  Naming symmetry with person_scores and no more 'va' abbreviation.
  Updates DDL, migration, and SQL references in src/ + tests/.
  Legacy migrations (v* ≤ 54) still reference va_scores for
  existing-DB upgrade paths.

  VAScoreResult Pydantic class rename is deferred to a separate task.
  ```
