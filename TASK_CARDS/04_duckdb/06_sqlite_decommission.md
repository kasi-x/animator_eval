# Task: SQLite 完全撤去 (`src/database.py` の DuckDB 化 + 旧 ETL 削除)

**ID**: `04_duckdb/06_sqlite_decommission`
**Priority**: 🟠 Major
**Estimated changes**: 約 +200 / -2000 lines (純減)、5-10 files
**Requires senior judgment**: **yes** (DDL 移行、Atlas migration 再生成)
**Blocks**: なし (本セクション最終)
**Blocked by**: `04_duckdb/05_analysis_cutover` 完了

---

## Goal

SQLite 依存を完全に削除し、`src/database.py` の役割を「silver.duckdb の DDL 定義 + 初期化」のみに縮小する。旧 SQLite migration 関数群 (v1-v55) を削除し、init は新 schema を直接 CREATE する単一経路に統一。

旧 `src/etl/integrate.py` (SQLite ETL) は廃止 (`src/etl/integrate_duckdb.py` が代替)。

---

## Hard constraints

- H1 anime.score を SILVER 表に追加しない
- H4 `credits.evidence_source` カラムを保持
- H5 既存テスト green 維持
- H7 破壊的 git 操作禁止 (大量削除でも `git rm` で commit 単位を分ける)

**本タスク固有**:
- **データ損失防止**: 既存 SQLite DB ファイル (`result/animetor.db`) を **削除しない**。整合性確認後でも archive/ に移すだけ
- **migration 削除前に commit**: legacy migration 関数の削除は別 commit (rollback しやすく)
- **Atlas migration を再生成**: `atlas migrate diff` を DuckDB env で実行、過去 v1-v55 の履歴は `migrations/legacy_sqlite/` に退避

---

## Pre-conditions

- [ ] `04_duckdb/05_analysis_cutover` 完了
- [ ] `pixi run test` pass (silver/gold が DuckDB で動いている)
- [ ] `result/silver.duckdb` と `result/gold.duckdb` が pipeline で正しく生成される
- [ ] 旧 SQLite との parity 確認済み:
  ```bash
  # 旧 silver SQLite vs 新 silver DuckDB の主要 count が一致
  sqlite3 result/animetor.db "SELECT COUNT(*) FROM anime"
  duckdb result/silver.duckdb "SELECT COUNT(*) FROM anime"
  # ± 1% 以内なら OK (dedup ルール差で多少のずれは許容)
  ```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/database.py` | DuckDB 用に縮小 (~9000 行 → ~300 行) |
| `src/etl/integrate.py` | **削除** (SQLite ETL、`integrate_duckdb.py` が代替) |
| `src/etl/__init__.py` | export を `integrate_duckdb` に変更 |
| `migrations/` | DuckDB 版を新規生成、旧 v1-v55 を `legacy_sqlite/` に退避 |
| `atlas.hcl` | DuckDB env を default に |
| `pixi.toml` | sqlite3 依存削除 (Python 標準なので不要だが明示的に確認) |
| `CLAUDE.md` | "SQLite WAL mode" → "DuckDB" に更新、testing patterns 更新 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/utils/display_lookup.py` | report 用、bronze parquet 経由に切替 (別カードで) |
| `result/animetor.db` (旧 SQLite DB) | 削除せず archive/ に移動 |

---

## Steps

### Step 1: parity 確認

```bash
# 主要テーブルの行数を比較
for table in anime persons credits anime_studios anime_genres anime_tags; do
  sq=$(sqlite3 result/animetor.db "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "?")
  dk=$(duckdb result/silver.duckdb "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "?")
  echo "$table  sqlite=$sq  duckdb=$dk"
done
```

差が 1% を超えるテーブルがあれば、`04_duckdb/03_integrate_etl` の dedup ロジックを見直す (本カードを開始する前に)。

### Step 2: 旧 SQLite DB を archive

```bash
mkdir -p archive/
mv result/animetor.db archive/animetor_pre_duckdb_$(date +%Y%m%d).db
# 削除はしない。万一の比較用に保持
```

### Step 3: `src/database.py` 縮小

**残すもの**:
- `init_silver_db(path)` — silver.duckdb の DDL 定義 (anime, persons, credits, ... の `CREATE TABLE` のみ)
- 必要なら `init_gold_db(path)` (`gold_writer.py` から DDL を移管 or そのまま gold_writer.py に残す)

**削除するもの**:
- `migrations` dict と全 `_migrate_v*_to_v*()` 関数 (~8500 行)
- `get_connection()` (sqlite3 ベース)
- `DEFAULT_DB_PATH` (sqlite3 用)
- `upsert_anime`, `upsert_anime_display` 等 SQLite 専用関数
- `init_db()` (旧 SQLite init)

**ファイル目標**: 9000 行 → 300 行以下。

### Step 4: 旧 `integrate.py` 削除

```bash
git rm src/etl/integrate.py
```

`src/etl/__init__.py` を更新:

```python
from src.etl.integrate_duckdb import integrate

__all__ = ["integrate"]
```

### Step 5: Atlas migration 再生成

旧 migration を退避:

```bash
mkdir -p migrations/legacy_sqlite/
git mv migrations/v55_add_gold_layer.sql migrations/legacy_sqlite/
# 他 v1-v54 もあれば全部移す
```

DuckDB 用 migration を新規生成:

```bash
# atlas.hcl を duckdb env に切替後
pixi run atlas migrate diff initial_duckdb_schema --env duckdb
```

生成された SQL を `migrations/duckdb/v1_initial.sql` として commit。

### Step 6: テスト全面更新

CLAUDE.md "Critical Testing Patterns" を更新:

```markdown
- **Monkeypatch silver path**:
  ```python
  monkeypatch.setattr(
      "src.analysis.silver_reader.DEFAULT_SILVER_PATH",
      tmp_path / "silver.duckdb",
  )
  ```
- **Monkeypatch gold path**:
  ```python
  monkeypatch.setattr(
      "src.analysis.gold_writer.DEFAULT_GOLD_DB_PATH",
      tmp_path / "gold.duckdb",
  )
  ```
- 旧 `monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", ...)` は廃止
```

既存テストで `DEFAULT_DB_PATH` を patch しているものを全て新 monkeypatch ポイントに置換:

```bash
rg -l 'DEFAULT_DB_PATH' tests/
# 全ファイルで置換
```

### Step 7: 全テスト実行

```bash
pixi run test
# 期待: 2161+ tests pass (削除されたテストはなし、書き換えのみ)
```

### Step 8: CLAUDE.md 更新

- "Tech Stack" セクションの "SQLite WAL mode (storage)" → "DuckDB (storage)"
- "Three-Layer Database Model" セクションの実装詳細を DuckDB ベースに更新
- "Schema Management" セクションの Atlas/DuckDB 統合を反映

---

## Verification

```bash
# 1. SQLite 完全撤去確認
rg 'sqlite3' src/   # 期待: 0 件 (or 完全に testing utility のみ)
rg 'get_connection|DEFAULT_DB_PATH' src/   # 期待: 0 件
rg '\.execute.*FROM scores\b|FROM anime_display' src/ tests/   # 0 件

# 2. integrate.py が削除されている
ls src/etl/integrate.py 2>&1   # No such file

# 3. 全テスト
pixi run test
# 期待: 全 pass

# 4. lint
pixi run lint

# 5. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
rg 'display_lookup' src/analysis/ src/pipeline_phases/

# 6. 完全 cycle smoke
rm -rf result/silver.duckdb result/gold.duckdb
pixi run integrate    # parquet → silver
pixi run pipeline     # silver → gold
duckdb result/gold.duckdb "SELECT COUNT(*) FROM person_scores"
# 期待: 一定数 > 0

# 7. atomic swap 同時実行
# ターミナル A: pixi run serve
# ターミナル B: 連続 curl
# ターミナル C: pixi run pipeline (or pixi run integrate)
# A の応答が完全に維持されること
```

---

## Stop-if conditions

- [ ] parity 確認 (Step 1) で SQLite と DuckDB の主要 count が大幅にずれている → 本カードを開始しない、`03` の dedup を見直す
- [ ] テスト書き換え (Step 6) で大量 fail → 新 monkeypatch ポイントの設計が不十分。silver_reader / gold_writer の分離を見直す
- [ ] Atlas migration 再生成で DDL に意図しない差が出る → senior 確認

---

## Rollback

旧 SQLite DB が archive にあるので最終手段は復旧可能。コード側は:

```bash
git checkout src/database.py src/etl/ migrations/ atlas.hcl CLAUDE.md
git checkout tests/
mv archive/animetor_pre_duckdb_*.db result/animetor.db
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] `src/database.py` が 500 行以下
- [ ] `rg 'sqlite3' src/` が 0 件
- [ ] CLAUDE.md / Tech Stack / Schema Management が DuckDB ベースに更新
- [ ] commit messages を 5+ commit に分割:
  ```
  Archive legacy SQLite DB (no deletion)
  Shrink src/database.py to silver/gold DDL only
  Remove src/etl/integrate.py (replaced by integrate_duckdb)
  Regenerate Atlas migrations for DuckDB
  Update tests: monkeypatch silver_reader/gold_writer paths
  Update CLAUDE.md: SQLite → DuckDB
  ```
