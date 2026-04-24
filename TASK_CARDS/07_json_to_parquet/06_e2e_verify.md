# Task: E2E 検証 — Parquet → SILVER DuckDB 投入

**ID**: `07_json_to_parquet/06_e2e_verify`
**Priority**: 🟠
**Estimated changes**: 約 +40 lines, 1 file (verification script) + TODO.md 更新
**Requires senior judgment**: no
**Blocks**: なし (最終カード)
**Blocked by**: `02_seesaawiki`, `03_allcinema`, `04_ann`, `05_madb` (ANN は NO-OP 可)

---

## Goal

`result/bronze/source=*/table=*/date=*/*.parquet` が `src/etl/integrate_duckdb.py` の ETL を通って SILVER DuckDB (`result/silver.duckdb`) に正しく投入されることを確認。

---

## Hard constraints

- H1 anime.score: SILVER `anime` テーブルに score カラムが存在しないこと (既存 invariant)
- **破壊的操作**: 本カードは SILVER を atomic swap で再生成する (一時 DB → 本番パス置換)。既存 silver.duckdb があれば事前にバックアップ

---

## Pre-conditions

- [ ] `02_seesaawiki` 完了 (parquet 生成済)
- [ ] `03_allcinema` 完了
- [ ] `04_ann` 完了 or NO-OP
- [ ] `05_madb` 完了

---

## Steps

### Step 1: integrate_duckdb.py の現行挙動確認

```bash
grep -n "def main\|BRONZE_ROOT\|DEFAULT_SILVER" src/etl/integrate_duckdb.py | head -10
```

実行コマンドを確定:

```bash
pixi run python -m src.etl.integrate_duckdb --help
```

### Step 2: 既存 SILVER バックアップ (任意)

```bash
if [ -f result/silver.duckdb ]; then
  cp result/silver.duckdb result/silver.duckdb.pre_migration.bak
fi
```

### Step 3: ETL 実行

```bash
pixi run python -m src.etl.integrate_duckdb --refresh
```

期待ログ: `parquet_read`, `silver_atomic_swap`, 件数サマリ。

### Step 4: SILVER 行数確認

`scripts/verify_bronze_silver_migration.py` (新規, 小 script):

```python
"""SILVER DuckDB 行数確認 (migration 後の health check)."""
from pathlib import Path
import duckdb

DEFAULT_SILVER = Path("result/silver.duckdb")


def main() -> None:
    if not DEFAULT_SILVER.exists():
        print("SILVER not found:", DEFAULT_SILVER)
        raise SystemExit(1)
    conn = duckdb.connect(str(DEFAULT_SILVER), read_only=True)
    tables = ["anime", "credits", "persons", "studios", "anime_studios"]
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"{t}: {n:,}")
        except duckdb.CatalogException:
            print(f"{t}: (missing)")
    # source 分布
    try:
        src_counts = conn.execute(
            "SELECT source, COUNT(*) FROM anime GROUP BY source ORDER BY 2 DESC"
        ).fetchall()
        print("\nanime by source:")
        for s, c in src_counts:
            print(f"  {s}: {c:,}")
    except duckdb.CatalogException:
        pass


if __name__ == "__main__":
    main()
```

```bash
pixi run python scripts/verify_bronze_silver_migration.py
```

### Step 5: 既存テスト実行

```bash
pixi run test-scoped tests/ -k "integrate_duckdb or silver"
```

### Step 6: 期待値チェック

| SILVER table | 期待下限 | 備考 |
|--------------|---------|------|
| `anime` | 5000+ | seesaawiki 8712 + madb 多数 + allcinema |
| `credits` | 50000+ | 作品平均 10+ credits |
| `persons` | 10000+ | 重複除去後 |

下回る場合 → dedup ロジックを疑う (BRONZE は重複許容、SILVER で dedup)。

### Step 7: TODO.md / DONE.md 更新

```bash
# TODO.md から「JSON → Parquet 移行」エントリ削除
# DONE.md に追記:
# - 07_json_to_parquet: seesaawiki/allcinema/madb の既存 JSON を BRONZE parquet 化、ETL 経由で SILVER DuckDB に投入 (2026-04-XX)
```

---

## Verification

```bash
# 1. SILVER 生成確認
test -f result/silver.duckdb && echo "SILVER OK"

# 2. 行数が期待下限を超える
pixi run python scripts/verify_bronze_silver_migration.py

# 3. lint / test
pixi run lint
pixi run test-scoped tests/ -k "integrate_duckdb"
```

---

## Stop-if conditions

- [ ] ETL が parquet を読めずエラー (schema 不一致 → 各 migration script で吐く dict のキー名を確認)
- [ ] SILVER 行数が期待下限の 10% 未満
- [ ] 既存 test が regression

---

## Rollback

```bash
# SILVER 復元
if [ -f result/silver.duckdb.pre_migration.bak ]; then
  mv result/silver.duckdb.pre_migration.bak result/silver.duckdb
fi

# verification script 削除
rm scripts/verify_bronze_silver_migration.py

# parquet は残す (他カードの成果物、削除しない)
```

---

## Completion signal

- [ ] SILVER DuckDB 生成済、期待行数クリア
- [ ] `pixi run test-scoped tests/ -k "integrate_duckdb"` pass
- [ ] TODO.md / DONE.md 更新済
- [ ] 作業ログに `DONE: 07_json_to_parquet/06_e2e_verify` 記録
