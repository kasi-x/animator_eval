# Task: BRONZE parquet 書出しと schema 登録

**ID**: `09_sakuga_atwiki/04_bronze_export`
**Priority**: 🔴
**Estimated changes**: 約 +300 / -0 lines, 4 files
**Requires senior judgment**: no
**Blocks**: `09_sakuga_atwiki/05_incremental_update`
**Blocked by**: `09_sakuga_atwiki/03_person_parser`

---

## Goal

`ParsedSakugaPerson` 群を BRONZE parquet 3 テーブル (`src_sakuga_atwiki_pages`, `src_sakuga_atwiki_persons`, `src_sakuga_atwiki_credits`) に書き出し、`src/db/schema.py` に登録する。

---

## Hard constraints

- `_hard_constraints.md` 参照
- **H1**: parquet に保存するのは credit 事実のみ。主観評価列 (「作画評価」ランク等) を作らない
- **H3 entity resolution 不変**: 本 card は BRONZE 出力まで。SILVER 層 `persons` テーブルへの統合は後続タスクで別途起票
- **H4 evidence_source**: `src_sakuga_atwiki_credits.evidence_source = "sakuga_atwiki"` 必須
- BRONZE は immutable: 既存 parquet を上書きしない。`date=YYYYMMDD` パーティション分離

---

## Pre-conditions

- [ ] `09_sakuga_atwiki/03_person_parser` 完了
- [ ] 人物ページ parse 成功率 >= 70%
- [ ] `git status` が clean
- [ ] `pixi run test` が pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | 追加: 3 SQLModel クラス (`SrcSakugaAtwikiPage`, `SrcSakugaAtwikiPerson`, `SrcSakugaAtwikiCredit`) |
| `src/scrapers/sakuga_atwiki_scraper.py` | 拡張: `export-bronze` CLI subcommand |
| `src/scrapers/bronze_writer.py` | 拡張: sakuga_atwiki 用 writer 関数 (既存パターン踏襲) |
| `tests/db/test_sakuga_atwiki_schema.py` | **新規**: SQLModel バリデーションテスト |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| SILVER/GOLD 層 (`anime`, `persons`, `credits` 等) | BRONZE 統合は別タスク |
| 既存 BRONZE テーブル (`src_anilist_*`, `src_allcinema_*` 等) | 共存 |

---

## Steps

### Step 1: Schema 定義

`src/db/schema.py` に追加:

```python
class SrcSakugaAtwikiPage(SQLModel, table=True):
    __tablename__ = "src_sakuga_atwiki_pages"
    __table_args__ = (Index("ix_sakuga_pages_kind", "page_kind"),)
    page_id: int = Field(primary_key=True)
    url: str
    title: str
    page_kind: str                # person / work / index / meta / unknown
    last_fetched_at: datetime
    html_sha256: str
    date_partition: str           # YYYYMMDD (parquet パーティション列)

class SrcSakugaAtwikiPerson(SQLModel, table=True):
    __tablename__ = "src_sakuga_atwiki_persons"
    __table_args__ = (Index("ix_sakuga_persons_name", "name"),)
    page_id: int = Field(primary_key=True, foreign_key="src_sakuga_atwiki_pages.page_id")
    name: str
    aliases_json: str             # JSON list として保存
    active_since_year: int | None
    date_partition: str

class SrcSakugaAtwikiCredit(SQLModel, table=True):
    __tablename__ = "src_sakuga_atwiki_credits"
    __table_args__ = (
        Index("ix_sakuga_credits_person", "person_page_id"),
        Index("ix_sakuga_credits_work", "work_title"),
    )
    id: int | None = Field(default=None, primary_key=True)
    person_page_id: int = Field(foreign_key="src_sakuga_atwiki_persons.page_id")
    work_title: str
    work_year: int | None
    work_format: str | None
    role_raw: str
    episode_raw: str | None
    episode_num: int | None
    evidence_source: str = Field(default="sakuga_atwiki")
    date_partition: str
```

### Step 2: Atlas migration 生成

```bash
pixi run atlas migrate diff --env local sakuga_atwiki_bronze
```

生成された SQL を `migrations/` 下で目視確認し、**既存テーブル (anilist 等) に影響が無い** ことを verify。

### Step 3: Writer 関数

`src/scrapers/bronze_writer.py` に追加:

```python
def write_sakuga_atwiki_bronze(
    persons: list[ParsedSakugaPerson],
    pages_metadata: list[dict],
    output_dir: Path,
    date_partition: str,
) -> dict[str, Path]:
    """3 parquet を書き出し、パス dict を返す。"""
```

- 出力パス:
  - `{output_dir}/source=sakuga_atwiki/table=pages/date={date_partition}/part-0.parquet`
  - `{output_dir}/source=sakuga_atwiki/table=persons/date={date_partition}/part-0.parquet`
  - `{output_dir}/source=sakuga_atwiki/table=credits/date={date_partition}/part-0.parquet`
- 既存 `bronze_writer.py` の allcinema / ann 用 writer を参考に pattern 踏襲

### Step 4: CLI

`src.scrapers.sakuga_atwiki_scraper export-bronze`:

- `--input data/sakuga/` (cache + discovered_pages.json)
- `--output result/bronze/`
- `--date YYYYMMDD` (指定無しは当日)
- parse → writer → 書出完了ログ

### Step 5: テスト

`tests/db/test_sakuga_atwiki_schema.py`:

- 3 SQLModel クラスの column 列挙、FK 関係、`evidence_source` default 値
- `write_sakuga_atwiki_bronze` を tmp_path で実行、parquet 3 つが期待スキーマで書き出されること

---

## Verification

```bash
# 1. Test
pixi run test-scoped tests/db/test_sakuga_atwiki_schema.py
pixi run test

# 2. Lint
pixi run lint

# 3. Schema 整合
pixi run atlas schema validate --env local

# 4. E2E (既存 cache 使用、HTTP 無し)
pixi run python -m src.scrapers.sakuga_atwiki_scraper export-bronze \
    --input data/sakuga/ --output /tmp/bronze_test --date 20260424

# 5. parquet 内容確認
pixi run python -c "
import duckdb
con = duckdb.connect()
print(con.execute(\"SELECT COUNT(*) FROM '/tmp/bronze_test/source=sakuga_atwiki/table=credits/date=20260424/*.parquet'\").fetchone())
print(con.execute(\"SELECT DISTINCT evidence_source FROM '/tmp/bronze_test/source=sakuga_atwiki/table=credits/date=20260424/*.parquet'\").fetchall())
"
# → credits count > 0, evidence_source = [('sakuga_atwiki',)]

# 6. 既存 BRONZE 非破壊確認
ls result/bronze/source=anilist/ result/bronze/source=allcinema/ 2>/dev/null
# → 既存 parquet がそのまま残存
```

---

## Stop-if conditions

- [ ] `pixi run atlas schema validate` が失敗
- [ ] 既存 BRONZE parquet が書き換わった痕跡 (ls -la タイムスタンプ差)
- [ ] `evidence_source` が `"sakuga_atwiki"` 以外で書き込まれている

---

## Rollback

```bash
git checkout src/db/schema.py src/scrapers/
rm -f tests/db/test_sakuga_atwiki_schema.py
rm -rf /tmp/bronze_test
# Atlas migration が applied 済みなら down migration 生成
pixi run atlas migrate down --env local
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] E2E で 3 parquet 生成、credits row 数 > 0
- [ ] 既存 BRONZE 非破壊
- [ ] `git diff --stat` が ±300 lines 以内
- [ ] 作業ログに `DONE: 09_sakuga_atwiki/04_bronze_export` と記録
