# Task: 6 Scraper を BronzeWriter 経由 (parquet 出力) に置換

**ID**: `04_duckdb/02_scraper_migration`
**Priority**: 🟠 Major
**Estimated changes**: 約 +60 / -90 lines, 7 files
**Requires senior judgment**: **yes** (各 scraper の write 経路特定)
**Blocks**: `04_duckdb/03_integrate_etl`
**Blocked by**: `04_duckdb/01_bronze_writer`

**吸収**: `03_consistency/01_scraper_unification.md` を本カードで吸収する (該当カードは `SUPERSEDED.md` にリネーム)

---

## Goal

6 scraper が `upsert_anime()` / `conn.execute("INSERT INTO anime ...")` 等で SQLite に直接書き込んでいる経路を全廃。代わりに `BronzeWriter.append(row)` で parquet ファイルに書き出すよう置換する。

scraper はもはや `sqlite3.Connection` を必要としない。

---

## Hard constraints

- H1 anime.score を scoring に使わない (BRONZE は score 保持して OK)
- H3 entity resolution ロジック不変 (本カードでは触らない)
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **scraper のフェッチ・パースロジックを変えない** (HTTP / GraphQL / HTML 解析は不変)
- **書き込み経路だけ差し替え**
- 各 scraper は **モデルを dict 化して `bw.append(dict)`** する。Pydantic model でも `.model_dump()` で OK
- `persons` / `credits` / `studios` の書き込みも parquet 化 (anime だけでなく全テーブル)
- 1 scraper run = 複数 BronzeWriter (`anime`, `persons`, `credits` ごとに 1 つ) を context manager で並行管理

---

## Pre-conditions

- [ ] `04_duckdb/01_bronze_writer` 完了 (`BronzeWriter` 動作確認済み)
- [ ] `pixi run test` pass
- [ ] `git status` clean
- [ ] 6 scraper の現状把握:
  ```bash
  for f in src/scrapers/seesaawiki_scraper.py src/scrapers/anilist_scraper.py \
           src/scrapers/keyframe_scraper.py src/scrapers/jvmg_fetcher.py \
           src/scrapers/mal_scraper.py src/scrapers/mediaarts_scraper.py \
           src/scrapers/allcinema_scraper.py src/scrapers/ann_scraper.py; do
    echo "=== $f ==="
    grep -nE 'upsert_anime|conn\.execute.*INSERT|conn\.execute.*UPDATE' "$f" | head -10
  done > /tmp/scraper_writes.txt
  cat /tmp/scraper_writes.txt
  ```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/anilist_scraper.py` | DB write を BronzeWriter に置換 |
| `src/scrapers/ann_scraper.py` | 同上 |
| `src/scrapers/allcinema_scraper.py` | 同上 |
| `src/scrapers/seesaawiki_scraper.py` | 同上 (DELETE 文も削除 — parquet 再生成で代替) |
| `src/scrapers/keyframe_scraper.py` | 同上 |
| `src/scrapers/jvmg_fetcher.py` | 同上 |
| `src/scrapers/mal_scraper.py` | 同上 |
| `src/scrapers/mediaarts_scraper.py` | 同上 |
| `tests/test_*scraper*.py` | parquet 出力前提に書き換え |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/bronze_writer.py` | `01` で確定 |
| `src/etl/integrate.py` | parquet 読み取り側は `03` で扱う |
| `src/database.py` | `06` で扱う |
| `src/analysis/`, `src/pipeline_phases/` | `05` で扱う |
| 各 scraper の HTTP / parse ロジック | 不変 (write 経路のみ置換) |

---

## Steps

### Step 0: scraper 1 本ずつやる

**重要**: 6 scraper を同時に変えると debug 不可能。**1 本完了 → test → commit → 次** のサイクルで進む。

推奨順 (簡単 → 複雑):

1. `mal_scraper.py` (1 箇所のみ)
2. `keyframe_scraper.py` (1 箇所)
3. `jvmg_fetcher.py` (1 箇所)
4. `mediaarts_scraper.py` (1 箇所)
5. `anilist_scraper.py` (1 箇所だが多くのデータ種類)
6. `ann_scraper.py`
7. `allcinema_scraper.py`
8. `seesaawiki_scraper.py` (最も複雑、DELETE 文あり)

### Step 1: 1 scraper の write 経路を置換 (テンプレ)

**Before** (各 scraper):

```python
from src.database import upsert_anime
from src.utils.config import get_connection

def scrape_one_anime(anime_id: int) -> None:
    raw = await fetch(anime_id)
    anime = parse(raw)
    with get_connection() as conn:
        upsert_anime(conn, anime)
        for credit in anime.credits:
            conn.execute("INSERT INTO credits ...", credit)
```

**After**:

```python
from src.scrapers.bronze_writer import BronzeWriter

async def scrape_batch(anime_ids: list[int]) -> None:
    with (
        BronzeWriter("anilist", table="anime") as anime_bw,
        BronzeWriter("anilist", table="credits") as credits_bw,
        BronzeWriter("anilist", table="persons") as persons_bw,
    ):
        for anime_id in anime_ids:
            raw = await fetch(anime_id)
            anime = parse(raw)
            anime_bw.append(anime.model_dump())
            for credit in anime.credits:
                credits_bw.append(credit.model_dump())
            for person in anime.persons:
                persons_bw.append(person.model_dump())
```

**ポイント**:
- `get_connection()` import を削除
- `upsert_anime` / `conn.execute(INSERT)` を `bw.append()` に
- バッチごとに 1 ファイル flush (context manager exit) → クラッシュ時はバッチ全体やり直し
- 大バッチなら途中で `bw.flush()` を明示呼び出し (1000 行ごと等) してチェックポイント刻む

### Step 2: DELETE 文の扱い (seesaawiki のみ)

`seesaawiki_scraper.py` の以下:

```python
conn.execute("DELETE FROM credits WHERE evidence_source='seesaawiki'")
conn.execute("DELETE FROM persons WHERE id LIKE 'seesaa:%'")
conn.execute("DELETE FROM anime WHERE id LIKE 'seesaa:%'")
```

これは「再スクレイプ前に旧データをクリア」する目的。parquet 化後は **不要** (parquet を新規ファイル名で書くだけで integrate 側が dedup)。**全削除**。

代わりに、再スクレイプ前に `bronze/source=seesaawiki/` の当日 partition を消すのは **scraper の責務外** (運用側で `rm -rf bronze/source=seesaawiki/date=2026-04-23/` する)。

### Step 3: テスト書き換え

各 scraper のテストは「scraper 実行後に DB に行が入っているか」を見ているはず。以下に書き換える:

```python
# Before
def test_anilist_scraper_inserts_anime(monkeypatch, tmp_path):
    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_path / "test.db")
    asyncio.run(scrape_one(1))
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM anime").fetchall()
    assert len(rows) == 1

# After
def test_anilist_scraper_writes_parquet(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path
    )
    asyncio.run(scrape_one(1))
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) >= 1
    table = pq.read_table(files[0])
    assert table.num_rows >= 1
```

### Step 4: 1 scraper 終わったらコミット

```bash
pixi run test-scoped tests/ -k "mal_scraper" -v
pixi run lint
git add -p src/scrapers/mal_scraper.py tests/test_mal_scraper.py
git commit -m "Migrate mal_scraper to BronzeWriter (parquet output)"
```

→ Step 1-4 を残り 7 scraper に繰り返す。

---

## Verification

各 scraper 完了時:

```bash
# 1. その scraper の SQLite 直叩きが消えた
grep -nE 'upsert_anime|conn\.execute.*INSERT' src/scrapers/{name}_scraper.py
# 期待: 0 件

# 2. BronzeWriter 経由で書いている
grep -n 'BronzeWriter' src/scrapers/{name}_scraper.py
# 期待: 1 件以上

# 3. テスト pass
pixi run test-scoped tests/ -k "{name}_scraper" -v
```

全 scraper 完了時:

```bash
# 4. 全 scraper で SQLite 直接書き込みが消えた
for f in src/scrapers/*_scraper.py src/scrapers/*_fetcher.py; do
  cnt=$(grep -cE 'upsert_anime|conn\.execute.*(INSERT|UPDATE|DELETE)' "$f")
  if [ "$cnt" -gt 0 ]; then
    echo "REMAINING in $f: $cnt"
  fi
done
# 期待: REMAINING が 1 行も出ない

# 5. 全テスト
pixi run test
# 期待: 2161+ tests pass

# 6. lint
pixi run lint

# 7. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
rg 'display_lookup' src/analysis/ src/pipeline_phases/
```

**T4** (commit 直前): `pixi run test` 1 回。

---

## Stop-if conditions

- [ ] 1 scraper migration 後にその scraper 系テストが fail
- [ ] BronzeWriter で `model_dump()` が想定外の dict 構造を返す (e.g., nested models が dict 内 dict)
- [ ] 既存テストが「DB に行が入った」を assert していて、parquet ベースに書き換えるとセマンティクス変化 → ユーザ確認
- [ ] 大バッチ (10000+ rows) を 1 file flush するとメモリ圧迫 → 1000 行ごとの明示 flush に切替

---

## Rollback

scraper 1 本単位でロールバック可能:

```bash
git checkout src/scrapers/{name}_scraper.py tests/test_{name}_scraper.py
pixi run test-scoped tests/ -k "{name}_scraper"
```

全部戻す場合:

```bash
git checkout src/scrapers/ tests/test_*scraper*.py
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] 8 scraper (mal/keyframe/jvmg/mediaarts/anilist/ann/allcinema/seesaawiki) 全てが BronzeWriter 経由
- [ ] `03_consistency/01_scraper_unification.md` を `SUPERSEDED.md` にリネーム済み:
  ```bash
  git mv TASK_CARDS/03_consistency/01_scraper_unification.md \
         TASK_CARDS/03_consistency/01_scraper_unification.SUPERSEDED.md
  ```
  ファイル先頭に追記:
  ```markdown
  # SUPERSEDED by 04_duckdb/02_scraper_migration

  本カードは scraper を SQLite ラッパー (`upsert_canonical_anime`) 経由に
  統一する案だったが、scraper を Parquet 出力に切替える方針 (04_duckdb/02)
  に統合された。改修を 1 度で済ませるため。
  ```
- [ ] commit messages: scraper ごとに 1 commit、最終 commit で `03_consistency/01` の SUPERSEDED 記載
