# Task: ANN scraper 多 BronzeWriter 結合

**ID**: `10_ann_scraper_extend/03_scraper_integration`
**Priority**: 🟠
**Estimated changes**: 約 +180 / -40 lines, 1 file (`src/scrapers/ann_scraper.py`) + integration test
**Requires senior judgment**: yes (checkpoint 互換性、parquet 書き込み順序、flush タイミング)
**Blocks**: `10_ann_scraper_extend/04_rescrape`
**Blocked by**: `10_ann_scraper_extend/02_parser_extend`

---

## Goal

`ann_scraper.py` が 8 種 BronzeWriter (anime / credits / persons / cast / company / episodes / releases / news / related) を並行保持し、parser の新戻り値 (`AnimeXmlParseResult`) から適切に振り分け書き込みする。

---

## Hard constraints

- **checkpoint 互換**: `anime_checkpoint.json` の `{all_ids, completed_ids}` 構造は維持。新フィールド不追加 (resume 動作を壊さない)。
- **partial failure safe**: anime 1 件の parse が 6 テーブルに分散書き込みされる中で途中失敗しても parquet ファイルは後続処理で重複除去可能 (BronzeWriter は immutable append のみ)。
- **BronzeWriter batching**: 8 writer 並行で RAM 増加するが、既存 `checkpoint_interval` (default 3) でこまめに flush で抑える。

---

## Pre-conditions

- [ ] Card 02 完了 (parser 新 shape 返却)
- [ ] `pixi run test-scoped tests/scrapers/test_ann_parser.py` pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/ann_scraper.py` | `_run_scrape_anime` に 6 新 writer 追加、`save_ann_anime` 相当を多テーブル書き分けに展開。`_run_scrape_persons` に credits_json / image_url 等の新列対応 |
| `tests/scrapers/test_ann_scraper_integration.py` (拡張 or 新規) | mock HTTP で 1 anime → 8 parquet ファイル全出力確認 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/bronze_writer.py` | source/table partition 構造そのまま使える |
| `src/scrapers/parsers/ann.py` | 02 で確定 |

---

## Steps

### Step 1: writer 束ね dataclass 導入

```python
from dataclasses import dataclass

@dataclass
class _AnimeBronzeWriters:
    anime: BronzeWriter
    credits: BronzeWriter
    cast: BronzeWriter
    company: BronzeWriter
    episodes: BronzeWriter
    releases: BronzeWriter
    news: BronzeWriter
    related: BronzeWriter

    def flush_all(self) -> None:
        for bw in (self.anime, self.credits, self.cast, self.company,
                   self.episodes, self.releases, self.news, self.related):
            bw.flush()
```

### Step 2: `save_ann_anime` を `save_anime_parse_result` に置換

```python
def save_anime_parse_result(
    writers: _AnimeBronzeWriters,
    result: AnimeXmlParseResult,
) -> tuple[int, int]:
    """Returns (n_anime, n_credits_total) for logging."""
    n_anime = 0
    n_credits = 0
    for rec in result.anime:
        anime_row = dataclasses.asdict(rec)
        anime_row.pop("staff", None)
        anime_row["fetched_at"] = datetime.now(timezone.utc).isoformat()
        anime_row["content_hash"] = hash_anime_data(anime_row)
        writers.anime.append(anime_row)
        n_anime += 1
        for entry in rec.staff:
            credit_row = dataclasses.asdict(entry)
            credit_row["ann_anime_id"] = rec.ann_id
            credit_row["role"] = parse_role(entry.task)
            writers.credits.append(credit_row)
            n_credits += 1
    for cast in result.cast:
        writers.cast.append(dataclasses.asdict(cast))
    for comp in result.company:
        writers.company.append(dataclasses.asdict(comp))
    for ep in result.episodes:
        writers.episodes.append(dataclasses.asdict(ep))
    for rel in result.releases:
        writers.releases.append(dataclasses.asdict(rel))
    for news in result.news:
        writers.news.append(dataclasses.asdict(news))
    for r in result.related:
        writers.related.append(dataclasses.asdict(r))
    return n_anime, n_credits
```

### Step 3: `_run_scrape_anime` 書き換え

```python
# 既存の 2 writer 生成を 8 writer に拡張
writers = _AnimeBronzeWriters(
    anime=BronzeWriter("ann", table="anime"),
    credits=BronzeWriter("ann", table="credits"),
    cast=BronzeWriter("ann", table="cast"),
    company=BronzeWriter("ann", table="company"),
    episodes=BronzeWriter("ann", table="episodes"),
    releases=BronzeWriter("ann", table="releases"),
    news=BronzeWriter("ann", table="news"),
    related=BronzeWriter("ann", table="related"),
)
# loop 内
result = parse_anime_xml(root)  # ← fetch_anime_batch 内部を書き直し
n_anime, n_credits = save_anime_parse_result(writers, result)
# flush タイミング
if (batch_idx + 1) % flush_every == 0:
    writers.flush_all()
# 最後
writers.flush_all()
```

`fetch_anime_batch` 戻り値型を `AnimeXmlParseResult` に変更 (parser の戻り値をそのまま返す)。

### Step 4: `_run_scrape_persons` 更新

`save_person_detail` は dataclass 全列を dict 化するだけなので既存関数で OK (新 field は自動で parquet schema に反映)。`description` deprecation 注意:

```python
# 既存
persons_bw.append(dataclasses.asdict(detail))
# detail.description と detail.description_raw の両方保存される (互換)
```

### Step 5: エラーハンドリング

1 anime の sub-entity (cast など) 抽出失敗時、その entity のみ skip し anime 本体は保存:

parser 側で partial result 許容にするか、`save_anime_parse_result` で try/except をエントリ単位でかける。parser が `list[AnnCastEntry]` を空で返すデザインなら scraper 側ロジック不要。

### Step 6: テスト

```python
# tests/scrapers/test_ann_scraper_integration.py
def test_ann_scrape_produces_8_tables(tmp_path, monkeypatch, respx_mock):
    # mock ANN XML response (fixture)
    respx_mock.get(...).respond(text=Path("tests/fixtures/ann_anime_sample.xml").read_text())
    monkeypatch.setenv("ANIMETOR_BRONZE_ROOT", str(tmp_path))
    # run 1 batch
    asyncio.run(_run_scrape_anime(limit=1, batch_size=1, delay=0, ...))
    # 8 テーブル全部 partition 存在確認
    for tbl in ("anime", "credits", "cast", "company", "episodes", "releases", "news", "related"):
        files = list((tmp_path / "source=ann" / f"table={tbl}").rglob("*.parquet"))
        assert files, f"missing table={tbl}"
```

---

## Verification

```bash
# 1. 既存 test regression なし
pixi run test-scoped tests/scrapers/test_ann_parser.py tests/scrapers/test_ann_scraper_integration.py

# 2. lint
pixi run lint

# 3. dry-run (1 anime のみ)
pixi run python -m src.scrapers.ann_scraper scrape-anime --limit 1 --data-dir /tmp/ann_dryrun

# 4. parquet 全テーブル存在確認
ls result/bronze/source=ann/
# 期待: table=anime / table=credits / table=cast / table=company / table=episodes /
#       table=releases / table=news / table=related
```

---

## Stop-if conditions

- [ ] checkpoint schema 変更が必要と判明 → Card 04 と同時設計
- [ ] 1 anime の parse で 8 テーブル全て 0 件 → parser 側 bug、02 に差し戻し
- [ ] RAM 使用量が既存の 4× 超 → flush_every を 3 → 1 に減らす、または async writer 化検討

---

## Rollback

```bash
git checkout src/scrapers/ann_scraper.py
rm -f tests/scrapers/test_ann_scraper_integration.py
rm -rf /tmp/ann_dryrun
```

---

## Completion signal

- [ ] `--limit 1` dry-run で 8 parquet partition 生成
- [ ] integration test pass
- [ ] `DONE: 10_ann_scraper_extend/03_scraper_integration` 記録
