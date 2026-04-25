# Task: `ScrapeRunner` 抽象化 + scraper 移行 (allcinema/ann/keyframe)

**ID**: `13_scraper_runner_refactor/04_runner_abstraction`
**Priority**: 🔴
**Estimated changes**: 約 +250 / -400 lines, 5 files (新規 `runner.py` + scraper 3 改修 + テスト)
**Requires senior judgment**: yes (6 scraper 中 3 個に手を入れる、checkpoint / progress / batch 統合)
**Blocks**: なし
**Blocked by**: `01_http_client_dedupe`, `02_cache_store_expansion`, `03_bronze_sink`

---

## Goal

`src/scrapers/runner.py` に `ScrapeRunner` を新設し、`_run_scrape_*` ループ骨格を共通化する。allcinema / ann / keyframe の `_run_scrape_*` を Runner で書き直し、関数行数 80〜100 → 30〜50 行に圧縮する。

---

## Hard constraints

- **既存 CLI flag を破壊しない**: `--limit` / `--resume` / `--force` / `--quiet` / `--progress` / `--delay` / `--data-dir` 全て維持
- **本番 scrape 並列実行中**: scraper コマンドが本番で動いている可能性あり、checkpoint ファイル名 (`checkpoint_cinema.json` 等) を変えない
- **rate limit / retry を緩めない**: Fetcher 経由で `_gate()` が必ず呼ばれること
- **既存 BRONZE スキーマ不変**: `BronzeSink` の mapper で従来と同じ row dict を返す
- **batch 処理を壊さない**: ann の `XmlBatchFetcher` (50 IDs / batch) は throughput 維持に必須
- **`structlog` 必須**

---

## Pre-conditions

- [ ] `01_http_client_dedupe` 完了
- [ ] `02_cache_store_expansion` 完了 (`fetchers.py` 存在)
- [ ] `03_bronze_sink` 完了 (`sinks.py` 存在)
- [ ] `git status` clean
- [ ] `pixi run test-scoped tests/scrapers/` baseline pass (件数記録)
- [ ] 既存 `_run_scrape_*` の挙動を読んで理解した:
  - `src/scrapers/allcinema_scraper.py:448-608` (`_run_scrape_cinema`, `_run_scrape_persons`)
  - `src/scrapers/ann_scraper.py:353-565` (`_run_scrape_anime`, `_run_scrape_persons`)
  - `src/scrapers/keyframe_scraper.py` の anime / person phase

---

## Files to create / modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/runner.py` | **新規**: `ScrapeRunner` クラス + `Stats` dataclass |
| `tests/scrapers/test_runner.py` | **新規**: 単体テスト (mock fetcher / parser / sink) |
| `src/scrapers/allcinema_scraper.py` | `_run_scrape_cinema` / `_run_scrape_persons` を Runner で書換 |
| `src/scrapers/ann_scraper.py` | `_run_scrape_anime` (batch) / `_run_scrape_persons` を Runner で書換 |
| `src/scrapers/keyframe_scraper.py` | anime phase / person phase を Runner で書換 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/parsers/*.py` | 純関数 parser、保持 |
| `src/scrapers/mal_scraper.py` | endpoint 多すぎ、Runner 適合困難。次回判断 |
| `src/scrapers/anilist_scraper.py` | GraphQL pagination 特殊、適合困難 |
| `src/scrapers/seesaawiki_scraper.py` | LLM fallback あり、特殊処理多い |
| `src/scrapers/bangumi_main.py` | 直前コミット (`4bdf4cf`) でリファクタ済 |

---

## Steps

### Step 1: `src/scrapers/runner.py` 新規作成

```python
"""ScrapeRunner: common loop for ID enumeration → fetch → parse → sink → checkpoint.

Replaces the duplicated _run_scrape_* skeleton across allcinema/ann/keyframe.

Pattern:
    runner = ScrapeRunner(
        fetcher=html_fetcher,
        parser=parse_cinema_html,   # (raw, id) → Record | None
        sink=bronze_sink,
        checkpoint=cp,
        label="allcinema cinema",
        flush=group.flush_all,
        flush_every=100,
    )
    stats = await runner.run(pending_ids, progress_override=None)
    log.info("done", **dataclasses.asdict(stats))
"""
from __future__ import annotations

import dataclasses
from typing import Any, Awaitable, Callable, Generic, Hashable, TypeVar

import structlog

from src.scrapers.checkpoint import Checkpoint
from src.scrapers.progress import scrape_progress

log = structlog.get_logger()

ID = TypeVar("ID", bound=Hashable)
Raw = TypeVar("Raw")
Rec = TypeVar("Rec")


@dataclasses.dataclass
class Stats:
    processed: int = 0       # 試行数 (fetcher 呼び出し回数)
    written:   int = 0       # sink が返した row 合計
    skipped:   int = 0       # fetcher が None (404 等)
    errors:    int = 0       # raised exception 件数 (fail-soft 時のみ)


class ScrapeRunner(Generic[ID, Raw, Rec]):
    """Common loop for ID-based scrape phases.

    Fetcher: ID → Raw | None       (None で skip = 404 等)
    Parser:  (Raw, ID) → Rec | None  (None で skip = parse 失敗)
    Sink:    Rec → int              (書いた row 数)

    Checkpoint state: cp.completed_set / cp.pending() を使う前提。
    """

    def __init__(
        self,
        *,
        fetcher: Callable[[ID], Awaitable[Raw | None]],
        parser:  Callable[[Raw, ID], Rec | None],
        sink:    Callable[[Rec], int],
        checkpoint: Checkpoint,
        label:   str,
        flush:   Callable[[], None],
        flush_every: int = 100,
        fail_soft: bool = True,
    ) -> None:
        self._fetcher = fetcher
        self._parser  = parser
        self._sink    = sink
        self._cp      = checkpoint
        self._label   = label
        self._flush   = flush
        self._flush_every = max(1, flush_every)
        self._fail_soft = fail_soft

    async def run(
        self,
        ids: list[ID],
        *,
        progress_override: bool | None = None,
    ) -> Stats:
        """Iterate ids, fetch + parse + sink + checkpoint.

        Assumes ids are already filtered by checkpoint (caller does cp.pending()).
        """
        stats = Stats()
        completed: set[Hashable] = self._cp.completed_set
        total = len(ids)
        log.info(f"{self._label}_start", pending=total, completed=len(completed))

        with scrape_progress(
            total=total, description=self._label, enabled=progress_override
        ) as p:
            for i, id_ in enumerate(ids, start=1):
                try:
                    raw = await self._fetcher(id_)
                    if raw is None:
                        stats.skipped += 1
                    else:
                        rec = self._parser(raw, id_)
                        if rec is None:
                            stats.skipped += 1
                        else:
                            stats.written += self._sink(rec)
                    completed.add(id_)
                    stats.processed += 1
                except Exception as exc:
                    stats.errors += 1
                    if not self._fail_soft:
                        raise
                    log.warning(
                        f"{self._label}_error",
                        id=str(id_),
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                p.advance()

                if stats.processed % self._flush_every == 0:
                    self._flush()
                    self._cp.sync_completed(completed)
                    self._cp.save()
                    p.log(
                        f"{self._label}_progress",
                        done=stats.processed,
                        remaining=total - stats.processed,
                        written=stats.written,
                        skipped=stats.skipped,
                        errors=stats.errors,
                    )

        # 最終 flush
        self._flush()
        self._cp.sync_completed(completed)
        self._cp.save()
        log.info(f"{self._label}_done", **dataclasses.asdict(stats))
        return stats
```

### Step 2: `tests/scrapers/test_runner.py` 新規作成

mock fetcher / parser / sink で Runner の動作確認 (60-100 行):
- 全件処理でフラッシュが期待回数呼ばれる
- fetcher が None → skipped++
- parser が None → skipped++
- sink が int を返す → written 合計
- raise → errors++ (fail_soft=True), 中断 (False)
- checkpoint sync_completed が呼ばれる

### Step 3: allcinema 移行

#### Before (`_run_scrape_cinema` 抜粋)

```python
async def _run_scrape_cinema(...) -> None:
    cp = resolve_checkpoint(...)
    completed: set[int] = cp.completed_set
    cinema_ids: list[int] = cp.get("cinema_ids") or []
    client = AllcinemaClient(delay=delay)
    try:
        with BronzeWriterGroup("allcinema", tables=["anime", "credits"]) as g:
            anime_bw = g["anime"]
            credits_bw = g["credits"]
            if not cinema_ids:
                ...  # sitemap fetch
            pending = cp.pending(cinema_ids, limit=limit)
            with scrape_progress(...) as p:
                for cinema_id in pending:
                    rec = await scrape_cinema(client, cinema_id)
                    completed.add(cinema_id)
                    ...
                    if rec is not None:
                        n_credits = save_anime_record(anime_bw, credits_bw, rec)
                        ...
                    if done_this_run % batch_save == 0:
                        g.flush_all()
                        cp.sync_completed(completed)
                        cp.save()
                        p.log(...)
            cp.sync_completed(completed)
            cp.save()
    finally:
        await client.close()
```

#### After (Runner 経由、想定 30-40 行)

```python
async def _run_scrape_cinema(...) -> None:
    cp = resolve_checkpoint(...)
    cinema_ids: list[int] = cp.get("cinema_ids") or []
    client = AllcinemaClient(delay=delay)
    try:
        if not cinema_ids:
            cinema_ids = await fetch_sitemap_ids(client, SITEMAP_CINEMA_PATTERN, SITEMAP_CINEMA_COUNT)
            cp["cinema_ids"] = cinema_ids
            cp.save()
        with BronzeWriterGroup("allcinema", tables=["anime", "credits"]) as g:
            fetcher = HtmlFetcher(client, lambda i: f"{CINEMA_BASE}{i}", namespace="allcinema/cinema")
            sink = BronzeSink(g, mapper=_map_cinema_record, hash_table="anime")
            runner = ScrapeRunner(
                fetcher=fetcher,
                parser=_parse_cinema_html,
                sink=sink,
                checkpoint=cp,
                label="allcinema_cinema",
                flush=g.flush_all,
                flush_every=batch_save,
            )
            await runner.run(cp.pending(cinema_ids, limit=limit), progress_override=progress_override)
    finally:
        await client.close()


def _map_cinema_record(rec: AllcinemaAnimeRecord) -> dict[str, list[dict]]:
    """Map AllcinemaAnimeRecord → BRONZE row dicts."""
    anime_row = asdict_record(rec, drop=("staff", "cast"))
    credits = [
        {**dataclasses.asdict(c), "cinema_id": rec.cinema_id}
        for c in (rec.staff + rec.cast)
    ]
    return {"anime": [anime_row], "credits": credits}
```

`save_anime_record` / `scrape_cinema` 関数は削除 (Runner + Fetcher で代替)。`AllcinemaClient.get()` が Fetcher 経由で呼ばれるよう、CSRF 取得ロジックは残す。

### Step 4: ann 移行 (batch 処理 + person 処理)

ann の anime phase は **batch fetch (50 IDs/req)** が必須なので、Runner の使い方を工夫:

**選択肢 A** (推奨): Runner の `ID` を `tuple[int, ...]` (= 1 batch) として渡し、Fetcher は `XmlBatchFetcher`、parser は `parse_anime_xml` で複数 anime をまとめて返す。Runner は batch 単位で 1 進捗。

**選択肢 B**: Runner に `batch_size` パラメータを足してループ内で batch するモードを追加。

→ **A を採用** (Runner 単純維持)。Sink は 1 record の代わりに `list[Record]` を受ける `MultiBronzeSink` を sinks.py に追加するか、mapper で `{table: rows...}` を batch 全体に対して返す方式。

実装上の注意:
- progress 表示が batch 単位になる (50 IDs ごと前進) → 既存挙動と異なる
- 既存挙動を維持したいなら `p.advance(len(batch))` 相当を Runner 内で扱う
- → **Runner に `weight: Callable[[ID], int] = lambda _: 1` を追加** して、batch ID は `weight = len(batch)` を返す方式。テスト時 weight=1 デフォルト。

ann person phase は HTML per-ID なので allcinema と同型。

### Step 5: keyframe 移行

keyframe anime phase: HtmlFetcher + `keyframe_html_parser.parse_anime_page` + BronzeSink (anime + studios + credits + ... 多 table)。
keyframe person phase: JsonFetcher + `keyframe_api_parser.parse_person` + BronzeSink (person + jobs + studios + credits)。

### Step 6: 既存ヘルパ関数の削除

- `allcinema_scraper.py`: `save_anime_record`, `save_person_record`, `scrape_cinema`, `scrape_person` を削除 (Runner 移行で不要)
- `ann_scraper.py`: `save_anime_parse_result`, `save_person_detail` を削除
- `keyframe_scraper.py`: 同等のヘルパ削除

dead code 即削除 (memory feedback)。

### Step 7: 各 scraper のテスト確認

```bash
pixi run test-scoped tests/scrapers/test_allcinema*.py
pixi run test-scoped tests/scrapers/test_ann*.py
pixi run test-scoped tests/scrapers/test_keyframe*.py
```

mock-based test が多い場合、mock の interface が変わる可能性あり。test を **必要最小限の修正** で pass させる。

---

## Verification

```bash
# 1. import OK
pixi run python -c "
from src.scrapers.runner import ScrapeRunner, Stats
from src.scrapers.allcinema_scraper import app as alc_app
from src.scrapers.ann_scraper import app as ann_app
from src.scrapers.keyframe_scraper import app as kf_app
print('OK')
"

# 2. Runner 単体テスト
pixi run test-scoped tests/scrapers/test_runner.py

# 3. 移行 scraper テスト
pixi run test-scoped tests/scrapers/test_allcinema*.py tests/scrapers/test_ann*.py tests/scrapers/test_keyframe*.py

# 4. 行数確認 (期待: 各 _run_scrape_* が 30-50 行に)
grep -A 50 "async def _run_scrape" src/scrapers/allcinema_scraper.py | head -100
wc -l src/scrapers/allcinema_scraper.py src/scrapers/ann_scraper.py src/scrapers/keyframe_scraper.py

# 5. Smoke test (allcinema 1 件のみ)
pixi run python -m src.scrapers.allcinema_scraper cinema --limit 1 --dry-run

# 6. Lint
pixi run lint

# 7. 全 scraper テスト
pixi run test-scoped tests/scrapers/

# 8. PR 直前のみ: フルテスト
# pixi run test
```

---

## Stop-if conditions

- 移行 scraper のテスト 1 件でも fail
- Runner 単体テスト fail
- `git diff --stat` が +500/-700 を超える (想定の 2 倍)
- batch 処理 (ann) で throughput が大幅低下 (50 IDs/req が 1 ID/req になっていないか確認)
- CLI flag 名が変わった

---

## Rollback

```bash
git checkout src/scrapers/allcinema_scraper.py \
             src/scrapers/ann_scraper.py \
             src/scrapers/keyframe_scraper.py
rm -f src/scrapers/runner.py tests/scrapers/test_runner.py
pixi run test-scoped tests/scrapers/
```

---

## Completion signal

- [ ] `src/scrapers/runner.py` 存在、`ScrapeRunner` + `Stats` 定義
- [ ] `tests/scrapers/test_runner.py` pass
- [ ] allcinema / ann / keyframe の各 `_run_scrape_*` が 30-50 行
- [ ] `_run_scrape_*` の旧ヘルパ (`save_anime_record` 等) 削除済
- [ ] 全 scraper テスト pass
- [ ] git log message: `refactor(scraper): extract ScrapeRunner + migrate allcinema/ann/keyframe (13_scraper_runner_refactor/04)`
