# Task: MAL scraper 多 BronzeWriter 結合 + 3 Phase 構造

**ID**: `12_mal_scraper_jikan/03_scraper_phases`
**Priority**: 🟠
**Estimated changes**: 約 +600 / -150 lines, 1 file (`src/scrapers/mal_scraper.py`) + integration test
**Requires senior judgment**: yes (checkpoint 互換性 / 28 BronzeWriter 並行 RAM / Phase 間 ID 受け渡し)
**Blocks**: `12_mal_scraper_jikan/04_rate_limit_strict`
**Blocked by**: `12_mal_scraper_jikan/02_parser_extend`

---

## Goal

`mal_scraper.py` が 28 BronzeWriter を並行保持し、Card 02 の parser 群から戻る dataclass を適切に振り分け書き込みする。**3 Phase 独立 checkpoint 構造** で、Phase 単位で resume 可能。

---

## Hard constraints

- **checkpoint 拡張**: 既存 `{last_fetched_page, last_fetched_index, total_*}` を `{phase, completed_anime_ids, completed_person_ids, completed_character_ids, completed_producer_ids, completed_manga_ids, last_page}` に拡張。旧 checkpoint 検知時は警告出して再開拒否 (互換性は捨てる、データ未取得状態のため影響なし)。
- **partial failure safe**: 1 anime あたり 13 endpoint fetch のうち失敗あっても、成功した parquet は append 済 (BronzeWriter immutable)。失敗 endpoint は次回再試行可能。失敗カウントは log に記録。
- **BronzeWriter batching**: 28 writer 並行で RAM 増加。`checkpoint_interval=10` (default を 50→10 に下げる) でこまめに `group.flush_all()`。
- **Phase 独立性**: Phase A 中断時 = Phase A から resume、Phase B 中断時 = Phase A 完了状態を信用して B から resume。

---

## Pre-conditions

- [ ] Card 02 完了 (18 parser 関数 + 22 fixture)
- [ ] `pixi run test-scoped tests/unit/test_mal_scraper_parse.py` pass
- [ ] `data/mal/` 不在確認 (旧 checkpoint なし)

---

## Files to modify

| File | 変更 |
|------|------|
| `src/scrapers/mal_scraper.py` | 既存 `_fetch_and_save` 全置換、3 Phase 関数 + `BronzeWriterGroup(28 tables)` |
| `tests/scrapers/test_mal_scraper_integration.py` | 新規、mock JikanClient で 3 anime + 5 person + 1 producer Phase A→B→C 完走テスト |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/parsers/mal.py` | Card 02 で完成 |
| `src/scrapers/bronze_writer.py` | `mal` source 既存登録、tables 制約なし |
| `src/scrapers/http_base.py` | Card 04 で rate limit 強化 |

---

## Phase 構造

### Phase A: anime + 13 sub endpoint (~60h)

```
1. /anime?order_by=mal_id&sort=asc を pagination で全 ID 列挙
   → checkpoint["all_anime_ids"] に保存
2. 各 mal_id について順次 fetch (1 anime = 13 endpoint):
   - /anime/{id}/full        → mal_anime + genres + relations + themes + external + streaming + studios (parse_anime_full 1 回で 7 dataclass)
   - /anime/{id}/staff       → staff_credits
   - /anime/{id}/characters  → anime_characters + va_credits
   - /anime/{id}/episodes    → episodes (pagination, 通常 1-3 ページ)
   - /anime/{id}/external    → 補完 (full と diff あれば追加)
   - /anime/{id}/streaming   → 補完
   - /anime/{id}/videos      → videos_promo + videos_ep
   - /anime/{id}/pictures    → pictures (URL のみ)
   - /anime/{id}/statistics  → statistics (display_*)
   - /anime/{id}/moreinfo    → moreinfo
   - /anime/{id}/recommendations → recommendations
   - /anime/{id}/news        → news
3. 副作用: Phase B/C で必要な person_id / character_id / producer_id / manga_id (relations から) を set に蓄積
4. checkpoint flush every 10 anime
```

### Phase B: persons + characters (~33h)

```
1. Phase A 蓄積 person_id set + character_id set を loop
2. 各 person について 2 endpoint:
   - /people/{id}/full       → mal_persons
   - /people/{id}/pictures   → person_pictures
3. 各 character について 2 endpoint:
   - /characters/{id}/full   → mal_characters
   - /characters/{id}/pictures → character_pictures
4. checkpoint flush every 50 entity
```

### Phase C: producers + manga + masters (~90h、manga 主因)

```
1. /producers?order_by=mal_id 全 ID 列挙 + Phase A 蓄積 producer_id set 統合
2. 各 producer 2 endpoint:
   - /producers/{id}/full    → mal_producers + producer_external
   - /producers/{id}/external → 補完
3. Phase A 蓄積 manga_id (relations から) を loop:
   - /manga/{id}/full        → mal_manga + manga_authors + manga_serializations + (manga relations)
4. masters (one-shot):
   - /genres/anime?filter=genres / explicit_genres / themes / demographics → master_genres
   - /magazines              → master_magazines (pagination)
   - /schedules?filter={day}  × 7 day → anime_schedule
```

---

## Files (extended)

### `mal_scraper.py` 新構造 (skeleton)

```python
import asyncio
import json
from datetime import datetime, timezone, date
from pathlib import Path

import structlog
import typer

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.cli_common import (
    CheckpointIntervalOpt, ProgressOpt, QuietOpt, ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.checkpoint import atomic_write_json, load_json_or
from src.scrapers.http_base import RateLimitedHttpClient
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging
from src.scrapers.progress import scrape_progress
from src.scrapers.parsers.mal import (
    parse_anime_full, parse_anime_staff_full, parse_anime_characters_va,
    parse_anime_episodes, parse_anime_external, parse_anime_streaming,
    parse_anime_videos, parse_anime_pictures, parse_anime_statistics,
    parse_anime_moreinfo, parse_anime_recommendations, parse_anime_news,
    parse_person_full, parse_person_pictures,
    parse_character_full, parse_character_pictures,
    parse_producer_full, parse_producer_external,
    parse_manga_full, parse_schedules,
    parse_master_genres, parse_master_magazines,
)

log = structlog.get_logger()

BASE_URL = "https://api.jikan.moe/v4"
CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "mal" / "checkpoint.json"

ALL_TABLES = [
    "anime", "anime_genres", "anime_relations", "anime_themes",
    "anime_external", "anime_streaming", "anime_studios",
    "anime_videos_promo", "anime_videos_ep", "anime_episodes",
    "anime_pictures", "anime_statistics", "anime_moreinfo",
    "anime_recommendations", "anime_characters", "va_credits",
    "staff_credits", "anime_news", "anime_schedule",
    "persons", "person_pictures",
    "characters", "character_pictures",
    "producers", "producer_external",
    "manga", "manga_authors", "manga_serializations",
    "master_genres", "master_magazines",
]   # 30 (28 dataclass tables + master_genres + master_magazines のうち一部 dataclass 重複の整理は実装時)

app = typer.Typer()


class JikanClient(RateLimitedHttpClient):
    """Card 04 で rate limit 強化される。本カードでは既存実装維持。"""
    # ... (既存と同じ + 新 endpoint 用 get_* メソッド多数追加)


def _empty_checkpoint() -> dict:
    return {
        "phase": "A",
        "all_anime_ids": [],
        "completed_anime_ids": [],
        "discovered_person_ids": [],
        "discovered_character_ids": [],
        "discovered_producer_ids": [],
        "discovered_manga_ids": [],
        "completed_person_ids": [],
        "completed_character_ids": [],
        "completed_producer_ids": [],
        "completed_manga_ids": [],
        "completed_phase_c_masters": False,
        "last_page_anime_list": 0,
        "timestamp": None,
    }


async def _phase_a_anime(client, group, ckpt, p, checkpoint_interval) -> None:
    """Phase A: anime list 列挙 + 各 mal_id の 13 endpoint fetch。"""
    # ... 詳細実装


async def _phase_b_persons_characters(client, group, ckpt, p, checkpoint_interval) -> None: ...


async def _phase_c_producers_manga_masters(client, group, ckpt, p, checkpoint_interval) -> None: ...


@app.command()
def main(
    phase: str = typer.Option("all", "--phase", help="A/B/C/all"),
    resume: ResumeOpt = True,
    checkpoint_interval: CheckpointIntervalOpt = 10,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Jikan v4 全 endpoint scrape (3 Phase)。"""
    from src.infra.logging import setup_logging
    setup_logging()
    log_path = configure_file_logging("mal")
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    ckpt = load_json_or(CHECKPOINT_FILE, _empty_checkpoint()) if resume else _empty_checkpoint()

    async def _run() -> None:
        client = JikanClient()
        group = BronzeWriterGroup("mal", tables=ALL_TABLES)
        try:
            with scrape_progress(total=None, description=f"MAL phase={phase}",
                                 enabled=resolve_progress_enabled(quiet, progress)) as p:
                if phase in ("A", "all"):
                    await _phase_a_anime(client, group, ckpt, p, checkpoint_interval)
                    ckpt["phase"] = "B"
                    atomic_write_json(CHECKPOINT_FILE, ckpt, indent=2)

                if phase in ("B", "all"):
                    await _phase_b_persons_characters(client, group, ckpt, p, checkpoint_interval)
                    ckpt["phase"] = "C"
                    atomic_write_json(CHECKPOINT_FILE, ckpt, indent=2)

                if phase in ("C", "all"):
                    await _phase_c_producers_manga_masters(client, group, ckpt, p, checkpoint_interval)
                    ckpt["phase"] = "DONE"
                    atomic_write_json(CHECKPOINT_FILE, ckpt, indent=2)

        finally:
            await client.close()
            group.flush_all()
            group.compact_all()

        log.info("mal_scrape_complete",
                 anime=len(ckpt["completed_anime_ids"]),
                 persons=len(ckpt["completed_person_ids"]),
                 characters=len(ckpt["completed_character_ids"]),
                 producers=len(ckpt["completed_producer_ids"]),
                 manga=len(ckpt["completed_manga_ids"]))

    asyncio.run(_run())


if __name__ == "__main__":
    app()
```

### `JikanClient` 拡張メソッド

既存 `get_anime_staff` / `get_top_anime` / `get_all_anime` に加え:

```python
async def get_anime_full(self, mal_id: int) -> dict: ...
async def get_anime_characters(self, mal_id: int) -> list[dict]: ...
async def get_anime_episodes(self, mal_id: int, page: int = 1) -> dict: ...  # paginated
async def get_anime_external(self, mal_id: int) -> list[dict]: ...
async def get_anime_streaming(self, mal_id: int) -> list[dict]: ...
async def get_anime_videos(self, mal_id: int) -> dict: ...
async def get_anime_pictures(self, mal_id: int) -> list[dict]: ...
async def get_anime_statistics(self, mal_id: int) -> dict: ...
async def get_anime_moreinfo(self, mal_id: int) -> dict: ...
async def get_anime_recommendations(self, mal_id: int) -> list[dict]: ...
async def get_anime_news(self, mal_id: int, page: int = 1) -> dict: ...
async def get_person_full(self, mal_person_id: int) -> dict: ...
async def get_person_pictures(self, mal_person_id: int) -> list[dict]: ...
async def get_character_full(self, mal_character_id: int) -> dict: ...
async def get_character_pictures(self, mal_character_id: int) -> list[dict]: ...
async def get_producer_full(self, mal_producer_id: int) -> dict: ...
async def get_producer_external(self, mal_producer_id: int) -> list[dict]: ...
async def get_producers_list(self, page: int = 1) -> dict: ...
async def get_manga_full(self, mal_manga_id: int) -> dict: ...
async def get_genres_anime(self, filter_kind: str | None = None) -> dict: ...
async def get_magazines(self, page: int = 1) -> dict: ...
async def get_schedules(self, day: str) -> dict: ...
```

全て `cache_store` 経由 (既存 `get` メソッド経由で自動 cache)。

---

## Steps

### Step 1: ALL_TABLES 一覧確定

Card 01 dataclass 28 個から `BronzeWriterGroup(tables=...)` 渡す table 名 list を確定 (上記 skeleton 参照)。

### Step 2: `JikanClient` 22 メソッド追加

各 endpoint 用の薄い wrapper。既存 `self.get(endpoint, params)` 経由で自動 cache + retry。

### Step 3: `_phase_a_anime` 実装

- `/anime?order_by=mal_id&sort=asc` で all_anime_ids 列挙 (~26000)
- 中断 resume: `completed_anime_ids` から差分計算
- 1 anime につき 13 endpoint fetch を `asyncio.gather` で並行 (rate limit は Card 04 で吸収)
- parse → dataclass → `group["..."].append(asdict(record))`
- discovered_* set へ ID 蓄積 (relations の `target_mal_id` から manga_id も拾う)
- 10 anime ごとに `group.flush_all()` + checkpoint 保存

### Step 4: `_phase_b_persons_characters` 実装

- discovered_person_ids set を loop、completed_person_ids 差分のみ
- 同様に character

### Step 5: `_phase_c_producers_manga_masters` 実装

- producers list 列挙 + discovered_producer_ids 統合
- manga: discovered_manga_ids loop
- masters one-shot

### Step 6: integration test

`tests/scrapers/test_mal_scraper_integration.py`:

```python
async def test_3_phases_e2e(monkeypatch, tmp_path):
    """mock JikanClient で 3 anime + 5 person + 2 character + 1 producer + 1 manga
    完走、28 parquet partition 全部生成確認。"""
```

mock fixture は Card 02 の `tests/fixtures/scrapers/mal/*.json` 再利用。

### Step 7: dry-run

```bash
pixi run python -m src.scrapers.mal_scraper --phase A --resume false 2>&1 | head
# rate limit 抑制のため Ctrl-C で 5 anime 処理後中断、parquet 出力確認
find result/bronze/source=mal -name "*.parquet" | head -30
```

---

## Verification

```bash
# 1. integration test
pixi run test-scoped tests/scrapers/test_mal_scraper_integration.py -v

# 2. dry-run 1 anime
pixi run python -m src.scrapers.mal_scraper --phase A --resume false &
sleep 60 && kill %1
ls result/bronze/source=mal/table=*/  # 13+ table partition 確認

# 3. lint
pixi run lint
```

---

## Stop-if conditions

- [ ] BronzeWriterGroup が 28 table 並行保持で OOM → batch_size 縮小 / table 分離
- [ ] `/anime?order_by=mal_id` で全 26000 ID 列挙が完走しない (Jikan が ID range 制限) → ID 範囲分割 (1-10000 / 10001-20000 / ...) で再試行
- [ ] checkpoint json サイズが 100MB 超 (ID set 巨大化) → set を SQLite に逃がす

---

## Rollback

```bash
git checkout src/scrapers/mal_scraper.py
rm -f tests/scrapers/test_mal_scraper_integration.py
rm -rf data/mal result/bronze/source=mal
```

---

## Completion signal

- [ ] integration test pass (3 anime + 5 person + 2 character + 1 producer + 1 manga 完走)
- [ ] dry-run で 13+ parquet partition 生成
- [ ] checkpoint json round-trip (中断 → resume → 同一結果)
- [ ] `lint` pass
- [ ] `DONE: 12_mal_scraper_jikan/03_scraper_phases` 記録
