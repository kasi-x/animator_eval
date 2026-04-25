# Task: anilist_scraper を共通 CLI / progress に統合

**ID**: `11_scraper_unification/01_anilist_cli_unify`
**Priority**: 🟠
**Estimated changes**: 約 +40 / -30 lines, 1 file (`src/scrapers/anilist_scraper.py`)
**Requires senior judgment**: no (機械的置換)
**Blocks**: `11_scraper_unification/03_http_client_base`
**Blocked by**: (なし)

---

## Goal

anilist_scraper の CLI に `--limit` alias / `--quiet` / `--progress` を追加し、内部の Rich `Progress(...)` 直接利用を共通の `scrape_progress` に置換する。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- 既存の `--count` flag を削除しない (alias として `--limit` 追加のみ)
- 既存 Rich の visualization (rate-limit 表示、`--rate-limit-text` 等) があれば維持
- AniList API の rate-limit 挙動は触らない
- progress bar の出力先 (console) は変更可だが、structlog の log line は維持

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `pixi run python -c "from src.scrapers import anilist_scraper; print('OK')"` が通る
- [ ] `src/scrapers/cli_common.py` / `src/scrapers/progress.py` が存在することを確認

```bash
ls src/scrapers/cli_common.py src/scrapers/progress.py
```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/anilist_scraper.py` | `main()` の CLI option / `_fetch_staff_phase` / `_fetch_person_details_phase` / `fetch_persons()` |

## Files to NOT touch

| File | 理由 |
|------|------|
| その他 scraper | 別 card で対応済 |
| `src/scrapers/cli_common.py` | 既に必要な型 alias 揃っている |

---

## Steps

### Step 1: import 追加

`src/scrapers/anilist_scraper.py` 冒頭の他 import 群と並べて以下を追加:

```python
from src.scrapers.cli_common import (
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.progress import scrape_progress
```

### Step 2: `main()` CLI に共通 flag 追加

`@app.command()` の `def main(...)` を探す (`grep -n "^def main" src/scrapers/anilist_scraper.py`)。

引数末尾に追加:

```python
def main(
    count: int = typer.Option(50, "--count", "--limit", "-n", help="...; alias --limit"),
    # ... 既存引数 ...
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
```

注意:
- `count` の `typer.Option` 第一引数は default (50)、第 2-4 引数で flag 名を複数渡す
- `--limit` を alias 追加だけ — 既存の `--count` は残す

### Step 3: `_fetch_staff_phase` / `_fetch_person_details_phase` の signature に `progress_override` 追加

```bash
grep -n "async def _fetch_staff_phase\|async def _fetch_person_details_phase" src/scrapers/anilist_scraper.py
```

各関数の最後に keyword 引数 `progress_override: bool | None = None` を追加し、内部で Rich `Progress(...)` を `scrape_progress(...)` に置換する。

例 (`_fetch_staff_phase`):

```python
# Before:
with Progress(SpinnerColumn(...), BarColumn(...), ...) as bar:
    task = bar.add_task("...", total=len(anime_ids))
    for ...:
        ...
        bar.update(task, advance=1)

# After:
with scrape_progress(
    total=len(anime_ids),
    description="anilist staff phase",
    enabled=progress_override,
) as p:
    for ...:
        ...
        p.advance()
```

中間 log は `p.log("event", **fields)` で。

### Step 4: `fetch_persons()` 関数 (line ≈ 2024) を同様に置換

`grep -n "def fetch_persons" src/scrapers/anilist_scraper.py` で位置確認。`finally` 内で `persons_bw.flush()` + `persons_bw.compact()` は維持。

### Step 5: `main()` から `progress_override` を渡す

```python
asyncio.run(
    _fetch_staff_phase(
        ...,
        progress_override=resolve_progress_enabled(quiet, progress),
    )
)
```

各 phase 関数呼出に `progress_override=resolve_progress_enabled(quiet, progress)` を追加。

---

## Verification

```bash
# 1. import OK
pixi run python -c "from src.scrapers import anilist_scraper"

# 2. CLI help が新 option 表示
pixi run python -m src.scrapers.anilist_scraper --help | grep -E '\-\-limit|\-\-quiet|\-\-progress'
# 3 行出力されること

# 3. Lint
pixi run lint src/scrapers/anilist_scraper.py

# 4. Dry run (実 API は叩かない)
pixi run python -m src.scrapers.anilist_scraper --limit 1 --quiet --help
# Usage が表示され、引数受付が動くこと
```

---

## Stop-if conditions

- `pixi run python -c "from src.scrapers import anilist_scraper"` が ImportError
- 既存 `--count` が消える (CLI help から消えていないか確認)
- 既存 Rich Progress の rate-limit 表示が必要だった場合 (Tabular な情報表示があるなら scrape_progress でカバー困難 → 中断)
- `git diff --stat src/scrapers/anilist_scraper.py` が +100/-100 を超える (想定の 2 倍超 = 統合過剰)

---

## Rollback

```bash
git checkout src/scrapers/anilist_scraper.py
pixi run python -c "from src.scrapers import anilist_scraper"
```

---

## Completion signal

- [ ] CLI help に `--limit` / `--quiet` / `--progress` が表示される
- [ ] 既存 `--count` も維持されている
- [ ] import エラーなし
- [ ] Lint pass
- [ ] git log message: `scraper(anilist): unify CLI flags and progress reporting (11_scraper_unification/01)`
