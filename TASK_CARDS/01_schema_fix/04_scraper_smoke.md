# Task: Scraper → 新 schema 書き込みの smoke 確認

**ID**: `01_schema_fix/04_scraper_smoke`
**Priority**: 🔴 Critical (成功判定の最終 gate 2/2)
**Estimated changes**: 約 +80 lines (新テスト or 実データでの動作確認)
**Blocks**: なし
**Blocked by**: `03_fresh_init_smoke` (fresh init がまず成立すること)

---

## Goal

「**新 scraping データが新 schema に正しく入る**」を確認。本セクションの成功判定 2 つのうち 2 つ目 = 本セクション完了判定。

---

## 方針

- 実際の scraper を 1 本動かしてみるのが最良 (AniList が安定、モックなし)
- テストでやるなら、scraper の write path (`upsert_canonical_anime` 経由) を synthetic data で呼ぶ
- **テストが煩雑になるなら実走確認で OK**。ログに「N anime, M credits 書けた」が出れば合格

---

## Option A: 実 scraper で smoke (推奨)

最小単位の scrape:

```bash
# Fresh DB (03 で作ったもの or 新規)
# AniList から 1 本だけ取って書き込み成功するか

pixi run python -c "
import asyncio, sqlite3, tempfile, pathlib
from src.database import init_db
from src.scrapers.anilist_scraper import scrape_single  # or equivalent

async def main():
    p = pathlib.Path(tempfile.mktemp(suffix='.db'))
    conn = sqlite3.connect(p)
    init_db(conn)

    # 適当な anime 1 本 (anilist id)
    await scrape_single(conn, anilist_id=1)  # Cowboy Bebop

    anime_count = conn.execute('SELECT COUNT(*) FROM anime').fetchone()[0]
    credits_count = conn.execute('SELECT COUNT(*) FROM credits').fetchone()[0]
    persons_count = conn.execute('SELECT COUNT(*) FROM persons').fetchone()[0]
    print(f'anime={anime_count}, persons={persons_count}, credits={credits_count}')
    assert anime_count == 1, 'anime not written'
    assert credits_count > 0, 'no credits written'
    print('OK: scraper → new schema works')

asyncio.run(main())
"
```

**期待出力**:
```
anime=1, persons=20+, credits=30+
OK: scraper → new schema works
```

---

## Option B: テストで smoke (実 API 叩けない CI 環境なら)

```python
# tests/test_scraper_smoke.py
"""Scraper writes must succeed against the new schema (synthetic input)."""
from __future__ import annotations
import sqlite3
from pathlib import Path

import pytest

from src.database import init_db
from src.etl.integrate import upsert_canonical_anime
from src.models import Anime  # new unified model


def test_canonical_upsert_writes_all_layers(tmp_path: Path):
    db = tmp_path / "smoke.db"
    conn = sqlite3.connect(db)
    try:
        init_db(conn)
        anime = Anime(
            id="anilist:1",
            title_ja="テスト作品",
            title_en="Test Work",
            year=2025,
        )
        upsert_canonical_anime(conn, anime, evidence_source="anilist")
        conn.commit()

        # Silver
        row = conn.execute("SELECT id, title_ja FROM anime").fetchone()
        assert row == ("anilist:1", "テスト作品")

        # Bronze snapshot
        row = conn.execute("SELECT anilist_id FROM src_anilist_anime").fetchone()
        assert row is not None
    finally:
        conn.close()
```

---

## Verification

```bash
# Option A
pixi run python -c "..."  # 上記 Option A スクリプト
# 期待: "OK: scraper → new schema works"

# Option B
pixi run pytest tests/test_scraper_smoke.py -v
# 期待: 1 passed
```

---

## Completion signal (= 01_schema_fix 全体の完了)

- [ ] Option A または B どちらかが通る
- [ ] 新 schema にデータが入り、SELECT で引ける
- [ ] コミット: `Add scraper smoke test against new schema`

これで本セクションの目的「**一回きりのデータ移行と新規書き込みが成功**」が達成。

以後の `02_phase4`, `03_consistency`, `04_duckdb`, `05_hamilton` へ進める。
