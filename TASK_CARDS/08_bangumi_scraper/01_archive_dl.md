# Task: bangumi Archive dump DL + 展開

**ID**: `08_bangumi_scraper/01_archive_dl`
**Priority**: 🔴
**Estimated changes**: 約 +180 lines, 2 files 新規
**Requires senior judgment**: no
**Blocks**: `02_subjects_parquet`, `03_subject_relations`, `04_person_detail`, `05_character_detail`
**Blocked by**: なし

---

## Goal

`bangumi/Archive` の最新 release zip を DL → `data/bangumi/dump/<release_tag>/` に展開 → manifest.json 書き出し。後続 card はこの manifest を読んで parquet 化する。

---

## Hard constraints

- H1 viewer metric (score/rank) は保存 OK だが scoring 流入禁止
- **破壊的操作禁止**: 既存 `data/bangumi/` 消さない、追加のみ
- **User-Agent**: bangumi 規約に従い `animetor_eval/<version> (https://github.com/kashi-x)` 形式を必ず付ける (GitHub release DL でも)
- zip 展開先は **release_tag でパーティション**。既存展開済み tag は skip (idempotent)

---

## Pre-conditions

- [x] `git status` clean
- [x] `data/bangumi/` 書込み可
- [x] `pixi run lint` baseline pass

---

## Step 0: Archive 仕様調査

```bash
# release 一覧
curl -sH 'User-Agent: animetor_eval/0.1 (https://github.com/kashi-x)' \
  https://api.github.com/repos/bangumi/Archive/releases/latest \
  | python -c "import json,sys; d=json.load(sys.stdin); print(d['tag_name']); [print(a['name'],a['size'],a['browser_download_url']) for a in d['assets']]"
```

期待: zip asset (おそらく `dump.zip` or `dump-YYYY-MM-DD.zip`) が 1 個以上。中身 jsonlines:

- `subject.jsonlines` (全 subject、type=2 anime 以外も含む)
- `subject-persons.jsonlines` (subject × person 関係 + role)
- `subject-characters.jsonlines` (subject × character 関係)
- `subject-relations.jsonlines` (subject 間の続編/関連)
- `person.jsonlines`
- `character.jsonlines`
- `person-characters.jsonlines` (声優 cast 関係)

**注意**: ファイル名は release ごとに変化しうる → manifest は glob して実在するものを記録する。

---

## Files to create

| File | 内容 |
|------|------|
| `src/scrapers/bangumi_dump.py` | `fetch_latest_release()`, `download_and_extract(tag, dest)`, `build_manifest(extract_dir)` |
| `scripts/fetch_bangumi_dump.py` | CLI entrypoint (typer + Rich progress) |

## Files to NOT touch

| File | 理由 |
|------|------|
| 既存 scraper (`anilist_scraper.py` 等) | 別ソース |
| `data/bangumi/` 以下の既存ファイル | 上書き禁止 |

---

## Steps

### Step 1: `src/scrapers/bangumi_dump.py`

責務:

- `fetch_latest_release_meta() -> dict` — GitHub API から release_tag + asset URL 群取得
- `download_zip(url, dest_zip, chunk=8192) -> Path` — httpx stream DL (progress hook 可)
- `extract_zip(zip_path, extract_dir) -> list[Path]` — zipfile で jsonlines 全展開
- `build_manifest(extract_dir, release_tag) -> dict` — `{release_tag, downloaded_at, files: [{name, size, sha256, line_count}]}`
- SHA256 は 512KB chunk で計算、line_count は `sum(1 for _ in open(...))`

### Step 2: `scripts/fetch_bangumi_dump.py`

```
pixi run python scripts/fetch_bangumi_dump.py [--tag TAG] [--force]
```

- `--tag` 省略時は latest
- `--force` 無しで tag 既存なら skip
- 出力: `data/bangumi/dump/<tag>/*.jsonlines` + `data/bangumi/dump/<tag>/manifest.json`
- 最後に `data/bangumi/dump/latest -> <tag>` symlink 更新 (atomic: tmp → rename)

---

## Verification

```bash
pixi run python scripts/fetch_bangumi_dump.py --tag <適当な past tag>
ls data/bangumi/dump/<tag>/
cat data/bangumi/dump/<tag>/manifest.json | python -m json.tool | head -30

# idempotent 確認 (2回目 skip)
pixi run python scripts/fetch_bangumi_dump.py --tag <同じ tag>
# → "already extracted, skip" ログ

pixi run lint
```

---

## Stop-if conditions

- [ ] zip 展開後ファイル数が manifest と不一致
- [ ] SHA256 計算が止まる (メモリ破裂)
- [ ] GitHub API が 404/403 (rate limit) → PAT 不要な public release なので通常問題なし、403 なら stop
- [ ] `git diff --stat` が 400 lines 超

---

## Rollback

```bash
git checkout src/scrapers/bangumi_dump.py scripts/fetch_bangumi_dump.py
rm -rf data/bangumi/dump/<tag>/   # 部分展開なら手動削除
pixi run lint
```

---

## Completion signal

- [x] latest release 展開済 → manifest.json 正常生成
- [x] idempotent 動作確認
- [x] `data/bangumi/dump/latest` symlink 存在
- [x] DONE 記録

**DONE: 2026-04-25 — commit 9d3578e**
