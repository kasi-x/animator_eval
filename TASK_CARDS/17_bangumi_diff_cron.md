# Task: bangumi.tv 日次差分 API cron 化

**ID**: `17_bangumi_diff_cron`
**Priority**: 🟡 (待機系、dump 安定運用後)
**Estimated changes**: 約 +200 / -0 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: bangumi.tv archive dump 安定運用 (`TODO §13.1` ✅ 完了済) + 運用週次 review

---

## Goal

bangumi.tv の archive dump を週次 / 月次再 DL する代わりに、`/v0/{subjects,persons,characters}/{id}` API を日次 cron で叩いて差分のみ取得する経路を構築する。

---

## Hard constraints

- **H1**: `score` / `rank` は raw 保持、SILVER scoring 流入禁止 (既存方針)
- **H4**: `evidence_source = 'bangumi'`
- **H5**: 既存テスト破壊禁止
- **rate limit**: 1 req/s 厳守 (既存 `src/scrapers/queries/bangumi.py`)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] BRONZE `source=bangumi` 既存テーブル確認 (subjects / persons / characters / person_characters / subject_persons / subject_characters)
- [ ] 既存 dump scraper (`src/scrapers/bangumi_main.py` / `bangumi_dump.py`) 把握
- [ ] `pixi run test` baseline pass

---

## 設計

### 差分検出戦略

bangumi API は `last_modified` 持つ。日次 cron で:
1. 前回実行時刻 (checkpoint) を読込
2. 全 subject_id を iterate して `/v0/subjects/{id}` 叩く
3. レスポンスの `last_modified > prev_run_time` のみを BRONZE 書込
4. checkpoint を更新

ただし全 subject iterate は 1 req/s で 3,715 件 = 1 時間。`if-modified-since` header 利用で 304 早期 return 可能か要確認。

代替案: GraphQL `subjectsBetween` (時刻範囲フィルタ) があるか調査。

### cron 経路

実装案:
1. **systemd timer**: `~/.config/systemd/user/bangumi-diff.timer` 日次 02:00
2. **anacron**: `/etc/anacron/anime-update.cron`
3. **pixi 起動 wrapper**: `pixi run bangumi-diff` で起動可能、systemd は wrapper 呼ぶだけ

推奨: **systemd timer + pixi wrapper**

### checkpoint 管理

`result/bronze/source=bangumi/_checkpoint.json` に `{last_run: ISO8601, last_modified_max: ISO8601}` を保持。

---

## Files to create

| File | 内容 |
|------|------|
| `src/scrapers/bangumi_diff.py` | 日次差分 entry point |
| `~/.config/systemd/user/bangumi-diff.{service,timer}` | systemd unit (個人運用、リポジトリ外) |
| `tests/scrapers/test_bangumi_diff.py` | checkpoint + 差分検出 unit test |

## Files to modify

| File | 変更内容 |
|------|---------|
| `pixi.toml` | `[tasks] bangumi-diff = "python -m src.scrapers.bangumi_diff"` 追加 |
| `docs/scraper_ethics.md` | bangumi 差分運用記録 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/bangumi_main.py` (dump 系) | 並行運用、dump は月次バックアップとして残す |
| `src/scrapers/queries/bangumi.py` rate limit | 既存設定不変 |

---

## Steps

### Step 1: API 差分機能の確認

```bash
# if-modified-since header 効くか
curl -i -H "If-Modified-Since: Wed, 01 May 2026 00:00:00 GMT" \
  https://api.bgm.tv/v0/subjects/1
# 304 Not Modified で帰るか確認
```

GraphQL `subjectsBetween` があるかも確認。

### Step 2: bangumi_diff.py 実装

```python
def run_diff(checkpoint_path: Path, bronze_root: Path):
    prev = load_checkpoint(checkpoint_path)
    new_subjects, new_persons, new_chars = [], [], []
    for sid in iterate_subject_ids():
        data = fetch_subject_with_if_modified_since(sid, prev["last_run"])
        if data is None:  # 304
            continue
        new_subjects.append(data)
        ...
    write_bronze(bronze_root, today, new_subjects, ...)
    save_checkpoint(checkpoint_path, last_run=now())
```

### Step 3: pixi task + systemd unit

```toml
[tasks]
bangumi-diff = "python -m src.scrapers.bangumi_diff"
```

```ini
# bangumi-diff.service
[Service]
Type=oneshot
WorkingDirectory=%h/dev/animetor_eval
ExecStart=/usr/bin/env pixi run bangumi-diff

# bangumi-diff.timer
[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true
```

### Step 4: テスト

- 合成 fixture で checkpoint 読込 / 304 skip / 差分書込確認
- 既存 dump 経路のテスト不変

### Step 5: 監視

- 7 日連続成功確認後 cron 化
- 失敗時 pushbullet / email 通知 (既存 `src/infra/` の logging に組込)

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. テスト
pixi run test-scoped tests/scrapers/test_bangumi_diff.py

# 3. dry-run
pixi run bangumi-diff --dry-run --limit 50

# 4. checkpoint 動作確認
cat result/bronze/source=bangumi/_checkpoint.json
```

---

## Stop-if conditions

- [ ] `if-modified-since` が API でサポートされていない → 全件 iterate しか手段なし、cost-benefit 再評価
- [ ] rate limit (1 req/s) で日次完走不可 → 週次に変更
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm src/scrapers/bangumi_diff.py
rm tests/scrapers/test_bangumi_diff.py
rm -f ~/.config/systemd/user/bangumi-diff.{service,timer}
git checkout pixi.toml docs/scraper_ethics.md
systemctl --user disable bangumi-diff.timer
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] systemd timer 7 日稼働確認
- [ ] checkpoint 自動更新確認
- [ ] DONE: `17_bangumi_diff_cron`

---

## 関連

- `TODO.md §13.6`: 旧記述。本カード完了時に「→ TASK_CARDS/17」へ書き換え
- `src/scrapers/bangumi_main.py`: dump 経路 (月次バックアップとして残存)
- `src/scrapers/queries/bangumi.py`: 既存 GraphQL/REST query 定義
