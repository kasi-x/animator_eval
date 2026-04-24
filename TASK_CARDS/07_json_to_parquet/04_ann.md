# Task: ANN checkpoint / cache → BRONZE Parquet

**ID**: `07_json_to_parquet/04_ann`
**Priority**: 🟡
**Estimated changes**: 約 +150 lines, 1 file (新規 script) / または **no-op**
**Requires senior judgment**: yes (データ実在次第)
**Blocks**: `06_e2e_verify`
**Blocked by**: `01_common_utils`

---

## Goal

ANN (AnimeNewsNetwork) の scrape 済データを parquet 化する。ただし **既存 JSON 実体が checkpoint (進捗 dict) のみで実データを持たない可能性が高い** → その場合は `no-op` として「ANN は再 scrape 必要」と結論。

---

## Pre-condition 調査 (最初に必ずやる)

```bash
ls -la data/ann/
pixi run python -c "
import json
d = json.load(open('data/ann/anime_checkpoint.json'))
print('keys:', list(d.keys()))
print('all_ids len:', len(d.get('all_ids', [])))
print('completed_ids len:', len(d.get('completed_ids', [])))
# データが checkpoint のみなら実データはない
"
ls data/scraper_cache/ 2>/dev/null
```

**判定フロー:**

| 状況 | 判定 | アクション |
|------|------|-----------|
| `anime_checkpoint.json` が `{all_ids, completed_ids}` のみ、他 JSON なし | **実データ不在** | Step NO-OP へ |
| `persons_checkpoint.json` にも同様のみ、詳細データなし | **実データ不在** | Step NO-OP へ |
| 個別 anime JSON が `data/ann/anime/*.json` 等に存在 | **実データあり** | Step IMPL へ |
| HTTP cache (`data/scraper_cache/ann/`) に応答本体が存在 | **部分的復元可** | Step CACHE-IMPL へ |

---

## Step NO-OP (実データ不在時)

```markdown
# scripts/migrate_ann_to_parquet.py は作成しない
# 代わりに TODO.md に以下を追記:
#
# - [ ] ANN scraper 再実行: checkpoint.json の all_ids を使い、新 bronze_writer 経路で parquet 出力
#       (既存 HTTP skip は effective、未完了 ID のみ fetch)
```

本カードの commit は TODO.md 追記のみ。

---

## Step IMPL (実データあり)

`scripts/migrate_ann_to_parquet.py` を `02_seesaawiki.md` と同パターンで作成。`AnnAnime` / `AnnCredit` / `AnnPerson` dataclass は `src/scrapers/ann_scraper.py` line 331, 337, 344 参照 (`anime_bw.append(anime_row)` の直前で何を作っているか確認)。

---

## Step CACHE-IMPL (HTTP cache 復元)

`data/scraper_cache/ann/` の hash JSON から:
1. 各 cache ファイルを read
2. `ann_scraper.py` 内の `parse_<endpoint>` 関数と同じパーサを適用
3. `AnnAnime` / `AnnCredit` 構造に変換
4. BronzeWriter で書き出し

ただし url/endpoint 情報が hash のみでは復元不能なケースあり → Step NO-OP 併用可。

---

## Verification

```bash
# no-op 選択時
grep "ANN scraper 再実行" TODO.md && echo "NO-OP OK"

# impl 選択時
test -n "$(find result/bronze/source=ann -name '*.parquet' 2>/dev/null)" && echo "PARQUET OK"
pixi run lint
```

---

## Stop-if conditions

- [ ] 判定フローで 2 択以上迷う → user に確認
- [ ] ANN の既存データ構造が scraper と乖離 → user に確認

---

## Rollback

```bash
# impl 選択時
rm -rf result/bronze/source=ann/
rm scripts/migrate_ann_to_parquet.py

# no-op 選択時
git checkout TODO.md
```

---

## Completion signal

- [ ] no-op: TODO.md に「ANN 再 scrape」エントリ追加済
- [ ] impl: parquet 生成 + lint pass
- [ ] 作業ログに `DONE: 07_json_to_parquet/04_ann` + 選択した Step (NO-OP/IMPL/CACHE-IMPL) 記録
