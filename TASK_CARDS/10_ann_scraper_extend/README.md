# 10 ANN scraper / parser 拡張 + 再 scrape

**Objective**: ANN XML encyclopedia (`api.xml?anime=ID`) と persons HTML (`people.php?id=ID`) から現在捨てている raw データを漏れなく BRONZE parquet へ保存する。

**方針** (ユーザ確定 2026-04-24):

1. **Option B**: 今回は再 scrape。raw cache layer は別 section (§12 相当) で将来化。
2. **parquet は複雑でも raw 形式優先**: BRONZE 層は XML/HTML から抽出した生文字列をそのまま格納。SILVER 移行時の解釈設計は後続検討。
3. **新規 parquet テーブル 6 つ追加** (+ 既存 anime/credits/persons 拡張)。JSON 列で圧縮せず、関係は別テーブルで正規化。
4. **fetch 追加呼び出しなし**: 既存 XML / HTML 応答に全フィールド含まれる → parser 拡張のみで全フィールド回収可能。

## カード構成

| ID | 内容 | Priority |
|----|------|---------|
| [01_schema_design](01_schema_design.md) | BRONZE 8 テーブル schema 決定 (dataclass + 列一覧、まだコード書かない) | 🟠 |
| [02_parser_extend](02_parser_extend.md) | `src/scrapers/parsers/ann.py` 拡張: `parse_anime_xml`, `parse_person_html` + 新規 parser 関数 | 🟠 |
| [03_scraper_integration](03_scraper_integration.md) | `src/scrapers/ann_scraper.py` 多 BronzeWriter 対応 (8 テーブル書き分け) | 🟠 |
| [04_rescrape](04_rescrape.md) | checkpoint リセット → 全件再 scrape (anime 27000 + persons 全件) | 🟠 |

## 実施順

`01 → 02 → 03 → 04` 順守。schema 確定なしに parser 書かない、parser なしに scraper 結合しない、結合確認なしに再 scrape しない。

## 完了判定

- BRONZE `source=ann/table={anime,credits,persons,cast,company,episodes,releases,news,related}` に新 parquet 存在
- 旧来列がすべて含まれる (regression なし)
- Hard Rule 遵守 (ratings は display-only metadata として保存、scoring path 不参入)

## 関連

- 旧 `TODO.md §11` を本 TASK_CARDS/10 に全面移管。§11 は「本カード参照」だけに書き換える (Card 04 完了時)。
- 前身カード `07_json_to_parquet/04_ann.md` は NO-OP 判定済 (2026-04-24 commit 9d3578e 周辺)。
