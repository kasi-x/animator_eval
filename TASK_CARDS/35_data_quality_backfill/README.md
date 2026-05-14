# 35_data_quality_backfill

Resolved 層に値が**到達していない**列の流入路修復を集約。entity resolution (merge) 問題は `19_resolved_cluster_fix/` に、scraper 拡張は `12-16` 系に分離されているが、本カテゴリは **「conformed には信号があるのに resolved に届かない」または「scraper 側で取得済だが loader に書込パス無し」** のケースを扱う。

## カード

| ID | 内容 | Priority | Status |
|----|------|----------|--------|
| [01_nationality_backfill.md](01_nationality_backfill.md) | resolved.persons.nationality 流入路修復 (現状全件 `'[]'`、anilist hometown / tmdb hometown 経路復活) | 🟠 | 未着手 |

## 起票背景

`15_extension_reports/04_o4_foreign_talent` と `26_industry_structure/02_international_collab` の実装は完了 + テスト pass しているが、入力データ (nationality) が 0% 充足のため実運用で意味のあるレポートが出ない。根本原因は ETL データ流入路の欠陥。
