# 01_schema_fix — 新 schema への一発移行

**優先度**: 🔴 Critical (`00` → `01` → `02` → `03` → `04` 順)
**方針**: 段階的 migration を書かず、新 schema を直接定義して旧 DB から一発コピー

---

## 背景

当初の計画では v54 → v55 の漸進 migration を careful に書く方針だったが、**「一回きりのデータ移行が成功すれば OK」**の運用前提に切り替えたため、構造を大幅に簡素化:

- ❌ migration 履歴を丁寧に保持する → もう不要
- ❌ parity test で init_db == migrated を証明する → 意味がない
- ❌ 細かい Stop-if / Rollback 手順 → やり直せば済む
- ❌ `database.py` の integrity を careful に修復 → 新コードで書き直す
- ✅ 新 schema を目標として直接定義する
- ✅ 旧 DB から新 DB に一発コピー
- ✅ smoke 2 本(fresh init + scraper 書き込み)が通れば完了

---

## カード構成

```
00_target_schema.md      新 schema を DDL として書き下ろす (init_db_v2)
01_one_shot_copy.md      旧 DB → 新 DB の一発コピースクリプト
02_legacy_cleanup.md     src/database.py の 54+ migration 関数を全削除
03_fresh_init_smoke.md   smoke 1/2: ゼロから init が動く
04_scraper_smoke.md      smoke 2/2: scraper が新 schema に書ける
```

**全カード順序厳守**。`00` なしで `01` は書けない。`01` 成功なしで `02` は危険。

---

## `_naming_decisions/` について

当初 `07-14` として書かれた 8 つの「命名整理タスク」は、**新 schema 設計の材料**として `_naming_decisions/` に退避。個別の migration カードとしては実行しない。新 schema (`00_target_schema`) に全決定を反映する。

参照:
- `_naming_decisions/README.md` — 全命名決定の索引
- 個別資料 (07-14): 各論点ごとの判断根拠

---

## 削減されたもの

以前あった以下のカード・セクションは**削除**:

- 旧 `01_sources_ddl_conflict.md` 〜 `06_v56_defer_comment.md` (6 カード)
  - 理由: v54 → v55 の漸進 migration は新方針では不要
- 旧 `07_schema_baseline/` セクション全体 (4 カード)
  - 理由: migration 削除と fresh init 検証は新 `02`/`03` に統合

`_naming_decisions/` 内の旧カード (07-14) は参照資料として保持。

---

## 成功判定

本セクション完了 = 以下 2 つが成立:

1. **Fresh init**: ゼロから DB を作ると新 schema になる (`03`)
2. **Scraper write**: 新規スクレイピングで新 schema にデータが入る (`04`)

この 2 つが通れば、以後の作業 (`02_phase4`, `03_consistency`, `04_duckdb`, `05_hamilton`) に進める。
