# 03_consistency — コード一貫性

**優先度**: 🟠 Major
**実行順序**: 5 枚のカードは互いにほぼ独立。推奨順: 04 → 01 → 02 → 03 → 05

---

## 背景

コードベースに散在する「動くが設計と乖離している」箇所を修復します:

1. 6 scraper が `upsert_anime()` を直接呼び、`integrate.py` の統一パスを bypass
2. `upsert_anime_display()` が呼ばれ続けており、bronze 参照の設計思想と矛盾
3. `meta_entity_resolution_audit` テーブルに書き込むコードが一切ない (defamation 防御が空)
4. `credits.episode` が sentinel `-1` を使っており、NULL 意味付けに未移行
5. `src/etl/__init__.py` が空で、公開 API が不明

---

## カード一覧

| ID | タイトル | 複雑度 |
|----|---------|---------|
| 01 | Scraper 統一 (6 ファイル) | 中 |
| 02 | `anime_display` 書き込み停止 | 小 |
| 03 | `meta_entity_resolution_audit` 書き込み追加 | 中 |
| 04 | `credits.episode` sentinel → NULL 移行 | 小 |
| 05 | `src/etl/__init__.py` の exports 整理 | 小 |

---

## 並行実行について

**03_consistency/** のカードは `01_schema_fix/` 完了を前提とし、**`02_phase4/` とは並行実行可能**。

各カードは独立したファイル群に触るため、同じブランチで順次 commit していけば問題ない。ただし 1 カードずつ verification を通してから次へ。
