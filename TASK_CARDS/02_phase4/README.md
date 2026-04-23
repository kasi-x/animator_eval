# 02_phase4 — Phase 4 残務

**優先度**: 🟠 Major
**実行順序**: 01-05 は **互いに独立** (並行可能)。ただし 02 は 01 完了後に実行推奨

---

## 背景

Phase 1-3 (データ層、gold 層、レポート再編) は完了済み。Phase 4 (v2 gate の自動化) は骨格実装が完了しているが、以下の残務がある:

- `meta_lineage` テーブルに 5 レポートの lineage 情報を投入する必要あり
- フレッシュ schema での pipeline smoke test が未整備
- Method Notes validation gate の CI 組込みが未完
- Full lineage check の完全実装
- Technical appendix の語彙棚卸し

---

## カード一覧

| ID | タイトル | 判断 judgement 必要? |
|----|---------|---------------|
| 01 | meta_lineage population (5 レポート) | yes (各レポートのロジックを理解する必要) |
| 02 | Pipeline smoke test (フレッシュ v55) | no (典型的なテスト追加) |
| 03 | Method Notes validation gate (CI) | no (CI ファイル追加) |
| 04 | Lineage check の完全実装 | yes (ロジック設計) |
| 05 | Technical appendix vocabulary audit | no (lint 実行 + exception 記録) |

---

## 依存関係

```
01_schema_fix/ 全完了  ←  必須前提 (schema v55 で走らないと失敗する)
      ↓
02_phase4/01 (meta_lineage)  ←→  03_consistency/ (並行可)
02_phase4/02 (smoke test)
02_phase4/03 (method notes validation)
02_phase4/04 (full lineage check) ← 01 完了後に推奨
02_phase4/05 (tech appendix vocab) ← 独立
```

**推奨順序**: 01 → 04 → 02 → 03 → 05 (意味論的な依存)。並行実行するなら 01 と 05 は衝突しにくい。
