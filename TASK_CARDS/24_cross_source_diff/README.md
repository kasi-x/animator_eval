# 24 Cross-source 値違い検出 + 表記揺れ / 誤入力分類

**Objective**: 各 source (anilist/mal/ann/bgm/madb/seesaawiki/keyframe) で同 entity (anime/person/studio) の値が異なるケースを集計、表記揺れ vs 誤入力を分類、marimo notebook で対話的確認。

## 動機

5 層 Resolved 層の代表値選抜の精度向上には、source 間差異の **構造的理解** が必要:
- 表記揺れ (新字体/旧字体、全角/半角、句読点) → 自動正規化で吸収可
- 誤入力 (typo、桁ずれ、年違い) → 個別訂正 + audit

## カード

- [01_diff_audit_marimo](01_diff_audit_marimo.md) — diff 集計 + 分類 + marimo notebook 一括実装
