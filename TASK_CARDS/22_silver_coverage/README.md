# 22 SILVER coverage 修正

**Objective**: silver_loaders の覆い率不足を source 別に診断 + 修正。21/01 で発覚した anime_studios 97.5% missing 等。

## 前提

- 21/01 AKM 修正で `silver_anime_loaded with_studios=14,069 / total=562,191 (2.5%)` 判明
- silver_completeness audit (18/02) は coverage 計算修正済 (19/02)
- silver_dedup audit (18/01) で重複検出済

## カード構成

| ID | 内容 | Priority |
|----|------|---------|
| [01_anime_studios_coverage](01_anime_studios_coverage.md) | 562K anime の 97.5% に anime_studios 紐付けがない問題の調査 + 修正 | 🟠 |
