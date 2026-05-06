# Task: Resolved 層 cluster over-merge 修正

**ID**: `19_resolved_cluster_fix`
**Priority**: 🟠 High
**Created**: 2026-05-05
**Blocks**: AKM / scoring 全般 (cluster の精度が個人スコア集計直結)

---

## 背景 (発見した over-merge)

`result/resolved.duckdb` 目視 audit で 2 系統の重大 over-merge 検出:

### A. anime — madb 話数行が title+year で過剰集約
| cluster size | title | year | 内容 |
|---|---|---|---|
| **1089** | サザエさん | NULL | madb M-prefix 話数 row |
| 323 / 307 | あおきいろ | 2024 / 2025 | 同上 |
| 255 / 252 | シナぷしゅ | 2022 / 2025 | 同上 |

**root cause**: madb JSON-LD `@graph` 内 C-series item (シリーズ概念) と M-manifestation item (個別放送回) が **同 anime parquet row として混在流入**。`schema:isPartOf` (parent C↔M link) は raw に存在するが `parse_jsonld_dump` (`src/scrapers/parsers/mediaarts.py:611-679`) が anime row 自体には保存していない。conformed/resolved に届く時点で link 喪失 → title+year fallback が全部潰す。

### B. persons — TMDb 同名 first-name で過剰集約
| cluster size | name_en | 内容 |
|---|---|---|
| **47** | Jonas | 全 src=tmdb、id 連番 (p5567xxx) |
| 28 | Ryan Cooper | 同上 |
| 25 | David / Elmer | 同上 |

**root cause**: `entity_resolution.exact_match_cluster` (`src/analysis/entity/entity_resolution.py:151`) の en_name_groups merge 経路。`name_ja/ko/zh` 空 + `name_en` あり → en で merge。homonym guard `_definitely_different` の numeric ID キーに **`tmdb_id` が含まれていない** (`_numeric_id_key`, line 86-92) → tmdb 同名異人を分離できず全集約。

---

## Sub-cards

| ID | Title | Priority | Blocks |
|---|---|---|---|
| [01_madb_parent_link.md](01_madb_parent_link.md) | madb parser に `parent_madb_id` + `record_type` 追加 → cluster は C 起点 + M 別保持 | 🟠 | A 系 cluster 全件 |
| [02_persons_tmdb_homonym.md](02_persons_tmdb_homonym.md) | `_numeric_id_key` に tmdb_id 追加 + en_name_groups guard 強化 | 🟠 | B 系 cluster |
| [03_audit_post_fix.md](03_audit_post_fix.md) | 修正後 cluster size 分布再計測 + 戦略 LLM 再 review | 🟡 | 01/02 後 |
| [04_canonical_id_collision.md](04_canonical_id_collision.md) | canonical_id をメンバ ID ハッシュに置換 → year=None 同タイトル衝突 (20,511 row silent drop) 解消 | 🔴 | resolved 全数保証 |

---

## Success criteria (全 sub-card 完了時点)

- anime cluster: source_count > 100 の cluster ゼロ
- persons cluster: 全 src 同一 + 同 first-name only の cluster ≤ 5 (LLM 検証)
- meta_resolution_audit reason 分布: tie_break 比率 < 5% (現 6.5% / anime)
- credits 行数: 減らない (情報量保持の確証)
