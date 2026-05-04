# Task: format 8 カテゴリ正規化 (OVA / Special 問題)

**ID**: `24_cross_source_diff/05_format_taxonomy`
**Priority**: 🟠

## ゴール

3 種混在の format 列を 3 層で扱う:
1. `fine_format` (source 別ラベル維持)
2. `broad_format` (8 カテゴリ正規化)
3. LLM judgment (24/02 経由、不整合 case のみ)

## 8 カテゴリ宣言

| broad_format | 含む |
|----|----|
| `tv` | TV / TV_SHORT / TV special |
| `movie` | Movie / MOVIE |
| `ova_special` | OVA / OAV / Special / SPECIAL |
| `ona` | ONA |
| `short` | TV_SHORT (< 15 min)、SHORT |
| `music` | Music / MUSIC / PV / PV_CM |
| `cm` | CM |
| `other` | OTHER / GAME / unmapped |

注: `TV special` は `tv` (放送形式優先) or `ova_special` (single 性) 議論あり → **`tv` 採用** (放送経路優先)。

## 範囲

- 修正: `src/etl/normalize/column_rules.py` (`format_8_category` rule type 追加)
- 修正: `src/etl/audit/cross_source_consensus.py` (broad / fine 並走判定)
- 修正: `tests/test_etl/test_cross_source_consensus.py`
- 修正: `notebooks/cross_source_diff.py` (broad/fine 表示分離)

## 完了条件

- consensus 再生成、`broad_format` consensus が大幅 unanimous 化
- `fine_format` 不整合は `outlier_sources` で検出
- LLM 判定対象に `format_taxonomy_diff` flag
