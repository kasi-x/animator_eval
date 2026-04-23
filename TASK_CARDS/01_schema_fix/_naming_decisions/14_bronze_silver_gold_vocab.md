# Task: BRONZE/SILVER/GOLD ラベルを機能的名称に統一

**ID**: `01_schema_fix/14_bronze_silver_gold_vocab`
**Priority**: 🟢 Low
**Estimated changes**: コード 0 行（ドキュメント・コメントのみ）
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし（いつでも可）

---

## Goal

`BRONZE/SILVER/GOLD` というメダリオン・アーキテクチャの語彙は概念的に正確だが、
実際のテーブルプレフィックスは異なる:

| 概念 (Medallion) | 実テーブルプレフィックス | 説明 |
|-----------------|------------------------|------|
| BRONZE | `src_*` | スクレイパー生データ |
| SILVER | (prefix なし) | canonical 正規化データ |
| GOLD | `feat_*`, `agg_*`, `meta_*` | 分析出力 |

この二重化が新参者の混乱を招く。このタスクでは**コードを変えず**、
ドキュメントとコメントだけを更新して「プレフィックスと概念の対応」を明示する。

---

## Hard constraints

- コード変更ゼロ（`.py` ファイル変更なし）
- テスト変更ゼロ

---

## Changes

### 変更 A: `CLAUDE.md` の Three-Layer Database Model セクション更新

以下の対応表を明記:

```markdown
### テーブル命名規則

| 概念層 (Medallion) | SQLite プレフィックス | 例 |
|-------------------|--------------------|----|
| BRONZE | `src_` | `src_anilist_anime`, `src_mal_anime` |
| SILVER | (プレフィックスなし) | `anime`, `persons`, `credits`, `sources`, `roles` |
| GOLD | `feat_`, `agg_`, `meta_` (分析) / `ops_` (業務出力) | `feat_person_scores`, `agg_milestones`, `meta_lineage` |
| 運用管理 | `ops_` | `ops_policy_score`, `ops_hr_observation` (v57 以降) |
```

### 変更 B: `src/database.py` の `init_db()` docstring 更新

セクションコメントに以下を追記:
```python
# Bronze (src_*): scraped raw data, immutable
# Silver (no prefix): canonical normalized data, score-free
# Gold (feat_/agg_/meta_/ops_*): analysis outputs
```

### 変更 C: `docs/ARCHITECTURE.md` の schema セクション更新

BRONZE/SILVER/GOLD と SQLite プレフィックスの対応を追記。

---

## Verification

```bash
pixi run lint   # clean (doc-only change)
grep -r "BRONZE\|SILVER\|GOLD" CLAUDE.md | head -5  # updated entries visible
```

---

## Commit message

```
docs: clarify BRONZE/SILVER/GOLD ↔ src_/feat_/ops_ prefix mapping
```
