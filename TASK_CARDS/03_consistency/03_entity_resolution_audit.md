# Task: `meta_entity_resolution_audit` への書き込み追加

**ID**: `03_consistency/03_entity_resolution_audit`
**Priority**: 🟠 Major
**Estimated changes**: 約 +40 / -0 lines, 1-2 files
**Requires senior judgment**: **yes** (どの変数から merge evidence を取るか判断要)
**Blocks**: (なし)
**Blocked by**: (なし、独立して実行可)

---

## Goal

`src/analysis/entity_resolution.py` の merge 処理において、各 person の「統合理由」を `meta_entity_resolution_audit` テーブルに記録する。defamation 監査時に「なぜ A と B を同一人物と判定したか」を SQL 一発で調べられるようにする。

---

## Hard constraints

- **H3 entity resolution ロジックは不変**: 本タスクは**記録の追加のみ**。閾値・類似度計算・マージ条件を**絶対に変更しない**
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- すでに merge 処理が終わって `canonical_name` / `merge_method` / `merge_confidence` が決まっている箇所に **INSERT のみ** 追加
- 追加のロジック分岐や早期 return を書かない

---

## Pre-conditions

- [ ] `pixi run test` pass
- [ ] `meta_entity_resolution_audit` テーブルが DB 内に存在することを確認:
  ```bash
  sqlite3 result/animetor.db "PRAGMA table_info(meta_entity_resolution_audit);"
  # 期待: person_id, canonical_name, merge_method, merge_confidence,
  #       merged_from_keys, merge_evidence, merged_at 等
  ```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/analysis/entity_resolution.py` | merge 完了箇所で `meta_entity_resolution_audit` に INSERT |

共通ヘルパー化が適切なら:
| File | 変更内容 |
|------|---------|
| `src/analysis/entity_resolution_audit.py` (新規・任意) | `record_merge(conn, ...)` ヘルパー |

---

## Files to NOT touch

- **`entity_resolution.py` のマージ判定ロジック** (閾値、類似度計算、AI 呼び出し、step 順序、ブロッキング条件 **全て不変**)
- `src/analysis/ai_entity_resolution.py`
- `src/analysis/entity_resolution_eval.py`

---

## Steps

### Step 0: 対象テーブルのスキーマ確認

```bash
sqlite3 result/animetor.db "PRAGMA table_info(meta_entity_resolution_audit);"
```

想定カラム (実 DB で確認):
- `person_id` TEXT PRIMARY KEY
- `canonical_name` TEXT NOT NULL
- `merge_method` TEXT NOT NULL (`'exact_match'`, `'cross_source'`, `'romaji'`, `'similarity'`, `'ai_assisted'`, `'manual'`)
- `merge_confidence` REAL NOT NULL (0.0-1.0)
- `merged_from_keys` TEXT NOT NULL (JSON array, 例: `["anilist:123","ann:456"]`)
- `merge_evidence` TEXT NOT NULL (自然文)
- `merged_at` TIMESTAMP NOT NULL

スキーマが異なる場合は実 DB に合わせる。

### Step 1: merge 完了箇所の特定

```bash
grep -n 'canonical_name\|merge_method\|def.*resolve\|def.*merge' src/analysis/entity_resolution.py | head -20
```

merge 結果が確定する関数(例: `merge_clusters()`, `resolve_persons()`, `_finalize_cluster()` など)を特定。**名前はコードに応じて変わる**ので現物で確認。

### Step 2: INSERT 処理を追加 (記録のみ)

merge が確定した各 person について、以下の INSERT を追加:

```python
import json

def _record_entity_merge(
    conn,
    *,
    person_id: str,
    canonical_name: str,
    merge_method: str,
    merge_confidence: float,
    source_keys: list[str],
    evidence: str,
) -> None:
    """Record an entity merge decision for later audit.

    This function MUST NOT influence merge logic — it only persists
    a record after the decision has been made elsewhere.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO meta_entity_resolution_audit
            (person_id, canonical_name, merge_method, merge_confidence,
             merged_from_keys, merge_evidence, merged_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            person_id,
            canonical_name,
            merge_method,
            float(merge_confidence),
            json.dumps(source_keys, ensure_ascii=False),
            evidence,
        ),
    )
```

そして既存の merge 完了箇所から呼び出す:

```python
# 例: exact_match_cluster 完了後
for cluster in exact_match_clusters:
    canonical_id = cluster.canonical_id  # 既存ロジックで決まった
    canonical_name = cluster.canonical_name
    _record_entity_merge(
        conn,
        person_id=canonical_id,
        canonical_name=canonical_name,
        merge_method="exact_match",
        merge_confidence=1.0,
        source_keys=[m.source_key for m in cluster.members],
        evidence=f"exact name match across {len(cluster.members)} sources",
    )
```

**merge method ごとに evidence 文字列**を使い分ける:
| method | evidence 例 |
|--------|------------|
| `exact_match` | `"exact name match across N sources"` |
| `cross_source` | `"cross-source ID match (anilist=123, ann=456)"` |
| `romaji` | `"romaji normalization: 渡辺→Watanabe (JW=1.00)"` |
| `similarity` | `"Jaro-Winkler=0.97, shared works=3"` |
| `ai_assisted` | `"LLM decision (Qwen3): confidence=0.85"` |

### Step 3: トランザクション境界の確認

`entity_resolution.py` は大量の person を処理するため、INSERT をループ内で 1 件ずつ commit すると遅い。既存の batch commit 境界に合わせて `executemany` or 同じトランザクションで複数 INSERT するのが望ましい。

### Step 4: テスト追加

`tests/test_entity_resolution_audit.py` (新規):

```python
def test_exact_match_records_audit_row(tmp_path, monkeypatch):
    # ...既存の entity_resolution テストパターンを参考に
    # exact_match で 2 person が merge される fixture を用意
    # merge 実行後、meta_entity_resolution_audit に対応行があること
    ...
```

既存 fixture が使えるなら使う。**merge ロジック自体のテストは変えない** (H3)。

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "entity_resolution"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile src/analysis/entity_resolution.py

# 2. INSERT が追加された
rg -n 'meta_entity_resolution_audit' src/analysis/
# 期待: 1+ 件 (INSERT 文)

# 3. merge ロジックが不変
git diff src/analysis/entity_resolution.py | grep -E '^[-+].*(threshold|similarity|confidence_min|jaro|cluster)'
# 期待: 0 件 (閾値や類似度関連のロジック行に変更なし)

# 4. 既存テスト pass
pixi run test -- -k "entity_resolution" -v
# 期待: 既存テスト + 新規 audit テストが全て pass

# 5. 全テスト
pixi run test-scoped tests/ -k "entity_resolution"

# 6. Lint
pixi run lint

# 7. 実 pipeline で audit 行が入る
# (smoke 実行、synthetic データで)
pixi run python -c "
import tempfile, pathlib
from src.database import get_connection, init_db, run_migrations
# ... 最小 fixture で entity_resolution を走らせ、
# meta_entity_resolution_audit から SELECT して空でないことを確認
"
```

---

## Stop-if conditions

- [ ] Verification 3 で merge ロジック関連行が変更されている **→ H3 違反、即中断**
- [ ] 既存の entity_resolution テスト fail
- [ ] merge method の evidence 文字列が不明確 (ユーザ質問)

---

## Rollback

```bash
git checkout src/analysis/entity_resolution.py
rm -f src/analysis/entity_resolution_audit.py tests/test_entity_resolution_audit.py
pixi run test-scoped tests/ -k "entity_resolution"
```

---

## Completion signal

- [ ] 全 verification pass
- [ ] `git diff` で merge ロジック関連の変更が**ない**ことを目視確認
- [ ] `git commit`:
  ```
  Record entity merge decisions in meta_entity_resolution_audit

  Adds a record-only INSERT at each merge confirmation point.
  Merge logic, thresholds, and similarity computations are
  explicitly unchanged (H3 invariant).

  Enables SQL-level audit: "why were person A and B merged?"
  ```
