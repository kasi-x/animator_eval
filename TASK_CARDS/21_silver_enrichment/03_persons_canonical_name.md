# Task: persons canonical_name 列追加 (NFKC + 旧字体→新字体)

**ID**: `21_silver_enrichment/03_persons_canonical_name`
**Priority**: 🟡
**Estimated changes**: 約 +250 / -10 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

`persons` に `canonical_name_ja` 列を追加し、NFKC + 旧字体→新字体変換 + 全角/半角統一で正規化済の名前を保持する。entity_resolution は触らず、検索/dedup 補助情報として活用。

---

## Hard constraints

- **H3**: entity_resolution ロジック不変、本タスクは新規列追加のみ
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- 並列衝突回避: schema.py 末尾の `persons` 拡張セクションを末尾追記のみ (Card 04 と衝突しないよう注意)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] persons 行数: 271,070
- [ ] `pixi run test` baseline pass

---

## 設計

### canonical_name_ja 計算

```python
import unicodedata

# 旧字体→新字体マッピング (主要 200 字程度)
KYU_SHIN_MAP = {
    "邊": "辺", "邉": "辺", "齊": "斉", "齋": "斎", "靑": "青",
    "黒": "黒", "塚": "塚",  # 既新字
    # ... ja-kyu-shin など standard mapping
}

def canonical_name_ja(name: str | None) -> str | None:
    if not name:
        return None
    # 1. NFKC 正規化 (全角→半角、合成文字、互換文字)
    s = unicodedata.normalize("NFKC", name)
    # 2. 旧字体→新字体
    s = "".join(KYU_SHIN_MAP.get(ch, ch) for ch in s)
    # 3. 余分な空白除去
    s = " ".join(s.split())
    return s
```

外部ライブラリ候補:
- `mojimoji` (全角/半角): 既存依存ある? 確認要
- `jaconv` (NFKC + 旧→新): 軽量、推奨

`pixi.toml` 依存追加が必要なら追加。

### 既存データ backfill

```sql
-- python UDF or 一括計算
UPDATE persons SET canonical_name_ja = canonical_fn(name_ja)
WHERE canonical_name_ja IS NULL OR canonical_name_ja = ''
```

### 索引追加

```sql
CREATE INDEX IF NOT EXISTS idx_persons_canonical_name_ja
    ON persons(canonical_name_ja)
```

検索高速化用。

---

## Files to create / modify

| File | 変更内容 |
|------|---------|
| `src/etl/normalize/__init__.py` | 新規パッケージ init |
| `src/etl/normalize/canonical_name.py` | `canonical_name_ja(name)` 関数 + KYU_SHIN_MAP |
| `tests/test_etl/test_canonical_name.py` | 各種パターン (NFKC / 旧字体 / 全半角) |
| `src/db/schema.py` | persons に `canonical_name_ja TEXT` 列追加 (末尾追記) |
| `src/etl/silver_loaders/anilist.py` (選択) | INSERT 時に canonical_name_ja を設定 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |

---

## Steps

### Step 1: 依存ライブラリ確認/追加

```bash
grep -E "jaconv|mojimoji" pixi.toml
```

`jaconv` 不在なら `pixi.toml` に追加 (`pixi add jaconv`).

### Step 2: canonical_name.py 実装

KYU_SHIN_MAP は ja-kyu-shin 標準のものを参照 (Wikipedia 旧字体一覧 / Unicode 互換)。

### Step 3: schema.py 末尾追記

`persons` 拡張セクションに `canonical_name_ja` ALTER TABLE。

### Step 4: backfill 関数

```python
def backfill(conn) -> int:
    """persons テーブルに canonical_name_ja を一括書込。
    既に値ある行はスキップ (idempotent)。"""
```

CLI entry point: `pixi run python -m src.etl.normalize.canonical_name backfill`

### Step 5: テスト

```python
def test_nfkc():
    assert canonical_name_ja("Ｈ・Ｐ・ラブクラフト") == "H・P・ラブクラフト"

def test_kyu_shin():
    assert canonical_name_ja("渡邊") == "渡辺"
    assert canonical_name_ja("齊藤") == "斉藤"

def test_idempotent():
    s = canonical_name_ja("渡邊")
    assert canonical_name_ja(s) == s
```

### Step 6: 実行

```bash
pixi run python -m src.etl.normalize.canonical_name backfill
duckdb result/silver.duckdb -c "
SELECT name_ja, canonical_name_ja
FROM persons
WHERE name_ja != canonical_name_ja
LIMIT 20
"
```

旧→新変換例の sample 確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_canonical_name.py
duckdb result/silver.duckdb -c "
SELECT 
  COUNT(*) FILTER (WHERE canonical_name_ja IS NOT NULL) AS canonical,
  COUNT(*) FILTER (WHERE canonical_name_ja != name_ja) AS changed
FROM persons
"
```

期待: canonical > 250,000 (ほぼ全行)、changed > 0 (旧→新変換あり)

---

## Stop-if conditions

- [ ] `jaconv` 依存追加不可 (pixi 解決失敗) → 自前 mapping のみで着地
- [ ] backfill 中に index 競合
- [ ] 既存テスト破壊 (新規)

---

## Rollback

```bash
git checkout src/db/schema.py
rm -rf src/etl/normalize/
rm tests/test_etl/test_canonical_name.py
duckdb result/silver.duckdb -c "ALTER TABLE persons DROP COLUMN canonical_name_ja"
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] persons 全行 backfill 済
- [ ] 旧→新変換 sample 確認 ("渡邊" → "渡辺" 等)
- [ ] DONE: `21_silver_enrichment/03_persons_canonical_name`
