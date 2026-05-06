# Task: canonical_id collision による silent drop 修正

**ID**: `19_resolved_cluster_fix/04_canonical_id_collision`
**Priority**: 🔴 Critical
**Created**: 2026-05-07
**Estimated changes**: ~+80 / -15 lines, 1 file + 1 test file
**Blocks**: resolved.anime 全数保証、AKM / scoring 全般
**Blocked by**: なし (独立修正)

---

## Goal

`build_cross_source_anime_clusters` の canonical_id 生成をタイトル+年文字列ハッシュから
**ソート済みメンバ ID ハッシュ**に置換し、`year=None` + 同タイトルの独立 UF group 間で
canonical_id が衝突して dict 上書きされる silent drop を解消する。

---

## Hard constraints

- **H1**: anime.score 系列は触らない
- **H3**: entity_resolution の merge 条件 (UF union logic) は不変。canonical_id 計算式のみ変更
- **既存 DB rebuild 未実施**: 本タスクはコード修正 + テスト止まり。`result/resolved.duckdb`
  の再構築はユーザが別途実行する
- **persons / studios の canonical_id ロジック不変** (`_persons_cluster.py` は別設計)

---

## Pre-conditions

- [x] `git status` に関係ない変更が混在していないこと
- [x] `pixi run lint` が clean であること
- [x] `tests/test_etl/test_resolved_canonical_id_uniqueness.py` が存在すること

---

## Files to modify

| File | 変更内容 |
|------|----------|
| `src/etl/resolved/_cross_source_ids.py` | `_compute_canonical_id` 関数追加 + `build_cross_source_anime_clusters` の line 318-335 書き換え |

## Files to create

| File | 内容 |
|------|------|
| `tests/test_etl/test_resolved_canonical_id_uniqueness.py` | 3 ケース regression テスト |

---

## Implementation outline

### 問題: cluster_key がタイトル+年文字列に依存していた

旧実装 (bug):
```python
cluster_key = f"{_norm(title_ja)}|{rep.get('year') or ''}"
digest = hashlib.sha256(cluster_key.encode()).hexdigest()[:12]
canonical_id = f"resolved:anime:{digest}"
result[canonical_id] = sc_rows  # dict 上書き → silent drop
```

`rep.year=None` かつ `subclusters==1` 時に `cluster_key = "{title}|"` となり、
別 UF group の同タイトル + `year=None` rep と衝突。`result[canonical_id] = sc_rows` は
後勝ちで上書きされる。

### 修正: メンバ ID ソートハッシュに置換

```python
def _compute_canonical_id(
    member_rows: list[dict[str, Any]],
    format_suffix: str | None,
) -> str:
    import hashlib
    member_ids_sorted = sorted(r["id"] for r in member_rows)
    parts = list(member_ids_sorted)
    if format_suffix:
        parts.append(f"__fmt__{format_suffix}")
    key = "\x1f".join(parts)  # ASCII unit separator
    digest = hashlib.sha256(key.encode()).hexdigest()[:12]
    return f"resolved:anime:{digest}"
```

`build_cross_source_anime_clusters` の for ループ末尾を以下に置換:
```python
fmt_suffix = (rep.get("format") or "").strip().upper() or None if len(subclusters) >= 2 else None
canonical_id = _compute_canonical_id(sc_rows, format_suffix=fmt_suffix)
result[canonical_id] = sc_rows
```

### 設計原則

- **Idempotent**: 同じ member set + format_suffix → 同じ canonical_id (rebuild でも安定)
- **Collision-free**: 異なる member set → 異なる canonical_id (distinct UF group は必ず別 ID)
- **format_suffix**: UF group を複数 subcluster に format 分割した場合のみ渡す (単一 subcluster は None)

---

## Audit / verification

### 影響数値 (修正前の実証データ)

- conformed 506,673 row → resolved cluster member 合計 486,162 row = **20,511 row silent drop (4.0%)**
- 実証症例: `サイボーグ009` canonical_id `resolved:anime:f0a80109dc92`
  - UF group A (bgm:s13605, year=None, fmt=NULL) が
  - UF group B (keyframe:..., anilist:8394, mal:a8394, tmdb:tv:56427, year=1968/None, fmt=TV) を上書き
  - → 1968 TV cluster 完全消失 (4 row)

### 修正後の期待値

- canonical_id 衝突数 = 0 (by construction: 異なるメンバ → 異なるハッシュ)
- 全 conformed row が何らかの cluster に流入 (sum == N)

---

## Known caveats (許容仕様)

1. **メンバ追加・削除で canonical_id 変動**: conformed snapshot が変わると canonical_id も変わる。
   downstream credits / scores は全 rebuild 前提。fixed input に対しては idempotent を維持。
2. **既存 `result/resolved.duckdb` の anime canonical_id は全部別物になる**: 全 rebuild 必須。
   本タスクでは rebuild 実行しない (ユーザ判断)。

---

## Rollback

```bash
git revert HEAD  # _cross_source_ids.py + test file の変更を元に戻す
```

DB は変更していないので rollback 不要。

---

## Done criteria

- [x] `src/etl/resolved/_cross_source_ids.py` 修正完了
  - [x] `_compute_canonical_id` 関数追加 (docstring: Idempotent / Collision-free 明記)
  - [x] `build_cross_source_anime_clusters` の cluster_key 文字列生成を `_compute_canonical_id` 呼び出しに置換
- [x] `tests/test_etl/test_resolved_canonical_id_uniqueness.py` 作成
  - [x] ケース 1: サイボーグ009 1968 TV regression (2 cluster, 4 row cluster member 検証)
  - [x] ケース 2: 全 row coverage (sum == N, year=None 独立グループ 3 つ衝突なし)
  - [x] ケース 3: idempotency (shuffle 不変, 単体 hash 検証)
- [x] `pixi run lint` clean
- [x] `pixi run test-scoped tests/test_etl/test_resolved_canonical_id_uniqueness.py` pass
- [ ] `result/resolved.duckdb` 全 rebuild (ユーザ判断 — 本タスク scope 外)
