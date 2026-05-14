# Task: cluster 修正後 audit + 戦略 LLM 再 review

**Status**: ✅ 完了 (2026-05-15、commit f0d4547、audit script `scripts/monitoring/audit_cluster_quality.py` 実装済、Jonas/David/サザエさん/あおきいろ 既知 over-merge 全件 check 機能統合)

**ID**: `19_resolved_cluster_fix/03_audit_post_fix`
**Priority**: 🟡 Medium
**Estimated changes**: ~+150 / -0 lines, 2 files (audit script + log)
**Requires senior judgment**: no (実行 + 結果記録)
**Blocks**: なし
**Blocked by**: 01_madb_parent_link, 02_persons_tmdb_homonym

---

## Goal

01 / 02 適用後の resolved 層全体 audit を実施し、`merge_strategy.json` の **3rd-round LLM verdict (wrong_value=12, split=3)** からの改善 / 退行を定量化する。

---

## Hard constraints

- 修正前の audit baseline 記録必須 (本 README 内の数値と照合)
- LLM コスト: 1 round = 9,394 sample = 約 USD 0.X、最大 2 round で打ち切り

---

## Pre-conditions

- [ ] 01 + 02 の done criteria 全達成
- [ ] `result/resolved.duckdb` 最新状態

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/monitoring/audit_cluster_quality.py` | (1) cluster size 分布 / (2) reason 分布 / (3) src 単独巨大 cluster top-N / (4) over-merge 既知 sample (Jonas / サザエさん) check |
| `result/audit/cluster_audit_v1.3.md` | 結果記録 (BEFORE / AFTER 比較表) |

---

## Implementation outline

### Step 1: audit script
```python
# scripts/monitoring/audit_cluster_quality.py
def audit():
    con = duckdb.connect("result/resolved.duckdb", read_only=True)

    # 1. cluster size 分布 (anime / persons / studios)
    for entity in ("anime", "persons", "studios"):
        size_dist = con.execute(...).fetchdf()
        # 大 cluster (size > 50) を warn

    # 2. reason 分布 比較 (本 README baseline と diff)
    # baseline: anime tie_break 6.5%, person tie_break 1.4%, person majority 0.2%

    # 3. 同 src 内大 cluster (over-merge 候補)
    # WHERE source_count > 50 AND len(distinct sources) = 1

    # 4. 既知 over-merge 修正検証
    # Jonas / David / サザエさん / あおきいろ
```

### Step 2: LLM 戦略再 review (オプション)
01 / 02 が成功した場合、`merge_strategy.json` 3rd-round 比較で:
- wrong_value: 12 (v1.2) → ? (v1.3 期待 < 5)
- split: 3 (v1.2) → ? (v1.3 期待 0-1)
- 新 issue (e.g. parent_madb_id 復元による未知の cluster 結合)

→ 改善があれば `merge_strategy.json` v1.3 にバンプ + version_history 追記。

### Step 3: 報告書 `result/audit/cluster_audit_v1.3.md`
| 項目 | BEFORE (v1.2) | AFTER (v1.3) | diff |
|---|---|---|---|
| anime cluster max size | 1089 | ≤ 10 (期待) | ✅ |
| persons cluster max (tmdb-only) | 47 | 1 (期待) | ✅ |
| anime tie_break ratio | 6.5% | ? | ? |
| persons tie_break ratio | 1.4% | ? | ? |
| credits 行数 | 2,801,457 | 不変期待 | ✅/❌ |
| LLM wrong_value | 12 | ? | ? |
| LLM split | 3 | ? | ? |

---

## Done criteria

- [ ] audit script 実装 + 実行可能
- [ ] BEFORE / AFTER 比較表完成
- [ ] (オプション) LLM 再 review 完了 + `merge_strategy.json` v1.3 bump
- [ ] 既知 over-merge 全件 (Jonas / サザエさん / あおきいろ / シナぷしゅ) 解消確認
- [ ] credits 行数 不変 (情報量保持確証)
- [ ] 退行 (新 over-merge / 過剰分離) ゼロ
