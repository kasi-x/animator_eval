# Task: 高位 venue 投稿時の snapshot + Zenodo DOI exception

**ID**: `32_publication/01_replication_snapshot_exception`
**Priority**: 🟡
**Estimated changes**: doc-only + script (snapshot 取得自動化)
**Requires senior judgment**: low
**Blocks**: Labour Economics / J. Cultural Economics 投稿
**Blocked by**: なし

---

## Goal

通常運用は snapshot_policy = not_taken (`STANCE.md §5.4`) だが、
高位 venue 投稿時のみ snapshot + Zenodo DOI を **exception** として取得する仕組みを整える。

---

## 経路別価値

publication 経路の必須通過点 (Labour Economics 等は data availability 強制)。

---

## Method

### snapshot 内容

- `result/resolved.duckdb` (Resolved 層)
- `result/gold.duckdb` (Mart)
- 当該論文で使用した `src/analysis/` コード hash
- 当該論文の version of `pixi.lock`
- `docs/method_notes/<paper>.md`

### 配布先

- Zenodo (DOI 付与)
- 論文に DOI 記載

### exception 発火条件

- Labour Economics / J. Cultural Economics / Applied Network Science journal 投稿時
- それ以外 (preprint / workshop / 国内学会) は通常 not_taken

### Frozen score version

論文記載の数値が時間経過後も再現できるよう、
λ 重み + score 計算結果を当該論文用に **frozen version tag** で保存:

```sql
INSERT INTO mart.meta_score_frozen
  (paper_id, person_id, theta_i, iv, frozen_at, lambda_weights_json)
VALUES ...
```

将来 `lambda_recal = data_driven` で λ が更新されても、論文用 frozen 数値は不変。

---

## Files

| File | 内容 |
|------|------|
| `scripts/publication/snapshot.py` | snapshot 取得 + Zenodo upload |
| `src/db/schema.py` | meta_score_frozen table 追加 |
| `docs/method_notes/replication_snapshot.md` | exception 発火条件 / 手順 |

---

## Pre-conditions

- [ ] STANCE.md §5.4 (replication policy) 確定 (済)
- [ ] 最初の高位 venue 投稿予定確定

---

## Stop-if

- Zenodo に必要 metadata 不足 → 別 archive (figshare / 大学リポジトリ) 検討
