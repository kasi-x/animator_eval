# Task: スタジオ別 若手育成パイプライン強度 (bus factor)

**ID**: `26_industry_structure/03_studio_pipeline_strength`
**Priority**: 🟠
**Estimated changes**: +400 / -0 lines, 3 files
**Requires senior judgment**: medium
**Blocks**: HR brief / Business brief 「組織健全性」section
**Blocked by**: なし

---

## Goal

スタジオごとに「若手 theta_i 成長率」「中堅滞留率」「key person 集中度」を算出し、
事業継続性 (bus factor) と人材パイプラインの健全性を可視化。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business (投資家)** | 投資判断時の人材リスク評価 |
| **Business (HR)** | 自社 vs 他スタジオベンチマーク |
| **政策** | 中堅枯渇 (O2) と接続、業界全体パイプライン |

---

## Hard constraints

- **H1**: anime.score 不可。pipeline metric は credit 構造のみ
- 「優秀な若手」frame 禁止 → 「theta 成長軌跡」「役職進行率」
- スタジオ単位の名指しは「Findings」と「Interpretation」分離必須

---

## Method

### Metrics by studio s, year y

```
young_theta_growth[s, y]    = mean(Δtheta_i / year) for i with tenure < 5 at s
mid_career_retention[s, y]  = P(staff at s in year y | staff at s in year y-3, tenure 5-15)
key_person_concentration[s, y] = top-3 staff の credit_share total at s
bus_factor[s, y]            = inverse of HHI on staff credit_share
```

### CI

cluster bootstrap (cluster = staff)、studio-year level で 95% CI。

### Comparison

- studio tier (大手 / 中堅 / 個人) 別分布
- structural break (主要スタジオ複数で同期した変化点)

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/studio/pipeline_strength.py` | metric 計算 |
| `scripts/report_generators/reports/studio_pipeline.py` | HR + Business brief |
| `tests/analysis/studio/test_pipeline_strength.py` | toy studio で検証 |

---

## Pre-conditions

- [ ] AKM theta_i 安定
- [ ] credits.evidence_source 健全 (H4)
- [ ] スタジオ tenure 推定の baseline 確立

---

## Stop-if

- 若手 sample n < 30 per studio per year → metric 不安定、studio aggregation 粗くする
