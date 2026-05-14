# Task: funding 計画設計

**ID**: `31_business/03_funding_plan`
**Priority**: 🟡
**Estimated changes**: doc-only
**Requires senior judgment**: yes
**Blocks**: なし (なくても継続可)
**Blocked by**: `31_business/01_startup_form` (法人形態確定)

---

## Goal

学振 / 民間助成 / 科研費 / VC / B2C 自走 の優先順位を設計。
labor-first スタンス (`STANCE.md §7.2`) との整合性を確保。

---

## 候補と特性

| 候補 | 規模 | 期間 | 制約 | labor-first 整合性 |
|------|------|------|------|-------------------|
| 自己資金 + 給与収入 | 〜数十万 / 年 | 継続 | 規模拡大不可 | ◎ (制約なし) |
| 学振 PD | 年 ~430 万 | 3 年 | D 修了後、テーマ固定 | ◎ |
| 学振 DC | 年 ~250 万 | 2-3 年 | D 在学中 | ◎ |
| 科研費 (代表) | 年 100-1000 万 | 3-5 年 | 研究機関所属、研究目的 | ◎ |
| SciREX | 年 数百万 | 1-3 年 | 政策研究目的 | ◎ |
| 財団助成 | 数十-数百万 | 1-2 年 | 多様、文化系も | ○-◎ |
| Anthropic Startup | クレジット | 期間限定 | API 利用前提 | ○ |
| VC (seed) | 数千万-1 億 | 数年 | exit 圧力 | △ (使用者 exit 圧力との緊張) |
| B2C 自走 | 売上次第 | — | 顧客獲得必要 | ◎ (主流線) |

---

## 推奨優先順 (案)

1. **自己資金 + 学振 (DC 在学 / PD D 修了後)** — 主軸
2. **科研費 (代表) or 民間助成** — 機関 affiliation 経由
3. **B2C 自走** — 中長期、`31_business/02_b2c_design` 完成後
4. **VC** — 必要 + labor-first スタンス維持できる term sheet が出る場合のみ
5. **Anthropic Startup** — クレジット補助、本筋ではない

---

## Steps

### Step 1: 学振応募スケジュール確認

- DC: 通常 5 月公募 → 翌春採用
- PD: 通常 5 月公募 → 翌春採用
- 残年限と齟齬がないかチェック

### Step 2: 民間助成 / 財団検索

- アーツカウンシル東京 / 新国立劇場 / セゾン文化財団 / カシオ科学振興財団 / 等
- 文化系 + 情報系 + 労働経済系のクロス領域可能な財団

### Step 3: VC 探索 (必要時のみ)

- labor-first スタンスを term sheet に明記できる lead investor を探す
- exit 圧力との折り合い (impact 投資・mission lock-in)

---

## Pre-conditions

- [ ] `31_business/01_startup_form` 完了 (法人形態確定で応募主体決定)
- [ ] 残年限 / 学振応募可能性確認 (ユーザーへの質問)

---

## Stop-if

- 学振 / 科研費 全落ち → B2C 自走 + 民間助成 に縮退
