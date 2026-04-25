# Task: 人物ページ parser 実装 (credit 抽出)

**ID**: `09_sakuga_atwiki/03_person_parser`
**Priority**: 🔴
**Estimated changes**: 約 +500 / -0 lines, 3 files
**Requires senior judgment**: yes (regex 設計と LLM fallback 判定ロジック)
**Blocks**: `09_sakuga_atwiki/04_bronze_export`
**Blocked by**: `09_sakuga_atwiki/02_page_discovery`

---

## Goal

`page_kind == "person"` と分類された HTML を parse し、`ParsedSakugaPerson` dataclass (name, aliases, credits = list[ParsedSakugaCredit]) を返す parser を実装する。regex 主系 + LLM fallback (既存 Ollama/Qwen3 パターン踏襲)。

---

## Hard constraints

- `_hard_constraints.md` 参照
- **H2 能力 framing 禁止**: 本文中の主観評価テキスト (「神作画」「作画崩壊」等) は **parse 対象に含めない**。ParsedSakugaCredit に評価列を持たせない
- **H3 entity resolution 不変**: 本 card は BRONZE 生データの parse のみ。マッチング処理は一切書かない
- **個人情報除外**: 本名・生年月日・住所等が本文にあっても parse しない / 保存しない (`extract_credits` は role + work + episode のみ返す)
- LLM 呼び出し: `src/scrapers/seesaawiki_scraper.py` の `llm_fallback` パターンを参照、モデル (`qwen3:8b`) / endpoint を共通化

---

## Pre-conditions

- [ ] `09_sakuga_atwiki/02_page_discovery` 完了
- [ ] `data/sakuga/cache/*.html.gz` に人物ページが最低 30 件蓄積済
- [ ] `data/sakuga/discovered_pages.json` に `page_kind == "person"` エントリが 30 件以上

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/parsers/sakuga_atwiki.py` | 拡張: `parse_person_page(html: str) -> ParsedSakugaPerson` 追加 |
| `src/runtime/models.py` | 追加: `ParsedSakugaPerson`, `ParsedSakugaCredit` dataclass |
| `tests/scrapers/test_sakuga_atwiki_parser.py` | **新規**: fixture 30 件での parse 率/正解率テスト |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 不変 |
| `src/scrapers/seesaawiki_scraper.py` | 今 card では他 parser を壊さない、但し実装参考にしてよい |

---

## Steps

### Step 1: データクラス定義

`src/runtime/models.py` に追加 (既存 `Credit` / `Person` とは別に BRONZE 専用):

```python
@dataclass(frozen=True, slots=True)
class ParsedSakugaCredit:
    work_title: str          # raw (「AKIRA」「機動警察パトレイバー the Movie 3」等)
    work_year: int | None    # 抽出できれば。失敗時 None
    work_format: str | None  # "劇場" / "TV" / "OVA" / "TVSP" / None
    role_raw: str            # raw role string (「原画」「作画監督」「オープニング作画」等)
    episode_raw: str | None  # raw episode 指定 (「3話」「#5,7,9」「OP」等)
    episode_num: int | None  # 正規化後の話数。複数話指定時は先頭のみ。範囲/リスト詳細は episode_raw に保持

@dataclass(frozen=True, slots=True)
class ParsedSakugaPerson:
    page_id: int
    name: str                # ページ title から抽出した主名
    aliases: list[str]       # 別名・英字綴り・旧芸名
    active_since_year: int | None   # 最初期クレジット年 (抽出できれば)
    credits: list[ParsedSakugaCredit]
    source_html_sha256: str  # キャッシュ照合用
```

### Step 2: regex 主系 parser

`parse_person_page(html)` のロジック:

1. **title 抽出**: `<title>下谷智之 - 作画@wiki - atwiki...</title>` → `name = "下谷智之"`
2. **wikibody 抽出**: `<div id="wikibody">` 内のみ対象
3. **別名抽出**: 本文先頭の「別名」「旧名」「英字表記」直後の行を取得
4. **セクション分割**: h2/h3 の「フィルモグラフィ」「参加作品」「代表作」以下を credit 候補ブロックとする
5. **行単位 parse**:
   - パターン A: `[作品名] ([スタジオ].[年]) — [役職]` 形式
   - パターン B: 箇条書き `- [役職] [話数]` (先行見出しが作品名)
   - パターン C: テーブル形式 (tr/td を列ごとに分解)
   - 既存 `src/scrapers/parsers/seesaawiki.py` の `parse_credit_line` を参考に、同種のヘルパーを sakuga 用に実装
6. **話数正規化**: `_parse_episode_ranges` 相当のヘルパーを sakuga 側でも用意 (seesaawiki 版の import は NG — 依存グラフ独立を維持)

### Step 3: LLM fallback

regex で `credits` が 0 件かつ wikibody の長さが 500 文字以上 (= 内容はあるのに parse 失敗) の場合:

- `llm_fallback(wikibody_text) -> list[ParsedSakugaCredit]` を呼ぶ
- Prompt: few-shot (人物ページサンプル 2 件 → 期待 JSON 出力) を固定し、temperature=0
- **Ollama 不在 / タイムアウト時は graceful degradation** (credits=[] で返却、`evidence_source=` に `"sakuga_atwiki:regex_failed"` タグ)
- fallback は既存 `seesaawiki_scraper.py` のパターンを踏襲

### Step 4: 品質測定スクリプト (scripts/)

`scripts/measure_sakuga_parser.py` (新規):

- `discovered_pages.json` の全 `person` ページについて parse 実行
- 統計出力: total / regex_ok / llm_fallback_ok / 完全失敗 件数、credit 抽出数分布 (ヒストグラム)
- CI には含めない (手動実行)

### Step 5: テスト

`tests/scrapers/test_sakuga_atwiki_parser.py`:

- fixture 30 件 (実ページの最小サブセット、個人名は masked しても可) を `tests/fixtures/sakuga/persons/` に保存
- 各 fixture について期待 credit 件数 ±10% を許容範囲としてアサート
- regex 0 件 → LLM fallback 呼び出しがモック経由で行われることのテスト (Ollama 実 call はしない)

---

## Verification

```bash
# 1. Unit
pixi run test-scoped tests/scrapers/test_sakuga_atwiki_parser.py
pixi run test   # 全体 green

# 2. Lint
pixi run lint

# 3. 品質測定 (実キャッシュ経由、HTTP 無し)
pixi run python scripts/measure_sakuga_parser.py
# 期待: regex_ok / total >= 0.70 (個人ページの 70% 以上を regex のみで parse 可能)

# 4. 主観語混入チェック (defamation 防止)
pixi run python -c "
from src.scrapers.parsers.sakuga_atwiki import parse_person_page
# fixture サンプルを一つ parse → credits の role_raw/work_title に
# 「神」「崩壊」「ダメ」等の主観語が含まれないことをアサート
"

# 5. H2 lint
rg -E '(ability|skill|talent|competence|capability|能力|実力|優秀)' src/scrapers/parsers/sakuga_atwiki.py
# → 0 件 (変数名・コメント含む)
```

---

## Stop-if conditions

- [ ] regex_ok / total < 0.50 → regex 設計を見直す (LLM fallback 任せにしない)
- [ ] `pixi run test` が既存テストを 1 件でも break
- [ ] fixture 30 件で credit 総数が 0 → parser が根本的に動作していない
- [ ] 主観語混入が 1 件でも検出 → 即中断、抽出ロジックから本文地の文を除外

---

## Rollback

```bash
git checkout src/scrapers/parsers/ src/runtime/models.py
rm -f tests/scrapers/test_sakuga_atwiki_parser.py
rm -rf tests/fixtures/sakuga/
rm -f scripts/measure_sakuga_parser.py
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] 品質測定で regex_ok/total >= 0.70
- [ ] `git diff --stat` が ±500 lines 以内
- [ ] 作業ログに `DONE: 09_sakuga_atwiki/03_person_parser` と記録
