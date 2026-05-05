"""Field 別の value validator (代表値選抜時の異常値検出)。

LLM 検証 (3 周目) で残った 12 件 wrong_value のうち、戦略 priority list
で解決できないデータ品質問題を value-level で検出。invalid 判定 → 次 tier
fallback。

検出ルール (LLM 検証結果に基づく):

| field | rule | LLM 検出例 |
|---|---|---|
| anime.title_en | 数字のみ | TMDb '546456' |
| anime.title_en | 全文字逆スペル疑い (`Snospis Eht`) | 検出困難、保留 |
| person.name_ja | 末尾角括弧 suffix `[製作]` 等 | madb '日映科学映画製作所[製作]' |
| person.name_ja | 機関/組織名キーワード | madb '立命館大学政策科学研究科' |
| person.name_ja | 空白区切り 3+ 名 (複数人混在) | seesaa '越智浩一 池口裕児 石野桂子 Adil Tahir' |
| person.name_en | 数字のみ | (将来検出用) |

Note: LLM 誤判定 (null 採用が正しいケース等) は補正対象外。
"""

from __future__ import annotations

import re
from typing import Any

# 機関/組織キーワード (人名と区別)
_INSTITUTION_KEYWORDS = (
    # 教育/研究
    "大学", "研究科", "学部", "学院", "研究所", "研究室", "高等学校", "高校",
    # 営利法人
    "株式会社", "(株)", "（株）", "有限会社", "(有)", "（有）",
    "合同会社", "(同)", "（同）",
    # 公益/財団
    "財団法人", "公益財団", "社団法人", "公益社団",
    "一般社団", "一般財団", "特定非営利活動法人", "NPO法人",
    "独立行政法人",
    # 制作所/出版/医療/宗教
    "製作所", "出版社", "出版会", "印刷所",
    "病院", "教会", "神社", "寺院",
    # 公的機関
    "市役所", "区役所", "町役場", "府庁", "都庁", "政府",
)

_ROLE_SUFFIX_RE = re.compile(r"[\[【［].*?[\]】］]\s*$")

# 全角/半角空白区切り
_SPLIT_RE = re.compile(r"[\s　]+")


def _is_numeric_only(value: str) -> bool:
    """数字 (+空白/区切り) のみで構成されているか。"""
    return bool(re.fullmatch(r"[\d\s\-_/.]+", value.strip()))


# 日本語文字 (CJK Unified / Hiragana / Katakana / 半角カタカナ / 全角英数等)
_JA_CHAR_RE = re.compile(
    r"[぀-ゟ"   # Hiragana
    r"゠-ヿ"   # Katakana
    r"一-鿿"   # CJK Unified
    r"㐀-䶿"   # CJK Extension A
    r"ｦ-ﾟ"   # Hankaku Katakana
    r"]"
)


def _has_japanese_char(value: str) -> bool:
    """value に日本語 (ひらがな/カタカナ/漢字/半角カナ) が 1 文字でも含まれるか。"""
    return bool(_JA_CHAR_RE.search(value))


# Episode/Lesson/Track 等の番号 suffix
_EPISODE_TOKEN_RE = re.compile(
    r"\b(Episode|Lesson|Track|Vol|Volume|Chapter|Part|Stage|Round|Ep|Eps|EP)\s*[\d０-９]+\b",
    re.IGNORECASE,
)


def _has_episode_token(value: str) -> bool:
    """'Episode 6' 'Lesson 21' 'Track-12' 等のエピソード番号 token を含むか。"""
    return bool(_EPISODE_TOKEN_RE.search(value))


def _has_role_suffix_brackets(value: str) -> bool:
    """末尾に角括弧 suffix `[製作]` 等を持つか。"""
    return bool(_ROLE_SUFFIX_RE.search(value))


def _is_institution_name(value: str) -> bool:
    """機関/組織名 (人名でない) か判定。"""
    return any(k in value for k in _INSTITUTION_KEYWORDS)


def _has_multiple_persons(value: str) -> bool:
    """空白区切りで 3+ token = 複数人混在の可能性。

    人名は通常 1 token (`田中太郎`) または 2 token (`田中 太郎` / `John Smith`)。
    3+ token は別人物の連結と推定。
    """
    tokens = [t for t in _SPLIT_RE.split(value.strip()) if t]
    return len(tokens) >= 3


def is_invalid_for_field(field: str, value: Any) -> bool:
    """field 別の value 妥当性チェック。invalid → True。

    戻り値 True の場合、その値は採用せず次 tier fallback。
    """
    if not isinstance(value, str):
        return False  # 非 str は別 layer で扱う
    v = value.strip()
    if not v:
        return False  # 空文字は _is_empty で別途扱う (本 module では「異常値」のみ)

    if field in ("title_en", "title_ja"):
        if _is_numeric_only(v):
            return True

    if field == "title_ja":
        # 日本語文字 1 つも含まない値は title_ja として invalid
        # (madb の 'GUN HAZARD' 'TECMO SUPER BOWL' 'JUST DANCE WiiU' 等)
        if not _has_japanese_char(v):
            return True
        # 'Episode 6' 'Lesson 21' 等のエピソード番号 suffix を含む
        # (madb の 'オーバーロードⅣ Episode6' 等)
        if _has_episode_token(v):
            return True

    if field == "name_ja":
        if _has_role_suffix_brackets(v):
            return True
        if _is_institution_name(v):
            return True
        if _has_multiple_persons(v):
            return True

    if field == "name_en":
        if _is_numeric_only(v):
            return True

    return False


# ── Cleansing (parser 修正時の参照実装) ─────────────────────────────────────
#
# 検出された異常値の根本原因は parser/scraper 側 (seesaawiki: 複数名 1 cell
# 24,124 件、madb: 末尾 [xxx] suffix 8,130 件)。本来は scraper / parser を
# 直して bronze 段階から綺麗にすべき。
#
# 現状は `is_invalid_for_field()` を Resolved 層 _select で使う一段防衛のみ。
# 以下の cleanse_* 関数は **未使用**。parser 修正時に同じ判定ロジックを
# bronze writer / scraper の name 抽出処理に移植するための参照実装として残す。
#
# 移植先候補:
#   - src/scrapers/seesaawiki_scraper.py の persons 抽出 (複数名 split)
#   - src/scrapers/mediaarts_scraper.py の credit 抽出 (suffix strip)
#
# TODO: parser 修正後、本 module の cleanse_* は削除可。validator は残す
#       (上流追加 source や既知データ汚染への二次防衛として有用)。


def cleanse_name_ja(value: Any) -> str | None:
    """name_ja の正規化。None/空/異常値は None 返却、末尾 [xxx] suffix は strip。

    挙動:
      - None / 空文字 → None
      - 機関名キーワード含む → None
      - 空白区切り 3+ tokens (複数名混在) → None
      - 末尾 [xxx] / 【xxx】 suffix → strip して残部返却 (空なら None)
      - その他 → strip した値
    """
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if _is_institution_name(s):
        return None
    if _has_multiple_persons(s):
        return None
    # 末尾 [xxx] strip
    s = _ROLE_SUFFIX_RE.sub("", s).strip()
    if not s:
        return None
    return s


def cleanse_title(value: Any) -> str | None:
    """title (ja/en) の正規化。数字のみは None、その他 strip。"""
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if _is_numeric_only(s):
        return None
    return s
