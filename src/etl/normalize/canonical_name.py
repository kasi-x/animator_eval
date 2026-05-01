"""persons.canonical_name_ja 正規化モジュール (Card 21/03).

NFKC + 旧字体→新字体変換 + 余白正規化で canonical_name_ja を生成する。
entity_resolution ロジックは変更しない (H3)。

CLI entry point:
    python -m src.etl.normalize.canonical_name backfill
"""
from __future__ import annotations

import sys
import unicodedata
from typing import Optional


# ---------------------------------------------------------------------------
# 旧字体 → 新字体 マッピング (Kyujitai → Shinjitai)
# JIS X 0208 旧字体一覧 + 人名で頻出する追加字形を収録。
# no-op (key == value) エントリは除外済み。
# ---------------------------------------------------------------------------
KYU_SHIN_MAP: dict[str, str] = {
    # ─── あ行 ─────────────────────────────────────────────────────────────
    "亞": "亜",
    "惡": "悪",
    "壓": "圧",
    "圍": "囲",
    "爲": "為",
    "醫": "医",
    "壹": "壱",
    "稻": "稲",
    "飮": "飲",
    "隱": "隠",
    "營": "営",
    "榮": "栄",
    "衞": "衛",
    "驛": "駅",
    "圓": "円",
    "鹽": "塩",
    "奧": "奥",
    "應": "応",
    "歐": "欧",
    "黃": "黄",
    "溫": "温",
    "穩": "穏",
    # ─── か行 ─────────────────────────────────────────────────────────────
    "假": "仮",
    "價": "価",
    "畫": "画",
    "懷": "懐",
    "繪": "絵",
    "擴": "拡",
    "殼": "殻",
    "覺": "覚",
    "學": "学",
    "嶽": "岳",
    "樂": "楽",
    "勸": "勧",
    "卷": "巻",
    "關": "関",
    "觀": "観",
    "歡": "歓",
    "龜": "亀",
    "舊": "旧",
    "據": "拠",
    "擧": "挙",
    "峽": "峡",
    "挾": "挟",
    "敎": "教",
    "驅": "駆",
    "區": "区",
    "勳": "勲",
    "薰": "薫",
    "徑": "径",
    "惠": "恵",
    "輕": "軽",
    "藝": "芸",
    "擊": "撃",
    "缺": "欠",
    "劍": "剣",
    "權": "権",
    "縣": "県",
    "效": "効",
    "廣": "広",
    "號": "号",
    "國": "国",
    "黑": "黒",
    # ─── さ行 ─────────────────────────────────────────────────────────────
    "濟": "済",
    "齋": "斎",
    "碎": "砕",
    "齊": "斉",
    "剩": "剰",
    "纖": "繊",
    "辭": "辞",
    "實": "実",
    "寫": "写",
    "釋": "釈",
    "壽": "寿",
    "從": "従",
    "縱": "縦",
    "獸": "獣",
    "澁": "渋",
    "收": "収",
    "處": "処",
    "獎": "奨",
    "將": "将",
    "燒": "焼",
    "稱": "称",
    "乘": "乗",
    "淨": "浄",
    "狀": "状",
    "疊": "畳",
    "孃": "嬢",
    "讓": "譲",
    "寢": "寝",
    "愼": "慎",
    "盡": "尽",
    "圖": "図",
    "澀": "渋",
    "攝": "摂",
    "戰": "戦",
    "潛": "潜",
    "總": "総",
    "續": "続",
    "臟": "臓",
    "帶": "帯",
    "滯": "滞",
    "體": "体",
    "對": "対",
    # ─── た行 ─────────────────────────────────────────────────────────────
    "燈": "灯",
    "當": "当",
    "黨": "党",
    "讀": "読",
    "獨": "独",
    # ─── な行 ─────────────────────────────────────────────────────────────
    "貳": "弐",
    "惱": "悩",
    "腦": "脳",
    # ─── は行 ─────────────────────────────────────────────────────────────
    "廢": "廃",
    "拜": "拝",
    "賣": "売",
    "發": "発",
    "髮": "髪",
    "蠻": "蛮",
    "晚": "晩",
    "祕": "秘",
    "佛": "仏",
    "邊": "辺",
    "邉": "辺",
    "辯": "弁",
    "瓣": "弁",
    "辨": "弁",
    "變": "変",
    "寶": "宝",
    "豐": "豊",
    # ─── ま行 ─────────────────────────────────────────────────────────────
    "萬": "万",
    "滿": "満",
    "彌": "弥",
    "默": "黙",
    # ─── や行 ─────────────────────────────────────────────────────────────
    "藥": "薬",
    "譯": "訳",
    # ─── ら行 ─────────────────────────────────────────────────────────────
    "與": "与",
    "餘": "余",
    "豫": "予",
    "謠": "謡",
    "來": "来",
    "覽": "覧",
    "亂": "乱",
    "龍": "竜",
    "曆": "暦",
    "靈": "霊",
    "勵": "励",
    "戀": "恋",
    "齡": "齢",
    "勞": "労",
    "樓": "楼",
    "錄": "録",
    # ─── わ行 ─────────────────────────────────────────────────────────────
    "灣": "湾",
    # ─── 人名頻出字形 (姓名でよく出る旧字・異体字) ────────────────────────
    "澤": "沢",
    "橫": "横",
    "瀨": "瀬",
    "濱": "浜",
    "莊": "荘",
    "嶋": "島",
    "嶌": "島",
    "德": "徳",
    "眞": "真",
    "聰": "聡",
    "彥": "彦",
    "栁": "柳",
    "桝": "枡",
    "靑": "青",
    "﨑": "崎",   # compatibility CJK variant U+F9B2
}


def canonical_name_ja(name: Optional[str]) -> Optional[str]:
    """Return NFKC-normalized + 旧字体→新字体変換 + whitespace-collapsed name.

    Args:
        name: raw Japanese name string, may be None/empty.

    Returns:
        Normalized string, or None if input is None/empty.

    Examples:
        >>> canonical_name_ja("Ｈ・Ｐ・ラブクラフト")
        'H・P・ラブクラフト'
        >>> canonical_name_ja("渡邊")
        '渡辺'
        >>> canonical_name_ja("齊藤")
        '斉藤'
        >>> canonical_name_ja(None)
    """
    if not name:
        return None

    # 1. NFKC 正規化 (全角→半角英数、合成文字展開、互換文字統一)
    s = unicodedata.normalize("NFKC", name)

    # 2. 旧字体→新字体 変換
    s = "".join(KYU_SHIN_MAP.get(ch, ch) for ch in s)

    # 3. 余分な空白除去 (連続空白 → 単一空白、前後トリム)
    s = " ".join(s.split())

    return s if s else None


# ---------------------------------------------------------------------------
# Backfill: persons テーブルに canonical_name_ja を一括書込
# ---------------------------------------------------------------------------

_DDL_CANONICAL_COLUMN = (
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS canonical_name_ja VARCHAR"
)

_DDL_CANONICAL_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_persons_canonical_name_ja"
    " ON persons(canonical_name_ja)"
)


def backfill(conn: object) -> int:
    """Write canonical_name_ja for all persons rows where it is NULL or empty.

    Idempotent: rows with an existing non-empty value are skipped.

    Args:
        conn: open DuckDB connection to silver.duckdb.

    Returns:
        Number of rows updated.
    """
    # Ensure column exists
    conn.execute(_DDL_CANONICAL_COLUMN)
    conn.execute(_DDL_CANONICAL_INDEX)

    # Fetch rows needing normalization.
    # Rows with empty name_ja produce NULL canonical_name_ja (no-op) and are
    # excluded to keep backfill idempotent.
    rows: list[tuple[str, str]] = conn.execute(
        "SELECT id, name_ja FROM persons"
        " WHERE name_ja != ''"
        "   AND (canonical_name_ja IS NULL OR canonical_name_ja = '')"
    ).fetchall()

    if not rows:
        return 0

    updates: list[tuple[str, str]] = [
        (canonical_name_ja(row[1]) or "", row[0]) for row in rows
    ]

    conn.executemany(
        "UPDATE persons SET canonical_name_ja = ? WHERE id = ?",
        updates,
    )

    return len(updates)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _cli_backfill() -> None:
    """CLI: python -m src.etl.normalize.canonical_name backfill"""
    import duckdb

    from pathlib import Path

    silver_path = Path(__file__).parents[3] / "result" / "silver.duckdb"
    conn = duckdb.connect(str(silver_path))
    try:
        n = backfill(conn)
        conn.commit()
        print(f"canonical_name_ja backfill complete: {n} rows updated")

        # Verification summary
        result = conn.execute(
            "SELECT"
            "  COUNT(*) FILTER (WHERE canonical_name_ja IS NOT NULL) AS canonical,"
            "  COUNT(*) FILTER (WHERE canonical_name_ja != name_ja) AS changed"
            " FROM persons"
        ).fetchone()
        if result:
            print(f"  canonical non-null: {result[0]:,}")
            print(f"  changed (kyu→shin or NFKC): {result[1]:,}")

        # Sample of changed rows
        samples = conn.execute(
            "SELECT name_ja, canonical_name_ja FROM persons"
            " WHERE canonical_name_ja IS NOT NULL AND canonical_name_ja != name_ja"
            " LIMIT 20"
        ).fetchall()
        if samples:
            print("\nSample transformations:")
            for orig, canon in samples:
                print(f"  {orig!r} -> {canon!r}")
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        _cli_backfill()
    else:
        print("Usage: python -m src.etl.normalize.canonical_name backfill")
        sys.exit(1)
