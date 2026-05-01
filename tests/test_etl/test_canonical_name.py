"""Tests for src/etl/normalize/canonical_name.py (Card 21/03).

Covers:
- NFKC normalization (full-width → half-width)
- 旧字体 → 新字体 conversion
- Whitespace collapsing
- Idempotency
- None / empty edge cases
- backfill() on an in-memory DuckDB fixture
"""
from __future__ import annotations

import duckdb
import pytest

from src.etl.normalize.canonical_name import (
    KYU_SHIN_MAP,
    backfill,
    canonical_name_ja,
)


# ---------------------------------------------------------------------------
# Unit tests: canonical_name_ja()
# ---------------------------------------------------------------------------


class TestNfkc:
    """NFKC 正規化: 全角→半角、互換文字統一."""

    def test_fullwidth_ascii_letters(self) -> None:
        assert canonical_name_ja("Ｈ・Ｐ・ラブクラフト") == "H・P・ラブクラフト"

    def test_fullwidth_digits(self) -> None:
        assert canonical_name_ja("１２３") == "123"

    def test_halfwidth_katakana(self) -> None:
        # ｱ (halfwidth) → ア (fullwidth katakana) via NFKC
        assert canonical_name_ja("ｱﾆﾒ") == "アニメ"

    def test_normal_hiragana_unchanged(self) -> None:
        assert canonical_name_ja("ほりえ") == "ほりえ"

    def test_normal_katakana_unchanged(self) -> None:
        assert canonical_name_ja("ホリエ") == "ホリエ"


class TestKyuShin:
    """旧字体 → 新字体 変換."""

    def test_watanabe_variant_nabe(self) -> None:
        # 邊 → 辺
        assert canonical_name_ja("渡邊") == "渡辺"

    def test_watanabe_variant_nabe2(self) -> None:
        # 邉 → 辺 (another variant)
        assert canonical_name_ja("渡邉") == "渡辺"

    def test_saito_variant(self) -> None:
        # 齊 → 斉
        assert canonical_name_ja("齊藤") == "斉藤"

    def test_saito_variant2(self) -> None:
        # 齋 → 斎
        assert canonical_name_ja("齋藤") == "斎藤"

    def test_ao_old(self) -> None:
        # 靑 → 青
        assert canonical_name_ja("靑木") == "青木"

    def test_koku_old(self) -> None:
        # 國 → 国
        assert canonical_name_ja("中國") == "中国"

    def test_sawa_old(self) -> None:
        # 澤 → 沢
        assert canonical_name_ja("澤田") == "沢田"

    def test_hama_old(self) -> None:
        # 濱 → 浜
        assert canonical_name_ja("濱田") == "浜田"

    def test_ko_old(self) -> None:
        # 廣 → 広
        assert canonical_name_ja("廣田") == "広田"

    def test_toku_old(self) -> None:
        # 德 → 徳
        assert canonical_name_ja("德川") == "徳川"

    def test_so_old(self) -> None:
        # 聰 → 聡
        assert canonical_name_ja("聰子") == "聡子"

    def test_ju_old(self) -> None:
        # 壽 → 寿
        assert canonical_name_ja("壽太郎") == "寿太郎"

    def test_hiko_old(self) -> None:
        # 彥 → 彦
        assert canonical_name_ja("彥一") == "彦一"

    def test_compat_cjk_zaki(self) -> None:
        # 﨑 (U+F9B2, compatibility CJK) → 崎
        assert canonical_name_ja("山﨑") == "山崎"

    def test_shima_old(self) -> None:
        # 嶋 → 島
        assert canonical_name_ja("嶋田") == "島田"

    def test_yon_old(self) -> None:
        # 豐 → 豊
        assert canonical_name_ja("豐田") == "豊田"

    def test_ma_old(self) -> None:
        # 眞 → 真
        assert canonical_name_ja("眞子") == "真子"

    def test_ryo_old(self) -> None:
        # 龍 → 竜
        assert canonical_name_ja("龍介") == "竜介"


class TestWhitespace:
    """余分な空白の正規化."""

    def test_leading_trailing_stripped(self) -> None:
        assert canonical_name_ja("  田中  ") == "田中"

    def test_inner_whitespace_collapsed(self) -> None:
        assert canonical_name_ja("田中  一郎") == "田中 一郎"

    def test_newline_collapsed(self) -> None:
        assert canonical_name_ja("田中\n一郎") == "田中 一郎"


class TestEdgeCases:
    """None / empty / already-normalized edge cases."""

    def test_none_returns_none(self) -> None:
        assert canonical_name_ja(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert canonical_name_ja("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert canonical_name_ja("   ") is None

    def test_already_normalized_unchanged(self) -> None:
        assert canonical_name_ja("田中一郎") == "田中一郎"

    def test_latin_only(self) -> None:
        assert canonical_name_ja("John Smith") == "John Smith"


class TestIdempotency:
    """canonical_name_ja は冪等であること."""

    def test_idempotent_kyu_shin(self) -> None:
        s = canonical_name_ja("渡邊")
        assert canonical_name_ja(s) == s

    def test_idempotent_nfkc(self) -> None:
        s = canonical_name_ja("Ｈ・Ｐ・ラブクラフト")
        assert canonical_name_ja(s) == s

    def test_idempotent_normal_name(self) -> None:
        s = canonical_name_ja("宮崎 駿")
        assert canonical_name_ja(s) == s


class TestKyuShinMap:
    """KYU_SHIN_MAP の基本整合性チェック."""

    def test_no_noop_entries(self) -> None:
        """マップに key == value のエントリがないこと (no-op は除去済み)."""
        noop = {k: v for k, v in KYU_SHIN_MAP.items() if k == v}
        assert noop == {}, f"no-op entries found: {list(noop)[:5]}"

    def test_known_entries_present(self) -> None:
        assert KYU_SHIN_MAP.get("邊") == "辺"
        assert KYU_SHIN_MAP.get("邉") == "辺"
        assert KYU_SHIN_MAP.get("齊") == "斉"
        assert KYU_SHIN_MAP.get("齋") == "斎"
        assert KYU_SHIN_MAP.get("澤") == "沢"


# ---------------------------------------------------------------------------
# Integration test: backfill() on in-memory DuckDB
# ---------------------------------------------------------------------------


@pytest.fixture()
def silver_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with minimal persons table."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE persons (
            id      VARCHAR PRIMARY KEY,
            name_ja VARCHAR NOT NULL DEFAULT ''
        )
        """
    )
    # Insert rows with various normalization scenarios
    conn.executemany(
        "INSERT INTO persons (id, name_ja) VALUES (?, ?)",
        [
            ("p1", "渡邊"),        # 旧→新
            ("p2", "齊藤"),        # 旧→新
            ("p3", "Ｈ・Ｐ"),      # NFKC
            ("p4", "田中一郎"),    # already normalized
            ("p5", ""),            # empty → NULL
            ("p6", "濱田"),        # 旧→新
        ],
    )
    yield conn
    conn.close()


class TestBackfill:
    """backfill() integration tests."""

    def test_returns_row_count(self, silver_conn: duckdb.DuckDBPyConnection) -> None:
        n = backfill(silver_conn)
        # p5 has empty name_ja → skipped to keep backfill idempotent
        assert n == 5

    def test_column_created(self, silver_conn: duckdb.DuckDBPyConnection) -> None:
        backfill(silver_conn)
        cols = [
            row[0]
            for row in silver_conn.execute("DESCRIBE persons").fetchall()
        ]
        assert "canonical_name_ja" in cols

    def test_kyu_shin_converted(self, silver_conn: duckdb.DuckDBPyConnection) -> None:
        backfill(silver_conn)
        val = silver_conn.execute(
            "SELECT canonical_name_ja FROM persons WHERE id = 'p1'"
        ).fetchone()[0]
        assert val == "渡辺"

    def test_nfkc_converted(self, silver_conn: duckdb.DuckDBPyConnection) -> None:
        backfill(silver_conn)
        val = silver_conn.execute(
            "SELECT canonical_name_ja FROM persons WHERE id = 'p3'"
        ).fetchone()[0]
        assert val == "H・P"

    def test_already_normalized_row(
        self, silver_conn: duckdb.DuckDBPyConnection
    ) -> None:
        backfill(silver_conn)
        val = silver_conn.execute(
            "SELECT canonical_name_ja FROM persons WHERE id = 'p4'"
        ).fetchone()[0]
        assert val == "田中一郎"

    def test_empty_name_gives_empty_canonical(
        self, silver_conn: duckdb.DuckDBPyConnection
    ) -> None:
        backfill(silver_conn)
        val = silver_conn.execute(
            "SELECT canonical_name_ja FROM persons WHERE id = 'p5'"
        ).fetchone()[0]
        # empty string → canonical_name_ja('') is None → stored as ''
        assert val == "" or val is None

    def test_idempotent_on_rerun(
        self, silver_conn: duckdb.DuckDBPyConnection
    ) -> None:
        n1 = backfill(silver_conn)
        n2 = backfill(silver_conn)
        assert n1 == 5  # p5 (empty name_ja) is skipped
        assert n2 == 0  # all eligible rows already have canonical_name_ja

    def test_hama_old_converted(
        self, silver_conn: duckdb.DuckDBPyConnection
    ) -> None:
        backfill(silver_conn)
        val = silver_conn.execute(
            "SELECT canonical_name_ja FROM persons WHERE id = 'p6'"
        ).fetchone()[0]
        assert val == "浜田"
