"""DB テーブルの行を表す typed dataclasses.

各 dataclass はテーブルの全カラムをフィールドとして持つ。
SQLite から取得した raw 値をそのまま保持し、JSON 文字列のパースや
型変換は行わない（それは Pydantic モデルの責務）。

## 設計意図

`sqlite3.Row` の `row["col_name"]` 文字列アクセスをなくし、
`.col_name` 属性アクセスに変換することで:
- IDE の補完が効く
- カラム名リネーム時に型チェッカーが検出できる
- `tests/test_db_schema.py` が PRAGMA と突き合わせてドリフトを検出する

## カラム変更時の手順

1. マイグレーションを追加 (`database.py`)
2. 対応する dataclass フィールドを更新 (`db_rows.py`)
3. Pydantic モデルの `from_db_row()` を更新 (`models.py`)
4. テスト実行 — `test_db_schema.py` が不整合を検出する
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _from_sqlite_row(cls, row) -> object:
    """sqlite3.Row または dict から dataclass インスタンスを生成する.

    テーブルに存在しないフィールドは dataclass のデフォルト値を使用する。
    dataclass に存在しないカラムは無視する（前方互換）。
    """
    field_names = {f.name for f in dataclasses.fields(cls)}
    if hasattr(row, "keys"):
        kwargs = {k: row[k] for k in row.keys() if k in field_names}
    else:
        kwargs = {k: v for k, v in zip(row.keys(), tuple(row)) if k in field_names}
    return cls(**kwargs)


# ---------------------------------------------------------------------------
# persons テーブル
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class PersonRow:
    """persons テーブルの 1 行."""

    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: str = "[]"        # JSON 文字列
    mal_id: int | None = None
    anilist_id: int | None = None
    canonical_id: str | None = None
    updated_at: str | None = None
    image_large: str | None = None
    image_medium: str | None = None
    image_large_path: str | None = None
    image_medium_path: str | None = None
    date_of_birth: str | None = None
    age: int | None = None
    gender: str | None = None
    years_active: str = "[]"   # JSON 文字列
    hometown: str | None = None
    blood_type: str | None = None
    description: str | None = None
    favourites: int | None = None
    site_url: str | None = None
    madb_id: str | None = None

    @classmethod
    def from_row(cls, row) -> "PersonRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# anime テーブル
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class AnimeRow:
    """anime テーブルの 1 行."""

    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    episodes: int | None = None
    mal_id: int | None = None
    anilist_id: int | None = None
    score: float | None = None
    updated_at: str | None = None
    cover_large: str | None = None
    cover_extra_large: str | None = None
    cover_medium: str | None = None
    banner: str | None = None
    cover_large_path: str | None = None
    banner_path: str | None = None
    description: str | None = None
    format: str | None = None
    status: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: int | None = None
    source: str | None = None
    genres: str = "[]"         # JSON 文字列
    tags: str = "[]"           # JSON 文字列
    popularity_rank: int | None = None
    favourites: int | None = None
    studios: str = "[]"        # JSON 文字列
    synonyms: str = "[]"       # JSON 文字列
    mean_score: int | None = None
    country_of_origin: str | None = None
    is_licensed: int | None = None   # SQLite BOOLEAN → 0/1
    is_adult: int | None = None
    hashtag: str | None = None
    site_url: str | None = None
    trailer_url: str | None = None
    trailer_site: str | None = None
    relations_json: str | None = None
    external_links_json: str | None = None
    rankings_json: str | None = None
    madb_id: str | None = None
    quarter: int | None = None
    work_type: str | None = None
    scale_class: str | None = None

    @classmethod
    def from_row(cls, row) -> "AnimeRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# credits テーブル
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class CreditRow:
    """credits テーブルの 1 行."""

    id: int
    person_id: str
    anime_id: str
    role: str
    raw_role: str = ""
    episode: int = -1
    source: str = ""
    updated_at: str | None = None
    credit_year: int | None = None
    credit_quarter: int | None = None

    @classmethod
    def from_row(cls, row) -> "CreditRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# scores テーブル
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ScoreRow:
    """scores テーブルの 1 行."""

    person_id: str
    person_fe: float = 0.0
    studio_fe_exposure: float = 0.0
    birank: float = 0.0
    patronage: float = 0.0
    dormancy: float = 1.0
    awcc: float = 0.0
    iv_score: float = 0.0
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "ScoreRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# score_history テーブル
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ScoreHistoryRow:
    """score_history テーブルの 1 行."""

    id: int
    person_id: str
    person_fe: float = 0.0
    studio_fe_exposure: float = 0.0
    birank: float = 0.0
    patronage: float = 0.0
    dormancy: float = 1.0
    awcc: float = 0.0
    iv_score: float = 0.0
    run_at: str | None = None
    year: int | None = None
    quarter: int | None = None

    @classmethod
    def from_row(cls, row) -> "ScoreHistoryRow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# character_voice_actors テーブル
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class VARow:
    """character_voice_actors テーブルの 1 行."""

    id: int
    character_id: str
    person_id: str
    anime_id: str
    character_role: str = ""
    source: str = ""
    updated_at: str | None = None

    @classmethod
    def from_row(cls, row) -> "VARow":
        return _from_sqlite_row(cls, row)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# テーブル名 → Row クラスの対応表 (スキーマテスト用)
# ---------------------------------------------------------------------------

TABLE_ROW_MAP: dict[str, type] = {
    "persons": PersonRow,
    "anime": AnimeRow,
    "credits": CreditRow,
    "scores": ScoreRow,
    "score_history": ScoreHistoryRow,
    "character_voice_actors": VARow,
}
