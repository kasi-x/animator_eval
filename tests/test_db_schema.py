"""DB スキーマ vs コード整合性テスト.

カラム名変更・マイグレーション追加時にすぐ検出できるようにする。

## テスト内容

1. `test_row_fields_match_db_schema` — PRAGMA table_info と db_rows.py の
   dataclass フィールドを突き合わせ。不一致があれば失敗する。

2. `test_pydantic_models_cover_row_fields` — Pydantic モデルが db_rows.py の
   全フィールドを持つことを確認。DB → dataclass → Pydantic の変換が網羅的。

3. `test_openapi_schema_includes_score_fields` — FastAPI の OpenAPI スキーマが
   scores テーブルのカラムを含む PersonResult モデルを持つことを確認。
   API ドメインとDB ドメインが分離していても、フィールド名の一致を保証する。
"""

from __future__ import annotations

import dataclasses
import sqlite3

import pytest

from src.database import get_connection, init_db
from src.db_rows import TABLE_ROW_MAP, AnimeRow, PersonRow, ScoreRow


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fresh_conn(tmp_path_factory):
    """フルマイグレーション済みの空DBコネクション."""
    db_path = tmp_path_factory.mktemp("schema_test") / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    conn.commit()
    yield conn
    conn.close()


def _db_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}  # col index 1 = name


def _dataclass_fields(cls) -> set[str]:
    return {f.name for f in dataclasses.fields(cls)}


# ---------------------------------------------------------------------------
# Test 1: dataclass フィールド ⊆ DB カラム (未定義カラムへのアクセスを防ぐ)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table,row_cls", list(TABLE_ROW_MAP.items()))
def test_row_fields_subset_of_db_schema(fresh_conn, table, row_cls):
    """dataclass の全フィールドがDBカラムとして存在すること.

    フィールド名がDB上に存在しないと実行時にキーエラーになる。
    マイグレーションを追加したら、ここで検出できる。
    """
    db_cols = _db_columns(fresh_conn, table)
    row_fields = _dataclass_fields(row_cls)
    missing_in_db = row_fields - db_cols
    assert not missing_in_db, (
        f"[{table}] db_rows.{row_cls.__name__} has fields not in DB schema: {missing_in_db}\n"
        f"  → マイグレーションを追加するか、dataclass フィールドを削除してください。"
    )


@pytest.mark.parametrize("table,row_cls", list(TABLE_ROW_MAP.items()))
def test_db_schema_subset_of_row_fields(fresh_conn, table, row_cls):
    """DBカラムが dataclass フィールドとして定義されていること.

    新しいカラムをマイグレーションで追加したら、対応フィールドを
    dataclass に追加することをこのテストが強制する。
    """
    db_cols = _db_columns(fresh_conn, table)
    # 内部管理カラムは除外 (id, updated_at は多くのテーブルで自動管理)
    INTERNAL_COLS = {"updated_at"}
    db_cols -= INTERNAL_COLS

    row_fields = _dataclass_fields(row_cls)
    missing_in_code = db_cols - row_fields
    assert not missing_in_code, (
        f"[{table}] DB has columns not in db_rows.{row_cls.__name__}: {missing_in_code}\n"
        f"  → db_rows.py に新しいフィールドを追加してください。"
    )


# ---------------------------------------------------------------------------
# Test 2: Pydantic モデルが DB dataclass の主要フィールドをカバーする
# ---------------------------------------------------------------------------


def test_person_model_covers_person_row():
    """Person モデルが PersonRow の主要フィールドをカバーすること."""
    from src.models import Person

    person_fields = set(Person.model_fields.keys())
    row_fields = _dataclass_fields(PersonRow)
    # 内部管理フィールドは除外
    INTERNAL = {"updated_at", "canonical_id"}
    row_fields -= INTERNAL

    missing = row_fields - person_fields
    assert not missing, (
        f"Person model missing fields from PersonRow: {missing}\n"
        f"  → models.py の Person クラスにフィールドを追加し、"
        f"from_db_row() にマッピングを追加してください。"
    )


def test_anime_model_covers_anime_row():
    """Anime モデルが AnimeRow の主要フィールドをカバーすること."""
    from src.models import BronzeAnime as Anime

    anime_fields = set(Anime.model_fields.keys())
    row_fields = _dataclass_fields(AnimeRow)
    INTERNAL = {"updated_at"}
    row_fields -= INTERNAL

    missing = row_fields - anime_fields
    assert not missing, (
        f"Anime model missing fields from AnimeRow: {missing}\n"
        f"  → models.py の Anime クラスにフィールドを追加し、"
        f"from_db_row() にマッピングを追加してください。"
    )


def test_score_result_covers_score_row():
    """ScoreResult モデルが ScoreRow の主要フィールドをカバーすること."""
    from src.models import ScoreResult

    score_fields = set(ScoreResult.model_fields.keys())
    row_fields = _dataclass_fields(ScoreRow)
    INTERNAL = {"updated_at"}
    row_fields -= INTERNAL

    missing = row_fields - score_fields
    assert not missing, (
        f"ScoreResult model missing fields from ScoreRow: {missing}\n"
        f"  → models.py の ScoreResult クラスにフィールドを追加し、"
        f"from_db_row() にマッピングを追加してください。"
    )


# ---------------------------------------------------------------------------
# Test 3: OpenAPI スキーマ / API ルートの整合性
# ---------------------------------------------------------------------------


def test_openapi_key_routes_exist():
    """主要APIルートが OpenAPI スキーマに含まれていること."""
    from src.api import app

    schema = app.openapi()
    paths = set(schema.get("paths", {}).keys())

    required_routes = {
        "/api/health",
        "/api/persons",
        "/api/persons/{person_id}",
        "/api/persons/search",
    }
    missing = required_routes - paths
    assert not missing, f"OpenAPI から以下のルートが消えています: {missing}"


def test_api_person_response_contains_score_fields(tmp_path, monkeypatch):
    """GET /api/persons/{id} のレスポンスが scores テーブルの主要フィールドを含むこと.

    scores テーブルのカラム名と API レスポンスのキー名が一致することを確認する。
    カラム名リファクタリング時に API 側の更新漏れを検出する。
    """
    import src.database as db_mod
    from src.database import get_connection, init_db
    from fastapi.testclient import TestClient

    db_path = tmp_path / "test_api.db"
    monkeypatch.setattr(db_mod, "DEFAULT_DB_PATH", db_path)

    conn = get_connection(db_path)
    init_db(conn)
    conn.execute("INSERT INTO persons(id, name_ja) VALUES ('p1', 'テスト太郎')")
    conn.execute(
        "INSERT INTO person_scores(person_id, iv_score, birank, person_fe, "
        "studio_fe_exposure, patronage, dormancy, awcc) "
        "VALUES ('p1', 0.9, 0.8, 0.7, 0.6, 0.5, 0.95, 0.4)"
    )
    conn.commit()
    conn.close()

    from src.api import app

    client = TestClient(app)
    resp = client.get("/api/persons/p1")
    assert resp.status_code == 200, resp.text

    body = resp.json()
    # scores テーブルの全カラム名が API レスポンスに存在すること
    score_cols = {
        "person_fe",
        "studio_fe_exposure",
        "birank",
        "patronage",
        "dormancy",
        "awcc",
        "iv_score",
    }
    missing = score_cols - set(body.keys())
    assert not missing, (
        f"GET /api/persons/{{id}} レスポンスに scores カラムが存在しない: {missing}\n"
        f"  → api.py の _row_to_person() でフィールドマッピングを確認してください。"
    )
