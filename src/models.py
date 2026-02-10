"""データモデル定義 (Pydantic v2)."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, computed_field


class Role(str, Enum):
    """アニメ制作における役職."""

    DIRECTOR = "director"
    CHIEF_ANIMATION_DIRECTOR = "chief_animation_director"
    ANIMATION_DIRECTOR = "animation_director"
    KEY_ANIMATOR = "key_animator"
    SECOND_KEY_ANIMATOR = "second_key_animator"
    IN_BETWEEN = "in_between"
    STORYBOARD = "storyboard"
    EPISODE_DIRECTOR = "episode_director"
    CHARACTER_DESIGNER = "character_designer"
    MECHANICAL_DESIGNER = "mechanical_designer"
    ART_DIRECTOR = "art_director"
    COLOR_DESIGNER = "color_designer"
    PHOTOGRAPHY_DIRECTOR = "photography_director"
    EFFECTS = "effects"
    PRODUCER = "producer"
    SOUND_DIRECTOR = "sound_director"
    MUSIC = "music"
    SERIES_COMPOSITION = "series_composition"
    SCREENPLAY = "screenplay"
    ORIGINAL_CREATOR = "original_creator"
    BACKGROUND_ART = "background_art"
    CGI_DIRECTOR = "cgi_director"
    LAYOUT = "layout"
    OTHER = "other"


# MAL/AniList の役職文字列 → Role へのマッピング
ROLE_MAP: dict[str, Role] = {
    "director": Role.DIRECTOR,
    "chief animation director": Role.CHIEF_ANIMATION_DIRECTOR,
    "animation director": Role.ANIMATION_DIRECTOR,
    "key animation": Role.KEY_ANIMATOR,
    "key animator": Role.KEY_ANIMATOR,
    "2nd key animation": Role.SECOND_KEY_ANIMATOR,
    "second key animation": Role.SECOND_KEY_ANIMATOR,
    "in-between animation": Role.IN_BETWEEN,
    "in-betweens": Role.IN_BETWEEN,
    "storyboard": Role.STORYBOARD,
    "episode director": Role.EPISODE_DIRECTOR,
    "character design": Role.CHARACTER_DESIGNER,
    "character designer": Role.CHARACTER_DESIGNER,
    "mechanical design": Role.MECHANICAL_DESIGNER,
    "art director": Role.ART_DIRECTOR,
    "art direction": Role.ART_DIRECTOR,
    "color design": Role.COLOR_DESIGNER,
    "director of photography": Role.PHOTOGRAPHY_DIRECTOR,
    "special effects": Role.EFFECTS,
    # 日本語
    "監督": Role.DIRECTOR,
    "総作画監督": Role.CHIEF_ANIMATION_DIRECTOR,
    "作画監督": Role.ANIMATION_DIRECTOR,
    "原画": Role.KEY_ANIMATOR,
    "第二原画": Role.SECOND_KEY_ANIMATOR,
    "動画": Role.IN_BETWEEN,
    "絵コンテ": Role.STORYBOARD,
    "演出": Role.EPISODE_DIRECTOR,
    "キャラクターデザイン": Role.CHARACTER_DESIGNER,
    "美術監督": Role.ART_DIRECTOR,
    "色彩設計": Role.COLOR_DESIGNER,
    "撮影監督": Role.PHOTOGRAPHY_DIRECTOR,
    # 追加役職 (EN)
    "producer": Role.PRODUCER,
    "assistant producer": Role.PRODUCER,
    "sound director": Role.SOUND_DIRECTOR,
    "music": Role.MUSIC,
    "music production": Role.MUSIC,
    "series composition": Role.SERIES_COMPOSITION,
    "screenplay": Role.SCREENPLAY,
    "script": Role.SCREENPLAY,
    "original creator": Role.ORIGINAL_CREATOR,
    "original character design": Role.CHARACTER_DESIGNER,
    "original story": Role.ORIGINAL_CREATOR,
    "background art": Role.BACKGROUND_ART,
    "cgi director": Role.CGI_DIRECTOR,
    "3d director": Role.CGI_DIRECTOR,
    "layout": Role.LAYOUT,
    "layout animation": Role.LAYOUT,
    "assistant animation director": Role.ANIMATION_DIRECTOR,
    "sub character design": Role.CHARACTER_DESIGNER,
    "prop design": Role.MECHANICAL_DESIGNER,
    "action animation director": Role.ANIMATION_DIRECTOR,
    "chief director": Role.DIRECTOR,
    "assistant director": Role.EPISODE_DIRECTOR,
    "unit director": Role.EPISODE_DIRECTOR,
    # 追加役職 (JA)
    "プロデューサー": Role.PRODUCER,
    "音響監督": Role.SOUND_DIRECTOR,
    "音楽": Role.MUSIC,
    "シリーズ構成": Role.SERIES_COMPOSITION,
    "脚本": Role.SCREENPLAY,
    "原作": Role.ORIGINAL_CREATOR,
    "背景美術": Role.BACKGROUND_ART,
    "CGI監督": Role.CGI_DIRECTOR,
    "レイアウト": Role.LAYOUT,
    "副監督": Role.EPISODE_DIRECTOR,
}


def parse_role(raw: str) -> Role:
    """役職文字列を Role enum にマッピングする."""
    normalized = raw.strip().lower()
    return ROLE_MAP.get(normalized, Role.OTHER)


class Person(BaseModel):
    """アニメ業界の人物."""

    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: list[str] = Field(default_factory=list)
    mal_id: int | None = None
    anilist_id: int | None = None

    # 画像（AniList）
    image_large: str | None = None
    image_medium: str | None = None
    image_large_path: str | None = None  # ローカル保存パス
    image_medium_path: str | None = None

    # プロフィール情報
    date_of_birth: str | None = None  # YYYY-MM-DD形式
    age: int | None = None
    gender: str | None = None
    years_active: list[int] = Field(default_factory=list)
    hometown: str | None = None
    blood_type: str | None = None
    description: str | None = None  # 経歴・説明

    # 人気度指標
    favourites: int | None = None  # お気に入り数

    # 外部リンク
    site_url: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_name(self) -> str:
        return self.name_ja or self.name_en or self.id


class Anime(BaseModel):
    """アニメ作品."""

    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    episodes: int | None = None
    mal_id: int | None = None
    anilist_id: int | None = None
    score: float | None = None
    studio: str | None = None

    # 画像（AniList）
    cover_large: str | None = None
    cover_extra_large: str | None = None
    cover_medium: str | None = None
    banner: str | None = None
    cover_large_path: str | None = None  # ローカル保存パス
    banner_path: str | None = None

    # 詳細情報
    description: str | None = None  # あらすじ
    format: str | None = None  # TV, MOVIE, OVA, ONA, SPECIAL, MUSIC
    status: str | None = None  # FINISHED, RELEASING, NOT_YET_RELEASED, CANCELLED
    start_date: str | None = None  # YYYY-MM-DD
    end_date: str | None = None
    duration: int | None = None  # 分/話
    source: str | None = None  # ORIGINAL, MANGA, LIGHT_NOVEL, etc.

    # 分類・タグ
    genres: list[str] = Field(default_factory=list)
    tags: list[dict] = Field(default_factory=list)  # [{"name": str, "rank": int}]

    # 人気度指標
    popularity_rank: int | None = None
    favourites: int | None = None

    # 制作情報
    studios: list[str] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_title(self) -> str:
        return self.title_ja or self.title_en or self.id


class Credit(BaseModel):
    """クレジット — 人物×作品×役職の関係."""

    person_id: str
    anime_id: str
    role: Role
    episode: int | None = None
    source: str = ""


class ScoreResult(BaseModel):
    """3軸評価結果."""

    person_id: str
    authority: float = 0.0
    trust: float = 0.0
    skill: float = 0.0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def composite(self) -> float:
        """統合スコア（重み付き平均）."""
        from src.utils.config import COMPOSITE_WEIGHTS

        return (
            self.authority * COMPOSITE_WEIGHTS["authority"]
            + self.trust * COMPOSITE_WEIGHTS["trust"]
            + self.skill * COMPOSITE_WEIGHTS["skill"]
        )
