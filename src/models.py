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
    # 分析対象外だが明示的に区別するロール
    VOICE_ACTOR = "voice_actor"
    THEME_SONG = "theme_song"  # 主題歌・挿入歌
    ADR = "adr"  # 吹き替え関連（ADR Director, ADR Script等）
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
    # MADB固有ロール (日本語)
    "作画": Role.KEY_ANIMATOR,  # MADB固有（AniListは「原画」）
    "文芸": Role.SCREENPLAY,  # 文芸部 → 脚本
    "総監督": Role.DIRECTOR,
    "撮影": Role.PHOTOGRAPHY_DIRECTOR,
    "制作進行": Role.PRODUCER,
    "動画チェック": Role.IN_BETWEEN,
    "原案": Role.ORIGINAL_CREATOR,
    "音楽監督": Role.SOUND_DIRECTOR,
    "メカニックデザイン": Role.MECHANICAL_DESIGNER,
    "メカニカルデザイン": Role.MECHANICAL_DESIGNER,
    "美術": Role.BACKGROUND_ART,
    "色彩設定": Role.COLOR_DESIGNER,
    "色指定": Role.COLOR_DESIGNER,
    "特殊効果": Role.EFFECTS,
    "エフェクト": Role.EFFECTS,
    "3dcg": Role.CGI_DIRECTOR,
    "cg": Role.CGI_DIRECTOR,
    "構成": Role.SERIES_COMPOSITION,
    # 声優・音楽関連（分析対象外だが明示的に区別）
    "voice actor": Role.VOICE_ACTOR,
    "voice acting": Role.VOICE_ACTOR,
    "theme song performance": Role.THEME_SONG,
    "theme song arrangement": Role.THEME_SONG,
    "theme song composition": Role.THEME_SONG,
    "theme song lyrics": Role.THEME_SONG,
    "insert song performance": Role.THEME_SONG,
    "insert song lyrics": Role.THEME_SONG,
    "ending theme": Role.THEME_SONG,
    "opening theme": Role.THEME_SONG,
    # 吹き替え関連
    "adr director": Role.ADR,
    "adr script": Role.ADR,
    "adr director assistant": Role.ADR,
}


def parse_role(raw: str) -> Role:
    """役職文字列を Role enum にマッピングする.

    エピソード特定の役職（括弧付き）を正しく処理：
    - "Animation Director (ep 10)" → "animation director" → Role.ANIMATION_DIRECTOR
    - "Key Animation (eps 21, 25)" → "key animation" → Role.KEY_ANIMATOR
    """
    import re

    # 括弧とその中身を除去（エピソード番号など）
    # 例: "Animation Director (ep 10)" → "Animation Director"
    cleaned = re.sub(r"\s*\([^)]*\)", "", raw)

    # 正規化: 小文字化、前後の空白削除
    normalized = cleaned.strip().lower()

    return ROLE_MAP.get(normalized, Role.OTHER)


class Person(BaseModel):
    """アニメ業界の人物."""

    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: list[str] = Field(default_factory=list)
    mal_id: int | None = None
    anilist_id: int | None = None
    madb_id: str | None = None  # メディア芸術DB URI

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
    madb_id: str | None = None  # メディア芸術DB URI
    score: float | None = None

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
    mean_score: int | None = None  # 単純平均スコア（averageScoreとは別算出）

    # 制作情報
    studios: list[str] = Field(default_factory=list)

    # 追加メタデータ（AniList拡張）
    synonyms: list[str] = Field(default_factory=list)  # 別名・別タイトル
    country_of_origin: str | None = None  # ISO 3166-1 alpha-2 (JP, CN, KR, etc.)
    is_licensed: bool | None = None
    is_adult: bool | None = None  # R18フラグ
    hashtag: str | None = None  # 公式Twitterハッシュタグ
    site_url: str | None = None  # AniList URL
    trailer_url: str | None = None  # トレーラーURL
    trailer_site: str | None = None  # トレーラーサイト名 (youtube, dailymotion)

    # 複合データ（JSON文字列でDB保存）
    relations_json: str | None = None  # 関連作品（続編/前日譚等）
    external_links_json: str | None = None  # 外部リンク（配信サイト等）
    rankings_json: str | None = None  # ランキング情報

    @computed_field  # type: ignore[prop-decorator]
    @property
    def studio(self) -> str | None:
        """主制作スタジオ（studiosリストの先頭、後方互換用）."""
        return self.studios[0] if self.studios else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_title(self) -> str:
        return self.title_ja or self.title_en or self.id


class AnimeRelation(BaseModel):
    """アニメ間の関連（続編・前日譚等）."""

    anime_id: str
    related_anime_id: str  # "anilist:{id}"
    relation_type: str = ""  # SEQUEL, PREQUEL, SIDE_STORY, PARENT, etc.
    related_title: str = ""
    related_format: str | None = None  # TV, MOVIE, OVA, etc.


class Character(BaseModel):
    """アニメキャラクター."""

    id: str  # "anilist:c{anilist_id}"
    name_ja: str = ""
    name_en: str = ""
    aliases: list[str] = Field(default_factory=list)
    anilist_id: int | None = None

    # 画像
    image_large: str | None = None
    image_medium: str | None = None

    # プロフィール
    description: str | None = None
    gender: str | None = None
    date_of_birth: str | None = None  # YYYY-MM-DD
    age: str | None = None  # 文字列 (AniList APIが文字列で返す)
    blood_type: str | None = None
    favourites: int | None = None
    site_url: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_name(self) -> str:
        return self.name_ja or self.name_en or self.id


class Studio(BaseModel):
    """アニメ制作スタジオ."""

    id: str  # "anilist:s{anilist_id}"
    name: str = ""
    anilist_id: int | None = None
    is_animation_studio: bool | None = None
    favourites: int | None = None
    site_url: str | None = None


class AnimeStudio(BaseModel):
    """アニメ×スタジオの関係."""

    anime_id: str
    studio_id: str
    is_main: bool = False


class CharacterVoiceActor(BaseModel):
    """キャラクター×声優×作品の関係."""

    character_id: str
    person_id: str
    anime_id: str
    character_role: str = ""  # MAIN, SUPPORTING, BACKGROUND
    source: str = ""


class Credit(BaseModel):
    """クレジット — 人物×作品×役職の関係."""

    person_id: str
    anime_id: str
    role: Role
    raw_role: str | None = None  # 元のロール文字列（API由来）を保存
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
