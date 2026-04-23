"""データモデル定義 (Pydantic v2)."""

from __future__ import annotations

import json
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, computed_field

if TYPE_CHECKING:
    from src.db_rows import AnimeRow, CreditRow, PersonRow, ScoreRow


class Role(str, Enum):
    """アニメ制作における役職 (23分類).

    統合済み (v27マイグレーション後):
      CHIEF_ANIMATION_DIRECTOR → ANIMATION_DIRECTOR
      STORYBOARD → EPISODE_DIRECTOR
      MECHANICAL_DESIGNER → CHARACTER_DESIGNER
      ART_DIRECTOR → BACKGROUND_ART
      COLOR_DESIGNER → FINISHING
      EFFECTS → PHOTOGRAPHY_DIRECTOR
      THEME_SONG → MUSIC
      SERIES_COMPOSITION → SCREENPLAY
      ADR → VOICE_ACTOR

    OTHER と SPECIAL は別概念:
      OTHER    = その他 — ロール特定不可・分類不能なクレジット
      SPECIAL  = スペシャル — スペシャルサンクス・ゲスト参加・制作外特別枠
    """

    DIRECTOR = "director"
    ANIMATION_DIRECTOR = "animation_director"  # +総作画監督
    KEY_ANIMATOR = "key_animator"
    SECOND_KEY_ANIMATOR = "second_key_animator"  # 第二原画
    IN_BETWEEN = "in_between"
    EPISODE_DIRECTOR = "episode_director"  # +絵コンテ, シリーズ構成 は SCREENPLAY へ
    CHARACTER_DESIGNER = "character_designer"  # +メカデザイン
    PHOTOGRAPHY_DIRECTOR = "photography_director"  # +エフェクト(撮影=コンポジット+特効)
    PRODUCER = "producer"
    PRODUCTION_MANAGER = "production_manager"  # 制作進行・制作デスク・各種進行
    SOUND_DIRECTOR = "sound_director"
    MUSIC = "music"  # +主題歌・挿入歌・演奏
    SCREENPLAY = "screenplay"  # +シリーズ構成
    ORIGINAL_CREATOR = "original_creator"
    BACKGROUND_ART = "background_art"  # +美術監督
    CGI_DIRECTOR = "cgi_director"
    LAYOUT = "layout"
    FINISHING = "finishing"  # +色彩設計・色指定・仕上げ・検査
    EDITING = "editing"  # 編集・ポスプロ
    SETTINGS = "settings"  # 設定系
    VOICE_ACTOR = "voice_actor"  # +ADR
    LOCALIZATION = "localization"  # 各国語版スタッフ（翻訳・吹替演出・各国版P等）
    OTHER = "other"  # その他 — ロール特定不可・分類不能なクレジット
    SPECIAL = "special"  # スペシャルサンクス・ゲスト参加・制作外特別枠


# MAL/AniList の役職文字列 → Role へのマッピング
ROLE_MAP: dict[str, Role] = {
    "director": Role.DIRECTOR,
    "chief animation director": Role.ANIMATION_DIRECTOR,
    "animation director": Role.ANIMATION_DIRECTOR,
    "key animation": Role.KEY_ANIMATOR,
    "key animator": Role.KEY_ANIMATOR,
    "2nd key animation": Role.SECOND_KEY_ANIMATOR,
    "second key animation": Role.SECOND_KEY_ANIMATOR,
    "in-between animation": Role.IN_BETWEEN,
    "in-betweens": Role.IN_BETWEEN,
    "storyboard": Role.EPISODE_DIRECTOR,
    "episode director": Role.EPISODE_DIRECTOR,
    "character design": Role.CHARACTER_DESIGNER,
    "character designer": Role.CHARACTER_DESIGNER,
    "mechanical design": Role.CHARACTER_DESIGNER,
    "art director": Role.BACKGROUND_ART,
    "art direction": Role.BACKGROUND_ART,
    "color design": Role.FINISHING,
    "director of photography": Role.PHOTOGRAPHY_DIRECTOR,
    "special effects": Role.PHOTOGRAPHY_DIRECTOR,
    # 日本語
    "監督": Role.DIRECTOR,
    "総作画監督": Role.ANIMATION_DIRECTOR,
    "作画監督": Role.ANIMATION_DIRECTOR,
    "原画": Role.KEY_ANIMATOR,
    "第二原画": Role.SECOND_KEY_ANIMATOR,
    "動画": Role.IN_BETWEEN,
    "絵コンテ": Role.EPISODE_DIRECTOR,
    "演出": Role.EPISODE_DIRECTOR,
    "キャラクターデザイン": Role.CHARACTER_DESIGNER,
    "美術監督": Role.BACKGROUND_ART,
    "色彩設計": Role.FINISHING,
    "撮影監督": Role.PHOTOGRAPHY_DIRECTOR,
    # 追加役職 (EN)
    "producer": Role.PRODUCER,
    "assistant producer": Role.PRODUCER,
    "sound director": Role.SOUND_DIRECTOR,
    "music": Role.MUSIC,
    "music production": Role.MUSIC,
    "series composition": Role.SCREENPLAY,
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
    "prop design": Role.CHARACTER_DESIGNER,
    "action animation director": Role.ANIMATION_DIRECTOR,
    "chief director": Role.DIRECTOR,
    "assistant director": Role.EPISODE_DIRECTOR,
    "unit director": Role.EPISODE_DIRECTOR,
    # 追加役職 (JA)
    "プロデューサー": Role.PRODUCER,
    "音響監督": Role.SOUND_DIRECTOR,
    "音楽": Role.MUSIC,
    "シリーズ構成": Role.SCREENPLAY,
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
    "制作進行": Role.PRODUCTION_MANAGER,
    "動画チェック": Role.IN_BETWEEN,
    "原案": Role.ORIGINAL_CREATOR,
    "音楽監督": Role.SOUND_DIRECTOR,
    "メカニックデザイン": Role.CHARACTER_DESIGNER,
    "メカニカルデザイン": Role.CHARACTER_DESIGNER,
    "美術": Role.BACKGROUND_ART,
    "色彩設定": Role.FINISHING,
    "色指定": Role.FINISHING,
    "特殊効果": Role.PHOTOGRAPHY_DIRECTOR,
    "エフェクト": Role.PHOTOGRAPHY_DIRECTOR,
    "3dcg": Role.CGI_DIRECTOR,
    "cg": Role.CGI_DIRECTOR,
    "構成": Role.SCREENPLAY,
    # SeesaaWiki固有ロール — 動画系
    "動仕": Role.IN_BETWEEN,  # 動画仕上げ
    "動画チェッカー": Role.IN_BETWEEN,
    # SeesaaWiki — 作画系
    "作画監督補佐": Role.ANIMATION_DIRECTOR,
    "アクション作画監督": Role.ANIMATION_DIRECTOR,
    "アニメーター": Role.KEY_ANIMATOR,
    "原動画": Role.KEY_ANIMATOR,
    # SeesaaWiki — 仕上・検査系
    "仕上": Role.FINISHING,
    "仕上げ": Role.FINISHING,
    "仕上検査": Role.FINISHING,
    "仕上げ検査": Role.FINISHING,
    "検査": Role.FINISHING,
    "彩色": Role.FINISHING,
    "デジタルペイント": Role.FINISHING,
    "セル検査": Role.FINISHING,
    "色指定検査": Role.FINISHING,
    "モデルチェック": Role.FINISHING,
    "ファイナルチェック": Role.FINISHING,
    # SeesaaWiki — 演出・コンテ系
    "コンテ": Role.EPISODE_DIRECTOR,
    "演出助手": Role.EPISODE_DIRECTOR,
    "アシスタントディレクター": Role.EPISODE_DIRECTOR,
    "シリーズディレクター": Role.DIRECTOR,
    # SeesaaWiki — 撮影系
    "デジタル撮影": Role.PHOTOGRAPHY_DIRECTOR,
    "線撮影": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影監督補佐": Role.PHOTOGRAPHY_DIRECTOR,
    # SeesaaWiki — 美術系
    "美術補": Role.BACKGROUND_ART,
    "美術設定": Role.BACKGROUND_ART,
    "背景": Role.BACKGROUND_ART,
    "色彩設計補佐": Role.FINISHING,
    # SeesaaWiki — デザイン系
    "サブキャラクターデザイン": Role.CHARACTER_DESIGNER,
    "ゲストキャラクターデザイン": Role.CHARACTER_DESIGNER,
    "ゲストキャラデザイン": Role.CHARACTER_DESIGNER,
    "デザインワークス": Role.CHARACTER_DESIGNER,
    "メカデザイン": Role.CHARACTER_DESIGNER,
    # SeesaaWiki — 制作管理系
    "制作": Role.PRODUCER,
    "制作協力": Role.PRODUCER,
    "制作担当": Role.PRODUCTION_MANAGER,
    "制作デスク": Role.PRODUCTION_MANAGER,
    "制作進行補佐": Role.PRODUCTION_MANAGER,
    "制作マネージャー": Role.PRODUCTION_MANAGER,
    "設定制作": Role.PRODUCTION_MANAGER,
    "アニメーション制作担当": Role.PRODUCTION_MANAGER,
    "アニメーション制作協力": Role.PRODUCER,
    "アニメーション制作": Role.PRODUCER,
    "アニメーションプロデューサー": Role.PRODUCER,
    "アシスタントプロデューサー": Role.PRODUCER,
    "アソシエイトプロデューサー": Role.PRODUCER,
    "製作進行": Role.PRODUCTION_MANAGER,
    "背景進行": Role.PRODUCTION_MANAGER,
    "製作": Role.PRODUCER,
    # SeesaaWiki — 音響系
    "音響効果": Role.SOUND_DIRECTOR,
    "音響制作": Role.SOUND_DIRECTOR,
    "音響制作担当": Role.SOUND_DIRECTOR,
    "効果": Role.SOUND_DIRECTOR,
    "録音": Role.SOUND_DIRECTOR,
    "音楽プロデューサー": Role.MUSIC,
    "音楽制作": Role.MUSIC,
    "音楽協力": Role.MUSIC,
    # SeesaaWiki — 編集系
    "ビデオ編集": Role.EDITING,
    "編集": Role.EDITING,
    # SeesaaWiki — CG
    "cgワークス": Role.CGI_DIRECTOR,
    "cgエフェクト": Role.PHOTOGRAPHY_DIRECTOR,
    # SeesaaWiki — 音楽・主題歌
    "作曲": Role.MUSIC,
    "作詞": Role.MUSIC,
    "編曲": Role.MUSIC,
    "主題歌": Role.MUSIC,
    "うた": Role.MUSIC,
    # SeesaaWiki — 監督補助・管理拡張
    "メカ作画監督": Role.ANIMATION_DIRECTOR,
    "作画監督協力": Role.ANIMATION_DIRECTOR,
    "美術監督補": Role.BACKGROUND_ART,
    "美術進行": Role.PRODUCTION_MANAGER,
    "仕上進行": Role.PRODUCTION_MANAGER,
    "企画": Role.PRODUCER,
    "協力": Role.SPECIAL,
    "製作担当": Role.PRODUCER,
    "設定補佐": Role.SETTINGS,
    "宣伝協力": Role.SPECIAL,
    "録音助手": Role.SOUND_DIRECTOR,
    "録音スタジオ": Role.SPECIAL,
    "現像": Role.EDITING,
    "デジタル彩色": Role.FINISHING,
    "レタッチ": Role.EDITING,
    "背景スキャン": Role.EDITING,
    "ゼログラフ": Role.EDITING,
    "ネガ編集": Role.EDITING,
    "記録": Role.SPECIAL,
    "アニメーション": Role.SPECIAL,
    "調整": Role.EDITING,
    "脚本協力": Role.SCREENPLAY,
    "担当制作": Role.PRODUCTION_MANAGER,
    "シナリオ": Role.SCREENPLAY,
    "3dモデリング": Role.CGI_DIRECTOR,
    "忍術創案": Role.SPECIAL,
    "歌": Role.MUSIC,
    "宣伝": Role.SPECIAL,
    "設定進行": Role.PRODUCTION_MANAGER,
    "助監督": Role.DIRECTOR,
    "選曲": Role.MUSIC,
    "プロデュース": Role.PRODUCER,
    "美術補佐": Role.BACKGROUND_ART,
    "美術ボード": Role.BACKGROUND_ART,
    "メカデザイン協力": Role.CHARACTER_DESIGNER,
    "番組宣伝": Role.SPECIAL,
    "文芸担当": Role.SCREENPLAY,
    "モデル制作": Role.CGI_DIRECTOR,
    "モデリング": Role.CGI_DIRECTOR,
    "モーション": Role.CGI_DIRECTOR,
    "コンポジット": Role.PHOTOGRAPHY_DIRECTOR,
    "チーフディレクター": Role.DIRECTOR,
    "ユニットディレクター": Role.EPISODE_DIRECTOR,
    "整音": Role.SOUND_DIRECTOR,
    "ミキサー": Role.SOUND_DIRECTOR,
    "キャラクター著作": Role.CHARACTER_DESIGNER,
    "プロップデザイン": Role.SETTINGS,
    "設定協力": Role.SETTINGS,
    "制作事務": Role.PRODUCTION_MANAGER,
    "arrangement": Role.MUSIC,
    "設定": Role.SETTINGS,
    "構成協力": Role.SCREENPLAY,
    "スコア": Role.MUSIC,
    # SeesaaWiki — high-frequency extended roles
    "ストーリーボード": Role.EPISODE_DIRECTOR,
    "コンセプトデザイン": Role.CHARACTER_DESIGNER,
    "デジタルペイント検査": Role.FINISHING,
    "製作協力": Role.PRODUCER,
    "仕上協力": Role.FINISHING,
    "進行": Role.PRODUCTION_MANAGER,
    "cg制作": Role.CGI_DIRECTOR,
    "cgディレクター": Role.CGI_DIRECTOR,
    "背景制作": Role.BACKGROUND_ART,
    "演助進行": Role.PRODUCTION_MANAGER,
    "編集助手": Role.EDITING,
    "アニメーション絵コンテ": Role.EPISODE_DIRECTOR,
    "美術担当": Role.BACKGROUND_ART,
    "作画監督補": Role.ANIMATION_DIRECTOR,
    "オフライン編集": Role.EDITING,
    "著作": Role.ORIGINAL_CREATOR,
    "デジタル合成": Role.PHOTOGRAPHY_DIRECTOR,
    "cgi": Role.CGI_DIRECTOR,
    "仕上チェック": Role.FINISHING,
    "広報": Role.SPECIAL,
    "スペシャルサンクス": Role.SPECIAL,
    "スタジオコーディネート": Role.PRODUCER,
    "アクション原画": Role.KEY_ANIMATOR,
    "メインアニメーター": Role.KEY_ANIMATOR,
    "レイアウト作画監督": Role.ANIMATION_DIRECTOR,
    "原作協力": Role.ORIGINAL_CREATOR,
    "美術監修": Role.BACKGROUND_ART,
    "キャラクター原案": Role.CHARACTER_DESIGNER,
    "原作イラスト": Role.ORIGINAL_CREATOR,
    "エグゼクティブプロデューサー": Role.PRODUCER,
    "録音調整": Role.SOUND_DIRECTOR,
    "制作プロダクション": Role.PRODUCER,
    "製作著作": Role.PRODUCER,
    "共同制作": Role.PRODUCER,
    "バトル監修": Role.DIRECTOR,
    "美術デザイン": Role.BACKGROUND_ART,
    "オープニングディレクター": Role.DIRECTOR,
    "オープニング演出": Role.EPISODE_DIRECTOR,
    "エンディング演出": Role.EPISODE_DIRECTOR,
    "イラスト": Role.SPECIAL,
    "トレス": Role.IN_BETWEEN,
    "タイトル": Role.SPECIAL,
    "色彩指定": Role.FINISHING,
    "仕上げ管理": Role.FINISHING,
    "動画チェック補": Role.IN_BETWEEN,
    "線撮協力": Role.SPECIAL,
    "企画協力": Role.PRODUCER,
    # LLM validation round 2
    "製作総指揮": Role.PRODUCER,
    "音響演出": Role.SOUND_DIRECTOR,
    "基本設定": Role.SETTINGS,
    "技術協力": Role.SPECIAL,
    "ライティング": Role.PHOTOGRAPHY_DIRECTOR,
    "アソシエイト": Role.PRODUCER,
    "cgスーパーバイザー": Role.CGI_DIRECTOR,
    "音響制作協力": Role.SOUND_DIRECTOR,
    "制作デスク補佐": Role.PRODUCTION_MANAGER,
    "編成": Role.SPECIAL,
    "動画編集": Role.EDITING,
    "管理": Role.PRODUCTION_MANAGER,
    # LLM validation round 3
    "チーフプロデューサー": Role.PRODUCER,
    "プランニングマネージャー": Role.PRODUCER,
    "音響プロデューサー": Role.SOUND_DIRECTOR,
    "アニメーション監督": Role.DIRECTOR,
    "色指定補": Role.FINISHING,
    "監修": Role.DIRECTOR,
    "題字": Role.SPECIAL,
    "アシスタント": Role.SPECIAL,
    "ラインテスト": Role.FINISHING,
    "トレース": Role.FINISHING,
    "音響": Role.SOUND_DIRECTOR,
    "スタジオ": Role.SPECIAL,
    "オリジナルサウンドトラック": Role.MUSIC,
    "漫画": Role.ORIGINAL_CREATOR,
    "制作統括": Role.PRODUCER,
    "音楽a&r": Role.MUSIC,
    # LLM validation round 4
    "作画監督チーフ": Role.ANIMATION_DIRECTOR,
    "演出チーフ": Role.EPISODE_DIRECTOR,
    "タイトルロゴデザイン": Role.SPECIAL,
    "キャラクター設計": Role.CHARACTER_DESIGNER,
    "落語監修": Role.SPECIAL,
    "監修協力": Role.SPECIAL,
    "sf考証": Role.SPECIAL,
    "製作助手": Role.PRODUCTION_MANAGER,
    "音楽担当": Role.MUSIC,
    "アニメーションキャラクター": Role.CHARACTER_DESIGNER,
    "メカ・エフェクト作画監督": Role.ANIMATION_DIRECTOR,
    "色指定補佐": Role.FINISHING,
    # === Bulk addition: high-frequency unknown roles ===
    # CG / 3D
    "2dエフェクト": Role.PHOTOGRAPHY_DIRECTOR,
    "cg制作進行": Role.PRODUCTION_MANAGER,
    "cg進行": Role.PRODUCTION_MANAGER,
    "3dcg制作": Role.CGI_DIRECTOR,
    "cgiデザイナー": Role.CGI_DIRECTOR,
    "cgiディレクター": Role.CGI_DIRECTOR,
    "cgi協力": Role.SPECIAL,
    "cgiプロデューサー": Role.PRODUCER,
    "cg制作協力": Role.SPECIAL,
    "cg監督": Role.CGI_DIRECTOR,
    "cgプロダクトマネージャー": Role.PRODUCTION_MANAGER,
    "cgモデリングチーフ": Role.CGI_DIRECTOR,
    "cg制作プロデューサー": Role.PRODUCER,
    "3dcgi": Role.CGI_DIRECTOR,
    "3d": Role.CGI_DIRECTOR,
    "3d美術": Role.BACKGROUND_ART,
    "3dlo": Role.LAYOUT,
    "3dloリード": Role.LAYOUT,
    "3dレイアウト": Role.LAYOUT,
    "2dワークス": Role.PHOTOGRAPHY_DIRECTOR,
    "2dデザイン": Role.PHOTOGRAPHY_DIRECTOR,
    "2dモニターワークス": Role.PHOTOGRAPHY_DIRECTOR,
    "3d.c.g": Role.CGI_DIRECTOR,
    "コンポジットディレクター": Role.PHOTOGRAPHY_DIRECTOR,
    "背景3dモデリング": Role.BACKGROUND_ART,
    "キャラクターモデリング": Role.CGI_DIRECTOR,
    "モデラー": Role.CGI_DIRECTOR,
    "シニアデジタルアーティスト": Role.CGI_DIRECTOR,
    "デジタルアーティスト": Role.CGI_DIRECTOR,
    # 作画 extended
    "動画作監": Role.ANIMATION_DIRECTOR,
    "原画作監": Role.ANIMATION_DIRECTOR,
    "作監": Role.ANIMATION_DIRECTOR,
    "作監補": Role.ANIMATION_DIRECTOR,
    "キャラ作監": Role.ANIMATION_DIRECTOR,
    "メカ作監": Role.ANIMATION_DIRECTOR,
    "キャラクター作画監督": Role.ANIMATION_DIRECTOR,
    "メカニック作画監督": Role.ANIMATION_DIRECTOR,
    "メカニカル作画監督": Role.ANIMATION_DIRECTOR,
    "アクション作監": Role.ANIMATION_DIRECTOR,
    "動物作画監督": Role.ANIMATION_DIRECTOR,
    "プロップ作画監督": Role.ANIMATION_DIRECTOR,
    "総作画監督補佐": Role.ANIMATION_DIRECTOR,
    "総作画監督補": Role.ANIMATION_DIRECTOR,
    "総作監補佐": Role.ANIMATION_DIRECTOR,
    "エフェクト作画監督": Role.ANIMATION_DIRECTOR,
    "レイアウト監修": Role.LAYOUT,
    "作画監修": Role.ANIMATION_DIRECTOR,
    "エピローグ総作画監督": Role.ANIMATION_DIRECTOR,
    "原画作監補佐": Role.ANIMATION_DIRECTOR,
    "作画協力": Role.SPECIAL,
    "動画協力": Role.SPECIAL,
    "原画協力": Role.SPECIAL,
    "リードアニメーター": Role.KEY_ANIMATOR,
    "アニメーションディレクター": Role.DIRECTOR,
    "アニメーションキャラクターデザイン": Role.CHARACTER_DESIGNER,
    "キャラクターデザイン協力": Role.CHARACTER_DESIGNER,
    "サブキャラクターデザイン協力": Role.CHARACTER_DESIGNER,
    "ゲストデザイン": Role.CHARACTER_DESIGNER,
    "クリーチャーデザイン": Role.CHARACTER_DESIGNER,
    "ビジュアルデザイン": Role.CHARACTER_DESIGNER,
    "原画設計": Role.KEY_ANIMATOR,
    "画コンテ": Role.EPISODE_DIRECTOR,
    "ストーリィボード": Role.EPISODE_DIRECTOR,
    "アニメーションストーリーボード": Role.EPISODE_DIRECTOR,
    "変身原画": Role.KEY_ANIMATOR,
    "アクション作画": Role.KEY_ANIMATOR,
    # 仕上 extended
    "仕上助手": Role.FINISHING,
    "仕上特効": Role.FINISHING,
    "仕上検査補佐": Role.FINISHING,
    "仕上げ協力": Role.FINISHING,
    "ペイント": Role.FINISHING,
    "ペイント検査補佐": Role.FINISHING,
    "彩画": Role.FINISHING,
    "検査補佐": Role.FINISHING,
    "検査協力": Role.FINISHING,
    "デジタル仕上": Role.FINISHING,
    "デジタル仕上げ": Role.FINISHING,
    "デジタル検査": Role.FINISHING,
    "デジタル動画検査": Role.FINISHING,
    "色指定仕上検査": Role.FINISHING,
    "デジタル特効": Role.PHOTOGRAPHY_DIRECTOR,
    "デジタル動画": Role.IN_BETWEEN,
    "動画チーフ": Role.IN_BETWEEN,
    "動仕制作": Role.PRODUCTION_MANAGER,
    # 編集 extended
    "オンライン編集": Role.EDITING,
    "hd編集": Role.EDITING,
    "フォーマット編集": Role.EDITING,
    "デジタル編集": Role.EDITING,
    # 撮影 extended
    "コンポジット撮影": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影協力": Role.SPECIAL,
    "撮影チーフ": Role.PHOTOGRAPHY_DIRECTOR,
    "モニターワークス": Role.PHOTOGRAPHY_DIRECTOR,
    "モニターグラフィック": Role.PHOTOGRAPHY_DIRECTOR,
    "モニターグラフィックス": Role.PHOTOGRAPHY_DIRECTOR,
    "スキャン": Role.EDITING,
    "スキャニング": Role.EDITING,
    "フィルム": Role.EDITING,
    # 美術 extended
    "美術監督補佐": Role.BACKGROUND_ART,
    "美術協力": Role.SPECIAL,
    "美術背景": Role.BACKGROUND_ART,
    "美術統括": Role.BACKGROUND_ART,
    "背景協力": Role.SPECIAL,
    "背景管理": Role.PRODUCTION_MANAGER,
    "背景監修": Role.BACKGROUND_ART,
    # 設定 extended
    "設定管理": Role.SETTINGS,
    "画面設計": Role.SETTINGS,
    "原図": Role.SETTINGS,
    "原図整理": Role.SETTINGS,
    "原図監修": Role.SETTINGS,
    "衣装デザイン": Role.SETTINGS,
    "衣装協力": Role.SPECIAL,
    "服装設定": Role.SETTINGS,
    "キャラクター監修": Role.CHARACTER_DESIGNER,
    # 音響 extended
    "音楽制作協力": Role.SPECIAL,
    "音楽ディレクター": Role.MUSIC,
    "サウンドミキサー": Role.SOUND_DIRECTOR,
    # 演出 extended
    "演出補": Role.EPISODE_DIRECTOR,
    "演出協力": Role.SPECIAL,
    "ディレクター": Role.DIRECTOR,
    # 制作管理 extended
    "制作管理": Role.PRODUCTION_MANAGER,
    "制作プロデューサー": Role.PRODUCER,
    "宣伝プロデューサー": Role.SPECIAL,
    "制作進行チーフ": Role.PRODUCTION_MANAGER,
    "文芸進行": Role.PRODUCTION_MANAGER,
    "モデル協力": Role.SPECIAL,
    "モデル進行管理": Role.PRODUCTION_MANAGER,
    "進行協力": Role.SPECIAL,
    # デザイン extended
    "デザイン協力": Role.SPECIAL,
    "ロゴデザイン": Role.SPECIAL,
    "タイトルデザイン": Role.SPECIAL,
    "ゲストキャラクター原案": Role.CHARACTER_DESIGNER,
    # Special (non-production)
    "セールスプロモーション": Role.SPECIAL,
    "ライセンス": Role.SPECIAL,
    "ライセンス担当": Role.SPECIAL,
    "海外セールス": Role.SPECIAL,
    "海外担当": Role.SPECIAL,
    "海外ライセンス": Role.SPECIAL,
    "配給": Role.SPECIAL,
    "配給営業": Role.SPECIAL,
    "劇場営業": Role.SPECIAL,
    "番組担当": Role.SPECIAL,
    "番組協力": Role.SPECIAL,
    "取材協力": Role.SPECIAL,
    "協力プロダクション": Role.SPECIAL,
    "協力スタジオ": Role.SPECIAL,
    "編集スタジオ": Role.SPECIAL,
    "web制作": Role.SPECIAL,
    "公式サイト制作": Role.SPECIAL,
    "システム管理": Role.SPECIAL,
    "宣伝担当": Role.SPECIAL,
    "宣伝パブリシティ": Role.SPECIAL,
    "操演": Role.SPECIAL,
    "実写ディレクター": Role.SPECIAL,
    "実写制作協力": Role.SPECIAL,
    "キャスティング協力": Role.SPECIAL,
    "コーディネーター": Role.SPECIAL,
    "フォント協力": Role.SPECIAL,
    "プロモーション協力": Role.SPECIAL,
    # 音楽 extended
    "挿入歌": Role.MUSIC,
    "ボーカル": Role.MUSIC,
    "lyrics": Role.MUSIC,
    "artist": Role.MUSIC,
    "エンドカードイラスト": Role.SPECIAL,
    "エンドカード": Role.SPECIAL,
    "pv制作": Role.SPECIAL,
    "アイキャッチ": Role.SPECIAL,
    "予告アニメーション": Role.SPECIAL,
    "提供原画": Role.SPECIAL,
    "振り付け": Role.SPECIAL,
    "楽器監修": Role.SPECIAL,
    "主題歌協力": Role.SPECIAL,
    # その他
    "補佐": Role.SPECIAL,
    "デジタル作画": Role.KEY_ANIMATOR,
    "原案協力": Role.ORIGINAL_CREATOR,
    "デジタル制作": Role.PRODUCTION_MANAGER,
    "ツール開発": Role.SPECIAL,
    "モーションアドバイザー": Role.SPECIAL,
    "メカニカルコーディネーター": Role.CHARACTER_DESIGNER,
    "児童画": Role.SPECIAL,
    "webプロモーション": Role.SPECIAL,
    "作品提供": Role.SPECIAL,
    # 声優・音楽関連（分析対象外だが明示的に区別）
    "voice actor": Role.VOICE_ACTOR,
    "voice acting": Role.VOICE_ACTOR,
    "theme song performance": Role.MUSIC,
    "theme song arrangement": Role.MUSIC,
    "theme song composition": Role.MUSIC,
    "theme song lyrics": Role.MUSIC,
    "insert song performance": Role.MUSIC,
    "insert song lyrics": Role.MUSIC,
    "ending theme": Role.MUSIC,
    "opening theme": Role.MUSIC,
    # 吹き替え関連
    "adr director": Role.VOICE_ACTOR,
    "adr script": Role.VOICE_ACTOR,
    "adr director assistant": Role.VOICE_ACTOR,
    # =================================================================
    # SeesaaWiki round 2: 慎重に分類した残りの未分類ロール
    # 原則: 時代/ツールの違い(デジタル vs アナログ)は区別しない
    #        部門名+担当/助手 = その部門の職種
    #        施設名/商品名 = SPECIAL (人ではない)
    # =================================================================
    # --- CG/3D: 伝統職のデジタル版は同じRole、CG固有職はCGI_DIRECTOR ---
    # CGコンポジット = 撮影(コンポジット=撮影部門の仕事)
    "cgコンポジター": Role.PHOTOGRAPHY_DIRECTOR,
    "cgカメラワーク": Role.PHOTOGRAPHY_DIRECTOR,
    "リードコンポジター": Role.PHOTOGRAPHY_DIRECTOR,
    "リードコンポジッター": Role.PHOTOGRAPHY_DIRECTOR,
    "コンポジター": Role.PHOTOGRAPHY_DIRECTOR,
    "コンポジッター": Role.PHOTOGRAPHY_DIRECTOR,
    # CGレイアウト = レイアウト(道具が違うだけ)
    "cgレイアウト": Role.LAYOUT,
    # CG背景 = 背景(道具が違うだけ)
    "cgバックグラウンド": Role.BACKGROUND_ART,
    "背景3d": Role.BACKGROUND_ART,
    # CGアニメーション = 原画(ポーズ/タイミング/動きの設計は同じ仕事)
    "cgアニメーション": Role.KEY_ANIMATOR,
    "3dcgアニメーション": Role.KEY_ANIMATOR,
    "3dアニメーション": Role.KEY_ANIMATOR,
    # CGレタッチ = レタッチ(編集/ポスプロ工程)
    "cgレタッチ": Role.EDITING,
    # CG固有職(モデリング, リギング等 — 伝統アニメに対応なし)
    "3dモデラー": Role.CGI_DIRECTOR,
    "cgモデラー": Role.CGI_DIRECTOR,
    "cgモデリング・リーダー": Role.CGI_DIRECTOR,
    "cgモデリング・開発": Role.CGI_DIRECTOR,
    "モデリング/リギング": Role.CGI_DIRECTOR,
    "モデリングアーティスト": Role.CGI_DIRECTOR,
    "モデリングチーフ": Role.CGI_DIRECTOR,
    "モデリングデザイナー": Role.CGI_DIRECTOR,
    "モデリングリード": Role.CGI_DIRECTOR,
    "モデリング・リギング": Role.CGI_DIRECTOR,
    "リードモデラー": Role.CGI_DIRECTOR,
    "リガー": Role.CGI_DIRECTOR,
    "リギング": Role.CGI_DIRECTOR,
    "セットアップ": Role.CGI_DIRECTOR,
    "レンダリング": Role.CGI_DIRECTOR,
    "テクスチャペインター": Role.CGI_DIRECTOR,
    "モーションデザイナー": Role.CGI_DIRECTOR,
    "モーションキャプチャー": Role.CGI_DIRECTOR,
    "キャラモデラー": Role.CGI_DIRECTOR,
    # CG部門の一般的な職名
    "cgiアート": Role.CGI_DIRECTOR,
    "cgiチーフデザイナー": Role.CGI_DIRECTOR,
    "cgアセット": Role.CGI_DIRECTOR,
    "cgアセットデザイナー": Role.CGI_DIRECTOR,
    "cgアーティスト": Role.CGI_DIRECTOR,
    "cgアート": Role.CGI_DIRECTOR,
    "cgスタッフ": Role.CGI_DIRECTOR,
    "cgチーフ": Role.CGI_DIRECTOR,
    "cgチーフデザイナー": Role.CGI_DIRECTOR,
    "cgテクニカルデザイナー": Role.CGI_DIRECTOR,
    "cgディレクター助手": Role.CGI_DIRECTOR,
    "cgデザイナー": Role.CGI_DIRECTOR,
    "cg作成": Role.CGI_DIRECTOR,
    "cg監督補佐・cgデザイナー": Role.CGI_DIRECTOR,
    "アセットチーフ": Role.CGI_DIRECTOR,
    # 3DCG表記揺れ
    "2dcgチーフ": Role.CGI_DIRECTOR,
    "3cgi": Role.CGI_DIRECTOR,
    "3d-cgi": Role.CGI_DIRECTOR,
    "3d.c.g.i": Role.CGI_DIRECTOR,
    "3d.cgi": Role.CGI_DIRECTOR,
    "3dbg": Role.BACKGROUND_ART,
    "3dc.g.i": Role.CGI_DIRECTOR,
    "3dcgiワークス": Role.CGI_DIRECTOR,
    "3dcgスタッフ": Role.CGI_DIRECTOR,
    "3dcgチーフ": Role.CGI_DIRECTOR,
    "3dcgデザイナー": Role.CGI_DIRECTOR,
    "3dcgワーク": Role.CGI_DIRECTOR,
    "3dcgワークス": Role.CGI_DIRECTOR,
    "3dデザイナー": Role.CGI_DIRECTOR,
    "3dマネジメント": Role.CGI_DIRECTOR,
    "3dワーク": Role.CGI_DIRECTOR,
    "3dワークス": Role.CGI_DIRECTOR,
    # --- エフェクト: デジタル特効=特効(時代の違い) ---
    "ae・特効": Role.PHOTOGRAPHY_DIRECTOR,
    "vfx": Role.PHOTOGRAPHY_DIRECTOR,
    "特効": Role.PHOTOGRAPHY_DIRECTOR,
    "デジタル特殊効果": Role.PHOTOGRAPHY_DIRECTOR,
    "デジタル撮影&vfx": Role.PHOTOGRAPHY_DIRECTOR,
    "特技効果・デザインワークス": Role.PHOTOGRAPHY_DIRECTOR,
    "特殊効果・スクリプト開発": Role.PHOTOGRAPHY_DIRECTOR,
    "エフェクトアーティスト": Role.PHOTOGRAPHY_DIRECTOR,
    "エフェクトデザイナー": Role.PHOTOGRAPHY_DIRECTOR,
    "エフェクト開発": Role.PHOTOGRAPHY_DIRECTOR,
    # 2Dエフェクト/モニターワーク
    "2dcg": Role.PHOTOGRAPHY_DIRECTOR,
    "2dcgi": Role.PHOTOGRAPHY_DIRECTOR,
    "2dvfx": Role.PHOTOGRAPHY_DIRECTOR,
    "2dworks": Role.PHOTOGRAPHY_DIRECTOR,
    "2dエフェクトチーフ": Role.PHOTOGRAPHY_DIRECTOR,
    "2dグラフィック": Role.PHOTOGRAPHY_DIRECTOR,
    "2dグラフィックス": Role.PHOTOGRAPHY_DIRECTOR,
    "2dデザインワークス": Role.PHOTOGRAPHY_DIRECTOR,
    "2dデジタル": Role.PHOTOGRAPHY_DIRECTOR,
    "2dモニター": Role.PHOTOGRAPHY_DIRECTOR,
    "2dモニターワーク": Role.PHOTOGRAPHY_DIRECTOR,
    "2dワーク": Role.PHOTOGRAPHY_DIRECTOR,
    "モーショングラフィック": Role.PHOTOGRAPHY_DIRECTOR,
    "モーショングラフィックス": Role.PHOTOGRAPHY_DIRECTOR,
    # --- 撮影: コンポジット=撮影(同じ部門) ---
    "撮影助手": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影管理": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影チーム長": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影監督補": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影・sfx": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影 / 編集 / モーショングラフィックス": Role.PHOTOGRAPHY_DIRECTOR,
    "撮影担当": Role.PHOTOGRAPHY_DIRECTOR,
    "線撮": Role.PHOTOGRAPHY_DIRECTOR,
    # スキャン = ポスプロ(編集)工程
    "bgスキャニング": Role.EDITING,
    "bgスキャン": Role.EDITING,
    "bg補正": Role.EDITING,
    "scan": Role.EDITING,
    "背景スキャニング": Role.EDITING,
    # --- 色彩: 色彩設計/色指定(創造) vs 仕上げ(実行) ---
    "カラリスト": Role.FINISHING,
    "カラーコーディネイト": Role.FINISHING,
    "カラーマネジメント": Role.FINISHING,
    "ゲスト色彩設計": Role.FINISHING,
    "ゲスト色彩設計・色指定": Role.FINISHING,
    "色彩": Role.FINISHING,
    "色彩設定補佐": Role.FINISHING,
    "色彩設計補": Role.FINISHING,
    "色指定・検査・貼込": Role.FINISHING,
    "色指定助手": Role.FINISHING,
    "色指定検査補佐": Role.FINISHING,
    "色指定補助": Role.FINISHING,
    # --- 仕上げ: セル検査=デジタル検査(時代の違い) ---
    "セル検": Role.FINISHING,
    "セル検査補佐": Role.FINISHING,
    "デジタル・ペイント": Role.FINISHING,
    "仕上げチーフ": Role.FINISHING,
    "仕上げ助手": Role.FINISHING,
    "仕上げ検査補佐": Role.FINISHING,
    "仕上処理": Role.FINISHING,
    "仕上検査補": Role.FINISHING,
    "仕上管理": Role.FINISHING,
    "彩色チェック": Role.FINISHING,
    "着彩": Role.FINISHING,
    "加工担当": Role.FINISHING,
    "動画仕上げ管理": Role.FINISHING,
    "動画仕上管理": Role.FINISHING,
    "エアブラシワーク": Role.FINISHING,
    "タッチ/ブラシ": Role.FINISHING,
    "検査補": Role.FINISHING,
    "検査補助": Role.FINISHING,
    "チェック補": Role.FINISHING,
    "デジタル修正": Role.FINISHING,
    "デジタル処理": Role.FINISHING,
    "データチェック": Role.FINISHING,
    # --- 動画 ---
    "二原": Role.SECOND_KEY_ANIMATOR,
    "動検": Role.IN_BETWEEN,
    "動画チェック補佐": Role.IN_BETWEEN,
    "動画検査・デジタル修正": Role.IN_BETWEEN,
    "動画検査補佐": Role.IN_BETWEEN,
    "動画サポーター": Role.IN_BETWEEN,
    "動画管理": Role.FINISHING,  # 動仕管理=仕上げ寄り
    # --- 原画/レイアウト ---
    "メインレイアウト": Role.LAYOUT,
    "レイアウトチェッカー": Role.LAYOUT,
    "レイアウトチェック": Role.LAYOUT,
    "レイアウト・チェック": Role.LAYOUT,
    "レイアウト修正": Role.LAYOUT,
    "割絵": Role.KEY_ANIMATOR,
    "原絵師": Role.KEY_ANIMATOR,
    "第弐原絵師": Role.SECOND_KEY_ANIMATOR,
    "タイミング": Role.KEY_ANIMATOR,  # アニメーションタイミング=原画の技術
    # --- 作画監督 ---
    "アクション作画監督補": Role.ANIMATION_DIRECTOR,
    "キャラクター作画監督補佐": Role.ANIMATION_DIRECTOR,
    "レイアウト作画監督補佐": Role.ANIMATION_DIRECTOR,
    "作監補佐": Role.ANIMATION_DIRECTOR,
    "原画作画監督補佐": Role.ANIMATION_DIRECTOR,
    "原画作監補": Role.ANIMATION_DIRECTOR,
    "総作監補": Role.ANIMATION_DIRECTOR,
    "絵師頭": Role.ANIMATION_DIRECTOR,  # 古い呼称の作画統括
    # --- 演出 ---
    "演出サポート": Role.EPISODE_DIRECTOR,
    "演出補佐": Role.EPISODE_DIRECTOR,
    "演助": Role.EPISODE_DIRECTOR,
    "演出統括": Role.DIRECTOR,  # 統括=監督級
    # --- 監督 ---
    "監督助手": Role.DIRECTOR,
    "監督補": Role.DIRECTOR,
    "監督補佐": Role.DIRECTOR,
    "副監督補佐": Role.DIRECTOR,
    "ディレクション": Role.DIRECTOR,
    "アートディレクション": Role.BACKGROUND_ART,
    "背景監督補": Role.BACKGROUND_ART,
    "音響監督助手": Role.SOUND_DIRECTOR,
    # --- 編集: フィルム/ビデオ/デジタル=同じ仕事(時代の違い) ---
    "dcpマスタリング": Role.EDITING,
    "digital.tp": Role.EDITING,
    "eed": Role.EDITING,
    "tp": Role.EDITING,
    "tp修正": Role.EDITING,
    "テレシネ": Role.EDITING,
    "フィルムレコーディング": Role.EDITING,
    "ポストプロダクション": Role.EDITING,
    "デジタルシネマエンジニア": Role.EDITING,
    "デジタルシネママスタリング": Role.EDITING,
    "デジタルラボ": Role.EDITING,
    "編集アシスタント": Role.EDITING,
    "編集デスク": Role.EDITING,
    "編集補佐": Role.EDITING,
    # 部門名+助手/担当 = その部門の人(時代の接頭辞は無視)
    "hd編集制作担当": Role.EDITING,
    "hd編集助手": Role.EDITING,
    "hd編集担当": Role.EDITING,
    "ビデオ編集デスク": Role.EDITING,
    "ビデオ編集助手": Role.EDITING,
    "ビデオ編集担当": Role.EDITING,
    "オンライン編集デスク": Role.EDITING,
    "オンライン編集助手": Role.EDITING,
    "オンライン編集担当": Role.EDITING,
    "オフライン編集助手": Role.EDITING,
    "フォーマット編集担当": Role.EDITING,
    "ラボ・デスク": Role.EDITING,
    "ビデオエディター": Role.EDITING,
    "ve": Role.EDITING,  # Video Engineer
    "vtrワーク": Role.EDITING,
    "調整助手": Role.EDITING,
    # --- 音響: 録音技師=ミキサー=エンジニア(呼称の違い) ---
    "ma": Role.SOUND_DIRECTOR,
    "フォーリー": Role.SOUND_DIRECTOR,
    "効果助手": Role.SOUND_DIRECTOR,
    "整音助手": Role.SOUND_DIRECTOR,
    "音効": Role.SOUND_DIRECTOR,
    "選曲・効果": Role.SOUND_DIRECTOR,
    "サウンドエディター": Role.SOUND_DIRECTOR,
    "サウンド・エディター": Role.SOUND_DIRECTOR,
    "サウンド・ミキサー": Role.SOUND_DIRECTOR,
    "ミキシングエンジニア": Role.SOUND_DIRECTOR,
    "ミックスエンジニア": Role.SOUND_DIRECTOR,
    "レコーディング&ミキサー": Role.SOUND_DIRECTOR,
    "レコーディング&ミキシングエンジニア": Role.SOUND_DIRECTOR,
    "レコーディング&ミックスエンジニア": Role.SOUND_DIRECTOR,
    "レコーディングエンジニア": Role.SOUND_DIRECTOR,
    "録音アシスタント": Role.SOUND_DIRECTOR,
    "録音エンジニア": Role.SOUND_DIRECTOR,
    "録音技術": Role.SOUND_DIRECTOR,
    "hdレコーディング": Role.SOUND_DIRECTOR,
    "音楽録音": Role.SOUND_DIRECTOR,
    "音響助手": Role.SOUND_DIRECTOR,
    "音響効果助手": Role.SOUND_DIRECTOR,
    "音響製作": Role.SOUND_DIRECTOR,
    "音響調整": Role.SOUND_DIRECTOR,
    "音響制作デスク": Role.SOUND_DIRECTOR,
    "音響担当": Role.SOUND_DIRECTOR,
    "アシスタントミキサー": Role.SOUND_DIRECTOR,
    # --- 音楽: 演奏者=THEME_SONG、制作=MUSIC ---
    "bass": Role.MUSIC,
    "drums": Role.MUSIC,
    "guitar": Role.MUSIC,
    "horn": Role.MUSIC,
    "strings": Role.MUSIC,
    "vocal": Role.MUSIC,
    "トランペット": Role.MUSIC,
    "ドラム": Role.MUSIC,
    "ベース": Role.MUSIC,
    "ピアノ演奏": Role.MUSIC,
    "コーラス": Role.MUSIC,
    "演奏": Role.MUSIC,
    "歌唱": Role.MUSIC,
    "エンディングテーマ": Role.MUSIC,
    "エンディング曲": Role.MUSIC,
    "オープニング曲": Role.MUSIC,
    "words": Role.MUSIC,
    "a&r": Role.MUSIC,
    "主題歌プロデュース": Role.MUSIC,
    "音楽プロデュース": Role.MUSIC,
    "楽曲コーディネート": Role.MUSIC,
    "一部作曲・原曲": Role.MUSIC,
    "作詩": Role.MUSIC,
    "音楽制作担当": Role.MUSIC,
    "音楽製作": Role.MUSIC,
    "指揮": Role.MUSIC,  # 音楽指揮者
    # --- 美術/背景 ---
    "美監補佐": Role.BACKGROUND_ART,
    "美術監督捕": Role.BACKGROUND_ART,
    "美術デザイン補佐": Role.BACKGROUND_ART,
    "美術話数担当": Role.BACKGROUND_ART,
    "美術3d作業": Role.BACKGROUND_ART,
    "美術デジタルワークス": Role.BACKGROUND_ART,
    "美術助手": Role.BACKGROUND_ART,
    "美術補正": Role.BACKGROUND_ART,
    "美術設定補佐": Role.BACKGROUND_ART,
    "美術設計": Role.BACKGROUND_ART,
    "イメージボード": Role.BACKGROUND_ART,
    "コンセプトアート": Role.BACKGROUND_ART,
    "ビジュアルアート": Role.BACKGROUND_ART,
    "ビジュアルコンセプト": Role.BACKGROUND_ART,
    "背景チーフ": Role.BACKGROUND_ART,
    "背景デジタル処理": Role.BACKGROUND_ART,
    "背景レイアウト": Role.BACKGROUND_ART,
    "背景レタッチ": Role.BACKGROUND_ART,
    "背景担当": Role.BACKGROUND_ART,
    "背景統括": Role.BACKGROUND_ART,
    "背景補正": Role.BACKGROUND_ART,
    "話数背景担当": Role.BACKGROUND_ART,
    # --- 設定 ---
    "設定・資料": Role.SETTINGS,
    "設定考証": Role.SETTINGS,
    "設定補": Role.SETTINGS,
    "設計補佐": Role.SETTINGS,
    "設定制作補佐": Role.SETTINGS,
    "設定担当": Role.SETTINGS,
    "アクセサリーデザイン案": Role.SETTINGS,
    "サブ・小物": Role.SETTINGS,
    "プロップデザイン補佐": Role.SETTINGS,
    "衣装コンセプトデザイン・アシスタント": Role.SETTINGS,
    # --- 脚本 ---
    "文芸助手": Role.SCREENPLAY,
    "脚本事務": Role.SCREENPLAY,
    "脚本構成": Role.SCREENPLAY,
    "脚色": Role.SCREENPLAY,
    "ストーリー": Role.SCREENPLAY,
    "ストーリーエディター": Role.SCREENPLAY,
    "チーフライター": Role.SCREENPLAY,
    # --- 絵コンテ ---
    "絵コンテ・演出担当": Role.EPISODE_DIRECTOR,
    "絵コンテ清書": Role.EPISODE_DIRECTOR,
    "アニマティックアーティスト": Role.EPISODE_DIRECTOR,  # アニマティクス=動くコンテ
    # --- デザイン ---
    "キャラクター": Role.CHARACTER_DESIGNER,
    "キャラクターデザイン補佐": Role.CHARACTER_DESIGNER,
    "サブキャラクター": Role.CHARACTER_DESIGNER,
    "チーフデザイナー": Role.CHARACTER_DESIGNER,
    "デザイナー": Role.CHARACTER_DESIGNER,
    "デザインワーク": Role.CHARACTER_DESIGNER,
    "ビジュアルワークス": Role.CHARACTER_DESIGNER,
    "メカデザインワークス": Role.CHARACTER_DESIGNER,
    "メカデザイン補佐": Role.CHARACTER_DESIGNER,
    "メカニック": Role.CHARACTER_DESIGNER,
    "メカニックワーク": Role.CHARACTER_DESIGNER,
    "メカ修正": Role.CHARACTER_DESIGNER,
    # --- 制作進行(真の現場管理職のみ) ---
    "制作アシスタント": Role.PRODUCTION_MANAGER,
    "制作サポート": Role.PRODUCTION_MANAGER,
    "制作チーフ": Role.PRODUCTION_MANAGER,
    "制作デスク補": Role.PRODUCTION_MANAGER,
    "制作事務統括": Role.PRODUCTION_MANAGER,
    "制作助手": Role.PRODUCTION_MANAGER,
    "制作応援": Role.PRODUCTION_MANAGER,
    "制作担当補佐": Role.PRODUCTION_MANAGER,
    "制作補佐": Role.PRODUCTION_MANAGER,
    "制作話数担当": Role.PRODUCTION_MANAGER,
    "制作進行アシスタント": Role.PRODUCTION_MANAGER,
    "制作進行補": Role.PRODUCTION_MANAGER,
    "デスク": Role.PRODUCTION_MANAGER,
    "デジタル制作管理": Role.PRODUCTION_MANAGER,
    "進行チーフ": Role.PRODUCTION_MANAGER,
    "進行補佐": Role.PRODUCTION_MANAGER,
    "話数制作担当": Role.PRODUCTION_MANAGER,
    "スタジオ制作担当": Role.PRODUCTION_MANAGER,
    "cg制作デスク": Role.PRODUCTION_MANAGER,
    "cg制作担当": Role.PRODUCTION_MANAGER,
    "cg制作管理": Role.PRODUCTION_MANAGER,
    "コンポジット制作担当": Role.PRODUCTION_MANAGER,
    "bank管理": Role.PRODUCTION_MANAGER,
    "デジタル管理": Role.PRODUCTION_MANAGER,
    "背景進行補佐": Role.PRODUCTION_MANAGER,  # 進行=管理職
    # --- プロデューサー(ビジネス側) ---
    "製作デスク": Role.PRODUCER,
    "製作管理": Role.PRODUCER,
    "製作統括": Role.PRODUCER,
    "製作補": Role.PRODUCER,
    "製作業務": Role.PRODUCER,
    "製作・発売": Role.PRODUCER,
    "製作委員会": Role.PRODUCER,
    "プロデューサー補": Role.PRODUCER,
    "ap": Role.PRODUCER,  # Assistant Producer
    "アニメーションプロデュース": Role.PRODUCER,
    "アニメーション制作統括": Role.PRODUCER,
    "プランニングマネジャー": Role.PRODUCER,
    # --- VOICE_ACTOR ---
    "ナレーション": Role.VOICE_ACTOR,
    "出演": Role.VOICE_ACTOR,
    "パーソナリティ": Role.VOICE_ACTOR,
    # --- SPECIAL: 非制作部門/施設名/商品名 ---
    # 施設名(人ではない)
    "hdビデオ編集スタジオ": Role.SPECIAL,
    "hd編集スタジオ": Role.SPECIAL,
    "hd編集室": Role.SPECIAL,
    "maスタジオ": Role.SPECIAL,
    "アフレコスタジオ": Role.SPECIAL,
    "オフライン編集スタジオ": Role.SPECIAL,
    "オンライン編集スタジオ": Role.SPECIAL,
    "ダビングスタジオ": Role.SPECIAL,
    "ビデオ編集スタジオ": Role.SPECIAL,
    "レコーディングスタジオ": Role.SPECIAL,
    "収録スタジオ": Role.SPECIAL,
    "音響スタジオ": Role.SPECIAL,
    "現像所": Role.SPECIAL,
    "レーベル": Role.SPECIAL,
    "hd編集アシスタント": Role.SPECIAL,  # スタジオ付帯
    # 商品名
    "オリジナルサウンドトラック盤": Role.SPECIAL,
    "サウンドトラック盤": Role.SPECIAL,
    # 営業/宣伝/ライツ/法務
    "企画営業": Role.SPECIAL,
    "企画担当": Role.SPECIAL,
    "営業": Role.SPECIAL,
    "宣伝プロデュース": Role.SPECIAL,
    "宣伝・販促": Role.SPECIAL,
    "宣伝協力・ダンス": Role.SPECIAL,
    "宣伝広報": Role.SPECIAL,
    "宣伝統括": Role.SPECIAL,
    "販促": Role.SPECIAL,
    "販促担当": Role.SPECIAL,
    "販売": Role.SPECIAL,
    "販売プロモーション": Role.SPECIAL,
    "販売促進": Role.SPECIAL,
    "プロモーション": Role.SPECIAL,
    "プロモーター": Role.SPECIAL,
    "プロモート": Role.SPECIAL,
    "劇場宣伝": Role.SPECIAL,
    "制作宣伝": Role.SPECIAL,
    "制作広報": Role.SPECIAL,
    "広報担当": Role.SPECIAL,
    "パブリシティ": Role.SPECIAL,
    "プロダクション営業": Role.SPECIAL,
    "ライセンシング": Role.SPECIAL,
    "ライツ担当": Role.SPECIAL,
    "ライツプロモート": Role.SPECIAL,
    "国内ライセンス": Role.SPECIAL,
    "配信": Role.SPECIAL,
    "配信ライセンス": Role.SPECIAL,
    "配信担当": Role.SPECIAL,
    "配給統括": Role.SPECIAL,
    "配給調整": Role.SPECIAL,
    "法務担当": Role.SPECIAL,
    "版権担当": Role.SPECIAL,
    "版権管理": Role.SPECIAL,
    "商品化担当": Role.SPECIAL,
    "海外セールス担当": Role.SPECIAL,
    "海外プロモート": Role.SPECIAL,
    "海外営業": Role.SPECIAL,
    "海外渉外": Role.SPECIAL,
    "海外販売": Role.SPECIAL,
    "パッケージ営業": Role.SPECIAL,
    "パッケージ製造": Role.SPECIAL,
    "mdライセンス担当": Role.SPECIAL,
    "セールスプランニング": Role.SPECIAL,
    "マーケティング": Role.SPECIAL,
    # Web/HP
    "web担当": Role.SPECIAL,
    "ホームページ": Role.SPECIAL,
    "公式ホームページ": Role.SPECIAL,
    "オフィシャルサイト": Role.SPECIAL,
    "携帯サイト": Role.SPECIAL,
    # タイトルデザイン/テロップ
    "タイトルリスワーク": Role.SPECIAL,
    "タイトルロゴ": Role.SPECIAL,
    "タイトル・リスワーク": Role.SPECIAL,
    "テロップ": Role.SPECIAL,
    "メインタイトル": Role.SPECIAL,
    "サブタイトル": Role.SPECIAL,
    "サブタイトル題字": Role.SPECIAL,
    "リスワーク": Role.SPECIAL,
    "筆文字": Role.SPECIAL,
    "フォト・タイプ": Role.SPECIAL,
    # 出演/パフォーマンス(非制作)
    "ダンサー": Role.SPECIAL,
    "ダンス振付": Role.SPECIAL,
    "振付": Role.SPECIAL,
    "照明": Role.SPECIAL,
    "演技事務": Role.SPECIAL,
    "特殊演技": Role.SPECIAL,
    "狂言": Role.SPECIAL,
    "藝頭": Role.SPECIAL,
    # 考証/指導
    "方言指導": Role.SPECIAL,
    "時代考証": Role.SPECIAL,
    "テクニカルアドバイザー": Role.SPECIAL,
    "アドバイザー": Role.SPECIAL,
    "俳優担当": Role.SPECIAL,
    "原作担当": Role.SPECIAL,
    # 翻訳/通訳
    "翻訳": Role.SPECIAL,
    "通訳": Role.SPECIAL,
    "和訳": Role.SPECIAL,
    "韓国語通訳・翻訳": Role.SPECIAL,
    # メディアミックス派生(アニメ→他媒体、原作者ではない)
    "コミカライズ": Role.SPECIAL,
    "コミック": Role.SPECIAL,
    "コミック連載": Role.SPECIAL,
    "ノベライズ": Role.SPECIAL,
    "漫画連載": Role.SPECIAL,
    "予告マンガ": Role.SPECIAL,
    # その他
    "アイキャッチ/オリジナルカード紹介": Role.SPECIAL,
    "アイキャッチデザイナー": Role.SPECIAL,
    "アイキャッチ・ラストカット": Role.SPECIAL,
    "パブリックデザイナー": Role.SPECIAL,
    "データ放送": Role.SPECIAL,
    "放送": Role.SPECIAL,
    "次回予告": Role.SPECIAL,
    "op/edアニメーション": Role.SPECIAL,
    "オープニングアニメーション": Role.SPECIAL,
    "キャスティング": Role.SPECIAL,
    "参考資料": Role.SPECIAL,
    "ツール・スクリプト開発": Role.SPECIAL,
    "スクリプト開発": Role.SPECIAL,
    "r&d・インフラ開発": Role.SPECIAL,
    "pd": Role.SPECIAL,
    "トリック案": Role.SPECIAL,
    "掲載": Role.SPECIAL,
    "テクニカルサポート": Role.SPECIAL,
    # IT系
    "プログラマー": Role.SPECIAL,
    "システム": Role.SPECIAL,
    "システムエンジニア": Role.SPECIAL,
    "システム・マネージメント": Role.SPECIAL,
    "ラボ・マネージメント": Role.SPECIAL,
    # 曲名(ロールではない)
    "曲名": Role.SPECIAL,
    # === ANN固有ロール (英語表記) ===
    # ANN Encyclopedia は英語ロール名を使用する
    "direction": Role.DIRECTOR,
    "chief direction": Role.DIRECTOR,
    "series direction": Role.DIRECTOR,
    "assistant direction": Role.EPISODE_DIRECTOR,
    "episode direction": Role.EPISODE_DIRECTOR,
    "unit direction": Role.EPISODE_DIRECTOR,
    "animation direction": Role.ANIMATION_DIRECTOR,
    "chief animation direction": Role.ANIMATION_DIRECTOR,
    "action animation direction": Role.ANIMATION_DIRECTOR,
    "mecha animation direction": Role.ANIMATION_DIRECTOR,
    "effects animation direction": Role.ANIMATION_DIRECTOR,
    "finish animation": Role.FINISHING,
    "creature design": Role.CHARACTER_DESIGNER,
    "weapon design": Role.CHARACTER_DESIGNER,
    "color setting": Role.FINISHING,
    "digital paint": Role.FINISHING,
    "photography": Role.PHOTOGRAPHY_DIRECTOR,
    "compositing": Role.PHOTOGRAPHY_DIRECTOR,
    "cgi direction": Role.CGI_DIRECTOR,
    "3d direction": Role.CGI_DIRECTOR,
    "cg direction": Role.CGI_DIRECTOR,
    "3d cgi": Role.CGI_DIRECTOR,
    "sound direction": Role.SOUND_DIRECTOR,
    "original work": Role.ORIGINAL_CREATOR,
    "original novel": Role.ORIGINAL_CREATOR,
    "original manga": Role.ORIGINAL_CREATOR,
    "executive producer": Role.PRODUCER,
    "line producer": Role.PRODUCER,
    "animation producer": Role.PRODUCER,
    "production management": Role.PRODUCTION_MANAGER,
    "setting design": Role.SETTINGS,
    "setting": Role.SETTINGS,
}


# Suffix-based role classification for Japanese roles not in ROLE_MAP.
# Longer suffixes first to avoid partial matches.
# This catches compound roles like "銃器エフェクト作画監督" → ANIMATION_DIRECTOR.
_ROLE_SUFFIX_CLASSIFY: list[tuple[str, Role]] = [
    ("総作画監督", Role.ANIMATION_DIRECTOR),
    ("作画監督", Role.ANIMATION_DIRECTOR),
    ("撮影監督", Role.PHOTOGRAPHY_DIRECTOR),
    ("美術監督", Role.BACKGROUND_ART),
    ("音響監督", Role.SOUND_DIRECTOR),
    ("プロデューサー", Role.PRODUCER),
    ("スーパーバイザー", Role.PRODUCER),
    ("コーディネーター", Role.PRODUCTION_MANAGER),
    ("マネージャー", Role.PRODUCTION_MANAGER),
    ("ディレクター", Role.DIRECTOR),
    ("アニメーター", Role.KEY_ANIMATOR),
    ("コンポジット", Role.PHOTOGRAPHY_DIRECTOR),
    ("モデリング", Role.CGI_DIRECTOR),
    ("エフェクト", Role.PHOTOGRAPHY_DIRECTOR),
    ("イラスト", Role.SPECIAL),
    ("作監", Role.ANIMATION_DIRECTOR),
    ("原画", Role.KEY_ANIMATOR),
    ("作画", Role.KEY_ANIMATOR),
    ("動画", Role.IN_BETWEEN),
    ("仕上げ", Role.FINISHING),
    ("仕上", Role.FINISHING),
    ("検査", Role.FINISHING),
    ("撮影", Role.PHOTOGRAPHY_DIRECTOR),
    ("美術", Role.BACKGROUND_ART),
    ("背景", Role.BACKGROUND_ART),
    ("音楽", Role.MUSIC),
    ("音響", Role.SOUND_DIRECTOR),
    ("脚本", Role.SCREENPLAY),
    ("編集", Role.EDITING),
    ("編曲", Role.MUSIC),
    ("作曲", Role.MUSIC),
    ("演出", Role.EPISODE_DIRECTOR),
    ("監督", Role.DIRECTOR),
    ("監修", Role.DIRECTOR),
    ("設定", Role.SETTINGS),
    ("デザイン", Role.CHARACTER_DESIGNER),
    ("制作", Role.PRODUCTION_MANAGER),
    ("進行", Role.PRODUCTION_MANAGER),
    ("協力", Role.SPECIAL),
    ("補佐", Role.SPECIAL),
    ("担当", Role.SPECIAL),
    ("原案", Role.ORIGINAL_CREATOR),
]


def parse_role(raw: str) -> Role:
    """役職文字列を Role enum にマッピングする.

    エピソード特定の役職（括弧付き）を正しく処理：
    - "Animation Director (ep 10)" → "animation director" → Role.ANIMATION_DIRECTOR
    - "Key Animation (eps 21, 25)" → "key animation" → Role.KEY_ANIMATOR

    言語タグ付きローカライゼーション役職を検出：
    - "Producer (English)" → Role.LOCALIZATION
    - "ADR Script (Italian)" → Role.LOCALIZATION (ADR系はVOICE_ACTORだが言語タグ優先)

    未知の日本語ロールはサフィックスマッチングで分類：
    - "銃器作画監督" → *作画監督 → ANIMATION_DIRECTOR
    - "CG制作進行" → *進行 → PRODUCTION_MANAGER
    """
    import re

    # 言語タグ検出: "(English)", "(Italian)", "(German)" 等
    # エピソード番号 "(ep 10)" や "(OP)"/"(ED)" とは区別する
    _LANG_TAG_RE = re.compile(
        r"\("
        r"(?:English|Italian|German|French|Spanish|Portuguese"
        r"|Brazilian\s+Portuguese|Latin\s+American\s+Spanish"
        r"|Korean|Chinese|Mandarin|Cantonese|Thai|Hungarian"
        r"|Polish|Czech|Dutch|Swedish|Norwegian|Danish|Finnish"
        r"|Russian|Turkish|Arabic|Hebrew|Hindi|Indonesian"
        r"|Malay|Vietnamese|Filipino|Romanian|Greek|Catalan)"
        r"(?:\s*[;,][^)]*)?"  # optional "; 1st dub", "; eps 314-400" etc.
        r"\)",
        re.IGNORECASE,
    )
    if _LANG_TAG_RE.search(raw):
        return Role.LOCALIZATION

    # 括弧とその中身を除去（エピソード番号など）
    # 例: "Animation Director (ep 10)" → "Animation Director"
    cleaned = re.sub(r"\s*\([^)]*\)", "", raw)

    # 正規化: 小文字化、前後の空白削除
    normalized = cleaned.strip().lower()

    result = ROLE_MAP.get(normalized)
    if result is not None:
        return result

    # Suffix-based fallback for Japanese compound roles
    for suffix, role in _ROLE_SUFFIX_CLASSIFY:
        if raw.endswith(suffix) and len(raw) > len(suffix):
            return role

    return Role.SPECIAL


class Person(BaseModel):
    """アニメ業界の人物."""

    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: list[str] = Field(default_factory=list)
    mal_id: int | None = None
    anilist_id: int | None = None
    madb_id: str | None = None  # メディア芸術DB URI
    ann_id: int | None = None  # Anime News Network Encyclopedia ID
    allcinema_id: int | None = None  # allcinema.net person ID

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

    @classmethod
    def from_db_row(cls, row: "PersonRow") -> "Person":
        return cls(
            id=row.id,
            name_ja=row.name_ja,
            name_en=row.name_en,
            aliases=json.loads(row.aliases),
            mal_id=row.mal_id,
            anilist_id=row.anilist_id,
            madb_id=getattr(row, "madb_id", None),
            ann_id=getattr(row, "ann_id", None),
            image_large=getattr(row, "image_large", None),
            image_medium=getattr(row, "image_medium", None),
            image_large_path=getattr(row, "image_large_path", None),
            image_medium_path=getattr(row, "image_medium_path", None),
            date_of_birth=row.date_of_birth,
            age=getattr(row, "age", None),
            gender=getattr(row, "gender", None),
            years_active=json.loads(getattr(row, "years_active", "[]")),
            hometown=getattr(row, "hometown", None),
            blood_type=row.blood_type,
            description=row.description,
            favourites=row.favourites,
            site_url=row.site_url,
        )


class AnimeAnalysis(BaseModel):
    """アニメ作品（分析層 — スコア・表示情報を含まない）.

    DEPRECATED: anime_analysis テーブルへの書き込みは v53 以降フリーズ済み。
    新規分析コードは Anime モデルを使用すること。
    このクラスは既存分析モジュール (skill.py 等) との後方互換のため残存。
    """

    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    quarter: int | None = None  # 1-4, derived from season or start_date month
    episodes: int | None = None
    mal_id: int | None = None
    anilist_id: int | None = None
    madb_id: str | None = None  # メディア芸術DB URI
    ann_id: int | None = None  # Anime News Network Encyclopedia ID
    allcinema_id: int | None = None  # allcinema.net cinema ID

    # 詳細情報（構造的メタデータ）
    format: str | None = None  # TV, MOVIE, OVA, ONA, SPECIAL, MUSIC
    status: str | None = None  # FINISHED, RELEASING, NOT_YET_RELEASED, CANCELLED
    start_date: str | None = None  # YYYY-MM-DD
    end_date: str | None = None
    duration: int | None = None  # 分/話
    original_work_type: str | None = None  # ORIGINAL, MANGA, LIGHT_NOVEL, etc.
    source: str | None = None  # legacy alias for original_work_type

    # v26: K-means 規模分類
    work_type: str | None = None  # 'tv' | 'tanpatsu'
    scale_class: str | None = None  # 'large' | 'medium' | 'small'

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_title(self) -> str:
        return self.title_ja or self.title_en or self.id


class BronzeAnime(BaseModel):
    """アニメ作品（bronze/raw 用モデル）.

    表示・収集補助メタデータを含む。分析層は AnimeAnalysis を使うこと。
    """

    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    quarter: int | None = None
    episodes: int | None = None
    mal_id: int | None = None
    anilist_id: int | None = None
    madb_id: str | None = None
    ann_id: int | None = None
    allcinema_id: int | None = None
    format: str | None = None
    status: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: int | None = None
    original_work_type: str | None = None
    source: str | None = None  # legacy alias for original_work_type
    work_type: str | None = None
    scale_class: str | None = None

    score: float | None = None
    cover_large: str | None = None
    cover_extra_large: str | None = None
    cover_medium: str | None = None
    banner: str | None = None
    cover_large_path: str | None = None
    banner_path: str | None = None
    description: str | None = None
    genres: list[str] = Field(default_factory=list)
    tags: list[dict] = Field(default_factory=list)
    popularity_rank: int | None = None
    favourites: int | None = None
    mean_score: int | None = None
    studios: list[str] = Field(default_factory=list)
    synonyms: list[str] = Field(default_factory=list)
    country_of_origin: str | None = None
    is_licensed: bool | None = None
    is_adult: bool | None = None
    hashtag: str | None = None
    site_url: str | None = None
    trailer_url: str | None = None
    trailer_site: str | None = None
    relations_json: str | None = None
    external_links_json: str | None = None
    rankings_json: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_title(self) -> str:
        return self.title_ja or self.title_en or self.id

    @computed_field  # type: ignore[prop-decorator]
    @property
    def studio(self) -> str | None:
        return self.studios[0] if self.studios else None

    @classmethod
    def from_db_row(cls, row: "AnimeRow") -> "BronzeAnime":
        genres_raw = getattr(row, "genres", "[]")
        tags_raw = getattr(row, "tags", "[]")
        studios_raw = getattr(row, "studios", "[]")
        synonyms_raw = getattr(row, "synonyms", "[]")
        return cls(
            id=row.id,
            title_ja=row.title_ja,
            title_en=row.title_en,
            year=row.year,
            season=row.season,
            quarter=row.quarter,
            episodes=row.episodes,
            mal_id=getattr(row, "mal_id", None),
            anilist_id=getattr(row, "anilist_id", None),
            madb_id=getattr(row, "madb_id", None),
            ann_id=getattr(row, "ann_id", None),
            allcinema_id=getattr(row, "allcinema_id", None),
            score=getattr(row, "score", None),
            cover_large=getattr(row, "cover_large", None),
            cover_extra_large=getattr(row, "cover_extra_large", None),
            cover_medium=getattr(row, "cover_medium", None),
            banner=getattr(row, "banner", None),
            cover_large_path=getattr(row, "cover_large_path", None),
            banner_path=getattr(row, "banner_path", None),
            description=getattr(row, "description", None),
            format=row.format,
            status=row.status,
            start_date=row.start_date,
            end_date=row.end_date,
            duration=row.duration,
            original_work_type=getattr(row, "original_work_type", None) or getattr(row, "source", None),
            source=getattr(row, "original_work_type", None) or getattr(row, "source", None),
            genres=json.loads(genres_raw or "[]"),
            tags=json.loads(tags_raw or "[]"),
            popularity_rank=getattr(row, "popularity_rank", None),
            favourites=getattr(row, "favourites", None),
            studios=json.loads(studios_raw or "[]"),
            synonyms=json.loads(synonyms_raw or "[]"),
            mean_score=getattr(row, "mean_score", None),
            country_of_origin=getattr(row, "country_of_origin", None),
            is_licensed=(
                bool(getattr(row, "is_licensed"))
                if getattr(row, "is_licensed", None) is not None
                else None
            ),
            is_adult=(
                bool(getattr(row, "is_adult"))
                if getattr(row, "is_adult", None) is not None
                else None
            ),
            hashtag=getattr(row, "hashtag", None),
            site_url=getattr(row, "site_url", None),
            trailer_url=getattr(row, "trailer_url", None),
            trailer_site=getattr(row, "trailer_site", None),
            relations_json=getattr(row, "relations_json", None),
            external_links_json=getattr(row, "external_links_json", None),
            rankings_json=getattr(row, "rankings_json", None),
            work_type=row.work_type,
            scale_class=row.scale_class,
        )


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
    evidence_source: str | None = None
    credit_year: int | None = None  # 帰属年（長期作品は話数ごとに異なる）
    credit_quarter: int | None = None  # 帰属四半期 (1-4)

    @classmethod
    def from_db_row(cls, row: "CreditRow") -> "Credit":
        src = row.evidence_source or ""
        return cls(
            person_id=row.person_id,
            anime_id=row.anime_id,
            role=Role(row.role),
            raw_role=row.raw_role or None,
            episode=row.episode,
            source=src,
            evidence_source=src,
            credit_year=row.credit_year,
            credit_quarter=row.credit_quarter,
        )


class ScoreResult(BaseModel):
    """評価結果 — 8-component structural estimation framework.

    Components:
        person_fe: AKM person fixed effect (θ_i) — individual talent isolated from studio
        studio_fe_exposure: Weighted studio FE exposure — studio environment effect
        birank: BiRank score — bipartite PageRank network centrality
        patronage: Patronage premium — director backing value
        dormancy: Dormancy multiplier (0-1) — activity recency penalty
        awcc: AWCC — community bridging (structural holes)
        ndi: NDI — network disruption potential
        iv_score: Integrated Value — CV-optimized weighted combination (primary metric)
    """

    person_id: str
    person_fe: float = 0.0
    studio_fe_exposure: float = 0.0
    birank: float = 0.0
    patronage: float = 0.0
    dormancy: float = 1.0
    awcc: float = 0.0
    ndi: float = 0.0
    iv_score: float = 0.0
    iv_score_historical: float = 0.0
    #: 初期クレジットから推定したキャリア畑（加工データ、pipeline Phase 6 で計算）
    #: 値: 'animator' / 'animator_director' / 'director' /
    #:     'production' / 'technical' / 'multi_track'
    career_track: str = "multi_track"

    @classmethod
    def from_db_row(cls, row: "ScoreRow") -> "ScoreResult":
        return cls(
            person_id=row.person_id,
            person_fe=row.person_fe,
            studio_fe_exposure=row.studio_fe_exposure,
            birank=row.birank,
            patronage=row.patronage,
            dormancy=row.dormancy,
            awcc=row.awcc,
            iv_score=row.iv_score,
            career_track=getattr(row, "career_track", "multi_track"),
        )


class VAScoreResult(BaseModel):
    """声優評価結果 — Voice Actor scoring result.

    Components:
        person_fe: VA AKM person fixed effect
        sd_fe_exposure: Sound director FE exposure
        birank: VA BiRank score
        patronage: VA patronage premium
        trust: VA trust score (SD repeat casting)
        dormancy: Dormancy multiplier (0-1)
        awcc: Community bridging (placeholder)
        va_iv_score: VA Integrated Value (primary metric)
        character_diversity_index: CDI (0-1)
        casting_tier: lead_specialist / versatile / ensemble / newcomer
        replacement_difficulty: RDI (0-1)
    """

    person_id: str
    person_fe: float = 0.0
    sd_fe_exposure: float = 0.0
    birank: float = 0.0
    patronage: float = 0.0
    trust: float = 0.0
    dormancy: float = 1.0
    awcc: float = 0.0
    va_iv_score: float = 0.0
    character_diversity_index: float = 0.0
    main_role_count: int = 0
    supporting_role_count: int = 0
    total_characters: int = 0
    casting_tier: str = "newcomer"
    replacement_difficulty: float = 0.0
