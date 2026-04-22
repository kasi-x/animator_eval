"""SeesaaWiki anime staff credit scraper.

Scrapes per-episode credit data from seesaawiki.jp/w/radioi_34/,
a community-maintained anime staff database with ~8,694 pages.

Data is more granular than AniList/MAL (episode-level credits:
脚本, 演出, 作画監督, 原画, etc.).

Two-tier parsing: regex (primary, ~80%+) with LLM fallback (Ollama/Qwen3)
for non-standard formats.

This scraper only fetches and stores data. Matching with AniList is handled
separately in the pipeline's Entity Resolution phase.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import httpx
import structlog
import typer
from bs4 import BeautifulSoup

from src.models import BronzeAnime, Credit, Person, parse_role
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL, SCRAPE_DELAY_SECONDS

log = structlog.get_logger()

# =============================================================================
# Constants
# =============================================================================

BASE_URL = "https://seesaawiki.jp/w/radioi_34"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DEFAULT_DELAY = SCRAPE_DELAY_SECONDS  # overridable via ANIMETOR_SCRAPE_DELAY
DEFAULT_DATA_DIR = Path("data/seesaawiki")

# Known Japanese role names — used for is_known_role flag, NOT for parse filtering.
# All "RoleName：PersonName" lines are parsed regardless of whether the role is known.
KNOWN_ROLES_JA: set[str] = {
    # Direction
    "監督",
    "総監督",
    "副監督",
    "シリーズディレクター",
    # Script
    "脚本",
    "シリーズ構成",
    "構成",
    # Storyboard / Episode direction
    "絵コンテ",
    "コンテ",
    "演出",
    "演出助手",
    "アシスタントディレクター",
    # Animation supervision
    "作画監督",
    "総作画監督",
    "アクション作画監督",
    "作画監督補佐",
    # Key animation / In-between
    "原画",
    "第二原画",
    "作画",
    "アニメーター",
    "原動画",
    "動画",
    "動画検査",
    "動画チェック",
    "動画チェッカー",
    "動仕",
    # Finishing / Inspection
    "仕上げ",
    "仕上",
    "仕上検査",
    "仕上げ検査",
    "検査",
    "彩色",
    "デジタルペイント",
    "セル検査",
    "色指定検査",
    "モデルチェック",
    "ファイナルチェック",
    # Design
    "キャラクターデザイン",
    "サブキャラクターデザイン",
    "ゲストキャラクターデザイン",
    "ゲストキャラデザイン",
    "デザインワークス",
    "メカニックデザイン",
    "メカニカルデザイン",
    "メカデザイン",
    # Art / Background
    "美術監督",
    "美術",
    "美術補",
    "美術設定",
    "背景",
    "背景美術",
    "色彩設計",
    "色彩設計補佐",
    "色彩設定",
    "色指定",
    # Photography
    "撮影監督",
    "撮影",
    "撮影監督補佐",
    "デジタル撮影",
    "線撮影",
    # Sound
    "音響監督",
    "音楽",
    "音楽監督",
    "音響効果",
    "音響制作",
    "音響制作担当",
    "効果",
    "録音",
    "音楽プロデューサー",
    "音楽制作",
    "音楽協力",
    # Production management
    "プロデューサー",
    "アニメーションプロデューサー",
    "アシスタントプロデューサー",
    "アソシエイトプロデューサー",
    "制作進行",
    "制作進行補佐",
    "制作",
    "制作協力",
    "制作担当",
    "制作デスク",
    "制作マネージャー",
    "設定制作",
    "背景進行",
    "アニメーション制作担当",
    "アニメーション制作協力",
    "アニメーション制作",
    "製作",
    "製作進行",
    # Original work
    "原作",
    "原案",
    # Effects / CG
    "特殊効果",
    "特効",
    "エフェクト",
    "CG",
    "CGI監督",
    "3DCG",
    "CGワークス",
    # Layout / Edit
    "レイアウト",
    "編集",
    "ビデオ編集",
    # Music / Theme songs
    "作曲",
    "作詞",
    "編曲",
    "主題歌",
    "うた",
    # Additional production / support
    "企画",
    "協力",
    "製作担当",
    "設定補佐",
    "宣伝協力",
    "録音助手",
    "録音スタジオ",
    "現像",
    # Extended supervision / assistance
    "メカ作画監督",
    "作画監督協力",
    "美術監督補",
    "美術進行",
    "仕上進行",
    # Digital / CG extended
    "CGエフェクト",
    "デジタル彩色",
    "レタッチ",
    "背景スキャン",
    "ゼログラフ",
    "ネガ編集",
    "記録",
    # Other common
    "アニメーション",
    "WEB担当",
    "調整",
    "脚本協力",
    "担当制作",
    "シナリオ",
    "3Dモデリング",
    "忍術創案",
    "歌",
    "宣伝",
    "設定進行",
    "助監督",
    "選曲",
    "プロデュース",
    "タイミング",
    "美術補佐",
    "美術ボード",
    "メカデザイン協力",
    "番組宣伝",
    "文芸担当",
    # CG / 3D extended
    "モデル制作",
    "モデリング",
    "モーション",
    "コンポジット",
    # Extended direction
    "チーフディレクター",
    "ユニットディレクター",
    "副監督補佐",
    # Extended sound
    "整音",
    "ミキサー",
    "MA",
    "選曲・効果",
    # Misc credits
    "キャラクター著作",
    "プロップデザイン",
    "設定協力",
    "Arrangement",
    "制作事務",
    # Settings / Design extended
    "設定",
    "構成協力",
    # Music extended
    "スコア",
    # High-frequency unknowns from full parse
    "ストーリーボード",
    "コンセプトデザイン",
    "デジタルペイント検査",
    "製作協力",
    "仕上協力",
    "進行",
    "CG制作",
    "CGディレクター",
    "背景制作",
    "演助進行",
    "編集助手",
    "アニメーション絵コンテ",
    "美術担当",
    "作画監督補",
    "オフライン編集",
    "著作",
    "デジタル合成",
    "CGI",
    "仕上チェック",
    "文芸",
    "広報",
    "スペシャルサンクス",
    "スタジオコーディネート",
    "アクション原画",
    "メインアニメーター",
    "レイアウト作画監督",
    "原作協力",
    "美術監修",
    "キャラクター原案",
    "原作イラスト",
    # LLM-flagged common unknowns
    "エグゼクティブプロデューサー",
    "録音調整",
    "制作プロダクション",
    "製作著作",
    "共同制作",
    "バトル監修",
    "美術デザイン",
    "オープニングディレクター",
    "オープニング演出",
    "エンディング演出",
    "イラスト",
    "トレス",
    "タイトル",
    "色彩指定",
    "仕上げ管理",
    "動画チェック補",
    "線撮協力",
    "企画協力",
    # LLM validation round 2
    "製作総指揮",
    "音響演出",
    "基本設定",
    "技術協力",
    "ライティング",
    "アソシエイト",
    "CGスーパーバイザー",
    "音響制作協力",
    "制作デスク補佐",
    "編成",
    "動画編集",
    "管理",
    # LLM validation round 3
    "チーフプロデューサー",
    "プランニングマネージャー",
    "音響プロデューサー",
    "アニメーション監督",
    "色指定補",
    "監修",
    "題字",
    "アシスタント",
    "ラインテスト",
    "トレース",
    "音響",
    "スタジオ",
    "オリジナルサウンドトラック",
    "漫画",
    "制作統括",
    "音楽A&R",
    # LLM validation round 4
    "作画監督チーフ",
    "演出チーフ",
    "タイトルロゴデザイン",
    "キャラクター設計",
    "落語監修",
    "監修協力",
    "SF考証",
    "製作助手",
    "音楽担当",
    "アニメーションキャラクター",
    "メカ・エフェクト作画監督",
    "色指定補佐",
    # === Bulk addition: freq >= 10 unknown roles (manually classified) ===
    # CG / 3D
    "2Dエフェクト",
    "CG制作進行",
    "CG進行",
    "3DCG制作",
    "CGIデザイナー",
    "CGアニメーター",
    "CGモデラー",
    "CGモデリング",
    "CGIディレクター",
    "CGI協力",
    "CGプロデューサー",
    "CG制作協力",
    "CG監督",
    "CGプロダクトマネージャー",
    "CGモデリングチーフ",
    "CG制作プロデューサー",
    "CGIプロデューサー",
    "3DCGI",
    "3D",
    "3D美術",
    "3DLO",
    "3DLOリード",
    "3Dレイアウト",
    "2Dワークス",
    "2Dデザイン",
    "2Dモニターワークス",
    "3D.C.G",
    "CGデザイナー",
    "コンポジットディレクター",
    "背景3Dモデリング",
    "キャラクターモデリング",
    "モデラー",
    "モデリング協力",
    "シニアデジタルアーティスト",
    "デジタルアーティスト",
    # 作画 extended
    "動画作監",
    "原画作監",
    "作監",
    "作監補",
    "キャラ作監",
    "メカ作監",
    "キャラクター作画監督",
    "メカニック作画監督",
    "メカニカル作画監督",
    "アクション作監",
    "動物作画監督",
    "プロップ作画監督",
    "総作画監督補佐",
    "総作画監督補",
    "総作監補佐",
    "エフェクト作画監督",
    "レイアウト監修",
    "作画監修",
    "エピローグ総作画監督",
    "原画作監補佐",
    "作画協力",
    "動画協力",
    "原画協力",
    "リードアニメーター",
    "アニメーションディレクター",
    "アニメーションキャラクターデザイン",
    "キャラクターデザイン協力",
    "サブキャラクターデザイン協力",
    "ゲストデザイン",
    "クリーチャーデザイン",
    "ビジュアルデザイン",
    "原画設計",
    "画コンテ",
    "ストーリィボード",
    "アニメーションストーリーボード",
    "変身原画",
    "アクション作画",
    # 仕上 extended
    "仕上助手",
    "仕上特効",
    "仕上検査補佐",
    "仕上げ協力",
    "ペイント",
    "ペイント検査補佐",
    "彩画",
    "検査補佐",
    "検査協力",
    "デジタル仕上",
    "デジタル仕上げ",
    "デジタル検査",
    "デジタル動画検査",
    "色指定仕上検査",
    "デジタル特効",
    "デジタル動画",
    "動画チーフ",
    "動仕制作",
    # 編集 extended
    "オンライン編集",
    "HD編集",
    "フォーマット編集",
    "デジタル編集",
    # 撮影 extended
    "コンポジット撮影",
    "撮影協力",
    "撮影チーフ",
    "モニターワークス",
    "モニターグラフィック",
    "モニターグラフィックス",
    "スキャン",
    "スキャニング",
    "フィルム",
    # 美術 extended
    "美術監督補佐",
    "美術協力",
    "美術背景",
    "美術統括",
    "背景協力",
    "背景管理",
    "背景監修",
    # 設定 extended
    "設定管理",
    "画面設計",
    "原図",
    "原図整理",
    "原図監修",
    "衣装デザイン",
    "衣装協力",
    "服装設定",
    "キャラクター監修",
    # 音響 extended
    "音楽制作協力",
    "音楽ディレクター",
    "サウンドミキサー",
    # 演出 extended
    "演出補",
    "演出協力",
    "ディレクター",
    # 制作管理 extended
    "制作管理",
    "制作プロデューサー",
    "CG制作プロデューサー",
    "宣伝プロデューサー",
    "制作進行チーフ",
    "文芸進行",
    "モデル協力",
    "モデル進行管理",
    "進行協力",
    # デザイン extended
    "デザイン協力",
    "ロゴデザイン",
    "タイトルデザイン",
    "ゲストキャラクター原案",
    # Special (non-production)
    "セールスプロモーション",
    "ライセンス",
    "ライセンス担当",
    "海外セールス",
    "海外担当",
    "海外ライセンス",
    "配給",
    "配給営業",
    "劇場営業",
    "番組担当",
    "番組協力",
    "取材協力",
    "協力プロダクション",
    "協力スタジオ",
    "編集スタジオ",
    "WEB制作",
    "公式サイト制作",
    "システム管理",
    "宣伝担当",
    "宣伝パブリシティ",
    "操演",
    "実写ディレクター",
    "実写制作協力",
    "キャスティング協力",
    "コーディネーター",
    "フォント協力",
    "プロモーション協力",
    # 音楽 extended
    "挿入歌",
    "ボーカル",
    "Lyrics",
    "Music",
    "Artist",
    "エンドカードイラスト",
    "エンドカード",
    "PV制作",
    "アイキャッチ",
    "予告アニメーション",
    "提供原画",
    "振り付け",
    "楽器監修",
    "主題歌協力",
    # 制作補助
    "補佐",
    "デジタル作画",
    "原案協力",
    "スーパーバイザー",
    "デジタル制作",
    "ツール開発",
    "モーションアドバイザー",
    "ゲストメカニックデザイン",
    # その他（作品固有だが一応認識）
    "児童画",
    "WEBプロモーション",
    "作品提供",
    "メカニカルコーディネーター",
    # === LLM検証 (2026-03-16) で検出された未登録ロール ===
    # 音響系
    "録音監督",
    "音響制作デスク",
    "音響調整",
    # 制作管理系
    "プロダクションマネージャー",
    "プロデューサー補",
    "製作補",
    # 文芸系
    "文芸助手",
    "脚本構成",
    # CG / ポスプロ
    "3DCG監督",
    "デジタル特殊効果",
    "テレシネ",
    # デザイン / 設定系
    "美術設定監修",
    "デザイン監修",
    "色設定",
    "アクセサリーデザイン案",
    # 協力 / プロデュース
    "プロデュース協力",
    "キャスティング",
    # 音楽系
    "演奏",
    "指揮",
    "OPテーマプロデューサー",
    "OPテーマ音楽制作協力",
    # 放送 / メディア
    "NETプロデューサー",
    "データ放送",
    "掲載",
    # 監修
    "時代考証",
    # === UNKNOWN roles (2026-03-16 full scan, freq >= 10) ===
    # アニメーション協力 / 制作協力系
    "アニメーション協力",
    "アニメーション制作統括",
    "アニメーション共同制作",
    "制作事務統括",
    "制作助手",
    "制作補佐",
    "制作チーフ",
    "制作アシスタント",
    "制作広報",
    "制作デスク補",
    # 振付
    "ダンス振付",
    "振付",
    # 動画検査系
    "動画検査補佐",
    "動画管理",
    "動画検査・デジタル修正",
    # 作画監督系
    "作監協力",
    "作監補佐",
    "アクション・エフェクト作画監督",
    "チーフアニメーター",
    "キャラ原図・総作画監督",
    # キーアニメーター
    "キーアニメーター",
    # VFX / デジタル系
    "ビジュアルエフェクト",
    "デジタルワークス",
    "デジタルコンポジット",
    "2DCG",
    "2DVFX",
    "デジタル・ペイント",
    "2DCGデザイン",
    "2Dグラフィックス",
    "2Dグラフィック",
    "2Dデジタル",
    "2Dデザインワークス",
    "2Dモニター",
    "ビデオワークス",
    "VTRワーク",
    "VTR編集",
    "デジタルエフェクト",
    # 演出系
    "アニメーション演出",
    "演出補佐",
    "チーフ演出",
    "シリーズ演出",
    "監督補佐",
    "監督補",
    "監督助手",
    # 3D系
    "3Dアニメーター",
    "3D監督",
    "3DCGディレクター",
    "3DCGIディレクター",
    "3Dディレクター",
    "3DCGプロデューサー",
    "3DCGチーフ",
    "3DCGワークス",
    "3D制作",
    "3Dワーク",
    "3Dマネジメント",
    "3D制作進行",
    "3Dプロデューサー",
    "3DCG制作協力",
    "3DCG協力",
    "3DCGモデリング",
    # 撮影系
    "撮影チーム長",
    "BGスキャニング",
    "撮影管理",
    "撮影制作",
    "撮影監修",
    "副撮影監督",
    "撮影監督補",
    "撮影助手",
    # 編集系
    "VTR編集",
    "オンライン編集担当",
    "HD編集助手",
    "HD編集担当",
    "HD編集スタジオ",
    "HDビデオ編集",
    "HD編集制作担当",
    "HD編集室",
    "HD編集アシスタント",
    "オフライン編集助手",
    "オフライン編集スタジオ",
    "オフライン編集協力",
    "編集協力",
    "編集アシスタント",
    "編集補佐",
    "編集デスク",
    "映像編集",
    "ビデオ編集スタジオ",
    "ビデオ編集デスク",
    "ビデオ編集助手",
    "ビデオフォーマット編集",
    # ポストプロダクション
    "ポストプロダクション",
    "フィルムレコーディング",
    # 音響系
    "アシスタントミキサー",
    "サウンドエディター",
    "サウンド・エディター",
    "サウンドデザイン",
    "サウンド・ミキサー",
    "サウンドミキサー",
    "音響効果助手",
    "効果助手",
    "音響スタジオ",
    "音響監督助手",
    "音響製作",
    "音響助手",
    "音響担当",
    "録音制作",
    "録音演出",
    "録音ディレクター",
    "録音エンジニア",
    "録音アシスタント",
    "録音技術",
    "レコーディングスタジオ",
    "レコーディングエンジニア",
    "アフレコ演出",
    "アフレコスタジオ",
    "収録スタジオ",
    "ダビングスタジオ",
    "オーディオディレクター",
    "MAスタジオ",
    "フォーリー",
    # 音楽系
    "作編曲",
    "音楽製作",
    "音楽制作担当",
    "音楽プロデュース",
    "音楽アシスタントプロデューサー",
    "音楽ディレクター",
    "音楽演出",
    "音楽コーディネーター",
    "A&R",
    "歌唱",
    "ピアノ演奏",
    "Guitar",
    "Bass",
    "作詞・作編曲",
    "作詩・作曲",
    "作詩",
    "サウンドトラック盤",
    "オリジナルサウンドトラック盤",
    "主題歌プロデュース",
    "主題歌制作",
    # プロデューサー系
    "ラインプロデューサー",
    "協力プロデューサー",
    "アニメーション制作プロデューサー",
    "企画プロデューサー",
    "エグゼクティブ・プロデューサー",
    "エクゼクティブプロデューサー",
    "共同エグゼクティブプロデューサー",
    "共同プロデューサー",
    "クリエイティブプロデューサー",
    "コンテンツプロデューサー",
    "COプロデューサー",
    "アソシエイツプロデューサー",
    "製作プロデューサー",
    "企画営業プロデューサー",
    "ゼネラルプロデューサー",
    "総合プロデューサー",
    "アニメーションプロデュース",
    "担当プロデューサー",
    # 制作管理系
    "プロダクションマネージャー",
    "プロダクション・マネージャー",
    "プログラムマネージャー",
    "プロダクトマネージャー",
    "CGプロダクションマネージャー",
    "製作統括",
    "製作管理",
    "製作デスク",
    "製作補",
    # 宣伝・マーケティング系
    "プロモーション",
    "宣伝広報",
    "宣伝プロデュース",
    "宣伝デザイン",
    "広報担当",
    "マーケティングディレクター",
    "プロダクション営業",
    "プロダクションデザイン",
    "セールスプランニング",
    "ライセンシング",
    "販促",
    "販促担当",
    "販売プロモーション",
    # デザイン系
    "モンスターデザイン",
    "セットデザイン",
    "コスチュームデザイン",
    "メインキャラクターデザイン",
    "オリジナルキャラクターデザイン",
    "キャラクターデザイン原案",
    "キャラクターデザイン補佐",
    "キャラクター・デザイン",
    "ゲストキャラクター・デザイン",
    "キャラクター設定",
    "キャラクター設定協力",
    "プロップ・サブキャラクターデザイン",
    "プロップ・衣装デザイン",
    "プロップデザイン協力",
    "サブデザイン",
    "メカ・プロップデザイン",
    "サブメカニックデザイン",
    "メカニック・デザイン",
    "メカニカル・デザイン",
    "デザインワーク",
    "グラフィックデザイン",
    "テクスチャーデザイン",
    "カラーデザイン",
    "モニターデザイン",
    "マテリアルデザイン",
    "メインタイトルデザイン",
    "メインタイトル",
    "サブタイトルデザイン",
    "公式サイトデザイン",
    "公式HPデザイン",
    "イメージボード",
    "ビジュアルアート",
    "ビジュアルコンセプト",
    # 美術系
    "美術助手",
    "美術設定協力",
    "美術設定補佐",
    "背景デザイン",
    "背景担当",
    "背景監督",
    # 設定系
    "設定デザイン",
    "設定担当",
    "設定マネージャー",
    "設定考証",
    "設定考証協力",
    "小物設定",
    "小物デザイン",
    "衣装デザイン協力",
    "衣装コンセプトデザイン・アシスタント",
    # 色彩系
    "色彩設計補",
    "色彩設計/指定検査",
    "色彩設計・指定検査",
    "色彩設定補佐",
    "カラーコーディネイト",
    "カラリスト",
    # テロップ / タイトル
    "テロップ",
    "タイトルリスワーク",
    "タイトル・リスワーク",
    "リスワーク",
    "タイトルロゴ",
    "筆文字",
    "サブタイトル",
    "サブタイトル題字",
    # CG系追加
    "CGラインディレクター",
    "CGテクニカルディレクター",
    "モデリングディレクター",
    "モデリング/リギング",
    "モデリング・リギング",
    "CG制作担当",
    # 文芸系
    "文芸制作",
    "文芸協力",
    "文芸設定",
    "ストーリーエディター",
    "ストーリー原案",
    "チーフライター",
    # web / HP
    "ホームページ制作",
    "公式ホームページ制作",
    "公式HP制作",
    "公式ホームページ",
    "オフィシャルHP制作",
    "オフィシャルサイト",
    "ウェブデザイン",
    "web制作",
    "Web制作",
    "Web担当",
    "WEBデザイン",
    "web・モバイル制作",
    "ホームページ",
    "HP制作",
    "携帯サイト",
    # 版権 / ライセンス
    "版権制作",
    "版権担当",
    "版権管理",
    "ライツ",
    "ライツ担当",
    "ライツプロモート",
    "国内ライセンス",
    "MDライセンス担当",
    # 特殊
    "特別協力",
    "作品協力",
    "資料協力",
    "プラグイン協力",
    "キャスティングマネージャー",
    "エディター",
    "ビデオエディター",
    "アドバイザー",
    "アシスタントエンジニア",
    "テクニカルディレクター",
    # OP/ED/PV系
    "オープニングアニメーション",
    "OP/EDアニメーション",
    "予告編ディレクター",
    "予告編制作",
    "コミック連載",
    "漫画連載",
    "コミカライズ",
    "ノベライズ",
    "コミック",
    "コミック協力",
    "連載協力",
    # 制作委員会（記録用）
    "製作委員会",
    # カード原画（作品固有）
    "アニメオリジナルカード原画",
    # スタジオ関連
    "スタジオ制作担当",
    "演技事務",
    "ストーリーボード",
    "アニメーションストーリーボード",
    # その他
    "デザイン",
    "担当",
    "AP",
    "EED",
    "配信",
    "販売",
    "製作・発売",
    "マーケティング",
    "BGスキャン",
    "撮影 / 編集 / モーショングラフィックス",
    "モーショングラフィックス",
    "パブリシティ",
    "パブリックデザイナー",
    "デジタル管理",
    "システム",
    "企画監修",
    "企画担当",
    "企画制作",
    "企画営業",
    "企画設定協力",
    "シリーズ監修",
    "クリエイティブディレクター",
    "チーフ・ディレクター",
    "アクション監修",
    "アクション監督",
    "アクションディレクター",
    "特技監督",
    "銃器デザイン",
    "武器デザイン",
    "原作監修",
    "原作担当",
    "ロケハン協力",
    "ロケーション協力",
    "車両協力",
    "出版協力",
    "デスク",
    "エフェクト開発",
    "スクリプト開発",
    "アニマティックアーティスト",
    "レーベル",
    "曲名",
    "Illustrator",
    "lyrics",
    "現像所",
    "ラボ・プロデューサー",
    "ラボマネージャー",
    "プロモーター",
    "宣伝・販促",
    "チーフマネージャー",
    "アニメーションツール",
    "放送進行協力",
    # === Round 2 (2026-03-16, post-HTML-fix scan, freq >= 50) ===
    # 表記揺れ
    "第2原画",
    # キャラクター / メカ略称
    "キャラクター",
    "メカニック",
    # CG / 3D追加
    "CGアニメーション",
    "3D.C.G.I",
    "3Dアニメーション",
    "3DCGアニメーション",
    "3DCGアニメーター",
    "CGバックグラウンド",
    "CGI制作",
    "CGアセット",
    "3D背景",
    "CGカメラワーク",
    # ツール / テック
    "ツール・スクリプト開発",
    # 仕上系追加
    "仕上げ検査補佐",
    "彩色チェック",
    "色指定・色検査",
    # デザイン / 背景追加
    "背景レイアウト",
    "サブ・小物",
    # デジタル
    "デジタルワーク",
    "2Dエフェクト&コンポジット",
    # 翻訳
    "翻訳",
    # アイキャッチ
    "アイキャッチ原画",
    "提供バックイラスト",
    # 作品固有だが高頻度
    "プリ☆チャンライブ演出",
    "プリパラライブ演出",
    # freq >= 30 の一般ロール
    "カット制作・モデリング協力",
    "色彩設計/指定検査",
    "ストーリーエディター",
    "レコーディングスタジオ",
    "アーティスト",
    "オンライン編集デスク",
    "プランニングマネジャー",
    "アイキャッチ/オリジナルカード紹介",
    "デザイン協力・アイキャッチ作画",
    "宣伝協力・ダンス",
    "アイキャッチ・ラストカット",
    "オンライン編集スタジオ",
    "特殊効果・スクリプト開発",
    "実写ロケ協力",
    "HDビデオ編集スタジオ",
    "制作事務",
    # 括弧付きロール（作品固有修飾子）
    "脚本協力",
    "脚本協力(忍術創案)",
    # === Round 3 (2026-03-16, post-HTML-fix round 2, freq >= 100) ===
    # 作画系
    "ペイント検査",
    "エフェクト作監",
    "第一原画",
    "キャラ作画監督",
    "総作監",
    "モンスター作画監督",
    "料理作画監督",
    "動検",
    "動画監督",
    "原画作画監督補佐",
    "総作画監督協力",
    "仕上検査協力",
    "BANK原画",
    "BANK管理",
    # 演出・絵コンテ系
    "絵コンテ協力",
    "演助",
    "次回予告",
    # 制作進行系
    "進行補佐",
    "制作話数担当",
    "仕上管理",
    "仕上制作",
    # CG系
    "CGスタッフ",
    "CGアーティスト",
    "CGIアート",
    "CGIチーフデザイナー",
    "CG制作管理",
    "CG協力",
    "3D-CGI",
    "3Dモデラー",
    "3DCGデザイナー",
    "3Dワークス",
    "3DCG制作進行",
    "CG背景",
    "2D撮影",
    # デザイン系
    "モデリングデザイナー",
    "デザイナー",
    "チーフデザイナー",
    "ゲストモンスターデザイン",
    "クリーチャーデザイン協力",
    "サブキャラクター設定",
    "サブ設定協力",
    "小物・エフェクト設定",
    # レイアウト
    "レイアウト協力",
    # VFX
    "VFX",
    # 音楽系
    "エンディング曲",
    "オープニング曲",
    "音効",
    # 仕上系
    "着彩",
    "指定検査",
    # 衣装系
    "衣装設定",
    # 美術系
    "美術ボード協力",
    # イラスト系
    "イラストレーション",
    "エンディングイラスト",
    "アイキャッチイラスト",
    # 撮影系
    "撮影担当",
    "デジタル背景",
    "実写撮影",
    "TAP",
    # 配信系
    "配信担当",
    # === Round 4 (2026-03-16, freq >= 20 体系的追加) ===
    # ---- 動画仕上系 (表記揺れ: 動仕 は登録済み) ----
    "動画仕上",
    "動画仕上げ",
    "動画仕上管理",
    "動画仕上進行",
    "動画仕上協力",
    "動仕協力",
    # ---- レイアウト系 ----
    "レイアウトチェック",
    "レイアウト・チェック",
    "レイアウトチェッカー",
    "レイアウト修正",
    "メインレイアウト",
    "レイアウト作監",
    "レイアウト作画監督補佐",
    # ---- 作画系 追加 ----
    "原画作画監督",
    "チーフ作画監督",
    "特別作画監督",
    "メカ総作画監督",
    "キャラクター総作画監督",
    "アクション・ユニット作画監督",
    "アクション・エフェクト作監",
    "銃器作画監督",
    "ライブパート作画監督",
    "メカニック作監",
    "クリーチャー総作画監督",
    "キャラクター作監",
    "メカニック作画協力",
    "アクション作画監督補",
    "キャラクター作画監督協力",
    "キャラクター作画監督補佐",
    "メカ作画監督協力",
    "メカ修正",
    "総作監補",
    "総作画監修",
    "作画監修協力",
    "メカニックワーク",
    "ED原画",
    "OP原画",
    "BANK原画",
    "後提供イラスト",
    "アクションアニメーター",
    "リードキャラクターアニメーター",
    "若手原画",
    "二原",
    "二原画",
    "二原協力",
    "第1原画",
    "予告原画",
    "予告作画",
    "オープニング原画",
    "エンディング原画",
    "アバン原画",
    "カラー原画",
    "歴代プリキュア原画",
    "動画サポーター",
    "原絵師",
    "絵師頭",
    "割絵",
    # ---- 仕上系 追加 ----
    "仕上検査補",
    "仕上げ検査補佐",
    "仕上げ検査協力",
    "仕上げチーフ",
    "仕上げ助手",
    "仕上処理",
    "彩色検査",
    "色指定助手",
    "色指定検査補佐",
    "セル検",
    "セル検査補佐",
    "検査補",
    "チェック補",
    "ペイント協力",
    "色検査",
    "色彩",
    "動画チェック補佐",
    # ---- デジタル / TP系 ----
    "TP",
    "TP協力",
    "TP修正",
    "DIGITAL.TP",
    "デジタル修正",
    "デジタル処理",
    "デジタルシネママスタリング",
    "デジタルシネマエンジニア",
    "デジタル撮影監督",
    "デジタル撮影&VFX",
    "デジタル進行",
    "デジタル制作管理",
    "デジタル背景",
    "デジタルラボ",
    "デジタルCG制作",
    "コンピューター処理",
    # ---- CG系 追加 ----
    "CG美術",
    "CG作成",
    "CGコンポジター",
    "CGモデリング協力",
    "CGチーフデザイナー",
    "CGチーフ",
    "オリジナルCGモーション制作",
    "CG制作デスク",
    "CGIテクニカルディレクター",
    "CGI制作進行",
    "CGIアシスタントプロデューサー",
    "CGIモデリングディレクター",
    "CGIキャラクターテクニカルディレクター",
    "CGIラインマネージャー",
    "CGIチーフデザイナー",
    "CGラインプロデューサー",
    "CGリードアニメーター",
    "CGレタッチ",
    "CGレイアウト",
    "CGディレクター助手",
    "CGモデリング・開発",
    "CGモデリング・リーダー",
    "CGモデリングディレクター",
    "CG監督補佐・CGデザイナー",
    "CG演出",
    "CG設定制作",
    "CGアセットデザイナー",
    "CGアートディレクター",
    "CGアート",
    "CGデザイン",
    "CGテクニカルデザイナー",
    "CG制作コーディネーター",
    # ---- 3D系 追加 ----
    "3D撮影",
    "3DCGI協力",
    "3D.CGI",
    "3DC.G.I",
    "3CGI",
    "3DCGチーフアニメーター",
    "3DCGスタッフ",
    "3DCG検査",
    "3DCGワーク",
    "3DCGIアニメーター",
    "3DCGIワークス",
    "3DCGI制作",
    "3DCGマネージャー",
    "3DCG撮影",
    "3Dデザイナー",
    "3Dデザイン",
    "3Dコンポジット",
    "3DBG",
    "3Dモデリング協力",
    "3D技術協力",
    "3Dスペシャルエフェクト",
    "3Dデザイン・モデリング",
    "3D協力",
    # ---- 2D系 追加 ----
    "2Dworks",
    "2DWorks",
    "2DCGI",
    "2DCGチーフ",
    "2D背景",
    "2Dワーク",
    "2Dエフェクトチーフ",
    "2Dコンポジット&エフェクト",
    "2Dモニターワーク",
    "2DCGデザイン",
    "2Dデザイン協力",
    "2Dマテリアルデザイン",
    # ---- コンポジット / 撮影系 追加 ----
    "コンポジター",
    "コンポジッター",
    "リードコンポジッター",
    "リードコンポジター",
    "コンポジット制作担当",
    "SCAN",
    "背景スキャニング",
    "背景デジタル処理",
    "撮影進行",
    "背景3D",
    "背景3Dモデリング",
    "背景モデリング",
    "背景チーフ",
    "背景統括",
    "背景補正",
    "背景レタッチ",
    "背景制作進行",
    "背景監督補",
    "背景進行補佐",
    "背景用モデリング",
    # ---- 演出系 追加 ----
    "演出監督",
    "コーナー監督",
    "担当演出",
    "演出サポート",
    "演出統括",
    "OP演出",
    "ED演出",
    "ライブ演出",
    "ライブパート演出",
    "絵コンテ・演出担当",
    "絵コンテ演出",
    "絵コンテ清書",
    "コンテ協力",
    # ---- 美術系 追加 ----
    "美術制作",
    "美術設計",
    "美術補正",
    "美術話数担当",
    "美術3D作業",
    "美術デジタルワークス",
    "美術監督捕",
    "美術・設定監督",
    "各話美術デザイン",
    "美術デザイン協力",
    "美術デザイン補佐",
    "美監補佐",
    "担当美術",
    "話数背景担当",
    # ---- 設定系 追加 ----
    "画面設定",
    "メカ設定",
    "メカ設定協力",
    "銃器設定",
    "設計補佐",
    "設定・資料",
    "設定制作補佐",
    "設定制作進行",
    "設定補",
    "料理設定",
    "服装デザイン",
    # ---- デザイン系 追加 ----
    "サブキャラデザイン",
    "ゲストキャラクターデザイン協力",
    "ちびキャラデザイン",
    "ゲストメカデザイン",
    "ゲストメカニカルデザイン",
    "メカデザイン補佐",
    "メカデザインワークス",
    "メカニックデザイン協力",
    "オープニングデザイン",
    "エンドカードデザイン",
    "フォントデザイン",
    "テロップデザイン",
    "UIデザイン",
    "貼り込み素材デザイン",
    "コンセプトアート",
    "コンセプトデザイン協力",
    "ビジュアルワークス",
    "ビジュアルディレクター",
    "プロップ作監",
    "プロップ監修",
    "プロップ協力",
    "プロップデザイン補佐",
    "ゲストプロップデザイン",
    "スピリットデザイン",
    "クリーチャーモデリング",
    "コスチュームデザイン協力",
    "キャラモデラー",
    # ---- モデリング / リギング ----
    "モデリングスーパーバイザー",
    "モデリングアーティスト",
    "モデリングチーフ",
    "モデリングリード",
    "モデリングコーディネーター",
    "リガー",
    "リギング",
    "リギングスーパーバイザー",
    "セットアップ",
    # ---- モーション ----
    "モーションキャプチャー",
    "モーションデザイナー",
    "モーションディレクター",
    "モーショングラフィック",
    # ---- 監修系 ----
    "エフェクト監修",
    "LO監修",
    "特殊効果監修",
    "脚本監修",
    "シナリオ監修",
    "特効監修",
    # ---- エフェクト ----
    "エフェクトディレクター",
    "エフェクトスーパーバイザー",
    "エフェクトデザイナー",
    "エフェクトアーティスト",
    "エフェクト原案",
    "AE・特効",
    "エアブラシワーク",
    # ---- 制作管理系 追加 ----
    "制作コーディネーター",
    "制作進行アシスタント",
    "制作進行補",
    "制作進行協力",
    "制作担当補佐",
    "制作サポート",
    "制作応援",
    "制作宣伝",
    "進行チーフ",
    "制作チーフ",
    "プロジェクトマネージャー",
    "ラインマネージャー",
    "フロアマネージャー",
    "アシスタントマネージャー",
    "ショットマネージャー",
    "アセットマネージャー",
    "アセットチーフ",
    "アセットテクニカルディレクター",
    "スタッフマネージャー",
    "マネージャー",
    "システムマネージャー",
    "テクニカルスーパーバイザー",
    "テクニカルアドバイザー",
    "テクニカルサポート",
    "ラボコーディネーター",
    "ラボ・マネージメント",
    "ラボ・デスク",
    "システム・マネージメント",
    "システムエンジニア",
    "OP制作進行",
    "ED制作進行",
    "話数制作担当",
    # ---- 音響系 追加 ----
    "録音進行",
    "整音助手",
    # ---- 音楽系 追加 ----
    "エンディングテーマ",
    "コーラス",
    "ベース",
    "ドラム",
    "Drums",
    "トランペット",
    "Horn",
    "Vocal",
    "Strings",
    "作曲&編曲",
    "楽曲コーディネート",
    "楽曲制作協力",
    "楽器作監",
    "音楽録音",
    "音楽宣伝",
    "ミキサー",
    "ミキシングエンジニア",
    "ミックスエンジニア",
    "レコーディング&ミキサー",
    "レコーディング&ミキシングエンジニア",
    "レコーディング&ミックスエンジニア",
    # ---- 声優 / ナレ系 ----
    "ナレーション",
    "パーソナリティ",
    "ダンサー",
    "出演",
    "方言指導",
    "俳優担当",
    # ---- 営業 / プロモ系 追加 ----
    "劇場宣伝",
    "パッケージ営業",
    "海外販売",
    "海外営業",
    "海外渉外",
    "海外プロモート",
    "海外セールス担当",
    "配給統括",
    "配給調整",
    "配信担当",
    "プロモート",
    "宣伝統括",
    "ライセンシング",
    "商品化担当",
    "製作業務",
    "パッケージ制作",
    "パッケージ製造",
    "統括プロデューサー",
    "アドバイザリープロデューサー",
    # ---- 翻訳 / 海外 ----
    "和訳",
    "通訳",
    "翻訳協力",
    "韓国語通訳・翻訳",
    # ---- 実写系 ----
    "実写協力",
    "実写撮影",
    # ---- 色彩追加 ----
    "カラーコーディネーター",
    "カラーマネジメント",
    "ゲスト色彩設計",
    "ゲスト色彩設計・色指定",
    "色設計・検査",
    "色指定・検査・貼込",
    "色指定補助",
    # ---- 編集系 追加 ----
    "ビデオ編集担当",
    "VIDEO編集",
    "フォーマット編集担当",
    "DCPマスタリング",
    # ---- イラスト系 追加 ----
    "アイキャッチ作画",
    "アイキャッチデザイナー",
    "エンドイラスト",
    "提供イラスト",
    "予告イラスト",
    "イラスト協力",
    "次回予告イラスト",
    "劇中イラスト",
    "絵本イラスト",
    "EDカードイラスト",
    "ENDカード原画",
    # ---- OP/ED制作系 ----
    "エンディング制作",
    "エンディングディレクター",
    "OP作画監督",
    "ED作画監督",
    # ---- 脚本系追加 ----
    "脚色",
    "脚本制作",
    "脚本事務",
    "脚本進行",
    "ストーリー",
    "ストーリー/脚本",
    "シリーズ構成協力",
    # ---- レンダリング / テクニカル ----
    "レンダリング",
    "テクスチャペインター",
    "プログラマー",
    "R&D・インフラ開発",
    # ---- Director等英語 ----
    "Director",
    "Producer",
    # ---- 放送 / その他 ----
    "放送",
    "DA",
    "PD",
    "加工担当",
    "ディレクション",
    "アートディレクター",
    "アートディレクション",
    "照明",
    "参考資料",
    "素材協力",
    "法務担当",
    "営業",
    "ライン作画監督",
    "ライン",
    "タイミング撮影",
    "線撮",
    "デザインワーク",  # 表記揺れ（デザインワークス は登録済み）
    # ---- Live2D / VFX ----
    "VFXスーパーバイザー",
    "Live2Dアニメーター",
    # 正規化先のbase roles
    "チーフ",
    "リード",
    # Round 5.5: 最終残りロール
    "オペレーター",
    "エンジニア",
    "調整助手",
    "技術",
    "BG補正",
    "タッチ/ブラシ",
    "データチェック",
    "販売促進",
    "配信ライセンス",
    "統括",
    "VE",
    "HDレコーディング",
    "特殊演技",
    "検査補助",
    "フォト・タイプ",
    "トリック案",
    "動画仕上げ管理",
    "第弐原絵師",
    "藝頭",
    "原絵師",  # 大江戸ロケット固有
    "狂言",
    "予告マンガ",
    "撮影・SFX",
    "特技効果・デザインワークス",
    "一部作曲・原曲",
    "一部原曲・作曲",
    "原画作監補",
    "words",
    # === Round 5 (2026-03-16, 残り一般ロール freq >= 20) ===
    "動画検査協力",
    "リードCGアニメーター",
    "リードモデラー",
    "オンライン編集助手",
    "サブキャラクター",
    "アニメオリジナルカード絵作画",
    "リードアーティスト",
    # メタ — parse_credit_line でスタジオ名→OTHER変換時に使用
    "OTHER",
}

# Substrings to skip — wiki UI/nav text that gets falsely matched as credit lines
_SKIP_SUBSTRINGS: tuple[str, ...] = (
    "このページを編集",
    "新規ページを作成",
    "添付ファイル一覧",
    "コメントをかく",
    "ユーザID",
    "ユーザーID",
    "ログインする",
    "利用規約",
    "先頭へ",
    "最強カード",  # 遊戯王カードゲーム関連ノイズ
)

# Metadata "role" words that match colon pattern but are NOT credits
# e.g. "放送時間：毎週日曜 18:00" or "話数：24"
_METADATA_ROLES: set[str] = {
    "放送時間",
    "放送期間",
    "放送局",
    "放送日",
    "放送枠",
    "話数",
    "全話数",
    "期間",
    "時間",
    "尺",
    "公開日",
    "放映期間",
    "放映局",
    "ジャンル",
    "原作連載",
    "連載",
    "公式サイト",
    "公式HP",
    "URL",
    "備考",
    "注",
    "出典",
    "参考",
    "関連",
    "前番組",
    "次番組",
    "同時ネット",
    "配信開始",
    "配信期間",
    "配信日",
    "公開日",
    "公開時期",
    "劇場公開日",
    "初回上映日",
    "初回放送日",
    "上映時間",
    "上映期間",
    "放送話数",
    "全話",
    "公開",
    "封切",
    "発売日",
    "発売元",
    "発売時期",
    "収録時間",
    "配信話数",
    "配信時期",
    "販売元",
    "発売",
    "国内配信",
    # Show-specific episode naming conventions (not credit roles)
    "Episode",
    "EPISODE",
    "Karte",
    "PHASE",
    "Phase",
    "CHAPTER",
    "Chapter",
    "ROGUE",
    "Mission",
    "FILE",
    "File",
    "stage",
    "Stage",
    "Act",
    "Program",
    "OPERATION",
    "Focus",
    "border",
    "trap",
    "track",
    "Order",
    "Qubit",
    "Steal",
    "Load",
    "CODE",
}

# TV番組コーナー名・冗談系 — credit lineとしてパースしない
_JOKE_CORNER_SUBSTRINGS: tuple[str, ...] = (
    "ポケモン講座",
    "ポケモン川柳",
    "ポケモントリビア",
    "お題のポケモン",
    "ポケモンライブキャスター",
    "ポケモンホロキャスター",
    "ポケモン大百科",
    "ポケモン大検定",
    "バトスピ講座",
    "アプモンデータラボ",
    "アプリ格言",
    "バディポリス講座",
    "バスケットボール講座",
    "トリビアクイズ",
    "ミニコーナー",
    "DUEL",
    "MISSION",
    "こたえ",
    "ヒント",
    "投稿",
    "正解",
    "問題",
    "じゃんけん",
    "選択肢",
    "答え",
    "プププつうしん",
    "カード紹介",
    "今日のカード",
    "今週の一番星",
    "今日のフェアリル",
    "猿田教官の補習授業",
    "スカッとカスカベ",
    # Detected from 300-page quality audit
    "今日の格言",
    "キャプテン今日の格言",
    "FCイナズマイレブン",
    "びっくりラッキーマンボ",
    "Trick",
    "ラッキーマン",
    "ガッツポーズ",
    "次回予告ナレーション",
    # Show-specific episode naming used as section headers
    "どこでもドアかくれんぼ",
    "ジャスティス・カザミ",
    "Re:RISE NEWS",
)

# Regex: episode header detection
_RE_EPISODE = re.compile(r"(?:第|#)\s*(\d+)\s*話?")

# Regex: generic credit line — "何か：名前" or "何か　名前" pattern
# Supports two separator styles:
#   1. Colon: "脚本：山田太郎" or "脚本:山田太郎"
#   2. Space: "脚本　山田太郎" — after NFKC normalization, fullwidth space → half-width
# Role part: sequence of non-separator chars (CJK, Latin, digits, etc.)
# Allows multi-role joined by ・/／  (e.g. "絵コンテ・演出：田中")
_RE_CREDIT_COLON = re.compile(
    r"([^\s：:、,・/／]{2,}(?:\s*[・/／]\s*[^\s：:、,・/／]{2,})*)"
    r"\s*[：:]\s*(.+)"
)
# Space-separated pattern: role must be CJK-heavy to avoid false matches on English text
# e.g. "脚本　山田太郎" (post-NFKC: "脚本 山田太郎")
_RE_CREDIT_SPACE = re.compile(
    r"([\u3000-\u9fff\uf900-\ufaff]{2,}(?:\s*[・/／]\s*[\u3000-\u9fff\uf900-\ufaff]{2,})*)"
    r"\s{1,4}"  # 1-4 spaces (post-NFKC fullwidth → single half-width)
    r"([^\s].+)"
)

# Regex: multi-role split — "絵コンテ・演出" → ["絵コンテ", "演出"]
_RE_MULTI_ROLE = re.compile(r"\s*[・/／]\s*")

# Regex: name separators
_RE_NAME_SPLIT = re.compile(r"[、,　/／]+")


def _split_names_paren_aware(text: str) -> list[str]:
    """Split names on 、,　/／→ but not inside parentheses.

    "藤家和正（第1話〜第21話、第23話〜第33話）、田中太郎"
    → ["藤家和正（第1話〜第21話、第23話〜第33話）", "田中太郎"]

    "知久敦(1話)→内山雄太(2話〜)"
    → ["知久敦(1話)", "内山雄太(2話〜)"]
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch in "(（〔[【":
            depth += 1
            current.append(ch)
        elif ch in ")）〕]】":
            depth = max(0, depth - 1)
            current.append(ch)
        elif depth == 0 and ch in "、,\u3000/／→":
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(ch)
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


# Company/studio name indicators — person names containing these are filtered out
_COMPANY_INDICATORS: tuple[str, ...] = (
    "スタジオ",
    "プロダクション",
    "プロデュース",
    "アニメーション",
    "エンタテインメント",
    "エンターテインメント",
    "フィルム",
    "ピクチャーズ",
    "クリエイティブ",
    "株式会社",
    "有限会社",
    "合同会社",
    "製作委員会",
    "製作所",
    "工房",
    "ミュージック",
    "レコード",
)
# Exact company names that don't match the indicators above
_COMPANY_EXACT: set[str] = {
    "ぴえろ",
    "ガイナックス",
    "GAINAX",
    "XEBEC",
    "ゼクシズ",
    "サンライズ",
    "SUNRISE",
    "ボンズ",
    "BONES",
    "SHAFT",
    "マッドハウス",
    "MADHOUSE",
    "ゴンゾ",
    "GONZO",
    "タツノコプロ",
    "トムス",
    "TMS",
    "OLM",
    "SILVER LINK.",
    "MAPPA",
    "WIT STUDIO",
    "ufotable",
    "CloverWorks",
    "A-1 Pictures",
    "京都アニメーション",
    "東映アニメーション",
    "ぎゃろっぷ",
    "J.C.STAFF",
    "P.A.WORKS",
    # Detected as UNKNOWN roles in full scan (studio section headers)
    "マジックハウス",
    "ジェイ・フィルム",
    "ECHOプロ",
    "ブレインズ・ベース",
    "エー・ライン",
    "美峰デジタルワークス",
    "美峰",
    "はだしぷろ",
    "バップ",
    "デジタルバスキュール",
    "IMAGICA",
    "イマジカ",
}


app = typer.Typer()


# =============================================================================
# URL and ID helpers
# =============================================================================


def decode_euc_jp_url(url: str) -> str:
    """Decode EUC-JP percent-encoded URL components."""
    return unquote(url, encoding="euc-jp", errors="replace")


def make_seesaa_person_id(name_ja: str) -> str:
    """Generate deterministic person ID from normalized name.

    Format: "seesaa:p_{hash12}"
    """
    normalized = unicodedata.normalize("NFKC", name_ja)
    normalized = re.sub(r"\s+", "", normalized)
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"seesaa:p_{hash_hex}"


def make_seesaa_anime_id(title: str) -> str:
    """Generate deterministic anime ID from normalized title.

    Format: "seesaa:{hash12}"
    """
    normalized = unicodedata.normalize("NFKC", title)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"seesaa:{hash_hex}"


# =============================================================================
# Page list enumeration (Phase 1)
# =============================================================================


async def fetch_page_list(
    client: httpx.AsyncClient,
    page_num: int,
) -> list[dict[str, str]]:
    """Fetch one page of the wiki page index.

    Returns list of {"url": str, "title": str}.
    """
    url = f"{BASE_URL}/l/?p={page_num}&order=lastupdate&on_desc=1"
    resp = await client.get(url, headers=HEADERS)
    resp.raise_for_status()
    html = resp.content.decode("euc-jp", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    pages: list[dict[str, str]] = []
    # Page links are in the content area — look for links to wiki pages
    for a_tag in soup.select("a[href]"):
        href = a_tag.get("href", "")
        if not isinstance(href, str):
            continue
        # Wiki page links: /w/radioi_34/d/...
        if "/w/radioi_34/d/" in href:
            title = a_tag.get_text(strip=True)
            if title:
                full_url = (
                    f"https://seesaawiki.jp{href}" if href.startswith("/") else href
                )
                pages.append({"url": full_url, "title": title})

    return pages


async def fetch_all_page_urls(
    client: httpx.AsyncClient,
    delay: float = DEFAULT_DELAY,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> list[dict[str, str]]:
    """Enumerate all wiki pages via the page list index.

    Caches result to data_dir/page_urls.json.
    """
    cache_path = data_dir / "page_urls.json"
    if cache_path.exists():
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        log.info("seesaa_page_list_cached", count=len(cached))
        return cached

    all_pages: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for page_num in range(87):  # ~87 list pages (100 items each)
        try:
            pages = await fetch_page_list(client, page_num)
        except httpx.HTTPStatusError as e:
            log.warning(
                "seesaa_list_page_error", page=page_num, status=e.response.status_code
            )
            break
        except httpx.HTTPError as e:
            log.warning("seesaa_list_page_error", page=page_num, error=str(e))
            break

        if not pages:
            log.info("seesaa_list_page_empty", page=page_num)
            break

        new_count = 0
        for p in pages:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                all_pages.append(p)
                new_count += 1

        log.info("seesaa_list_page", page=page_num, new=new_count, total=len(all_pages))
        await asyncio.sleep(delay)

    # Cache
    data_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(all_pages, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("seesaa_page_list_complete", total=len(all_pages))
    return all_pages


# =============================================================================
# HTML extraction
# =============================================================================


def extract_wiki_body(html: str) -> str:
    """Extract the main wiki content text from a page's HTML.

    Uses <br> → newline replacement instead of get_text(separator="\\n")
    to preserve inline element text (e.g. <span>) on the same line as
    preceding text nodes. This keeps indentation intact for credit lines like:
        撮影　<span>青島彩　戸祭彰悟</span>
    which should become one line: "　　撮影　青島彩　戸祭彰悟"
    """
    soup = BeautifulSoup(html, "html.parser")

    # SeesaaWiki content is in #page-body (main article content)
    body = None
    for selector in ("#page-body", "#page-body-inner", "#content-body", ".wiki-body"):
        body = soup.select_one(selector)
        if body:
            break

    if not body:
        body = soup.find("body")
    if not body:
        return ""

    # Remove elements that leak non-credit content (JS, ads, nav, footer)
    for tag in body.find_all(
        [
            "script",
            "noscript",
            "style",
            "iframe",
            "object",
            "embed",
            "form",
            "input",
            "select",
            "textarea",
            "nav",
            "footer",
            "header",
        ]
    ):
        tag.decompose()
    # Remove ad/tracking divs (SeesaaWiki patterns)
    for div in body.find_all(
        "div",
        class_=lambda c: (
            c
            and any(
                kw in c
                for kw in (
                    "ad-",
                    "ads-",
                    "ad_",
                    "ads_",
                    "seesaa-ad",
                    "page-info",
                    "page-footer",
                    "side-bar",
                    "ad-label",
                    "page-navi",
                    "comment",
                )
            )
        ),
    ):
        div.decompose()
    for div in body.find_all(
        "div",
        id=lambda i: (
            i and any(kw in i for kw in ("ad", "comment", "navi", "footer", "sidebar"))
        ),
    ):
        div.decompose()

    # Replace <br> tags with newlines — this is the actual line separator in wiki markup.
    # get_text(separator="\n") would insert newlines at every tag boundary (including
    # <span>), stripping indentation from inline elements.
    for br in body.find_all("br"):
        br.replace_with("\n")

    return body.get_text()


# =============================================================================
# Regex parser (Tier 1)
# =============================================================================


def _is_company_name(name: str) -> bool:
    """Check if a name looks like a company/studio rather than a person."""
    if any(ind in name for ind in _COMPANY_INDICATORS):
        return True
    # Check exact matches (NFKC-normalized, whitespace-stripped)
    normalized = re.sub(r"\s+", "", name)
    return normalized in _COMPANY_EXACT


# Regex for space-separated Japanese names within a names field
# e.g. "白男川由美 清水理智子 荻原穂美" → 3 names
# Matches: CJK surname(1-3 chars) + optional space + CJK given name(1-4 chars)
_RE_JA_NAME = re.compile(r"[\u3000-\u9fff\uf900-\ufaff]{2,6}")


def _split_space_separated_names(names_text: str) -> list[str]:
    """Split names that are separated by spaces (half-width, after NFKC).

    Handles the common wiki pattern where multiple names appear on one line
    separated by spaces: "白男川由美 清水理智子 荻原穂美"

    Also handles mixed CJK + Latin names: "竹村美音 前田紗希 THU YEN MINH CHAU"
    by splitting on multi-space boundaries when at least 2 CJK names are present.
    """
    # Already handled by _RE_NAME_SPLIT if fullwidth separators present
    tokens = names_text.split()
    if len(tokens) <= 1:
        return [names_text]

    # Check if all tokens look like Japanese names (2-6 CJK characters)
    all_ja_names = all(
        _RE_JA_NAME.fullmatch(t.strip("※*#　 ")) is not None
        for t in tokens
        if len(t.strip("※*#　 ")) >= 2
    )
    if all_ja_names and len([t for t in tokens if len(t.strip("※*#　 ")) >= 2]) >= 2:
        return tokens

    # Mixed CJK + Latin names: split on 2+ spaces (common formatting)
    # e.g. "竹村美音  前田紗希  THU YEN  MINH CHAU"
    multi_space_parts = re.split(r"\s{2,}", names_text.strip())
    if len(multi_space_parts) >= 2:
        ja_count = sum(
            1
            for p in multi_space_parts
            if _RE_JA_NAME.fullmatch(p.strip("※*#　 ")) is not None
        )
        if ja_count >= 2:
            return multi_space_parts

    return [names_text]


# Regex: parenthetical affiliation — "太郎(Xスタジオ)" or "太郎（Xスタジオ）"
_RE_AFFILIATION = re.compile(r"\s*[\(（]([^)）]+)[\)）]\s*$")
# Regex: trailing bracket content — episode ranges "〔3-8,25〕" "[1-3]" etc.
_RE_BRACKET_SUFFIX = re.compile(r"\s*[〔\[【]([^〕\]】]*)[〕\]】]\s*$")

# Episode range notation: 第1話〜第21話, 1-21, #35, etc.
_RE_EP_RANGE = re.compile(r"(?:第\s*)?(\d+)\s*話?\s*[〜~\-ー–]\s*(?:第\s*)?(\d+)\s*話?")
_RE_EP_SINGLE = re.compile(r"(?:第\s*)?(\d+)\s*話")
_RE_EP_HASH = re.compile(r"#\s*(\d+)")
# Open-ended range: "4話〜" or "第4話-" (N onwards, no end specified)
_RE_EP_OPEN = re.compile(r"(?:第\s*)?(\d+)\s*話?\s*[〜~\-ー–]\s*$")


def _parse_episode_ranges(text: str) -> tuple[list[int] | None, int | None]:
    """Parse episode range notation into episode numbers.

    Handles: 第1話〜第21話、第23話〜第33話、第35話 → ([1..21, 23..33, 35], None)
    Also: 〔3-8,25〕 → ([3..8, 25], None), #1〜#21 → ([1..21], None)
    Open-ended: 4話〜 → ([], 4)  — needs total_episodes to expand

    Returns:
        (episodes, episode_from) where:
        - episodes: sorted list of resolved episode numbers, or None
        - episode_from: open-ended start (N話〜), resolved at save time using anime.episodes
    """
    episodes: set[int] = set()
    episode_from: int | None = None
    bare_numbers: list[int] = []  # bare "25" — only include if other ep patterns found
    segments = re.split(r"[、,]", text)
    for seg in segments:
        seg = seg.strip()
        # Try closed range first: 第1話〜第21話, 1-21, etc.
        m = _RE_EP_RANGE.search(seg)
        if m:
            start, end = int(m.group(1)), int(m.group(2))
            if 0 < start <= end <= 9999:
                episodes.update(range(start, end + 1))
            continue
        # Try open-ended range: 4話〜 (N onwards, resolve later with total_episodes)
        m = _RE_EP_OPEN.search(seg)
        if m:
            ep = int(m.group(1))
            if 0 < ep <= 9999:
                episode_from = ep
            continue
        # Try single with 話 or 第: 第35話
        m = _RE_EP_SINGLE.search(seg)
        if m:
            ep = int(m.group(1))
            if 0 < ep <= 9999:
                episodes.add(ep)
            continue
        # Try hash: #35
        m = _RE_EP_HASH.search(seg)
        if m:
            ep = int(m.group(1))
            if 0 < ep <= 9999:
                episodes.add(ep)
            continue
        # Bare number: only valid in context of other episode notation (e.g. "3-8,25")
        m = re.fullmatch(r"\s*(\d+)\s*", seg)
        if m:
            ep = int(m.group(1))
            if 0 < ep <= 9999:
                bare_numbers.append(ep)
    # Include bare numbers only if we already found real episode patterns
    if (episodes or episode_from) and bare_numbers:
        episodes.update(bare_numbers)
    if episodes or episode_from is not None:
        return sorted(episodes) if episodes else [], episode_from
    return None, None


def _clean_name(
    name: str,
) -> tuple[str, str | None, list[int] | None, int | None]:
    """Clean a person name: NFKC normalize, strip whitespace and artifacts.

    Returns (cleaned_name, affiliation, episodes, episode_from).
    - affiliation: studio/company from parenthetical, e.g. "Xスタジオ"
    - episodes: resolved episode list, e.g. [1,2,...,21,35]
    - episode_from: open-ended start (N話〜), resolved at save time
    """
    name = unicodedata.normalize("NFKC", name)
    name = name.strip()

    # Extract trailing parenthetical — could be affiliation or episode numbers
    affiliation: str | None = None
    episodes: list[int] | None = None
    episode_from: int | None = None
    m = _RE_AFFILIATION.search(name)
    if m:
        paren_content = m.group(1).strip()
        # Check if parenthetical looks like a studio/company name
        if _is_company_name(paren_content) or any(
            ind in paren_content for ind in ("スタジオ", "プロ", "社", "制作")
        ):
            affiliation = paren_content
        else:
            # Try parsing as episode ranges
            episodes, episode_from = _parse_episode_ranges(paren_content)
        # Remove the parenthetical either way
        name = name[: m.start()]

    # Strip trailing bracket content: "高寺雄〔3-8,25〕" → "高寺雄"
    bm = _RE_BRACKET_SUFFIX.search(name)
    if bm:
        # Try parsing bracket content as episode ranges too
        if episodes is None and episode_from is None:
            episodes, episode_from = _parse_episode_ranges(bm.group(1))
        name = name[: bm.start()]

    # Strip trailing Vol./MAP markers: "○○ Vol.2" → "○○"
    name = re.sub(r"\s*(?:Vol\.\S*|MAP[\-\s]*\d+)\s*$", "", name, flags=re.IGNORECASE)

    # Remove common artifacts and replacement characters from bad encoding
    name = name.strip("※*#　 \ufffd")
    return name, affiliation, episodes, episode_from


@dataclass
class ParsedCredit:
    """A single parsed credit entry with ordering metadata."""

    role: str  # Raw role name as found in the page
    name: str  # Cleaned person name
    position: (
        int  # 0-based position within this role (0 = first listed = highest authority)
    )
    is_known_role: bool  # Whether this role is in KNOWN_ROLES_JA
    is_company: bool = False  # True if name is a company/studio, not a person
    affiliation: str | None = (
        None  # Studio/company affiliation extracted from parenthetical
    )
    episodes: list[int] | None = (
        None  # Per-name episode ranges: [1,2,...,21,23,...,33,35]
    )
    episode_from: int | None = (
        None  # Open-ended start: N話〜 → expand at save using anime.episodes
    )

    def to_dict(self) -> dict:
        d = {
            "role": self.role,
            "name": self.name,
            "position": self.position,
            "is_known_role": self.is_known_role,
        }
        if self.is_company:
            d["is_company"] = True
        if self.affiliation:
            d["affiliation"] = self.affiliation
        if self.episodes:
            d["episodes"] = self.episodes
        if self.episode_from is not None:
            d["episode_from"] = self.episode_from
        return d


# Regex for tabular format: multiple "Role Name" pairs on one line separated by spaces
# e.g. "美術監督 東潤一         色彩設計 江口亜紗美"
# Detects: known_role + spaces + name + multiple_spaces + known_role
_RE_TABULAR = re.compile(
    r"([\u3000-\u9fff\uf900-\ufaff]{2,})"  # role1 (CJK)
    r"\s+"  # space
    r"([^\s]+(?:\s[^\s]+)?)"  # name1 (1-2 tokens)
    r"\s{2,}"  # 2+ spaces (tabular gap)
    r"([\u3000-\u9fff\uf900-\ufaff]{2,})"  # role2 (CJK)
    r"\s+"  # space
    r"(.+)"  # rest (name2 + possibly more pairs)
)


# Role normalization: suffix-based pattern matching for unknown roles.
# Order matters — longer/more specific suffixes first.
# When an unknown role ends with a known suffix, it is normalized to the base role.
_ROLE_SUFFIX_MAP: list[tuple[str, str]] = [
    # ---- Longer suffixes first (more specific) ----
    ("総作画監督", "総作画監督"),
    ("作画監督", "作画監督"),
    ("撮影監督", "撮影監督"),
    ("美術監督", "美術監督"),
    ("音響監督", "音響監督"),
    ("プロデューサー", "プロデューサー"),
    ("ディレクター", "ディレクター"),
    ("スーパーバイザー", "スーパーバイザー"),
    ("コーディネーター", "コーディネーター"),
    ("アニメーター", "アニメーター"),
    ("マネージャー", "マネージャー"),
    ("アシスタント", "アシスタント"),
    ("コンポジット", "コンポジット"),
    ("モデリング", "モデリング"),
    ("エフェクト", "エフェクト"),
    ("イラスト", "イラスト"),
    # ---- Medium suffixes ----
    ("作監", "作監"),
    ("原画", "原画"),
    ("作画", "作画"),
    ("動画", "動画"),
    ("仕上げ", "仕上げ"),
    ("仕上", "仕上"),
    ("撮影", "撮影"),
    ("美術", "美術"),
    ("背景", "背景"),
    ("音楽", "音楽"),
    ("音響", "音響"),
    ("脚本", "脚本"),
    ("編集", "編集"),
    ("編曲", "編曲"),
    ("作曲", "作曲"),
    ("演出", "演出"),
    ("監督", "監督"),
    ("監修", "監修"),
    ("検査", "検査"),
    ("設定", "設定"),
    ("デザイン", "デザインワークス"),
    ("制作", "制作"),
    ("進行", "制作進行"),
    ("協力", "協力"),
    ("補佐", "補佐"),
    ("担当", "担当"),
    ("チーフ", "チーフ"),
    ("リード", "リード"),
    ("原案", "原案"),
]


def _normalize_role(role: str) -> str:
    """Normalize an unknown role to a known base role via suffix matching.

    Only applies when the role is NOT already in KNOWN_ROLES_JA.
    Returns the normalized role (which should be in KNOWN_ROLES_JA).
    """
    if role in KNOWN_ROLES_JA:
        return role
    for suffix, base_role in _ROLE_SUFFIX_MAP:
        if role.endswith(suffix) and len(role) > len(suffix):
            return base_role
    return role


def _try_parse_tabular(line: str) -> list[ParsedCredit] | None:
    """Try to parse a tabular format line with multiple role-name pairs.

    e.g. "美術監督 東潤一         色彩設計 江口亜紗美"
    Returns None if not tabular format.
    """
    m = _RE_TABULAR.match(line)
    if not m:
        return None
    role1, name1, role2, rest = m.group(1), m.group(2), m.group(3), m.group(4)
    # BOTH roles must be known to avoid false positives where person names
    # in the name field get mistaken for role2 (e.g. "背景進行補佐 竹村美音 前田紗希 ...")
    if role1 not in KNOWN_ROLES_JA or role2 not in KNOWN_ROLES_JA:
        return None

    # Re-construct as separate credit lines and parse each
    results: list[ParsedCredit] = []
    # First pair
    sub1 = parse_credit_line(f"{role1}：{name1.strip()}")
    results.extend(sub1)
    # Remaining text may contain more pairs — recurse via parse_credit_line
    remaining = f"{role2} {rest}"
    # Check if remaining itself is tabular
    sub2 = _try_parse_tabular(remaining)
    if sub2 is not None:
        results.extend(sub2)
    else:
        sub2 = parse_credit_line(f"{role2}：{rest.strip()}")
        results.extend(sub2)
    return results


def parse_credit_line(line: str) -> list[ParsedCredit]:
    """Parse a single credit line into ParsedCredit entries.

    Parses ANY "RoleText：Name1、Name2" pattern (not limited to known roles).
    Position tracks listing order within each role (0 = first = most senior).

    Examples:
    - "脚本：山田太郎" → [ParsedCredit("脚本", "山田太郎", 0, True)]
    - "絵コンテ・演出：田中裕太" → [PC("絵コンテ",...,0,True), PC("演出",...,0,True)]
    - "原画：佐藤、鈴木、田中" → [PC("原画","佐藤",0,..), PC("原画","鈴木",1,..), ...]
    - "制作デスク：山本一" → [ParsedCredit("制作デスク", "山本一", 0, False)]
    """
    raw_line = line
    line = unicodedata.normalize("NFKC", line.strip())

    # Skip lines that are clearly not credit lines
    if not line or line.startswith(("http", "//", "※", "/*", "<!--")):
        return []
    # Skip lines containing HTML tags (leaked through extract_wiki_body)
    if "<" in line and ">" in line:
        return []
    # Skip lines with heavy encoding corruption
    if line.count("\ufffd") >= 2:
        return []
    # Skip lines containing URLs (common in wiki footers)
    if "http://" in line or "https://" in line or "www." in line:
        return []
    # Skip lines that look like JS/JSON fragments (ad scripts)
    if line.startswith(("{", "}", "var ", "function")) or "zoneid" in line.lower():
        return []
    # Skip indented lines — these are name continuations (e.g. "　　　石井里奈　福島光瑠")
    # not "Role　Name" pairs. Check the RAW line before strip().
    stripped_raw = raw_line.lstrip("\n\r")
    if stripped_raw and stripped_raw[0] in (" ", "\u3000", "\t"):
        return []
    # Skip wiki UI/nav text and footnote markers
    if line.startswith(("*", "カテゴリ")):
        return []
    if any(sub in line for sub in _SKIP_SUBSTRINGS):
        return []
    # Skip TV show corner names / joke credits (e.g. "ポケモン川柳：...", "DUEL：...")
    if any(sub in line for sub in _JOKE_CORNER_SUBSTRINGS):
        return []
    # Skip voice actor cast lines: "キャラ名(CV：声優名)" or "キャラ名（CV：..."
    # Also "CV：声優名" (CV as role)
    if "(CV" in line or "（CV" in line or "(cv" in line:
        return []
    if line.startswith("CV") and ("：" in line or ":" in line):
        return []
    # Skip anime title lines: "Re:take", "Re:ゼロ..." etc.
    # and artist credit lines: "SawanoHiroyuki[nZk]:mizuki"
    # Also "(MF文庫J「Re:ゼロ..." — book imprint lines with title fragments
    if line.startswith("Re:") or line.startswith("Re："):
        return []
    if line.startswith("(") or line.startswith("（"):
        return []
    # Skip "Artist[tag]:vocalist" patterns (e.g. "SawanoHiroyuki[nZk]:mizuki")
    # Only when [ appears before : (artist name contains bracket tag)
    bracket_pos = line.find("[")
    colon_pos = min(
        line.find(":") if ":" in line else len(line),
        line.find("：") if "：" in line else len(line),
    )
    if 0 <= bracket_pos < colon_pos:
        return []

    # Skip extremely long lines (wiki table dumps with all episodes concatenated)
    if len(line) > 500:
        return []

    # Try tabular format first: "美術監督 東潤一         色彩設計 江口亜紗美"
    # Only when no colon present (colon lines are unambiguous)
    if "：" not in line and ":" not in line:
        tabular = _try_parse_tabular(line)
        if tabular is not None:
            return tabular

    # Try colon pattern first (higher precision), then space pattern
    match = _RE_CREDIT_COLON.match(line)
    space_match = False
    if not match:
        match = _RE_CREDIT_SPACE.match(line)
        space_match = True
    if not match:
        return []

    role_text = match.group(1)
    names_text = match.group(2)

    # Skip episode title lines: "Karte：00「オペの順番」", "EPISODE:001「...」",
    # "PHASE:01「...」", "Mission：8「...」", etc.
    # Detected when names_text starts with a digit or「 (episode number/title).
    names_stripped = names_text.strip()
    if names_stripped and (
        names_stripped[0].isdigit() or names_stripped.startswith("「")
    ):
        # Also check role_text doesn't look like a real production role
        if role_text not in KNOWN_ROLES_JA:
            return []

    # For space-separated matches, require at least one known role to prevent
    # person-name lines like "伊藤良樹　河村徹" from being misinterpreted.
    # Colon-separated lines ("RoleName：PersonName") are unambiguous and parsed for all roles.
    if space_match:
        candidate_roles = _RE_MULTI_ROLE.split(role_text)
        if not any(r.strip() in KNOWN_ROLES_JA for r in candidate_roles):
            return []

    # Skip metadata lines (e.g. "話数：24", "放送時間：日曜 18:00")
    if role_text in _METADATA_ROLES:
        return []

    # When the "role" text is actually a company/studio name
    # (e.g. "スタジオ・ブーメラン：田中太郎"), mark names with that affiliation.
    # The role is set to "OTHER" (production support at the named studio).
    role_is_company = _is_company_name(role_text)
    company_affiliation: str | None = role_text if role_is_company else None

    # Split multi-role: "絵コンテ・演出" → ["絵コンテ", "演出"]
    # BUT only split if ALL parts are known roles. Otherwise keep as one compound role.
    # e.g. "メカ・エフェクト作画監督" → keep as-is (compound role name)
    #      "脚本・絵コンテ・原画" → split (all parts are known)
    #      "プロダクション・マネージャー" → keep as-is (compound role name)
    raw_parts = _RE_MULTI_ROLE.split(role_text)
    raw_parts = [r.strip() for r in raw_parts if r.strip()]
    # Special case: "作" in compound roles means "作曲" (e.g. "作・編曲" = "作曲・編曲")
    raw_parts = ["作曲" if r == "作" else r for r in raw_parts]
    raw_parts = [r for r in raw_parts if len(r) >= 2]

    if not raw_parts:
        return []

    if len(raw_parts) == 1:
        roles = raw_parts
    elif all(r in KNOWN_ROLES_JA for r in raw_parts):
        # All parts known → genuine multi-role (e.g. "絵コンテ・演出")
        roles = raw_parts
    else:
        # Some parts unknown → compound role name, keep as single role
        roles = [role_text]

    # Normalize unknown roles via suffix pattern matching.
    # e.g. "演奏作画協力" → "協力" (SPECIAL), "Cパート原画" → "原画",
    #      "アルマノクス総作画監督" → "総作画監督"
    roles = [_normalize_role(r) for r in roles]

    # Split names — order is preserved (position 0 = first listed = most senior)
    # Use paren-aware splitter to avoid breaking episode ranges inside ()（）
    raw_names = _split_names_paren_aware(names_text)
    # Further split space-separated Japanese names (e.g. "白男川由美 清水理智子 荻原穂美")
    expanded_names: list[str] = []
    for n in raw_names:
        expanded_names.extend(_split_space_separated_names(n))

    # Clean names and extract affiliations/episodes; mark companies
    cleaned_entries: list[
        tuple[str, str | None, bool, list[int] | None, int | None]
    ] = []
    for n in expanded_names:
        cleaned, affiliation, episodes, episode_from = _clean_name(n)
        if len(cleaned) >= 2:
            is_company = _is_company_name(cleaned)
            cleaned_entries.append(
                (cleaned, affiliation, is_company, episodes, episode_from)
            )

    # When role text is a company name, override role list and set affiliation
    if role_is_company:
        roles = ["OTHER"]
    results: list[ParsedCredit] = []
    for role in roles:
        known = role in KNOWN_ROLES_JA
        pos = 0
        for name, affiliation, is_company, episodes, episode_from in cleaned_entries:
            # Merge affiliation: company_affiliation from role overrides per-name affiliation
            eff_affiliation = company_affiliation or affiliation
            results.append(
                ParsedCredit(
                    role=role,
                    name=name,
                    position=pos,
                    is_known_role=known,
                    is_company=is_company,
                    affiliation=eff_affiliation,
                    episodes=episodes,
                    episode_from=episode_from,
                )
            )
            if not is_company:
                pos += 1  # Position only counts persons, not companies

    return results


# =============================================================================
# LLM委託パース問題 (2026-03-16 品質監査)
#
# 以下の問題は正規表現での対処コストが高く、LLM (Ollama/Qwen3) に委託する:
#
# 1. 非アニメページ（年表・索引ページ）の誤パース
#    例: "2010年代後半" ページで会社名がロールとしてパースされる
#    理由: ページ種別の判定は正規表現では困難。タイトルパターンの網羅が非現実的
#    対処: parse_with_llm() で unknown role 率 >50% の場合にフォールバック済み
#
# 2. キャスト行の一部が正規表現のセクションヘッダー検出を回避するケース
#    例: ヘッダーなしでキャラ名：声優名が並ぶページ
#    理由: セクションヘッダーがない場合、キャラ名とロール名の区別は文脈依存
#    対処: validate_parse_with_llm() で検出可能。reparse --llm で修正される
#
# これらはreparse時に --llm フラグで Tier2 LLMパーサーにフォールバックされる。
# 正規表現パーサー単体では対応不要（コスト対効果が見合わない）。
# =============================================================================

# Cast/voice actor section headers — lines after these are character:VA, not staff credits
_CAST_SECTION_HEADERS: tuple[str, ...] = (
    "キャスト",
    "声の出演",
    "出演",
    "出演者",
    "CAST",
    "Cast",
    "ゲスト声優",
    "ゲストキャスト",
    "レギュラーキャスト",
)


def _is_cast_section_header(line: str) -> bool:
    """Check if a line marks the start of a cast (voice actor) section."""
    normalized = unicodedata.normalize("NFKC", line.strip())
    # Exact match or header-like pattern (e.g. "■キャスト", "【キャスト】")
    cleaned = normalized.lstrip("■□●○★☆▼▲◆◇【〔「『").rstrip("】〕」』")
    return cleaned in _CAST_SECTION_HEADERS


def _is_staff_section_header(line: str) -> bool:
    """Check if a line marks the start of a staff section (resumes credit parsing)."""
    normalized = unicodedata.normalize("NFKC", line.strip())
    cleaned = normalized.lstrip("■□●○★☆▼▲◆◇【〔「『").rstrip("】〕」』")
    return cleaned in ("スタッフ", "STAFF", "Staff", "メインスタッフ", "制作スタッフ")


def parse_episodes(body_text: str) -> list[dict]:
    """Parse wiki body text into per-episode credit records.

    Returns list of:
        {"episode": int|None, "credits": [ParsedCredit, ...]}
    """
    lines = body_text.split("\n")
    episodes: list[dict] = []
    current_episode: int | None = None
    current_credits: list[ParsedCredit] = []
    in_cast_section = False

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            continue

        # Track cast vs staff sections
        if _is_cast_section_header(stripped):
            in_cast_section = True
            continue
        if _is_staff_section_header(stripped):
            in_cast_section = False
            continue

        # Check for episode header (resets cast section flag — new episode)
        ep_match = _RE_EPISODE.search(stripped)
        if ep_match:
            # Save previous episode
            if current_credits:
                episodes.append(
                    {"episode": current_episode, "credits": current_credits}
                )
            current_episode = int(ep_match.group(1))
            current_credits = []
            in_cast_section = False
            continue

        # Skip lines in cast sections (character:VA, not staff credits)
        if in_cast_section:
            continue

        # Pass raw_line (not stripped) so indentation check works
        credits = parse_credit_line(raw_line)
        if credits:
            current_credits.extend(credits)

    # Save last episode
    if current_credits:
        episodes.append({"episode": current_episode, "credits": current_credits})

    return episodes


def parse_series_staff(body_text: str) -> list[ParsedCredit]:
    """Parse series-level staff credits (before any episode headers).

    Returns: [ParsedCredit, ...]
    """
    lines = body_text.split("\n")
    series_credits: list[ParsedCredit] = []
    in_cast_section = False

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            continue

        # Stop at first episode header
        if _RE_EPISODE.search(stripped):
            break

        # Track cast vs staff sections
        if _is_cast_section_header(stripped):
            in_cast_section = True
            continue
        if _is_staff_section_header(stripped):
            in_cast_section = False
            continue
        if in_cast_section:
            continue

        # Pass raw_line (not stripped) so indentation check works
        credits = parse_credit_line(raw_line)
        if credits:
            series_credits.extend(credits)

    return series_credits


# =============================================================================
# LLM parser (Tier 2 — fallback)
# =============================================================================


def check_llm_available() -> bool:
    """Check if Ollama endpoint is available."""
    from src.utils.config import LLM_BASE_URL, LLM_TIMEOUT

    try:
        ollama_base = LLM_BASE_URL.replace("/v1", "")
        response = httpx.get(f"{ollama_base}/api/tags", timeout=LLM_TIMEOUT)
        return response.status_code == 200
    except Exception as e:
        log.info("llm_not_available", error=str(e))
        return False


def build_extraction_prompt(body_text: str) -> str:
    """Build the LLM prompt for credit extraction."""
    # Truncate to 4000 chars to fit context
    truncated = body_text[:4000]
    return f"""以下のアニメスタッフクレジットテキストから、スタッフ情報をJSON配列で抽出してください。

各要素のフォーマット:
{{"episode": 話数(数字またはnull), "role": "役職名", "name": "人名"}}

役職は原文のまま（脚本、演出、作画監督、原画、動画、etc.）。
エピソード番号がない場合はnullにしてください。

テキスト:
{truncated}

JSON配列のみを出力してください:"""


def parse_with_llm(body_text: str) -> list[dict]:
    """Extract credits using Ollama LLM.

    Returns list of {"episode": int|None, "role": str, "name": str}.
    Gracefully returns empty list if LLM is unavailable.
    """
    from src.utils.config import (
        LLM_BASE_URL,
        LLM_MAX_TOKENS,
        LLM_MODEL_NAME,
        LLM_TEMPERATURE,
        LLM_TIMEOUT,
    )

    prompt = build_extraction_prompt(body_text)
    ollama_base = LLM_BASE_URL.replace("/v1", "")

    try:
        response = httpx.post(
            f"{ollama_base}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": LLM_TEMPERATURE,
                    "num_predict": LLM_MAX_TOKENS,
                },
            },
            timeout=LLM_TIMEOUT * 3,  # LLM extraction needs more time
        )
        response.raise_for_status()
        result = response.json()

        answer = result.get("response", "").strip()
        if not answer:
            answer = result.get("thinking", "").strip()

        # Parse JSON from response (handle markdown code fences)
        json_text = answer
        # Remove ```json ... ``` fences
        fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", json_text, re.DOTALL)
        if fence_match:
            json_text = fence_match.group(1)

        # Try to find JSON array in text
        bracket_match = re.search(r"\[.*\]", json_text, re.DOTALL)
        if bracket_match:
            json_text = bracket_match.group(0)

        records = json.loads(json_text)
        if not isinstance(records, list):
            log.warning("llm_invalid_response", type=type(records).__name__)
            return []

        # Validate and clean records
        valid: list[dict] = []
        for r in records:
            if not isinstance(r, dict):
                continue
            role = r.get("role", "")
            name = r.get("name", "")
            if role and name and len(name) >= 2:
                valid.append(
                    {
                        "episode": r.get("episode"),
                        "role": str(role),
                        "name": _clean_name(str(name)),
                    }
                )

        log.info("llm_extraction", raw_count=len(records), valid_count=len(valid))
        return valid

    except (httpx.HTTPError, httpx.TimeoutException) as e:
        log.warning("llm_extraction_error", error=str(e))
        return []
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("llm_json_parse_error", error=str(e))
        return []


def validate_parse_with_llm(
    body_text: str,
    parsed_credits: list[ParsedCredit],
) -> dict:
    """Ask LLM to validate whether regex-parsed credits look correct.

    Called when >50% of parsed credits have unknown roles —
    might indicate the regex is matching non-credit lines.

    Returns:
        {"should_halt": bool, "reason": str}
    """
    from src.utils.config import (
        LLM_BASE_URL,
        LLM_MODEL_NAME,
        LLM_TEMPERATURE,
        LLM_TIMEOUT,
    )

    # Build sample of what regex parsed
    sample_lines: list[str] = []
    for c in parsed_credits[:20]:
        tag = "KNOWN" if c.is_known_role else "UNKNOWN"
        sample_lines.append(f"  [{tag}] {c.role}：{c.name} (pos={c.position})")
    sample = "\n".join(sample_lines)

    # Truncate body for context
    truncated_body = body_text[:2000]

    prompt = f"""/no_think
以下はアニメスタッフクレジットページのテキストと、正規表現パーサーの出力です。

パーサーの出力を検証してください:
- [KNOWN] = 既知の役職名として認識済み
- [UNKNOWN] = 未知の役職名（新しい役職かパースミスか）

パーサー出力:
{sample}

元テキスト（冒頭2000文字）:
{truncated_body}

質問: パーサーの出力は正しいですか？
- [UNKNOWN]の項目は本当にスタッフクレジットの役職名ですか？
- パースミス（非クレジット行を誤ってパースした等）はありますか？
- 人名が役職としてパースされていませんか？

以下のJSON形式のみで回答してください。説明不要:
{{"correct": true/false, "reason": "問題点の簡潔な説明（問題なければ'OK'）"}}"""

    ollama_base = LLM_BASE_URL.replace("/v1", "")
    try:
        response = httpx.post(
            f"{ollama_base}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": LLM_TEMPERATURE,
                    "num_predict": 2000,
                },
            },
            timeout=LLM_TIMEOUT * 5,
        )
        response.raise_for_status()
        result = response.json()

        answer = result.get("response", "").strip()
        if not answer:
            answer = result.get("thinking", "").strip()

        # Qwen3 thinking mode: JSON may be at the very end after reasoning.
        # Combine both fields and search for the last JSON object.
        full_text = (
            result.get("thinking", "") + "\n" + result.get("response", "")
        ).strip()
        if not answer:
            answer = full_text

        # Try to extract JSON — use findall and take the LAST match (most likely the answer)
        json_matches = re.findall(r"\{[^{}]*\}", full_text)
        json_match = None
        for candidate in reversed(json_matches):
            if "correct" in candidate:
                json_match = candidate
                break
        if not json_match and json_matches:
            json_match = json_matches[-1]
        if json_match:
            validation = json.loads(json_match)
            is_correct = validation.get("correct", True)
            reason = validation.get("reason", "")

            log.info(
                "llm_validation",
                correct=is_correct,
                reason=reason[:200],
            )

            return {
                "should_halt": not is_correct,
                "reason": reason,
                "llm_raw": answer,
            }

        # Can't parse response — don't halt (be conservative)
        log.warning("llm_validation_unparseable", answer=answer[:200])
        return {"should_halt": False, "reason": "LLM response unparseable"}

    except (httpx.HTTPError, httpx.TimeoutException, json.JSONDecodeError) as e:
        log.warning("llm_validation_error", error=str(e))
        return {"should_halt": False, "reason": f"LLM error: {e}"}


# =============================================================================
# Local data saving (raw HTML + parsed intermediate)
# =============================================================================


def _safe_filename(title: str) -> str:
    """Convert a page title to a safe filename (hash-based to avoid encoding issues)."""
    normalized = unicodedata.normalize("NFKC", title)
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
    # Keep a truncated title prefix for human readability
    safe = re.sub(r"[^\w\-]", "_", normalized)[:60]
    return f"{safe}_{hash_hex}"


def save_raw_html(data_dir: Path, title: str, html: str) -> Path:
    """Save raw HTML to data_dir/raw/{safe_filename}.html."""
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{_safe_filename(title)}.html"
    path.write_text(html, encoding="utf-8")
    return path


def save_parsed_intermediate(
    data_dir: Path,
    title: str,
    anime_id: str,
    body_text: str,
    episodes: list[dict],
    series_staff: list[ParsedCredit],
    llm_records: list[dict],
    parser_used: str,
    llm_validation: dict | None = None,
) -> Path:
    """Save parsed intermediate data to data_dir/parsed/{safe_filename}.json.

    Stores body text, regex-parsed episodes, series staff, LLM records,
    and LLM validation results for later verification.
    """
    parsed_dir = data_dir / "parsed"
    parsed_dir.mkdir(parents=True, exist_ok=True)

    intermediate = {
        "title": title,
        "anime_id": anime_id,
        "parser_used": parser_used,  # "regex", "llm", "regex+llm"
        "body_text_length": len(body_text),
        "body_text": body_text,
        "llm_validation": llm_validation,
        "episodes": [
            {
                "episode": ep["episode"],
                "credits": [c.to_dict() for c in ep["credits"]],
            }
            for ep in episodes
        ],
        "series_staff": [c.to_dict() for c in series_staff],
        "llm_records": llm_records,
    }

    path = parsed_dir / f"{_safe_filename(title)}.json"
    path.write_text(
        json.dumps(intermediate, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


# =============================================================================
# Checkpoint
# =============================================================================


def save_checkpoint(
    data_dir: Path,
    processed_urls: list[str],
    stats: dict,
) -> None:
    """Save scraping progress to checkpoint file."""
    data_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = data_dir / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {"processed_urls": processed_urls, "stats": stats},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def load_checkpoint(data_dir: Path) -> tuple[set[str], dict]:
    """Load checkpoint, returning (processed_urls_set, stats)."""
    checkpoint_path = data_dir / "checkpoint.json"
    if not checkpoint_path.exists():
        return set(), {}
    try:
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return set(data.get("processed_urls", [])), data.get("stats", {})
    except (json.JSONDecodeError, KeyError):
        return set(), {}


# =============================================================================
# Orchestrator
# =============================================================================


async def scrape_seesaawiki(
    conn,
    data_dir: Path | None = None,
    max_pages: int = 0,
    checkpoint_interval: int = SCRAPE_CHECKPOINT_INTERVAL,
    delay: float = DEFAULT_DELAY,
    use_llm: bool = True,
    fresh: bool = False,
    list_only: bool = False,
    fetch_only: bool = False,
) -> dict:
    """Scrape credit data from SeesaaWiki.

    1. Enumerate all wiki pages
    2. Fetch each page and parse credits (regex + optional LLM fallback)
    3. Save to DB

    Args:
        conn: SQLite connection
        data_dir: Data directory for caches/checkpoints
        max_pages: Maximum pages to process (0 = all)
        checkpoint_interval: How often to save checkpoint (pages)
        delay: Seconds between requests
        use_llm: Whether to use LLM fallback for unparseable pages
        fresh: Ignore existing checkpoint
        list_only: Only enumerate pages, don't scrape

    Returns:
        Statistics dict
    """
    from src.database import (
        insert_credit,
        update_data_source,
        upsert_person,
        upsert_src_seesaawiki_anime,
    )
    from src.etl.integrate import upsert_canonical_anime

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    stats = {
        "pages_processed": 0,
        "pages_skipped": 0,
        "pages_failed": 0,
        "anime_created": 0,
        "credits_created": 0,
        "persons_created": 0,
        "llm_fallbacks": 0,
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        # Phase 1: Enumerate pages
        all_pages = await fetch_all_page_urls(client, delay=delay, data_dir=data_dir)

        if list_only:
            log.info("seesaa_list_only", total_pages=len(all_pages))
            stats["pages_processed"] = len(all_pages)
            return stats

        if max_pages > 0:
            all_pages = all_pages[:max_pages]

        # Load checkpoint
        if fresh:
            processed_urls: set[str] = set()
        else:
            processed_urls, saved_stats = load_checkpoint(data_dir)
            if saved_stats:
                stats.update(saved_stats)
            if processed_urls:
                log.info("seesaa_checkpoint_loaded", processed=len(processed_urls))

        # Check LLM availability once
        llm_available = use_llm and check_llm_available()
        if use_llm and not llm_available:
            log.warning("seesaa_llm_unavailable", mode="regex_only")

        person_cache: dict[str, Person] = {}  # name -> Person (dedup)
        processed_list: list[str] = list(processed_urls)

        # Phase 2 & 3: Fetch, parse, and save
        for idx, page_info in enumerate(all_pages):
            page_url = page_info["url"]
            page_title = page_info["title"]

            if page_url in processed_urls:
                stats["pages_skipped"] += 1
                continue

            try:
                resp = await client.get(page_url, headers=HEADERS)
                resp.raise_for_status()
                html = resp.content.decode("euc-jp", errors="replace")
            except (httpx.HTTPError, httpx.TimeoutException) as e:
                log.warning("seesaa_fetch_error", url=page_url, error=str(e))
                stats["pages_failed"] += 1
                await asyncio.sleep(delay)
                continue

            # Save raw HTML locally (always — for later reparse)
            save_raw_html(data_dir, page_title, html)

            if fetch_only:
                processed_urls.add(page_url)
                processed_list.append(page_url)
                stats["pages_processed"] += 1
                if stats["pages_processed"] % checkpoint_interval == 0:
                    save_checkpoint(data_dir, processed_list, stats)
                    log.info(
                        "seesaa_fetch_checkpoint",
                        progress=f"{idx + 1}/{len(all_pages)}",
                        pages=stats["pages_processed"],
                    )
                await asyncio.sleep(delay)
                continue

            body_text = extract_wiki_body(html)

            # Parse with regex (Tier 1)
            episodes = parse_episodes(body_text)
            series_staff = parse_series_staff(body_text)

            # Count total regex credits
            all_credits: list[ParsedCredit] = []
            for ep in episodes:
                all_credits.extend(ep["credits"])
            all_credits.extend(series_staff)
            regex_credits = len(all_credits)

            # Count unknown-role credits for LLM validation
            unknown_credits = [c for c in all_credits if not c.is_known_role]

            # LLM fallback (Tier 2): if regex yields <3 credits and text is substantial
            llm_records: list[dict] = []
            if llm_available and regex_credits < 3 and len(body_text) > 500:
                llm_records = parse_with_llm(body_text)
                if llm_records:
                    stats["llm_fallbacks"] += 1

            # LLM validation (Tier 1.5): validate unknown-role regex results
            # If many credits have unknown roles, ask LLM to verify the parse
            llm_validation: dict | None = None
            if llm_available and unknown_credits and regex_credits >= 3:
                unknown_ratio = len(unknown_credits) / regex_credits
                if unknown_ratio > 0.5:
                    llm_validation = validate_parse_with_llm(
                        body_text,
                        all_credits,
                    )
                    if llm_validation and llm_validation.get("should_halt"):
                        # LLM says the parse looks wrong — halt
                        log.error(
                            "seesaa_parse_validation_failed",
                            url=page_url,
                            title=page_title,
                            unknown_ratio=f"{unknown_ratio:.0%}",
                            llm_reason=llm_validation.get("reason", ""),
                            sample_unknowns=[
                                f"{c.role}:{c.name}" for c in unknown_credits[:5]
                            ],
                        )
                        # Save what we have so far
                        conn.commit()
                        save_checkpoint(data_dir, processed_list, stats)
                        log.error(
                            "seesaa_halted",
                            message=(
                                "Parse validation failed — regex produced mostly "
                                "unknown roles and LLM flagged the result as incorrect. "
                                "Check the page manually and update the parser."
                            ),
                            page_url=page_url,
                        )
                        sys.exit(1)

            # Generate anime ID
            anime_id = make_seesaa_anime_id(page_title)

            # Determine which parser was used
            parser_used = "regex"
            if llm_records and regex_credits < 3:
                parser_used = "llm" if regex_credits == 0 else "regex+llm"

            # Save parsed intermediate data for verification
            save_parsed_intermediate(
                data_dir,
                page_title,
                anime_id,
                body_text,
                episodes,
                series_staff,
                llm_records,
                parser_used,
                llm_validation=llm_validation,
            )

            # Upsert anime
            anime = BronzeAnime(
                id=anime_id,
                title_ja=page_title,
            )
            upsert_canonical_anime(conn, anime, evidence_source="seesaawiki")
            upsert_src_seesaawiki_anime(conn, anime_id, page_title, None, None)
            stats["anime_created"] += 1

            # Derive total episode count from parsed data (for open-ended ranges)
            total_episodes = (
                max((ep_data["episode"] or 0 for ep_data in episodes), default=0)
                or None
            )

            # Save regex-parsed credits
            for ep_data in episodes:
                episode_num = ep_data["episode"]
                for credit in ep_data["credits"]:
                    _save_credit(
                        conn,
                        person_cache,
                        stats,
                        anime_id,
                        credit,
                        episode=episode_num,
                        upsert_person=upsert_person,
                        insert_credit=insert_credit,
                        total_episodes=total_episodes,
                    )

            # Save series-level staff
            for credit in series_staff:
                _save_credit(
                    conn,
                    person_cache,
                    stats,
                    anime_id,
                    credit,
                    episode=None,
                    upsert_person=upsert_person,
                    insert_credit=insert_credit,
                    total_episodes=total_episodes,
                )

            # Save LLM-parsed credits (only if regex didn't find much)
            if llm_records and regex_credits < 3:
                for record in llm_records:
                    pc = ParsedCredit(
                        role=record["role"],
                        name=record["name"],
                        position=0,  # LLM doesn't preserve ordering
                        is_known_role=record["role"] in KNOWN_ROLES_JA,
                    )
                    _save_credit(
                        conn,
                        person_cache,
                        stats,
                        anime_id,
                        pc,
                        episode=record.get("episode"),
                        upsert_person=upsert_person,
                        insert_credit=insert_credit,
                        total_episodes=total_episodes,
                    )

            processed_urls.add(page_url)
            processed_list.append(page_url)
            stats["pages_processed"] += 1

            # Checkpoint
            if stats["pages_processed"] % checkpoint_interval == 0:
                conn.commit()
                save_checkpoint(data_dir, processed_list, stats)
                log.info(
                    "seesaa_checkpoint",
                    progress=f"{idx + 1}/{len(all_pages)}",
                    **stats,
                )

            await asyncio.sleep(delay)

    # Final commit
    conn.commit()
    update_data_source(conn, "seesaawiki", stats["credits_created"])
    conn.commit()

    # Final checkpoint
    save_checkpoint(data_dir, processed_list, stats)

    log.info("seesaa_scrape_complete", source="seesaawiki", **stats)
    return stats


def _save_credit(
    conn,
    person_cache: dict[str, Person],
    stats: dict,
    anime_id: str,
    parsed: ParsedCredit,
    episode: int | None,
    upsert_person,
    insert_credit,
    total_episodes: int | None = None,
) -> None:
    """Save a single credit record to the database.

    Handles three cases:
    - Person credit → upsert person + insert credit
    - Company/studio credit (is_company=True) → record as anime_studio relationship
    - Person with affiliation → insert credit + record affiliation

    When parsed.episode_from is set (open-ended range like "4話〜"),
    expands to episode_from..total_episodes using the anime's episode count.
    """
    from src.database import insert_person_affiliation, insert_src_seesaawiki_credit

    if parsed.is_company:
        # Store as studio involvement, not person credit
        _save_studio_credit(conn, anime_id, parsed.name, parsed.role, stats)
        insert_src_seesaawiki_credit(
            conn,
            anime_id,
            parsed.name,
            parsed.role,
            parsed.role,
            affiliation=None,
            is_company=True,
        )
        return

    if parsed.name not in person_cache:
        person_id = make_seesaa_person_id(parsed.name)
        person = Person(
            id=person_id,
            name_ja=parsed.name,
        )
        upsert_person(conn, person)
        person_cache[parsed.name] = person
        stats["persons_created"] += 1
    else:
        person = person_cache[parsed.name]

    role = parse_role(parsed.role)

    # Resolve episode list: merge explicit episodes + open-ended range
    resolved_episodes: list[int] = list(parsed.episodes) if parsed.episodes else []
    if parsed.episode_from is not None and total_episodes:
        resolved_episodes.extend(range(parsed.episode_from, total_episodes + 1))
    # Deduplicate and sort
    if resolved_episodes:
        resolved_episodes = sorted(set(resolved_episodes))

    # If the credit has per-name episode ranges, expand into one credit per episode
    if resolved_episodes:
        for ep in resolved_episodes:
            credit = Credit(
                person_id=person.id,
                anime_id=anime_id,
                role=role,
                raw_role=parsed.role,
                episode=ep,
                source="seesaawiki",
            )
            insert_credit(conn, credit)
            insert_src_seesaawiki_credit(
                conn,
                anime_id,
                parsed.name,
                str(role),
                parsed.role,
                episode=ep,
                affiliation=parsed.affiliation,
            )
            stats["credits_created"] += 1
    else:
        credit = Credit(
            person_id=person.id,
            anime_id=anime_id,
            role=role,
            raw_role=parsed.role,
            episode=episode,
            source="seesaawiki",
        )
        insert_credit(conn, credit)
        insert_src_seesaawiki_credit(
            conn,
            anime_id,
            parsed.name,
            str(role),
            parsed.role,
            episode=episode,
            affiliation=parsed.affiliation,
        )
        stats["credits_created"] += 1

    # Record studio affiliation if present (e.g. "太郎(Xスタジオ)")
    if parsed.affiliation:
        aff_key = (person.id, anime_id, parsed.affiliation)
        if aff_key not in _affiliation_cache:
            insert_person_affiliation(
                conn,
                person.id,
                anime_id,
                parsed.affiliation,
                source="seesaawiki",
            )
            _affiliation_cache.add(aff_key)
            stats["affiliations_recorded"] = stats.get("affiliations_recorded", 0) + 1


# In-memory caches to avoid redundant DB writes for studios/affiliations
_studio_id_cache: set[str] = set()  # studio IDs already upserted
_anime_studio_cache: set[tuple[str, str]] = (
    set()
)  # (anime_id, studio_id) already inserted
_affiliation_cache: set[tuple[str, str, str]] = (
    set()
)  # (person_id, anime_id, studio_name)


def _reset_save_caches() -> None:
    """Reset in-memory caches (called at start of reparse/scrape)."""
    _studio_id_cache.clear()
    _anime_studio_cache.clear()
    _affiliation_cache.clear()


def _save_studio_credit(
    conn,
    anime_id: str,
    studio_name: str,
    role: str,
    stats: dict,
) -> None:
    """Record a studio/company found in credit lines as an anime_studio relationship."""
    from src.database import insert_anime_studio, upsert_studio
    from src.models import AnimeStudio, Studio

    studio_id = f"seesaa:s_{hashlib.sha256(unicodedata.normalize('NFKC', studio_name).encode()).hexdigest()[:12]}"

    # Only upsert studio once per session
    if studio_id not in _studio_id_cache:
        studio = Studio(
            id=studio_id,
            name=studio_name,
            is_animation_studio=True,
        )
        upsert_studio(conn, studio)
        _studio_id_cache.add(studio_id)

    # Only insert anime-studio link once per (anime, studio) pair
    pair = (anime_id, studio_id)
    if pair not in _anime_studio_cache:
        is_main = role in ("アニメーション制作", "制作", "アニメーション")
        anime_studio = AnimeStudio(
            anime_id=anime_id,
            studio_id=studio_id,
            is_main=is_main,
        )
        insert_anime_studio(conn, anime_studio)
        _anime_studio_cache.add(pair)
        stats["studios_recorded"] = stats.get("studios_recorded", 0) + 1


# =============================================================================
# CLI
# =============================================================================


def reparse_from_raw(
    conn,
    data_dir: Path | None = None,
    use_llm: bool = False,
    checkpoint_interval: int = SCRAPE_CHECKPOINT_INTERVAL,
) -> dict:
    """Re-parse all saved raw HTML files and update DB.

    This allows re-running the parser on previously fetched pages
    without making any HTTP requests.
    """
    from src.database import (
        insert_credit,
        update_data_source,
        upsert_person,
        upsert_src_seesaawiki_anime,
    )
    from src.etl.integrate import upsert_canonical_anime

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    raw_dir = data_dir / "raw"
    if not raw_dir.exists():
        log.error("reparse_no_raw_dir", path=str(raw_dir))
        return {}

    # Load page URL list for title mapping
    page_urls_path = data_dir / "page_urls.json"
    title_by_filename: dict[str, str] = {}
    if page_urls_path.exists():
        pages = json.loads(page_urls_path.read_text(encoding="utf-8"))
        for p in pages:
            fname = _safe_filename(p["title"])
            title_by_filename[fname] = p["title"]

    # Reset in-memory caches
    _reset_save_caches()

    # Clear existing seesaa data from DB
    conn.execute("DELETE FROM credits WHERE evidence_source='seesaawiki'")
    conn.execute("DELETE FROM persons WHERE id LIKE 'seesaa:%'")
    conn.execute("DELETE FROM anime WHERE id LIKE 'seesaa:%'")
    try:
        conn.execute("DELETE FROM person_affiliations WHERE source='seesaawiki'")
    except Exception:
        pass  # Table may not exist pre-migration
    conn.execute("DELETE FROM studios WHERE id LIKE 'seesaa:%'")
    conn.execute("DELETE FROM anime_studios WHERE studio_id LIKE 'seesaa:%'")
    conn.commit()
    log.info("reparse_cleared_db")

    stats = {
        "pages_processed": 0,
        "pages_failed": 0,
        "anime_created": 0,
        "credits_created": 0,
        "persons_created": 0,
        "llm_fallbacks": 0,
    }

    llm_available = use_llm and check_llm_available()
    person_cache: dict[str, Person] = {}
    html_files = sorted(raw_dir.glob("*.html"))
    log.info("reparse_start", total_files=len(html_files))

    for idx, html_path in enumerate(html_files):
        stem = html_path.stem
        title = title_by_filename.get(stem, stem)

        try:
            html = html_path.read_text(encoding="utf-8")
        except Exception as e:
            log.warning("reparse_read_error", file=str(html_path), error=str(e))
            stats["pages_failed"] += 1
            continue

        body_text = extract_wiki_body(html)

        # Parse
        episodes = parse_episodes(body_text)
        series_staff = parse_series_staff(body_text)
        regex_credits = sum(len(ep["credits"]) for ep in episodes) + len(series_staff)

        llm_records: list[dict] = []
        if llm_available and regex_credits < 3 and len(body_text) > 500:
            llm_records = parse_with_llm(body_text)
            if llm_records:
                stats["llm_fallbacks"] += 1

        anime_id = make_seesaa_anime_id(title)

        parser_used = "regex"
        if llm_records and regex_credits < 3:
            parser_used = "llm" if regex_credits == 0 else "regex+llm"

        # Save parsed intermediate
        save_parsed_intermediate(
            data_dir,
            title,
            anime_id,
            body_text,
            episodes,
            series_staff,
            llm_records,
            parser_used,
        )

        # DB upsert
        anime = BronzeAnime(id=anime_id, title_ja=title)
        upsert_canonical_anime(conn, anime, evidence_source="seesaawiki")
        upsert_src_seesaawiki_anime(conn, anime_id, title, None, None)
        stats["anime_created"] += 1

        total_episodes = (
            max((ep_data["episode"] or 0 for ep_data in episodes), default=0) or None
        )

        for ep_data in episodes:
            for credit in ep_data["credits"]:
                _save_credit(
                    conn,
                    person_cache,
                    stats,
                    anime_id,
                    credit,
                    episode=ep_data["episode"],
                    upsert_person=upsert_person,
                    insert_credit=insert_credit,
                    total_episodes=total_episodes,
                )

        for credit in series_staff:
            _save_credit(
                conn,
                person_cache,
                stats,
                anime_id,
                credit,
                episode=None,
                upsert_person=upsert_person,
                insert_credit=insert_credit,
                total_episodes=total_episodes,
            )

        if llm_records and regex_credits < 3:
            for record in llm_records:
                pc = ParsedCredit(
                    role=record["role"],
                    name=record["name"],
                    position=0,
                    is_known_role=record["role"] in KNOWN_ROLES_JA,
                )
                _save_credit(
                    conn,
                    person_cache,
                    stats,
                    anime_id,
                    pc,
                    episode=record.get("episode"),
                    upsert_person=upsert_person,
                    insert_credit=insert_credit,
                    total_episodes=total_episodes,
                )

        stats["pages_processed"] += 1

        if stats["pages_processed"] % checkpoint_interval == 0:
            conn.commit()
            log.info(
                "reparse_checkpoint",
                progress=f"{idx + 1}/{len(html_files)}",
                **stats,
            )

    conn.commit()
    update_data_source(conn, "seesaawiki", stats["credits_created"])
    conn.commit()

    log.info("reparse_complete", **stats)
    return stats


@app.command()
def scrape(
    max_pages: int = typer.Option(
        0, "--max-pages", "-n", help="Maximum pages to process (0 = all)"
    ),
    checkpoint: int = typer.Option(
        10, "--checkpoint", "-c", help="Checkpoint interval (pages)"
    ),
    delay: float = typer.Option(
        DEFAULT_DELAY, "--delay", "-d", help="Delay between requests (seconds)"
    ),
    use_llm: bool = typer.Option(
        True, "--llm/--no-llm", help="Use LLM fallback for unparseable pages"
    ),
    fresh: bool = typer.Option(False, "--fresh", help="Ignore existing checkpoint"),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", help="Data directory"
    ),
    list_only: bool = typer.Option(
        False, "--list-only", help="Only enumerate pages, don't scrape"
    ),
    fetch_only: bool = typer.Option(
        False, "--fetch-only", help="Only fetch raw HTML, don't parse or save to DB"
    ),
) -> None:
    """Fetch and parse credit data from SeesaaWiki."""
    from src.database import db_connection, init_db
    from src.log import setup_logging

    setup_logging()

    with db_connection() as conn:
        init_db(conn)
        stats = asyncio.run(
            scrape_seesaawiki(
                conn,
                data_dir=data_dir,
                max_pages=max_pages,
                checkpoint_interval=checkpoint,
                delay=delay,
                use_llm=use_llm,
                fresh=fresh,
                list_only=list_only,
                fetch_only=fetch_only,
            )
        )

    log.info("seesaa_scrape_saved", **stats)


@app.command()
def reparse(
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", help="Data directory with raw/ HTML files"
    ),
    use_llm: bool = typer.Option(
        False, "--llm/--no-llm", help="Use LLM fallback for unparseable pages"
    ),
    checkpoint: int = typer.Option(
        50, "--checkpoint", "-c", help="Checkpoint interval"
    ),
) -> None:
    """Re-parse saved raw HTML files (no HTTP requests).

    Clears existing seesaawiki data from DB, re-parses all raw/*.html files,
    and saves results. Use this after updating the parser.
    """
    from src.database import db_connection, init_db
    from src.log import setup_logging

    setup_logging()

    with db_connection() as conn:
        init_db(conn)
        stats = reparse_from_raw(
            conn,
            data_dir=data_dir,
            use_llm=use_llm,
            checkpoint_interval=checkpoint,
        )

    log.info("seesaa_reparse_saved", **stats)


@app.command("validate-samples")
def validate_samples(
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", help="Data directory with raw/ HTML files"
    ),
    num_samples: int = typer.Option(
        10, "--num-samples", "-n", help="Number of random pages to validate"
    ),
    seed: int = typer.Option(42, "--seed", help="Random seed for sample selection"),
) -> None:
    """Validate parsed data quality using local LLM (Ollama/Qwen3).

    Picks random raw HTML files, parses them with the regex parser,
    and asks the local LLM to check for systemic issues.
    """
    import random
    from src.log import setup_logging

    setup_logging()

    if not check_llm_available():
        log.error("llm_not_available", hint="Start Ollama: ollama serve")
        raise typer.Exit(1)

    raw_dir = data_dir / "raw"
    if not raw_dir.exists():
        log.error("no_raw_dir", path=str(raw_dir))
        raise typer.Exit(1)

    html_files = sorted(raw_dir.glob("*.html"))
    if not html_files:
        log.error("no_raw_files")
        raise typer.Exit(1)

    # Load title mapping
    page_urls_path = data_dir / "page_urls.json"
    title_by_filename: dict[str, str] = {}
    if page_urls_path.exists():
        pages = json.loads(page_urls_path.read_text(encoding="utf-8"))
        for p in pages:
            title_by_filename[_safe_filename(p["title"])] = p["title"]

    rng = random.Random(seed)
    samples = rng.sample(html_files, min(num_samples, len(html_files)))

    issues_found = 0
    total_validated = 0

    for html_path in samples:
        stem = html_path.stem
        title = title_by_filename.get(stem, stem)

        html = html_path.read_text(encoding="utf-8", errors="replace")
        body_text = extract_wiki_body(html)
        if len(body_text.strip()) < 100:
            log.info("validate_skip_short", title=title)
            continue

        episodes = parse_episodes(body_text)
        series_staff = parse_series_staff(body_text)
        all_credits = series_staff[:]
        for ep in episodes:
            all_credits.extend(ep["credits"])

        if not all_credits:
            log.info("validate_skip_no_credits", title=title)
            continue

        total_validated += 1
        log.info(
            "validate_sample",
            title=title,
            credits=len(all_credits),
            known=sum(1 for c in all_credits if c.is_known_role),
        )

        result = validate_parse_with_llm(body_text, all_credits)
        if result.get("should_halt"):
            issues_found += 1
            log.warning(
                "validate_issue_found",
                title=title,
                reason=result.get("reason", "")[:300],
            )
        else:
            log.info("validate_ok", title=title, reason=result.get("reason", "")[:100])

    log.info(
        "validate_complete",
        total_validated=total_validated,
        issues_found=issues_found,
        verdict="PASS"
        if issues_found == 0
        else f"ISSUES ({issues_found}/{total_validated})",
    )


if __name__ == "__main__":
    app()
