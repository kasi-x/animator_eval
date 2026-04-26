"""SeesaaWiki credit line parsers and data classes."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import structlog

log = structlog.get_logger()

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
    # animation drawing (extended)
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
    # finishing (extended)
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
    # editing (extended)
    "オンライン編集",
    "HD編集",
    "フォーマット編集",
    "デジタル編集",
    # photography (extended)
    "コンポジット撮影",
    "撮影協力",
    "撮影チーフ",
    "モニターワークス",
    "モニターグラフィック",
    "モニターグラフィックス",
    "スキャン",
    "スキャニング",
    "フィルム",
    # art/background (extended)
    "美術監督補佐",
    "美術協力",
    "美術背景",
    "美術統括",
    "背景協力",
    "背景管理",
    "背景監修",
    # settings/design sheets (extended)
    "設定管理",
    "画面設計",
    "原図",
    "原図整理",
    "原図監修",
    "衣装デザイン",
    "衣装協力",
    "服装設定",
    "キャラクター監修",
    # sound (extended)
    "音楽制作協力",
    "音楽ディレクター",
    "サウンドミキサー",
    # direction/episode direction (extended)
    "演出補",
    "演出協力",
    "ディレクター",
    # production management (extended)
    "制作管理",
    "制作プロデューサー",
    "CG制作プロデューサー",
    "宣伝プロデューサー",
    "制作進行チーフ",
    "文芸進行",
    "モデル協力",
    "モデル進行管理",
    "進行協力",
    # design (extended)
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
    # music (extended)
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
    # production support
    "補佐",
    "デジタル作画",
    "原案協力",
    "スーパーバイザー",
    "デジタル制作",
    "ツール開発",
    "モーションアドバイザー",
    "ゲストメカニックデザイン",
    # misc (work-specific but recognized)
    "児童画",
    "WEBプロモーション",
    "作品提供",
    "メカニカルコーディネーター",
    # === Unregistered roles detected by LLM validation (2026-03-16) ===
    # sound-related
    "録音監督",
    "音響制作デスク",
    "音響調整",
    # production management-related
    "プロダクションマネージャー",
    "プロデューサー補",
    "製作補",
    # literary / script-related
    "文芸助手",
    "脚本構成",
    # CG / post-production
    "3DCG監督",
    "デジタル特殊効果",
    "テレシネ",
    # design / settings-related
    "美術設定監修",
    "デザイン監修",
    "色設定",
    "アクセサリーデザイン案",
    # cooperation / produce
    "プロデュース協力",
    "キャスティング",
    # music-related
    "演奏",
    "指揮",
    "OPテーマプロデューサー",
    "OPテーマ音楽制作協力",
    # broadcasting / media
    "NETプロデューサー",
    "データ放送",
    "掲載",
    # supervision
    "時代考証",
    # === UNKNOWN roles (2026-03-16 full scan, freq >= 10) ===
    # animation cooperation / production support
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
    # choreography
    "ダンス振付",
    "振付",
    # in-between QC
    "動画検査補佐",
    "動画管理",
    "動画検査・デジタル修正",
    # animation direction
    "作監協力",
    "作監補佐",
    "アクション・エフェクト作画監督",
    "チーフアニメーター",
    "キャラ原図・総作画監督",
    # key animator
    "キーアニメーター",
    # VFX / digital
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
    # direction-related
    "アニメーション演出",
    "演出補佐",
    "チーフ演出",
    "シリーズ演出",
    "監督補佐",
    "監督補",
    "監督助手",
    # 3D-related
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
    # photography-related
    "撮影チーム長",
    "BGスキャニング",
    "撮影管理",
    "撮影制作",
    "撮影監修",
    "副撮影監督",
    "撮影監督補",
    "撮影助手",
    # editing-related
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
    # post-production
    "ポストプロダクション",
    "フィルムレコーディング",
    # sound-related
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
    # music-related
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
    # producer-related
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
    # production management-related
    "プロダクションマネージャー",
    "プロダクション・マネージャー",
    "プログラムマネージャー",
    "プロダクトマネージャー",
    "CGプロダクションマネージャー",
    "製作統括",
    "製作管理",
    "製作デスク",
    "製作補",
    # promotion / marketing
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
    # design-related
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
    # art / background
    "美術助手",
    "美術設定協力",
    "美術設定補佐",
    "背景デザイン",
    "背景担当",
    "背景監督",
    # settings sheets
    "設定デザイン",
    "設定担当",
    "設定マネージャー",
    "設定考証",
    "設定考証協力",
    "小物設定",
    "小物デザイン",
    "衣装デザイン協力",
    "衣装コンセプトデザイン・アシスタント",
    # color design
    "色彩設計補",
    "色彩設計/指定検査",
    "色彩設計・指定検査",
    "色彩設定補佐",
    "カラーコーディネイト",
    "カラリスト",
    # telop / title
    "テロップ",
    "タイトルリスワーク",
    "タイトル・リスワーク",
    "リスワーク",
    "タイトルロゴ",
    "筆文字",
    "サブタイトル",
    "サブタイトル題字",
    # CG-related (additional)
    "CGラインディレクター",
    "CGテクニカルディレクター",
    "モデリングディレクター",
    "モデリング/リギング",
    "モデリング・リギング",
    "CG制作担当",
    # literary / script-related
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
    # rights / licensing
    "版権制作",
    "版権担当",
    "版権管理",
    "ライツ",
    "ライツ担当",
    "ライツプロモート",
    "国内ライセンス",
    "MDライセンス担当",
    # special
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
    # OP/ED/PV
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
    # production committee (record only)
    "製作委員会",
    # card key animation (work-specific)
    "アニメオリジナルカード原画",
    # studio-related
    "スタジオ制作担当",
    "演技事務",
    "ストーリーボード",
    "アニメーションストーリーボード",
    # other / misc
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
    # spelling variants
    "第2原画",
    # character / mecha abbreviations
    "キャラクター",
    "メカニック",
    # CG / 3D (additional)
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
    # tools / tech
    "ツール・スクリプト開発",
    # finishing (additional)
    "仕上げ検査補佐",
    "彩色チェック",
    "色指定・色検査",
    # design / background (additional)
    "背景レイアウト",
    "サブ・小物",
    # digital
    "デジタルワーク",
    "2Dエフェクト&コンポジット",
    # translation
    "翻訳",
    # eyecatch
    "アイキャッチ原画",
    "提供バックイラスト",
    # work-specific but high-frequency
    "プリ☆チャンライブ演出",
    "プリパラライブ演出",
    # general roles with freq >= 30
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
    # parenthetical roles (work-specific modifiers)
    "脚本協力",
    "脚本協力(忍術創案)",
    # === Round 3 (2026-03-16, post-HTML-fix round 2, freq >= 100) ===
    # animation drawing
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
    # direction / storyboard
    "絵コンテ協力",
    "演助",
    "次回予告",
    # production management
    "進行補佐",
    "制作話数担当",
    "仕上管理",
    "仕上制作",
    # CG
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
    # design-related
    "モデリングデザイナー",
    "デザイナー",
    "チーフデザイナー",
    "ゲストモンスターデザイン",
    "クリーチャーデザイン協力",
    "サブキャラクター設定",
    "サブ設定協力",
    "小物・エフェクト設定",
    # layout
    "レイアウト協力",
    # VFX
    "VFX",
    # music-related
    "エンディング曲",
    "オープニング曲",
    "音効",
    # finishing
    "着彩",
    "指定検査",
    # costume
    "衣装設定",
    # art / background
    "美術ボード協力",
    # illustration
    "イラストレーション",
    "エンディングイラスト",
    "アイキャッチイラスト",
    # photography-related
    "撮影担当",
    "デジタル背景",
    "実写撮影",
    "TAP",
    # distribution
    "配信担当",
    # === Round 4 (2026-03-16, freq >= 20 systematic additions) ===
    # ---- in-between finishing (spelling variants: 動仕 already registered) ----
    "動画仕上",
    "動画仕上げ",
    "動画仕上管理",
    "動画仕上進行",
    "動画仕上協力",
    "動仕協力",
    # ---- layout ----
    "レイアウトチェック",
    "レイアウト・チェック",
    "レイアウトチェッカー",
    "レイアウト修正",
    "メインレイアウト",
    "レイアウト作監",
    "レイアウト作画監督補佐",
    # ---- animation drawing (additional) ----
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
    # ---- finishing (additional) ----
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
    # ---- digital / TP ----
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
    # ---- CG (additional) ----
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
    # ---- 3D (additional) ----
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
    # ---- 2D (additional) ----
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
    # ---- composite / photography (additional) ----
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
    # ---- direction (additional) ----
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
    # ---- art / background (additional) ----
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
    # ---- settings sheets (additional) ----
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
    # ---- design (additional) ----
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
    # ---- modeling / rigging ----
    "モデリングスーパーバイザー",
    "モデリングアーティスト",
    "モデリングチーフ",
    "モデリングリード",
    "モデリングコーディネーター",
    "リガー",
    "リギング",
    "リギングスーパーバイザー",
    "セットアップ",
    # ---- motion ----
    "モーションキャプチャー",
    "モーションデザイナー",
    "モーションディレクター",
    "モーショングラフィック",
    # ---- supervision ----
    "エフェクト監修",
    "LO監修",
    "特殊効果監修",
    "脚本監修",
    "シナリオ監修",
    "特効監修",
    # ---- effects ----
    "エフェクトディレクター",
    "エフェクトスーパーバイザー",
    "エフェクトデザイナー",
    "エフェクトアーティスト",
    "エフェクト原案",
    "AE・特効",
    "エアブラシワーク",
    # ---- production management (additional) ----
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
    # ---- sound (additional) ----
    "録音進行",
    "整音助手",
    # ---- music (additional) ----
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
    # ---- voice actor / narration ----
    "ナレーション",
    "パーソナリティ",
    "ダンサー",
    "出演",
    "方言指導",
    "俳優担当",
    # ---- sales / promotion (additional) ----
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
    # ---- translation / overseas ----
    "和訳",
    "通訳",
    "翻訳協力",
    "韓国語通訳・翻訳",
    # ---- live-action ----
    "実写協力",
    "実写撮影",
    # ---- color (additional) ----
    "カラーコーディネーター",
    "カラーマネジメント",
    "ゲスト色彩設計",
    "ゲスト色彩設計・色指定",
    "色設計・検査",
    "色指定・検査・貼込",
    "色指定補助",
    # ---- editing (additional) ----
    "ビデオ編集担当",
    "VIDEO編集",
    "フォーマット編集担当",
    "DCPマスタリング",
    # ---- illustration (additional) ----
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
    # ---- OP/ED production ----
    "エンディング制作",
    "エンディングディレクター",
    "OP作画監督",
    "ED作画監督",
    # ---- screenplay (additional) ----
    "脚色",
    "脚本制作",
    "脚本事務",
    "脚本進行",
    "ストーリー",
    "ストーリー/脚本",
    "シリーズ構成協力",
    # ---- rendering / technical ----
    "レンダリング",
    "テクスチャペインター",
    "プログラマー",
    "R&D・インフラ開発",
    # ---- English titles (Director, etc.) ----
    "Director",
    "Producer",
    # ---- broadcast / misc ----
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
    "デザインワーク",  # spelling variant (デザインワークス already registered)
    # ---- Live2D / VFX ----
    "VFXスーパーバイザー",
    "Live2Dアニメーター",
    # base roles for normalization
    "チーフ",
    "リード",
    # Round 5.5: final remaining roles
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
    # === Round 5 (2026-03-16, remaining general roles freq >= 20) ===
    "動画検査協力",
    "リードCGアニメーター",
    "リードモデラー",
    "オンライン編集助手",
    "サブキャラクター",
    "アニメオリジナルカード絵作画",
    "リードアーティスト",
    # meta — used in parse_credit_line for studio name → OTHER conversion
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
    "最強カード",  # Yu-Gi-Oh card game noise
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

# TV show corner names / joke credits — not parsed as credit lines
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

# Regex: generic credit line — "Something：Name" or "Something　Name" pattern
# Supports two separator styles:
#   1. Colon: "脚本：PersonName" or "脚本:PersonName"
#   2. Space: "脚本　PersonName" — after NFKC normalization, fullwidth space → half-width
# Role part: sequence of non-separator chars (CJK, Latin, digits, etc.)
# Allows multi-role joined by ・/／  (e.g. "絵コンテ・演出：PersonName")
_RE_CREDIT_COLON = re.compile(
    r"([^\s：:、,・/／]{2,}(?:\s*[・/／]\s*[^\s：:、,・/／]{2,})*)"
    r"\s*[：:]\s*(.+)"
)
# Space-separated pattern: role must be CJK-heavy to avoid false matches on English text
# e.g. "脚本　PersonName" (post-NFKC: "脚本 PersonName")
_RE_CREDIT_SPACE = re.compile(
    r"([\u3000-\u9fff\uf900-\ufaff]{2,}(?:\s*[・/／]\s*[\u3000-\u9fff\uf900-\ufaff]{2,})*)"
    r"\s{1,4}"  # 1-4 spaces (post-NFKC fullwidth → single half-width)
    r"([^\s].+)"
)

# Regex: multi-role split — e.g. "絵コンテ・演出" → ["絵コンテ", "演出"]
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
# e.g. multiple CJK names in a row → split into separate entries
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
    # e.g. "CJKName  CJKName  THU YEN  MINH CHAU"
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


# Regex: parenthetical affiliation — "PersonName(StudioName)" or "PersonName（StudioName）"
_RE_AFFILIATION = re.compile(r"\s*[\(（]([^)）]+)[\)）]\s*$")
# Regex: trailing bracket content — episode ranges e.g. "〔3-8,25〕" "[1-3]" etc.
_RE_BRACKET_SUFFIX = re.compile(r"\s*[〔\[【]([^〕\]】]*)[〕\]】]\s*$")

# Episode range notation: e.g. 第1話〜第21話, 1-21, #35, etc.
_RE_EP_RANGE = re.compile(r"(?:第\s*)?(\d+)\s*話?\s*[〜~\-ー–]\s*(?:第\s*)?(\d+)\s*話?")
_RE_EP_SINGLE = re.compile(r"(?:第\s*)?(\d+)\s*話")
_RE_EP_HASH = re.compile(r"#\s*(\d+)")
# Open-ended range: e.g. "4話〜" or "第4話-" (N onwards, no end specified)
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
        # Try closed range first: e.g. 第1話〜第21話, 1-21, etc.
        m = _RE_EP_RANGE.search(seg)
        if m:
            start, end = int(m.group(1)), int(m.group(2))
            if 0 < start <= end <= 9999:
                episodes.update(range(start, end + 1))
            continue
        # Try open-ended range: e.g. 4話〜 (N onwards, resolve later with total_episodes)
        m = _RE_EP_OPEN.search(seg)
        if m:
            ep = int(m.group(1))
            if 0 < ep <= 9999:
                episode_from = ep
            continue
        # Try single episode with 話 or 第: e.g. 第35話
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

    # Strip trailing bracket content: e.g. "PersonName〔3-8,25〕" → "PersonName"
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
# e.g. "art_director PersonName         color_designer PersonName"
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

    e.g. "art_director PersonName         color_designer PersonName"
    Returns None if not tabular format.
    """
    m = _RE_TABULAR.match(line)
    if not m:
        return None
    role1, name1, role2, rest = m.group(1), m.group(2), m.group(3), m.group(4)
    # BOTH roles must be known to avoid false positives where person names
    # in the name field get mistaken for role2 (e.g. "BGM_role PersonName PersonName ...")
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
    # Skip indented lines — these are name continuations (e.g. "　　　PersonName　PersonName")
    # not "Role　Name" pairs. Check the RAW line before strip().
    stripped_raw = raw_line.lstrip("\n\r")
    if stripped_raw and stripped_raw[0] in (" ", "\u3000", "\t"):
        return []
    # Skip wiki UI/nav text and footnote markers
    if line.startswith(("*", "カテゴリ")):
        return []
    if any(sub in line for sub in _SKIP_SUBSTRINGS):
        return []
    # Skip TV show corner names / joke credits (e.g. Pokémon segment names, "DUEL：...")
    if any(sub in line for sub in _JOKE_CORNER_SUBSTRINGS):
        return []
    # Skip voice actor cast lines: "CharName(CV：VoiceName)" or "CharName（CV：..."
    # Also "CV：VoiceName" (CV as role)
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

    # Try tabular format first: e.g. "美術監督 PersonName         色彩設計 PersonName"
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

    # Skip episode title lines: "Karte：00「EpisodeTitle」", "EPISODE:001「...」",
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
    # person-name lines like "PersonName　PersonName" from being misinterpreted.
    # Colon-separated lines ("RoleName：PersonName") are unambiguous and parsed for all roles.
    if space_match:
        candidate_roles = _RE_MULTI_ROLE.split(role_text)
        if not any(r.strip() in KNOWN_ROLES_JA for r in candidate_roles):
            return []

    # Skip metadata lines (e.g. "話数：24", "放送時間：schedule")
    if role_text in _METADATA_ROLES:
        return []

    # When the "role" text is actually a company/studio name
    # (e.g. "StudioName：PersonName"), mark names with that affiliation.
    # The role is set to "OTHER" (production support at the named studio).
    role_is_company = _is_company_name(role_text)
    company_affiliation: str | None = role_text if role_is_company else None

    # Split multi-role: e.g. "絵コンテ・演出" → ["絵コンテ", "演出"]
    # BUT only split if ALL parts are known roles. Otherwise keep as one compound role.
    # e.g. "メカ・エフェクト作画監督" → keep as-is (compound role name)
    #      "脚本・絵コンテ・原画" → split (all parts are known)
    #      "プロダクション・マネージャー" → keep as-is (compound role name)
    raw_parts = _RE_MULTI_ROLE.split(role_text)
    raw_parts = [r.strip() for r in raw_parts if r.strip()]
    # Special case: "作" in compound roles means "作曲" (composition) (e.g. "作・編曲" = "作曲・編曲")
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
    # e.g. compound-prefix + 協力 → "協力" (SPECIAL); prefix + 原画 → "原画"
    roles = [_normalize_role(r) for r in roles]

    # Split names — order is preserved (position 0 = first listed = most senior)
    # Use paren-aware splitter to avoid breaking episode ranges inside ()（）
    raw_names = _split_names_paren_aware(names_text)
    # Further split space-separated Japanese names (e.g. multiple names in a row)
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
# LLM-delegated parsing problems (quality audit 2026-03-16)
#
# The following issues have high regex-mitigation cost and are delegated to LLM (Ollama/Qwen3):
#
# 1. Mis-parsing of non-anime pages (timeline/index pages)
#    e.g. company names are parsed as roles on "2010年代後半" pages
#    Reason: page-type detection is hard with regex; enumerating title patterns is unrealistic
#    Handled: parse_with_llm() falls back when unknown-role rate > 50%
#
# 2. Some cast lines bypass regex section-header detection
#    e.g. pages where キャラ名：声優名 appear without a section header
#    Reason: distinguishing character names from role names is context-dependent without headers
#    Handled: detectable by validate_parse_with_llm(); fixed via reparse --llm
#
# These are handled at reparse time with the --llm flag (Tier-2 LLM parser fallback).
# The regex parser alone does not need to handle them (cost/benefit does not justify it).
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
# Inline section parsing (date-range grouped format)
# =============================================================================

# Numbered date-range section: "1. 4月3日-6月26日 ..." or "2．10月2日..."
_RE_INLINE_SECTION = re.compile(
    r"^\s*(\d+)[.．]\s+(\d+月\d+日)",
    re.MULTILINE,
)


def _has_inline_sections(body_text: str) -> bool:
    """True if page uses numbered date-range sections instead of episode headers.

    Heuristic: has >=2 numbered date sections (N. M月D日) but no standard
    episode headers (第N話 / #N).
    """
    section_hits = _RE_INLINE_SECTION.findall(body_text)
    episode_hits = _RE_EPISODE.findall(body_text)
    return len(section_hits) >= 2 and len(episode_hits) == 0


def parse_inline_sections(body_text: str) -> list[dict]:
    """Parse pages with numbered date-range sub-sections.

    Used for pages like OP/ED制作 blocks in サザエさん yearly pages,
    where sections look like:
        1. 1月10日-3月27日 宮崎県版
          演出：森田浩光
          美術：佐藤博

    Returns [{"episode": section_number, "credits": [...]}].
    Each section number is synthetic (1, 2, 3…) — not a broadcast episode.
    To avoid collision with real episode numbers these are stored as-is and
    downstream can choose to collapse them (set episode=None after dedup).
    """
    lines = body_text.split("\n")
    sections: list[dict] = []
    current_section: int | None = None
    current_credits: list[ParsedCredit] = []
    in_cast = False

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            continue

        if _is_cast_section_header(stripped):
            in_cast = True
            continue
        if _is_staff_section_header(stripped):
            in_cast = False
            continue

        m = _RE_INLINE_SECTION.match(stripped)
        if m:
            if current_credits:
                sections.append({"episode": current_section, "credits": current_credits})
            current_section = int(m.group(1))
            current_credits = []
            in_cast = False
            continue

        if in_cast:
            continue

        credits = parse_credit_line(raw_line)
        if credits:
            current_credits.extend(credits)

    if current_credits:
        sections.append({"episode": current_section, "credits": current_credits})

    return sections


def collapse_inline_sections(sections: list[dict]) -> list[ParsedCredit]:
    """Merge inline sections into series-level credits, deduplicating by (name, role).

    Use this when section numbers are not meaningful broadcast episodes
    (e.g. OP/ED version blocks) — preserves unique (name, role) pairs only.
    """
    seen: set[tuple[str, str]] = set()
    merged: list[ParsedCredit] = []
    for sec in sections:
        for credit in sec["credits"]:
            key = (credit.name, credit.role)
            if key not in seen:
                seen.add(key)
                merged.append(credit)
    return merged
