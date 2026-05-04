# Task: 既存 source の未統合 BRONZE table 監査 + 統合計画

**ID**: `14_silver_extend/10_extra_tables_audit`
**Priority**: 🟡

## 動機

各 source の BRONZE は既統合分以外にも多数 table 持つ:

### MAL (28 table の 9 が取込済、19 未統合)
- 取込済: anime / persons / staff_credits / anime_characters / va_credits / anime_genres / anime_studios / anime_relations / anime_recommendations
- **未統合 (display 系含む)**: anime_episodes / anime_external / anime_moreinfo / anime_news / anime_pictures / anime_videos_promo / anime_videos_ep / anime_themes / anime_statistics / anime_streaming / etc.

### ANN
- 取込済: anime / persons / credits / cast / company / episodes / releases / news / related (Card 14/03 で全 9 統合済)

### AniList
- 取込済: anime / persons / characters / character_voice_actors / studios / credits / anime_studios / relations
- 確認: 全 BRONZE table 統合済?

### bangumi / keyframe / mediaarts / seesaawiki / sakuga_atwiki
- 既存統合カード後の追加 BRONZE table 確認

## ゴール

1. 各 source の BRONZE table 列挙 + Conformed 統合状態 audit
2. 未統合 table の取込価値判定 (display only / scoring 価値あり / etc.)
3. 取込推奨 table のサブカード起票 (10/01〜)

## 範囲

- 新規: `src/etl/audit/bronze_to_conformed_coverage.py` (各 source × 各 table の取込率)
- 出力: `result/audit/bronze_conformed_coverage.md`
- 既存 22/03 silver_column_coverage.md と並走 (本カードは table-level、22/03 は column-level)

## 完了条件

- audit レポート生成
- 未統合 table 一覧 + 取込価値 priority 判定
- サブカード起票推奨リスト
