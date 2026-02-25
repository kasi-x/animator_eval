#!/bin/bash
# スクレイピング失敗の修復スクリプト
#
# 3段階で修復:
#   1. クレジット取得失敗アニメの再スクレイプ (666件)
#   2. 正規化テーブル + メタデータ欠損の補完 (全20,382件)
#   3. 404/削除済みアニメの検出とマーク
#
# Usage:
#   bash scripts/fix_scraping_failures.sh [step]
#
#   step:
#     1 | retry      - クレジット取得失敗の再スクレイプ
#     2 | backfill   - 正規化テーブル + メタデータ補完
#     3 | cleanup    - 404アニメの検出・マーク
#     all            - 全ステップ実行 (デフォルト)
#     status         - 現在のDB状態を表示

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# ──────────────────────────────────────
# ユーティリティ
# ──────────────────────────────────────

header() {
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "════════════════════════════════════════════════════════════"
    echo ""
}

# ──────────────────────────────────────
# Step 0: DB状態確認
# ──────────────────────────────────────

show_status() {
    header "📊 DB Status"
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py status
}

# ──────────────────────────────────────
# Step 1: クレジット取得失敗の再スクレイプ
# ──────────────────────────────────────

step_retry() {
    header "📋 Step 1: Check Missing Credits"
    echo "クレジットが0件のアニメの状況を表示します"
    echo "（AniListにスタッフ未登録 → 別データソースで補完推奨）"
    echo ""
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py retry-no-credits
}

# ──────────────────────────────────────
# Step 2: 正規化テーブル + メタデータ補完
# ──────────────────────────────────────

step_backfill() {
    header "📥 Step 2: Backfill Normalized Tables + Metadata"
    echo "studios, characters, relations テーブルと"
    echo "country_of_origin 等のメタデータを補完します"
    echo ""
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py backfill
}

# ──────────────────────────────────────
# Step 3: 404アニメの検出
# ──────────────────────────────────────

step_cleanup() {
    header "🧹 Step 3: Detect & Mark Deleted Anime"
    echo "AniList APIで404を返すアニメを検出します"
    echo ""
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py detect-deleted
}

# ──────────────────────────────────────
# メイン
# ──────────────────────────────────────

STEP="${1:-all}"

case "$STEP" in
    status)
        show_status
        ;;
    1|retry)
        show_status
        step_retry
        show_status
        ;;
    2|backfill)
        show_status
        step_backfill
        show_status
        ;;
    3|cleanup)
        show_status
        step_cleanup
        show_status
        ;;
    all)
        show_status
        step_retry
        step_backfill
        step_cleanup
        show_status
        echo ""
        echo "✅ 全ステップ完了"
        ;;
    *)
        echo "Usage: $0 [status|1|retry|2|backfill|3|cleanup|all]"
        exit 1
        ;;
esac
