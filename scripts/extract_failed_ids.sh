#!/bin/bash
# ログファイルから失敗したアニメIDを抽出

LOG_FILE="${1:-scraper.log}"
OUTPUT="${2:-failed_anime_ids.txt}"

if [ ! -f "$LOG_FILE" ]; then
    echo "❌ Log file not found: $LOG_FILE"
    exit 1
fi

echo "📄 Extracting failed anime IDs from: $LOG_FILE"

# staff_list_fetch_failed から anime_id を抽出
grep "staff_list_fetch_failed" "$LOG_FILE" | \
    grep -oP 'anime_id=anilist:\K\d+' | \
    sort -u > "$OUTPUT"

COUNT=$(wc -l < "$OUTPUT")

if [ "$COUNT" -eq 0 ]; then
    echo "✅ No failed anime IDs found"
    rm -f "$OUTPUT"
else
    echo "✅ Extracted $COUNT unique failed anime IDs to: $OUTPUT"
    echo ""
    echo "Failed IDs:"
    cat "$OUTPUT"
fi
