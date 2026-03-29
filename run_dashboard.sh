#!/bin/bash
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
cd /Users/junsumacmini/magi || exit 1

LOG_DIR="/Users/junsumacmini/magi/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/dashboard.log"

echo "" >> "$LOG_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" >> "$LOG_FILE"
echo "$(date '+%Y-%m-%d %H:%M:%S') — 현황판 업데이트 시작" >> "$LOG_FILE"

/opt/homebrew/bin/python3 /Users/junsumacmini/magi/update_dashboard.py >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') — ❌ 실패 (exit=$EXIT_CODE)" >> "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') — ✅ 완료" >> "$LOG_FILE"
fi
