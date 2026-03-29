#!/bin/bash
LOG_FILE="$HOME/magi/logs/cleanup.log"
mkdir -p "$(dirname "$LOG_FILE")"
echo "$(date '+%Y-%m-%d %H:%M:%S') [REBOOT] 즉시 재부팅 실행" >> "$LOG_FILE"
sudo shutdown -r now
