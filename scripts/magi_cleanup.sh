#!/bin/bash
LOG_FILE="$HOME/magi/logs/cleanup.log"
mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [CLEANUP] $1" >> "$LOG_FILE"; }
log "========== 일일 강제 종료 시작 =========="
for svc in com.magi.engine com.magi.scalping com.magi.grid com.magi.breakout com.magi.update.all com.magi.dashboard com.magi.dashboard.scalping com.magi.dashboard.grid com.magi.dashboard.breakout com.magi.trading; do
    launchctl list | grep -q "$svc" && launchctl stop "$svc" 2>/dev/null && log "  launchctl stop $svc"
done
sleep 2
for pattern in trading_engine.py scalping_engine.py grid_engine.py breakout_engine.py update_all.py update_dashboard.py market_sensor.py; do
    pids=$(pgrep -f "$pattern" 2>/dev/null)
    [ -n "$pids" ] && echo "$pids" | xargs kill -9 2>/dev/null && log "  kill -9 $pattern (PIDs: $(echo $pids | tr '\n' ' '))"
done
sleep 1
remaining=$(pgrep -f "magi_|magi/" 2>/dev/null)
[ -n "$remaining" ] && echo "$remaining" | xargs kill -9 2>/dev/null && log "  [경고] 잔여 프로세스 강제 종료: $remaining"
log "========== 강제 종료 완료 =========="
