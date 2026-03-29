#!/usr/bin/env python3
"""
🐕 MAGI Watchdog — watchdog.py
60초마다 4개 엔진 heartbeat 감시
엔진별 스캔주기에 맞춘 개별 타임아웃 적용

수정: 별이 (Claude Opus 4.6)
- 기존 180초 단일 타임아웃 → 엔진별 개별 타임아웃
- 카이/제트/부메랑: 1020초 (15분 + 2분)
- 릴리스: 3720초 (1시간 + 2분)
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path.home() / "magi"))
from magi_common import (
    load_config, create_logger, send_telegram,
    get_all_positions, close_position,
    BASE_DIR, KST
)

logger = create_logger("watchdog", "watchdog.log")

# ──────────────────────────────────────────────
# 엔진별 타임아웃 (초)
# 스캔주기 + 2분 여유
# ──────────────────────────────────────────────
ENGINE_TIMEOUTS = {
    "kai": 1020,        # 15분 + 2분
    "jet": 1020,        # 15분 + 2분
    "boomerang": 1020,  # 15분 + 2분
    "release": 3720,    # 1시간 + 2분
}

ENGINE_NAMES = {
    "kai": "카이", "jet": "제트",
    "boomerang": "부메랑", "release": "릴리스"
}
ENGINE_ICONS = {
    "kai": "🌊", "jet": "⚡",
    "boomerang": "🪃", "release": "👁️"
}

# 전 엔진 무응답 시 긴급 청산 전 2회 연속 확인 (오탐 방지)
all_dead_count = 0
ALL_DEAD_THRESHOLD = 2  # 2회 연속 (= 2분) 확인 후 긴급 청산


def check_heartbeats():
    """각 엔진 heartbeat 확인. 반환: {engine: (alive, elapsed)}"""
    results = {}
    for engine, timeout in ENGINE_TIMEOUTS.items():
        hb_path = BASE_DIR / f"heartbeat_{engine}.json"
        try:
            with open(hb_path, "r") as f:
                data = json.load(f)
            elapsed = time.time() - data.get("epoch", 0)
            alive = elapsed < timeout
            results[engine] = (alive, elapsed)
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            results[engine] = (False, -1)
    return results


def emergency_close_all(config):
    """전 엔진 무응답 시 모든 포지션 긴급 청산"""
    logger.critical("🚨 전 엔진 무응답! 긴급 청산 실행")
    all_pos = get_all_positions(config)
    if not all_pos:
        logger.info("활성 포지션 없음 — 긴급 청산 불필요")
        return 0

    closed = 0
    for pos in all_pos:
        try:
            close_side = "Sell" if pos["side"] == "Buy" else "Buy"
            strat = pos["strategy"]
            acct = config["bybit"]["accounts"][strat]
            result = close_position(
                pos["symbol"], close_side, pos["size"],
                acct["api_key"], acct["api_secret"]
            )
            if result:
                logger.info(f"긴급 청산 완료: {pos['symbol']} {pos['side']} ({strat})")
                closed += 1
        except Exception as e:
            logger.error(f"긴급 청산 실패: {pos['symbol']} — {e}")
    return closed


def main():
    global all_dead_count
    config = load_config()
    now_str = datetime.now(KST).strftime("%H:%M:%S")

    results = check_heartbeats()

    # 개별 엔진 상태 로깅
    dead_engines = []
    alive_count = 0
    for engine, (alive, elapsed) in results.items():
        name = ENGINE_NAMES[engine]
        icon = ENGINE_ICONS[engine]
        timeout = ENGINE_TIMEOUTS[engine]
        if alive:
            alive_count += 1
            logger.info(f"{icon} {name}: 정상 ({elapsed:.0f}초/{timeout}초)")
        else:
            dead_engines.append(engine)
            if elapsed >= 0:
                logger.warning(f"{icon} {name}: 무응답 {elapsed:.0f}초 (임계: {timeout}초)")
            else:
                logger.warning(f"{icon} {name}: heartbeat 파일 없음")

    # 개별 엔진 무응답 알림
    if dead_engines and alive_count > 0:
        # 일부 엔진만 죽은 경우 → 텔레그램 경고
        all_dead_count = 0  # 전체 무응답 카운터 리셋
        msg_lines = [f"🐕 <b>[신지] 엔진 무응답 감지</b>", f"🕐 {now_str}"]
        for engine in dead_engines:
            _, elapsed = results[engine]
            name = ENGINE_NAMES[engine]
            icon = ENGINE_ICONS[engine]
            if elapsed >= 0:
                msg_lines.append(f"{icon} {name}: 무응답 {elapsed:.0f}초")
            else:
                msg_lines.append(f"{icon} {name}: heartbeat 없음")
        msg_lines.append(f"\n정상 엔진: {alive_count}개")
        send_telegram(config, "\n".join(msg_lines))
        return

    # 전 엔진 무응답 확인
    if alive_count == 0 and len(results) == 4:
        all_dead_count += 1
        logger.warning(f"전 엔진 무응답 ({all_dead_count}/{ALL_DEAD_THRESHOLD})")

        if all_dead_count >= ALL_DEAD_THRESHOLD:
            # 2회 연속 확인 → 긴급 청산
            closed = emergency_close_all(config)
            msg = (
                f"🚨 <b>[신지] 전 엔진 무응답 — 긴급 청산</b>\n"
                f"🕐 {now_str}\n"
                f"청산 포지션: {closed}건\n"
                f"연속 무응답: {all_dead_count}회"
            )
            send_telegram(config, msg)
            all_dead_count = 0  # 리셋
        else:
            msg = (
                f"⚠️ <b>[신지] 전 엔진 무응답 감지</b>\n"
                f"🕐 {now_str}\n"
                f"연속 {all_dead_count}회 — 다음 확인 시 긴급 청산 실행"
            )
            send_telegram(config, msg)
    else:
        all_dead_count = 0  # 정상이면 리셋


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"watchdog 크래시: {e}\n{traceback.format_exc()}")
        try:
            send_telegram(load_config(), f"🚨 [watchdog] 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
