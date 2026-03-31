#!/usr/bin/env python3
"""
🛡 MAGI v3.0 워치독 — watchdog_v3.py
5분마다 실행: 엔진 heartbeat 타임스탬프 체크
스캔이 예상 주기의 2.5배 동안 없으면 텔레그램 경보
"""

import sys
import json
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from magi_v3_common import load_config, send_telegram, BASE_DIR, create_logger

logger = create_logger("watchdog", "watchdog.log")

ENGINES = [
    ("hinokami", "heartbeat_hinokami.json", 600),   # 4H × 2.5 = 600분
    ("nite",     "heartbeat_nite.json",     300),   # 2H × 2.5 = 300분
    ("dashboard","heartbeat_dashboard.json", 15),    # 5분 × 3 = 15분
]

ALERT_STATE_PATH = BASE_DIR / "watchdog_alert_state.json"


def load_alert_state():
    if ALERT_STATE_PATH.exists():
        return json.loads(ALERT_STATE_PATH.read_text())
    return {}


def save_alert_state(state):
    ALERT_STATE_PATH.write_text(json.dumps(state, default=str))


def main():
    config = load_config()
    now = datetime.now(timezone.utc)
    alert_state = load_alert_state()

    for eng_name, hb_file, max_stale in ENGINES:
        hb_path = BASE_DIR / hb_file

        if not hb_path.exists():
            status = "RED"
            msg = "heartbeat 파일 없음"
        else:
            hb = json.loads(hb_path.read_text())
            last = datetime.fromisoformat(hb["timestamp"])
            elapsed = (now - last).total_seconds() / 60

            if elapsed > max_stale:
                status = "RED"
                msg = f"{int(elapsed)}분 전 마지막 스캔 (한도: {max_stale}분)"
            else:
                status = "GREEN"
                msg = None

        if status == "RED":
            last_alert = alert_state.get(eng_name)
            if last_alert:
                last_alert_dt = datetime.fromisoformat(last_alert)
                if (now - last_alert_dt).total_seconds() < 3600:
                    logger.info(f"{eng_name}: 장애 지속 중 (재경보 대기)")
                    continue

            emoji = "🚨" if eng_name != "dashboard" else "⚠️"
            send_telegram(config, f"{emoji} <b>워치독 경보: {eng_name}</b>\n{msg}\ncron 또는 엔진 프로세스 점검 필요!")
            alert_state[eng_name] = now.isoformat()
            logger.warning(f"{eng_name}: 경보 발송 — {msg}")
        else:
            if eng_name in alert_state:
                del alert_state[eng_name]
                logger.info(f"{eng_name}: 정상 복귀")

    save_alert_state(alert_state)


if __name__ == "__main__":
    main()
