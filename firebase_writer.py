#!/usr/bin/env python3
"""
🔥 firebase_writer.py — MAGI × 픽셀 오피스 Firebase 연동
설계: 별이 | 구현: 미츠리

Firebase Realtime DB에 에이전트 상태를 실시간으로 업데이트.
모든 함수는 try-except로 감싸서 실패해도 트레이딩에 영향 없음.

사용법:
    import firebase_writer as fw
    fw.init_firebase(config)
    fw.update_agent_status("kai", {"status": "active", "mood": "coding", "task": "BTC 분석 중"})
    fw.push_event("kai", "BTC 롱 진입 ($68,450)")
"""

import time
import logging

logger = logging.getLogger("firebase_writer")

# ===== 상태 =====
_initialized = False
_db = None

# ===== 초기화 =====
def init_firebase(config):
    """Firebase Admin SDK 초기화 (최초 1회)"""
    global _initialized, _db

    if _initialized:
        return True

    firebase_cfg = config.get("firebase", {})
    if not firebase_cfg.get("enabled", False):
        logger.info("🔥 Firebase 비활성화 (config.firebase.enabled = false)")
        return False

    try:
        import firebase_admin
        from firebase_admin import credentials, db

        sa_path = firebase_cfg.get("service_account_path", "firebase_service_account.json")
        db_url = firebase_cfg.get("database_url", "")

        if not db_url:
            logger.warning("🔥 Firebase database_url이 설정되지 않음")
            return False

        # 절대 경로가 아니면 ~/magi 기준으로 변환
        from pathlib import Path
        sa_full = Path(sa_path)
        if not sa_full.is_absolute():
            sa_full = Path.home() / "magi" / sa_path

        if not sa_full.exists():
            logger.warning(f"🔥 서비스 계정 파일 없음: {sa_full}")
            return False

        cred = credentials.Certificate(str(sa_full))
        firebase_admin.initialize_app(cred, {"databaseURL": db_url})
        _db = db
        _initialized = True
        logger.info("🔥 Firebase 초기화 성공")
        return True

    except Exception as e:
        logger.warning(f"🔥 Firebase 초기화 실패 (트레이딩 영향 없음): {e}")
        return False


# ===== 에이전트 상태 업데이트 =====
def update_agent_status(agent_id, status_data):
    """
    에이전트 상태를 Firebase에 PATCH.
    status_data 예시: {"status": "active", "mood": "coding", "task": "BTC 분석 중"}
    자동으로 updatedAt 타임스탬프 추가.
    """
    if not _initialized:
        return

    try:
        status_data["updatedAt"] = int(time.time() * 1000)  # Unix ms
        ref = _db.reference(f"team_magi/{agent_id}")
        ref.update(status_data)
    except Exception as e:
        logger.warning(f"🔥 상태 업데이트 실패 [{agent_id}]: {e}")


# ===== 이벤트 타임라인 push =====
def push_event(who, action):
    """
    이벤트 타임라인에 새 이벤트 추가.
    who: 에이전트 id (예: "kai", "byeol")
    action: 이벤트 설명 (예: "BTC 롱 진입 ($68,450)")
    """
    if not _initialized:
        return

    try:
        ref = _db.reference("team_magi_events")
        ref.push({
            "who": who,
            "action": action,
            "timestamp": int(time.time() * 1000)
        })
    except Exception as e:
        logger.warning(f"🔥 이벤트 push 실패 [{who}]: {e}")


# ===== 여러 에이전트 일괄 업데이트 =====
def update_all_status(status_map):
    """
    여러 에이전트 상태를 한번에 업데이트.
    status_map: {"kai": {"status": "active", ...}, "jet": {...}, ...}
    """
    if not _initialized:
        return

    try:
        now_ms = int(time.time() * 1000)
        updates = {}
        for agent_id, data in status_map.items():
            data["updatedAt"] = now_ms
            for key, val in data.items():
                updates[f"team_magi/{agent_id}/{key}"] = val
        ref = _db.reference()
        ref.update(updates)
    except Exception as e:
        logger.warning(f"🔥 일괄 상태 업데이트 실패: {e}")


# ===== PnL 요약 업데이트 =====
def update_pnl(date_str, pnl_data):
    """
    오늘의 PnL 데이터를 Firebase에 업데이트.
    date_str: "2026-03-30"
    pnl_data: {"kai": {"pnl": 12.5, "trades": 3}, ...}
    """
    if not _initialized:
        return

    try:
        ref = _db.reference(f"team_magi_pnl/{date_str}")
        ref.update(pnl_data)
    except Exception as e:
        logger.warning(f"🔥 PnL 업데이트 실패: {e}")
