#!/usr/bin/env python3
"""
🔧 MAGI 일일 시스템 점검 — daily_check.py
매일 04:30 KST 자동 실행 (launchd)
8개 항목 점검 → 자동 복구(1회) → 노션 기록 → 텔레그램 발송

설계: 소니 (Claude Opus 4.6) — 2차 설계서 전원 검토 반영
코딩: 별이 (Claude Opus 4.6)
운용: 신지 (Claude Sonnet 4.6, 맥미니)
"""

import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path.home() / "magi"))
from magi_common import (
    load_config, create_logger, send_telegram,
    bybit_public_get, BASE_DIR, KST, ACTIVE_COINS_PATH
)

logger = create_logger("daily_check", "daily_check.log")

# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────
ENGINES = ["kai", "jet", "boomerang", "release"]
ENGINE_NAMES = {"kai": "카이", "jet": "제트", "boomerang": "부메랑", "release": "릴리스"}
ENGINE_ICONS = {"kai": "🌊", "jet": "⚡", "boomerang": "🪃", "release": "👁️"}

# heartbeat 임계값 (초): 스캔주기 + 2분
HEARTBEAT_THRESHOLDS = {
    "kai": 1020,        # 15분 + 2분 = 17분
    "jet": 1020,
    "boomerang": 1020,
    "release": 3720,    # 1시간 + 2분 = 62분
}

# launchd 서비스명
LAUNCHD_SERVICES = {
    "kai": "com.magi.kai",
    "jet": "com.magi.jet",
    "boomerang": "com.magi.boomerang",
    "release": "com.magi.release",
    "watchdog": "com.magi.watchdog",
    "dashboard": "com.magi.dashboard",
}

LOG_DIR = BASE_DIR / "logs"
NOTION_API_BASE = "https://api.notion.com/v1"


# ──────────────────────────────────────────────
# 노션 API 헬퍼
# ──────────────────────────────────────────────
def notion_request(method, endpoint, token, payload=None):
    """노션 API 호출 (urllib)"""
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError
    url = f"{NOTION_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    for attempt in range(3):
        try:
            data = json.dumps(payload).encode() if payload else None
            req = Request(url, data=data, headers=headers, method=method)
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError, TimeoutError) as e:
            logger.warning(f"노션 API 재시도 ({attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(2)
    return None


def notion_insert_check(config, results):
    """신지 점검 로그 DB에 점검 결과 INSERT"""
    token = config["notion"]["token"]
    db_id = config["notion"]["shingi_db"]

    # 노션 properties 구성
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    properties = {
        "점검일시": {"title": [{"text": {"content": now_str}}]},
        "점검유형": {"select": {"name": results["check_type"]}},
        "카이상태": {"select": {"name": results["engines"]["kai"]}},
        "제트상태": {"select": {"name": results["engines"]["jet"]}},
        "부메랑상태": {"select": {"name": results["engines"]["boomerang"]}},
        "릴리스상태": {"select": {"name": results["engines"]["release"]}},
        "watchdog상태": {"select": {"name": results["watchdog"]}},
        "update_all상태": {"select": {"name": results["update_all"]}},
        "마켓센서상태": {"select": {"name": results["market_sensor"]}},
        "종목풀만료": {"select": {"name": results["coin_pool"]}},
        "API상태": {"select": {"name": results["api"]}},
        "디스크사용률": {"number": results["disk_pct"]},
        "에러유무": {"select": {"name": results["error_status"]}},
        "에러건수": {"number": results["error_count"]},
        "자동복구시도": {"select": {"name": results["recovery"]}},
        "로그정리건수": {"number": results["log_cleanup"]},
        "조치사항": {"rich_text": [{"text": {"content": results["actions"][:2000]}}]},
    }

    payload = {"parent": {"database_id": db_id}, "properties": properties}
    resp = notion_request("POST", "/pages", token, payload)
    if resp and resp.get("id"):
        logger.info(f"노션 점검 로그 기록 완료: {resp['id']}")
        return True
    else:
        logger.error(f"노션 점검 로그 기록 실패: {resp}")
        return False


# ──────────────────────────────────────────────
# 점검 1: 엔진 heartbeat 확인
# ──────────────────────────────────────────────
def check_engine_heartbeats():
    """
    각 엔진 heartbeat 확인.
    반환: {engine: ("정상"|"경고"|"중단", elapsed_seconds)}
    """
    results = {}
    for engine in ENGINES:
        hb_path = BASE_DIR / f"heartbeat_{engine}.json"
        threshold = HEARTBEAT_THRESHOLDS[engine]
        try:
            with open(hb_path, "r") as f:
                data = json.load(f)
            elapsed = time.time() - data.get("epoch", 0)
            if elapsed < threshold:
                results[engine] = ("정상", elapsed)
            else:
                results[engine] = ("경고", elapsed)  # heartbeat 만료
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            results[engine] = ("중단", -1)  # 파일 없음
    return results


# ──────────────────────────────────────────────
# 점검 2: watchdog 상태
# ──────────────────────────────────────────────
def check_watchdog():
    """watchdog 로그 마지막 수정 확인. 2분 이내 = 정상"""
    # stdout/stderr 중 더 최근 것을 기준으로 판정
    candidates = [LOG_DIR / "watchdog_stdout.log", LOG_DIR / "watchdog_stderr.log"]
    latest_mtime = 0
    for p in candidates:
        try:
            mt = os.path.getmtime(p)
            if mt > latest_mtime:
                latest_mtime = mt
        except FileNotFoundError:
            pass
    if latest_mtime == 0:
        return "중단", -1
    elapsed = time.time() - latest_mtime
    if elapsed < 120:  # 2분
        return "정상", elapsed
    else:
        return "경고", elapsed


# ──────────────────────────────────────────────
# 점검 3: update_all 상태
# ──────────────────────────────────────────────
def check_update_all():
    """update_all(dashboard) 로그 마지막 수정 확인. 10분 이내 = 정상"""
    # stdout/stderr 중 더 최근 것을 기준으로 판정
    candidates = [LOG_DIR / "dashboard_stdout.log", LOG_DIR / "dashboard_stderr.log"]
    latest_mtime = 0
    for p in candidates:
        try:
            mt = os.path.getmtime(p)
            if mt > latest_mtime:
                latest_mtime = mt
        except FileNotFoundError:
            pass
    if latest_mtime == 0:
        return "오류", -1
    elapsed = time.time() - latest_mtime
    if elapsed < 600:  # 10분
        return "정상", elapsed
    else:
        return "오류", elapsed


# ──────────────────────────────────────────────
# 점검 4: 마켓센서 + 종목풀 만료
# ──────────────────────────────────────────────
def check_market_sensor():
    """
    active_coins.json의 updated_at 확인.
    반환: (sensor_status, pool_status, age_hours)
    """
    try:
        with open(ACTIVE_COINS_PATH, "r") as f:
            data = json.load(f)
        updated_at = data.get("updated_at", "")
        updated_dt = datetime.fromisoformat(updated_at)
        age_hours = (datetime.now(KST) - updated_dt).total_seconds() / 3600
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return "중단", "24h만료", -1

    # 마켓센서 상태 (6시간 주기 → 6.5시간 여유)
    if age_hours < 6.5:
        sensor = "정상"
    elif age_hours < 12:
        sensor = "경고"
    else:
        sensor = "중단"

    # 종목풀 만료 상태
    if age_hours < 12:
        pool = "정상"
    elif age_hours < 24:
        pool = "12h경고"
    else:
        pool = "24h만료"

    return sensor, pool, age_hours


# ──────────────────────────────────────────────
# 점검 5: 바이비트 API 연결
# ──────────────────────────────────────────────
def check_bybit_api():
    """바이비트 /v5/market/time 호출 테스트"""
    try:
        result = bybit_public_get("/v5/market/time")
        if result and result.get("timeSecond"):
            return "정상"
    except Exception:
        pass
    return "오류"


# ──────────────────────────────────────────────
# 점검 6: 디스크 용량
# ──────────────────────────────────────────────
def check_disk():
    """df -h / → 사용률(%) 반환"""
    try:
        result = subprocess.run(
            ["df", "-h", "/"], capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            for p in parts:
                if p.endswith("%"):
                    return int(p.replace("%", ""))
    except Exception:
        pass
    return -1


# ──────────────────────────────────────────────
# 점검 7: 에러 로그 스캔 (최근 24시간)
# ──────────────────────────────────────────────
def check_error_logs():
    """최근 24시간 stderr 로그에서 실제 에러만 수집 (INFO/DEBUG 제외)"""
    error_count = 0
    error_details = []
    cutoff = time.time() - 86400  # 24시간 전
    # 실제 에러 레벨만 필터링 (INFO, DEBUG는 정상 동작 로그)
    ERROR_KEYWORDS = ["[ERROR]", "[CRITICAL]", "[WARNING]", "Traceback", "Exception", "Error:"]

    for log_file in LOG_DIR.glob("*stderr.log"):
        try:
            mtime = os.path.getmtime(log_file)
            if mtime < cutoff:
                continue  # 24시간 이전 파일 스킵
            # 파일 내용에서 에러 라인 추출
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
            recent_errors = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # INFO/DEBUG 레벨은 정상 로그 → 스킵
                if "[INFO]" in line or "[DEBUG]" in line:
                    continue
                # 실제 에러 키워드 포함 여부 확인
                is_error = any(kw in line for kw in ERROR_KEYWORDS)
                if not is_error:
                    continue
                # 타임스탬프 파싱 시도 (2026-03-28 04:30:00 형식)
                try:
                    ts_str = line[:19]
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    ts = ts.replace(tzinfo=KST)
                    if ts.timestamp() >= cutoff:
                        recent_errors.append(line)
                except (ValueError, IndexError):
                    # 타임스탬프 없는 라인 (Traceback 등) — 파일 mtime이 24h 이내이면 포함
                    if mtime >= cutoff:
                        recent_errors.append(line)

            if recent_errors:
                error_count += len(recent_errors)
                fname = log_file.name
                preview = recent_errors[-1][:100]
                error_details.append(f"{fname}: {len(recent_errors)}건 (최신: {preview})")
        except Exception as e:
            logger.warning(f"에러 로그 스캔 실패 ({log_file.name}): {e}")

    return error_count, error_details


# ──────────────────────────────────────────────
# 자동 복구: launchctl kickstart (1회)
# ──────────────────────────────────────────────
def auto_restart(service_key):
    """launchctl kickstart로 서비스 재시작 (1회). 반환: True/False"""
    service_name = LAUNCHD_SERVICES.get(service_key)
    if not service_name:
        return False
    try:
        # uid 가져오기
        uid_result = subprocess.run(
            ["id", "-u"], capture_output=True, text=True, timeout=5
        )
        uid = uid_result.stdout.strip()
        # kickstart -k: 기존 실행 중이면 kill 후 재시작
        result = subprocess.run(
            ["launchctl", "kickstart", "-k", f"gui/{uid}/{service_name}"],
            capture_output=True, text=True, timeout=30
        )
        logger.info(f"자동 재시작 시도: {service_name} → returncode={result.returncode}")
        return result.returncode == 0
    except Exception as e:
        logger.error(f"자동 재시작 실패 ({service_name}): {e}")
        return False


# ──────────────────────────────────────────────
# 30일 이상 로그 자동 삭제
# ──────────────────────────────────────────────
def cleanup_old_logs():
    """30일 이상 로그 파일 삭제. 반환: 삭제 건수"""
    deleted = 0
    cutoff = time.time() - (30 * 86400)
    try:
        for log_file in LOG_DIR.glob("*.log"):
            try:
                mtime = os.path.getmtime(log_file)
                if mtime < cutoff:
                    log_file.unlink()
                    logger.info(f"로그 삭제: {log_file.name}")
                    deleted += 1
            except Exception as e:
                logger.warning(f"로그 삭제 실패 ({log_file.name}): {e}")
    except Exception as e:
        logger.error(f"로그 정리 실패: {e}")
    return deleted


# ──────────────────────────────────────────────
# 텔레그램 리포트 생성
# ──────────────────────────────────────────────
def build_telegram_report(results, check_type="정기"):
    """텔레그램 발송용 메시지 생성"""
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    icon_type = "🔧" if check_type == "정기" else "🚨"

    lines = [
        f"{icon_type} <b>MAGI 일일 점검 완료</b>",
        f"🕐 {now_str}",
        "",
    ]

    # 엔진 상태
    for engine in ENGINES:
        icon = ENGINE_ICONS[engine]
        status = results["engines"][engine]
        emoji = "✅" if status == "정상" else ("⚠️" if status == "경고" else "🚨")
        extra = ""
        if engine in results.get("restart_results", {}):
            r = results["restart_results"][engine]
            extra = f" → 자동 재시작 {'성공' if r else '실패'}"
        lines.append(f"{icon} {ENGINE_NAMES[engine]}: {emoji} {status}{extra}")

    # watchdog
    wd_status = results["watchdog"]
    wd_emoji = "✅" if wd_status == "정상" else ("⚠️" if wd_status == "경고" else "🚨")
    wd_extra = ""
    if "watchdog" in results.get("restart_results", {}):
        r = results["restart_results"]["watchdog"]
        wd_extra = f" → 자동 재시작 {'성공' if r else '실패'}"
    lines.append(f"🐕 watchdog: {wd_emoji} {wd_status}{wd_extra}")

    # update_all
    ua_status = results["update_all"]
    ua_emoji = "✅" if ua_status == "정상" else "🚨"
    ua_extra = ""
    if "dashboard" in results.get("restart_results", {}):
        r = results["restart_results"]["dashboard"]
        ua_extra = f" → 자동 재시작 {'성공' if r else '실패'}"
    lines.append(f"📋 update_all: {ua_emoji} {ua_status}{ua_extra}")

    # 마켓센서
    ms_status = results["market_sensor"]
    ms_emoji = "✅" if ms_status == "정상" else ("⚠️" if ms_status == "경고" else "🚨")
    lines.append(f"📊 마켓센서: {ms_emoji} {ms_status}")

    # API
    api_status = results["api"]
    api_emoji = "✅" if api_status == "정상" else "🚨"
    lines.append(f"🌐 바이비트 API: {api_emoji} {api_status}")

    # 디스크
    disk_pct = results["disk_pct"]
    if disk_pct < 0:
        lines.append("💾 디스크: ❓ 확인 불가")
    elif disk_pct >= 90:
        lines.append(f"💾 디스크: 🚨 {disk_pct}% 사용")
    elif disk_pct >= 80:
        lines.append(f"💾 디스크: ⚠️ {disk_pct}% 사용")
    else:
        lines.append(f"💾 디스크: {disk_pct}% 사용")

    # 에러 로그
    lines.append(f"📝 에러 로그: {results['error_count']}건 (24h)")

    # 로그 정리
    lines.append(f"🧹 로그 정리: {results['log_cleanup']}건 삭제")

    # 최종 결과
    lines.append("")
    has_issues = any([
        any(results["engines"][e] != "정상" for e in ENGINES),
        results["watchdog"] != "정상",
        results["update_all"] != "정상",
        results["market_sensor"] == "중단",
        results["api"] != "정상",
        disk_pct >= 90,
    ])

    recovery = results.get("recovery", "없음")
    if not has_issues:
        lines.append("결과: 전 시스템 정상 ✅")
    elif recovery == "성공":
        issue_count = sum([
            sum(1 for e in ENGINES if results["engines"][e] != "정상"),
            1 if results["watchdog"] != "정상" else 0,
            1 if results["update_all"] != "정상" else 0,
        ])
        lines.append(f"결과: 이상 {issue_count}건 — 자동 복구 완료 ⚠️")
    else:
        lines.append("결과: 이상 발견 — 수동 확인 필요 🚨")

    return "\n".join(lines)


# ──────────────────────────────────────────────
# 메인 점검 로직
# ──────────────────────────────────────────────
def main(check_type="정기"):
    config = load_config()
    logger.info(f"=== MAGI 일일 점검 시작 ({check_type}) ===")

    results = {
        "check_type": check_type,
        "engines": {},
        "watchdog": "정상",
        "update_all": "정상",
        "market_sensor": "정상",
        "coin_pool": "정상",
        "api": "정상",
        "disk_pct": 0,
        "error_status": "없음",
        "error_count": 0,
        "recovery": "없음",
        "log_cleanup": 0,
        "actions": "",
        "restart_results": {},
    }
    actions = []

    # ── 1. 엔진 heartbeat 점검 ──
    logger.info("① 엔진 heartbeat 점검")
    hb_results = check_engine_heartbeats()
    for engine, (status, elapsed) in hb_results.items():
        results["engines"][engine] = status
        if elapsed >= 0:
            logger.info(f"  {ENGINE_NAMES[engine]}: {status} ({elapsed:.0f}초)")
        else:
            logger.warning(f"  {ENGINE_NAMES[engine]}: {status} (heartbeat 파일 없음)")

    # ── 2. watchdog 점검 ──
    logger.info("② watchdog 점검")
    wd_status, wd_elapsed = check_watchdog()
    results["watchdog"] = wd_status
    logger.info(f"  watchdog: {wd_status} ({wd_elapsed:.0f}초)")

    # ── 3. update_all 점검 ──
    logger.info("③ update_all 점검")
    ua_status, ua_elapsed = check_update_all()
    results["update_all"] = ua_status
    logger.info(f"  update_all: {ua_status} ({ua_elapsed:.0f}초)")

    # ── 4. 마켓센서 + 종목풀 점검 ──
    logger.info("④ 마켓센서 + 종목풀 점검")
    ms_status, pool_status, age_hours = check_market_sensor()
    results["market_sensor"] = ms_status
    results["coin_pool"] = pool_status
    if age_hours >= 0:
        logger.info(f"  마켓센서: {ms_status} ({age_hours:.1f}시간)")
    else:
        logger.warning("  마켓센서: 중단 (파일 없음)")
    logger.info(f"  종목풀: {pool_status}")

    # ── 5. 바이비트 API 점검 ──
    logger.info("⑤ 바이비트 API 점검")
    api_status = check_bybit_api()
    results["api"] = api_status
    logger.info(f"  바이비트 API: {api_status}")

    # ── 6. 디스크 점검 ──
    logger.info("⑥ 디스크 점검")
    disk_pct = check_disk()
    results["disk_pct"] = disk_pct
    logger.info(f"  디스크 사용률: {disk_pct}%")

    # ── 7. 에러 로그 스캔 ──
    logger.info("⑦ 에러 로그 스캔 (최근 24시간)")
    error_count, error_details = check_error_logs()
    results["error_count"] = error_count
    results["error_status"] = "없음" if error_count == 0 else "있음"
    logger.info(f"  에러: {error_count}건")
    for detail in error_details:
        logger.info(f"    {detail}")

    # ── 8. 자동 복구 (이상 시 1회) ──
    logger.info("⑧ 자동 복구 판정")
    need_restart = []

    # 엔진 이상 → kickstart
    for engine in ENGINES:
        if results["engines"][engine] != "정상":
            need_restart.append(engine)

    # watchdog 이상 → kickstart
    if results["watchdog"] != "정상":
        need_restart.append("watchdog")

    # update_all 이상 → kickstart
    if results["update_all"] != "정상":
        need_restart.append("dashboard")

    if need_restart:
        logger.info(f"  자동 재시작 대상: {need_restart}")
        restart_success = True
        for svc in need_restart:
            ok = auto_restart(svc)
            results["restart_results"][svc] = ok
            action_name = ENGINE_NAMES.get(svc, svc)
            if ok:
                actions.append(f"{action_name} 자동 재시작 시도")
            else:
                actions.append(f"{action_name} 자동 재시작 실패")
                restart_success = False

        # 60초 대기 후 재확인
        logger.info("  60초 대기 후 재확인...")
        time.sleep(60)

        # 재확인
        recheck_all_ok = True
        for svc in need_restart:
            if svc in ENGINES:
                hb_path = BASE_DIR / f"heartbeat_{svc}.json"
                threshold = HEARTBEAT_THRESHOLDS[svc]
                try:
                    with open(hb_path, "r") as f:
                        data = json.load(f)
                    elapsed = time.time() - data.get("epoch", 0)
                    if elapsed < threshold:
                        results["engines"][svc] = "정상"
                        actions.append(f"{ENGINE_NAMES[svc]} 재시작 후 정상 확인")
                        logger.info(f"  재확인 {ENGINE_NAMES[svc]}: 정상 ({elapsed:.0f}초)")
                    else:
                        recheck_all_ok = False
                        logger.warning(f"  재확인 {ENGINE_NAMES[svc]}: 여전히 이상 ({elapsed:.0f}초)")
                except Exception:
                    recheck_all_ok = False
            elif svc == "watchdog":
                wd_s, wd_e = check_watchdog()
                if wd_s == "정상":
                    results["watchdog"] = "정상"
                    actions.append("watchdog 재시작 후 정상 확인")
                else:
                    recheck_all_ok = False
            elif svc == "dashboard":
                ua_s, ua_e = check_update_all()
                if ua_s == "정상":
                    results["update_all"] = "정상"
                    actions.append("update_all 재시작 후 정상 확인")
                else:
                    recheck_all_ok = False

        if recheck_all_ok:
            results["recovery"] = "성공"
            logger.info("  자동 복구 성공 ✅")
        else:
            results["recovery"] = "실패"
            logger.warning("  자동 복구 실패 — 수동 확인 필요 🚨")
    else:
        logger.info("  이상 없음 — 자동 복구 불필요")

    # ── 추가 알림 (Level 2) ──
    # 마켓센서 만료경고
    if results["market_sensor"] == "경고":
        actions.append(f"마켓센서 {age_hours:.1f}시간 미갱신 — 수동 실행 필요")
    elif results["market_sensor"] == "중단":
        actions.append(f"마켓센서 {age_hours:.1f}시간 미갱신 — 신규 진입 중단 상태")

    # API 오류
    if results["api"] != "정상":
        actions.append("바이비트 API 연결 실패 — 거래 불가 상태")

    # 디스크 위험
    if disk_pct >= 90:
        actions.append(f"디스크 사용률 {disk_pct}% — 로그 정리 필요")
    elif disk_pct >= 80:
        actions.append(f"디스크 사용률 {disk_pct}% — 주의")

    # 에러 로그
    if error_count > 0:
        actions.append(f"최근 24시간 에러 로그 {error_count}건")

    results["actions"] = " / ".join(actions) if actions else "이상 없음"

    # ── 9. 30일 이상 로그 자동 삭제 ──
    logger.info("⑨ 30일 이상 로그 자동 삭제")
    log_cleanup = cleanup_old_logs()
    results["log_cleanup"] = log_cleanup
    logger.info(f"  삭제: {log_cleanup}건")

    # ── 10. 노션 기록 ──
    logger.info("⑩ 노션 점검 로그 기록")
    notion_ok = notion_insert_check(config, results)
    if not notion_ok:
        actions.append("노션 기록 실패")
        results["actions"] = " / ".join(actions) if actions else results["actions"]

    # ── 11. 텔레그램 발송 (항상) ──
    logger.info("⑪ 텔레그램 리포트 발송")
    report = build_telegram_report(results, check_type)
    send_ok = send_telegram(config, report)
    if send_ok:
        logger.info("  텔레그램 발송 완료 ✅")
    else:
        logger.error("  텔레그램 발송 실패 ❌")

    logger.info(f"=== MAGI 일일 점검 완료 ===")
    return results


if __name__ == "__main__":
    try:
        # 인자로 "긴급" 전달 시 긴급 점검
        ctype = "긴급" if len(sys.argv) > 1 and sys.argv[1] == "긴급" else "정기"
        main(ctype)
    except Exception as e:
        logger.critical(f"daily_check 크래시: {e}\n{traceback.format_exc()}")
        try:
            send_telegram(load_config(), f"🚨 [daily_check] 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
