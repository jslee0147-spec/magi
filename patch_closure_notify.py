#!/usr/bin/env python3
"""
patch_closure_notify.py — closed-pnl 기반 청산 알림 시스템 (근본 해결)
설계: 별이 + 성단(星團)
대상: ~/magi/magi_common.py + 4개 엔진

=== 문제 ===
오류 #10에서 "tp1_hit 분기 추가"로 해결했다고 기록했지만,
실제로는 청산 알림이 여전히 안 옴.

근본 원인: TP/SL은 바이비트 거래소 측에서 자동 실행되는데,
엔진의 manage_position()이 이를 감지하지 못하는 구조적 문제.
- 진입: 엔진이 직접 주문 → 즉시 감지 → 알림 OK
- 청산: 거래소가 자동 실행 → 엔진은 모름 → 알림 누락

=== 해결 ===
closed-pnl API를 활용한 청산 감지.
바이비트 closed-pnl은 모든 청산(TP/SL/수동/강제) 내역을 기록하므로,
이를 매 스캔마다 조회하여 새 청산 건이 있으면 텔레그램 알림 발송.

수정 내용:
  [magi_common.py] check_and_notify_closures() 함수 추가
  [4개 엔진]      scan_once() 끝에 check_and_notify_closures() 호출 추가

사용법 (맥 터미널):
  cp ~/Desktop/OneDrive/팀\ 마기/별이/patch_closure_notify.py ~/magi/
  cd ~/magi && python3 patch_closure_notify.py
"""
import sys
from pathlib import Path

BASE = Path(__file__).parent
if not (BASE / "magi_common.py").exists():
    BASE = Path.home() / "magi"

# ─────────────────────────────────────────────
# 공통 패치 유틸
# ─────────────────────────────────────────────
def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    tmp.rename(path)

def patch(content, old, new, label):
    if old not in content:
        print(f"  ⚠️  [{label}] 대상 코드를 찾을 수 없습니다 — 이미 적용되었거나 코드 구조가 다릅니다")
        return content, False
    if new in content:
        print(f"  ✅  [{label}] 이미 적용됨 — 건너뜀")
        return content, False
    content = content.replace(old, new, 1)
    print(f"  ✅  [{label}] 패치 완료")
    return content, True

def verify_syntax(path):
    import py_compile
    try:
        py_compile.compile(str(path), doraise=True)
        return True
    except py_compile.PyCompileError as e:
        print(f"  ❌  문법 오류: {e}")
        return False

# ─────────────────────────────────────────────
# 수정 1: magi_common.py에 청산 알림 함수 추가
# ─────────────────────────────────────────────
CLOSURE_NOTIFY_FUNC = '''

# ─────────────────────────────────────────────
# closed-pnl 기반 청산 알림 (별이 + 성단 설계)
# ─────────────────────────────────────────────
def fetch_recent_closed_pnl(api_key, api_secret, limit=20):
    """최근 청산 내역 조회 (알림용, 최신 limit건)"""
    params = {
        "category": "linear",
        "limit": str(limit),
    }
    result = http_get("/v5/position/closed-pnl", params, api_key, api_secret)
    if result and result.get("retCode") == 0:
        return result.get("result", {}).get("list", [])
    return []


def check_and_notify_closures(api_key, api_secret, strategy_name, strategy_icon, config, logger):
    """closed-pnl 기반 청산 알림 — 새 청산 감지 시 텔레그램 발송

    매 스캔마다 호출. 바이비트 closed-pnl에서 새 청산 건을 발견하면
    텔레그램으로 알림을 보내고, 알림 완료 목록을 로컬 파일에 저장.

    Args:
        api_key: 바이비트 API 키
        api_secret: 바이비트 API 시크릿
        strategy_name: 전략 이름 (예: "카이", "제트")
        strategy_icon: 전략 아이콘 (예: "🌊", "⚡")
        config: 설정 dict (텔레그램 설정 포함)
        logger: 로거
    """
    import json as _json
    from pathlib import Path as _Path

    notified_path = _Path(__file__).parent / f"notified_closures_{strategy_name}.json"

    # 1. 알림 완료 목록 로드
    notified = set()
    try:
        if notified_path.exists():
            with open(notified_path, "r") as f:
                notified = set(_json.load(f))
    except Exception as e:
        logger.warning(f"[{strategy_name}] 알림 목록 로드 실패: {e}")

    # 2. 최근 closed-pnl 조회
    try:
        closures = fetch_recent_closed_pnl(api_key, api_secret)
    except Exception as e:
        logger.warning(f"[{strategy_name}] closed-pnl 조회 실패: {e}")
        return

    if not closures:
        return

    # 3. 새 청산 건 감지 → 텔레그램 알림
    new_count = 0
    for c in closures:
        order_id = c.get("orderId", "")
        if not order_id or order_id in notified:
            continue

        symbol = c.get("symbol", "?")
        side = c.get("side", "?")
        pnl = float(c.get("closedPnl", 0))
        closed_size = c.get("closedSize", "?")
        exit_price = c.get("avgExitPrice", "?")
        entry_price = c.get("avgEntryPrice", "?")

        # PnL로 청산 유형 추론
        if pnl > 0:
            exit_type = "🎯 익절"
        elif pnl < 0:
            exit_type = "🛑 손절"
        else:
            exit_type = "⚪ 청산"

        side_kr = "롱" if side == "Buy" else "숏"

        msg = (
            f"{strategy_icon} [{strategy_name}] {exit_type}\\n"
            f"  {symbol} {side_kr} ×{closed_size}\\n"
            f"  진입: {entry_price} → 청산: {exit_price}\\n"
            f"  PnL: ${pnl:+.2f}"
        )

        try:
            send_telegram(config, msg)
            logger.info(f"{strategy_icon} [{strategy_name}] 청산 알림: {symbol} {side_kr} {exit_type} PnL=${pnl:+.2f}")
        except Exception as e:
            logger.error(f"[{strategy_name}] 청산 알림 발송 실패: {e}")

        notified.add(order_id)
        new_count += 1

    # 4. 알림 완료 목록 저장 (최근 500건만 유지, 무한 증가 방지)
    if new_count > 0:
        try:
            notified_list = sorted(notified)[-500:]
            tmp_path = notified_path.with_suffix(".tmp")
            with open(tmp_path, "w") as f:
                _json.dump(notified_list, f)
            tmp_path.rename(notified_path)
            logger.info(f"[{strategy_name}] 새 청산 {new_count}건 알림 완료")
        except Exception as e:
            logger.error(f"[{strategy_name}] 알림 목록 저장 실패: {e}")
'''

# ─────────────────────────────────────────────
# 수정 2: 4개 엔진에 check_and_notify_closures() 호출 추가
# ─────────────────────────────────────────────
ENGINES = {
    "engine_kai.py":       ("카이", "🌊"),
    "engine_jet.py":       ("제트", "⚡"),
    "engine_boomerang.py": ("부메랑", "🪃"),
    "engine_release.py":   ("릴리스", "👁️"),
}


def patch_magi_common():
    """magi_common.py에 check_and_notify_closures() 함수 추가"""
    print("\n📦 magi_common.py — 청산 알림 함수 추가")
    path = BASE / "magi_common.py"
    if not path.exists():
        print(f"  ❌ 파일 없음: {path}")
        return False

    content = read_file(path)

    # 이미 적용됐는지 확인
    if "check_and_notify_closures" in content:
        print("  ✅ 이미 적용됨 — 건너뜀")
        return True

    # http_get 함수가 존재하는지 확인 (API 호출 기반)
    if "def http_get" not in content:
        print("  ❌ http_get 함수를 찾을 수 없습니다 — magi_common.py 구조가 예상과 다릅니다")
        return False

    # send_telegram 함수가 존재하는지 확인
    if "def send_telegram" not in content:
        print("  ❌ send_telegram 함수를 찾을 수 없습니다")
        return False

    # 파일 끝에 함수 추가
    content = content.rstrip() + "\n" + CLOSURE_NOTIFY_FUNC + "\n"

    write_file(path, content)
    if verify_syntax(path):
        print("  ✅ magi_common.py 패치 완료")
        return True
    else:
        print("  ❌ 문법 오류 — 패치 실패")
        return False


def patch_engine(filename, name, icon):
    """엔진 파일에 check_and_notify_closures() 호출 추가"""
    path = BASE / filename
    if not path.exists():
        print(f"  ⚠️  {filename} 없음 — 건너뜀")
        return False

    content = read_file(path)

    # 이미 적용됐는지 확인
    if "check_and_notify_closures" in content:
        print(f"  ✅  {filename} ({name}) — 이미 적용됨")
        return True

    changes = 0

    # ──────────────────────────────────────────
    # 방법 A: scan_once() 함수 끝에 추가
    # 엔진마다 scan_once() 안에서 스캔 완료 후 청산 체크
    # ──────────────────────────────────────────

    # 패턴: scan_once() 안의 마지막 logger.info (스캔 완료 로그)
    # 일반적으로 "스캔 완료" 또는 "scan complete" 로그 후에 추가

    # 방법 1: manage_existing() 호출부 뒤에 추가
    # manage_existing이 포함된 for 루프 뒤에 청산 체크 추가
    closure_check_code = f'''
        # ── closed-pnl 기반 청산 알림 (별이 + 성단) ──
        try:
            mc.check_and_notify_closures(
                api_key, api_secret,
                "{name}", "{icon}", config, logger
            )
        except Exception as _e:
            logger.warning(f"청산 알림 체크 실패: {{_e}}")
'''

    # 패턴: 각 엔진의 스캔 루프 마지막에 sleep이 있음
    # "time.sleep" 바로 앞에 삽입
    sleep_patterns = [
        "        time.sleep(SCAN_INTERVAL)",
        "        time.sleep(scan_interval)",
        "        time.sleep(interval)",
    ]

    inserted = False
    for sp in sleep_patterns:
        if sp in content:
            content, ok = patch(content,
                sp,
                closure_check_code + sp,
                f"{name} — closed-pnl 청산 알림 체크 추가"
            )
            if ok:
                changes += 1
                inserted = True
            break

    # 패턴 2: sleep을 못 찾으면 "logger.info" 스캔 완료 로그 뒤에 시도
    if not inserted:
        scan_complete_patterns = [
            f'        logger.info(f"🌊',
            f'        logger.info(f"⚡',
            f'        logger.info(f"🪃',
            f'        logger.info(f"👁',
            '        logger.info(f"스캔 완료',
            '        logger.info("스캔 완료',
        ]
        for sp in scan_complete_patterns:
            if sp in content:
                # 해당 줄의 끝을 찾아서 그 다음에 삽입
                idx = content.index(sp)
                # 줄 끝 찾기
                newline_idx = content.index("\n", idx)
                before = content[:newline_idx + 1]
                after = content[newline_idx + 1:]
                content = before + closure_check_code + after
                changes += 1
                inserted = True
                print(f"  ✅  [{name} — closed-pnl 청산 알림 체크 추가 (스캔 로그 뒤)] 패치 완료")
                break

    if not inserted:
        print(f"  ⚠️  {filename} ({name}) — 삽입 위치를 찾지 못함. 수동 추가 필요:")
        print(f"       scan_once() 또는 메인 루프 끝에 다음 코드를 추가하세요:")
        print(f'       mc.check_and_notify_closures(api_key, api_secret, "{name}", "{icon}", config, logger)')
        return False

    if changes > 0:
        write_file(path, content)
        if verify_syntax(path):
            print(f"  ✅  {filename} ({name}) — 패치 완료")
            return True
        else:
            print(f"  ❌  {filename} ({name}) — 문법 오류")
            return False

    return False


def init_notified_files():
    """기존 closed-pnl을 초기 알림 완료 목록으로 등록 (첫 실행 시 폭주 방지)

    패치 적용 직후 엔진이 시작되면, 기존 모든 청산 건에 대해
    텔레그램이 한꺼번에 폭주할 수 있음. 이를 방지하기 위해
    현재까지의 closed-pnl을 "이미 알림 완료"로 등록.
    """
    import json

    print("\n📋 초기 알림 완료 목록 생성 (알림 폭주 방지)")

    config_path = BASE / "config.json"
    if not config_path.exists():
        print("  ⚠️  config.json 없음 — 초기화 건너뜀 (첫 실행 시 수동 확인 필요)")
        return

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        print(f"  ⚠️  config.json 읽기 실패: {e}")
        return

    strategies = config.get("strategies", {})

    for strat_key, (name, icon) in ENGINES.items():
        strat_name_map = {
            "engine_kai.py": "kai",
            "engine_jet.py": "jet",
            "engine_boomerang.py": "boomerang",
            "engine_release.py": "release",
        }
        strat_id = strat_name_map.get(strat_key, "")
        strat_config = strategies.get(strat_id, {})
        api_key = strat_config.get("api_key", "")
        api_secret = strat_config.get("api_secret", "")

        if not api_key or not api_secret:
            print(f"  ⚠️  {name} — API 키 없음, 건너뜀")
            continue

        notified_path = BASE / f"notified_closures_{name}.json"
        if notified_path.exists():
            print(f"  ✅  {name} — 이미 초기화됨")
            continue

        try:
            # magi_common import 시도
            sys.path.insert(0, str(BASE))
            import magi_common as mc
            closures = mc.fetch_recent_closed_pnl(api_key, api_secret, limit=100)

            existing_ids = [c.get("orderId", "") for c in closures if c.get("orderId")]
            with open(notified_path, "w") as f:
                json.dump(existing_ids, f)
            print(f"  ✅  {name} — {len(existing_ids)}건 초기 등록 완료")
        except Exception as e:
            # magi_common이 없거나 API 호출 실패 시 빈 파일 생성
            print(f"  ⚠️  {name} — API 조회 실패 ({e}), 빈 목록으로 초기화")
            print(f"       ⚠️  첫 실행 시 기존 청산 건에 대한 알림이 발생할 수 있습니다")
            with open(notified_path, "w") as f:
                json.dump([], f)


def main():
    print("=" * 60)
    print("🔔 MAGI 청산 알림 패치 (closed-pnl 기반 근본 해결)")
    print("   설계: 별이 + 성단(星團)")
    print("   대상: magi_common.py + 4개 엔진")
    print("=" * 60)

    if not (BASE / "magi_common.py").exists():
        print(f"\n❌ magi_common.py를 찾을 수 없습니다: {BASE}")
        print("   ~/magi/ 디렉토리에서 실행해주세요.")
        sys.exit(1)

    results = {}

    # 1단계: magi_common.py 패치
    results["magi_common"] = patch_magi_common()

    if not results["magi_common"]:
        print("\n❌ magi_common.py 패치 실패 — 엔진 패치를 진행할 수 없습니다")
        sys.exit(1)

    # 2단계: 4개 엔진 패치
    for filename, (name, icon) in ENGINES.items():
        print(f"\n{icon} {name} ({filename})")
        results[filename] = patch_engine(filename, name, icon)

    # 3단계: 초기 알림 완료 목록 생성 (선택적)
    print("\n" + "-" * 60)
    try:
        init_notified_files()
    except Exception as e:
        print(f"  ⚠️  초기화 실패: {e}")
        print("  → 패치는 정상 적용됨. 첫 실행 시 기존 청산 알림이 올 수 있음")

    # 최종 결과
    print("\n" + "=" * 60)
    print("최종 결과:")
    for target, success in results.items():
        status = "✅ 성공" if success else "❌ 실패/건너뜀"
        print(f"  {target}: {status}")

    success_count = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\n{success_count}/{total} 패치 완료")

    if success_count >= 2:  # magi_common + 최소 1개 엔진
        print("\n📋 배포 후 확인사항:")
        print("  1. 엔진 재시작: launchctl unload/load 또는 서비스 재시작")
        print("  2. 첫 스캔 후 로그 확인: ~/magi/logs/engine_*.log")
        print("  3. 테스트: 수동으로 작은 포지션 진입 후 TP/SL로 청산 → 알림 확인")
        print("  4. notified_closures_*.json 파일이 ~/magi/에 생성되는지 확인")

    print("=" * 60)


if __name__ == "__main__":
    main()
