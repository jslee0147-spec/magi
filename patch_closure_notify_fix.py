#!/usr/bin/env python3
r"""
patch_closure_notify_fix.py — 엔진 문법 오류 복구 + 재패치
설계: 별이

문제: patch_closure_notify.py가 엔진의 try 블록 안에 새 try/except를
삽입하면서 문법 오류 발생. 이 스크립트가:
  1. 잘못 삽입된 코드 블록 제거 (복구)
  2. try/except 없이 단순 호출로 재삽입 (check_and_notify_closures 자체에 예외 처리 내장)

사용법 (맥 터미널):
  cp ~/Desktop/OneDrive/팀\ 마기/별이/patch_closure_notify_fix.py ~/magi/
  cd ~/magi && python3 patch_closure_notify_fix.py
"""
import sys
from pathlib import Path

BASE = Path(__file__).parent
if not (BASE / "magi_common.py").exists():
    BASE = Path.home() / "magi"

ENGINES = {
    "engine_kai.py":       ("카이", "🌊"),
    "engine_jet.py":       ("제트", "⚡"),
    "engine_boomerang.py": ("부메랑", "🪃"),
    "engine_release.py":   ("릴리스", "👁\ufe0f"),
}

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    tmp.rename(path)

def verify_syntax(path):
    import py_compile
    try:
        py_compile.compile(str(path), doraise=True)
        return True
    except py_compile.PyCompileError as e:
        print(f"  ❌  문법 오류: {e}")
        return False


def fix_engine(filename, name, icon):
    """엔진 파일에서 잘못된 삽입 제거 후 올바르게 재삽입"""
    path = BASE / filename
    if not path.exists():
        print(f"  ⚠️  {filename} 없음 — 건너뜀")
        return False

    content = read_file(path)

    # ─── 1단계: 잘못 삽입된 코드 블록 제거 ───
    # 삽입된 블록의 시작/끝 마커로 찾기
    marker = "# ── closed-pnl 기반 청산 알림 (별이 + 성단) ──"

    if marker in content:
        lines = content.split("\n")
        new_lines = []
        skip = False
        for line in lines:
            stripped = line.strip()
            if marker in line:
                skip = True
                continue
            if skip:
                # 삽입 블록의 끝 감지: 빈 줄이거나 블록 외부 코드
                if stripped == "":
                    continue  # 빈 줄 건너뜀
                if stripped.startswith("mc.check_and_notify_closures"):
                    continue
                if stripped.startswith("try:") and "check" not in stripped:
                    # 원래 코드의 try: 가 아닌 삽입된 try:
                    continue
                if stripped.startswith('logger.warning(f"청산 알림 체크 실패'):
                    continue
                if stripped.startswith("except Exception as _e:"):
                    continue
                if stripped == ")":
                    continue
                # 인자 줄들 (api_key, api_secret, name, icon, config, logger)
                if skip and stripped.startswith(("api_key", "api_secret", '"' + name, '"' + icon, "config,", "logger")):
                    continue
                if skip and stripped.startswith(("mc.check", '"%s"' % name, '"%s"' % icon)):
                    continue
                # 닫는 괄호
                if stripped == ")":
                    continue
                # 블록 끝 — 이제 원래 코드
                skip = False
                new_lines.append(line)
            else:
                new_lines.append(line)

        content = "\n".join(new_lines)
        print(f"  ✅  잘못된 삽입 제거 완료")
    else:
        print(f"  ℹ️  삽입된 블록 없음 (이미 깨끗하거나 미적용)")

    # 이미 올바르게 적용된 경우
    if "check_and_notify_closures" in content:
        # 문법 검증
        if verify_syntax(path):
            print(f"  ✅  {filename} 이미 정상")
            return True
        else:
            # 여전히 문법 오류 → 강제 제거 후 재삽입
            lines = content.split("\n")
            content = "\n".join(
                l for l in lines
                if "check_and_notify_closures" not in l
                and "청산 알림 체크 실패" not in l
                and "closed-pnl 기반 청산 알림" not in l
            )
            print(f"  ✅  강제 제거 완료")

    # ─── 2단계: 올바르게 재삽입 ───
    # 전략: while True 루프의 time.sleep 바로 앞에 단순 호출 삽입
    # check_and_notify_closures()는 내부에서 모든 예외를 처리하므로
    # try/except 래퍼 불필요

    # 삽입할 코드 (try/except 없이 단순 호출)
    call_line = (
        f'\n        # ── closed-pnl 기반 청산 알림 (별이 + 성단) ──\n'
        f'        mc.check_and_notify_closures(\n'
        f'            api_key, api_secret,\n'
        f'            "{name}", "{icon}", config, logger\n'
        f'        )\n'
    )

    inserted = False

    # 방법 1: time.sleep 앞에 삽입 (가장 안전 — 루프 끝, try 블록 밖)
    sleep_patterns = [
        "        time.sleep(SCAN_INTERVAL)",
        "        time.sleep(scan_interval)",
        "        time.sleep(interval)",
        "        time.sleep(",
    ]
    for sp in sleep_patterns:
        if sp in content:
            content = content.replace(sp, call_line + sp, 1)
            inserted = True
            print(f"  ✅  time.sleep 앞에 삽입 완료")
            break

    # 방법 2: while True 루프의 except 블록 뒤에 삽입
    if not inserted:
        # "except" 후 logger.error 패턴 찾기
        lines = content.split("\n")
        for i, line in enumerate(lines):
            if "while True" in line or "while running" in line:
                # while 루프 발견 — 그 안의 마지막 except 블록 뒤에 삽입
                # 단, 이 방법은 복잡하므로 수동 안내
                break

    if not inserted:
        print(f"  ⚠️  자동 삽입 실패 — 수동 추가 필요:")
        print(f"       메인 루프(while True)의 time.sleep 바로 앞에:")
        print(f"       mc.check_and_notify_closures(api_key, api_secret, \"{name}\", \"{icon}\", config, logger)")
        # 문법은 복구해서 저장
        write_file(path, content)
        verify_syntax(path)
        return False

    # ─── 3단계: 저장 + 문법 검증 ───
    write_file(path, content)
    if verify_syntax(path):
        print(f"  ✅  {filename} ({name}) — 패치 완료!")
        return True
    else:
        print(f"  ❌  {filename} ({name}) — 여전히 문법 오류")
        print(f"       수동 확인 필요: {path}")
        return False


def main():
    print("=" * 60)
    print("🔧 MAGI 청산 알림 패치 — 엔진 복구 + 재패치")
    print("   설계: 별이")
    print("=" * 60)

    # magi_common.py 확인
    mc_path = BASE / "magi_common.py"
    if "check_and_notify_closures" not in read_file(mc_path):
        print("\n⚠️  magi_common.py에 check_and_notify_closures가 없습니다")
        print("   먼저 patch_closure_notify.py를 실행해주세요")
        sys.exit(1)
    else:
        print("\n✅ magi_common.py — check_and_notify_closures 확인됨")

    # 4개 엔진 복구 + 재패치
    results = {}
    for filename, (name, icon) in ENGINES.items():
        print(f"\n{icon} {name} ({filename})")
        results[filename] = fix_engine(filename, name, icon)

    # 초기화: notified_closures 파일 생성 (빈 파일)
    print("\n📋 초기 알림 목록 생성")
    for filename, (name, icon) in ENGINES.items():
        notified_path = BASE / f"notified_closures_{name}.json"
        if not notified_path.exists():
            with open(notified_path, "w") as f:
                f.write("[]")
            print(f"  ✅  {name} — 빈 목록 생성")
        else:
            print(f"  ✅  {name} — 이미 존재")

    # 최종 결과
    print("\n" + "=" * 60)
    print("최종 결과:")
    for target, success in results.items():
        status = "✅ 성공" if success else "❌ 실패"
        print(f"  {target}: {status}")

    success_count = sum(1 for v in results.values() if v)
    print(f"\n{success_count}/4 엔진 패치 완료")

    if success_count == 4:
        print("\n📋 다음 단계:")
        print("  1. 엔진 재시작:")
        print("     launchctl unload ~/Library/LaunchAgents/com.magi.engine_*.plist")
        print("     launchctl load ~/Library/LaunchAgents/com.magi.engine_*.plist")
        print("  2. 로그 확인: tail -f ~/magi/logs/engine_kai_stderr.log")
        print("  3. 테스트 후 텔레그램에서 청산 알림 확인")
    elif success_count > 0:
        failed = [k for k, v in results.items() if not v]
        print(f"\n⚠️  실패한 엔진: {', '.join(failed)}")
        print("   해당 엔진 코드를 수동으로 확인해주세요")

    print("=" * 60)


if __name__ == "__main__":
    main()
