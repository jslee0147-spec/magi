#!/usr/bin/env python3
"""
MAGI 엔진 로그 패치 — manage_existing() 청산/TP1 로그 추가
설계: 소니 | 구현: 별이

문제: 4개 엔진 manage_existing()에서 청산/TP1 시 텔레그램만 보내고 logger.info() 없음
수정: 각 result 분기에 logger.info() 추가

사용법 (맥 터미널):
  cp ~/Desktop/OneDrive/팀\ 마기/별이/patch_engine_logs.py ~/magi/
  cd ~/magi && python3 patch_engine_logs.py
"""
import sys
from pathlib import Path

BASE = Path(__file__).parent
if not (BASE / "engine_kai.py").exists():
    BASE = Path.home() / "magi"

ENGINES = {
    "engine_kai.py":       ("카이", "🌊"),
    "engine_jet.py":       ("제트", "⚡"),
    "engine_boomerang.py": ("부메랑", "🪃"),
    "engine_release.py":   ("릴리스", "👁️"),
}

def patch_file(filepath, name, icon):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    changes = 0

    # 패치 1: "closed" 분기에 로그 추가
    old_closed = '''            if result == "closed":
                send_telegram(config,'''
    new_closed = f'''            if result == "closed":
                logger.info(f"{icon} [{name}] 청산: {{pos['symbol']}} {{side_kr}} PnL=${{float(pos.get('unrealisedPnl', 0)):+.2f}}")
                send_telegram(config,'''

    if old_closed in content and new_closed not in content:
        content = content.replace(old_closed, new_closed, 1)
        changes += 1

    # 패치 2: "tp1_hit" 분기에 로그 추가
    old_tp1 = '''            elif result == "tp1_hit":
                send_telegram(config,'''
    new_tp1 = f'''            elif result == "tp1_hit":
                logger.info(f"{icon} [{name}] TP1 도달: {{pos['symbol']}} {{side_kr}} (50% 청산, SL→진입가)")
                send_telegram(config,'''

    if old_tp1 in content and new_tp1 not in content:
        content = content.replace(old_tp1, new_tp1, 1)
        changes += 1

    if changes > 0:
        tmp = filepath.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        tmp.rename(filepath)

    return changes

def main():
    print("=" * 50)
    print("🔧 엔진 로그 패치 시작")
    print("=" * 50)

    total = 0
    for filename, (name, icon) in ENGINES.items():
        filepath = BASE / filename
        if not filepath.exists():
            print(f"  ⚠️  {filename} 없음 — 건너뜀")
            continue
        changes = patch_file(filepath, name, icon)
        if changes > 0:
            print(f"  ✅  {filename} ({name}) — {changes}건 패치")
        else:
            print(f"  ✅  {filename} ({name}) — 이미 적용됨")
        total += changes

    print("=" * 50)
    print(f"✅ 총 {total}건 패치 완료")
    print("=" * 50)

if __name__ == "__main__":
    main()
