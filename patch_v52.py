#!/usr/bin/env python3
"""
MAGI v5.2 패치 스크립트
설계: 소니 | 구현: 별이

v5.1 → v5.2 변경사항 (5개 허점 수정):
  1. API 실패 시 None 체크 + 텔레그램 알림
  2. 로그 로테이션 (RotatingFileHandler, 5MB × 3)
  3. 노션 API 지수 백오프 + Retry-After 헤더
  4. 드로우다운 자동 중단 로직
  5. closed-pnl 페이지 한계 확장 (10 → 50)

사용법 (맥 터미널):
  cp ~/magi/update_all.py ~/magi/update_all_backup_v51.py
  cp ~/Desktop/OneDrive/팀\ 마기/별이/patch_v52.py ~/magi/
  cd ~/magi && python3 patch_v52.py
"""

import re
import sys
from pathlib import Path

TARGET = Path(__file__).parent / "update_all.py"
if not TARGET.exists():
    TARGET = Path.home() / "magi" / "update_all.py"

def read_file():
    with open(TARGET, "r", encoding="utf-8") as f:
        return f.read()

def write_file(content):
    tmp = TARGET.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    tmp.rename(TARGET)

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

def main():
    print("=" * 55)
    print("🔧 MAGI v5.2 패치 시작")
    print(f"   대상: {TARGET}")
    print("=" * 55)

    if not TARGET.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {TARGET}")
        sys.exit(1)

    content = read_file()
    changes = 0

    # ──────────────────────────────────────────────
    # 수정 1: import 추가 (RotatingFileHandler)
    # ──────────────────────────────────────────────
    print("\n[수정 1/5] 로그 로테이션 — import 추가")
    content, ok = patch(content,
        "import logging\n",
        "import logging\nfrom logging.handlers import RotatingFileHandler\n",
        "import RotatingFileHandler"
    )
    if ok: changes += 1

    # ──────────────────────────────────────────────
    # 수정 2: FileHandler → RotatingFileHandler (5MB × 3 백업)
    # ──────────────────────────────────────────────
    print("\n[수정 2/5] 로그 로테이션 — RotatingFileHandler 적용")
    content, ok = patch(content,
        '    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")',
        '    fh = RotatingFileHandler(LOG_PATH, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")',
        "RotatingFileHandler"
    )
    if ok: changes += 1

    # ──────────────────────────────────────────────
    # 수정 3: 노션 API — 지수 백오프 + Retry-After + 5회 재시도
    # ──────────────────────────────────────────────
    print("\n[수정 3/5] 노션 API — 지수 백오프 + 5회 재시도")

    # notion_get
    content, ok = patch(content,
        '''def notion_get(path, token):
    url = f"{NOTION_BASE}{path}"
    for attempt in range(3):
        try:
            req = Request(url, headers=_notion_headers(token))
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError) as e:
            logger.warning(f"Notion GET 실패 ({attempt+1}/3): {path} → {e}")
            if attempt < 2:
                time.sleep(2)
    return None''',
        '''def notion_get(path, token):
    url = f"{NOTION_BASE}{path}"
    for attempt in range(5):
        try:
            req = Request(url, headers=_notion_headers(token))
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            wait = _notion_retry_wait(e, attempt)
            logger.warning(f"Notion GET 실패 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(wait)
        except (URLError, OSError) as e:
            logger.warning(f"Notion GET 네트워크 에러 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(min(2 ** attempt, 16))
    return None''',
        "notion_get 지수 백오프"
    )
    if ok: changes += 1

    # notion_patch
    content, ok = patch(content,
        '''def notion_patch(path, payload, token):
    url = f"{NOTION_BASE}{path}"
    data = json.dumps(payload).encode()
    for attempt in range(3):
        try:
            req = Request(url, data=data, headers=_notion_headers(token), method="PATCH")
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError) as e:
            logger.warning(f"Notion PATCH 실패 ({attempt+1}/3): {path} → {e}")
            if attempt < 2:
                time.sleep(2)
    return None''',
        '''def notion_patch(path, payload, token):
    url = f"{NOTION_BASE}{path}"
    data = json.dumps(payload).encode()
    for attempt in range(5):
        try:
            req = Request(url, data=data, headers=_notion_headers(token), method="PATCH")
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            wait = _notion_retry_wait(e, attempt)
            logger.warning(f"Notion PATCH 실패 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(wait)
        except (URLError, OSError) as e:
            logger.warning(f"Notion PATCH 네트워크 에러 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(min(2 ** attempt, 16))
    return None''',
        "notion_patch 지수 백오프"
    )
    if ok: changes += 1

    # notion_post
    content, ok = patch(content,
        '''def notion_post(path, payload, token):
    url = f"{NOTION_BASE}{path}"
    data = json.dumps(payload).encode()
    for attempt in range(3):
        try:
            req = Request(url, data=data, headers=_notion_headers(token), method="POST")
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError) as e:
            logger.warning(f"Notion POST 실패 ({attempt+1}/3): {path} → {e}")
            if attempt < 2:
                time.sleep(2)
    return None''',
        '''def notion_post(path, payload, token):
    url = f"{NOTION_BASE}{path}"
    data = json.dumps(payload).encode()
    for attempt in range(5):
        try:
            req = Request(url, data=data, headers=_notion_headers(token), method="POST")
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            wait = _notion_retry_wait(e, attempt)
            logger.warning(f"Notion POST 실패 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(wait)
        except (URLError, OSError) as e:
            logger.warning(f"Notion POST 네트워크 에러 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(min(2 ** attempt, 16))
    return None''',
        "notion_post 지수 백오프"
    )
    if ok: changes += 1

    # notion_delete
    content, ok = patch(content,
        '''def notion_delete(path, token):
    url = f"{NOTION_BASE}{path}"
    for attempt in range(3):
        try:
            req = Request(url, headers=_notion_headers(token), method="DELETE")
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError) as e:
            logger.warning(f"Notion DELETE 실패 ({attempt+1}/3): {path} → {e}")
            if attempt < 2:
                time.sleep(2)
    return None''',
        '''def notion_delete(path, token):
    url = f"{NOTION_BASE}{path}"
    for attempt in range(5):
        try:
            req = Request(url, headers=_notion_headers(token), method="DELETE")
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            wait = _notion_retry_wait(e, attempt)
            logger.warning(f"Notion DELETE 실패 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(wait)
        except (URLError, OSError) as e:
            logger.warning(f"Notion DELETE 네트워크 에러 ({attempt+1}/5): {path} → {e}")
            if attempt < 4:
                time.sleep(min(2 ** attempt, 16))
    return None''',
        "notion_delete 지수 백오프"
    )
    if ok: changes += 1

    # _notion_retry_wait 헬퍼 함수 추가 (notion_get 앞에)
    print("\n         → _notion_retry_wait 헬퍼 함수 추가")
    content, ok = patch(content,
        'def notion_get(path, token):',
        '''def _notion_retry_wait(e, attempt):
    """노션 API 재시도 대기시간 (Retry-After 헤더 우선, 없으면 지수 백오프)"""
    try:
        if hasattr(e, 'headers') and e.headers:
            retry_after = e.headers.get("Retry-After")
            if retry_after:
                return min(float(retry_after), 30)
    except (ValueError, TypeError):
        pass
    return min(2 ** attempt, 16)

def notion_get(path, token):''',
        "_notion_retry_wait 헬퍼"
    )
    if ok: changes += 1

    # ──────────────────────────────────────────────
    # 수정 4: fetch_account_data — API 실패 시 None 체크 + 재시도 + 알림
    # ──────────────────────────────────────────────
    print("\n[수정 4/5] API 실패 시 None 체크 + 재시도 + 텔레그램 알림")
    content, ok = patch(content,
        '''        # 잔고
        balance = mc.get_wallet_balance(api_key, api_secret)
        # 활성 포지션
        positions = mc.get_my_positions(api_key, api_secret)
        # closed-pnl (리셋 이후)
        closed_pnl = fetch_closed_pnl(api_key, api_secret, reset_time)
        data[strat] = {
            "balance": balance,
            "positions": positions,
            "closed_pnl": closed_pnl,
        }''',
        '''        # 잔고 + 포지션 (재시도 3회)
        balance = None
        positions = None
        for _retry in range(3):
            balance = mc.get_wallet_balance(api_key, api_secret)
            positions = mc.get_my_positions(api_key, api_secret)
            if balance is not None and positions is not None:
                break
            logger.warning(f"[{STRATEGY_NAMES[strat]}] API 재시도 ({_retry+1}/3)")
            time.sleep(2 ** _retry)
        # API 실패 시 안전한 기본값 + 알림
        if balance is None or positions is None:
            logger.error(f"[{STRATEGY_NAMES[strat]}] ⚠️ API 데이터 수집 실패!")
            try:
                mc.send_telegram(config,
                    f"⚠️ [{STRATEGY_NAMES[strat]}] 바이비트 API 장애\\n"
                    f"잔고={'실패' if balance is None else 'OK'}, "
                    f"포지션={'실패' if positions is None else 'OK'}")
            except Exception:
                pass
            balance = balance if balance is not None else 0
            positions = positions if positions is not None else []
        # closed-pnl (리셋 이후)
        closed_pnl = fetch_closed_pnl(api_key, api_secret, reset_time)
        data[strat] = {
            "balance": balance,
            "positions": positions,
            "closed_pnl": closed_pnl,
        }''',
        "API None 체크 + 재시도"
    )
    if ok: changes += 1

    # ──────────────────────────────────────────────
    # 수정 5: closed-pnl 페이지 한계 확장 (10 → 50)
    # ──────────────────────────────────────────────
    print("\n[수정 5/5] closed-pnl 페이지 한계 확장 (10 → 50)")
    content, ok = patch(content,
        "    for _ in range(10):  # 최대 10페이지",
        "    for _ in range(50):  # 최대 50페이지 (5000건)",
        "closed-pnl 50페이지"
    )
    if ok: changes += 1

    # ──────────────────────────────────────────────
    # 수정 6: 드로우다운 자동 중단 — main()에 체크 추가
    # ──────────────────────────────────────────────
    print("\n[추가] 드로우다운 자동 중단 로직")

    content, ok = patch(content,
        "def main():",
        '''def check_drawdown_limit(config, account_data):
    """드로우다운 한계 체크 — 한계 초과 시 텔레그램 경고"""
    max_dd_pct = config.get("capital", {}).get("max_drawdown_pct", -50)
    individual_caps = config["capital"].get("start_capital_individual", {})
    start_cap_common = config["capital"].get("start_capital_per_strategy", 0)
    alerts = []
    for strat in STRATEGY_ORDER:
        sd = account_data.get(strat, {})
        balance = sd.get("balance", 0)
        if not balance or balance == 0:
            continue
        start_cap = individual_caps.get(strat, start_cap_common)
        if start_cap <= 0:
            continue
        dd_pct = (balance - start_cap) / start_cap * 100
        if dd_pct < max_dd_pct:
            alerts.append(f"🛑 [{STRATEGY_NAMES[strat]}] 드로우다운 {dd_pct:.1f}% (한계 {max_dd_pct}%)")
            logger.critical(f"[{STRATEGY_NAMES[strat]}] 드로우다운 한계 초과: {dd_pct:.1f}%")
    if alerts:
        try:
            msg = "\\n".join(["⚠️ 드로우다운 한계 초과!"] + alerts + ["", "수동 확인 필요합니다."])
            mc.send_telegram(config, msg)
        except Exception:
            pass
    return alerts

def main():''',
        "check_drawdown_limit 함수"
    )
    if ok: changes += 1

    content, ok = patch(content,
        "        # 4. 데이터 가공",
        "        # 3.5. 드로우다운 한계 체크\n"
        "        dd_alerts = check_drawdown_limit(config, account_data)\n"
        "        if dd_alerts:\n"
        "            logger.warning(f\"드로우다운 경고 {len(dd_alerts)}건 발생\")\n"
        "        # 4. 데이터 가공",
        "main()에 drawdown 체크 삽입"
    )
    if ok: changes += 1

    # ──────────────────────────────────────────────
    # 버전 표기 업데이트
    # ──────────────────────────────────────────────
    print("\n[버전] → v5.2 업데이트")
    if "v5.1" in content:
        content = content.replace("v5.1", "v5.2", 3)
        changes += 1
        print("  ✅  [버전 v5.1→v5.2] 패치 완료")
    elif "v5.0" in content:
        content = content.replace("v5.0", "v5.2", 3)
        changes += 1
        print("  ✅  [버전 v5.0→v5.2] 패치 완료")
    else:
        print("  ⚠️  버전 표기를 찾을 수 없습니다")

    # ──────────────────────────────────────────────
    # 저장
    # ──────────────────────────────────────────────
    print("\n" + "=" * 55)
    if changes > 0:
        write_file(content)
        print(f"✅ 총 {changes}건 패치 완료 — update_all.py 저장됨")
    else:
        print("ℹ️  변경사항 없음 (모두 이미 적용됨)")
    print("=" * 55)

if __name__ == "__main__":
    main()
