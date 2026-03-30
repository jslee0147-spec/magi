#!/usr/bin/env python3
"""
MAGI update_all.py v5.2 — 통합 현황판 (아카이브 스타일) + 거래 로그 동기화
설계: 소니 | 구현: 별이
실행: launchd 5분 주기 (com.magi.dashboard)
역할:
  - Single Writer: 노션 기록은 오직 이 파일만 담당
  - Single Source of Truth: 바이비트 closed-pnl API가 유일한 진실
  - 원자적 처리: 1회 API 호출 데이터 → 현황판 + 로그 동시 업데이트

v5.0 변경: 현황판 레이아웃을 아카이브 스타일(가로 넓은 테이블)로 전환
v5.2 변경: 거래로그 중복 방지를 로컬 원장(synced_orders.json) 기반으로 근본 개선
  - 노션 DB 조회 의존 제거 → 로컬 파일 기반 (무제한, 즉시, 안정)
  - 최초 실행 시 노션 DB에서 기존 orderId 자동 수집 (초기화)
  - 개별 orderId 단위 체크 (파이프 분리 포함)
  - 원자적 파일 교체 (tmp → rename)
"""
import json
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError
# 같은 디렉토리의 공통 모듈
import magi_common as mc
# 픽셀 오피스 Firebase 연동
import firebase_writer as fw
# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────
BASE_DIR = mc.BASE_DIR
KST = mc.KST
DASHBOARD_BLOCKS_PATH = BASE_DIR / "dashboard_blocks.json"
TRADE_EVENTS_PATH = mc.TRADE_EVENTS_PATH
ACTIVE_COINS_PATH = mc.ACTIVE_COINS_PATH
MARKET_SNAPSHOT_PATH = BASE_DIR / "market_snapshot.json"
SYNCED_ORDERS_PATH = BASE_DIR / "synced_orders.json"
POSITION_ENTRIES_PATH = BASE_DIR / "position_entries.json"
NOTION_BASE = "https://api.notion.com/v1"
NOTION_VER = "2022-06-28"
STRATEGY_ORDER = ["kai", "jet", "boomerang", "release"]
STRATEGY_ICONS = {"kai": "🌊", "jet": "⚡", "boomerang": "🪃", "release": "👁️"}
STRATEGY_NAMES = {"kai": "카이", "jet": "제트", "boomerang": "부메랑", "release": "릴리스"}
# ──────────────────────────────────────────────
# 로거
# ──────────────────────────────────────────────
LOG_PATH = BASE_DIR / "logs" / "update_all.log"
os.makedirs(LOG_PATH.parent, exist_ok=True)
logger = logging.getLogger("update_all")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = RotatingFileHandler(LOG_PATH, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(sh)
# ══════════════════════════════════════════════
# Notion API 헬퍼
# ══════════════════════════════════════════════
def _notion_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }
def _notion_retry_wait(e, attempt):
    """노션 API 재시도 대기시간 (Retry-After 헤더 우선, 없으면 지수 백오프)"""
    try:
        if hasattr(e, 'headers') and e.headers:
            retry_after = e.headers.get("Retry-After")
            if retry_after:
                return min(float(retry_after), 30)
    except (ValueError, TypeError):
        pass
    return min(2 ** attempt, 16)

def notion_get(path, token):
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
    return None
def notion_patch(path, payload, token):
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
    return None
def notion_post(path, payload, token):
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
    return None
def notion_delete(path, token):
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
    return None
# ──────────────────────────────────────────────
# Notion 셀/행 헬퍼
# ──────────────────────────────────────────────
def make_cell(text, color="default"):
    """Notion table cell (rich_text 배열)"""
    rt = {"type": "text", "text": {"content": str(text)}}
    if color != "default":
        rt["annotations"] = {"color": color}
    return [rt]
def pnl_color(value):
    """수익 → red, 손실 → blue, 0 → default"""
    if value > 0:
        return "red"
    elif value < 0:
        return "blue"
    return "default"
def fmt_num(value, decimals=2):
    """숫자 포맷"""
    if value is None:
        return "—"
    return f"{value:,.{decimals}f}"
def fmt_pct(value, decimals=2):
    """퍼센트 포맷 (+/- 부호)"""
    if value is None:
        return "—"
    return f"{value:+.{decimals}f}%"
def fmt_usd(value, decimals=2):
    """USD 포맷"""
    if value is None:
        return "—"
    prefix = "+" if value > 0 else ""
    return f"{prefix}${value:,.{decimals}f}"
def fmt_hold_time(seconds):
    """보유시간 포맷 (23일 6시간 / 3시간 38분 / 45분)"""
    if seconds is None or seconds <= 0:
        return "—"
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    mins = int((seconds % 3600) // 60)
    if days > 0:
        return f"{days}일 {hours}시간"
    if hours > 0:
        return f"{hours}시간 {mins}분"
    return f"{mins}분"
# ── 포지션 진입시간 추적 (createdTime 버그 우회) ──
def load_position_entries():
    """로컬 포지션 진입시간 캐시 로드"""
    if POSITION_ENTRIES_PATH.exists():
        try:
            return json.loads(POSITION_ENTRIES_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}

def save_position_entries(entries):
    """로컬 포지션 진입시간 캐시 저장"""
    POSITION_ENTRIES_PATH.write_text(
        json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")

def fetch_last_exec_time(symbol, side, api_key, api_secret):
    """바이비트 체결 이력에서 해당 포지션의 마지막 진입 시각 조회
    Funding 등 비거래 체결을 제외하고 execType=Trade만 조회
    """
    params = {
        "category": "linear",
        "symbol": symbol,
        "side": side,  # 진입 방향 (Buy=롱진입, Sell=숏진입)
        "limit": "5",  # Funding 체결 건너뛰기 위해 여유분
    }
    result = mc.bybit_private_get("/v5/execution/list", params, api_key, api_secret)
    if result and result.get("list"):
        for ex in result["list"]:
            if ex.get("execType") == "Trade":
                exec_time = int(ex.get("execTime", 0))
                if exec_time:
                    return exec_time / 1000  # ms → seconds
    return None

def populate_new_position_entries(config, account_data, entries):
    """새로 발견된 포지션의 실제 진입시각을 바이비트 체결 이력에서 조회"""
    for strat in STRATEGY_ORDER:
        positions = account_data.get(strat, {}).get("positions", [])
        if not positions:
            continue
        acct = config["bybit"]["accounts"][strat]
        for pos in positions:
            symbol = pos.get("symbol", "")
            key = f"{strat}_{symbol}"
            avg_price = str(pos.get("avgEntryPrice", pos.get("avgPrice", "")))
            side = pos.get("side", "")
            existing = entries.get(key, {})
            if existing.get("avgPrice") == avg_price and existing.get("side") == side:
                continue  # 이미 캐시됨
            # 새 포지션 → 바이비트에서 실제 진입시각 조회
            real_ts = fetch_last_exec_time(symbol, side, acct["api_key"], acct["api_secret"])
            if real_ts:
                entries[key] = {"avgPrice": avg_price, "side": side, "entry_ts": real_ts}
                logger.info(f"[{STRATEGY_NAMES[strat]}] {symbol} 실제 진입시각 조회 완료")
            else:
                entries[key] = {"avgPrice": avg_price, "side": side, "entry_ts": time.time()}
                logger.warning(f"[{STRATEGY_NAMES[strat]}] {symbol} 체결 이력 없음 — 현재 시각 사용")

def get_real_entry_time(strategy, pos, entries):
    """실제 진입시각 반환 — 캐시에서 조회 (없으면 현재시각 폴백)
    바이비트 one-way 모드에서 createdTime이 리셋되지 않는 버그 우회
    """
    symbol = pos.get("symbol", "")
    key = f"{strategy}_{symbol}"
    avg_price = str(pos.get("avgEntryPrice", pos.get("avgPrice", "")))
    side = pos.get("side", "")
    existing = entries.get(key, {})
    if existing.get("avgPrice") == avg_price and existing.get("side") == side:
        return existing.get("entry_ts", time.time())
    # 폴백 (populate_new_position_entries에서 이미 처리됐어야 함)
    now_ts = time.time()
    entries[key] = {"avgPrice": avg_price, "side": side, "entry_ts": now_ts}
    return now_ts

def patch_row(row_id, cells, token):
    """table_row PATCH"""
    time.sleep(0.35)  # Notion rate limit (3 req/s)
    return notion_patch(f"/blocks/{row_id}", {"table_row": {"cells": cells}}, token)
def patch_paragraph(block_id, text, token, bold=False, italic=False, color="default"):
    """paragraph 블록 텍스트 PATCH"""
    rt = {"type": "text", "text": {"content": text}}
    annotations = {}
    if bold:
        annotations["bold"] = True
    if italic:
        annotations["italic"] = True
    if color != "default":
        annotations["color"] = color
    if annotations:
        rt["annotations"] = annotations
    time.sleep(0.35)
    return notion_patch(f"/blocks/{block_id}", {
        "paragraph": {"rich_text": [rt]}
    }, token)
# ──────────────────────────────────────────────
# v5.0 신규: 헤딩/콜아웃 PATCH 헬퍼
# ──────────────────────────────────────────────
def rt_text(content, bold=False, italic=False, color="default"):
    """rich_text 엘리먼트 생성"""
    rt = {"type": "text", "text": {"content": content}}
    annotations = {}
    if bold:
        annotations["bold"] = True
    if italic:
        annotations["italic"] = True
    if color != "default":
        annotations["color"] = color
    if annotations:
        rt["annotations"] = annotations
    return rt
def patch_heading2(block_id, rich_text, token):
    """heading_2 블록 PATCH"""
    time.sleep(0.35)
    return notion_patch(f"/blocks/{block_id}", {"heading_2": {"rich_text": rich_text}}, token)
def patch_heading3(block_id, rich_text, token):
    """heading_3 블록 PATCH"""
    time.sleep(0.35)
    return notion_patch(f"/blocks/{block_id}", {"heading_3": {"rich_text": rich_text}}, token)
def patch_quote(block_id, rich_text, token):
    """quote 블록 PATCH"""
    time.sleep(0.35)
    return notion_patch(f"/blocks/{block_id}", {"quote": {"rich_text": rich_text}}, token)
def patch_paragraph_rt(block_id, rich_text, token):
    """paragraph 블록에 rich_text 배열 직접 PATCH"""
    time.sleep(0.35)
    return notion_patch(f"/blocks/{block_id}", {"paragraph": {"rich_text": rich_text}}, token)
# ──────────────────────────────────────────────
# 테이블 동적 행 관리
# ──────────────────────────────────────────────
def delete_table_data_rows(table_id, token):
    """테이블의 데이터 행 삭제 (헤더 행 유지)"""
    children = notion_get(f"/blocks/{table_id}/children?page_size=100", token)
    if not children or "results" not in children:
        return []
    rows = children["results"]
    # 첫 행은 헤더 → 유지, 나머지 삭제
    deleted = []
    for row in rows[1:]:
        time.sleep(0.35)
        notion_delete(f"/blocks/{row['id']}", token)
        deleted.append(row["id"])
    return deleted
def append_table_rows(table_id, rows_data, token):
    """테이블에 데이터 행 추가"""
    children = []
    for cells in rows_data:
        children.append({
            "type": "table_row",
            "table_row": {"cells": cells}
        })
    if children:
        time.sleep(0.35)
        return notion_patch(f"/blocks/{table_id}/children", {"children": children}, token)
    return None
# ══════════════════════════════════════════════
# 대시보드 블록 캐시 관리 (v5.0: 아카이브 레이아웃)
# ══════════════════════════════════════════════
def init_dashboard_blocks(config):
    """대시보드 블록 ID 초기화 (아카이브 스타일 레이아웃)

    섹션 구조 (divider 기준):
    [0] H1 타이틀 + H2 BTC + H3 D+ + paragraph 업데이트
    [1] quote (시장 상태 + 종목 풀)
    [2] H2 전체합산 + table(5열) + table(3열)
    [3] H2 수익률순위 + table(4열)
    [4] H2 종합포지션 + table(6열, 동적)
    [5] H2 종합오늘거래 + table(8열, 동적) + paragraph
    [6] H2 카이 + table(계좌) + table(실적) + H3 포지션 + table(동적) + H3 거래 + paragraph
    [7] H2 제트 + (동일)
    [8] H2 부메랑 + (동일)
    [9] H2 릴리스 + (동일)
    """
    token = config["notion"]["token"]
    page_id = config["notion"]["dashboard_page_id"]
    logger.info("대시보드 블록 ID 초기화 시작 (v5.0 아카이브 레이아웃)...")
    # 1) 최상위 블록 가져오기
    all_blocks = []
    cursor = None
    while True:
        path = f"/blocks/{page_id}/children?page_size=100"
        if cursor:
            path += f"&start_cursor={cursor}"
        resp = notion_get(path, token)
        if not resp:
            logger.error("대시보드 블록 조회 실패")
            return None
        all_blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    logger.info(f"대시보드 블록 {len(all_blocks)}개 조회 완료")
    # 2) 섹션별 분류 (divider 기준)
    sections = [[]]
    for block in all_blocks:
        if block.get("type") == "divider":
            sections.append([])
        else:
            sections[-1].append(block)
    logger.info(f"섹션 {len(sections)}개 분류 완료")
    cache = {}
    def get_table_row_ids(table_block_id):
        """테이블의 행 블록 ID 목록 (헤더 제외)"""
        time.sleep(0.35)
        resp = notion_get(f"/blocks/{table_block_id}/children?page_size=100", token)
        if not resp:
            return []
        rows = resp.get("results", [])
        return [r["id"] for r in rows[1:]]  # 헤더 제외
    def find_by_type(section_blocks, block_type):
        return [b for b in section_blocks if b["type"] == block_type]
    # ── Section 0: 헤더 (H1 타이틀, H2 BTC, H3 D+, paragraph 업데이트) ──
    if len(sections) > 0:
        sec = sections[0]
        h2s = find_by_type(sec, "heading_2")
        h3s = find_by_type(sec, "heading_3")
        paras = find_by_type(sec, "paragraph")
        if h2s:
            cache["btc_heading"] = h2s[0]["id"]
        if h3s:
            cache["dday_heading"] = h3s[0]["id"]
        if paras:
            cache["update_text"] = paras[0]["id"]
    # ── Section 1: 마켓 상태 (quote) ──
    if len(sections) > 1:
        sec = sections[1]
        quotes = find_by_type(sec, "quote")
        if quotes:
            cache["market_quote"] = quotes[0]["id"]
    # ── Section 2: 전체 합산 (table 5열 + table 3열) ──
    if len(sections) > 2:
        sec = sections[2]
        tables = find_by_type(sec, "table")
        if len(tables) >= 2:
            cache["total_table1"] = tables[0]["id"]
            rows1 = get_table_row_ids(tables[0]["id"])
            cache["total_row1"] = rows1[0] if rows1 else None
            cache["total_table2"] = tables[1]["id"]
            rows2 = get_table_row_ids(tables[1]["id"])
            cache["total_row2"] = rows2[0] if rows2 else None
    # ── Section 3: 수익률 순위 (table 4열) ──
    if len(sections) > 3:
        sec = sections[3]
        tables = find_by_type(sec, "table")
        if tables:
            cache["ranking_table"] = tables[0]["id"]
            cache["ranking_rows"] = get_table_row_ids(tables[0]["id"])
    # ── Section 4: 종합 포지션 (table 6열, 동적) ──
    if len(sections) > 4:
        sec = sections[4]
        tables = find_by_type(sec, "table")
        if tables:
            cache["positions_table"] = tables[0]["id"]
    # ── Section 5: 종합 오늘의 거래 (table 8열, 동적 + paragraph) ──
    if len(sections) > 5:
        sec = sections[5]
        tables = find_by_type(sec, "table")
        paras = find_by_type(sec, "paragraph")
        if tables:
            cache["trades_table"] = tables[0]["id"]
        if paras:
            cache["trades_text"] = paras[-1]["id"]
    # ── Section 6-9: 전략별 ──
    strat_map = {6: "kai", 7: "jet", 8: "boomerang", 9: "release"}
    for idx, strat in strat_map.items():
        if len(sections) > idx:
            sec = sections[idx]
            tables = find_by_type(sec, "table")
            h3s = find_by_type(sec, "heading_3")
            paras = find_by_type(sec, "paragraph")
            # table[0] = 계좌 (4열), table[1] = 실적 (4열), table[2] = 포지션 (5열, 동적)
            if len(tables) >= 1:
                cache[f"{strat}_account_table"] = tables[0]["id"]
                rows = get_table_row_ids(tables[0]["id"])
                cache[f"{strat}_account_row"] = rows[0] if rows else None
            if len(tables) >= 2:
                cache[f"{strat}_stats_table"] = tables[1]["id"]
                rows = get_table_row_ids(tables[1]["id"])
                cache[f"{strat}_stats_row"] = rows[0] if rows else None
            if len(tables) >= 3:
                cache[f"{strat}_pos_table"] = tables[2]["id"]
            # h3[0] = 포지션 헤딩, h3[1] = 거래 헤딩
            if len(h3s) >= 1:
                cache[f"{strat}_pos_heading"] = h3s[0]["id"]
            if len(h3s) >= 2:
                cache[f"{strat}_trades_heading"] = h3s[1]["id"]
            # paragraph = 거래 텍스트 (마지막 paragraph)
            if paras:
                cache[f"{strat}_trades_text"] = paras[-1]["id"]
    # 캐시 저장
    tmp_path = BASE_DIR / "dashboard_blocks.tmp"
    with open(tmp_path, "w") as f:
        json.dump(cache, f, indent=2)
    os.rename(tmp_path, DASHBOARD_BLOCKS_PATH)
    logger.info(f"대시보드 블록 캐시 저장 완료 ({len(cache)} 항목)")
    return cache
def load_dashboard_blocks(config):
    """대시보드 블록 캐시 로드 (없으면 초기화)"""
    if DASHBOARD_BLOCKS_PATH.exists():
        with open(DASHBOARD_BLOCKS_PATH, "r") as f:
            return json.load(f)
    return init_dashboard_blocks(config)
# ══════════════════════════════════════════════
# 바이비트 데이터 수집
# ══════════════════════════════════════════════
def fetch_btc_price():
    """BTC 현재 가격"""
    result = mc.bybit_public_get("/v5/market/tickers", {
        "category": "linear", "symbol": "BTCUSDT"
    })
    if result and result.get("list"):
        return float(result["list"][0].get("lastPrice", 0))
    return None
def fetch_account_data(config):
    """4개 계정 데이터 수집 (잔고 + 포지션 + closed-pnl)"""
    reset_time = config["capital"].get("reset_time", "")
    accounts = config["bybit"]["accounts"]
    data = {}
    for strat in STRATEGY_ORDER:
        acct = accounts.get(strat, {})
        api_key = acct.get("api_key", "")
        api_secret = acct.get("api_secret", "")
        if not api_key or "YOUR_" in api_key:
            data[strat] = {"balance": 0, "positions": [], "closed_pnl": []}
            continue
        # 잔고 + 포지션 (재시도 3회)
        balance = None
        positions = None
        for _retry in range(3):
            balance = mc.get_wallet_balance(api_key, api_secret)
            positions = mc.get_my_positions(api_key, api_secret)
            if balance is not None and positions is not None:
                break
            logger.warning(f"[{STRATEGY_NAMES[strat]}] API 재시도 ({_retry+1}/3)")
            time.sleep(2 ** _retry)
        # API 실패 시 직전 성공값 유지 + 알림
        if balance is None or positions is None:
            logger.error(f"[{STRATEGY_NAMES[strat]}] ⚠️ API 데이터 수집 실패!")
            # 직전 성공값 유지 (DD 계산 오류 방지)
            if balance is None:
                last_eq_file = BASE_DIR / f"last_equity_{strat}.json"
                if last_eq_file.exists():
                    try:
                        last_data = json.loads(last_eq_file.read_text(encoding="utf-8"))
                        balance = last_data.get("balance", 0)
                        logger.warning(f"[{STRATEGY_NAMES[strat]}] 직전 잔고값 사용: ${balance:.2f}")
                    except (json.JSONDecodeError, ValueError):
                        balance = 0
                else:
                    balance = 0
            try:
                mc.send_telegram(config,
                    f"⚠️ [{STRATEGY_NAMES[strat]}] 바이비트 API 장애\n"
                    f"잔고={'직전값 사용' if balance > 0 else '실패'}, "
                    f"포지션={'실패' if positions is None else 'OK'}")
            except Exception:
                pass
            positions = positions if positions is not None else []
        else:
            # 성공 시 직전 잔고 저장
            last_eq_file = BASE_DIR / f"last_equity_{strat}.json"
            try:
                tmp = last_eq_file.with_suffix(".json.tmp")
                tmp.write_text(json.dumps({"balance": balance, "ts": time.time()}), encoding="utf-8")
                tmp.replace(last_eq_file)
            except Exception:
                pass
        # closed-pnl (리셋 이후)
        closed_pnl = fetch_closed_pnl(api_key, api_secret, reset_time)
        data[strat] = {
            "balance": balance,
            "positions": positions,
            "closed_pnl": closed_pnl,
        }
    return data
def fetch_closed_pnl(api_key, api_secret, reset_time):
    """바이비트 closed-pnl 조회 (리셋 시각 이후만)"""
    all_trades = []
    cursor = ""
    # reset_time을 epoch ms로 변환
    start_time = 0
    if reset_time:
        try:
            dt = datetime.fromisoformat(reset_time)
            start_time = int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            pass
    for _ in range(50):  # 최대 50페이지 (5000건)
        params = {
            "category": "linear",
            "limit": "100",
        }
        if start_time:
            params["startTime"] = str(start_time)
        if cursor:
            params["cursor"] = cursor
        result = mc.bybit_private_get("/v5/position/closed-pnl",
                                      params, api_key, api_secret)
        if not result:
            break
        trades = result.get("list", [])
        all_trades.extend(trades)
        cursor = result.get("nextPageCursor", "")
        if not cursor or len(trades) < 100:
            break
    return all_trades
# ══════════════════════════════════════════════
# 로컬 파일 읽기
# ══════════════════════════════════════════════
def read_trade_events():
    """trade_events.json 읽기"""
    try:
        with open(TRADE_EVENTS_PATH, "r") as f:
            data = json.load(f)
        return data.get("events", [])
    except (FileNotFoundError, json.JSONDecodeError):
        return []
def read_active_coins_status():
    """active_coins.json 상태 읽기"""
    try:
        with open(ACTIVE_COINS_PATH, "r") as f:
            data = json.load(f)
        coins = data.get("coins", [])
        updated_at = data.get("updated_at", "")
        try:
            dt = datetime.fromisoformat(updated_at)
            age_h = (datetime.now(KST) - dt).total_seconds() / 3600
        except (ValueError, TypeError):
            age_h = 999
        status = "정상" if age_h < 12 else ("12h 경고" if age_h < 24 else "24h 만료")
        return len(coins), status
    except (FileNotFoundError, json.JSONDecodeError):
        return 0, "파일 없음"
def read_market_state():
    """market_snapshot.json에서 시장 상태 읽기"""
    try:
        with open(MARKET_SNAPSHOT_PATH, "r") as f:
            data = json.load(f)
        return data.get("market_state", "—")
    except (FileNotFoundError, json.JSONDecodeError):
        return "—"
# ══════════════════════════════════════════════
# 데이터 처리 (v5.0: 미실현/실현 PnL 분리)
# ══════════════════════════════════════════════
def process_data(config, account_data, btc_price, trade_events, position_entries):
    """수집 데이터 → 대시보드용 가공 (position_entries: 실제 진입시간 캐시)"""
    now = datetime.now(KST)
    d_day = config["capital"].get("d_day", "")
    # 전략별 개별 시작자본 (없으면 공통값 사용)
    individual_caps = config["capital"].get("start_capital_individual", {})
    start_cap_common = config["capital"].get("start_capital_per_strategy", 0)
    # D+ 계산
    d_plus = 0
    if d_day:
        try:
            d_start = datetime.strptime(d_day, "%Y-%m-%d").replace(tzinfo=KST)
            d_plus = max(0, (now - d_start).days)
        except ValueError:
            pass
    # 종목풀 상태
    coin_count, coin_status = read_active_coins_status()
    market_state = read_market_state()
    # 전략별 통계
    strategy_stats = {}
    all_positions = []
    all_today_trades = []
    today_str = now.strftime("%Y-%m-%d")
    for strat in STRATEGY_ORDER:
        sd = account_data.get(strat, {})
        balance = sd.get("balance", 0)
        positions = sd.get("positions", [])
        closed_pnl = sd.get("closed_pnl", [])
        # 오늘 청산 거래
        today_trades = []
        total_pnl = 0
        wins = 0
        losses = 0
        for t in closed_pnl:
            pnl = float(t.get("closedPnl", 0))
            total_pnl += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
            # 오늘 거래 필터
            close_ts = int(t.get("updatedTime", 0)) / 1000
            close_dt = datetime.fromtimestamp(close_ts, tz=KST)
            entry_ts = int(t.get("createdTime", 0)) / 1000
            trade_hold_sec = (close_ts - entry_ts) if entry_ts > 0 and close_ts > 0 else 0
            # 수익률 계산
            t_entry = float(t.get("avgEntryPrice", 0) or 0)
            t_exit = float(t.get("avgExitPrice", 0) or 0)
            t_leverage = float(t.get("leverage", 1) or 1)
            t_side = t.get("side", "")  # 청산 방향
            if t_side == "Sell" and t_entry > 0:  # 원래 롱
                t_pnl_pct = (t_exit - t_entry) / t_entry * 100
            elif t_side == "Buy" and t_entry > 0:  # 원래 숏
                t_pnl_pct = (t_entry - t_exit) / t_entry * 100
            else:
                t_pnl_pct = 0
            t_roi = t_pnl_pct * t_leverage
            if close_dt.strftime("%Y-%m-%d") == today_str:
                today_trades.append({
                    "strategy": strat,
                    "symbol": t.get("symbol", ""),
                    "side": t_side,  # 청산 방향 (반전 필요)
                    "pnl": pnl,
                    "pnl_pct": t_pnl_pct,
                    "roi": t_roi,
                    "close_time": close_dt,
                    "orderId": t.get("orderId", ""),
                    "hold_seconds": trade_hold_sec,
                })
        total_trades = wins + losses
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        # v5.0: 미실현 PnL 합산
        strat_unrealized = 0
        # 포지션에 전략 태그 추가
        for pos in positions:
            entry_price = pos.get("avgPrice", 0)
            mark_price = pos.get("markPrice", 0)
            side = pos.get("side", "")
            if isinstance(entry_price, str):
                entry_price = float(entry_price) if entry_price else 0
            if isinstance(mark_price, str):
                mark_price = float(mark_price) if mark_price else 0
            if side == "Buy" and entry_price > 0:
                roi_pct = (mark_price - entry_price) / entry_price * 100
            elif side == "Sell" and entry_price > 0:
                roi_pct = (entry_price - mark_price) / entry_price * 100
            else:
                roi_pct = 0
            # 보유시간 계산 (createdTime 대신 로컬 추적 — 바이비트 버그 우회)
            entry_ts = get_real_entry_time(strat, pos, position_entries)
            hold_seconds = max(0, time.time() - entry_ts) if entry_ts > 0 else 0
            unrealised_pnl = pos.get("unrealisedPnl", 0)
            if isinstance(unrealised_pnl, str):
                unrealised_pnl = float(unrealised_pnl) if unrealised_pnl else 0
            strat_unrealized += unrealised_pnl
            all_positions.append({
                "strategy": strat,
                "symbol": pos.get("symbol", ""),
                "side": "롱" if side == "Buy" else "숏",
                "roi": roi_pct,
                "pnl": unrealised_pnl,
                "hold_seconds": hold_seconds,
            })
        all_today_trades.extend(today_trades)
        strategy_stats[strat] = {
            "balance": balance,
            "positions": positions,
            "position_count": len(positions),
            "today_trades": today_trades,
            "today_trade_count": len(today_trades),
            "total_pnl": total_pnl,
            "unrealized_pnl": strat_unrealized,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total_trades": total_trades,
        }
    # 전체 합산 (전략별 개별 시작자본)
    total_start = sum(individual_caps.get(s, start_cap_common) for s in STRATEGY_ORDER)
    total_equity = sum(s["balance"] for s in strategy_stats.values())
    total_pnl = total_equity - total_start if total_start > 0 else sum(s["total_pnl"] for s in strategy_stats.values())
    total_unrealized = sum(s["unrealized_pnl"] for s in strategy_stats.values())
    total_realized = total_pnl - total_unrealized
    total_wins = sum(s["wins"] for s in strategy_stats.values())
    total_losses = sum(s["losses"] for s in strategy_stats.values())
    total_all = total_wins + total_losses
    total_win_rate = (total_wins / total_all * 100) if total_all > 0 else 0
    # ROI 순위 (전략별 개별 시작자본 기준) — v5.0: equity 포함
    rankings = []
    for strat in STRATEGY_ORDER:
        s = strategy_stats[strat]
        scap = individual_caps.get(strat, start_cap_common)
        if scap > 0:
            roi = (s["balance"] - scap) / scap * 100
        else:
            roi = s["total_pnl"]
        rankings.append({"strategy": strat, "roi": roi, "equity": s["balance"]})
    rankings.sort(key=lambda x: x["roi"], reverse=True)
    # 포지션 ROI 내림차순
    all_positions.sort(key=lambda x: x["roi"], reverse=True)
    # 오늘 거래 시간순
    all_today_trades.sort(key=lambda x: x["close_time"])
    # 다음 업데이트 시간 (5분 주기)
    next_update = now + timedelta(minutes=5)
    return {
        "btc_price": btc_price,
        "d_plus": d_plus,
        "d_day_str": d_day,
        "now": now,
        "next_update": next_update,
        "coin_count": coin_count,
        "coin_status": coin_status,
        "market_state": market_state,
        "rankings": rankings,
        "all_positions": all_positions,
        "all_today_trades": all_today_trades,
        "total_start": total_start,
        "total_equity": total_equity,
        "total_pnl": total_pnl,
        "total_unrealized_pnl": total_unrealized,
        "total_realized_pnl": total_realized,
        "total_roi": (total_pnl / total_start * 100) if total_start > 0 else 0,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "total_win_rate": total_win_rate,
        "strategy_stats": strategy_stats,
        "individual_caps": individual_caps,
        "start_cap_common": start_cap_common,
    }
# ══════════════════════════════════════════════
# 대시보드 업데이트 (v5.0: 아카이브 스타일)
# ══════════════════════════════════════════════
def update_dashboard(config, blocks, data):
    """노션 대시보드 전체 업데이트 (아카이브 스타일 레이아웃)"""
    token = config["notion"]["token"]
    # ── 1. 헤더: BTC 가격 (H2) ──
    btc_id = blocks.get("btc_heading")
    if btc_id:
        btc_str = f"₿ BTC ${fmt_num(data['btc_price'], 1)}" if data["btc_price"] else "₿ BTC —"
        patch_heading2(btc_id, [rt_text(btc_str)], token)
    # ── 2. D-Day (H3) ──
    dday_id = blocks.get("dday_heading")
    if dday_id:
        patch_heading3(dday_id, [rt_text(f"D+{data['d_plus']}")], token)
    # ── 3. 업데이트 시각 (paragraph) ──
    update_id = blocks.get("update_text")
    if update_id:
        now_str = data["now"].strftime("%H:%M:%S")
        next_str = data["next_update"].strftime("%H:%M:%S")
        text = f"⏰ 마지막 업데이트  {now_str}  |  ⏭️ 다음 업데이트  {next_str}  KST"
        patch_paragraph_rt(update_id, [rt_text(text, bold=True)], token)
    # ── 4. 마켓 상태 (quote) ──
    market_id = blocks.get("market_quote")
    if market_id:
        text = f"🌐 시장  {data['market_state']}  |  📊 종목 풀  {data['coin_count']} / 20"
        patch_quote(market_id, [rt_text(text, bold=True)], token)
    # ── 5. 전체 합산 (5열 + 3열 테이블) ──
    total_row1 = blocks.get("total_row1")
    if total_row1:
        unreal_color = pnl_color(data["total_unrealized_pnl"])
        real_color = pnl_color(data["total_realized_pnl"])
        roi_color = pnl_color(data["total_roi"])
        patch_row(total_row1, [
            make_cell(f"${fmt_num(data['total_start'], 0)}"),
            make_cell(f"${fmt_num(data['total_equity'], 2)}"),
            make_cell(fmt_usd(data["total_unrealized_pnl"]), unreal_color),
            make_cell(fmt_usd(data["total_realized_pnl"]), real_color),
            make_cell(fmt_pct(data["total_roi"]), roi_color),
        ], token)
    total_row2 = blocks.get("total_row2")
    if total_row2:
        tw = data.get("total_wins", 0)
        tl = data.get("total_losses", 0)
        total_trades = tw + tl
        wr_str = f"{data['total_win_rate']:.1f}% ({tw}W {tl}L)" if total_trades > 0 else "거래 없음"
        pos_count = len(data["all_positions"])
        patch_row(total_row2, [
            make_cell(f"{total_trades}건"),
            make_cell(wr_str),
            make_cell(f"{pos_count}개"),
        ], token)
    # ── 6. 수익률 순위 (4열: 순위/전략/Equity/수익률) ──
    ranking_rows = blocks.get("ranking_rows", [])
    medals = ["🥇", "🥈", "🥉", "4️⃣"]
    for i, rank_data in enumerate(data["rankings"]):
        if i < len(ranking_rows):
            strat = rank_data["strategy"]
            icon = STRATEGY_ICONS[strat]
            name = STRATEGY_NAMES[strat]
            roi = rank_data["roi"]
            equity = rank_data.get("equity", 0)
            color = pnl_color(roi)
            patch_row(ranking_rows[i], [
                make_cell(medals[i] if i < len(medals) else str(i + 1)),
                make_cell(f"{icon} {name}"),
                make_cell(f"${fmt_num(equity, 2)}"),
                make_cell(fmt_pct(roi), color),
            ], token)
    # ── 7. 종합 포지션 (6열, 동적) ──
    pos_table = blocks.get("positions_table")
    if pos_table:
        delete_table_data_rows(pos_table, token)
        if data["all_positions"]:
            rows = []
            for p in data["all_positions"]:
                icon = STRATEGY_ICONS[p["strategy"]]
                color = pnl_color(p["roi"])
                hold_str = fmt_hold_time(p.get("hold_seconds", 0))
                side_emoji = "🟢 " if p["side"] == "롱" else "🔴 "
                rows.append([
                    make_cell(f"{icon} {STRATEGY_NAMES[p['strategy']]}"),
                    make_cell(p["symbol"].replace("USDT", "")),
                    make_cell(f"{side_emoji}{p['side']}"),
                    make_cell(hold_str),
                    make_cell(fmt_pct(p["roi"]), color),
                    make_cell(fmt_usd(p["pnl"]), color),
                ])
            append_table_rows(pos_table, rows, token)
        else:
            append_table_rows(pos_table, [[
                make_cell("—"), make_cell("—"), make_cell("—"),
                make_cell("—"), make_cell("—"), make_cell("—"),
            ]], token)
    # ── 8. 종합 오늘의 거래 (8열, 동적) ──
    trades_table = blocks.get("trades_table")
    trades_text = blocks.get("trades_text")
    if trades_table:
        delete_table_data_rows(trades_table, token)
        if data["all_today_trades"]:
            rows = []
            for i, t in enumerate(data["all_today_trades"], 1):
                icon = STRATEGY_ICONS[t["strategy"]]
                entry_dir = "롱" if t["side"] == "Sell" else "숏"
                side_emoji = "🟢 " if entry_dir == "롱" else "🔴 "
                result_emoji = "✅" if t["pnl"] > 0 else "❌"
                color = pnl_color(t["pnl"])
                rows.append([
                    make_cell(str(i)),
                    make_cell(f"{icon} {STRATEGY_NAMES[t['strategy']]}"),
                    make_cell(t["symbol"].replace("USDT", "")),
                    make_cell(f"{side_emoji}{entry_dir}"),
                    make_cell(result_emoji),
                    make_cell(fmt_pct(t.get("roi", 0)), color),
                    make_cell(fmt_usd(t["pnl"]), color),
                    make_cell(t["close_time"].strftime("%H:%M")),
                ])
            append_table_rows(trades_table, rows, token)
        else:
            append_table_rows(trades_table, [[
                make_cell("—"), make_cell("—"), make_cell("—"), make_cell("—"),
                make_cell("—"), make_cell("—"), make_cell("—"), make_cell("—"),
            ]], token)
    if trades_text:
        count = len(data["all_today_trades"])
        txt = f"오늘 {count}건 청산" if count > 0 else "오늘 청산 거래 없음"
        patch_paragraph(trades_text, txt, token, italic=(count == 0))
    # ── 9-12. 전략별 섹션 ──
    for strat in STRATEGY_ORDER:
        ss = data["strategy_stats"].get(strat, {})
        s_cap = data["individual_caps"].get(strat, data["start_cap_common"])
        equity = ss.get("balance", 0)
        unrealized = ss.get("unrealized_pnl", 0)
        wallet = equity - unrealized
        realized = ss.get("total_pnl", 0)  # closed-pnl 합계
        s_roi = ((equity - s_cap) / s_cap * 100) if s_cap > 0 else 0
        # 계좌 테이블 (시작자본/지갑잔고/Equity/미실현PnL)
        account_row = blocks.get(f"{strat}_account_row")
        if account_row:
            unreal_color = pnl_color(unrealized)
            patch_row(account_row, [
                make_cell(f"${fmt_num(s_cap, 2)}"),
                make_cell(f"${fmt_num(wallet, 2)}"),
                make_cell(f"${fmt_num(equity, 2)}"),
                make_cell(fmt_usd(unrealized), unreal_color),
            ], token)
        # 실적 테이블 (실현손익/수익률/총거래/승률)
        stats_row = blocks.get(f"{strat}_stats_row")
        if stats_row:
            real_color = pnl_color(realized)
            roi_color = pnl_color(s_roi)
            s_wins = ss.get("wins", 0)
            s_losses = ss.get("losses", 0)
            s_total = s_wins + s_losses
            wr_str = f"{ss.get('win_rate', 0):.1f}% ({s_wins}W {s_losses}L)" if s_total > 0 else "거래 없음"
            patch_row(stats_row, [
                make_cell(fmt_usd(realized), real_color),
                make_cell(fmt_pct(s_roi), roi_color),
                make_cell(f"{s_total}건"),
                make_cell(wr_str),
            ], token)
        # 포지션 헤딩 (H3)
        strat_positions = [p for p in data["all_positions"] if p["strategy"] == strat]
        pos_heading = blocks.get(f"{strat}_pos_heading")
        if pos_heading:
            pos_count = len(strat_positions)
            patch_heading3(pos_heading, [rt_text(f"📌 활성 포지션 — {pos_count}개")], token)
        # 포지션 테이블 (5열, 동적)
        pos_table_id = blocks.get(f"{strat}_pos_table")
        if pos_table_id:
            delete_table_data_rows(pos_table_id, token)
            if strat_positions:
                rows = []
                for p in strat_positions:
                    color = pnl_color(p["roi"])
                    hold_str = fmt_hold_time(p.get("hold_seconds", 0))
                    side_emoji = "🟢 " if p["side"] == "롱" else "🔴 "
                    rows.append([
                        make_cell(p["symbol"].replace("USDT", "")),
                        make_cell(f"{side_emoji}{p['side']}"),
                        make_cell(hold_str),
                        make_cell(fmt_pct(p["roi"]), color),
                        make_cell(fmt_usd(p["pnl"]), color),
                    ])
                append_table_rows(pos_table_id, rows, token)
            else:
                append_table_rows(pos_table_id, [[
                    make_cell("—"), make_cell("—"), make_cell("—"),
                    make_cell("—"), make_cell("—"),
                ]], token)
        # 거래 헤딩 (H3)
        strat_today = [t for t in data["all_today_trades"] if t["strategy"] == strat]
        trades_heading = blocks.get(f"{strat}_trades_heading")
        if trades_heading:
            t_count = len(strat_today)
            patch_heading3(trades_heading, [rt_text(f"📅 오늘 — {t_count}건")], token)
        # 거래 텍스트 (paragraph)
        trades_text_id = blocks.get(f"{strat}_trades_text")
        if trades_text_id:
            if strat_today:
                lines = []
                for t in strat_today:
                    d = "롱" if t["side"] == "Sell" else "숏"
                    coin = t['symbol'].replace('USDT', '')
                    result = "✅" if t["pnl"] > 0 else "❌"
                    hold = fmt_hold_time(t.get('hold_seconds', 0))
                    t_roi = t.get('roi', 0)
                    line = (f"{t['close_time'].strftime('%H:%M')} {coin} {d} · {hold} | "
                            f"{result} {fmt_usd(t['pnl'])} ({fmt_pct(t_roi)})")
                    lines.append(line)
                patch_paragraph(trades_text_id, "\n".join(lines), token)
            else:
                patch_paragraph(trades_text_id, "오늘 청산 거래 없음", token, italic=True)
    logger.info("대시보드 업데이트 완료")
# ══════════════════════════════════════════════
# 거래 로그 동기화
# ══════════════════════════════════════════════
# ── 로컬 원장 (synced_orders.json) ──────────────
def load_synced_orders():
    """로컬 원장 로드 — {strategy: set(orderId)} 반환"""
    if SYNCED_ORDERS_PATH.exists():
        try:
            data = json.loads(SYNCED_ORDERS_PATH.read_text(encoding="utf-8"))
            return {k: set(v) for k, v in data.items()}
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"synced_orders.json 파싱 실패, 백업 후 초기화: {e}")
            backup = SYNCED_ORDERS_PATH.with_suffix(".json.bak")
            SYNCED_ORDERS_PATH.rename(backup)
    return {}

def save_synced_orders(ledger):
    """로컬 원장 저장 — set → list 변환 후 JSON"""
    data = {k: sorted(v) for k, v in ledger.items()}
    tmp = SYNCED_ORDERS_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SYNCED_ORDERS_PATH)  # 원자적 교체
    logger.info(f"로컬 원장 저장: {sum(len(v) for v in data.values())}건")

def init_ledger_from_notion(config):
    """최초 1회: 노션 DB에 이미 있는 orderId로 원장 초기화"""
    token = config["notion"]["token"]
    db_ids = config["notion"]["trade_log_dbs"]
    ledger = {}
    for strat in STRATEGY_ORDER:
        db_id = db_ids.get(strat)
        if not db_id:
            continue
        ids = set()
        cursor = None
        for _ in range(100):  # 최대 10000건까지 스캔
            payload = {
                "filter": {
                    "property": "orderId",
                    "rich_text": {"is_not_empty": True}
                },
                "page_size": 100,
            }
            if cursor:
                payload["start_cursor"] = cursor
            time.sleep(0.35)
            resp = notion_post(f"/databases/{db_id}/query", payload, token)
            if not resp:
                break
            for page in resp.get("results", []):
                props = page.get("properties", {})
                oid_prop = props.get("orderId", {})
                rt = oid_prop.get("rich_text", [])
                if rt:
                    text = rt[0].get("plain_text", "")
                    # 파이프(|)로 연결된 복합 orderId도 개별 분리
                    for oid in text.split("|"):
                        oid = oid.strip()
                        if oid:
                            ids.add(oid)
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        ledger[strat] = ids
        logger.info(f"[{STRATEGY_NAMES[strat]}] 노션에서 {len(ids)}개 orderId 수집")
    save_synced_orders(ledger)
    return ledger
def get_next_trade_number(db_id, token):
    """다음 거래번호 조회 (현재 최대값 + 1)"""
    payload = {
        "sorts": [{"property": "거래번호", "direction": "descending"}],
        "page_size": 1,
    }
    time.sleep(0.35)
    resp = notion_post(f"/databases/{db_id}/query", payload, token)
    if resp and resp.get("results"):
        props = resp["results"][0].get("properties", {})
        num = props.get("거래번호", {}).get("number")
        return (num or 0) + 1
    return 1
def match_trade_event(order_id, symbol, strategy, trade_events):
    """trade_events.json에서 매칭되는 이벤트 찾기
    주의: '진입' 이벤트는 청산이유가 아니므로 제외해야 함"""
    # orderId로 직접 매칭
    for ev in trade_events:
        if ev.get("orderId") == order_id:
            return ev
    # symbol + strategy로 폴백 매칭 (진입 이벤트 제외 — 청산이유만 반환)
    for ev in reversed(trade_events):
        if (ev.get("symbol") == symbol
                and ev.get("strategy") == strategy
                and ev.get("reason") != "진입"):
            return ev
    return None
def estimate_close_reason(pnl_pct, time_stop_hours=None, hold_hours=0):
    """청산이유 추정 (힌트 없을 때 폴백)"""
    if pnl_pct > 0:
        return "TP1"  # 보수적 추정
    elif pnl_pct < -0.5:
        if time_stop_hours and hold_hours >= time_stop_hours * 0.9:
            return "시간손절"
        return "SL"
    else:
        return "본절"
def calc_hold_duration(entry_ts, close_ts):
    """보유시간 포맷"""
    diff = close_ts - entry_ts
    hours = int(diff / 3600)
    mins = int((diff % 3600) / 60)
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{mins}m"
def group_closed_pnl(trades, trade_events):
    """TP1/TP2 분할 청산 그룹핑
    같은 group_id 또는 (symbol + side + 5분 이내) → 1건으로 합산
    """
    groups = {}  # group_key → [trades]
    for t in trades:
        order_id = t.get("orderId", "")
        symbol = t.get("symbol", "")
        side = t.get("side", "")
        close_ts = int(t.get("updatedTime", 0)) / 1000
        # trade_events에서 group_id 찾기
        event = None
        for ev in trade_events:
            if ev.get("orderId") == order_id:
                event = ev
                break
        group_id = None
        if event:
            group_id = event.get("group_id")
        if not group_id:
            # 폴백: symbol + side + 5분 윈도우
            group_id = f"{symbol}_{side}"
            # 기존 그룹 중 5분 이내 있는지 확인
            matched = False
            for key in list(groups.keys()):
                if key.startswith(group_id):
                    last_ts = max(
                        int(tt.get("updatedTime", 0)) / 1000
                        for tt in groups[key]
                    )
                    if abs(close_ts - last_ts) < 300:  # 5분
                        groups[key].append(t)
                        matched = True
                        break
            if matched:
                continue
            group_id = f"{group_id}_{int(close_ts)}"
        if group_id not in groups:
            groups[group_id] = []
        groups[group_id].append(t)
    return groups
def create_trade_record(db_id, trade_data, strategy, token):
    """노션 거래 로그 DB에 레코드 생성"""
    props = {
        "코인": {"title": [{"text": {"content": trade_data["coin"]}}]},
        "거래번호": {"number": trade_data["trade_num"]},
        "결과": {"select": {"name": trade_data["result"]}},
        "방향": {"select": {"name": trade_data["direction"]}},
        "레버리지": {"number": trade_data["leverage"]},
        "진입가": {"number": trade_data["entry_price"]},
        "청산가": {"number": trade_data["close_price"]},
        "수량": {"number": trade_data["qty"]},
        "배팅금액": {"number": trade_data["position_size"]},
        "수익금": {"number": trade_data["pnl"]},
        "수익률": {"number": trade_data["pnl_pct"]},
        "ROI": {"number": trade_data["roi"]},
        "보유시간": {"rich_text": [{"text": {"content": trade_data["hold_time"]}}]},
        "청산이유": {"select": {"name": trade_data["close_reason"]}},
        "TP1도달": {"select": {"name": trade_data["tp1_reached"]}},
        "TP2도달": {"select": {"name": trade_data["tp2_reached"]}},
        "orderId": {"rich_text": [{"text": {"content": trade_data["order_id"]}}]},
    }
    # 진입시각
    if trade_data.get("entry_time"):
        props["진입시각"] = {
            "date": {"start": trade_data["entry_time"]}
        }
    # 청산시각
    if trade_data.get("close_time"):
        props["청산시각"] = {
            "date": {"start": trade_data["close_time"]}
        }
    # 전략별 고유 컬럼
    extras = trade_data.get("extras", {})
    if strategy == "kai":
        if "ema_state" in extras:
            props["EMA상태"] = {"select": {"name": extras["ema_state"]}}
        if "rsi" in extras:
            props["RSI"] = {"number": extras["rsi"]}
        if "adx" in extras and extras["adx"] is not None:
            props["ADX"] = {"number": extras["adx"]}
        if "ema20_proximity" in extras:
            props["EMA20괴리"] = {"number": extras["ema20_proximity"]}
    elif strategy == "jet":
        if "squeeze" in extras or "squeeze_status" in extras:
            sq = extras.get("squeeze_status", extras.get("squeeze", ""))
            props["스퀴즈여부"] = {"select": {"name": str(sq)[:100]}}
        if "time_stop" in extras:
            props["시간손절여부"] = {"select": {"name": extras["time_stop"]}}
        if "bandwidth" in extras:
            props["대역폭"] = {"number": extras["bandwidth"]}
        if "bw_expansion" in extras and extras["bw_expansion"] is not None:
            props["확장배수"] = {"number": extras["bw_expansion"]}
        if "volume_ratio" in extras and extras["volume_ratio"] is not None:
            props["거래량비율"] = {"number": extras["volume_ratio"]}
        if "adx" in extras and extras["adx"] is not None:
            props["ADX"] = {"number": extras["adx"]}
    elif strategy == "boomerang":
        if "rsi_direction" in extras:
            props["RSI방향"] = {"select": {"name": extras["rsi_direction"]}}
        if "rsi_curr" in extras:
            props["RSI현재"] = {"number": extras["rsi_curr"]}
        if "rsi_extreme" in extras:
            props["RSI극값"] = {"number": extras["rsi_extreme"]}
        if "bb_depth" in extras:
            props["BB위치"] = {"number": extras["bb_depth"]}
        if "bb_width" in extras:
            props["BB폭"] = {"number": extras["bb_width"]}
    elif strategy == "release":
        if "funding_rate" in extras:
            props["펀딩비"] = {"number": extras["funding_rate"]}
        if "oi_change" in extras:
            props["OI변화율"] = {"number": extras["oi_change"]}
        if "time_stop" in extras:
            props["시간손절여부"] = {"select": {"name": extras["time_stop"]}}
    # 공통: 시장 상태
    if "market_state" in extras:
        props["시장상태"] = {"select": {"name": extras["market_state"]}}
    payload = {
        "parent": {"database_id": db_id},
        "properties": props,
    }
    time.sleep(0.35)
    result = notion_post("/pages", payload, token)
    if result and result.get("id"):
        logger.info(f"거래 로그 생성: {trade_data['coin']} {trade_data['direction']} "
                     f"PnL={trade_data['pnl']:.2f} ({strategy})")
        return True
    else:
        logger.error(f"거래 로그 생성 실패: {trade_data['coin']} ({strategy})")
        return False
def sync_trade_logs(config, account_data, trade_events):
    """거래 로그 동기화 (로컬 원장 기반 중복 방지 + TP1/TP2 그룹핑)"""
    token = config["notion"]["token"]
    db_ids = config["notion"]["trade_log_dbs"]
    # ── 로컬 원장 로드 (없으면 노션에서 초기화) ──
    ledger = load_synced_orders()
    if not ledger:
        logger.info("로컬 원장 없음 → 노션 DB에서 초기 원장 생성 중...")
        ledger = init_ledger_from_notion(config)
    ledger_changed = False
    for strat in STRATEGY_ORDER:
        db_id = db_ids.get(strat)
        if not db_id:
            continue
        closed_pnl = account_data.get(strat, {}).get("closed_pnl", [])
        if not closed_pnl:
            continue
        # 전략별 원장 (없으면 빈 set)
        existing_ids = ledger.get(strat, set())
        logger.info(f"[{STRATEGY_NAMES[strat]}] 원장 {len(existing_ids)}건, "
                     f"closed-pnl {len(closed_pnl)}건")
        # 전략별 trade_events 필터
        strat_events = [e for e in trade_events if e.get("strategy") == strat]
        # TP1/TP2 그룹핑
        groups = group_closed_pnl(closed_pnl, strat_events)
        # 다음 거래번호
        next_num = get_next_trade_number(db_id, token)
        for group_id, group_trades in groups.items():
            # 그룹 내 모든 orderId가 원장에 있으면 스킵
            group_order_ids = [t.get("orderId", "") for t in group_trades]
            if all(oid in existing_ids for oid in group_order_ids if oid):
                continue
            # 그룹 합산
            total_pnl = sum(float(t.get("closedPnl", 0)) for t in group_trades)
            total_qty = sum(float(t.get("qty", 0)) for t in group_trades)
            # 대표 거래 (첫 번째)
            rep = group_trades[0]
            symbol = rep.get("symbol", "")
            coin = symbol.replace("USDT", "")
            # side 반전 (BUG-008): 바이비트 side = 청산 방향
            close_side = rep.get("side", "")
            direction = "롱" if close_side == "Sell" else "숏"
            entry_price = float(rep.get("avgEntryPrice", 0))
            close_price = float(rep.get("avgExitPrice", 0))
            leverage = float(rep.get("leverage", 1))
            # 수익률 계산
            if entry_price > 0:
                if direction == "롱":
                    pnl_pct = (close_price - entry_price) / entry_price * 100
                else:
                    pnl_pct = (entry_price - close_price) / entry_price * 100
                roi = pnl_pct * leverage
            else:
                pnl_pct = 0
                roi = 0
            # 시각
            entry_ts = int(rep.get("createdTime", 0)) / 1000
            close_ts = int(group_trades[-1].get("updatedTime", 0)) / 1000
            hold_time = calc_hold_duration(entry_ts, close_ts) if entry_ts and close_ts else "—"
            entry_time_str = datetime.fromtimestamp(entry_ts, tz=KST).isoformat() if entry_ts else None
            close_time_str = datetime.fromtimestamp(close_ts, tz=KST).isoformat() if close_ts else None
            # 배팅금액
            position_size = entry_price * total_qty if entry_price else 0
            # 결과
            result = "승" if total_pnl > 0 else ("패" if total_pnl < 0 else "승")
            # 청산이유 (trade_events 매칭)
            event = None
            for oid in group_order_ids:
                event = match_trade_event(oid, symbol, strat, strat_events)
                if event:
                    break
            if event:
                close_reason = event.get("reason", "수동")
                tp1_reached = "Y" if event.get("tp1_reached") else "N"
                tp2_reached = "Y" if event.get("tp2_reached") else "N"
                extras = {}
                if "ema_state" in event:
                    extras["ema_state"] = event["ema_state"]
                if "squeeze_status" in event:
                    extras["squeeze"] = event["squeeze_status"]
                if "rsi_direction" in event:
                    extras["rsi_direction"] = event["rsi_direction"]
                if "funding_rate" in event:
                    extras["funding_rate"] = event["funding_rate"]
                if "oi_change" in event:
                    extras["oi_change"] = event["oi_change"]
                if event.get("reason") == "시간손절":
                    extras["time_stop"] = "Y"
            else:
                # 폴백: 수익률로 추정
                hold_hours = (close_ts - entry_ts) / 3600 if entry_ts and close_ts else 0
                close_reason = estimate_close_reason(pnl_pct, None, hold_hours)
                tp1_reached = "Y" if pnl_pct > 0 else "N"
                tp2_reached = "N"
                extras = {}
            # orderId: 그룹의 모든 orderId를 '|'로 연결
            combined_oid = "|".join(oid for oid in group_order_ids if oid)
            trade_record = {
                "coin": coin,
                "trade_num": next_num,
                "result": result,
                "direction": direction,
                "leverage": leverage,
                "entry_price": entry_price,
                "close_price": close_price,
                "qty": total_qty,
                "position_size": round(position_size, 2),
                "pnl": round(total_pnl, 4),
                "pnl_pct": round(pnl_pct, 2),
                "roi": round(roi, 2),
                "entry_time": entry_time_str,
                "close_time": close_time_str,
                "hold_time": hold_time,
                "close_reason": close_reason,
                "tp1_reached": tp1_reached,
                "tp2_reached": tp2_reached,
                "order_id": combined_oid,
                "extras": extras,
            }
            if create_trade_record(db_id, trade_record, strat, token):
                next_num += 1
                # orderId를 원장에 즉시 추가 (이번 사이클 내 중복 방지)
                for oid in group_order_ids:
                    if oid:
                        existing_ids.add(oid)
                ledger_changed = True
        # 전략별 원장 갱신
        ledger[strat] = existing_ids
    # ── 원장 저장 (변경 있을 때만) ──
    if ledger_changed:
        save_synced_orders(ledger)
# ══════════════════════════════════════════════
# 열린 포지션 동기화 (진행중 레코드)
# ══════════════════════════════════════════════
def get_open_order_ids(db_id, token):
    """노션 DB에서 '진행중' 레코드의 orderId → page_id 맵 조회"""
    open_records = {}  # orderId → page_id
    cursor = None
    for _ in range(10):
        payload = {
            "filter": {
                "property": "결과",
                "select": {"equals": "진행중"}
            },
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor
        time.sleep(0.35)
        resp = notion_post(f"/databases/{db_id}/query", payload, token)
        if not resp:
            break
        for page in resp.get("results", []):
            page_id = page["id"]
            props = page.get("properties", {})
            oid_rt = props.get("orderId", {}).get("rich_text", [])
            oid = oid_rt[0].get("plain_text", "") if oid_rt else ""
            coin_title = props.get("코인", {}).get("title", [])
            coin = coin_title[0].get("plain_text", "") if coin_title else ""
            open_records[oid] = {"page_id": page_id, "coin": coin}
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return open_records
def archive_notion_page(page_id, token):
    """노션 페이지 아카이브 (삭제 대신)"""
    time.sleep(0.35)
    return notion_patch(f"/pages/{page_id}", {"archived": True}, token)
def sync_open_positions(config, account_data, trade_events, position_entries):
    """활성 포지션 ↔ '진행중' 레코드 자동 동기화
    1. 새 포지션 → '진행중' 레코드 생성
    2. 닫힌 포지션(진행중 레코드만 남음) → 아카이브
    3. 기존 진행중 레코드 → 현재 ROI/PnL 업데이트
    """
    token = config["notion"]["token"]
    db_ids = config["notion"]["trade_log_dbs"]
    now = datetime.now(KST)
    for strat in STRATEGY_ORDER:
        db_id = db_ids.get(strat)
        if not db_id:
            continue
        positions = account_data.get(strat, {}).get("positions", [])
        # 현재 열린 심볼 목록
        open_symbols = set()
        for pos in positions:
            open_symbols.add(pos.get("symbol", ""))
        # DB에서 '진행중' 레코드 조회
        open_records = get_open_order_ids(db_id, token)
        # --- 1) 닫힌 포지션의 '진행중' 레코드 아카이브 ---
        closed_symbols = set()
        for t in account_data.get(strat, {}).get("closed_pnl", []):
            closed_symbols.add(t.get("symbol", ""))
        for oid, info in open_records.items():
            coin = info["coin"]
            symbol = f"{coin}USDT"
            if symbol not in open_symbols:
                if symbol in closed_symbols:
                    archive_notion_page(info["page_id"], token)
                    logger.info(f"[{STRATEGY_NAMES[strat]}] 진행중 아카이브: {coin} (청산 확인됨)")
                else:
                    logger.info(f"[{STRATEGY_NAMES[strat]}] 진행중 유지: {coin} (closed-pnl 대기중)")
        # --- 2) 새 포지션 → '진행중' 레코드 생성 ---
        existing_coins = {info["coin"] for info in open_records.values()}
        strat_events = [e for e in trade_events if e.get("strategy") == strat]
        for pos in positions:
            symbol = pos.get("symbol", "")
            coin = symbol.replace("USDT", "")
            if coin in existing_coins:
                # --- 3) 기존 진행중 → ROI/PnL 업데이트 ---
                for oid, info in open_records.items():
                    if info["coin"] == coin:
                        entry_price = float(pos.get("avgPrice", 0) or 0)
                        mark_price = float(pos.get("markPrice", 0) or 0)
                        side = pos.get("side", "")
                        unrealised_pnl = float(pos.get("unrealisedPnl", 0) or 0)
                        if side == "Buy" and entry_price > 0:
                            pnl_pct = (mark_price - entry_price) / entry_price * 100
                        elif side == "Sell" and entry_price > 0:
                            pnl_pct = (entry_price - mark_price) / entry_price * 100
                        else:
                            pnl_pct = 0
                        leverage = float(pos.get("leverage", 1) or 1)
                        roi = pnl_pct * leverage
                        real_entry_ts = get_real_entry_time(strat, pos, position_entries)
                        hold_sec = max(0, time.time() - real_entry_ts) if real_entry_ts > 0 else 0
                        hold_str = fmt_hold_time(hold_sec)
                        time.sleep(0.35)
                        notion_patch(f"/pages/{info['page_id']}", {
                            "properties": {
                                "수익금": {"number": round(unrealised_pnl, 4)},
                                "수익률": {"number": round(pnl_pct, 2)},
                                "ROI": {"number": round(roi, 2)},
                                "보유시간": {"rich_text": [{"text": {"content": hold_str}}]},
                            }
                        }, token)
                        logger.info(f"[{STRATEGY_NAMES[strat]}] 진행중 업데이트: {coin} "
                                     f"ROI={roi:+.2f}% PnL={unrealised_pnl:+.4f}")
                        break
                continue
            # 신규 포지션 → 진행중 레코드 생성
            entry_price = float(pos.get("avgPrice", 0) or 0)
            mark_price = float(pos.get("markPrice", 0) or 0)
            side = pos.get("side", "")
            qty = float(pos.get("size", 0) or 0)
            leverage = float(pos.get("leverage", 1) or 1)
            unrealised_pnl = float(pos.get("unrealisedPnl", 0) or 0)
            real_entry_ts = get_real_entry_time(strat, pos, position_entries)
            direction = "롱" if side == "Buy" else "숏"
            position_size = entry_price * qty if entry_price else 0
            if side == "Buy" and entry_price > 0:
                pnl_pct = (mark_price - entry_price) / entry_price * 100
            elif side == "Sell" and entry_price > 0:
                pnl_pct = (entry_price - mark_price) / entry_price * 100
            else:
                pnl_pct = 0
            roi = pnl_pct * leverage
            hold_sec = max(0, time.time() - real_entry_ts) if real_entry_ts > 0 else 0
            hold_str = fmt_hold_time(hold_sec)
            entry_time_str = datetime.fromtimestamp(
                real_entry_ts, tz=KST).isoformat() if real_entry_ts else None
            # trade_events에서 진입 정보 찾기
            event = None
            for ev in strat_events:
                if ev.get("symbol") == symbol and ev.get("action") == "진입":
                    event = ev
                    break
            extras = {}
            if event:
                if "funding_rate" in event:
                    extras["funding_rate"] = event["funding_rate"]
                if "oi_change" in event:
                    extras["oi_change"] = event["oi_change"]
                if "ema_state" in event:
                    extras["ema_state"] = event["ema_state"]
                if "squeeze_status" in event:
                    extras["squeeze"] = event["squeeze_status"]
                if "rsi_direction" in event:
                    extras["rsi_direction"] = event["rsi_direction"]
            # 다음 거래번호
            next_num = get_next_trade_number(db_id, token)
            trade_record = {
                "coin": coin,
                "trade_num": next_num,
                "result": "진행중",
                "direction": direction,
                "leverage": leverage,
                "entry_price": entry_price,
                "close_price": 0,
                "qty": qty,
                "position_size": round(position_size, 2),
                "pnl": round(unrealised_pnl, 4),
                "pnl_pct": round(pnl_pct, 2),
                "roi": round(roi, 2),
                "entry_time": entry_time_str,
                "close_time": None,
                "hold_time": hold_str,
                "close_reason": "수동",  # 아직 청산 안 됨
                "tp1_reached": "N",
                "tp2_reached": "N",
                "order_id": f"open-{symbol}",
                "extras": extras,
            }
            if create_trade_record(db_id, trade_record, strat, token):
                logger.info(f"[{STRATEGY_NAMES[strat]}] 진행중 생성: {coin} {direction} "
                             f"@ {entry_price}")
    logger.info("열린 포지션 동기화 완료")
# ══════════════════════════════════════════════
# 메인 실행
# ══════════════════════════════════════════════
def check_drawdown_limit(config, account_data):
    """Phase 3: DD 보호 — 전략별(-25%) + 통합(-15%) 이중 방어
    update_all.py가 DD 파일 쓰기 담당 (엔진은 읽기만)"""
    individual_caps = config["capital"].get("start_capital_individual", {})
    start_cap_common = config["capital"].get("start_capital_per_strategy", 0)
    # DD 임계값 (테스트 기간 -25%, 대자본 -20%)
    dd_stop_pct = -25  # 테스트 기간
    dd_warn_pct = -15
    dd_recovery_pct = -8  # 복구 조건
    # 통합 DD 임계값
    total_stop_pct = -15
    total_warn_pct = -10
    alerts = []

    total_equity = 0
    total_start_cap = 0

    for strat in STRATEGY_ORDER:
        sd = account_data.get(strat, {})
        balance = sd.get("balance", 0)
        start_cap = individual_caps.get(strat, start_cap_common)
        if start_cap <= 0:
            continue
        total_equity += balance
        total_start_cap += start_cap

        dd_pct = (balance - start_cap) / start_cap * 100
        dd_file = BASE_DIR / f"dd_protection_{strat}.json"

        # 기존 DD 파일 로드
        dd_data = {}
        if dd_file.exists():
            try:
                dd_data = json.loads(dd_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, ValueError):
                dd_data = {}

        currently_paused = dd_data.get("paused", False)

        if dd_pct < dd_stop_pct and not currently_paused:
            # 중단 발동
            dd_data = {
                "paused": True, "dd_pct": round(dd_pct, 2),
                "paused_at": datetime.now(KST).isoformat(),
                "balance": balance, "start_cap": start_cap,
            }
            _dd_atomic_write(dd_file, dd_data)
            msg = (f"🚨 [{STRATEGY_NAMES[strat]}] DD {dd_pct:.1f}% — 신규 진입 자동 중단.\n"
                   f"기존 포지션 SL/TP 정상. DD -8% 이상 + 1시간 유지 시 자동 재개.")
            alerts.append(msg)
            mc.send_telegram(config, f"[MAGI] {msg}")
            logger.critical(f"[{STRATEGY_NAMES[strat]}] DD 중단 발동: {dd_pct:.1f}%")

        elif currently_paused and dd_pct >= dd_recovery_pct:
            # 복구 조건: -8% 이상 + 1시간 유지
            paused_at = dd_data.get("paused_at", "")
            recovery_start = dd_data.get("recovery_start", "")
            if not recovery_start:
                # 복구 관찰 시작
                dd_data["recovery_start"] = datetime.now(KST).isoformat()
                dd_data["dd_pct"] = round(dd_pct, 2)
                _dd_atomic_write(dd_file, dd_data)
                logger.info(f"[{STRATEGY_NAMES[strat]}] DD {dd_pct:.1f}% — 복구 관찰 시작 (1시간)")
            else:
                # 관찰 중 — 1시간 경과 확인
                try:
                    rec_dt = datetime.fromisoformat(recovery_start)
                    elapsed_h = (datetime.now(KST) - rec_dt).total_seconds() / 3600
                    if elapsed_h >= 1.0:
                        dd_data = {"paused": False, "dd_pct": round(dd_pct, 2),
                                   "recovered_at": datetime.now(KST).isoformat()}
                        _dd_atomic_write(dd_file, dd_data)
                        msg = f"✅ [{STRATEGY_NAMES[strat]}] DD {dd_pct:.1f}% (1시간 유지 확인) — 신규 진입 재개."
                        mc.send_telegram(config, f"[MAGI] {msg}")
                        logger.info(msg)
                except (ValueError, TypeError):
                    pass

        elif currently_paused and dd_pct < dd_recovery_pct:
            # 복구 조건 미충족 → 관찰 리셋
            dd_data["recovery_start"] = ""
            dd_data["dd_pct"] = round(dd_pct, 2)
            _dd_atomic_write(dd_file, dd_data)

        elif dd_pct < dd_warn_pct and not currently_paused:
            # 경고만 (5분마다 반복 방지: 10분 간격)
            last_warn = dd_data.get("last_warn", "")
            should_warn = True
            if last_warn:
                try:
                    lw_dt = datetime.fromisoformat(last_warn)
                    if (datetime.now(KST) - lw_dt).total_seconds() < 600:
                        should_warn = False
                except (ValueError, TypeError):
                    pass
            if should_warn:
                dd_data = {"paused": False, "dd_pct": round(dd_pct, 2),
                           "last_warn": datetime.now(KST).isoformat()}
                _dd_atomic_write(dd_file, dd_data)
                mc.send_telegram(config,
                    f"[MAGI] ⚠️ {STRATEGY_NAMES[strat]} DD {dd_pct:.1f}% — 주의 필요. 신규 진입 계속됨.")
                logger.warning(f"[{STRATEGY_NAMES[strat]}] DD 경고: {dd_pct:.1f}%")
        else:
            # 정상 상태 업데이트
            dd_data = {"paused": False, "dd_pct": round(dd_pct, 2)}
            _dd_atomic_write(dd_file, dd_data)

    # 통합 DD 체크
    if total_start_cap > 0:
        total_dd_pct = (total_equity - total_start_cap) / total_start_cap * 100
        dd_total_file = BASE_DIR / "dd_protection_total.json"
        dd_total_data = {}
        if dd_total_file.exists():
            try:
                dd_total_data = json.loads(dd_total_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, ValueError):
                dd_total_data = {}

        total_paused = dd_total_data.get("paused", False)

        if total_dd_pct < total_stop_pct and not total_paused:
            dd_total_data = {
                "paused": True, "dd_pct": round(total_dd_pct, 2),
                "paused_at": datetime.now(KST).isoformat(),
            }
            _dd_atomic_write(dd_total_file, dd_total_data)
            msg = f"🚨 통합 DD {total_dd_pct:.1f}% — 전 전략 신규 진입 중단."
            mc.send_telegram(config, f"[MAGI] {msg}")
            logger.critical(f"통합 DD 중단 발동: {total_dd_pct:.1f}%")
        elif total_paused and total_dd_pct >= dd_recovery_pct:
            recovery_start = dd_total_data.get("recovery_start", "")
            if not recovery_start:
                dd_total_data["recovery_start"] = datetime.now(KST).isoformat()
                dd_total_data["dd_pct"] = round(total_dd_pct, 2)
                _dd_atomic_write(dd_total_file, dd_total_data)
            else:
                try:
                    rec_dt = datetime.fromisoformat(recovery_start)
                    if (datetime.now(KST) - rec_dt).total_seconds() / 3600 >= 1.0:
                        dd_total_data = {"paused": False, "dd_pct": round(total_dd_pct, 2),
                                         "recovered_at": datetime.now(KST).isoformat()}
                        _dd_atomic_write(dd_total_file, dd_total_data)
                        mc.send_telegram(config, f"[MAGI] ✅ 통합 DD {total_dd_pct:.1f}% — 전 전략 재개.")
                except (ValueError, TypeError):
                    pass
        elif total_paused and total_dd_pct < dd_recovery_pct:
            dd_total_data["recovery_start"] = ""
            dd_total_data["dd_pct"] = round(total_dd_pct, 2)
            _dd_atomic_write(dd_total_file, dd_total_data)
        elif total_dd_pct < total_warn_pct and not total_paused:
            last_warn = dd_total_data.get("last_warn", "")
            should_warn = True
            if last_warn:
                try:
                    if (datetime.now(KST) - datetime.fromisoformat(last_warn)).total_seconds() < 600:
                        should_warn = False
                except (ValueError, TypeError):
                    pass
            if should_warn:
                dd_total_data = {"paused": False, "dd_pct": round(total_dd_pct, 2),
                                 "last_warn": datetime.now(KST).isoformat()}
                _dd_atomic_write(dd_total_file, dd_total_data)
                mc.send_telegram(config, f"[MAGI] ⚠️ 통합 DD {total_dd_pct:.1f}% — 주의 필요.")
        else:
            dd_total_data = {"paused": False, "dd_pct": round(total_dd_pct, 2)}
            _dd_atomic_write(dd_total_file, dd_total_data)

    return alerts


def _dd_atomic_write(filepath, data):
    """DD JSON 파일 원자적 쓰기"""
    filepath = Path(filepath)
    tmp = filepath.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(filepath)

# ══════════════════════════════════════════════
# 픽셀 오피스 상태 매핑
# ══════════════════════════════════════════════
def build_pixel_office_status(account_data):
    """하트비트 + 포지션 + 계좌 데이터를 픽셀 오피스 상태로 매핑"""
    status_map = {}
    for strat in STRATEGY_ORDER:
        sd = account_data.get(strat, {})
        positions = sd.get("positions", [])
        balance = sd.get("balance", 0)

        # 하트비트로 alive 여부 판단
        hb_path = BASE_DIR / f"heartbeat_{strat}.json"
        alive = False
        try:
            if hb_path.exists():
                with open(hb_path, "r") as f:
                    hb = json.load(f)
                elapsed = time.time() - hb.get("epoch", 0)
                alive = elapsed < 1200  # 20분 이내면 살아있음
        except Exception:
            pass

        if not alive:
            status_map[strat] = {
                "status": "offline",
                "mood": "idle",
                "task": ""
            }
        elif positions:
            # 포지션 보유 중 → 모니터링
            coins = [p.get("symbol", "?").replace("USDT", "") for p in positions]
            total_pnl = sum(float(p.get("unrealisedPnl", 0)) for p in positions)
            pnl_str = f"+${total_pnl:.1f}" if total_pnl >= 0 else f"-${abs(total_pnl):.1f}"
            status_map[strat] = {
                "status": "active",
                "mood": "monitoring",
                "task": f"📊 {', '.join(coins[:3])} ({pnl_str})"
            }
        else:
            # 포지션 없음 → 스캔 대기
            status_map[strat] = {
                "status": "active",
                "mood": "analyzing",
                "task": f"🔍 스캔 대기 (잔고 ${balance:.0f})"
            }

    # 별이: update_all 실행 중이면 active
    status_map["byeol"] = {
        "status": "active",
        "mood": "analyzing",
        "task": "📊 대시보드 업데이트 중"
    }

    return status_map

def build_pnl_summary(account_data):
    """closed-pnl 데이터에서 오늘의 PnL 요약 생성"""
    pnl_data = {}
    for strat in STRATEGY_ORDER:
        sd = account_data.get(strat, {})
        closed_list = sd.get("closed_pnl", [])
        total_pnl = 0.0
        trade_count = 0
        for cp in closed_list:
            pnl = float(cp.get("closedPnl", 0))
            total_pnl += pnl
            trade_count += 1
        pnl_data[strat] = {
            "pnl": round(total_pnl, 2),
            "trades": trade_count
        }
    return pnl_data

def main():
    start_time = time.time()
    logger.info("=" * 50)
    logger.info("📊 update_all.py v5.2 실행 시작")
    try:
        # 설정 로드
        config = mc.load_config()
        # 픽셀 오피스 Firebase 초기화
        fw.init_firebase(config)
        notion_cfg = config.get("notion", {})
        if not notion_cfg.get("token"):
            logger.error("Notion 토큰이 config.json에 없습니다!")
            return
        # 1. BTC 가격
        btc_price = fetch_btc_price()
        logger.info(f"BTC 가격: ${btc_price:,.0f}" if btc_price else "BTC 가격 조회 실패")
        # 2. 로컬 파일 읽기
        trade_events = read_trade_events()
        logger.info(f"trade_events: {len(trade_events)}건")
        # 3. 4개 계정 데이터 수집
        account_data = fetch_account_data(config)
        for strat in STRATEGY_ORDER:
            sd = account_data.get(strat, {})
            logger.info(f"[{STRATEGY_NAMES[strat]}] 잔고=${sd.get('balance',0):.2f}, "
                        f"포지션={len(sd.get('positions',[]))}건, "
                        f"closed-pnl={len(sd.get('closed_pnl',[]))}건")
        # 3.5. 드로우다운 한계 체크
        dd_alerts = check_drawdown_limit(config, account_data)
        if dd_alerts:
            logger.warning(f"드로우다운 경고 {len(dd_alerts)}건 발생")
        # 4. 포지션 진입시간 캐시 로드 + 신규 포지션 실제 진입시각 조회
        position_entries = load_position_entries()
        populate_new_position_entries(config, account_data, position_entries)
        save_position_entries(position_entries)
        # 5. 데이터 가공
        data = process_data(config, account_data, btc_price, trade_events, position_entries)
        # 5. 대시보드 블록 캐시 로드
        blocks = load_dashboard_blocks(config)
        if not blocks:
            logger.error("대시보드 블록 캐시 로드 실패!")
            return
        # 6. 대시보드 업데이트 (원자적: 같은 데이터로)
        update_dashboard(config, blocks, data)
        # 6.5. 픽셀 오피스 Firebase 상태 업데이트
        try:
            pixel_status = build_pixel_office_status(account_data)
            fw.update_all_status(pixel_status)
            logger.info("🔥 픽셀 오피스 상태 업데이트 완료")
            # PnL 요약도 push
            pnl_data = build_pnl_summary(account_data)
            if pnl_data:
                today = datetime.now(KST).strftime("%Y-%m-%d")
                fw.update_pnl(today, pnl_data)
                logger.info("🔥 PnL 요약 업데이트 완료")
        except Exception as e:
            logger.warning(f"🔥 픽셀 오피스 업데이트 실패 (트레이딩 무영향): {e}")
        # 7. 거래 로그 동기화 (같은 데이터로 — 원자적)
        sync_trade_logs(config, account_data, trade_events)
        # 8. 열린 포지션 → '진행중' 레코드 동기화
        sync_open_positions(config, account_data, trade_events, position_entries)
        elapsed = time.time() - start_time
        logger.info(f"📊 update_all.py v5.2 완료 ({elapsed:.1f}초)")
    except Exception as e:
        logger.error(f"update_all.py 에러: {e}")
        logger.error(traceback.format_exc())
        # 텔레그램 알림
        try:
            config = mc.load_config()
            mc.send_telegram(config, f"⚠️ update_all.py 에러\n{str(e)[:200]}")
        except Exception:
            pass
if __name__ == "__main__":
    main()
