#!/usr/bin/env python3
"""
MAGI 마켓센서 v2.0 — market_sensor.py
설계: 소니 | 구현: 별이
실행: launchd 6시간 주기 (00:09, 06:09, 12:09, 18:09 KST)

역할 3가지:
  1. 종목 풀 관리 (active_coins.json)
  2. 시장 데이터 수집 (1D 캔들 → EMA/RSI/ADX, 펀딩비/OI 스냅샷)
  3. 팀 리포트 (텔레그램 + market_snapshot.json)
"""

import json
import os
import sys
import time
import hmac
import hashlib
import logging
import logging.handlers
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# ──────────────────────────────────────────────
# 경로 설정
# ──────────────────────────────────────────────
BASE_DIR = Path.home() / "magi"
CONFIG_PATH = BASE_DIR / "config.json"
ACTIVE_COINS_PATH = BASE_DIR / "active_coins.json"
ACTIVE_COINS_TMP = BASE_DIR / "active_coins.tmp"
ACTIVE_COINS_BACKUP = BASE_DIR / "active_coins.backup.json"
MARKET_SNAPSHOT_PATH = BASE_DIR / "market_snapshot.json"
MARKET_SNAPSHOT_TMP = BASE_DIR / "market_snapshot.tmp"
LOG_PATH = BASE_DIR / "logs" / "sensor.log"

KST = timezone(timedelta(hours=9))
BYBIT_BASE = "https://api.bybit.com"

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
os.makedirs(LOG_PATH.parent, exist_ok=True)

logger = logging.getLogger("market_sensor")
logger.setLevel(logging.INFO)

fh = logging.handlers.RotatingFileHandler(
    LOG_PATH, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(sh)


# ──────────────────────────────────────────────
# 설정 로드
# ──────────────────────────────────────────────
def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


# ──────────────────────────────────────────────
# HTTP 유틸
# ──────────────────────────────────────────────
def http_get(url, headers=None, timeout=15):
    """GET 요청 (30초 간격 3회 재시도)"""
    for attempt in range(3):
        try:
            req = Request(url, headers=headers or {})
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError, TimeoutError) as e:
            logger.warning(f"HTTP GET 실패 (시도 {attempt+1}/3): {url[:80]}... → {e}")
            if attempt < 2:
                time.sleep(30)
    return None


def bybit_public_get(endpoint, params=None):
    """바이비트 공개 API (키 불필요)"""
    url = f"{BYBIT_BASE}{endpoint}"
    if params:
        url += "?" + urlencode(params)
    data = http_get(url)
    if data and data.get("retCode") == 0:
        return data.get("result", {})
    if data:
        logger.error(f"바이비트 API 에러: {data.get('retMsg', 'unknown')} — {endpoint}")
    return None


def bybit_private_get(endpoint, params, api_key, api_secret):
    """바이비트 인증 API (서브계정별 키 사용)"""
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    query_string = urlencode(sorted(params.items()))
    sign_payload = f"{timestamp}{api_key}{recv_window}{query_string}"
    signature = hmac.new(
        api_secret.encode(), sign_payload.encode(), hashlib.sha256
    ).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }
    url = f"{BYBIT_BASE}{endpoint}?{query_string}"
    data = http_get(url, headers=headers)
    if data and data.get("retCode") == 0:
        return data.get("result", {})
    if data:
        logger.error(f"바이비트 Private API 에러: {data.get('retMsg', 'unknown')} — {endpoint}")
    return None


# ──────────────────────────────────────────────
# Atomic Write
# ──────────────────────────────────────────────
def atomic_write(data, target_path, tmp_path):
    """tmp 파일에 쓰고 rename (OS 레벨 원자적)"""
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.rename(tmp_path, target_path)


# ──────────────────────────────────────────────
# ① 종목 풀 선정
# ──────────────────────────────────────────────
def fetch_coin_pool(config):
    """바이비트 USDT 무기한 전체 조회 → 거래대금 필터 → 상위 20개"""
    ms_cfg = config["market_sensor"]
    min_vol = ms_cfg["min_volume_24h"]
    max_coins = ms_cfg["max_coins"]
    min_days = ms_cfg["min_listing_days"]
    stables = set(ms_cfg["stablecoin_exclude"])

    # 전체 USDT 무기한 선물 티커 조회
    result = bybit_public_get("/v5/market/tickers", {"category": "linear"})
    if not result:
        logger.error("티커 조회 실패 — 이전 종목 풀 유지")
        return None

    tickers = result.get("list", [])
    now_ts = time.time()
    cutoff_ts = now_ts - (min_days * 86400)

    # 인스트루먼트 정보 (상장일 확인용)
    instruments_result = bybit_public_get("/v5/market/instruments-info", {
        "category": "linear", "limit": "1000"
    })
    instrument_map = {}
    if instruments_result:
        for inst in instruments_result.get("list", []):
            instrument_map[inst["symbol"]] = inst

    candidates = []
    for t in tickers:
        symbol = t.get("symbol", "")
        # USDT 마진만
        if not symbol.endswith("USDT"):
            continue
        # 스테이블코인 제외
        base = symbol.replace("USDT", "")
        if base in stables:
            continue
        # 거래대금 필터
        vol_24h = float(t.get("turnover24h", 0))
        if vol_24h < min_vol:
            continue
        # 상장일 필터
        inst = instrument_map.get(symbol, {})
        launch_time = inst.get("launchTime", "")
        if launch_time:
            try:
                launch_ts = int(launch_time) / 1000
                if launch_ts > cutoff_ts:
                    continue  # 30일 미만 상장
            except (ValueError, TypeError):
                pass

        candidates.append({
            "symbol": symbol,
            "display": base,
            "volume_24h": round(vol_24h),
            "last_price": float(t.get("lastPrice", 0)),
            "price_change_24h": float(t.get("price24hPcnt", 0)) * 100,
            "funding_rate": float(t.get("fundingRate", 0)),
        })

    # 거래대금 순 정렬 → 상위 max_coins
    candidates.sort(key=lambda x: x["volume_24h"], reverse=True)
    selected = candidates[:max_coins]

    # 티어 분류
    for i, coin in enumerate(selected):
        if coin["display"] in ("BTC", "ETH"):
            coin["tier"] = 1
        elif i < 10:
            coin["tier"] = 2
        else:
            coin["tier"] = 3

    return (selected, candidates)


def detect_pool_changes(new_coins, old_path, all_candidates=None):
    """종목 풀 변동 감지 (추가/제거 + 제거 사유 구분)"""
    added, removed = [], []
    try:
        with open(old_path, "r") as f:
            old_data = json.load(f)
        old_symbols = {c["symbol"] for c in old_data.get("coins", [])}
        old_map = {c["symbol"]: c for c in old_data.get("coins", [])}
    except (FileNotFoundError, json.JSONDecodeError):
        old_symbols = set()
        old_map = {}
    new_symbols = {c["symbol"] for c in new_coins}
    candidate_symbols = {c["symbol"] for c in all_candidates} if all_candidates else set()
    candidate_map = {c["symbol"]: c for c in all_candidates} if all_candidates else {}
    for s in new_symbols - old_symbols:
        coin = next(c for c in new_coins if c["symbol"] == s)
        added.append(coin)
    for s in old_symbols - new_symbols:
        if s in candidate_symbols:
            coin = candidate_map[s]
            coin["remove_reason"] = "순위탈락"
        else:
            coin = old_map.get(s, {"symbol": s, "display": s.replace("USDT", ""), "volume_24h": 0})
            coin["remove_reason"] = "거래대금미달"
        removed.append(coin)
    return added, removed

def save_coin_pool(coins):
    """종목 풀 저장 (atomic write + backup)"""
    now = datetime.now(KST).isoformat()
    data = {
        "updated_at": now,
        "coins": coins,
        "total": len(coins),
    }
    atomic_write(data, ACTIVE_COINS_PATH, ACTIVE_COINS_TMP)
    # 백업도 atomic write
    backup_tmp = BASE_DIR / "active_coins.backup.tmp"
    atomic_write(data, ACTIVE_COINS_BACKUP, backup_tmp)
    logger.info(f"종목 풀 저장 완료: {len(coins)}개 코인")


# ──────────────────────────────────────────────
# ② 기술 지표 계산 (EMA / RSI / ADX)
# ──────────────────────────────────────────────
def fetch_kline(symbol, interval="D", limit=100):
    """바이비트 캔들 데이터 조회"""
    result = bybit_public_get("/v5/market/kline", {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    })
    if not result:
        return None
    # 바이비트는 최신→과거 순 반환 → 뒤집기
    raw = result.get("list", [])
    candles = []
    for c in reversed(raw):
        candles.append({
            "ts": int(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5]),
        })
    return candles


def calc_ema(closes, period):
    """지수이동평균"""
    if len(closes) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for price in closes[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema


def calc_rsi(closes, period=14):
    """RSI 계산"""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_adx(candles, period=14):
    """ADX 계산"""
    if len(candles) < period * 2 + 1:
        return None
    tr_list, plus_dm_list, minus_dm_list = [], [], []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        prev_high = candles[i-1]["high"]
        prev_low = candles[i-1]["low"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        plus_dm = max(high - prev_high, 0) if (high - prev_high) > (prev_low - low) else 0
        minus_dm = max(prev_low - low, 0) if (prev_low - low) > (high - prev_high) else 0
        tr_list.append(tr)
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)

    # Smoothed TR, +DM, -DM
    atr = sum(tr_list[:period])
    plus_dm_s = sum(plus_dm_list[:period])
    minus_dm_s = sum(minus_dm_list[:period])
    dx_list = []
    for i in range(period, len(tr_list)):
        atr = atr - atr / period + tr_list[i]
        plus_dm_s = plus_dm_s - plus_dm_s / period + plus_dm_list[i]
        minus_dm_s = minus_dm_s - minus_dm_s / period + minus_dm_list[i]
        plus_di = (plus_dm_s / atr * 100) if atr != 0 else 0
        minus_di = (minus_dm_s / atr * 100) if atr != 0 else 0
        di_sum = plus_di + minus_di
        dx = abs(plus_di - minus_di) / di_sum * 100 if di_sum != 0 else 0
        dx_list.append(dx)

    if len(dx_list) < period:
        return None
    adx = sum(dx_list[:period]) / period
    for i in range(period, len(dx_list)):
        adx = (adx * (period - 1) + dx_list[i]) / period
    return adx


def analyze_coin(symbol):
    """코인별 기술 분석 (1D 캔들 기반)"""
    candles = fetch_kline(symbol, "D", 100)
    if not candles or len(candles) < 30:
        return None

    closes = [c["close"] for c in candles]

    # EMA 7, 25, 99
    ema7 = calc_ema(closes, 7)
    ema25 = calc_ema(closes, 25)
    ema99 = calc_ema(closes, 99)

    # EMA 정배열/역배열 판정
    if ema7 and ema25 and ema99:
        e7, e25, e99 = ema7[-1], ema25[-1], ema99[-1]
        if e7 > e25 > e99:
            ema_state = "정배열"
        elif e7 < e25 < e99:
            ema_state = "역배열"
        else:
            ema_state = "혼조"
    else:
        ema_state = "데이터부족"

    rsi = calc_rsi(closes, 14)
    adx = calc_adx(candles, 14)

    return {
        "ema_state": ema_state,
        "ema7": round(ema7[-1], 2) if ema7 else None,
        "ema25": round(ema25[-1], 2) if ema25 else None,
        "ema99": round(ema99[-1], 2) if ema99 else None,
        "rsi": round(rsi, 1) if rsi else None,
        "adx": round(adx, 1) if adx else None,
    }


def judge_direction(analysis):
    """코인별 방향 판정 (설계서 기준)"""
    if not analysis or analysis.get("rsi") is None or analysis.get("adx") is None:
        return "⚪", "중립"
    ema = analysis["ema_state"]
    rsi = analysis["rsi"]
    adx = analysis["adx"]

    if ema == "정배열" and rsi > 55 and adx > 25:
        return "🟢", "강한상승"
    elif ema == "정배열":
        return "🟡", "약한상승"
    elif ema == "역배열" and rsi < 45 and adx > 25:
        return "🔴", "강한하락"
    elif ema == "역배열":
        return "🟠", "약한하락"
    else:
        return "⚪", "중립"


def judge_market(directions):
    """시장 전체 판정 (60% 기준)"""
    total = len(directions)
    if total == 0:
        return "횡보장"
    bullish = sum(1 for _, d in directions if d in ("강한상승", "약한상승"))
    bearish = sum(1 for _, d in directions if d in ("강한하락", "약한하락"))
    if bullish / total >= 0.6:
        return "상승장"
    elif bearish / total >= 0.6:
        return "하락장"
    return "횡보장"


# ──────────────────────────────────────────────
# ③ 펀딩비 / OI 스냅샷 (릴리스 리포트용)
# ──────────────────────────────────────────────
def fetch_funding_oi_snapshot(coins):
    """종목 풀 코인의 펀딩비 + OI 스냅샷"""
    snapshots = []
    for coin in coins:
        symbol = coin["symbol"]
        # OI
        oi_result = bybit_public_get("/v5/market/open-interest", {
            "category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": "1"
        })
        oi_value = 0
        if oi_result and oi_result.get("list"):
            oi_value = float(oi_result["list"][0].get("openInterest", 0))

        snapshots.append({
            "symbol": symbol,
            "display": coin["display"],
            "funding_rate": coin.get("funding_rate", 0),
            "open_interest": oi_value,
        })
        time.sleep(0.1)  # rate limit 보호

    return snapshots


# ──────────────────────────────────────────────
# ④ 활성 포지션 + 당일 실현 손익 조회 (4개 서브계정)
# ──────────────────────────────────────────────
def fetch_all_positions(config):
    """4개 서브계정의 활성 포지션"""
    accounts = config["bybit"]["accounts"]
    strategies = config["strategies"]
    all_positions = []

    for strat_key, acct in accounts.items():
        api_key = acct["api_key"]
        api_secret = acct["api_secret"]
        if "YOUR_" in api_key:
            continue  # 미설정 계정 스킵

        result = bybit_private_get("/v5/position/list", {
            "category": "linear", "settleCoin": "USDT"
        }, api_key, api_secret)

        if result and result.get("list"):
            for pos in result["list"]:
                size = float(pos.get("size", 0))
                if size == 0:
                    continue
                side = pos.get("side", "")
                entry_price = float(pos.get("avgPrice", 0))
                mark_price = float(pos.get("markPrice", 0))
                unrealised_pnl = float(pos.get("unrealisedPnl", 0))
                leverage = float(pos.get("leverage", 1))
                # ROI 계산
                position_value = size * entry_price
                roi_pct = (unrealised_pnl / (position_value / leverage) * 100) if position_value > 0 else 0

                strat_info = strategies.get(strat_key, {})
                all_positions.append({
                    "strategy": strat_key,
                    "strategy_name": strat_info.get("name", strat_key),
                    "strategy_icon": strat_info.get("icon", ""),
                    "symbol": pos.get("symbol", ""),
                    "display": pos.get("symbol", "").replace("USDT", ""),
                    "side": "롱" if side == "Buy" else "숏",
                    "size": size,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealised_pnl": round(unrealised_pnl, 2),
                    "roi_pct": round(roi_pct, 2),
                    "leverage": leverage,
                })

    return all_positions


def fetch_today_closed_pnl(config):
    """4개 서브계정의 당일 실현 손익"""
    accounts = config["bybit"]["accounts"]
    strategies = config["strategies"]
    now = datetime.now(KST)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(today_start.timestamp() * 1000)

    all_closed = []
    for strat_key, acct in accounts.items():
        api_key = acct["api_key"]
        api_secret = acct["api_secret"]
        if "YOUR_" in api_key:
            continue

        result = bybit_private_get("/v5/position/closed-pnl", {
            "category": "linear", "limit": "50", "startTime": str(start_ms)
        }, api_key, api_secret)

        strat_info = strategies.get(strat_key, {})
        if result and result.get("list"):
            for trade in result["list"]:
                pnl = float(trade.get("closedPnl", 0))
                all_closed.append({
                    "strategy": strat_key,
                    "strategy_name": strat_info.get("name", strat_key),
                    "strategy_icon": strat_info.get("icon", ""),
                    "symbol": trade.get("symbol", ""),
                    "display": trade.get("symbol", "").replace("USDT", ""),
                    "side": "숏" if trade.get("side") == "Buy" else "롱",  # side 반전
                    "pnl": round(pnl, 2),
                    "result": "승" if pnl > 0 else "패",
                })

    return all_closed


def fetch_balances(config):
    """4개 서브계정 잔고"""
    accounts = config["bybit"]["accounts"]
    balances = {}
    for strat_key, acct in accounts.items():
        api_key = acct["api_key"]
        api_secret = acct["api_secret"]
        if "YOUR_" in api_key:
            balances[strat_key] = 0
            continue

        result = bybit_private_get("/v5/account/wallet-balance", {
            "accountType": "UNIFIED"
        }, api_key, api_secret)

        equity = 0
        if result and result.get("list"):
            for acct_data in result["list"]:
                equity = float(acct_data.get("totalEquity", 0))
                break
        balances[strat_key] = round(equity, 2)

    return balances


# ──────────────────────────────────────────────
# ⑤ 전략별 기회 감지 요약
# ──────────────────────────────────────────────
def strategy_summary(coins_analysis, positions, closed_pnl, config):
    """각 전략의 현재 상태 요약"""
    strategies = config["strategies"]
    summary = {}

    for strat_key, strat_info in strategies.items():
        strat_positions = [p for p in positions if p["strategy"] == strat_key]
        strat_closed = [c for c in closed_pnl if c["strategy"] == strat_key]
        wins = sum(1 for c in strat_closed if c["result"] == "승")
        losses = sum(1 for c in strat_closed if c["result"] == "패")
        max_pos = strat_info.get("max_positions", 3)

        # 전략별 상태 판단
        if strat_key == "kai":
            # 카이: EMA 정배열/역배열 코인 수 확인
            bearish_coins = sum(1 for ca in coins_analysis if ca.get("ema_state") == "역배열")
            bullish_coins = sum(1 for ca in coins_analysis if ca.get("ema_state") == "정배열")
            trend_count = bearish_coins + bullish_coins
            if trend_count > 0:
                state = "활성"
                detail = f"정배열 {bullish_coins}개 / 역배열 {bearish_coins}개"
            else:
                state = "대기"
                detail = "EMA 추세 미확인"

        elif strat_key == "jet":
            # 제트: BB 스퀴즈 감지는 엔진이 하므로 간략히
            state = "대기"
            detail = "스퀴즈 감지 — 엔진 확인 필요"

        elif strat_key == "boomerang":
            # 부메랑: RSI 극단
            overbought = sum(1 for ca in coins_analysis if ca.get("rsi") and ca["rsi"] > 70)
            oversold = sum(1 for ca in coins_analysis if ca.get("rsi") and ca["rsi"] < 30)
            extreme = overbought + oversold
            if extreme > 0:
                state = "활성"
                detail = f"RSI 극단 {extreme}개 (과매수 {overbought} / 과매도 {oversold})"
            else:
                state = "대기"
                detail = "RSI 극단 코인 0개"

        elif strat_key == "release":
            # 릴리스: 펀딩비 극단
            extreme_funding = sum(1 for ca in coins_analysis
                                  if abs(ca.get("funding_rate", 0)) >= 0.0008)
            if extreme_funding > 0:
                state = "주의"
                detail = f"극단 펀딩비 {extreme_funding}개 코인"
            else:
                state = "대기"
                detail = "펀딩비 정상 범위"
        else:
            state = "대기"
            detail = ""

        summary[strat_key] = {
            "name": strat_info["name"],
            "icon": strat_info["icon"],
            "state": state,
            "detail": detail,
            "positions": len(strat_positions),
            "max_positions": max_pos,
            "wins": wins,
            "losses": losses,
        }

    return summary


# ──────────────────────────────────────────────
# ⑥ 텔레그램 리포트
# ──────────────────────────────────────────────
def send_telegram(config, message):
    """텔레그램 메시지 발송"""
    token = config["telegram"]["bot_token"]
    chat_id = config["telegram"]["chat_id"]
    if "YOUR_" in token:
        logger.warning("텔레그램 미설정 — 리포트 발송 생략")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }).encode()
    req = Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                logger.info("텔레그램 리포트 발송 성공")
                return True
            else:
                logger.error(f"텔레그램 발송 실패: {result}")
                return False
    except Exception as e:
        logger.error(f"텔레그램 발송 에러: {e}")
        return False


def build_report(coins, coins_analysis, market_state, directions,
                 strat_summary, positions, closed_pnl, balances,
                 pool_added, pool_removed, config):
    """텔레그램 리포트 텍스트 생성"""
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    strategies = config["strategies"]

    # BTC/ETH 정보
    btc = next((c for c in coins if c["display"] == "BTC"), None)
    eth = next((c for c in coins if c["display"] == "ETH"), None)
    btc_a = next((ca for ca in coins_analysis if ca.get("symbol") == "BTCUSDT"), None)
    eth_a = next((ca for ca in coins_analysis if ca.get("symbol") == "ETHUSDT"), None)

    lines = [
        f"📊 <b>MAGI 마켓센서 v2.0</b>",
        f"🕐 {now}",
        "",
        f"📋 종목 풀: {len(coins)}개 코인 활성 (최대 20개)",
        f"(24h 거래대금 $50M 이상)",
        "",
        "═══ <b>시장 전체 요약</b> ═══",
    ]

    # 시장 상태
    bull_count = sum(1 for _, d in directions if d in ("강한상승", "약한상승"))
    bear_count = sum(1 for _, d in directions if d in ("강한하락", "약한하락"))
    lines.append(f"🌐 시장 상태: {market_state} ({bull_count}↑ {bear_count}↓ / {len(directions)})")

    if btc:
        ema_str = btc_a["ema_state"] if btc_a else "—"
        lines.append(f"📈 BTC: {btc['price_change_24h']:+.1f}% (24h) | EMA {ema_str}")
    if eth:
        ema_str = eth_a["ema_state"] if eth_a else "—"
        lines.append(f"📈 ETH: {eth['price_change_24h']:+.1f}% (24h) | EMA {ema_str}")

    # 전략별 현황
    lines.append("")
    lines.append("═══ <b>전략별 현황</b> ═══")
    for key in ["kai", "jet", "boomerang", "release"]:
        s = strat_summary.get(key, {})
        icon = s.get("icon", "")
        name = s.get("name", key)
        state = s.get("state", "—")
        detail = s.get("detail", "")
        wins = s.get("wins", 0)
        losses = s.get("losses", 0)
        pos = s.get("positions", 0)
        max_p = s.get("max_positions", 3)
        lines.append(f"{icon} {name}: {state} ({detail})")
        lines.append(f"   → 오늘 {wins}승 {losses}패 | 포지션 {pos}/{max_p}")

    # 종합 현황
    total_positions = len(positions)
    total_unrealised = sum(p["unrealised_pnl"] for p in positions)
    total_realised = sum(c["pnl"] for c in closed_pnl)
    total_wins = sum(1 for c in closed_pnl if c["result"] == "승")
    total_losses = sum(1 for c in closed_pnl if c["result"] == "패")
    total_equity = sum(balances.values())

    lines.append("")
    lines.append("═══ <b>종합 현황</b> ═══")
    lines.append(f"총 포지션: {total_positions}/10")
    lines.append(f"미실현 손익: ${total_unrealised:+.2f}")
    lines.append(f"오늘 실현 손익: ${total_realised:+.2f} ({total_wins}승 {total_losses}패)")
    lines.append(f"총 Equity: ${total_equity:,.2f}")

    # 종목 풀 변동
    if pool_added or pool_removed:
        lines.append("")
        lines.append("═══ <b>종목 풀 변동</b> ═══")
        for c in pool_added:
            vol_m = c["volume_24h"] / 1_000_000
            lines.append(f"➕ 추가: {c['symbol']} (${vol_m:.0f}M)")
        for c in pool_removed:
            vol_m = c["volume_24h"] / 1_000_000
            reason = c.get("remove_reason", "거래대금미달")
            if reason == "순위탈락":
                lines.append(f"➖ 제거: {c['symbol']} (${vol_m:.0f}M → 상위 20 순위 밖)")
            else:
                lines.append(f"➖ 제거: {c['symbol']} (${vol_m:.0f}M → 거래대금 기준 미달)")

    # 활성 포지션 상세
    if positions:
        lines.append("")
        lines.append("═══ <b>활성 포지션 상세</b> ═══")
        # ROI 높은 순
        sorted_pos = sorted(positions, key=lambda p: p["roi_pct"], reverse=True)
        for p in sorted_pos:
            lines.append(
                f"{p['strategy_icon']} {p['strategy_name']}: "
                f"{p['display']} {p['side']} ({p['roi_pct']:+.1f}%) "
                f"${p['unrealised_pnl']:+.2f}"
            )

    return "\n".join(lines)


# ──────────────────────────────────────────────
# ⑦ market_snapshot.json 저장
# ──────────────────────────────────────────────
def save_snapshot(coins, coins_analysis, market_state, directions,
                  strat_summary, positions, closed_pnl, balances,
                  funding_oi):
    """노션 기록용 스냅샷 저장"""
    now = datetime.now(KST).isoformat()
    snapshot = {
        "timestamp": now,
        "market_state": market_state,
        "coin_count": len(coins),
        "coins": coins,
        "analysis": coins_analysis,
        "directions": [{"icon": icon, "state": state} for icon, state in directions],
        "strategy_summary": strat_summary,
        "positions": positions,
        "closed_pnl": closed_pnl,
        "balances": balances,
        "funding_oi": funding_oi,
    }
    atomic_write(snapshot, MARKET_SNAPSHOT_PATH, MARKET_SNAPSHOT_TMP)
    logger.info("market_snapshot.json 저장 완료")


# ──────────────────────────────────────────────
# 메인 파이프라인
# ──────────────────────────────────────────────
def main():
    start_time = time.time()
    logger.info("=" * 50)
    logger.info("MAGI 마켓센서 v2.0 실행 시작")
    logger.info("=" * 50)

    # 설정 로드
    try:
        config = load_config()
    except Exception as e:
        logger.critical(f"config.json 로드 실패: {e}")
        sys.exit(1)

    # ① 종목 풀 선정
    logger.info("① 종목 풀 선정 시작")
    pool_result = fetch_coin_pool(config)
    if pool_result is None:
        # API 실패 → 이전 풀 유지
        try:
            with open(ACTIVE_COINS_PATH, "r") as f:
                old_data = json.load(f)
            coins = old_data.get("coins", [])
            logger.warning(f"API 실패 — 이전 종목 풀 사용 ({len(coins)}개)")
            send_telegram(config, "🚨 [MAGI] 마켓센서 API 실패 — 이전 종목 풀 유지")
        except FileNotFoundError:
            logger.critical("이전 종목 풀도 없음 — 종료")
            send_telegram(config, "🚨 [MAGI] 마켓센서 심각 오류 — 종목 풀 없음")
            sys.exit(1)
        pool_added, pool_removed = [], []
    else:
        coins, all_candidates = pool_result
        # 풀 변동 감지 (제거 사유 구분: 거래대금미달 vs 순위탈락)
        pool_added, pool_removed = detect_pool_changes(coins, ACTIVE_COINS_PATH, all_candidates)
        # 개별 변동 로깅
        for c in pool_added:
            logger.info(f"➕ 종목 추가: {c['symbol']} (${c['volume_24h']/1_000_000:.0f}M)")
        for c in pool_removed:
            reason = c.get("remove_reason", "거래대금미달")
            reason_kr = "상위 20 순위 밖" if reason == "순위탈락" else "거래대금 기준 미달"
            logger.info(f"➖ 종목 제거: {c['symbol']} (${c['volume_24h']/1_000_000:.0f}M → {reason_kr})")
        # 저장
        save_coin_pool(coins)

    logger.info(f"① 종목 풀 완료: {len(coins)}개 코인")

    # ② 기술 분석 (1D 캔들 → EMA/RSI/ADX)
    logger.info("② 기술 분석 시작")
    coins_analysis = []
    directions = []
    for coin in coins:
        analysis = analyze_coin(coin["symbol"])
        if analysis:
            analysis["symbol"] = coin["symbol"]
            analysis["display"] = coin["display"]
            analysis["funding_rate"] = coin.get("funding_rate", 0)
            coins_analysis.append(analysis)
            icon, direction = judge_direction(analysis)
            directions.append((icon, direction))
        else:
            logger.warning(f"분석 실패: {coin['symbol']}")
            directions.append(("⚪", "중립"))
        time.sleep(0.15)  # rate limit 보호

    logger.info(f"② 기술 분석 완료: {len(coins_analysis)}/{len(coins)}개")

    # ③ 시장 전체 판정
    market_state = judge_market(directions)
    logger.info(f"③ 시장 판정: {market_state}")

    # ④ 펀딩비/OI 스냅샷
    logger.info("④ 펀딩비/OI 스냅샷 수집")
    funding_oi = fetch_funding_oi_snapshot(coins)
    logger.info(f"④ 스냅샷 완료: {len(funding_oi)}개")

    # ⑤ 활성 포지션 + 당일 실현 손익
    logger.info("⑤ 포지션 + 실현 손익 조회")
    positions = fetch_all_positions(config)
    closed_pnl = fetch_today_closed_pnl(config)
    balances = fetch_balances(config)
    logger.info(f"⑤ 포지션 {len(positions)}개, 당일 거래 {len(closed_pnl)}건")

    # ⑥ 전략별 요약
    strat_summary = strategy_summary(coins_analysis, positions, closed_pnl, config)

    # ⑦ 리포트 생성 + 텔레그램 발송
    logger.info("⑦ 리포트 생성 + 텔레그램 발송")
    report = build_report(
        coins, coins_analysis, market_state, directions,
        strat_summary, positions, closed_pnl, balances,
        pool_added, pool_removed, config
    )
    send_telegram(config, report)

    # ⑧ market_snapshot.json 저장
    logger.info("⑧ 스냅샷 저장")
    save_snapshot(
        coins, coins_analysis, market_state, directions,
        strat_summary, positions, closed_pnl, balances,
        funding_oi
    )

    elapsed = time.time() - start_time
    logger.info(f"마켓센서 v2.0 실행 완료 — 소요 {elapsed:.1f}초")
    logger.info("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"마켓센서 크래시: {e}")
        logger.critical(traceback.format_exc())
        # 크래시 알림 시도
        try:
            config = load_config()
            send_telegram(config, f"🚨 [MAGI] 마켓센서 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
