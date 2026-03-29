#!/usr/bin/env python3
"""
MAGI 공통 모듈 — magi_common.py
4개 엔진이 공유하는 코어 기능:
  - 바이비트 API (주문, 포지션, 잔고)
  - 종목 충돌 방지 (바이비트 API 직접 조회)
  - 포지션 관리 (TP1/TP2 분할 청산, SL 이동, 시간손절)
  - active_coins.json 로드 + 만료 체크
  - heartbeat 갱신
  - trade_events.json 기록
  - 텔레그램 알림
  - 기술 지표 계산 (EMA, RSI, ADX, BB)
"""

import json
import os
import time
import hmac
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────
BASE_DIR = Path.home() / "magi"
CONFIG_PATH = BASE_DIR / "config.json"
ACTIVE_COINS_PATH = BASE_DIR / "active_coins.json"
TRADE_EVENTS_PATH = BASE_DIR / "trade_events.json"
POSITION_ENTRIES_PATH = BASE_DIR / "position_entries.json"
KST = timezone(timedelta(hours=9))
BYBIT_BASE = "https://api.bybit.com"


# ──────────────────────────────────────────────
# 설정 로드
# ──────────────────────────────────────────────
def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


# ──────────────────────────────────────────────
# 로거 생성
# ──────────────────────────────────────────────
def create_logger(name, log_filename):
    log_path = BASE_DIR / "logs" / log_filename
    os.makedirs(log_path.parent, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(sh)
    return logger


# ──────────────────────────────────────────────
# HTTP 유틸
# ──────────────────────────────────────────────
def http_get(url, headers=None, timeout=15):
    for attempt in range(3):
        try:
            req = Request(url, headers=headers or {})
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError, TimeoutError) as e:
            if attempt < 2:
                time.sleep(5)
    return None


def http_post(url, payload, headers=None, timeout=15):
    for attempt in range(3):
        try:
            data = json.dumps(payload, separators=(",", ":")).encode()
            req = Request(url, data=data, headers=headers or {}, method="POST")
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except (URLError, HTTPError, TimeoutError):
            if attempt < 2:
                time.sleep(5)
    return None


# ──────────────────────────────────────────────
# 바이비트 API
# ──────────────────────────────────────────────
def _sign(api_key, api_secret, params_str, timestamp):
    recv_window = "5000"
    sign_payload = f"{timestamp}{api_key}{recv_window}{params_str}"
    return hmac.new(api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()


def _auth_headers(api_key, api_secret, params_str):
    timestamp = str(int(time.time() * 1000))
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": _sign(api_key, api_secret, params_str, timestamp),
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": "5000",
        "Content-Type": "application/json",
    }


def bybit_public_get(endpoint, params=None):
    url = f"{BYBIT_BASE}{endpoint}"
    if params:
        url += "?" + urlencode(params)
    data = http_get(url)
    if data and data.get("retCode") == 0:
        return data.get("result", {})
    return None


def bybit_private_get(endpoint, params, api_key, api_secret):
    query_string = urlencode(sorted(params.items()))
    headers = _auth_headers(api_key, api_secret, query_string)
    url = f"{BYBIT_BASE}{endpoint}?{query_string}"
    data = http_get(url, headers=headers)
    if data and data.get("retCode") == 0:
        return data.get("result", {})
    return None


def bybit_private_post(endpoint, payload, api_key, api_secret):
    payload_str = json.dumps(payload, separators=(",", ":"))
    headers = _auth_headers(api_key, api_secret, payload_str)
    url = f"{BYBIT_BASE}{endpoint}"
    data = http_post(url, payload, headers=headers)
    if data and data.get("retCode") == 0:
        return data.get("result", {})
    if data:
        logging.getLogger().error(f"바이비트 POST 에러: {data.get('retMsg')} — {endpoint}")
    return None


# ──────────────────────────────────────────────
# 종목 풀 로드 + 만료 체크
# ──────────────────────────────────────────────
def load_active_coins(logger):
    """active_coins.json 로드 + 만료 체크. 반환: (coins, staleness_status)"""
    try:
        with open(ACTIVE_COINS_PATH, "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"active_coins.json 로드 실패: {e}")
        return None, "error"

    updated_at = data.get("updated_at", "")
    try:
        updated_dt = datetime.fromisoformat(updated_at)
        age_hours = (datetime.now(KST) - updated_dt).total_seconds() / 3600
    except (ValueError, TypeError):
        age_hours = 999

    if age_hours >= 24:
        logger.error(f"종목 풀 24시간 만료! ({age_hours:.1f}h) — 신규 진입 중단")
        return data.get("coins", []), "expired"
    elif age_hours >= 12:
        logger.warning(f"종목 풀 12시간 경과 ({age_hours:.1f}h) — 경고")
        return data.get("coins", []), "warning"
    else:
        return data.get("coins", []), "ok"


# ──────────────────────────────────────────────
# 포지션 관리
# ──────────────────────────────────────────────
def get_all_positions(config):
    """4개 서브계정 전체 활성 포지션 조회 (종목 충돌 방지용)"""
    all_positions = []
    for strat_key, acct in config["bybit"]["accounts"].items():
        if "YOUR_" in acct["api_key"]:
            continue
        result = bybit_private_get("/v5/position/list",
            {"category": "linear", "settleCoin": "USDT"},
            acct["api_key"], acct["api_secret"])
        if result:
            for pos in result.get("list", []):
                if float(pos.get("size", 0)) > 0:
                    all_positions.append({
                        "strategy": strat_key,
                        "symbol": pos["symbol"],
                        "side": pos["side"],
                        "size": float(pos["size"]),
                        "avgPrice": float(pos.get("avgPrice", 0)),
                        "markPrice": float(pos.get("markPrice", 0)),
                        "unrealisedPnl": float(pos.get("unrealisedPnl", 0)),
                        "leverage": float(pos.get("leverage", 1)),
                        "createdTime": pos.get("createdTime", ""),
                    })
    return all_positions


def get_my_positions(api_key, api_secret):
    """내 계정(전략) 활성 포지션만"""
    result = bybit_private_get("/v5/position/list",
        {"category": "linear", "settleCoin": "USDT"},
        api_key, api_secret)
    positions = []
    if result:
        for pos in result.get("list", []):
            if float(pos.get("size", 0)) > 0:
                positions.append({
                    "symbol": pos["symbol"],
                    "side": pos["side"],
                    "size": float(pos["size"]),
                    "avgPrice": float(pos.get("avgPrice", 0)),
                    "markPrice": float(pos.get("markPrice", 0)),
                    "unrealisedPnl": float(pos.get("unrealisedPnl", 0)),
                    "leverage": float(pos.get("leverage", 1)),
                    "createdTime": pos.get("createdTime", ""),
                    "takeProfit": pos.get("takeProfit", ""),
                    "stopLoss": pos.get("stopLoss", ""),
                })
    return positions


def check_collision(symbol, config):
    """종목 충돌 방지: 4개 계정 중 누구든 해당 코인 포지션 있으면 True"""
    all_pos = get_all_positions(config)
    return any(p["symbol"] == symbol for p in all_pos)


def count_global_positions(config):
    """전체 동시 포지션 수"""
    return len(get_all_positions(config))


def get_wallet_balance(api_key, api_secret):
    """USDT 잔고 조회. API 실패 시 None 반환 (재시도 루프 호환)"""
    result = bybit_private_get("/v5/account/wallet-balance",
        {"accountType": "UNIFIED"}, api_key, api_secret)
    if result and result.get("list"):
        for acct in result["list"]:
            equity = float(acct.get("totalEquity", 0))
            if equity > 0:
                return equity
    return None


# ──────────────────────────────────────────────
# 주문 실행
# ──────────────────────────────────────────────
def set_leverage(symbol, leverage, api_key, api_secret):
    """레버리지 설정"""
    return bybit_private_post("/v5/position/set-leverage", {
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": str(leverage),
        "sellLeverage": str(leverage),
    }, api_key, api_secret)


def place_market_order(symbol, side, qty, api_key, api_secret,
                       take_profit=None, stop_loss=None):
    """시장가 주문 (TP/SL 동시 설정)"""
    payload = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "GTC",
    }
    if take_profit:
        payload["takeProfit"] = str(take_profit)
    if stop_loss:
        payload["stopLoss"] = str(stop_loss)
    return bybit_private_post("/v5/order/create", payload, api_key, api_secret)


def close_position(symbol, side, qty, api_key, api_secret):
    """포지션 청산 (시장가). side는 청산 방향 (롱 청산=Sell, 숏 청산=Buy)"""
    return place_market_order(symbol, side, qty, api_key, api_secret)


def modify_position_sl(symbol, stop_loss, api_key, api_secret):
    """SL 수정 (TP1 도달 후 본절로 이동)"""
    return bybit_private_post("/v5/position/trading-stop", {
        "category": "linear",
        "symbol": symbol,
        "stopLoss": str(stop_loss),
    }, api_key, api_secret)


def cancel_all_orders(symbol, api_key, api_secret):
    """해당 심볼의 모든 조건부 주문 취소"""
    return bybit_private_post("/v5/order/cancel-all", {
        "category": "linear",
        "symbol": symbol,
    }, api_key, api_secret)


# ──────────────────────────────────────────────
# 진입 수량 계산
# ──────────────────────────────────────────────
def calc_entry_qty(balance, entry_pct, leverage, price, symbol_info=None):
    """진입 수량 계산: 잔고 × 5% × 레버리지 / 가격"""
    notional = balance * entry_pct * leverage
    qty = notional / price
    # 소수점 처리 (기본 3자리, 심볼별로 다를 수 있음)
    if price > 1000:
        qty = round(qty, 3)
    elif price > 10:
        qty = round(qty, 2)
    elif price > 1:
        qty = round(qty, 1)
    else:
        qty = round(qty, 0)
    return max(qty, 0)


# ──────────────────────────────────────────────
# TP/SL 가격 계산
# ──────────────────────────────────────────────
def calc_tp_sl(entry_price, side, tp1_pct, tp2_pct, sl_pct):
    """TP1, TP2, SL 가격 계산. side='Buy'(롱) or 'Sell'(숏)"""
    if side == "Buy":  # 롱
        tp1 = entry_price * (1 + tp1_pct / 100)
        tp2 = entry_price * (1 + tp2_pct / 100)
        sl = entry_price * (1 - sl_pct / 100)
    else:  # 숏
        tp1 = entry_price * (1 - tp1_pct / 100)
        tp2 = entry_price * (1 - tp2_pct / 100)
        sl = entry_price * (1 + sl_pct / 100)
    return round(tp1, 4), round(tp2, 4), round(sl, 4)


# ──────────────────────────────────────────────
# 포지션 관리 로직 (TP1/TP2/SL/시간손절)
# ──────────────────────────────────────────────
def manage_position(pos, strategy, api_key, api_secret, logger,
                    tp1_pct=2.0, tp2_pct=4.0, sl_pct=2.0,
                    time_stop_hours=None):
    """
    기존 포지션 관리. 반환: 'closed' | 'tp1_hit' | 'holding'
    - TP1 도달: 50% 청산 + SL을 진입가로 이동 (본절)
    - TP2 도달: 나머지 50% 청산
    - SL: 바이비트 서버측 처리 (이미 설정됨)
    - 시간손절: time_stop_hours 경과 시 전량 시장가 청산
    """
    symbol = pos["symbol"]
    side = pos["side"]  # Buy(롱) or Sell(숏)
    entry_price = pos["avgPrice"]
    mark_price = pos["markPrice"]
    size = pos["size"]
    created_time = pos.get("createdTime", "")

    # 수익률 계산
    if side == "Buy":
        pnl_pct = (mark_price - entry_price) / entry_price * 100
    else:
        pnl_pct = (entry_price - mark_price) / entry_price * 100

    # 시간손절 체크 (로컬 추적 기반 — createdTime 버그 우회)
    if time_stop_hours:
        try:
            elapsed_hours = _get_real_elapsed_hours(strategy, symbol, side, entry_price)
            if elapsed_hours >= time_stop_hours and pnl_pct < tp1_pct:
                close_side = "Sell" if side == "Buy" else "Buy"
                result = close_position(symbol, close_side, size, api_key, api_secret)
                if result:
                    logger.info(f"⏰ 시간손절: {symbol} {side} {elapsed_hours:.1f}h 경과, PnL {pnl_pct:+.2f}%")
                    write_trade_event(symbol, strategy, "시간손절",
                                      tp1_reached=pnl_pct >= tp1_pct,
                                      tp2_reached=False)
                    return "closed"
        except (ValueError, TypeError):
            pass

    # TP1 체크 (50% 청산)
    if pnl_pct >= tp1_pct and size > 0:
        half_qty = round(size / 2, 6)
        if half_qty > 0:
            close_side = "Sell" if side == "Buy" else "Buy"
            result = close_position(symbol, close_side, half_qty, api_key, api_secret)
            if result:
                logger.info(f"🎯 TP1 도달: {symbol} {side} {pnl_pct:+.2f}% → 50% 청산")
                # SL을 진입가로 이동 (본절 보장) — 3회 재시도 + 실패 시 전량 청산 fallback
                sl_moved = False
                for attempt in range(1, 4):
                    sl_result = modify_position_sl(symbol, entry_price, api_key, api_secret)
                    if sl_result is not None:
                        logger.info(f"🔒 SL → 진입가 이동 성공 (본절): {symbol} @ {entry_price} (시도 {attempt}/3)")
                        sl_moved = True
                        break
                    else:
                        logger.warning(f"⚠️ SL 이동 실패 (시도 {attempt}/3): {symbol}")
                        time.sleep(1)
                if not sl_moved:
                    # SL 이동 3회 전부 실패 → 남은 포지션 전량 시장가 청산 (safety fallback)
                    logger.error(f"🚨 SL 이동 3회 실패! 남은 포지션 전량 청산: {symbol}")
                    remaining = get_my_positions(api_key, api_secret)
                    for rp in remaining:
                        if rp["symbol"] == symbol:
                            fallback_side = "Sell" if rp["side"] == "Buy" else "Buy"
                            close_position(symbol, fallback_side, rp["size"], api_key, api_secret)
                            logger.info(f"🛡️ Fallback 청산 완료: {symbol} {rp['size']}")
                            break
                write_trade_event(symbol, strategy, "TP1",
                                  tp1_reached=True, tp2_reached=False)
                return "tp1_hit"

    # TP2 체크 (나머지 전량 청산)
    if pnl_pct >= tp2_pct and size > 0:
        close_side = "Sell" if side == "Buy" else "Buy"
        result = close_position(symbol, close_side, size, api_key, api_secret)
        if result:
            logger.info(f"🏆 TP2 도달: {symbol} {side} {pnl_pct:+.2f}% → 전량 청산")
            write_trade_event(symbol, strategy, "TP2",
                              tp1_reached=True, tp2_reached=True)
            return "closed"

    return "holding"


# ──────────────────────────────────────────────
# trade_events.json 기록

# ── 포지션 진입시간 추적 (createdTime 버그 우회) ──────
def _load_position_entries():
    """로컬 포지션 진입시간 캐시 로드"""
    if POSITION_ENTRIES_PATH.exists():
        try:
            return json.loads(POSITION_ENTRIES_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}

def _save_position_entries(entries):
    """로컬 포지션 진입시간 캐시 저장"""
    tmp = POSITION_ENTRIES_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(POSITION_ENTRIES_PATH)

def _get_real_elapsed_hours(strategy, symbol, side, avg_price):
    """실제 경과시간 반환 — avgPrice+side가 바뀌면 새 포지션으로 판단
    avgPrice는 숫자형으로 비교 (오차범위 0.01% 이내면 동일 포지션)"""
    entries = _load_position_entries()
    key = f"{strategy}_{symbol}"
    existing = entries.get(key, {})
    # 숫자형 비교: 0.01% 오차범위 이내면 동일 포지션
    try:
        old_price = float(existing.get("avgPrice", 0))
    except (ValueError, TypeError):
        old_price = 0
    price_match = (abs(avg_price - old_price) / old_price < 0.0001) if old_price > 0 else False
    if price_match and existing.get("side") == side:
        entry_ts = existing.get("entry_ts", time.time())
    else:
        # 새 포지션 → 현재 시각 기록
        entry_ts = time.time()
        entries[key] = {
            "avgPrice": avg_price,  # 숫자형으로 저장
            "side": side,
            "entry_ts": entry_ts,
        }
        _save_position_entries(entries)
    return (time.time() - entry_ts) / 3600


# ──────────────────────────────────────────────
def write_trade_event(symbol, strategy, reason, tp1_reached=False,
                      tp2_reached=False, extra=None):
    """trade_events.json에 청산이유 힌트 기록 (atomic write)"""
    events = []
    try:
        with open(TRADE_EVENTS_PATH, "r") as f:
            data = json.load(f)
            events = data.get("events", [])
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    event = {
        "symbol": symbol,
        "strategy": strategy,
        "reason": reason,
        "group_id": f"pos_{symbol}_{strategy}_{datetime.now(KST).strftime('%Y%m%d')}",
        "tp1_reached": tp1_reached,
        "tp2_reached": tp2_reached,
        "timestamp": datetime.now(KST).isoformat(),
    }
    if extra:
        event.update(extra)
    events.append(event)

    # atomic write
    tmp_path = BASE_DIR / "trade_events.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"events": events}, f, ensure_ascii=False, indent=2)
    os.rename(tmp_path, TRADE_EVENTS_PATH)


# ──────────────────────────────────────────────
# Heartbeat
# ──────────────────────────────────────────────
def update_heartbeat(strategy):
    """heartbeat 파일 갱신 (watchdog용)"""
    hb_path = BASE_DIR / f"heartbeat_{strategy}.json"
    data = {
        "strategy": strategy,
        "timestamp": datetime.now(KST).isoformat(),
        "epoch": time.time(),
    }
    tmp_path = BASE_DIR / f"heartbeat_{strategy}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f)
    os.rename(tmp_path, hb_path)


# ──────────────────────────────────────────────
# 텔레그램
# ──────────────────────────────────────────────
def send_telegram(config, message):
    token = config["telegram"]["bot_token"]
    chat_id = config["telegram"]["chat_id"]
    if "YOUR_" in token:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        data = json.dumps(payload).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode()).get("ok", False)
    except Exception:
        return False


# ──────────────────────────────────────────────
# 기술 지표
# ──────────────────────────────────────────────
def fetch_kline(symbol, interval="240", limit=200):
    """바이비트 캔들 데이터. interval: '60'=1H, '240'=4H, 'D'=1D"""
    result = bybit_public_get("/v5/market/kline", {
        "category": "linear", "symbol": symbol,
        "interval": interval, "limit": str(limit),
    })
    if not result:
        return None
    raw = result.get("list", [])
    candles = []
    for c in reversed(raw):
        candles.append({
            "ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
            "low": float(c[3]), "close": float(c[4]), "volume": float(c[5]),
        })
    return candles


def calc_ema(closes, period):
    if len(closes) < period:
        return []
    mult = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for price in closes[period:]:
        ema.append((price - ema[-1]) * mult + ema[-1])
    return ema


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
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
    if len(candles) < period * 2 + 1:
        return None
    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        ph, pl = candles[i-1]["high"], candles[i-1]["low"]
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
        pdm_list.append(max(h - ph, 0) if (h - ph) > (pl - l) else 0)
        ndm_list.append(max(pl - l, 0) if (pl - l) > (h - ph) else 0)
    atr = sum(tr_list[:period])
    pdm_s = sum(pdm_list[:period])
    ndm_s = sum(ndm_list[:period])
    dx_list = []
    for i in range(period, len(tr_list)):
        atr = atr - atr / period + tr_list[i]
        pdm_s = pdm_s - pdm_s / period + pdm_list[i]
        ndm_s = ndm_s - ndm_s / period + ndm_list[i]
        pdi = (pdm_s / atr * 100) if atr else 0
        ndi = (ndm_s / atr * 100) if atr else 0
        di_sum = pdi + ndi
        dx_list.append(abs(pdi - ndi) / di_sum * 100 if di_sum else 0)
    if len(dx_list) < period:
        return None
    adx = sum(dx_list[:period]) / period
    for i in range(period, len(dx_list)):
        adx = (adx * (period - 1) + dx_list[i]) / period
    return adx


def calc_bollinger(closes, period=20, std_mult=2):
    """볼린저밴드. 반환: (upper, middle, lower, bandwidth)"""
    if len(closes) < period:
        return None, None, None, None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    std = variance ** 0.5
    upper = middle + std_mult * std
    lower = middle - std_mult * std
    bandwidth = (upper - lower) / middle if middle else 0
    return upper, middle, lower, bandwidth


def calc_bb_bandwidth_history(closes, period=20, std_mult=2, lookback=120):
    """최근 lookback 봉의 BB bandwidth 히스토리"""
    bandwidths = []
    for i in range(period, min(len(closes), lookback + period)):
        window = closes[i-period:i]
        mid = sum(window) / period
        var = sum((x - mid) ** 2 for x in window) / period
        std = var ** 0.5
        bw = ((mid + std_mult * std) - (mid - std_mult * std)) / mid if mid else 0
        bandwidths.append(bw)
    return bandwidths


# ─────────────────────────────────────────────
# closed-pnl 기반 청산 알림 (별이 + 성단 설계)
# ─────────────────────────────────────────────
def fetch_recent_closed_pnl(api_key, api_secret, limit=20):
    """최근 청산 내역 조회 (알림용, 최신 limit건)"""
    params = {
        "category": "linear",
        "limit": str(limit),
    }
    result = bybit_private_get("/v5/position/closed-pnl", params, api_key, api_secret)
    if result and isinstance(result, dict):
        if "retCode" in result and result["retCode"] == 0:
            return result.get("result", {}).get("list", [])
        elif "list" in result:
            return result.get("list", [])
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
            f"{strategy_icon} [{strategy_name}] {exit_type}\n"
            f"  {symbol} {side_kr} ×{closed_size}\n"
            f"  진입: {entry_price} → 청산: {exit_price}\n"
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


# ──────────────────────────────────────────────
# Phase 3: 드로우다운 보호 (엔진 읽기 전용)
# ──────────────────────────────────────────────
def is_dd_paused(strategy, logger):
    """DD 보호 상태 확인 — 전략별 + 통합 둘 다 체크.
    update_all.py가 쓰기, 엔진은 읽기만."""
    # 전략별 DD
    dd_file = BASE_DIR / f"dd_protection_{strategy}.json"
    if dd_file.exists():
        try:
            data = json.loads(dd_file.read_text(encoding="utf-8"))
            if data.get("paused"):
                logger.warning(f"🚨 DD 보호 발동 중 — 신규 진입 중단 (DD {data.get('dd_pct', '?')}%)")
                return True
        except (json.JSONDecodeError, ValueError):
            pass
    # 통합 DD
    dd_total = BASE_DIR / "dd_protection_total.json"
    if dd_total.exists():
        try:
            data = json.loads(dd_total.read_text(encoding="utf-8"))
            if data.get("paused"):
                logger.warning(f"🚨 통합 DD 보호 발동 중 — 전 전략 신규 진입 중단 (DD {data.get('dd_pct', '?')}%)")
                return True
        except (json.JSONDecodeError, ValueError):
            pass
    return False


# ──────────────────────────────────────────────
# Phase 3: 연속 손실 방어
# ──────────────────────────────────────────────
def check_loss_streak(strategy, logger):
    """연속 손실 중단 상태 확인. 반환: True(중단 중) / False(정상)"""
    ls_file = BASE_DIR / f"loss_streak_{strategy}.json"
    if not ls_file.exists():
        return False
    try:
        data = json.loads(ls_file.read_text(encoding="utf-8"))
        if not data.get("paused"):
            return False
        resume_at = data.get("resume_at", "")
        if resume_at:
            resume_dt = datetime.fromisoformat(resume_at)
            if datetime.now(KST) >= resume_dt:
                # 중단 시간 만료 → 자동 재개
                data["paused"] = False
                _atomic_write(ls_file, data)
                logger.info(f"✅ 연속손실 중단 해제 — 신규 진입 재개")
                return False
        logger.warning(f"🚨 연속손실 방어 중 — {data.get('count', 0)}회 연손, "
                      f"재개: {resume_at}")
        return True
    except (json.JSONDecodeError, ValueError):
        return False


def update_loss_streak(strategy, is_loss, reason, config, logger):
    """청산 시 연속 손실 카운터 업데이트.
    is_loss: True(손절/시간손절), False(익절/본절)
    reason: 'SL', '시간손절', 'TP1', 'TP2' 등"""
    ls_file = BASE_DIR / f"loss_streak_{strategy}.json"
    try:
        data = json.loads(ls_file.read_text(encoding="utf-8")) if ls_file.exists() else {}
    except (json.JSONDecodeError, ValueError):
        data = {}

    # 손절 정의: SL + 시간손절만 카운트 (수동/본절 제외)
    loss_reasons = {"SL", "시간손절"}
    if is_loss and reason in loss_reasons:
        count = data.get("count", 0) + 1
        data["count"] = count
        if count >= 3:
            # 3회 연속 손절 → 2시간 중단
            resume_at = datetime.now(KST) + timedelta(hours=2)
            data["paused"] = True
            data["resume_at"] = resume_at.isoformat()
            logger.warning(f"🚨 연속손실 {count}회 — 2시간 진입 중단 (재개: {resume_at.strftime('%H:%M')})")
            strat_info = config.get("strategies", {}).get(strategy, {})
            send_telegram(config,
                f"🚨 [{strat_info.get('name', strategy)}] {count}회 연속 손절 — 2시간 진입 중단\n"
                f"재개 예정: {resume_at.strftime('%H:%M KST')}")
    else:
        # 승리 1회 → 카운트 리셋
        if data.get("count", 0) > 0:
            logger.info(f"✅ 연속손실 카운트 리셋 (이전: {data.get('count', 0)}회)")
        data["count"] = 0
        data["paused"] = False

    _atomic_write(ls_file, data)


# ──────────────────────────────────────────────
# Phase 3: 저유동성 필터링 (스프레드 + 블랙리스트)
# ──────────────────────────────────────────────
# 시가총액 Tier 분류 (종목 풀 순위 기반)
TIER1_SYMBOLS = {"BTCUSDT", "ETHUSDT"}
TIER2_SYMBOLS = {"SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT",
                 "TRXUSDT", "AVAXUSDT", "LINKUSDT"}
SPREAD_LIMITS = {1: 0.05, 2: 0.15, 3: 0.30}  # Tier별 스프레드 한계(%)


def _get_tier(symbol):
    """종목 Tier 분류"""
    if symbol in TIER1_SYMBOLS:
        return 1
    if symbol in TIER2_SYMBOLS:
        return 2
    return 3


def check_spread(symbol, logger):
    """진입 전 스프레드 체크. Fail-closed: API 실패 시 진입 차단 (소니 설계 판단).
    반환: True(통과) / False(차단)"""
    # 1차 시도
    result = bybit_public_get("/v5/market/orderbook", {
        "category": "linear", "symbol": symbol, "limit": "1"
    })
    # 실패 시 1회 재시도 (3초 대기)
    if not result:
        time.sleep(3)
        result = bybit_public_get("/v5/market/orderbook", {
            "category": "linear", "symbol": symbol, "limit": "1"
        })
    if not result:
        logger.warning(f"  🚫 SPREAD_CHECK_FAILED: {symbol} — 오더북 2회 조회 실패, 진입 스킵")
        return False  # Fail-closed: API 실패 → 진입 차단

    asks = result.get("a", [])
    bids = result.get("b", [])
    if not asks or not bids:
        logger.warning(f"  🚫 SPREAD_CHECK_FAILED: {symbol} — 오더북 데이터 부족, 진입 스킵")
        return False

    ask = float(asks[0][0])
    bid = float(bids[0][0])
    mid = (ask + bid) / 2
    if mid <= 0:
        return False

    spread_pct = (ask - bid) / mid * 100
    tier = _get_tier(symbol)
    limit = SPREAD_LIMITS[tier]

    if spread_pct > limit:
        logger.info(f"  🚫 {symbol} 스프레드 {spread_pct:.3f}% > 한계 {limit}% (Tier {tier}) — 진입 스킵")
        return False

    return True


def check_blacklist(symbol, config, logger):
    """블랙리스트 종목 체크. 반환: True(통과) / False(블랙리스트)"""
    blacklist = config.get("blacklist", [])
    if symbol in blacklist:
        logger.info(f"  ⛔ {symbol} 블랙리스트 — 진입 스킵")
        return False
    return True


# ──────────────────────────────────────────────
# 공통 유틸: atomic write
# ──────────────────────────────────────────────
def _atomic_write(filepath, data):
    """JSON 파일 원자적 쓰기 (tmp → rename)"""
    filepath = Path(filepath)
    tmp = filepath.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(filepath)

