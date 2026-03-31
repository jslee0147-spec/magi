#!/usr/bin/env python3
"""
MAGI v3.0 공통 모듈 — magi_v3_common.py
히노카미(롱) + 나이트(숏) 두 엔진이 공유하는 코어 기능:
  - 바이낸스 API (선물 주문, 포지션, 잔고)
  - 기술 지표 (SuperTrend, RSI, ATR, EMA, 거래량)
  - 노션 거래 로그 (row 생성/업데이트)
  - 텔레그램 알림
  - 동적 포지션 사이징
  - 하트비트 / 로그
"""

import json
import os
import time
import hmac
import hashlib
import logging
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
KST = timezone(timedelta(hours=9))
BINANCE_FAPI = "https://fapi.binance.com"

# ──────────────────────────────────────────────
# 설정 로드
# ──────────────────────────────────────────────
def load_config():
    with open(BASE_DIR / "config_v3.json", "r") as f:
        return json.load(f)

# ──────────────────────────────────────────────
# 로거 생성
# ──────────────────────────────────────────────
def create_logger(name, filename):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / filename, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
    # 콘솔 출력
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
    logger.addHandler(ch)
    return logger

# ──────────────────────────────────────────────
# 바이낸스 API 헬퍼
# ──────────────────────────────────────────────
class BinanceClient:
    """바이낸스 USDT-M 선물 API 클라이언트"""

    def __init__(self, api_key, api_secret, logger=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = logger or logging.getLogger("binance")
        self.recv_window = 5000

    def _sign(self, params):
        """HMAC SHA256 서명"""
        query = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return query + f"&signature={signature}"

    def _request(self, method, endpoint, params=None, signed=False):
        """API 요청"""
        if params is None:
            params = {}
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = self.recv_window
            query = self._sign(params)
        else:
            query = urlencode(params)

        url = f"{BINANCE_FAPI}{endpoint}"
        if method == "GET" and query:
            url += f"?{query}"

        headers = {"X-MBX-APIKEY": self.api_key}

        for attempt in range(3):
            try:
                if method == "POST":
                    data = query.encode() if query else None
                    req = Request(url, data=data, headers=headers, method="POST")
                    req.add_header("Content-Type", "application/x-www-form-urlencoded")
                else:
                    req = Request(url, headers=headers, method=method)

                with urlopen(req, timeout=15) as resp:
                    return json.loads(resp.read().decode())
            except (URLError, HTTPError, TimeoutError) as e:
                self.logger.warning(f"바이낸스 API 재시도 ({attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(1)
        return None

    # ── 공개 API ──
    def get_server_time(self):
        return self._request("GET", "/fapi/v1/time")

    def get_ticker_price(self, symbol):
        return self._request("GET", "/fapi/v1/ticker/price", {"symbol": symbol})

    def get_klines(self, symbol, interval, limit=100):
        """캔들 데이터 조회"""
        data = self._request("GET", "/fapi/v1/klines", {
            "symbol": symbol, "interval": interval, "limit": limit
        })
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df

    def get_exchange_info(self):
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def get_all_tickers(self):
        return self._request("GET", "/fapi/v1/ticker/24hr")

    # ── 계정 API (서명 필요) ──
    def get_account(self):
        return self._request("GET", "/fapi/v2/account", signed=True)

    def get_balance(self):
        return self._request("GET", "/fapi/v2/balance", signed=True)

    def get_positions(self):
        """현재 열린 포지션 조회"""
        account = self.get_account()
        if not account:
            return []
        positions = account.get("positions", [])
        return [p for p in positions if float(p.get("positionAmt", 0)) != 0]

    def get_equity(self):
        """총 자본 (wallet balance + unrealized PnL)"""
        account = self.get_account()
        if not account:
            return 0
        return float(account.get("totalMarginBalance", 0))

    # ── 주문 API ──
    def set_leverage(self, symbol, leverage):
        return self._request("POST", "/fapi/v1/leverage", {
            "symbol": symbol, "leverage": leverage
        }, signed=True)

    def set_margin_type(self, symbol, margin_type="CROSSED"):
        """마진 타입 설정 (CROSSED/ISOLATED)"""
        result = self._request("POST", "/fapi/v1/marginType", {
            "symbol": symbol, "marginType": margin_type
        }, signed=True)
        # -4046 "No need to change margin type" 에러는 정상 (이미 설정됨)
        if result and result.get("code") == -4046:
            return result
        if result and result.get("code") and result["code"] != 200:
            self.logger.warning(f"마진 타입 설정 실패: {symbol} → {result}")
        return result

    def place_market_order(self, symbol, side, quantity):
        """시장가 주문"""
        params = {
            "symbol": symbol,
            "side": side,  # "BUY" or "SELL"
            "type": "MARKET",
            "quantity": quantity,
        }
        result = self._request("POST", "/fapi/v1/order", params, signed=True)
        if result and result.get("orderId"):
            self.logger.info(f"주문 체결: {symbol} {side} {quantity} → orderId={result['orderId']}")
        else:
            self.logger.error(f"주문 실패: {symbol} {side} {quantity} → {result}")
        return result

    def close_position(self, symbol, side, quantity):
        """포지션 청산 (반대 방향 주문) — 3회 재시도 + 긴급 알림"""
        close_side = "SELL" if side == "BUY" else "BUY"
        for attempt in range(3):
            result = self.place_market_order(symbol, close_side, quantity)
            if result and result.get("orderId"):
                return result
            self.logger.warning(f"청산 재시도 ({attempt+1}/3): {symbol}")
            time.sleep(2)
        self.logger.critical(f"🚨 청산 3회 실패! {symbol} {close_side} {quantity} — 포지션 방치 위험!")
        return None

    def place_stop_market(self, symbol, side, stop_price, quantity):
        """서버 Stop Market 주문 (Hard Stop 안전망)
        봉 사이 급락/급등 시에도 서버가 자동 청산"""
        # 숏 포지션 SL = BUY STOP, 롱 포지션 SL = SELL STOP
        params = {
            "symbol": symbol,
            "side": side,  # 롱 SL: "SELL", 숏 SL: "BUY"
            "type": "STOP_MARKET",
            "stopPrice": f"{stop_price:.8f}".rstrip('0').rstrip('.'),
            "closePosition": "true",  # 전량 청산
            "timeInForce": "GTE_GTC",
        }
        result = self._request("POST", "/fapi/v1/order", params, signed=True)
        if result and result.get("orderId"):
            self.logger.info(f"서버 Stop 설정: {symbol} {side} @ ${stop_price:.2f} → orderId={result['orderId']}")
        else:
            self.logger.error(f"서버 Stop 실패: {symbol} {side} @ ${stop_price:.2f} → {result}")
        return result

    def cancel_all_orders(self, symbol):
        """해당 종목의 미체결 주문 전부 취소 (SL 갱신 시 기존 SL 취소용)"""
        return self._request("DELETE", "/fapi/v1/allOpenOrders", {
            "symbol": symbol
        }, signed=True)

    def get_order(self, symbol, order_id):
        return self._request("GET", "/fapi/v1/order", {
            "symbol": symbol, "orderId": order_id
        }, signed=True)

    # ── 종목 정보 ──
    def get_symbol_info(self, symbol):
        """종목 수량 정밀도 등"""
        info = self.get_exchange_info()
        if not info:
            return None
        for s in info.get("symbols", []):
            if s["symbol"] == symbol:
                return s
        return None

    def calc_quantity(self, symbol, usd_amount, price, leverage):
        """주문 수량 계산 (정밀도 맞춤)"""
        info = self.get_symbol_info(symbol)
        if not info:
            return 0

        # 수량 정밀도
        qty_precision = 3
        min_qty = 0.001
        for f in info.get("filters", []):
            if f["filterType"] == "LOT_SIZE":
                min_qty = float(f["minQty"])
                step = float(f["stepSize"])
                qty_precision = max(0, len(str(step).rstrip('0').split('.')[-1])) if '.' in str(step) else 0

        # 계산: (달러 × 레버리지) / 가격
        notional = usd_amount * leverage
        raw_qty = notional / price

        # 정밀도 맞춤
        qty = round(raw_qty, qty_precision)
        if qty < min_qty:
            return 0

        # 최소 주문 금액 확인 (notional filter)
        for f in info.get("filters", []):
            if f["filterType"] == "MIN_NOTIONAL":
                min_notional = float(f.get("notional", 5))
                if qty * price < min_notional:
                    return 0

        return qty


# ──────────────────────────────────────────────
# 기술 지표
# ──────────────────────────────────────────────
def calc_atr(df, period=10):
    """ATR 계산"""
    high = df["high"]
    low = df["low"]
    close = df["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def calc_supertrend(df, period=10, multiplier=3.0):
    """SuperTrend 계산"""
    atr = calc_atr(df, period)
    hl2 = (df["high"] + df["low"]) / 2

    upper = hl2 + (multiplier * atr)
    lower = hl2 - (multiplier * atr)

    supertrend = pd.Series(np.nan, index=df.index)
    direction = pd.Series(1, index=df.index)

    for i in range(1, len(df)):
        if lower.iloc[i] > lower.iloc[i-1] or df["close"].iloc[i-1] < lower.iloc[i-1]:
            pass
        else:
            lower.iloc[i] = lower.iloc[i-1]

        if upper.iloc[i] < upper.iloc[i-1] or df["close"].iloc[i-1] > upper.iloc[i-1]:
            pass
        else:
            upper.iloc[i] = upper.iloc[i-1]

        if pd.isna(supertrend.iloc[i-1]):
            direction.iloc[i] = 1
        elif supertrend.iloc[i-1] == upper.iloc[i-1]:
            direction.iloc[i] = -1 if df["close"].iloc[i] <= upper.iloc[i] else 1
        else:
            direction.iloc[i] = 1 if df["close"].iloc[i] >= lower.iloc[i] else -1

        supertrend.iloc[i] = lower.iloc[i] if direction.iloc[i] == 1 else upper.iloc[i]

    return supertrend, direction, atr


def calc_rsi(df, period=14):
    """RSI 계산"""
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_ema(series, period):
    """EMA 계산"""
    return series.ewm(span=period, adjust=False).mean()


def calc_volume_ratio(df, period=6):
    """거래량 비율 (이동평균 대비)"""
    avg_vol = df["volume"].rolling(window=period).mean()
    return df["volume"] / avg_vol


# ──────────────────────────────────────────────
# 시그널 스코어
# ──────────────────────────────────────────────
def calc_signal_score(rsi, volume_ratio, ema_distance_pct, side="long"):
    """종목 우선순위 스코어 (v1.2: EMA 거리 기반 1D 추세 점수)"""
    # RSI 강도
    if side == "long":
        if rsi >= 70: rsi_score = 1.0
        elif rsi >= 60: rsi_score = 0.8
        elif rsi >= 55: rsi_score = 0.5
        else: rsi_score = 0.0
    else:  # short
        if rsi <= 30: rsi_score = 1.0
        elif rsi <= 35: rsi_score = 0.8
        elif rsi <= 45: rsi_score = 0.5
        else: rsi_score = 0.0

    # 거래량 강도
    if volume_ratio >= 1.5: vol_score = 1.0
    elif volume_ratio >= 1.2: vol_score = 0.7
    elif volume_ratio >= 1.0: vol_score = 0.4
    else: vol_score = 0.0

    # 1D 추세 — EMA 거리(%) 기반 3단계 (v1.2 설계서)
    if ema_distance_pct >= 3.0: trend_score = 1.0
    elif ema_distance_pct >= 1.5: trend_score = 0.7
    else: trend_score = 0.4

    return (rsi_score * 0.4) + (vol_score * 0.3) + (trend_score * 0.3)


# ──────────────────────────────────────────────
# 동적 포지션 사이징
# ──────────────────────────────────────────────
def calc_position_size(equity, atr, entry_price, params):
    """ATR 기반 동적 포지션 사이징"""
    risk_amount = equity * params["MAX_LOSS_TRADE"]
    sl_distance_pct = (atr / entry_price) * params["HARD_STOP_ATR_MULT"]
    denom = sl_distance_pct * params["LEVERAGE"]

    if denom <= 0:
        return 0

    position_usd = risk_amount / denom
    min_pos = params["MIN_POSITION_USD"]
    max_pos = equity * params["MAX_POSITION_PCT"]
    position_usd = max(min_pos, min(position_usd, max_pos))

    if position_usd < min_pos:
        return 0

    return position_usd


# ──────────────────────────────────────────────
# 텔레그램 알림
# ──────────────────────────────────────────────
def send_telegram(config, message):
    """텔레그램 메시지 발송"""
    token = config["telegram"]["bot_token"]
    chat_id = config["telegram"]["chat_id"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "HTML"}).encode()
    headers = {"Content-Type": "application/json"}
    try:
        req = Request(url, data=data, headers=headers, method="POST")
        with urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        return False


# ──────────────────────────────────────────────
# 노션 거래 로그
# ──────────────────────────────────────────────
NOTION_API = "https://api.notion.com/v1"

def notion_request(method, endpoint, token, payload=None):
    """노션 API 호출"""
    url = f"{NOTION_API}{endpoint}"
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
            if attempt < 2:
                time.sleep(2)
    return None


def notion_create_trade_log(config, trade_data):
    """거래 로그 row 생성 (진입 시)"""
    token = config["notion"]["token"]
    db_id = config["notion"]["trade_log_db"]

    properties = {
        "코인": {"title": [{"text": {"content": trade_data["symbol"]}}]},
        "엔진": {"select": {"name": trade_data["engine"]}},
        "방향": {"select": {"name": trade_data["direction"]}},
        "결과": {"select": {"name": "진행중"}},
        "진입가": {"number": trade_data["entry_price"]},
        "진입시각": {"date": {"start": trade_data["entry_time"]}},
        "수량": {"number": trade_data["quantity"]},
        "배팅금액": {"number": trade_data["bet_amount"]},
        "레버리지": {"number": trade_data["leverage"]},
        "ATR": {"number": trade_data["atr"]},
        "RSI": {"number": trade_data["rsi"]},
        "시그널스코어": {"number": trade_data["signal_score"]},
        "1D필터": {"select": {"name": "통과"}},
    }

    if trade_data.get("order_id"):
        properties["orderId"] = {"rich_text": [{"text": {"content": str(trade_data["order_id"])}}]}

    payload = {"parent": {"database_id": db_id}, "properties": properties}
    result = notion_request("POST", "/pages", token, payload)
    if result and result.get("id"):
        return result["id"]
    return None


def notion_update_trade_log(config, page_id, close_data):
    """거래 로그 row 업데이트 (청산 시)"""
    token = config["notion"]["token"]

    properties = {
        "결과": {"select": {"name": close_data["result"]}},
        "청산가": {"number": close_data["exit_price"]},
        "청산시각": {"date": {"start": close_data["exit_time"]}},
        "수익금": {"number": close_data["pnl_usd"]},
        "수익률": {"number": close_data["pnl_pct"]},
        "ROI": {"number": close_data["pnl_pct"]},
        "청산이유": {"select": {"name": close_data["exit_reason"]}},
        "보유시간": {"rich_text": [{"text": {"content": close_data["hold_time"]}}]},
    }

    payload = {"properties": properties}
    return notion_request("PATCH", f"/pages/{page_id}", token, payload)


# ──────────────────────────────────────────────
# 하트비트 / 상태 관리
# ──────────────────────────────────────────────
def write_heartbeat(engine_name):
    """하트비트 파일 갱신"""
    hb_path = BASE_DIR / f"heartbeat_{engine_name}.json"
    data = {
        "engine": engine_name,
        "timestamp": datetime.now(KST).isoformat(),
        "epoch": time.time(),
    }
    with open(hb_path, "w") as f:
        json.dump(data, f)


def load_state(engine_name):
    """엔진 상태 파일 로드"""
    state_path = BASE_DIR / f"state_{engine_name}.json"
    if state_path.exists():
        with open(state_path, "r") as f:
            return json.load(f)
    return {
        "positions": {},
        "consecutive_losses": 0,
        "cooldown_until": None,
        "daily_pnl": 0,
        "weekly_pnl": 0,
        "last_day": None,
        "last_week": None,
        "total_trades": 0,
        "total_wins": 0,
        "total_pnl": 0,
        "notion_page_ids": {},
    }


def save_state(engine_name, state):
    """엔진 상태 파일 저장 (원자적 파일 교체 — 크래시 시 파일 손상 방지)"""
    state_path = BASE_DIR / f"state_{engine_name}.json"
    tmp_path = BASE_DIR / f"state_{engine_name}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(str(tmp_path), str(state_path))  # 원자적 교체


# ──────────────────────────────────────────────
# 종목 풀 관리
# ──────────────────────────────────────────────
def get_top_symbols(client, n=30, min_volume=10_000_000):
    """바이낸스 USDT 선물 거래량 상위 N개"""
    tickers = client.get_all_tickers()
    if not tickers:
        return []

    usdt_perps = []
    for t in tickers:
        sym = t.get("symbol", "")
        if sym.endswith("USDT") and not sym.endswith("_USDT"):
            vol = float(t.get("quoteVolume", 0))
            if vol >= min_volume:
                usdt_perps.append({"symbol": sym, "volume": vol})

    usdt_perps.sort(key=lambda x: x["volume"], reverse=True)
    return [s["symbol"] for s in usdt_perps[:n]]
