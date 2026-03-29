#!/usr/bin/env python3
"""
🪃 부메랑 (BOOMERANG) 엔진 — 평균회귀
스캔 주기: 15분 (매시 06분 오프셋)
진입: RSI ≤25(롱)/≥75(숏) + BB 이탈 + 되돌림 시작 + 1D 추세역행 필터
TP1: +2.0% (50%) | TP2: +3.5% (50%) | SL: -2.0% → 본절
"""

import sys
import traceback
from pathlib import Path
sys.path.insert(0, str(Path.home() / "magi"))

from magi_common import *

STRATEGY = "boomerang"
TP1_PCT = 2.0
TP2_PCT = 3.5
SL_PCT = 2.0
MAX_POSITIONS = 3
GLOBAL_MAX = 10
LEVERAGE = 5
ENTRY_PCT = 0.05
COOLDOWN_MINUTES = 30  # 청산 후 동일 종목 재진입 제한 시간(분)

logger = create_logger("engine_boomerang", "boomerang.log")

# ── 쿨다운 관리 ──
_COOLDOWN_PATH = Path.home() / "magi" / "cooldown_boomerang.json"

def _load_cooldowns():
    """쿨다운 목록 로드"""
    try:
        if _COOLDOWN_PATH.exists():
            import json as _json
            return _json.loads(_COOLDOWN_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        pass
    return {}

def _save_cooldowns(cd):
    """쿨다운 목록 저장"""
    import json as _json
    tmp = _COOLDOWN_PATH.with_suffix(".tmp")
    tmp.write_text(_json.dumps(cd, ensure_ascii=False), encoding="utf-8")
    tmp.replace(_COOLDOWN_PATH)

def _set_cooldown(symbol):
    """종목에 쿨다운 설정"""
    cd = _load_cooldowns()
    cd[symbol] = time.time()
    _save_cooldowns(cd)

def _is_on_cooldown(symbol):
    """쿨다운 중인지 확인. 만료된 항목은 자동 정리"""
    cd = _load_cooldowns()
    ts = cd.get(symbol)
    if ts is None:
        return False
    elapsed_min = (time.time() - ts) / 60
    if elapsed_min >= COOLDOWN_MINUTES:
        del cd[symbol]
        _save_cooldowns(cd)
        return False
    return True


def check_1d_trend_filter(symbol):
    """1D 추세 필터. 반환: 'strong_bull', 'strong_bear', 'neutral'"""
    candles = fetch_kline(symbol, "D", 100)
    if not candles or len(candles) < 50:
        return "neutral"
    closes = [c["close"] for c in candles]
    ema50 = calc_ema(closes, 50)
    if not ema50:
        return "neutral"
    # 가격이 EMA50 위에 있고 상승 추세 → strong_bull
    last_close = closes[-1]
    ema50_val = ema50[-1]
    rsi = calc_rsi(closes, 14)
    adx = calc_adx(candles, 14)
    if last_close > ema50_val and rsi and rsi > 55 and adx and adx > 25:
        return "strong_bull"
    elif last_close < ema50_val and rsi and rsi < 45 and adx and adx > 25:
        return "strong_bear"
    return "neutral"


def check_mean_reversion_entry(symbol):
    """
    평균회귀 진입 조건 체크.
    반환: 'Buy'(과매도→롱), 'Sell'(과매수→숏), None
    """
    candles = fetch_kline(symbol, "240", 200)
    if not candles or len(candles) < 30:
        return None
    closes = [c["close"] for c in candles]

    # RSI 극단 + 되돌림 시작
    rsi_curr = calc_rsi(closes, 14)
    rsi_prev = calc_rsi(closes[:-1], 14)
    if rsi_curr is None or rsi_prev is None:
        return None

    # BB(20, 2σ) 이탈 확인
    upper, middle, lower, bw = calc_bollinger(closes, 20, 2)
    if upper is None:
        return None

    # v2.3: RSI 극단 → 되돌림 진입 (직전봉 RSI가 임계값 충족 필수)
    recent_closes_3 = closes[-3:]
    direction = None

    # 과매도 → 롱: 직전봉 RSI ≤ 30 + BB 하단 이탈 + 현재 RSI 상승(되돌림 시작)
    had_bb_lower = any(c < lower for c in recent_closes_3)
    if rsi_prev <= 30 and had_bb_lower and rsi_curr > rsi_prev:
        direction = "Buy"

    # 과매수 → 숏: 직전봉 RSI ≥ 70 + BB 상단 이탈 + 현재 RSI 하락(되돌림 시작)
    had_bb_upper = any(c > upper for c in recent_closes_3)
    if not direction and rsi_prev >= 70 and had_bb_upper and rsi_curr < rsi_prev:
        direction = "Sell"

    if not direction:
        return None

    # 1D 추세 역행 필터
    trend = check_1d_trend_filter(symbol)
    if direction == "Buy" and trend == "strong_bear":
        logger.info(f"  ⚠️ {symbol} 롱 진입 — 1D 하락추세 주의 (차단 해제)")
    if direction == "Sell" and trend == "strong_bull":
        logger.info(f"  ⚠️ {symbol} 숏 진입 — 1D 상승추세 주의 (차단 해제)")

    rsi_dir = "과매도→롱" if direction == "Buy" else "과매수→숏"
    # BB depth: 현재 가격의 BB 밴드 내 위치 (0=하단, 100=상단)
    bb_depth = round((closes[-1] - lower) / (upper - lower) * 100, 2) if upper != lower else 50.0
    logger.info(f"  ✅ {symbol} 평균회귀 진입: RSI {rsi_prev:.1f}→{rsi_curr:.1f} ({rsi_dir})")
    return {"direction": direction, "rsi_curr": round(rsi_curr, 2),
            "rsi_prev": round(rsi_prev, 2),
            "rsi_extreme": round(rsi_prev, 2),
            "bb_depth": bb_depth, "bb_width": round(bw, 6),
            "rsi_dir": rsi_dir}


def _read_market_state():
    """market_snapshot.json에서 시장 상태 읽기"""
    try:
        import json as _json
        _p = Path(__file__).parent / "market_snapshot.json"
        with open(_p) as _f:
            return _json.load(_f).get("market_state", "—")
    except Exception:
        return "—"

def scan_and_trade(config):
    acct = config["bybit"]["accounts"][STRATEGY]
    api_key, api_secret = acct["api_key"], acct["api_secret"]

    coins, staleness = load_active_coins(logger)
    if not coins:
        return
    if staleness == "expired":
        manage_existing(config, api_key, api_secret)
        return

    # ── closed-pnl 기반 청산 알림 (별이 + 성단) ──
    check_and_notify_closures(
        api_key, api_secret,
        "부메랑", "🪃", config, logger
    )
    if staleness == "warning":
        send_telegram(config, "⚠️ [부메랑] 마켓센서 12시간 미갱신")

    manage_existing(config, api_key, api_secret)

    # Phase 3: DD 보호 + 연속손실 체크
    if is_dd_paused(STRATEGY, logger):
        return
    if check_loss_streak(STRATEGY, logger):
        return

    my_positions = get_my_positions(api_key, api_secret)
    if len(my_positions) >= MAX_POSITIONS:
        return
    if count_global_positions(config) >= GLOBAL_MAX:
        return

    balance = get_wallet_balance(api_key, api_secret)
    if not balance or balance <= 0:
        return

    for coin in coins:
        symbol = coin["symbol"]
        if any(p["symbol"] == symbol for p in my_positions):
            continue

        # 쿨다운 체크: 청산 후 일정 시간 동일 종목 재진입 차단
        if _is_on_cooldown(symbol):
            logger.info(f"  ⏳ {symbol} 쿨다운 중 (재진입 대기)")
            continue

        # Phase 3: 블랙리스트 + 스프레드 체크
        if not check_blacklist(symbol, config, logger):
            continue
        if not check_spread(symbol, logger):
            continue

        entry_info = check_mean_reversion_entry(symbol)
        if not entry_info:
            continue
        direction = entry_info["direction"]

        if check_collision(symbol, config):
            logger.info(f"  ⛔ {symbol} 종목 충돌")
            continue

        if count_global_positions(config) >= GLOBAL_MAX:
            break

        try:
            last_price = coin.get("last_price", 0)
            if last_price <= 0:
                c = fetch_kline(symbol, "240", 1)
                if c:
                    last_price = c[-1]["close"]
            if last_price <= 0:
                continue

            qty = calc_entry_qty(balance, ENTRY_PCT, LEVERAGE, last_price)
            if qty <= 0:
                continue

            set_leverage(symbol, LEVERAGE, api_key, api_secret)
            tp1, tp2, sl = calc_tp_sl(last_price, direction, TP1_PCT, TP2_PCT, SL_PCT)
            result = place_market_order(symbol, direction, qty, api_key, api_secret,
                                        take_profit=tp2, stop_loss=sl)
            if result:
                side_kr = "롱" if direction == "Buy" else "숏"
                rsi_dir = entry_info["rsi_dir"]
                logger.info(f"🪃 진입: {symbol} {side_kr} qty={qty} @ ~{last_price}")

                send_telegram(config,
                    f"🪃 <b>[부메랑] 진입</b>\n{symbol} {side_kr} ({rsi_dir})\n"
                    f"수량: {qty} | TP1: {tp1} | TP2: {tp2} | SL: {sl}")

                # Phase 1: indicator 스냅샷 기록
                _ms = _read_market_state()
                write_trade_event(symbol, STRATEGY, "진입", extra={
                    "rsi_direction": rsi_dir,
                    "rsi_curr": entry_info["rsi_curr"],
                    "rsi_prev": entry_info["rsi_prev"],
                    "rsi_extreme": entry_info["rsi_extreme"],
                    "bb_depth": entry_info["bb_depth"],
                    "bb_width": entry_info["bb_width"],
                    "market_state": _ms,
                })

                my_positions = get_my_positions(api_key, api_secret)
                if len(my_positions) >= MAX_POSITIONS:
                    break
        except Exception as e:
            logger.error(f"진입 에러 {symbol}: {e}")

        time.sleep(0.2)


def manage_existing(config, api_key, api_secret):
    positions = get_my_positions(api_key, api_secret)
    for pos in positions:
        try:
            result = manage_position(pos, STRATEGY, api_key, api_secret, logger,
                                     tp1_pct=TP1_PCT, tp2_pct=TP2_PCT, sl_pct=SL_PCT)
            side_kr = "롱" if pos["side"] == "Buy" else "숏"
            if result == "closed":
                _set_cooldown(pos["symbol"])  # 청산 후 쿨다운 설정
                # Phase 3: 연속손실 업데이트
                pnl = float(pos.get('unrealisedPnl', 0))
                update_loss_streak(STRATEGY, pnl < 0, "SL" if pnl < 0 else "TP", config, logger)
                logger.info(f"🪃 [부메랑] 청산: {pos['symbol']} {side_kr} PnL=${pnl:+.2f} (쿨다운 {COOLDOWN_MINUTES}분)")
                send_telegram(config,
                    f"🪃 <b>[부메랑] 청산 완료</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"PnL: ${pnl:+.2f}\n"
                    f"⏳ 재진입 쿨다운: {COOLDOWN_MINUTES}분")
            elif result == "tp1_hit":
                logger.info(f"🪃 [부메랑] TP1 도달: {pos['symbol']} {side_kr} (50% 청산, SL→진입가)")
                send_telegram(config,
                    f"🎯 <b>[부메랑] TP1 도달 (50% 청산)</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"SL → 진입가 이동 (본절 보장)")
        except Exception as e:
            logger.error(f"관리 에러 {pos['symbol']}: {e}")


def main():
    logger.info("=" * 40)
    logger.info("🪃 부메랑 엔진 스캔 시작")
    config = load_config()
    update_heartbeat(STRATEGY)
    scan_and_trade(config)
    update_heartbeat(STRATEGY)
    logger.info("🪃 부메랑 엔진 스캔 완료")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"부메랑 크래시: {e}\n{traceback.format_exc()}")
        try:
            send_telegram(load_config(), f"🚨 [부메랑] 엔진 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
