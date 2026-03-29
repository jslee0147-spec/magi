#!/usr/bin/env python3
"""
⚡ 제트 (JET) 엔진 — 변동성 돌파 (스퀴즈)
스캔 주기: 15분 (매시 03분 오프셋)
진입: BB 스퀴즈 대기 → 돌파 + 거래량 1.5배 + ADX 상승
TP1: +3.0% (50%) | TP2: +6.0% (50%) | SL: -2.5% → 본절
시간손절: 24시간 내 TP1 미도달 시 청산
"""

import sys
import traceback
from pathlib import Path
sys.path.insert(0, str(Path.home() / "magi"))

from magi_common import *

STRATEGY = "jet"
TP1_PCT = 3.0
TP2_PCT = 6.0
SL_PCT = 2.5
MAX_POSITIONS = 3
GLOBAL_MAX = 10
LEVERAGE = 5
ENTRY_PCT = 0.05
TIME_STOP_HOURS = 24
COOLDOWN_MINUTES = 30  # 청산 후 동일 종목 재진입 제한 시간(분)

logger = create_logger("engine_jet", "jet.log")

# ── 쿨다운 관리 ──
_COOLDOWN_PATH = Path.home() / "magi" / "cooldown_jet.json"

def _load_cooldowns():
    try:
        if _COOLDOWN_PATH.exists():
            import json as _json
            return _json.loads(_COOLDOWN_PATH.read_text(encoding="utf-8"))
    except (ValueError,):
        pass
    return {}

def _save_cooldowns(cd):
    import json as _json
    tmp = _COOLDOWN_PATH.with_suffix(".tmp")
    tmp.write_text(_json.dumps(cd, ensure_ascii=False), encoding="utf-8")
    tmp.replace(_COOLDOWN_PATH)

def _set_cooldown(symbol):
    cd = _load_cooldowns()
    cd[symbol] = time.time()
    _save_cooldowns(cd)

def _is_on_cooldown(symbol):
    cd = _load_cooldowns()
    ts = cd.get(symbol)
    if ts is None:
        return False
    if (time.time() - ts) / 60 >= COOLDOWN_MINUTES:
        del cd[symbol]
        _save_cooldowns(cd)
        return False
    return True


def check_squeeze_breakout(symbol):
    """
    BB 스퀴즈 감지 + 돌파 판정.
    반환: 'Buy'(상단돌파→롱), 'Sell'(하단돌파→숏), None
    """
    candles = fetch_kline(symbol, "240", 200)
    if not candles or len(candles) < 130:
        return None
    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    current = candles[-1]

    # BB(20, 2σ) 계산
    upper, middle, lower, bandwidth = calc_bollinger(closes, 20, 2)
    if upper is None:
        return None

    # BB bandwidth 히스토리 (120봉)
    bw_history = calc_bb_bandwidth_history(closes, 20, 2, 120)
    if len(bw_history) < 10:
        return None

    # 스퀴즈 판정: 현재 bandwidth가 최근 120봉의 하위 20%
    sorted_bw = sorted(bw_history)
    threshold_idx = max(1, int(len(sorted_bw) * 0.3))  # 하위 30% (기존 20%에서 완화)
    squeeze_threshold = sorted_bw[threshold_idx]

    # v2.2: 과거 6봉 이내 스퀴즈 이력 + 현재 bandwidth 급격 확대
    recent_bw = bw_history[-7:-1] if len(bw_history) >= 7 else bw_history[:-1]
    had_squeeze = any(bw <= squeeze_threshold for bw in recent_bw)
    if not had_squeeze:
        return None  # 최근 6봉 내 스퀴즈 이력 없음

    # bandwidth 급격 확대 확인 (직전 대비 1.5배 이상)
    if len(bw_history) >= 2:
        prev_bw = bw_history[-2]
        if prev_bw > 0 and bandwidth / prev_bw < 1.5:
            return None  # bandwidth 확대 불충분

    # 돌파 확인
    close_price = current["close"]
    direction = None
    if close_price > upper:
        direction = "Buy"   # 상단 돌파 → 롱
    elif close_price < lower:
        direction = "Sell"  # 하단 돌파 → 숏
    else:
        return None  # bandwidth 확대 중이지만 아직 돌파 안 됨

    # 거래량 확인: 돌파 캔들 거래량 > 직전 20봉 평균의 1.5배
    if len(volumes) >= 21:
        avg_vol = sum(volumes[-21:-1]) / 20
        if volumes[-1] < avg_vol * 1.2:  # 기존 1.5x에서 완화
            return None  # 거래량 부족

    # ADX 상승 확인 (현재 ADX > 직전 봉 ADX)
    adx_curr = calc_adx(candles, 14)
    adx_prev = calc_adx(candles[:-1], 14)
    if adx_curr is None or adx_prev is None:
        return None
    if adx_curr <= adx_prev:
        return None  # ADX 하락 중

    _bw_exp = round(bandwidth / bw_history[-2], 2) if len(bw_history) >= 2 and bw_history[-2] > 0 else None
    _avg_v = sum(volumes[-21:-1]) / 20 if len(volumes) >= 21 else None
    _vol_r = round(volumes[-1] / _avg_v, 2) if _avg_v and _avg_v > 0 else None
    logger.info(f"  ✅ {symbol} 스퀴즈 돌파: BW={bandwidth:.4f} thr={squeeze_threshold:.4f} "
                f"dir={'롱' if direction == 'Buy' else '숏'} ADX={adx_curr:.1f}")
    return {"direction": direction, "bandwidth": round(bandwidth, 6),
            "squeeze_threshold": round(squeeze_threshold, 6),
            "bw_expansion": _bw_exp, "volume_ratio": _vol_r,
            "adx": round(adx_curr, 2)}


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
        "제트", "⚡", config, logger
    )
    if staleness == "warning":
        send_telegram(config, "⚠️ [제트] 마켓센서 12시간 미갱신")

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

        # 쿨다운 체크
        if _is_on_cooldown(symbol):
            logger.info(f"  ⏳ {symbol} 쿨다운 중 (재진입 대기)")
            continue

        # Phase 3: 블랙리스트 + 스프레드 체크
        if not check_blacklist(symbol, config, logger):
            continue
        if not check_spread(symbol, logger):
            continue

        entry_info = check_squeeze_breakout(symbol)
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
                                        take_profit=tp1, stop_loss=sl)
            if result:
                side_kr = "롱" if direction == "Buy" else "숏"
                logger.info(f"⚡ 진입: {symbol} {side_kr} qty={qty} @ ~{last_price}")

                send_telegram(config,
                    f"⚡ <b>[제트] 진입</b>\n{symbol} {side_kr}\n"
                    f"수량: {qty} | TP1: {tp1} | TP2: {tp2} | SL: {sl}")
                # Phase 1: indicator 스냅샷 기록
                _ms = _read_market_state()
                write_trade_event(symbol, STRATEGY, "진입", extra={
                    "squeeze_status": f"BW={entry_info['bandwidth']} exp={entry_info['bw_expansion']}x",
                    "bandwidth": entry_info["bandwidth"],
                    "bw_expansion": entry_info["bw_expansion"],
                    "volume_ratio": entry_info["volume_ratio"],
                    "adx": entry_info["adx"],
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
                                     tp1_pct=TP1_PCT, tp2_pct=TP2_PCT, sl_pct=SL_PCT,
                                     time_stop_hours=TIME_STOP_HOURS)
            side_kr = "롱" if pos["side"] == "Buy" else "숏"
            if result == "closed":
                _set_cooldown(pos["symbol"])
                pnl = float(pos.get('unrealisedPnl', 0))
                update_loss_streak(STRATEGY, pnl < 0, "SL" if pnl < 0 else "TP", config, logger)
                logger.info(f"⚡ [제트] 청산: {pos['symbol']} {side_kr} PnL=${pnl:+.2f} (쿨다운 {COOLDOWN_MINUTES}분)")
                send_telegram(config,
                    f"⚡ <b>[제트] 청산 완료</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"PnL: ${pnl:+.2f}\n"
                    f"⏳ 재진입 쿨다운: {COOLDOWN_MINUTES}분")
            elif result == "tp1_hit":
                logger.info(f"⚡ [제트] TP1 도달: {pos['symbol']} {side_kr} (50% 청산, SL→진입가)")
                send_telegram(config,
                    f"🎯 <b>[제트] TP1 도달 (50% 청산)</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"SL → 진입가 이동 (본절 보장)")
        except Exception as e:
            logger.error(f"관리 에러 {pos['symbol']}: {e}")


def main():
    logger.info("=" * 40)
    logger.info("⚡ 제트 엔진 스캔 시작")
    config = load_config()
    update_heartbeat(STRATEGY)
    scan_and_trade(config)
    update_heartbeat(STRATEGY)
    logger.info("⚡ 제트 엔진 스캔 완료")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"제트 크래시: {e}\n{traceback.format_exc()}")
        try:
            send_telegram(load_config(), f"🚨 [제트] 엔진 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
