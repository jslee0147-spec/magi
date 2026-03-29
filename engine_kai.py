#!/usr/bin/env python3
"""
🌊 카이 (KAI) 엔진 — 추세추종
설계: 소니 | 구현: 별이
스캔 주기: 15분 (매시 00분 오프셋)
방향: 1D EMA 7/20/50 정배열→롱, 역배열→숏, 혼조→비활성
진입: 4H EMA20 터치 + RSI 40~60 + ADX > 25
TP1: +2.0% (50%) | TP2: +4.0% (50%) | SL: -2.0% → 본절

v1.1 변경: 청산/TP1 텔레그램 알림 버그 수정
"""
import sys
import traceback
from pathlib import Path
sys.path.insert(0, str(Path.home() / "magi"))
from magi_common import *
STRATEGY = "kai"
TP1_PCT = 2.0
TP2_PCT = 4.0
SL_PCT = 2.0
MAX_POSITIONS = 3
GLOBAL_MAX = 10
LEVERAGE = 5
ENTRY_PCT = 0.05  # 잔고의 5%
COOLDOWN_MINUTES = 30  # 청산 후 동일 종목 재진입 제한 시간(분)

logger = create_logger("engine_kai", "kai.log")

# ── 쿨다운 관리 ──
_COOLDOWN_PATH = Path.home() / "magi" / "cooldown_kai.json"

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
def check_1d_direction(symbol):
    """1D EMA 7/20/50 정배열/역배열 판정"""
    candles = fetch_kline(symbol, "D", 100)
    if not candles or len(candles) < 50:
        return None
    closes = [c["close"] for c in candles]
    ema7 = calc_ema(closes, 7)
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    if not (ema7 and ema20 and ema50):
        return None
    e7, e20, e50 = ema7[-1], ema20[-1], ema50[-1]
    if e7 > e20 > e50:
        return "Buy"   # 정배열 → 롱만
    elif e7 < e20 < e50:
        return "Sell"  # 역배열 → 숏만
    return None  # 혼조 → 비활성
def check_4h_entry(symbol, direction):
    """4H 진입 조건: EMA20 터치(±1%) + RSI 35~65 (ADX 참고)"""
    candles = fetch_kline(symbol, "240", 200)
    if not candles or len(candles) < 50:
        return False
    closes = [c["close"] for c in candles]
    current_price = closes[-1]
    # EMA 20 터치 확인 (가격이 EMA20 근처 ±0.5%)
    ema20 = calc_ema(closes, 20)
    if not ema20:
        return False
    ema20_val = ema20[-1]
    touch_range = ema20_val * 0.01  # 1.0% (기존 0.5%에서 확대)
    if abs(current_price - ema20_val) > touch_range:
        return False
    # RSI 40~60 (되돌림 구간)
    rsi = calc_rsi(closes, 14)
    if rsi is None or not (35 <= rsi <= 65):  # 기존 40~60에서 확대
        return False
    # ADX 참고 로깅 (필터 해제 — 성단 분석: ADX>25와 RSI 35~65가 상호 배제)
    adx = calc_adx(candles, 14)
    adx_str = f" ADX={adx:.1f}" if adx else ""
    ema20_prox = round((current_price - ema20_val) / ema20_val * 100, 3)  # % 괴리
    logger.info(f"  ✅ {symbol} 진입 조건 충족: EMA20={ema20_val:.2f} price={current_price:.2f} RSI={rsi:.1f}{adx_str}")
    return {"ema20": round(ema20_val, 4), "rsi": round(rsi, 2),
            "adx": round(adx, 2) if adx else None,
            "ema20_proximity": ema20_prox}
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
    """메인 스캔 루프"""
    acct = config["bybit"]["accounts"][STRATEGY]
    api_key, api_secret = acct["api_key"], acct["api_secret"]
    # 종목 풀 로드
    coins, staleness = load_active_coins(logger)
    if not coins:
        logger.error("종목 풀 없음 — 스캔 중단")
        return
    if staleness == "expired":
        logger.error("종목 풀 24h 만료 — 신규 진입 중단, 포지션 관리만")
        manage_existing(config, api_key, api_secret)
        return

    # ── closed-pnl 기반 청산 알림 (별이 + 성단) ──
    check_and_notify_closures(
        api_key, api_secret,
        "카이", "🌊", config, logger
    )
    if staleness == "warning":
        send_telegram(config, "⚠️ [카이] 마켓센서 12시간 미갱신 — 종목 풀 노후화 주의")
    # 기존 포지션 관리
    manage_existing(config, api_key, api_secret)
    # Phase 3: DD 보호 + 연속손실 체크
    if is_dd_paused(STRATEGY, logger):
        return
    if check_loss_streak(STRATEGY, logger):
        return
    # 내 포지션 수 확인
    my_positions = get_my_positions(api_key, api_secret)
    if len(my_positions) >= MAX_POSITIONS:
        logger.info(f"최대 포지션 도달 ({len(my_positions)}/{MAX_POSITIONS}) — 신규 스캔 스킵")
        return
    # 전체 포지션 상한 확인
    global_count = count_global_positions(config)
    if global_count >= GLOBAL_MAX:
        logger.info(f"전체 포지션 상한 도달 ({global_count}/{GLOBAL_MAX}) — 신규 스캔 스킵")
        return
    # 잔고 조회
    balance = get_wallet_balance(api_key, api_secret)
    if not balance or balance <= 0:
        logger.warning("잔고 0 — 스캔 중단")
        return
    # 종목 스캔
    for coin in coins:
        symbol = coin["symbol"]
        # 이미 포지션 있는지
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
        # 1D 방향 판정
        direction = check_1d_direction(symbol)
        if not direction:
            continue  # 혼조 → 스킵
        # 종목 충돌 방지 (4계정 전체)
        if check_collision(symbol, config):
            logger.info(f"  ⛔ {symbol} 종목 충돌 — 다른 전략 보유 중")
            continue
        # 4H 진입 조건 체크
        entry_info = check_4h_entry(symbol, direction)
        if not entry_info:
            continue
        # 전체 포지션 재확인
        if count_global_positions(config) >= GLOBAL_MAX:
            break
        # 진입 실행
        try:
            last_price = coin.get("last_price", 0)
            if last_price <= 0:
                candles = fetch_kline(symbol, "240", 1)
                if candles:
                    last_price = candles[-1]["close"]
            if last_price <= 0:
                continue
            qty = calc_entry_qty(balance, ENTRY_PCT, LEVERAGE, last_price)
            if qty <= 0:
                continue
            # 레버리지 설정
            set_leverage(symbol, LEVERAGE, api_key, api_secret)
            # TP/SL 계산
            tp1, tp2, sl = calc_tp_sl(last_price, direction, TP1_PCT, TP2_PCT, SL_PCT)
            # 시장가 주문 (SL만 설정, TP는 엔진이 관리)
            result = place_market_order(symbol, direction, qty, api_key, api_secret,
                                        take_profit=tp2, stop_loss=sl)
            if result:
                side_kr = "롱" if direction == "Buy" else "숏"
                logger.info(f"🌊 진입: {symbol} {side_kr} qty={qty} @ ~{last_price}")

                send_telegram(config,
                    f"🌊 <b>[카이] 진입</b>\n"
                    f"{symbol} {side_kr}\n"
                    f"수량: {qty} | 레버리지: {LEVERAGE}x\n"
                    f"TP1: {tp1} | TP2: {tp2} | SL: {sl}")
                # Phase 1: indicator 스냅샷 기록
                _ms = _read_market_state()
                write_trade_event(symbol, STRATEGY, "진입", extra={
                    "ema_state": f"EMA20={entry_info['ema20']}",
                    "rsi": entry_info["rsi"],
                    "adx": entry_info["adx"],
                    "ema20_proximity": entry_info["ema20_proximity"],
                    "market_state": _ms,
                })
                # 포지션 수 갱신
                my_positions = get_my_positions(api_key, api_secret)
                if len(my_positions) >= MAX_POSITIONS:
                    break
        except Exception as e:
            logger.error(f"진입 에러 {symbol}: {e}")

        time.sleep(0.2)
def manage_existing(config, api_key, api_secret):
    """기존 포지션 TP1/TP2/SL 관리 — v1.1: 모든 상태 알림"""
    positions = get_my_positions(api_key, api_secret)
    for pos in positions:
        try:
            result = manage_position(pos, STRATEGY, api_key, api_secret, logger,
                                     tp1_pct=TP1_PCT, tp2_pct=TP2_PCT, sl_pct=SL_PCT)
            side_kr = "롱" if pos["side"] == "Buy" else "숏"
            if result == "closed":
                _set_cooldown(pos["symbol"])
                pnl = float(pos.get('unrealisedPnl', 0))
                update_loss_streak(STRATEGY, pnl < 0, "SL" if pnl < 0 else "TP", config, logger)
                logger.info(f"🌊 [카이] 청산: {pos['symbol']} {side_kr} PnL=${pnl:+.2f} (쿨다운 {COOLDOWN_MINUTES}분)")
                send_telegram(config,
                    f"🌊 <b>[카이] 청산 완료</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"PnL: ${pnl:+.2f}\n"
                    f"⏳ 재진입 쿨다운: {COOLDOWN_MINUTES}분")
            elif result == "tp1_hit":
                logger.info(f"🌊 [카이] TP1 도달: {pos['symbol']} {side_kr} (50% 청산, SL→진입가)")
                send_telegram(config,
                    f"🎯 <b>[카이] TP1 도달 (50% 청산)</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"SL → 진입가 이동 (본절 보장)")
        except Exception as e:
            logger.error(f"포지션 관리 에러 {pos['symbol']}: {e}")
def main():
    logger.info("=" * 40)
    logger.info("🌊 카이 엔진 스캔 시작")
    config = load_config()
    update_heartbeat(STRATEGY)
    scan_and_trade(config)
    update_heartbeat(STRATEGY)
    logger.info("🌊 카이 엔진 스캔 완료")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"카이 크래시: {e}\n{traceback.format_exc()}")
        try:
            send_telegram(load_config(), f"🚨 [카이] 엔진 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
