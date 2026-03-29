#!/usr/bin/env python3
"""
👁️ 릴리스 (RELEASE) 엔진 — 펀딩비+OI 역발상
스캔 주기: 8시간 (펀딩비 정산 직후 00:00/08:00/16:00 UTC = 09:00/17:00/01:00 KST)
          + 1시간 주기 OI 모니터링
진입: 펀딩비 ≥ ±0.08% AND OI 24h 변화율 > +15%
TP1: +2.5% (50%) | TP2: +5.0% (50%) | SL: -3.0% → 본절
시간손절: 36시간 내 TP1 미도달 시 청산
"""

import sys
import traceback
from pathlib import Path
sys.path.insert(0, str(Path.home() / "magi"))

from magi_common import *

STRATEGY = "release"
TP1_PCT = 2.5
TP2_PCT = 5.0
SL_PCT = 3.0
MAX_POSITIONS = 3
GLOBAL_MAX = 10
LEVERAGE = 5
ENTRY_PCT = 0.05
TIME_STOP_HOURS = 36
FUNDING_THRESHOLD = 0.0005  # ±0.05% (기존 0.08%에서 완화 — 성단 분석)
OI_CHANGE_THRESHOLD = 10    # 24h OI 변화율 > +10% (기존 15%에서 완화 — 성단 분석)
COOLDOWN_MINUTES = 120  # 청산 후 동일 종목 재진입 제한 시간(분) — 2시간 (소니 승인)

logger = create_logger("engine_release", "release.log")

# ── 쿨다운 관리 ──
_COOLDOWN_PATH = Path.home() / "magi" / "cooldown_release.json"

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


def fetch_funding_rate(symbol):
    """현재 펀딩비 조회"""
    result = bybit_public_get("/v5/market/tickers", {
        "category": "linear", "symbol": symbol
    })
    if result and result.get("list"):
        return float(result["list"][0].get("fundingRate", 0))
    return None


def fetch_oi_change_24h(symbol):
    """24시간 OI 변화율 (%) 계산"""
    result = bybit_public_get("/v5/market/open-interest", {
        "category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": "25"
    })
    if not result or not result.get("list"):
        return None
    oi_list = result["list"]
    if len(oi_list) < 2:
        return None
    # 최신 OI와 24시간 전 OI
    current_oi = float(oi_list[0].get("openInterest", 0))
    # 가장 오래된 데이터 (약 24시간 전)
    old_oi = float(oi_list[-1].get("openInterest", 0))
    if old_oi == 0:
        return None
    change_pct = (current_oi - old_oi) / old_oi * 100
    return change_pct


def check_contrarian_entry(symbol):
    """
    펀딩비 극단 + OI 과열 동시 충족.
    반환: 'Buy'(숏과열→롱), 'Sell'(롱과열→숏), None
    """
    funding = fetch_funding_rate(symbol)
    if funding is None:
        return None, None, None

    oi_change = fetch_oi_change_24h(symbol)
    if oi_change is None:
        return None, None, None

    direction = None
    # 롱 과열 → 숏 진입
    if funding >= FUNDING_THRESHOLD and oi_change > OI_CHANGE_THRESHOLD:
        direction = "Sell"
    # 숏 과열 → 롱 진입
    elif funding <= -FUNDING_THRESHOLD and oi_change > OI_CHANGE_THRESHOLD:
        direction = "Buy"

    if direction:
        logger.info(f"  ✅ {symbol} 역발상 신호: 펀딩비={funding*100:.4f}% OI변화={oi_change:+.1f}% "
                     f"→ {'롱' if direction == 'Buy' else '숏'}")

    return direction, funding, oi_change


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
        "릴리스", "👁️", config, logger
    )
    if staleness == "warning":
        send_telegram(config, "⚠️ [릴리스] 마켓센서 12시간 미갱신")

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

        direction, funding, oi_change = check_contrarian_entry(symbol)
        if not direction:
            continue

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
                logger.info(f"👁️ 진입: {symbol} {side_kr} qty={qty} @ ~{last_price}")

                send_telegram(config,
                    f"👁️ <b>[릴리스] 진입</b>\n{symbol} {side_kr}\n"
                    f"펀딩비: {funding*100:.4f}% | OI변화: {oi_change:+.1f}%\n"
                    f"수량: {qty} | TP1: {tp1} | TP2: {tp2} | SL: {sl}")

                # Phase 1: indicator 스냅샷 기록 (+ market_state)
                _ms = _read_market_state()
                write_trade_event(symbol, STRATEGY, "진입", extra={
                    "funding_rate": funding, "oi_change": oi_change,
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
                logger.info(f"👁️ [릴리스] 청산: {pos['symbol']} {side_kr} PnL=${pnl:+.2f} (쿨다운 {COOLDOWN_MINUTES}분)")
                send_telegram(config,
                    f"👁️ <b>[릴리스] 청산 완료</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"PnL: ${pnl:+.2f}\n"
                    f"⏳ 재진입 쿨다운: {COOLDOWN_MINUTES}분")
            elif result == "tp1_hit":
                logger.info(f"👁️ [릴리스] TP1 도달: {pos['symbol']} {side_kr} (50% 청산, SL→진입가)")
                send_telegram(config,
                    f"🎯 <b>[릴리스] TP1 도달 (50% 청산)</b>\n"
                    f"{pos['symbol']} {side_kr}\n"
                    f"SL → 진입가 이동 (본절 보장)")
        except Exception as e:
            logger.error(f"관리 에러 {pos['symbol']}: {e}")


def main():
    logger.info("=" * 40)
    logger.info("👁️ 릴리스 엔진 스캔 시작")
    config = load_config()
    update_heartbeat(STRATEGY)
    scan_and_trade(config)
    update_heartbeat(STRATEGY)
    logger.info("👁️ 릴리스 엔진 스캔 완료")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"릴리스 크래시: {e}\n{traceback.format_exc()}")
        try:
            send_telegram(load_config(), f"🚨 [릴리스] 엔진 크래시!\n{e}")
        except Exception:
            pass
        sys.exit(1)
