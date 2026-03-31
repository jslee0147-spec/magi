#!/usr/bin/env python3
"""
🌙 나이트(Nite) — MAGI v3.0 Engine #2
숏 전용 추세추종 | SuperTrend ATR + 1D EMA(20)
설계서 v1.2 기준 구현

스캔: 2H 봉 종료 시 (UTC 매 짝수 시간 +30초)
"일격이탈하는 암살자" — 빠르게 들어가서, 빠르게 먹고, 빠르게 나온다.
"""

import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from magi_v3_common import (
    load_config, create_logger, BinanceClient, send_telegram,
    calc_supertrend, calc_rsi, calc_ema, calc_volume_ratio, calc_atr,
    calc_signal_score, calc_position_size, get_top_symbols,
    write_heartbeat, load_state, save_state, write_scan_log,
    notion_create_trade_log, notion_update_trade_log,
    KST, BASE_DIR
)

ENGINE_NAME = "nite"
SIDE = "short"
logger = create_logger(ENGINE_NAME, f"{ENGINE_NAME}.log")


def scan_and_trade(config, client, state, params, symbols):
    """메인 스캔 로직: ①청산 체크 → ②신규 진입"""
    now = datetime.now(timezone.utc)
    equity = client.get_equity()
    if equity <= 0:
        logger.error("자본 조회 실패 또는 0")
        return state

    logger.info(f"━━ 스캔 시작 | Equity: ${equity:.2f} | 포지션: {len(state['positions'])}개 ━━")

    # ── 스캔 퍼널 카운터 ──
    scan_log = {
        "scan_time": now.isoformat(),
        "engine": "nite",
        "equity": equity,
        "symbol_pool": len(symbols),
        "scanned_ok": 0,
        "kline_failed": 0,
        "funnel": {
            "st_signal": 0, "rsi_pass": 0, "volume_pass": 0,
            "atr_pass": 0, "ema_pass": 0, "score_pass": 0,
            "slot_available": 0, "entered": 0,
        },
        "near_miss": [],
        "positions": len(state["positions"]),
        "cooldown": bool(state.get("cooldown_until")),
        "api_errors": 0,
    }

    # ── 일일/주간 PnL 리셋 ──
    today = now.date().isoformat()
    week = now.isocalendar()[1]
    if state["last_day"] != today:
        state["daily_pnl"] = 0
        state["last_day"] = today
    if state["last_week"] != week:
        state["weekly_pnl"] = 0
        state["last_week"] = week

    # ══════════════════════════════════════
    # 1단계: 기존 포지션 청산 체크
    # ══════════════════════════════════════
    positions_to_close = []
    for symbol, pos in list(state["positions"].items()):
        try:
            df = client.get_klines(symbol, "2h", limit=30)
            if df.empty:
                continue
            current = df.iloc[-1]
            atr = calc_atr(df, params["ATR_PERIOD"]).iloc[-1]
            close_price = current["close"]
            low = current["low"]
            high = current["high"]

            hold_hours = (now - datetime.fromisoformat(pos["entry_time"])).total_seconds() / 3600
            current_pnl_pct = (pos["entry_price"] - close_price) / pos["entry_price"] * params["LEVERAGE"]

            # 트레일링 업데이트 (숏: 최저가 추적)
            if low < pos.get("lowest_price", pos["entry_price"]):
                pos["lowest_price"] = low
                new_trailing = low + (atr * params["TRAILING_ATR_MULTIPLE"])
                if new_trailing < pos["trailing_stop"]:
                    pos["trailing_stop"] = new_trailing

            # 본절 체크 (ATR × 0.5)
            profit_pct = (pos["entry_price"] - low) / pos["entry_price"]
            be_trigger = (pos["atr_at_entry"] / pos["entry_price"]) * params["BE_TRIGGER_ATR_MULT"]
            if not pos.get("be_activated") and profit_pct >= be_trigger:
                pos["be_activated"] = True
                pos["be_price"] = pos["entry_price"]
                logger.info(f"  {symbol} 본절 활성화 (수익 {profit_pct*100:.1f}%)")

            # SL = min(본절, 트레일링) — 숏에서 min = 진입가에 가까운 쪽 = 보수적
            if pos.get("be_activated") and pos.get("be_price"):
                pos["trailing_stop"] = min(pos["trailing_stop"], pos["be_price"])

            # 청산 조건 체크 (우선순위)
            exit_reason = None
            exit_price = None

            # 1. Hard Stop (숏: 고가가 HS 위)
            if high >= pos["hard_stop"]:
                exit_reason = "HARD_STOP"
                exit_price = pos["hard_stop"]
            # 2. Trailing Stop (숏: 종가가 TS 위)
            elif close_price >= pos["trailing_stop"]:
                exit_reason = "TRAILING_STOP"
                exit_price = pos["trailing_stop"]
            # 3. 시간손절 (수익 < +1% 시)
            elif hold_hours >= params["MAX_HOLD_HOURS_LOSS"]:
                if current_pnl_pct < params["PROFIT_THRESHOLD_TIME"]:
                    exit_reason = "TIME_STOP"
                    exit_price = close_price

            if exit_reason:
                positions_to_close.append((symbol, exit_price, exit_reason))

        except Exception as e:
            logger.error(f"  {symbol} 청산 체크 에러: {e}")

    # ── 청산 실행 ──
    for symbol, exit_price, reason in positions_to_close:
        pos = state["positions"][symbol]
        try:
            # 서버 Stop 주문 먼저 취소 (중복 청산 방지)
            cancel_result = client.cancel_all_orders(symbol)
            if not cancel_result:
                logger.warning(f"  {symbol} 주문 취소 실패 — 청산 스킵, 다음 스캔에서 재시도")
                send_telegram(config, f"⚠️ 나이트 {symbol} 주문 취소 실패 — 수동 확인 필요")
                continue

            qty = abs(float(pos["quantity"]))
            result = client.close_position(symbol, "SELL", qty)

            if not result:
                send_telegram(config, f"🚨 나이트 청산 실패!\n{symbol} {reason}\n포지션 방치 위험 — 수동 확인 필요!")
                continue

            actual_exit = exit_price
            if result and result.get("avgPrice"):
                actual_exit = float(result["avgPrice"])

            pnl_pct = (pos["entry_price"] - actual_exit) / pos["entry_price"] * params["LEVERAGE"]
            pnl_usd = pos["position_usd"] * pnl_pct

            state["daily_pnl"] += pnl_usd
            state["weekly_pnl"] += pnl_usd
            state["total_pnl"] += pnl_usd
            state["total_trades"] += 1

            if pnl_usd > 0:
                state["total_wins"] += 1
                state["consecutive_losses"] = 0
                trade_result = "승"
            elif pnl_usd == 0:
                state["consecutive_losses"] = 0
                trade_result = "본절"
            else:
                state["consecutive_losses"] += 1
                trade_result = "패"

            hold_hours = (now - datetime.fromisoformat(pos["entry_time"])).total_seconds() / 3600

            # 노션 업데이트
            page_id = state["notion_page_ids"].get(symbol)
            if page_id:
                notion_update_trade_log(config, page_id, {
                    "result": trade_result,
                    "exit_price": actual_exit,
                    "exit_time": now.isoformat(),
                    "pnl_usd": round(pnl_usd, 2),
                    "pnl_pct": round(pnl_pct, 4),
                    "exit_reason": reason,
                    "hold_time": f"{hold_hours:.1f}H",
                })
                del state["notion_page_ids"][symbol]

            # 텔레그램
            emoji = "🟢" if pnl_usd >= 0 else "🔴"
            msg = (f"🌙 <b>나이트 청산</b>\n"
                   f"{emoji} {symbol} | {reason}\n"
                   f"진입: ${pos['entry_price']:.2f} → 청산: ${actual_exit:.2f}\n"
                   f"PnL: ${pnl_usd:+.2f} ({pnl_pct*100:+.1f}%)\n"
                   f"보유: {hold_hours:.1f}H")
            send_telegram(config, msg)

            logger.info(f"  ✅ {symbol} 청산: {reason} | PnL ${pnl_usd:+.2f} ({pnl_pct*100:+.1f}%)")
            del state["positions"][symbol]

        except Exception as e:
            logger.error(f"  ❌ {symbol} 청산 실행 에러: {e}")

    # ══════════════════════════════════════
    # 2단계: 쿨다운 체크
    # ══════════════════════════════════════
    if state.get("cooldown_until"):
        cd = datetime.fromisoformat(state["cooldown_until"])
        if now < cd:
            logger.info(f"  쿨다운 중 ({cd.strftime('%m/%d %H:%M')}까지)")
            return state
        else:
            state["cooldown_until"] = None

    if state["consecutive_losses"] >= params["COOLDOWN_LOSSES"]:
        next_day = (now + timedelta(days=1)).replace(hour=8, minute=0, second=0)
        state["cooldown_until"] = next_day.isoformat()
        logger.warning(f"  ⚠️ {state['consecutive_losses']}연속 손절 → 쿨다운")
        send_telegram(config, f"🌙 나이트 쿨다운 발동\n{state['consecutive_losses']}회 연속 손절")
        return state

    if state["daily_pnl"] <= -(equity * params["DAILY_MAX_LOSS"]):
        logger.warning(f"  ⚠️ 일일 한도 (${state['daily_pnl']:.2f})")
        return state
    if state["weekly_pnl"] <= -(equity * params["WEEKLY_MAX_LOSS"]):
        logger.warning(f"  ⚠️ 주간 한도 (${state['weekly_pnl']:.2f})")
        return state

    # ══════════════════════════════════════
    # 3단계: 신규 진입 스캔 (숏만)
    # ══════════════════════════════════════
    available = params["MAX_POSITIONS"] - len(state["positions"])
    if available <= 0:
        logger.info(f"  슬롯 없음 ({len(state['positions'])}/{params['MAX_POSITIONS']})")
        return state

    scan_log["funnel"]["slot_available"] = available

    candidates = []
    for symbol in symbols:
        if symbol in state["positions"]:
            continue
        try:
            df = client.get_klines(symbol, "2h", limit=100)
            if len(df) < 50:
                scan_log["kline_failed"] += 1
                continue

            scan_log["scanned_ok"] += 1

            st, st_dir, atr = calc_supertrend(df, params["ATR_PERIOD"], params["ATR_MULTIPLIER"])
            df["rsi"] = calc_rsi(df, params["RSI_PERIOD"])
            df["vol_ratio"] = calc_volume_ratio(df, params["VOLUME_RATIO_PERIOD"])

            current = df.iloc[-1]
            prev_dir = st_dir.iloc[-2] if len(st_dir) > 1 else 0

            if not (st_dir.iloc[-1] == -1 and prev_dir == 1):
                continue
            scan_log["funnel"]["st_signal"] += 1

            if current["rsi"] >= params["RSI_THRESHOLD"]:
                gap = abs(current["rsi"] - params["RSI_THRESHOLD"]) / params["RSI_THRESHOLD"] * 100
                if gap <= 10:
                    scan_log["near_miss"].append({"symbol": symbol, "failed_at": "rsi", "value": round(current["rsi"], 1), "threshold": params["RSI_THRESHOLD"], "gap_pct": round(gap, 1)})
                continue
            scan_log["funnel"]["rsi_pass"] += 1

            if params["VOLUME_FILTER"] and current["vol_ratio"] < 1.0:
                continue
            scan_log["funnel"]["volume_pass"] += 1

            atr_20_avg = atr.iloc[-21:-1].mean()
            if atr.iloc[-1] > atr_20_avg * 2.0:
                continue
            scan_log["funnel"]["atr_pass"] += 1

            df_1d = client.get_klines(symbol, "1d", limit=30)
            if len(df_1d) < params["EMA_PERIOD_1D"]:
                continue
            ema_1d = calc_ema(df_1d["close"], params["EMA_PERIOD_1D"]).iloc[-1]
            if df_1d["close"].iloc[-1] >= ema_1d:
                dist = abs(df_1d["close"].iloc[-1] - ema_1d) / ema_1d * 100
                if dist <= 2.0:
                    scan_log["near_miss"].append({"symbol": symbol, "failed_at": "ema", "value": round(df_1d["close"].iloc[-1], 2), "threshold": round(ema_1d, 2), "gap_pct": round(dist, 1)})
                continue
            scan_log["funnel"]["ema_pass"] += 1

            ema_dist = abs(df_1d["close"].iloc[-1] - ema_1d) / ema_1d * 100
            score = calc_signal_score(current["rsi"], current["vol_ratio"], ema_dist, "short")
            if score < params["MIN_SIGNAL_SCORE"]:
                gap = abs(score - params["MIN_SIGNAL_SCORE"]) / params["MIN_SIGNAL_SCORE"] * 100
                if gap <= 10:
                    scan_log["near_miss"].append({"symbol": symbol, "failed_at": "score", "value": round(score, 2), "threshold": params["MIN_SIGNAL_SCORE"], "gap_pct": round(gap, 1)})
                continue
            scan_log["funnel"]["score_pass"] += 1

            candidates.append({
                "symbol": symbol,
                "price": current["close"],
                "atr": atr.iloc[-1],
                "rsi": current["rsi"],
                "vol_ratio": current["vol_ratio"],
                "score": score,
            })

        except Exception as e:
            logger.warning(f"  {symbol} 스캔 에러: {e}")

    candidates.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"  후보: {len(candidates)}개 (슬롯: {available})")
    logger.info(f"  [퍼널] ST:{scan_log['funnel']['st_signal']} → RSI:{scan_log['funnel']['rsi_pass']} → Vol:{scan_log['funnel']['volume_pass']} → ATR:{scan_log['funnel']['atr_pass']} → EMA:{scan_log['funnel']['ema_pass']} → Score:{scan_log['funnel']['score_pass']} → 진입:{scan_log['funnel']['entered']}")

    # ── 진입 실행 ──
    for cand in candidates[:available]:
        symbol = cand["symbol"]
        try:
            price = cand["price"]
            atr_val = cand["atr"]

            pos_usd = calc_position_size(equity, atr_val, price, params)
            if pos_usd <= 0:
                continue

            client.set_leverage(symbol, params["LEVERAGE"])
            client.set_margin_type(symbol, "CROSSED")

            qty = client.calc_quantity(symbol, pos_usd, price, params["LEVERAGE"])
            if qty <= 0:
                continue

            # 시장가 매도 (숏)
            result = client.place_market_order(symbol, "SELL", qty)
            if not result or not result.get("orderId"):
                continue

            actual_entry = float(result.get("avgPrice", price))
            hard_stop = actual_entry + (atr_val * params["HARD_STOP_ATR_MULT"])
            trailing_stop = actual_entry + (atr_val * params["TRAILING_ATR_MULTIPLE"])

            # 서버 Stop Market 설정 (봉 사이 급등 안전망, 숏은 BUY로 청산)
            stop_result = client.place_stop_market(symbol, "BUY", hard_stop, qty)
            stop_order_id = stop_result.get("orderId") if stop_result else None

            state["positions"][symbol] = {
                "entry_price": actual_entry,
                "entry_time": now.isoformat(),
                "quantity": qty,
                "position_usd": pos_usd,
                "atr_at_entry": atr_val,
                "hard_stop": hard_stop,
                "trailing_stop": trailing_stop,
                "lowest_price": actual_entry,
                "be_activated": False,
                "be_price": None,
                "order_id": result["orderId"],
                "stop_order_id": stop_order_id,
            }

            scan_log["funnel"]["entered"] += 1
            if not stop_result:
                send_telegram(config, f"⚠️ 나이트 {symbol} Stop Market 설정 실패 — 서버 보호 없음!")

            page_id = notion_create_trade_log(config, {
                "symbol": symbol,
                "engine": "🌙 나이트",
                "direction": "숏",
                "entry_price": actual_entry,
                "entry_time": now.isoformat(),
                "quantity": qty,
                "bet_amount": round(pos_usd, 2),
                "leverage": params["LEVERAGE"],
                "atr": round(atr_val, 2),
                "rsi": round(cand["rsi"], 1),
                "signal_score": round(cand["score"], 2),
                "order_id": result["orderId"],
            })
            if page_id:
                state["notion_page_ids"][symbol] = page_id

            msg = (f"🌙 <b>나이트 진입</b>\n"
                   f"📉 {symbol} 숏\n"
                   f"진입: ${actual_entry:.2f} | SL: ${hard_stop:.2f}\n"
                   f"금액: ${pos_usd:.2f} × {params['LEVERAGE']}x\n"
                   f"RSI: {cand['rsi']:.1f} | Score: {cand['score']:.2f}")
            send_telegram(config, msg)

            logger.info(f"  🌙 진입: {symbol} 숏 @ ${actual_entry:.2f} | SL ${hard_stop:.2f} | ${pos_usd:.2f}")

        except Exception as e:
            logger.error(f"  ❌ {symbol} 진입 실패: {e}")

    # 스캔 퍼널 로그 기록
    scan_log["positions"] = len(state["positions"])
    write_scan_log("nite", scan_log)

    return state


def main():
    config = load_config()
    engine_config = config["engines"][ENGINE_NAME]
    params = engine_config["params"]

    client = BinanceClient(engine_config["api_key"], engine_config["api_secret"], logger)
    state = load_state(ENGINE_NAME)

    logger.info(f"🌙 나이트 엔진 시작 | 숏 전용 | 2H | Top {params['TOP_N_SYMBOLS']}")

    symbols = get_top_symbols(client, params["TOP_N_SYMBOLS"])
    logger.info(f"종목 풀: {len(symbols)}개")

    try:
        state = scan_and_trade(config, client, state, params, symbols)
        save_state(ENGINE_NAME, state)
        write_heartbeat(ENGINE_NAME)
        logger.info(f"━━ 스캔 완료 | 포지션: {len(state['positions'])}개 ━━\n")
    except Exception as e:
        logger.critical(f"나이트 크래시: {e}\n{traceback.format_exc()}")
        send_telegram(config, f"🚨 나이트 크래시!\n{e}")
        raise


if __name__ == "__main__":
    main()
