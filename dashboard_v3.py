#!/usr/bin/env python3
"""
📋 MAGI v3.0 통합 현황판 — dashboard_v3.py
5분마다 실행: 히노카미 + 나이트 상태를 노션 현황판에 업데이트

업데이트 항목:
  - BTC 현재가, 시장상태
  - 엔진별 Equity/PnL/거래수/승률/활성포지션
  - 합산 통계
"""

import sys
import json
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from magi_v3_common import (
    load_config, create_logger, BinanceClient, send_telegram,
    notion_request, load_state, write_heartbeat, get_daily_scan_summary,
    calc_ema, KST, BASE_DIR
)

logger = create_logger("dashboard", "dashboard.log")


def get_market_state(btc_price, btc_ema50):
    """시장 상태 판별 (단순)"""
    if btc_price > btc_ema50 * 1.03:
        return "🟢 상승장"
    elif btc_price < btc_ema50 * 0.97:
        return "🔴 하락장"
    else:
        return "🟡 횡보장"


def build_dashboard_blocks(config, hinokami_state, nite_state, btc_info):
    """현황판 블록 생성"""
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")

    # 히노카미 통계
    h_equity = hinokami_state.get("_equity", 300)
    h_pnl = hinokami_state.get("total_pnl", 0)
    h_trades = hinokami_state.get("total_trades", 0)
    h_wins = hinokami_state.get("total_wins", 0)
    h_wr = (h_wins / h_trades * 100) if h_trades > 0 else 0
    h_pos = len(hinokami_state.get("positions", {}))
    h_cooldown = hinokami_state.get("cooldown_until")
    h_consec = hinokami_state.get("consecutive_losses", 0)

    # 나이트 통계
    n_equity = nite_state.get("_equity", 300)
    n_pnl = nite_state.get("total_pnl", 0)
    n_trades = nite_state.get("total_trades", 0)
    n_wins = nite_state.get("total_wins", 0)
    n_wr = (n_wins / n_trades * 100) if n_trades > 0 else 0
    n_pos = len(nite_state.get("positions", {}))
    n_cooldown = nite_state.get("cooldown_until")
    n_consec = nite_state.get("consecutive_losses", 0)

    # 합산
    total_equity = h_equity + n_equity
    total_pnl = h_pnl + n_pnl
    total_trades = h_trades + n_trades
    total_wins = h_wins + n_wins
    total_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0

    blocks = []

    # 헤더
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": f"⏰ 마지막 업데이트: {now}"}}
    ]}})
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": f"₿ BTC: ${btc_info['price']:,.2f} | {btc_info['market_state']}"}}
    ]}})

    # 구분선
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"}}
    ]}})

    # 합산
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": f"📊 전체 합산 | Equity: ${total_equity:,.2f} | PnL: ${total_pnl:+.2f} | 거래: {total_trades}건 | 승률: {total_wr:.1f}%"}}
    ]}})

    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"}}
    ]}})

    # 히노카미
    h_cd_str = f" | 🧊 쿨다운 ({h_cooldown[:16]})" if h_cooldown else ""
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": f"🔥 히노카미 (롱) | Equity: ${h_equity:,.2f} | PnL: ${h_pnl:+.2f} | 거래: {h_trades}건 | 승률: {h_wr:.1f}% | 포지션: {h_pos}개 | 연속손실: {h_consec}회{h_cd_str}"}}
    ]}})

    # 히노카미 활성 포지션
    for sym, pos in hinokami_state.get("positions", {}).items():
        entry = pos.get("entry_price", 0)
        sl = pos.get("trailing_stop", 0)
        blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
            {"type": "text", "text": {"content": f"  📈 {sym} 롱 | 진입: ${entry:.2f} | SL: ${sl:.2f} | {pos.get('entry_time', '')[:16]}"}}
        ]}})

    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"}}
    ]}})

    # 나이트
    n_cd_str = f" | 🧊 쿨다운 ({n_cooldown[:16]})" if n_cooldown else ""
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": f"🌙 나이트 (숏) | Equity: ${n_equity:,.2f} | PnL: ${n_pnl:+.2f} | 거래: {n_trades}건 | 승률: {n_wr:.1f}% | 포지션: {n_pos}개 | 연속손실: {n_consec}회{n_cd_str}"}}
    ]}})

    # 나이트 활성 포지션
    for sym, pos in nite_state.get("positions", {}).items():
        entry = pos.get("entry_price", 0)
        sl = pos.get("trailing_stop", 0)
        blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
            {"type": "text", "text": {"content": f"  📉 {sym} 숏 | 진입: ${entry:.2f} | SL: ${sl:.2f} | {pos.get('entry_time', '')[:16]}"}}
        ]}})

    # ━━━ 엔진 건강 섹션 ━━━
    blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
        {"type": "text", "text": {"content": "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"}}
    ]}})

    for eng_name, eng_label, interval_min in [("hinokami", "🔥 히노카미", 240), ("nite", "🌙 나이트", 120)]:
        hb_path = BASE_DIR / f"heartbeat_{eng_name}.json"
        if hb_path.exists():
            hb = json.loads(hb_path.read_text())
            last_dt = datetime.fromisoformat(hb["timestamp"])
            elapsed = (datetime.now(KST) - last_dt).total_seconds() / 60
            if elapsed <= interval_min * 1.5:
                status = "🟢 정상"
            elif elapsed <= interval_min * 2.5:
                status = "🟡 주의"
            else:
                status = "🔴 장애"
            elapsed_str = f"{int(elapsed)}분 전"
        else:
            status = "🔴 장애"
            elapsed_str = "heartbeat 없음"

        summary = get_daily_scan_summary(eng_name)
        if summary:
            scan_str = f"{summary['total_scans']}스캔 | ST:{summary['total_st_signals']} → RSI:{summary['total_rsi_pass']} → 진입:{summary['total_entered']}"
            miss_str = ""
            if summary.get("best_near_miss"):
                nm = summary["best_near_miss"]
                miss_str = f" | Near-miss: {nm['symbol']} ({nm['failed_at']} {nm['value']}/{nm['threshold']})"
        else:
            scan_str = "데이터 없음"
            miss_str = ""

        blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
            {"type": "text", "text": {"content": f"{eng_label}: {status} | 마지막 스캔: {elapsed_str}"}}
        ]}})
        blocks.append({"type": "paragraph", "paragraph": {"rich_text": [
            {"type": "text", "text": {"content": f"  어제: {scan_str}{miss_str}"}}
        ]}})

    return blocks


def update_notion_dashboard(config, blocks):
    """노션 현황판 페이지 업데이트 (기존 블록 삭제 후 재생성)"""
    token = config["notion"]["token"]
    page_id = config["notion"]["dashboard_page_id"]

    # 기존 블록 삭제
    existing = notion_request("GET", f"/blocks/{page_id}/children?page_size=100", token)
    if existing and existing.get("results"):
        for block in existing["results"]:
            notion_request("DELETE", f"/blocks/{block['id']}", token)
            time.sleep(0.1)

    # 새 블록 추가
    if blocks:
        # 100개 제한이므로 나눠서
        for i in range(0, len(blocks), 100):
            chunk = blocks[i:i+100]
            notion_request("PATCH", f"/blocks/{page_id}/children", token, {"children": chunk})

    return True


def send_daily_summary(config):
    """일일 요약 텔레그램 발송"""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    lines = [f"📊 <b>MAGI v3.0 일일 요약</b> ({yesterday})\n"]

    for eng_name, eng_label in [("hinokami", "🔥 히노카미 (롱)"), ("nite", "🌙 나이트 (숏)")]:
        summary = get_daily_scan_summary(eng_name, yesterday)
        if summary:
            lines.append(f"{eng_label}")
            lines.append(f"  스캔: {summary['total_scans']}회 ✅")
            lines.append(f"  ST전환: {summary['total_st_signals']} | RSI통과: {summary['total_rsi_pass']} | 진입: {summary['total_entered']}")
            if summary.get("best_near_miss"):
                nm = summary["best_near_miss"]
                lines.append(f"  Near-miss: {nm['symbol']} ({nm['failed_at']} {nm['value']}/{nm['threshold']})")
            else:
                lines.append(f"  Near-miss: 없음")
            if summary["total_api_errors"] > 0:
                lines.append(f"  ⚠️ API 오류: {summary['total_api_errors']}건")
            lines.append("")
        else:
            lines.append(f"{eng_label}")
            lines.append(f"  ❌ 스캔 데이터 없음!\n")

    all_ok = all((BASE_DIR / f"heartbeat_{e}.json").exists() for e in ["hinokami", "nite"])
    lines.append(f"시스템: {'🟢 정상' if all_ok else '🔴 점검 필요'}")
    send_telegram(config, "\n".join(lines))
    logger.info("📊 일일 요약 텔레그램 발송 완료")


def main():
    config = load_config()
    now_kst = datetime.now(KST)

    # 매일 09:00~09:04 KST 사이에 일일 요약 전송
    if now_kst.hour == 9 and now_kst.minute < 10:
        send_daily_summary(config)

    logger.info("📋 현황판 업데이트 시작")

    try:
        # 히노카미 클라이언트로 BTC 가격 조회
        h_config = config["engines"]["hinokami"]
        h_client = BinanceClient(h_config["api_key"], h_config["api_secret"], logger)

        # BTC 정보
        btc_ticker = h_client.get_ticker_price("BTCUSDT")
        btc_price = float(btc_ticker["price"]) if btc_ticker else 0

        btc_klines = h_client.get_klines("BTCUSDT", "1d", limit=60)
        btc_ema50 = calc_ema(btc_klines["close"], 50).iloc[-1] if len(btc_klines) >= 50 else btc_price

        btc_info = {
            "price": btc_price,
            "market_state": get_market_state(btc_price, btc_ema50),
        }

        # 엔진 상태 로드 + equity 조회
        h_state = load_state("hinokami")
        n_state = load_state("nite")

        h_equity = h_client.get_equity()
        h_state["_equity"] = h_equity

        n_config = config["engines"]["nite"]
        n_client = BinanceClient(n_config["api_key"], n_config["api_secret"], logger)
        n_equity = n_client.get_equity()
        n_state["_equity"] = n_equity

        # 블록 생성 + 업데이트
        blocks = build_dashboard_blocks(config, h_state, n_state, btc_info)
        update_notion_dashboard(config, blocks)

        write_heartbeat("dashboard")
        logger.info(f"📋 현황판 업데이트 완료 | H:${h_equity:.2f} N:${n_equity:.2f} BTC:${btc_price:,.2f}")

    except Exception as e:
        logger.error(f"현황판 에러: {e}\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
