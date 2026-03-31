"""
MAGI v3.0 — 켈트너 Phase 1 Baseline BT 실행
설계서 v1.1 파라미터 그대로, 한 글자도 안 바꿈
IS 9개월 + OOS 3개월 분리
"""
import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta

from bt_keltner_engine import (
    KeltnerConfig, run_keltner_backtest,
    analyze_keltner_results, print_keltner_results
)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bt_data")


def load_keltner_data():
    """1H + 4H 데이터 로드"""
    meta_path = f"{DATA_DIR}/symbols_meta.json"
    if not os.path.exists(meta_path):
        print("❌ 데이터 없음! bt_data_fetch.py를 먼저 실행하세요.")
        sys.exit(1)

    with open(meta_path) as f:
        symbols_meta = json.load(f)

    symbols_data = {}
    loaded = 0
    skipped = []

    for sym_info in symbols_meta:
        symbol = sym_info['symbol']
        safe_name = symbol.replace('/', '_').replace(':', '_')

        path_1h = f"{DATA_DIR}/{safe_name}_1h.csv"
        path_4h = f"{DATA_DIR}/{safe_name}_4h.csv"

        if not os.path.exists(path_1h):
            skipped.append(f"{symbol} (1H 없음)")
            continue
        if not os.path.exists(path_4h):
            skipped.append(f"{symbol} (4H 없음)")
            continue

        df_1h = pd.read_csv(path_1h, parse_dates=['timestamp'])
        df_4h = pd.read_csv(path_4h, parse_dates=['timestamp'])

        if len(df_1h) < 200:  # 최소 데이터 요건 (워밍업 + 충분한 거래 기간)
            skipped.append(f"{symbol} (1H {len(df_1h)}봉 부족)")
            continue

        symbols_data[symbol] = {'1h': df_1h, '4h': df_4h}
        loaded += 1

    print(f"✅ {loaded}개 종목 데이터 로드 완료 (1H + 4H)")
    if skipped:
        print(f"⚠️ 스킵: {', '.join(skipped[:5])}{'...' if len(skipped) > 5 else ''}")
    return symbols_data


def determine_is_oos_split(symbols_data):
    """IS/OOS 기간 자동 결정 — 다수 종목 기준 (신규 상장 종목에 끌려가지 않음)"""
    min_dates = []
    max_dates = []
    for symbol, data in symbols_data.items():
        df = data['1h']
        min_dates.append(df['timestamp'].min())
        max_dates.append(df['timestamp'].max())

    # 중앙값 기준으로 시작일 결정 (소수 신규 상장 종목에 끌려가지 않도록)
    sorted_mins = sorted(min_dates)
    # 전체 종목의 70% 이상이 커버하는 시작일 사용
    coverage_idx = int(len(sorted_mins) * 0.3)  # 상위 30% 제외
    robust_start = sorted_mins[coverage_idx]
    common_end = min(max_dates)
    total_days = (common_end - robust_start).days

    # IS: 처음 75% (약 9개월), OOS: 나머지 25% (약 3개월)
    is_days = int(total_days * 0.75)
    is_end = robust_start + timedelta(days=is_days)

    # 이 기간을 커버하는 종목 수 확인
    covered = sum(1 for d in min_dates if d <= robust_start)
    total = len(min_dates)

    print(f"\n📅 IS/OOS 분할")
    print(f"  전체: {robust_start.strftime('%Y-%m-%d')} ~ {common_end.strftime('%Y-%m-%d')} ({total_days}일)")
    print(f"  IS:   {robust_start.strftime('%Y-%m-%d')} ~ {is_end.strftime('%Y-%m-%d')} ({is_days}일)")
    print(f"  OOS:  {is_end.strftime('%Y-%m-%d')} ~ {common_end.strftime('%Y-%m-%d')} ({total_days - is_days}일)")
    print(f"  커버 종목: {covered}/{total}개 (시작일 기준)")

    return robust_start, is_end, common_end


def run_verification_checks(symbols_data, config):
    """부메랑 검증 체크리스트 (3개)"""
    print(f"\n{'='*65}")
    print(f"  🔍 검증 체크리스트 (부메랑 제안)")
    print(f"{'='*65}")

    # 아무 종목이나 하나 잡아서 스팟체크
    symbol = list(symbols_data.keys())[0]
    df = symbols_data[symbol]['1h'].copy()
    df_4h = symbols_data[symbol]['4h'].copy()

    from bt_keltner_engine import (
        calc_ema, calc_atr, calc_keltner_channel,
        calc_macd, calc_supertrend
    )

    # 체크 1: EMA, ATR, MACD 계산값
    ema = calc_ema(df['close'], config.KC_EMA_PERIOD)
    atr = calc_atr(df, config.ATR_PERIOD)
    _, _, histogram = calc_macd(df, config.MACD_FAST, config.MACD_SLOW, config.MACD_SIGNAL)

    # 마지막 5봉 출력
    print(f"\n  ✅ 체크1: 지표 스팟체크 ({symbol} 마지막 5봉)")
    print(f"  {'타임스탬프':<22} {'Close':>10} {'EMA(20)':>10} {'ATR(10)':>10} {'MACD Hist':>10}")
    for i in range(-5, 0):
        ts = df['timestamp'].iloc[i].strftime('%Y-%m-%d %H:%M')
        c = df['close'].iloc[i]
        e = ema.iloc[i]
        a = atr.iloc[i]
        h = histogram.iloc[i]
        print(f"  {ts:<22} {c:>10.2f} {e:>10.2f} {a:>10.2f} {h:>10.4f}")
    print(f"  → 트레이딩뷰에서 {symbol} 1H 차트와 대조해주세요")

    # 체크 2: 4H SuperTrend 필터
    _, dir_4h = calc_supertrend(df_4h, config.HTF_ATR_PERIOD, config.HTF_ATR_MULT)
    last_dir = dir_4h.iloc[-1]
    last_ts = df_4h['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M')
    print(f"\n  ✅ 체크2: 4H SuperTrend 필터")
    print(f"  {symbol} 마지막 4H: {last_ts}")
    print(f"  방향: {'상승 (1)' if last_dir == 1 else '하락 (-1)'}")
    print(f"  → 롱 진입 시 방향=1 이어야 함, 숏 진입 시 방향=-1 이어야 함")

    # 체크 3: 수수료 왕복 차감
    fee_rate = config.TAKER_FEE + config.SLIPPAGE
    roundtrip = fee_rate * 2  # 진입+청산
    test_pos = 100  # $100 포지션
    test_fee = test_pos * roundtrip
    print(f"\n  ✅ 체크3: 수수료 왕복 차감")
    print(f"  편도 수수료: {config.TAKER_FEE*100:.2f}% + 슬리피지: {config.SLIPPAGE*100:.2f}% = {fee_rate*100:.2f}%")
    print(f"  왕복: {roundtrip*100:.2f}%")
    print(f"  $100 포지션 예시: 왕복 수수료 ${test_fee:.2f}")

    print(f"\n{'='*65}")


def main():
    symbols_data = load_keltner_data()

    if not symbols_data:
        print("❌ 로드된 데이터 없음!")
        sys.exit(1)

    config = KeltnerConfig()

    # IS/OOS 기간 결정
    is_start, is_end, oos_end = determine_is_oos_split(symbols_data)

    # 검증 체크리스트 실행
    run_verification_checks(symbols_data, config)

    # ============================================
    # IS (In-Sample) 백테스트
    # ============================================
    print(f"\n{'='*65}")
    print(f"  Phase 1-A: IS (In-Sample) 백테스트")
    print(f"{'='*65}")

    engine_is = run_keltner_backtest(
        symbols_data, config,
        start_date=is_start, end_date=is_end
    )
    results_is = analyze_keltner_results(engine_is, config)
    print_keltner_results(results_is, "[IS]")

    # ============================================
    # OOS (Out-of-Sample) 백테스트
    # ============================================
    print(f"\n{'='*65}")
    print(f"  Phase 1-B: OOS (Out-of-Sample) 백테스트")
    print(f"{'='*65}")

    # OOS는 자본 리셋 ($300)
    engine_oos = run_keltner_backtest(
        symbols_data, config,
        start_date=is_end, end_date=oos_end
    )
    results_oos = analyze_keltner_results(engine_oos, config)
    print_keltner_results(results_oos, "[OOS]")

    # ============================================
    # IS vs OOS 비교
    # ============================================
    print(f"\n{'='*65}")
    print(f"  📊 IS vs OOS 비교")
    print(f"{'='*65}")

    if results_is and results_oos:
        comparison = [
            ("총 거래수", f"{results_is['total_trades']}", f"{results_oos['total_trades']}"),
            ("PF", f"{results_is['profit_factor']:.2f}", f"{results_oos['profit_factor']:.2f}"),
            ("승률", f"{results_is['win_rate']:.1f}%", f"{results_oos['win_rate']:.1f}%"),
            ("총 PnL", f"${results_is['total_pnl']:.2f}", f"${results_oos['total_pnl']:.2f}"),
            ("MDD", f"{results_is['max_drawdown_pct']:.1f}%", f"{results_oos['max_drawdown_pct']:.1f}%"),
            ("R:R", f"1:{results_is['rr_ratio']:.2f}", f"1:{results_oos['rr_ratio']:.2f}"),
            ("평균보유", f"{results_is['avg_hold_all']:.0f}H", f"{results_oos['avg_hold_all']:.0f}H"),
            ("연속패배", f"{results_is['max_consecutive_losses']}회", f"{results_oos['max_consecutive_losses']}회"),
            ("롱 PF", f"{results_is['long_stats']['pf']:.2f}", f"{results_oos['long_stats']['pf']:.2f}"),
            ("숏 PF", f"{results_is['short_stats']['pf']:.2f}", f"{results_oos['short_stats']['pf']:.2f}"),
        ]

        print(f"\n  {'항목':<14} {'IS':>12} {'OOS':>12}")
        print(f"  {'-'*38}")
        for label, v_is, v_oos in comparison:
            print(f"  {label:<14} {v_is:>12} {v_oos:>12}")

    # 결과 저장
    save_results(results_is, results_oos, engine_is, engine_oos)


def save_results(results_is, results_oos, engine_is, engine_oos):
    """결과 파일 저장"""
    # JSON 요약
    all_results = {
        'is': results_is,
        'oos': results_oos,
    }

    # monthly_pnl dict 변환
    for key in all_results:
        if all_results[key] and 'monthly_pnl' in all_results[key]:
            all_results[key]['monthly_pnl'] = dict(all_results[key]['monthly_pnl'])

    with open(f"{DATA_DIR}/keltner_baseline_results.json", 'w') as f:
        json.dump(all_results, f, indent=2, default=str)

    # Equity curve
    if engine_is.equity_curve:
        eq_is = pd.DataFrame(engine_is.equity_curve)
        eq_is.to_csv(f"{DATA_DIR}/equity_keltner_is.csv", index=False)

    if engine_oos.equity_curve:
        eq_oos = pd.DataFrame(engine_oos.equity_curve)
        eq_oos.to_csv(f"{DATA_DIR}/equity_keltner_oos.csv", index=False)

    # 트레이드 로그
    def trades_to_df(trades):
        return pd.DataFrame([{
            'symbol': t.symbol, 'side': t.side,
            'entry_price': t.entry_price, 'exit_price': t.exit_price,
            'signal_time': str(t.signal_time) if t.signal_time else '',
            'entry_time': str(t.entry_time), 'exit_time': str(t.exit_time),
            'pnl_usd': t.pnl_usd, 'pnl_pct': t.pnl_pct,
            'exit_reason': t.exit_reason, 'hold_hours': t.hold_hours,
            'atr_at_entry': t.atr_at_entry,
            'funding_fee': t.funding_fee,
        } for t in trades])

    if engine_is.trades:
        trades_to_df(engine_is.trades).to_csv(f"{DATA_DIR}/trades_keltner_is.csv", index=False)
    if engine_oos.trades:
        trades_to_df(engine_oos.trades).to_csv(f"{DATA_DIR}/trades_keltner_oos.csv", index=False)

    print(f"\n✅ 결과 저장 완료: {DATA_DIR}/")
    print(f"  - keltner_baseline_results.json (IS+OOS 요약)")
    print(f"  - equity_keltner_is/oos.csv (자본곡선)")
    print(f"  - trades_keltner_is/oos.csv (거래 로그)")


if __name__ == '__main__':
    main()
