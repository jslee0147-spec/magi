"""
MAGI v3.0 — 켈트너(Keltner) Channel 돌파 + Momentum 백테스트 엔진
설계서 v1.1 기준 완전 구현
Phase 1: Baseline BT — 파라미터 한 글자도 안 바꿈
"""
import pandas as pd
import numpy as np
import json
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
from datetime import timedelta

# ============================
# 설계서 v1.1 파라미터
# ============================
@dataclass
class KeltnerConfig:
    # === Keltner Channel ===
    KC_EMA_PERIOD: int = 20          # EMA 기간
    ATR_PERIOD: int = 10             # ATR 기간 (히노카미/나이트 통일)
    KC_ATR_MULT: float = 1.5         # KC 밴드 배수

    # === MACD ===
    MACD_FAST: int = 12
    MACD_SLOW: int = 26
    MACD_SIGNAL: int = 9

    # === 거래량 ===
    VOLUME_MA_PERIOD: int = 20
    VOLUME_MULT: float = 1.5         # 거래량 필터 배수

    # === HTF 필터 ===
    HTF_ATR_PERIOD: int = 10
    HTF_ATR_MULT: float = 3.0        # 4H SuperTrend

    # === 돌파 확인 ===
    BREAKOUT_PERIOD: int = 20        # 20기간 신고/저점

    # === 포지션 ===
    MAX_POSITIONS: int = 3
    MIN_POSITION_USD: float = 5.0    # 바이낸스 최소 주문액
    LEVERAGE: int = 5

    # === 리스크 ===
    RISK_PER_TRADE: float = 0.015    # 고정 1.5%
    DAILY_MAX_LOSS: float = 0.05     # -5% DD
    WEEKLY_MAX_LOSS: float = 0.10    # -10% DD

    # === 청산 ===
    SL_ATR_MULT: float = 2.5         # 초기 SL: ATR × 2.5
    BE_ATR_TRIGGER: float = 0.6      # BE 활성화: ATR × 0.6 유리 이동
    TRAILING_ATR_MULT: float = 2.0   # 트레일링: ATR × 2.0
    MAX_HOLD_HOURS: int = 48         # 최대 보유 48시간 (48봉)

    # === 쿨다운 ===
    COOLDOWN_HOURS: int = 4          # 손절 후 4시간
    COOLDOWN_LOSSES: int = 3         # 연속 3패 시 쿨다운
    SAME_SYMBOL_COOLDOWN: int = 4    # 같은 심볼 재진입 4시간

    # === 포지션 제한 ===
    MAX_LONG: int = 2                # 최대 롱 동시 2개
    MAX_SHORT: int = 2               # 최대 숏 동시 2개

    # === 비용 ===
    TAKER_FEE: float = 0.0005       # 0.05% (편도)
    SLIPPAGE: float = 0.0002         # 0.02%

    # === 초기 자본 ===
    INITIAL_CAPITAL: float = 300.0


# ============================
# 기술적 지표 계산
# ============================
def calc_ema(series, period):
    """EMA 계산"""
    return series.ewm(span=period, adjust=False).mean()

def calc_atr(df, period=10):
    """ATR 계산"""
    high = df['high']
    low = df['low']
    close = df['close'].shift(1)
    tr1 = high - low
    tr2 = (high - close).abs()
    tr3 = (low - close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    return atr

def calc_keltner_channel(df, ema_period=20, atr_period=10, atr_mult=1.5):
    """Keltner Channel 계산"""
    ema = calc_ema(df['close'], ema_period)
    atr = calc_atr(df, atr_period)
    upper = ema + (atr * atr_mult)
    lower = ema - (atr * atr_mult)
    return ema, upper, lower, atr

def calc_macd(df, fast=12, slow=26, signal=9):
    """MACD 계산"""
    ema_fast = calc_ema(df['close'], fast)
    ema_slow = calc_ema(df['close'], slow)
    macd_line = ema_fast - ema_slow
    signal_line = calc_ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calc_supertrend(df, period=10, multiplier=3.0):
    """SuperTrend 계산 (HTF 필터용)"""
    atr = calc_atr(df, period)
    hl2 = (df['high'] + df['low']) / 2
    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)

    supertrend = pd.Series(np.nan, index=df.index)
    direction = pd.Series(1, index=df.index)

    for i in range(1, len(df)):
        if lower_band.iloc[i] > lower_band.iloc[i-1] or df['close'].iloc[i-1] < lower_band.iloc[i-1]:
            pass
        else:
            lower_band.iloc[i] = lower_band.iloc[i-1]

        if upper_band.iloc[i] < upper_band.iloc[i-1] or df['close'].iloc[i-1] > upper_band.iloc[i-1]:
            pass
        else:
            upper_band.iloc[i] = upper_band.iloc[i-1]

        if pd.isna(supertrend.iloc[i-1]):
            direction.iloc[i] = 1
        elif supertrend.iloc[i-1] == upper_band.iloc[i-1]:
            direction.iloc[i] = -1 if df['close'].iloc[i] <= upper_band.iloc[i] else 1
        else:
            direction.iloc[i] = 1 if df['close'].iloc[i] >= lower_band.iloc[i] else -1

        supertrend.iloc[i] = lower_band.iloc[i] if direction.iloc[i] == 1 else upper_band.iloc[i]

    return supertrend, direction


# ============================
# 포지션 & 트레이드
# ============================
@dataclass
class Position:
    symbol: str
    side: str           # 'long' or 'short'
    entry_price: float
    entry_time: pd.Timestamp
    position_size_usd: float
    atr_at_entry: float

    # SL 관련
    initial_sl: float = 0.0
    trailing_sl: float = 0.0
    be_price: Optional[float] = None
    be_activated: bool = False

    # 추적
    highest_price: float = 0.0
    lowest_price: float = float('inf')
    _signal_time: Optional[pd.Timestamp] = None  # 신호 발생 시각

    def __post_init__(self):
        if self.side == 'long':
            self.initial_sl = self.entry_price - (self.atr_at_entry * 2.5)  # ATR × 2.5
            self.trailing_sl = self.initial_sl
            self.highest_price = self.entry_price
        else:
            self.initial_sl = self.entry_price + (self.atr_at_entry * 2.5)
            self.trailing_sl = self.initial_sl
            self.lowest_price = self.entry_price


@dataclass
class Trade:
    symbol: str
    side: str
    entry_price: float
    exit_price: float
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    position_size_usd: float
    pnl_usd: float
    pnl_pct: float
    exit_reason: str
    atr_at_entry: float
    hold_hours: float
    funding_fee: float = 0.0       # 펀딩비 (수정2 추가)
    signal_time: Optional[pd.Timestamp] = None  # 신호 발생 봉 시각 (수정1 추가)


# ============================
# 퍼널 추적기 (5단계)
# ============================
@dataclass
class FunnelTracker:
    """5단계 퍼널 통과율 추적"""
    total_candles: int = 0       # 전체 스캔한 캔들 수
    kc_breakout: int = 0         # 1단계: KC 돌파 + 20기간 신고/저점
    macd_pass: int = 0           # 2단계: MACD 히스토그램 방향 일치
    volume_pass: int = 0         # 3단계: 거래량 필터
    htf_pass: int = 0            # 4단계: 4H SuperTrend 방향 일치
    entered: int = 0             # 5단계: 실제 진입 (슬롯/쿨다운 통과)


# ============================
# 백테스트 엔진
# ============================
class KeltnerBacktestEngine:
    def __init__(self, config: KeltnerConfig):
        self.config = config
        self.capital = config.INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_curve = []
        self.daily_pnl = 0.0
        self.weekly_pnl = 0.0
        self.consecutive_losses = 0
        self.cooldown_until = None
        self.symbol_cooldown: Dict[str, pd.Timestamp] = {}  # 심볼별 쿨다운
        self.last_day = None
        self.last_week = None
        self.funnel = FunnelTracker()

    def calc_position_size(self, entry_price, atr):
        """동적 포지션 사이징 (v1.1 — 레버리지 반영)"""
        # 리스크 금액 = 자본 × 1.5%
        risk_amount = self.capital * self.config.RISK_PER_TRADE

        # SL 거리 (%) = ATR × 2.5 / 진입가
        sl_distance_pct = (atr * self.config.SL_ATR_MULT) / entry_price

        # 포지션 = 리스크 / (SL거리% × 레버리지)
        denom = sl_distance_pct * self.config.LEVERAGE
        if denom <= 0:
            return 0

        position_usd = risk_amount / denom

        # 마진 체크: 포지션/레버리지 ≤ 가용 자본
        margin_required = position_usd / self.config.LEVERAGE
        available_margin = self.capital - sum(
            p.position_size_usd / self.config.LEVERAGE
            for p in self.positions.values()
        )
        if margin_required > available_margin:
            position_usd = available_margin * self.config.LEVERAGE

        # 최소 주문액 체크
        if position_usd < self.config.MIN_POSITION_USD:
            return 0

        return position_usd

    def apply_fees(self, notional_value):
        """수수료 + 슬리피지 (편도)"""
        fee_rate = self.config.TAKER_FEE + self.config.SLIPPAGE
        return notional_value * fee_rate

    def open_position(self, symbol, side, entry_price, atr, timestamp):
        """포지션 오픈"""
        pos_size = self.calc_position_size(entry_price, atr)
        if pos_size <= 0:
            return False

        # 진입 수수료 차감 (레버리지 적용 노셔널 기준)
        notional = pos_size  # pos_size가 이미 노셔널
        fee = self.apply_fees(notional)
        self.capital -= fee

        pos = Position(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            entry_time=timestamp,
            position_size_usd=pos_size,
            atr_at_entry=atr,
        )

        self.positions[symbol] = pos
        self.funnel.entered += 1
        return True

    def close_position(self, symbol, exit_price, timestamp, reason, signal_time=None):
        """포지션 클로즈"""
        pos = self.positions[symbol]

        # PnL 계산
        if pos.side == 'long':
            pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - exit_price) / pos.entry_price

        pnl_leveraged = pnl_pct * self.config.LEVERAGE
        pnl_usd = pos.position_size_usd * pnl_pct * self.config.LEVERAGE

        # 청산 수수료 차감
        fee = self.apply_fees(pos.position_size_usd)
        pnl_usd -= fee

        # 펀딩비 차감 (수정2: 바이낸스 8시간 주기, 기본 0.015%)
        hold_hours = (timestamp - pos.entry_time).total_seconds() / 3600
        funding_cycles = hold_hours / 8.0
        funding_fee = pos.position_size_usd * 0.00015 * funding_cycles
        pnl_usd -= funding_fee

        self.capital += pnl_usd

        trade = Trade(
            symbol=symbol,
            side=pos.side,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            entry_time=pos.entry_time,
            exit_time=timestamp,
            position_size_usd=pos.position_size_usd,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_leveraged,
            exit_reason=reason,
            atr_at_entry=pos.atr_at_entry,
            hold_hours=hold_hours,
            funding_fee=funding_fee,
            signal_time=signal_time,
        )
        self.trades.append(trade)

        # 연속 손실 추적
        if pnl_usd < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        # 일일/주간 PnL
        self.daily_pnl += pnl_usd
        self.weekly_pnl += pnl_usd

        # 심볼 쿨다운 설정
        if pnl_usd < 0:
            self.symbol_cooldown[symbol] = timestamp + timedelta(hours=self.config.SAME_SYMBOL_COOLDOWN)

        del self.positions[symbol]
        return trade

    def update_trailing_stop(self, pos: Position, candle, current_atr):
        """트레일링 스톱 업데이트 — 봉 마감(종가) 기준, 스텝 없음 (v1.1)"""
        if pos.side == 'long':
            # 최고가 갱신
            if candle['high'] > pos.highest_price:
                pos.highest_price = candle['high']

            # 트레일링 SL: 최고가 - ATR × 2.0
            new_trailing = pos.highest_price - (current_atr * self.config.TRAILING_ATR_MULT)
            pos.trailing_sl = max(pos.trailing_sl, new_trailing)

            # BE 체크: 가격이 ATR × 0.6 이상 유리하게 이동
            profit_amount = candle['high'] - pos.entry_price
            be_trigger = pos.atr_at_entry * self.config.BE_ATR_TRIGGER

            if not pos.be_activated and profit_amount >= be_trigger:
                pos.be_activated = True
                pos.be_price = pos.entry_price  # 진입가로 SL 이동

            # SL 우선순위: SL = MAX(BE, Trailing) — v1.1
            if pos.be_activated and pos.be_price is not None:
                pos.trailing_sl = max(pos.trailing_sl, pos.be_price)

        else:  # short
            if candle['low'] < pos.lowest_price:
                pos.lowest_price = candle['low']

            new_trailing = pos.lowest_price + (current_atr * self.config.TRAILING_ATR_MULT)
            pos.trailing_sl = min(pos.trailing_sl, new_trailing)

            profit_amount = pos.entry_price - candle['low']
            be_trigger = pos.atr_at_entry * self.config.BE_ATR_TRIGGER

            if not pos.be_activated and profit_amount >= be_trigger:
                pos.be_activated = True
                pos.be_price = pos.entry_price

            if pos.be_activated and pos.be_price is not None:
                pos.trailing_sl = min(pos.trailing_sl, pos.be_price)

    def check_exit(self, pos: Position, candle, current_atr, timestamp):
        """청산 조건 체크 (우선순위 순서)"""
        close = candle['close']
        low = candle['low']
        high = candle['high']
        hold_hours = (timestamp - pos.entry_time).total_seconds() / 3600

        if pos.side == 'long':
            # 0순위: 마진콜 (레버리지 포지션 -100% = 청산)
            margin_loss_pct = (low - pos.entry_price) / pos.entry_price * self.config.LEVERAGE
            if margin_loss_pct <= -0.95:  # 마진 95% 소진 시 강제 청산
                liquidation_price = pos.entry_price * (1 - 0.95 / self.config.LEVERAGE)
                return liquidation_price, "LIQUIDATION"

            # 1순위: 일일/주간 DD → run_backtest에서 처리
            # 2순위: 초기 SL 또는 Trailing SL
            effective_sl = max(pos.initial_sl, pos.trailing_sl)
            if low <= effective_sl:
                return effective_sl, "SL_HIT"

            # 3순위: 시간 기반 청산 (48시간)
            if hold_hours >= self.config.MAX_HOLD_HOURS:
                return close, "TIME_STOP_48H"

        else:  # short
            margin_loss_pct = (pos.entry_price - high) / pos.entry_price * self.config.LEVERAGE
            if margin_loss_pct <= -0.95:
                liquidation_price = pos.entry_price * (1 + 0.95 / self.config.LEVERAGE)
                return liquidation_price, "LIQUIDATION"

            effective_sl = min(pos.initial_sl, pos.trailing_sl)
            if high >= effective_sl:
                return effective_sl, "SL_HIT"

            if hold_hours >= self.config.MAX_HOLD_HOURS:
                return close, "TIME_STOP_48H"

        return None, None

    def check_cooldown(self, timestamp):
        """쿨다운 체크"""
        # 글로벌 쿨다운
        if self.cooldown_until:
            if timestamp < self.cooldown_until:
                return True
            else:
                # 쿨다운 종료 → consecutive_losses 리셋
                self.cooldown_until = None
                self.consecutive_losses = 0

        # 연속 3패 쿨다운
        if self.consecutive_losses >= self.config.COOLDOWN_LOSSES:
            self.cooldown_until = timestamp + timedelta(hours=self.config.COOLDOWN_HOURS)
            return True

        # 일일 DD 체크
        if abs(self.daily_pnl) >= self.capital * self.config.DAILY_MAX_LOSS:
            next_day = timestamp.normalize() + timedelta(days=1, hours=0)
            self.cooldown_until = next_day
            return True

        return False

    def check_symbol_cooldown(self, symbol, timestamp):
        """심볼별 쿨다운 체크"""
        if symbol in self.symbol_cooldown:
            if timestamp < self.symbol_cooldown[symbol]:
                return True
        return False

    def reset_daily_weekly(self, timestamp):
        """일일/주간 리셋"""
        current_day = timestamp.date()
        current_week = timestamp.isocalendar()[1]

        if self.last_day != current_day:
            self.daily_pnl = 0.0
            self.last_day = current_day

        if self.last_week != current_week:
            self.weekly_pnl = 0.0
            self.last_week = current_week


def run_keltner_backtest(symbols_data: Dict[str, Dict[str, pd.DataFrame]],
                          config: KeltnerConfig,
                          start_date=None, end_date=None):
    """
    켈트너 멀티 종목 백테스트 실행
    symbols_data: {symbol: {'1h': df_1h, '4h': df_4h}}
    """
    engine = KeltnerBacktestEngine(config)

    # === 각 종목별 지표 사전 계산 ===
    indicators_1h = {}
    htf_direction = {}  # 4H SuperTrend 방향

    for symbol, data in symbols_data.items():
        df = data['1h'].copy()
        df_4h = data['4h'].copy()

        # --- 1H 지표 ---
        ema, kc_upper, kc_lower, atr = calc_keltner_channel(
            df, config.KC_EMA_PERIOD, config.ATR_PERIOD, config.KC_ATR_MULT
        )
        df['kc_ema'] = ema
        df['kc_upper'] = kc_upper
        df['kc_lower'] = kc_lower
        df['atr'] = atr

        # MACD
        macd_line, signal_line, histogram = calc_macd(df, config.MACD_FAST, config.MACD_SLOW, config.MACD_SIGNAL)
        df['macd_hist'] = histogram
        df['macd_hist_prev'] = histogram.shift(1)

        # 거래량 MA
        df['volume_ma'] = df['volume'].rolling(window=config.VOLUME_MA_PERIOD).mean()

        # 20기간 신고/저점
        df['highest_20'] = df['high'].rolling(window=config.BREAKOUT_PERIOD).max()
        df['lowest_20'] = df['low'].rolling(window=config.BREAKOUT_PERIOD).min()
        # shift(1)로 현재봉 제외 (현재봉이 신고/저점을 만드는 것이어야 함)
        df['prev_highest_20'] = df['high'].shift(1).rolling(window=config.BREAKOUT_PERIOD).max()
        df['prev_lowest_20'] = df['low'].shift(1).rolling(window=config.BREAKOUT_PERIOD).min()

        indicators_1h[symbol] = df

        # --- 4H HTF 필터 ---
        st_4h, dir_4h = calc_supertrend(df_4h, config.HTF_ATR_PERIOD, config.HTF_ATR_MULT)
        df_4h['st_direction'] = dir_4h
        df_4h['ts'] = df_4h['timestamp']
        htf_direction[symbol] = df_4h[['ts', 'st_direction']].copy()

    # === 공통 타임라인 (1H) ===
    all_timestamps = set()
    for symbol, df in indicators_1h.items():
        all_timestamps.update(df['timestamp'].tolist())
    all_timestamps = sorted(all_timestamps)

    # 날짜 필터
    if start_date:
        all_timestamps = [t for t in all_timestamps if t >= pd.Timestamp(start_date)]
    if end_date:
        all_timestamps = [t for t in all_timestamps if t <= pd.Timestamp(end_date)]

    # 워밍업 (50봉)
    warmup = 50

    print(f"\n켈트너 백테스트 시작")
    print(f"  기간: {all_timestamps[warmup]} ~ {all_timestamps[-1]}")
    print(f"  종목: {len(symbols_data)}개")
    print(f"  초기 자본: ${config.INITIAL_CAPITAL}")
    print(f"  설정: MAX_POS={config.MAX_POSITIONS}, LEV={config.LEVERAGE}x, Risk={config.RISK_PER_TRADE*100}%")
    print(f"  KC: EMA({config.KC_EMA_PERIOD}) ± ATR({config.ATR_PERIOD})×{config.KC_ATR_MULT}")
    print(f"  MACD: ({config.MACD_FAST},{config.MACD_SLOW},{config.MACD_SIGNAL})")
    print(f"  SL: ATR×{config.SL_ATR_MULT} | Trailing: ATR×{config.TRAILING_ATR_MULT} | BE: ATR×{config.BE_ATR_TRIGGER}")
    print()

    def get_htf_direction(symbol, timestamp):
        """1H 타임스탬프에 대응하는 4H SuperTrend 방향 조회"""
        htf = htf_direction.get(symbol)
        if htf is None:
            return 0
        # 현재 시각 이전의 가장 최근 4H 캔들
        prior = htf[htf['ts'] <= timestamp]
        if prior.empty:
            return 0
        return prior.iloc[-1]['st_direction']

    # 수정1: 딜레이 큐 — t봉 신호 → t+1봉 시가 진입 (Look-ahead bias 제거)
    pending_entries = []  # 대기열: 전 봉에서 발생한 신호

    for t_idx, timestamp in enumerate(all_timestamps):
        if t_idx < warmup:
            continue

        engine.reset_daily_weekly(timestamp)

        # === 0단계: 대기열 소화 — 전 봉 신호를 현재 봉 시가(open)에 진입 ===
        if pending_entries and not engine.check_cooldown(timestamp):
            available_slots = config.MAX_POSITIONS - len(engine.positions)
            current_longs = sum(1 for p in engine.positions.values() if p.side == 'long')
            current_shorts = sum(1 for p in engine.positions.values() if p.side == 'short')

            for pending in pending_entries[:available_slots]:
                sym = pending['symbol']
                # 이미 포지션 있으면 스킵
                if sym in engine.positions:
                    continue
                # 심볼 쿨다운 체크
                if engine.check_symbol_cooldown(sym, timestamp):
                    continue
                # 방향 제한 체크
                if pending['side'] == 'long' and current_longs >= config.MAX_LONG:
                    continue
                if pending['side'] == 'short' and current_shorts >= config.MAX_SHORT:
                    continue

                # 현재 봉의 open 가격으로 진입
                df = indicators_1h.get(sym)
                if df is None:
                    continue
                row = df[df['timestamp'] == timestamp]
                if row.empty:
                    continue
                row = row.iloc[0]

                entry_price = row['open']  # t+1봉 시가 진입!
                entry_atr = row['atr']

                success = engine.open_position(
                    sym, pending['side'], entry_price,
                    entry_atr, timestamp
                )
                if success:
                    # signal_time 기록을 위해 포지션에 저장
                    if sym in engine.positions:
                        engine.positions[sym]._signal_time = pending['signal_time']
                    if pending['side'] == 'long':
                        current_longs += 1
                    else:
                        current_shorts += 1

            pending_entries = []  # 대기열 초기화

        # === 1단계: 기존 포지션 청산 체크 ===
        symbols_to_close = []

        # 일일/주간 DD 강제 청산 (1순위)
        if abs(engine.daily_pnl) >= engine.capital * config.DAILY_MAX_LOSS:
            for symbol in list(engine.positions.keys()):
                df = indicators_1h.get(symbol)
                if df is None:
                    continue
                row = df[df['timestamp'] == timestamp]
                if row.empty:
                    continue
                symbols_to_close.append((symbol, row.iloc[0]['close'], "DAILY_DD_FORCE"))

        elif abs(engine.weekly_pnl) >= engine.capital * config.WEEKLY_MAX_LOSS:
            for symbol in list(engine.positions.keys()):
                df = indicators_1h.get(symbol)
                if df is None:
                    continue
                row = df[df['timestamp'] == timestamp]
                if row.empty:
                    continue
                symbols_to_close.append((symbol, row.iloc[0]['close'], "WEEKLY_DD_FORCE"))

        else:
            for symbol, pos in list(engine.positions.items()):
                df = indicators_1h.get(symbol)
                if df is None:
                    continue
                row = df[df['timestamp'] == timestamp]
                if row.empty:
                    continue
                row = row.iloc[0]

                # 보수적 순서: exit 체크 먼저 (이전 봉까지의 trailing 기준)
                exit_price, reason = engine.check_exit(pos, row, row['atr'], timestamp)
                if exit_price is not None:
                    symbols_to_close.append((symbol, exit_price, reason))
                else:
                    # exit 안 됐을 때만 trailing 업데이트
                    engine.update_trailing_stop(pos, row, row['atr'])

        for symbol, exit_price, reason in symbols_to_close:
            if symbol in engine.positions:
                sig_time = getattr(engine.positions[symbol], '_signal_time', None)
                engine.close_position(symbol, exit_price, timestamp, reason,
                                      signal_time=sig_time)

        # === 2단계: 쿨다운 체크 ===
        if engine.check_cooldown(timestamp):
            engine.equity_curve.append({
                'timestamp': timestamp,
                'capital': engine.capital,
                'positions': len(engine.positions),
            })
            continue

        # === 3단계: 주간 손실 한도 ===
        if abs(engine.weekly_pnl) >= engine.capital * config.WEEKLY_MAX_LOSS:
            engine.equity_curve.append({
                'timestamp': timestamp,
                'capital': engine.capital,
                'positions': len(engine.positions),
            })
            continue

        # === 4단계: 신규 진입 신호 스캔 → 대기열에 저장 (진입 안 함!) ===
        candidates = []

        for symbol, df in indicators_1h.items():
            if symbol in engine.positions:
                continue
            if engine.check_symbol_cooldown(symbol, timestamp):
                continue

            row = df[df['timestamp'] == timestamp]
            if row.empty:
                continue
            row = row.iloc[0]

            # NaN 체크
            if pd.isna(row['atr']) or pd.isna(row['kc_upper']) or pd.isna(row['macd_hist']):
                continue
            if pd.isna(row['volume_ma']) or row['volume_ma'] <= 0:
                continue

            engine.funnel.total_candles += 1

            # ===== 퍼널 1단계: KC 돌파 + 20기간 신고/저점 =====
            is_long_breakout = False
            is_short_breakout = False

            if (row['close'] > row['kc_upper'] and
                not pd.isna(row.get('prev_highest_20', np.nan)) and
                row['high'] > row['prev_highest_20']):
                is_long_breakout = True

            if (row['close'] < row['kc_lower'] and
                not pd.isna(row.get('prev_lowest_20', np.nan)) and
                row['low'] < row['prev_lowest_20']):
                is_short_breakout = True

            if not is_long_breakout and not is_short_breakout:
                continue

            engine.funnel.kc_breakout += 1

            # ===== 퍼널 2단계: MACD 히스토그램 방향 =====
            macd_ok = False
            side = None

            if is_long_breakout:
                if (row['macd_hist'] > 0 and
                    not pd.isna(row['macd_hist_prev']) and
                    row['macd_hist'] > row['macd_hist_prev']):
                    macd_ok = True
                    side = 'long'

            if is_short_breakout and not macd_ok:
                if (row['macd_hist'] < 0 and
                    not pd.isna(row['macd_hist_prev']) and
                    row['macd_hist'] < row['macd_hist_prev']):
                    macd_ok = True
                    side = 'short'

            if not macd_ok:
                continue

            engine.funnel.macd_pass += 1

            # ===== 퍼널 3단계: 거래량 필터 =====
            if row['volume'] < row['volume_ma'] * config.VOLUME_MULT:
                continue

            engine.funnel.volume_pass += 1

            # ===== 퍼널 4단계: 4H SuperTrend HTF 필터 =====
            htf_dir = get_htf_direction(symbol, timestamp)

            if side == 'long' and htf_dir != 1:
                continue
            if side == 'short' and htf_dir != -1:
                continue

            engine.funnel.htf_pass += 1

            candidates.append({
                'symbol': symbol,
                'side': side,
                'atr': row['atr'],
                'macd_hist': abs(row['macd_hist']),
                'signal_time': timestamp,  # 신호 발생 시각 기록
            })

        # 대기열에 저장 (진입하지 않음! 다음 봉에서 open 가격으로 진입)
        candidates.sort(key=lambda x: x['macd_hist'], reverse=True)
        pending_entries = candidates  # 다음 턴에서 소화

        engine.equity_curve.append({
            'timestamp': timestamp,
            'capital': engine.capital,
            'positions': len(engine.positions),
        })

    # 미청산 포지션 강제 청산 — 타임라인 마지막 시점 기준
    last_timestamp = all_timestamps[-1]
    for symbol in list(engine.positions.keys()):
        df = indicators_1h[symbol]
        # 타임라인 마지막 시점의 데이터 사용 (IS/OOS 기간 존중)
        row = df[df['timestamp'] <= last_timestamp]
        if not row.empty:
            last_row = row.iloc[-1]
            engine.close_position(symbol, last_row['close'], last_row['timestamp'], "BT_END")
        else:
            # 데이터 없으면 전체 마지막 사용 (fallback)
            last_row = df.iloc[-1]
            engine.close_position(symbol, last_row['close'], last_row['timestamp'], "BT_END")

    return engine


# ============================
# 결과 분석 (7개 필수 산출물)
# ============================
def analyze_keltner_results(engine: KeltnerBacktestEngine, config: KeltnerConfig):
    """Phase 1 Baseline BT 7개 필수 산출물"""
    trades = engine.trades

    if not trades:
        print("거래 없음!")
        return {}

    total_trades = len(trades)
    winners = [t for t in trades if t.pnl_usd > 0]
    losers = [t for t in trades if t.pnl_usd <= 0]

    win_rate = len(winners) / total_trades * 100 if total_trades > 0 else 0

    # === 산출물 #1: PF (Profit Factor, 수수료 포함) ===
    gross_profit = sum(t.pnl_usd for t in winners) if winners else 0
    gross_loss = abs(sum(t.pnl_usd for t in losers)) if losers else 0.001
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

    # === 산출물 #2: 롱/숏 분리 성과 ===
    long_trades = [t for t in trades if t.side == 'long']
    short_trades = [t for t in trades if t.side == 'short']

    def side_stats(side_trades, label):
        if not side_trades:
            return {'label': label, 'count': 0, 'pf': 0, 'win_rate': 0, 'avg_win': 0, 'avg_loss': 0}
        w = [t for t in side_trades if t.pnl_usd > 0]
        l = [t for t in side_trades if t.pnl_usd <= 0]
        gp = sum(t.pnl_usd for t in w) if w else 0
        gl = abs(sum(t.pnl_usd for t in l)) if l else 0.001
        return {
            'label': label,
            'count': len(side_trades),
            'pf': gp / gl if gl > 0 else 0,
            'win_rate': len(w) / len(side_trades) * 100,
            'avg_win': np.mean([t.pnl_usd for t in w]) if w else 0,
            'avg_loss': np.mean([abs(t.pnl_usd) for t in l]) if l else 0,
            'total_pnl': sum(t.pnl_usd for t in side_trades),
        }

    long_stats = side_stats(long_trades, '롱')
    short_stats = side_stats(short_trades, '숏')

    # === 산출물 #3: MDD ===
    eq = pd.DataFrame(engine.equity_curve)
    if not eq.empty:
        eq['peak'] = eq['capital'].cummax()
        eq['drawdown'] = (eq['capital'] - eq['peak']) / eq['peak'] * 100
        max_drawdown = eq['drawdown'].min()
    else:
        max_drawdown = 0

    # === 산출물 #4: 평균 보유시간 (전체/롱/숏) ===
    avg_hold_all = np.mean([t.hold_hours for t in trades])
    avg_hold_long = np.mean([t.hold_hours for t in long_trades]) if long_trades else 0
    avg_hold_short = np.mean([t.hold_hours for t in short_trades]) if short_trades else 0

    # === 산출물 #5: 5단계 퍼널 통과율 ===
    funnel = engine.funnel

    # === 산출물 #6: 월별 손익 ===
    monthly_pnl = {}
    for t in trades:
        month_key = t.exit_time.strftime('%Y-%m')
        monthly_pnl[month_key] = monthly_pnl.get(month_key, 0) + t.pnl_usd

    # === 산출물 #7: 연속 패배 최대 ===
    max_consecutive_losses = 0
    current_streak = 0
    for t in trades:
        if t.pnl_usd < 0:
            current_streak += 1
            max_consecutive_losses = max(max_consecutive_losses, current_streak)
        else:
            current_streak = 0

    # 추가 통계
    avg_win = np.mean([t.pnl_usd for t in winners]) if winners else 0
    avg_loss = np.mean([abs(t.pnl_usd) for t in losers]) if losers else 0
    rr_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    total_pnl = sum(t.pnl_usd for t in trades)
    total_return = (engine.capital - config.INITIAL_CAPITAL) / config.INITIAL_CAPITAL * 100

    # 청산 이유별
    exit_reasons = {}
    for t in trades:
        exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1

    results = {
        'total_trades': total_trades,
        'winners': len(winners),
        'losers': len(losers),
        'win_rate': win_rate,
        'profit_factor': profit_factor,
        'gross_profit': gross_profit,
        'gross_loss': gross_loss,
        'total_pnl': total_pnl,
        'total_return_pct': total_return,
        'final_capital': engine.capital,
        'max_drawdown_pct': max_drawdown,
        'avg_win_usd': avg_win,
        'avg_loss_usd': avg_loss,
        'rr_ratio': rr_ratio,
        'long_stats': long_stats,
        'short_stats': short_stats,
        'avg_hold_all': avg_hold_all,
        'avg_hold_long': avg_hold_long,
        'avg_hold_short': avg_hold_short,
        'funnel': {
            'total_candles': funnel.total_candles,
            'kc_breakout': funnel.kc_breakout,
            'macd_pass': funnel.macd_pass,
            'volume_pass': funnel.volume_pass,
            'htf_pass': funnel.htf_pass,
            'entered': funnel.entered,
        },
        'monthly_pnl': monthly_pnl,
        'max_consecutive_losses': max_consecutive_losses,
        'exit_reasons': exit_reasons,
    }

    return results


def print_keltner_results(results, config_label=""):
    """Phase 1 Baseline BT 결과 출력"""
    r = results

    print(f"\n{'='*65}")
    print(f"  ⚡ 켈트너 v1.1 Baseline BT 결과 {config_label}")
    print(f"{'='*65}")

    # === 산출물 #1: PF ===
    print(f"\n📊 #1 총 Profit Factor (수수료 포함)")
    print(f"  PF = {r['profit_factor']:.2f}")
    pf_status = "✅ PASS" if r['profit_factor'] >= 1.5 else "❌ FAIL (기준: ≥1.5)"
    print(f"  판정: {pf_status}")
    print(f"  Gross Profit: ${r['gross_profit']:.2f} | Gross Loss: ${r['gross_loss']:.2f}")

    # === 산출물 #2: 롱/숏 분리 ===
    print(f"\n📊 #2 롱/숏 분리 성과")
    for s in [r['long_stats'], r['short_stats']]:
        if s['count'] > 0:
            print(f"  [{s['label']}] 거래: {s['count']}건 | PF: {s['pf']:.2f} | "
                  f"승률: {s['win_rate']:.1f}% | 평균W: ${s['avg_win']:.2f} | "
                  f"평균L: ${s['avg_loss']:.2f} | 총PnL: ${s['total_pnl']:.2f}")
        else:
            print(f"  [{s['label']}] 거래 없음")

    # === 산출물 #3: MDD ===
    print(f"\n📊 #3 MDD (최대 낙폭)")
    print(f"  MDD = {r['max_drawdown_pct']:.1f}%")
    mdd_status = "✅ PASS" if abs(r['max_drawdown_pct']) <= 15 else "❌ FAIL (기준: ≤15%)"
    print(f"  판정: {mdd_status}")

    # === 산출물 #4: 평균 보유시간 ===
    print(f"\n📊 #4 평균 보유시간")
    print(f"  전체: {r['avg_hold_all']:.1f}시간 | 롱: {r['avg_hold_long']:.1f}시간 | 숏: {r['avg_hold_short']:.1f}시간")

    # === 산출물 #5: 5단계 퍼널 ===
    print(f"\n📊 #5 5단계 퍼널 통과율")
    f = r['funnel']
    total = f['total_candles'] if f['total_candles'] > 0 else 1
    print(f"  스캔 캔들: {f['total_candles']:,}")
    steps = [
        ("KC 돌파", f['kc_breakout']),
        ("MACD", f['macd_pass']),
        ("거래량", f['volume_pass']),
        ("HTF(4H)", f['htf_pass']),
        ("진입", f['entered']),
    ]
    for name, count in steps:
        pct = count / total * 100 if total > 0 else 0
        print(f"  → {name}: {count:,} ({pct:.2f}%)")

    # === 산출물 #6: 월별 손익 ===
    print(f"\n📊 #6 월별 손익")
    if r['monthly_pnl']:
        max_abs = max(abs(v) for v in r['monthly_pnl'].values()) if r['monthly_pnl'] else 1
        for month, pnl in sorted(r['monthly_pnl'].items()):
            bar_len = int(abs(pnl) / max_abs * 20) if max_abs > 0 else 0
            bar = '█' * bar_len
            sign = '+' if pnl >= 0 else ''
            icon = '🟢' if pnl >= 0 else '🔴'
            print(f"  {month}: {sign}${pnl:.2f} {icon} {bar}")

    # === 산출물 #7: 연속 패배 ===
    print(f"\n📊 #7 연속 패배 최대")
    print(f"  최대 연속 손실: {r['max_consecutive_losses']}회")

    # === 추가 통계 ===
    print(f"\n{'─'*65}")
    print(f"  추가 통계")
    print(f"{'─'*65}")
    print(f"  총 거래: {r['total_trades']}건 (롱 {r['long_stats']['count']} / 숏 {r['short_stats']['count']})")
    print(f"  승률: {r['win_rate']:.1f}% ({r['winners']}W / {r['losers']}L)")
    wr_status = "✅ PASS" if r['win_rate'] >= 45 else "❌ FAIL (기준: ≥45%)"
    print(f"  판정: {wr_status}")
    print(f"  총 PnL: ${r['total_pnl']:.2f}")
    print(f"  총 수익률: {r['total_return_pct']:.1f}%")
    print(f"  최종 자본: ${r['final_capital']:.2f}")
    print(f"  R:R = 1:{r['rr_ratio']:.2f}")

    print(f"\n  🚪 청산 이유")
    for reason, count in sorted(r['exit_reasons'].items(), key=lambda x: -x[1]):
        pct = count / r['total_trades'] * 100
        print(f"    {reason}: {count}건 ({pct:.0f}%)")

    # === 판정 요약 ===
    print(f"\n{'='*65}")
    print(f"  🎯 Phase 1 판정 요약")
    print(f"{'='*65}")
    print(f"  PF ≥ 1.5?    {r['profit_factor']:.2f}  {'✅' if r['profit_factor'] >= 1.5 else '❌'}")
    print(f"  승률 ≥ 45%?  {r['win_rate']:.1f}%  {'✅' if r['win_rate'] >= 45 else '❌'}")
    print(f"  MDD ≤ 15%?   {abs(r['max_drawdown_pct']):.1f}%  {'✅' if abs(r['max_drawdown_pct']) <= 15 else '❌'}")

    all_pass = (r['profit_factor'] >= 1.5 and r['win_rate'] >= 45 and abs(r['max_drawdown_pct']) <= 15)
    if all_pass:
        print(f"\n  🟢 Phase 2 진행 가능!")
    else:
        print(f"\n  🔴 기준 미달 — 파라미터 재설계 필요")
    print(f"{'='*65}")
