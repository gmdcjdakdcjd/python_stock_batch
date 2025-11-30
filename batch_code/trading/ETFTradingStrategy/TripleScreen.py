import pandas as pd
import numpy as np
from API import ETFAnalyzeKR
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal
import random

# -----------------------------
# 1. DB 연결 및 ETF 종목 정보 준비
# -----------------------------
mk = ETFAnalyzer.MarketDB()  # ETF DB 객체 생성 (ETF 정보 및 가격 데이터 접근)
company = mk.get_etf_info_optimization()  # 최적화용 ETF 정보 DataFrame
stocks = list(company['name'])  # 전체 ETF 종목명 리스트
name_to_code = {v: k for k, v in mk.codes.items()}  # 종목명→코드 매핑 딕셔너리

print(f"총 {len(stocks)}개 종목 스캔 시작...")

# -----------------------------
# 2. 전략 실행 요약 저장 (1회 실행 로그)
# -----------------------------
result_id = save_strategy_summary(
    strategy_name='TripleScreen_Trading',  # 전략명
    signal_date=pd.Timestamp.today().strftime('%Y-%m-%d'),  # 실행일
    signal_type='SCAN'  # 실행 타입
)

# -----------------------------
# 3. 개별 ETF 종목 전략 계산
# -----------------------------
buy_signals = []  # 매수 신호 저장 리스트
sell_signals = []  # 매도 신호 저장 리스트
start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')  # 6개월 전부터 데이터 조회

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)  # ETF별 6개월치 가격 데이터 조회
        if df is None or df.empty or len(df) < 130:
            continue  # 데이터 부족시 스킵

        # --------------------------
        # (1) MACD & Signal 계산
        # --------------------------
        ema60 = df['close'].ewm(span=60).mean()  # 60일 EMA
        ema130 = df['close'].ewm(span=130).mean()  # 130일 EMA
        macd = ema60 - ema130  # MACD
        signal = macd.ewm(span=45).mean()  # Signal
        macdhist = macd - signal  # MACD Histogram
        df = df.assign(ema130=ema130, ema60=ema60, macd=macd, signal=signal, macdhist=macdhist).dropna()

        # --------------------------
        # (2) Stochastic SlowD 계산
        # --------------------------
        ndays_high = df['high'].rolling(window=14, min_periods=1).max()  # 14일 최고가
        ndays_low = df['low'].rolling(window=14, min_periods=1).min()   # 14일 최저가
        fast_k = (df['close'] - ndays_low) / (ndays_high - ndays_low) * 100  # Fast %K
        slow_d = fast_k.rolling(window=3).mean()  # Slow %D
        df = df.assign(fast_k=fast_k, slow_d=slow_d).dropna()

        # --------------------------
        # (3) 매수/매도 신호 계산
        # --------------------------
        last = df.iloc[-1]  # 최근 거래일 데이터
        date = df.index[-1].strftime('%Y-%m-%d')
        price = float(last['close'])

        # 매도 신호 (EMA 상승 전환 + SlowD 하락 돌파 20선)
        if df.ema130.values[-2] < df.ema130.values[-1] and \
           df.slow_d.values[-2] >= 20 and df.slow_d.values[-1] < 20:
            action = 'SELL'
            sell_signals.append((s, price))
        # 매수 신호 (EMA 하락 전환 + SlowD 상승 돌파 80선)
        elif df.ema130.values[-2] > df.ema130.values[-1] and \
             df.slow_d.values[-2] <= 80 and df.slow_d.values[-1] > 80:
            action = 'BUY'
            buy_signals.append((s, price))
        else:
            continue

        # --------------------------
        # (4) 전략 신호 DB 저장
        # --------------------------
        save_strategy_signal(
            result_id=result_id,
            code=name_to_code.get(s, 'UNKNOWN'),
            name=s,
            action=action,
            price=price,
            signal_date=date
        )

        print(f"[{date}] {s} ({name_to_code.get(s, 'UNKNOWN')}) → {action} 신호 발생, 종가: {price:,.0f}")

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 4. 결과 요약 출력
# -----------------------------
print("\n실행 완료")
print(f"매수 신호: {len(buy_signals)}건")
print(f"매도 신호: {len(sell_signals)}건")
print(f"DB 저장 완료 (result_id={result_id})")

print(f"ROWCOUNT={len(buy_signals) + len(sell_signals)}")
print(f"CODECOUNT={len(buy_signals) + len(sell_signals)}")

print(f"DB 저장 완료 (result_id={result_id})")