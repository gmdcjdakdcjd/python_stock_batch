import pandas as pd
from API import AnalyzeKR
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal

# -----------------------------
# 1. DB 연결 및 기본 세팅
# -----------------------------
mk = Analyzer.MarketDB()  # 마켓DB 객체 생성 (종목 정보 및 가격 데이터 접근)
company = mk.get_comp_info_optimization()  # 최적화용 종목 정보 DataFrame
stocks = list(company['name'])  # 종목명 리스트 추출
name_to_code = {v: k for k, v in mk.codes.items()}  # 종목명→코드 매핑 딕셔너리

print(f"총 {len(stocks)}개 종목 스캔 시작...")

# -----------------------------
# 2. 전략 실행 요약 저장 (1회 실행 로그)
# -----------------------------
result_id = save_strategy_summary(
    strategy_name='BollingerBand_TrendFollowing',  # 전략명
    signal_date=pd.Timestamp.today().strftime('%Y-%m-%d'),  # 실행일
    signal_type='SCAN'  # 실행 타입
)

# -----------------------------
# 3. 개별 종목 전략 계산
# -----------------------------
buy_signals = []  # 매수 신호 저장 리스트
sell_signals = []  # 매도 신호 저장 리스트
start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')  # 6개월 전부터 데이터 조회
for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)  # 개별 종목의 6개월치 가격 데이터 조회
        if df is None or df.empty or len(df) < 20:
            continue  # 데이터 부족시 스킵

        # Bollinger Band 계산
        df['MA20'] = df['close'].rolling(window=20).mean()  # 20일 이동평균
        df['stddev'] = df['close'].rolling(window=20).std()  # 20일 표준편차
        df['upper'] = df['MA20'] + (df['stddev'] * 2)  # 상단 밴드
        df['lower'] = df['MA20'] - (df['stddev'] * 2)  # 하단 밴드
        df['PB'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])  # %B 지표

        # MFI 계산 (자금 흐름 지표)
        df['TP'] = (df['high'] + df['low'] + df['close']) / 3  # Typical Price
        df['PMF'] = 0  # Positive Money Flow
        df['NMF'] = 0  # Negative Money Flow
        for i in range(len(df.close) - 1):
            if df.TP.values[i] < df.TP.values[i + 1]:
                df.PMF.values[i + 1] = df.TP.values[i + 1] * df.volume.values[i + 1]
                df.NMF.values[i + 1] = 0
            else:
                df.NMF.values[i + 1] = df.TP.values[i + 1] * df.volume.values[i + 1]
                df.PMF.values[i + 1] = 0

        df['MFR'] = df['PMF'].rolling(window=10).sum() / df['NMF'].rolling(window=10).sum()  # Money Flow Ratio
        df['MFI10'] = 100 - 100 / (1 + df['MFR'])  # 10일 MFI
        df = df.dropna()  # 결측치 제거

        # 최근 거래일 기준 신호 판단
        last = df.iloc[-1]
        date = df.index[-1].strftime('%Y-%m-%d')
        price = float(last['close'])
        pb = float(last['PB'])
        mfi = float(last['MFI10'])

        # 매수 신호 (상단 돌파 — 추세추종)
        if pb > 0.8 and mfi > 80:
            action = 'BUY'
            buy_signals.append((s, price))

        # 매도 신호 (하단 이탈 — 추세추종)
        elif pb < 0.2 and mfi < 20:
            action = 'SELL'
            sell_signals.append((s, price))
        else:
            continue

        # 전략 신호 DB 저장
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
# 4. 요약 출력
# -----------------------------
print("\n실행 완료")
print(f"매수 신호: {len(buy_signals)}건")
print(f"매도 신호: {len(sell_signals)}건")
print(f"DB 저장 완료 (result_id={result_id})")


print(f"ROWCOUNT={len(buy_signals) + len(sell_signals)}")
print(f"CODECOUNT={len(buy_signals) + len(sell_signals)}")

print(f"DB 저장 완료 (result_id={result_id})")