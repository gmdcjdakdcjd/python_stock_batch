import pandas as pd
from API import ETFAnalyzeKR
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal

# -----------------------------
# 1. DB 연결 및 전략 기본정보
# -----------------------------
mk = ETFAnalyzer.MarketDB()  # ETF DB 객체 생성 (ETF 정보 및 가격 데이터 접근)
company = mk.get_etf_info_optimization()  # 최적화용 ETF 정보 DataFrame
stocks = list(company['name'])  # ETF 종목명 리스트 추출
name_to_code = {v: k for k, v in mk.codes.items()}  # 종목명→코드 매핑 딕셔너리

print(f"총 {len(stocks)}개 종목 스캔 시작...")

# -----------------------------
# 2. 전략 실행 요약 저장 (1회 실행 로그)
# -----------------------------
result_id = save_strategy_summary(
    strategy_name='BollingerBand_Reversal',  # 전략명
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
        df = mk.get_daily_price(s, start_date)  # ETF별 6개월치 가격 데이터 조회
        if df is None or df.empty or len(df) < 21:
            continue  # 데이터 부족시 스킵

        # Bollinger Band 계산
        df['MA20'] = df['close'].rolling(window=20).mean()  # 20일 이동평균
        df['stddev'] = df['close'].rolling(window=20).std()  # 20일 표준편차
        df['upper'] = df['MA20'] + (df['stddev'] * 2)  # 상단 밴드
        df['lower'] = df['MA20'] - (df['stddev'] * 2)  # 하단 밴드
        df['PB'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])  # %B 지표
        df['II'] = (2 * df['close'] - df['high'] - df['low']) / (df['high'] - df['low']) * df['volume']  # 매집/이탈 지표
        df['IIP21'] = df['II'].rolling(window=21).sum() / df['volume'].rolling(window=21).sum() * 100  # 21일 누적 매집/이탈
        df = df.dropna()  # 결측치 제거

        # 최근 거래일 데이터 기준으로 판단
        last = df.iloc[-1]
        date = df.index[-1].strftime('%Y-%m-%d')
        price = float(last['close'])
        pb = float(last['PB'])
        iip = float(last['IIP21'])

        # 매수/매도 신호 조건
        if pb < 0.05 and iip > 0:
            action = 'BUY'
            buy_signals.append((s, price))
        elif pb > 0.95 and iip < 0:
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
            signal_date=date  # 날짜 저장
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