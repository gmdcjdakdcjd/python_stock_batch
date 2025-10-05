import pandas as pd
from API import Analyzer
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal

# -----------------------------
# 1. DB 연결 및 기본 세팅
# -----------------------------
mk = Analyzer.MarketDB()
company = mk.get_comp_info_optimization()
stocks = list(company['name'])
name_to_code = {v: k for k, v in mk.codes.items()}

print(f"📊 총 {len(stocks)}개 종목 스캔 시작...")

# -----------------------------
# 2. 전략 실행 요약 저장 (1회 실행 로그)
# -----------------------------
result_id = save_strategy_summary(
    strategy_name='BollingerBand_TrendFollowing',
    signal_date=pd.Timestamp.today().strftime('%Y-%m-%d'),
    signal_type='SCAN'
)

# -----------------------------
# 3. 개별 종목 전략 계산
# -----------------------------
buy_signals = []
sell_signals = []
start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty or len(df) < 20:
            continue

        # Bollinger Band 계산
        df['MA20'] = df['close'].rolling(window=20).mean()
        df['stddev'] = df['close'].rolling(window=20).std()
        df['upper'] = df['MA20'] + (df['stddev'] * 2)
        df['lower'] = df['MA20'] - (df['stddev'] * 2)
        df['PB'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])

        # MFI 계산
        df['TP'] = (df['high'] + df['low'] + df['close']) / 3
        df['PMF'] = 0
        df['NMF'] = 0
        for i in range(len(df.close) - 1):
            if df.TP.values[i] < df.TP.values[i + 1]:
                df.PMF.values[i + 1] = df.TP.values[i + 1] * df.volume.values[i + 1]
                df.NMF.values[i + 1] = 0
            else:
                df.NMF.values[i + 1] = df.TP.values[i + 1] * df.volume.values[i + 1]
                df.PMF.values[i + 1] = 0

        df['MFR'] = df['PMF'].rolling(window=10).sum() / df['NMF'].rolling(window=10).sum()
        df['MFI10'] = 100 - 100 / (1 + df['MFR'])
        df = df.dropna()

        # 최근 거래일 기준 신호 판단
        last = df.iloc[-1]
        date = df.index[-1].strftime('%Y-%m-%d')
        price = float(last['close'])
        pb = float(last['PB'])
        mfi = float(last['MFI10'])

        # 🔻 매도 신호 (과매수 구간)
        if pb > 0.8 and mfi > 80:
            action = 'SELL'
            sell_signals.append((s, price))
        # 🔺 매수 신호 (과매도 구간)
        elif pb < 0.2 and mfi < 20:
            action = 'BUY'
            buy_signals.append((s, price))
        else:
            continue

        # DB 저장
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
        print(f"⚠️ {s} 처리 실패: {e}")

# -----------------------------
# 4. 요약 출력
# -----------------------------
print("\n✅ 실행 완료")
print(f"📈 매수 신호: {len(buy_signals)}건")
print(f"📉 매도 신호: {len(sell_signals)}건")
print(f"💾 DB 저장 완료 (result_id={result_id})")
