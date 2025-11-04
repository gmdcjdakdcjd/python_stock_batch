import pandas as pd
import numpy as np
import warnings
from API import Analyzer
from datetime import datetime
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal  # ✅ DB 저장 함수

warnings.filterwarnings("ignore", category=RuntimeWarning)

# -----------------------------
# 1️⃣ DB 연결 및 기본 세팅
# -----------------------------
mk = Analyzer.MarketDB()
company = mk.get_comp_info_optimization()
stocks = list(company['name'])
name_to_code = {v: k for k, v in mk.codes.items()}

print(f"총 {len(stocks)}개 종목 스캔 시작...\n")
start_date = '2022-11-24'

# -----------------------------
# 2️⃣ 주봉 기준 52주 종가 신저가 탐색
# -----------------------------
low_candidates = []

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty:
            continue

        # ✅ 날짜 인덱스 세팅
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        df = df.sort_index()

        # ✅ 주봉 데이터 (토요일 기준)
        weekly = pd.DataFrame()
        weekly['open'] = df['open'].resample('W-SAT').first()
        weekly['high'] = df['high'].resample('W-SAT').max()
        weekly['low'] = df['low'].resample('W-SAT').min()
        weekly['close'] = df['close'].resample('W-SAT').last()
        weekly['volume'] = df['volume'].resample('W-SAT').sum()
        weekly.dropna(inplace=True)

        if len(weekly) < 52:
            continue

        # ✅ 52주 종가 기준 최저가 계산
        weekly['LOW_52_CLOSE'] = weekly['close'].rolling(window=52).min()
        last = weekly.iloc[-1]  # 이번 주 데이터

        # ✅ 조건: 이번 주 종가가 52주 최저치 + 종가 10,000원 이상
        if last['close'] <= last['LOW_52_CLOSE'] and last['close'] >= 10000:
            low_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': last.name.strftime('%Y-%m-%d'),
                'close': last['close'],
                'low_52_close': last['LOW_52_CLOSE']
            })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 결과 출력 및 DB 저장
# -----------------------------
if low_candidates:
    df_low = pd.DataFrame(low_candidates)
    df_low.sort_values(by='close', ascending=True, inplace=True)

    print("📉 [주봉] 52주 종가 신저가 종목 리스트 (종가≥10,000원):\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "WEEKLY_52W_NEW_LOW"
    signal_type = "SELL"

    # 1) 요약 저장
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=today,
        signal_type=signal_type,
        total_return=None,
        total_risk=None,
        total_sharpe=None
    )

    print(f"🧾 [RESULT_ID] 이번 실행으로 저장된 result_id = {result_id}\n")

    # 2) 상세 저장
    for idx, row in enumerate(low_candidates, start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row['code'],
            name=row['name'],
            action='SELL',
            price=row['close'],
            old_price=None,
            returns=None,
            rank_order=idx,
            signal_date=row['date']
        )

    print(f"ROWCOUNT={len(low_candidates)}")
    print(f"CODECOUNT={len(low_candidates)}")
    print(f"RESULT_ID={result_id}")

    print(f"✅ [DB저장완료] {len(low_candidates)}건 (result_id={result_id})")

else:
    print("\n💤 [주봉] 52주 종가 신저가 종목 없음 — DB 저장 생략.")
