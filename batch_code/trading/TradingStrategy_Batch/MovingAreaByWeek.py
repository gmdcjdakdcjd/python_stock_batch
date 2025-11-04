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
# 2️⃣ 주봉 60이평 터치 종목 탐색
# -----------------------------
touch_candidates = []

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

        # ✅ 주간 데이터 (토요일 종가 기준)
        weekly = pd.DataFrame()
        weekly['open'] = df['open'].resample('W-SAT').first()
        weekly['high'] = df['high'].resample('W-SAT').max()
        weekly['low'] = df['low'].resample('W-SAT').min()
        weekly['close'] = df['close'].resample('W-SAT').last()
        weekly['volume'] = df['volume'].resample('W-SAT').sum()
        weekly.dropna(inplace=True)

        if len(weekly) < 60:
            continue

        # ✅ 60주 이동평균 계산
        weekly['MA60'] = weekly['close'].rolling(window=60).mean()

        # ✅ 최근 2주 비교
        if len(weekly) < 2:
            continue

        prev = weekly.iloc[-2]  # 지난주
        last = weekly.iloc[-1]  # 이번주

        if np.isnan(prev['MA60']) or prev['MA60'] == 0:
            continue

        # ✅ "이번주 종가 vs 지난주 MA60" 등락률
        diff_rate = ((last['close'] - prev['MA60']) / prev['MA60']) * 100

        # ✅ 조건: 60주선 ±5% 범위 터치
        if -1.0 <= diff_rate <= 5.0 and last['close'] >= 10000:
            touch_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': last.name.strftime('%Y-%m-%d'),
                'close': last['close'],
                'ma60_prev': prev['MA60'],
                'diff_rate(%)': round(diff_rate, 2)
            })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 결과 출력 및 DB 저장
# -----------------------------
if touch_candidates:
    df_touch = pd.DataFrame(touch_candidates)
    df_touch.sort_values(by='diff_rate(%)', inplace=True)

    print("📊 [주봉] 60주선 터치 종목 리스트 (±5% 범위, 지난주 MA60 기준):\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "WEEKLY_TOUCH_MA60"
    signal_type = "TOUCH"

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
    for idx, row in enumerate(touch_candidates, start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row['code'],
            name=row['name'],
            action='TOUCH',
            price=row['close'],
            old_price=row['ma60_prev'],
            returns=row['diff_rate(%)'],
            rank_order=idx,
            signal_date=row['date']
        )

    print(f"ROWCOUNT={len(touch_candidates)}")
    print(f"CODECOUNT={len(touch_candidates)}")
    print(f"RESULT_ID={result_id}")

    print(f"✅ [DB저장완료] {len(touch_candidates)}건 (result_id={result_id})")

else:
    print("\n💤 [주봉] 60주선 터치 종목 없음 — DB 저장 생략.")
