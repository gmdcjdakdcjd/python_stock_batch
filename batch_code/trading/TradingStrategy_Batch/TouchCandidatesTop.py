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

# -----------------------------
# 2️⃣ 종가 기준 볼린저 상단 터치 종목 탐색
# -----------------------------
touch_candidates = []
start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty or len(df) < 20:
            continue

        # ✅ 볼린저밴드 계산 (MA20, ±2σ)
        df['MA20'] = df['close'].rolling(window=20).mean()
        df['stddev'] = df['close'].rolling(window=20).std()
        df['upper'] = df['MA20'] + (df['stddev'] * 2)
        df['lower'] = df['MA20'] - (df['stddev'] * 2)

        last = df.iloc[-1]
        date = df.index[-1].strftime('%Y-%m-%d')

        close_price = last['close']
        upper_band = last['upper']

        # ✅ 상단선 대비 괴리율 계산 (종가 기준)
        diff_rate = ((close_price - upper_band) / upper_band) * 100

        # ✅ 조건:
        # - 종가가 상단 위로 크게 이탈하지 않음
        # - 종가가 상단선 기준으로 -1% ~ +1% 이내 (터치 근처)
        # - 종가 10,000원 이상
        if -1.0 <= diff_rate <= 1.0 and close_price >= 10000 and close_price <= upper_band * 1.01:
            touch_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': date,
                'close': round(close_price, 2),
                'upper_band': round(upper_band, 2),
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

    print("📊 [일봉] 볼린저밴드(MA20 / ±2σ) 종가 기준 상단 터치 구간 종목 리스트 (±1%, 종가≥10,000원):\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_BB_UPPER_TOUCH"
    signal_type = "SELL"  # 상단 터치는 매도 관점으로 분류

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
            action='SELL',
            price=row['close'],
            old_price=row['upper_band'],
            returns=row['diff_rate(%)'],
            rank_order=idx,
            signal_date=row['date']
        )

    print(f"ROWCOUNT={len(touch_candidates)}")
    print(f"CODECOUNT={len(touch_candidates)}")
    print(f"RESULT_ID={result_id}")

    print(f"✅ [DB저장완료] {len(touch_candidates)}건 (result_id={result_id})")

else:
    print("\n💤 [일봉] 종가 기준 볼린저 상단 터치 종목 없음 — DB 저장 생략.")
