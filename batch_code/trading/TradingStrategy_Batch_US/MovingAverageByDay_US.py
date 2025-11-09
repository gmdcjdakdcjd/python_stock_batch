import pandas as pd
import numpy as np
import warnings
from API import USAnalyzer as Analyzer
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

print(f"총 {len(stocks)}개 미국 종목 스캔 시작...\n")
start_date = '2022-11-24'

# -----------------------------
# 2️⃣ 일봉 60이평 터치 종목 탐색
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
        df = df.sort_index().dropna(subset=['close'])

        if len(df) < 60:
            continue

        # ✅ 60일 이동평균 계산
        df['MA60'] = df['close'].rolling(window=60, min_periods=1).mean()

        prev = df.iloc[-2]  # 어제
        last = df.iloc[-1]  # 오늘

        if np.isnan(prev['MA60']) or prev['MA60'] == 0:
            continue

        # ✅ "오늘 종가 vs 어제 MA60" 등락률
        diff_rate = ((last['close'] - prev['MA60']) / prev['MA60']) * 100

        # ✅ 조건: 60일선 ±1.5% 범위 & 종가 ≥ $10
        if -1.5 <= diff_rate <= 1.5 and last['close'] >= 10:
            touch_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': last.name.strftime('%Y-%m-%d'),
                'close': round(last['close'], 2),
                'ma60_prev': round(prev['MA60'], 2),
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

    print("📊 [일봉] 60일선 터치 종목 리스트 (±1.5% 범위, 종가 ≥ $10):\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_TOUCH_MA60_US"
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
    for idx, row in enumerate(df_touch.itertuples(), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row.code,
            name=row.name,
            action='TOUCH',
            price=row.close,
            old_price=row.ma60_prev,
            returns=row._asdict().get('diff_rate(%)'),
            rank_order=idx,
            signal_date=row.date
        )

    print(f"ROWCOUNT={len(df_touch)}")
    print(f"CODECOUNT={len(df_touch)}")
    print(f"RESULT_ID={result_id}")
    print(f"✅ [DB저장완료] {len(df_touch)}건 (result_id={result_id})")

else:
    print("\n💤 [일봉] 조건에 맞는 종목 없음 — DB 저장 생략.")
