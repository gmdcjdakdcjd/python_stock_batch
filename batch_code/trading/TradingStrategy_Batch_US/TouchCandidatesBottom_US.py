import pandas as pd
import numpy as np
import warnings
from API import USAnalyzer as Analyzer
from datetime import datetime
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal

warnings.filterwarnings("ignore", category=RuntimeWarning)

# -----------------------------
# 1️⃣ DB 연결 및 기본 세팅
# -----------------------------
mk = Analyzer.MarketDB()
company = mk.get_comp_info_optimization()
stocks = list(company['name'])
name_to_code = {v: k for k, v in mk.codes.items()}

print(f"총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

# -----------------------------
# 2️⃣ 종가 기준 볼린저 하단 터치 종목 탐색
# -----------------------------
touch_candidates = []

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty or len(df) < 20:
            continue

        # ✅ 볼린저밴드 계산 (MA20, ±2σ)
        df['MA20'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['stddev'] = df['close'].rolling(window=20, min_periods=1).std()
        df['upper'] = df['MA20'] + (df['stddev'] * 2)
        df['lower'] = df['MA20'] - (df['stddev'] * 2)

        df.dropna(subset=['lower', 'close'], inplace=True)
        if df.empty:
            continue

        last = df.iloc[-1]
        date = df.index[-1].strftime('%Y-%m-%d')

        close_price = last['close']
        lower_band = last['lower']

        # ✅ 하단선 대비 괴리율 계산
        diff_rate = ((close_price - lower_band) / lower_band) * 100

        # ✅ 조건:
        # - 종가가 하단선 근처 (±1.0%)
        # - 종가 ≥ $10
        # - 하단보다 살짝 위 or 동일
        if -1.0 <= diff_rate <= 1.0 and close_price >= 10 and close_price >= lower_band * 0.99:
            touch_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': date,
                'close': round(close_price, 2),
                'lower_band': round(lower_band, 2),
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

    print("📉 [일봉] 볼린저밴드 하단 터치 종목 리스트 (±1%, 종가 ≥ $10):\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_BB_LOWER_TOUCH_US"
    signal_type = "BUY"  # 하단 터치 → 매수관점

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
            action='BUY',
            price=row.close,
            old_price=row.lower_band,
            returns=row._asdict().get('diff_rate(%)'),
            rank_order=idx,
            signal_date=row.date
        )

    print(f"ROWCOUNT={len(df_touch)}")
    print(f"CODECOUNT={len(df_touch)}")
    print(f"RESULT_ID={result_id}")
    print(f"✅ [DB저장완료] {len(df_touch)}건 (result_id={result_id})")

else:
    print("\n💤 [일봉] 볼린저 하단 터치 종목 없음 — DB 저장 생략.")
