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

print(f"총 {len(stocks)}개 종목 스캔 시작...\n")
start_date = '2022-11-24'

# -----------------------------
# 2️⃣ 일봉 기준 120일 종가 신저가 ‘첫 발생’ 탐색
# -----------------------------
low_break_candidates = []

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

        if len(df) < 121:  # 최소 121일 이상 필요 (이전 구간 포함)
            continue

        # ✅ 최근 120일간 최저가 계산 (이전일까지)
        df['LOW_120_CLOSE'] = df['close'].shift(1).rolling(window=120).min()

        # ✅ 최근 2일 데이터
        prev = df.iloc[-2]  # 어제
        last = df.iloc[-1]  # 오늘

        # ✅ 조건: 오늘 처음으로 120일 종가 신저가 갱신 + 종가 ≥ $10
        if (
            not np.isnan(last['LOW_120_CLOSE'])
            and prev['close'] > prev['LOW_120_CLOSE']
            and last['close'] <= last['LOW_120_CLOSE']
            and last['close'] >= 10
        ):
            low_break_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': last.name.strftime('%Y-%m-%d'),
                'close': round(last['close'], 2),
                'low_120_close': round(last['LOW_120_CLOSE'], 2)
            })

    except Exception as e:
        print(f"⚠️ {s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 결과 출력 및 DB 저장
# -----------------------------
if low_break_candidates:
    df_low = pd.DataFrame(low_break_candidates)
    df_low.sort_values(by='close', ascending=True, inplace=True)

    print("📉 [일봉] 120일 종가 신저가 ‘첫 발생’ 종목 리스트 (종가 ≥ $10):\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_120D_NEW_LOW_US"
    signal_type = "SELL"

    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=today,
        signal_type=signal_type,
        total_return=None,
        total_risk=None,
        total_sharpe=None
    )
    print(f"🧾 [RESULT_ID] {result_id} 생성 완료\n")

    for idx, row in enumerate(df_low.itertuples(), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row.code,
            name=row.name,
            action='SELL',
            price=row.close,
            old_price=row.low_120_close,
            returns=None,
            rank_order=idx,
            signal_date=row.date
        )

    print(f"✅ [DB저장완료] {len(df_low)}건 (result_id={result_id})")
    print(f"ROWCOUNT={len(df_low)} | CODECOUNT={len(df_low)}")

else:
    print("\n💤 [일봉] 120일 종가 신저가 ‘첫 발생’ 종목 없음 — DB 저장 생략.")
