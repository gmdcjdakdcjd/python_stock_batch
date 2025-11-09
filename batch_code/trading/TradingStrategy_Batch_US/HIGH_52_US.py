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
start_date = '2022-11-24'

# -----------------------------
# 2️⃣ 주봉 기준 52주 종가 신고가 ‘첫 발생’ 탐색
# -----------------------------
new_high_candidates = []

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

        # ✅ 주봉 데이터 (토요일 종가 기준)
        weekly = pd.DataFrame()
        weekly['open'] = df['open'].resample('W-SAT').first()
        weekly['high'] = df['high'].resample('W-SAT').max()
        weekly['low'] = df['low'].resample('W-SAT').min()
        weekly['close'] = df['close'].resample('W-SAT').last()
        weekly['volume'] = df['volume'].resample('W-SAT').sum()
        weekly.dropna(subset=['close'], inplace=True)

        if len(weekly) < 52:
            continue

        # ✅ 52주 종가 최고가 계산
        weekly['HIGH_52_CLOSE'] = weekly['close'].rolling(window=52, min_periods=1).max()

        prev = weekly.iloc[-2]
        last = weekly.iloc[-1]

        # ✅ 조건: 52주 신고가 ‘첫 발생’ + 종가 ≥ $10
        if (
            last['close'] >= last['HIGH_52_CLOSE']
            and prev['close'] < prev['HIGH_52_CLOSE']
            and last['close'] >= 10
        ):
            new_high_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': last.name.strftime('%Y-%m-%d'),
                'close': round(last['close'], 2),
                'high_52_close': round(last['HIGH_52_CLOSE'], 2)
            })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 결과 출력 및 DB 저장
# -----------------------------
if new_high_candidates:
    df_high = pd.DataFrame(new_high_candidates)
    df_high.sort_values(by='close', ascending=False, inplace=True)

    print("🚀 [주봉] 52주 종가 신고가 ‘첫 발생’ 종목 리스트 (종가 ≥ $10):\n")
    print(df_high.to_string(index=False))
    print(f"\n총 {len(df_high)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "WEEKLY_52W_NEW_HIGH_US"
    signal_type = "BUY"

    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=today,
        signal_type=signal_type,
        total_return=None,
        total_risk=None,
        total_sharpe=None
    )

    print(f"🧾 [RESULT_ID] 이번 실행으로 저장된 result_id = {result_id}\n")

    for idx, row in enumerate(df_high.itertuples(), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row.code,
            name=row.name,
            action='BUY',
            price=row.close,
            old_price=row.high_52_close,
            returns=None,
            rank_order=idx,
            signal_date=row.date
        )

    print(f"ROWCOUNT={len(df_high)}")
    print(f"CODECOUNT={len(df_high)}")
    print(f"RESULT_ID={result_id}")
    print(f"✅ [DB저장완료] {len(df_high)}건 (result_id={result_id})")

else:
    print("\n💤 [주봉] 52주 종가 신고가 ‘첫 발생’ 종목 없음 — DB 저장 생략.")
