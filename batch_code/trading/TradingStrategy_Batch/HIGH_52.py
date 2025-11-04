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

        # ✅ 52주 종가 기준 최고가 계산
        weekly['HIGH_52_CLOSE'] = weekly['close'].rolling(window=52).max()

        prev = weekly.iloc[-2]  # 지난주
        last = weekly.iloc[-1]  # 이번주

        # ✅ 조건: 이번 주 처음으로 52주 종가 신고가 돌파 + 종가 10,000원 이상
        if (
            last['close'] >= last['HIGH_52_CLOSE'] and
            prev['close'] < prev['HIGH_52_CLOSE'] and
            last['close'] >= 10000
        ):
            new_high_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': last.name.strftime('%Y-%m-%d'),
                'close': last['close'],
                'high_52_close': last['HIGH_52_CLOSE']
            })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 결과 출력 및 DB 저장
# -----------------------------
if new_high_candidates:
    df_high = pd.DataFrame(new_high_candidates)
    df_high.sort_values(by='close', ascending=False, inplace=True)

    print("🚀 [주봉] 52주 종가 신고가 ‘첫 발생’ 종목 리스트 (종가≥10,000원):\n")
    print(df_high.to_string(index=False))
    print(f"\n총 {len(df_high)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "WEEKLY_52W_NEW_HIGH"
    signal_type = "BUY"

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
    for idx, row in enumerate(new_high_candidates, start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row['code'],
            name=row['name'],
            action='BUY',
            price=row['close'],
            old_price=None,
            returns=None,
            rank_order=idx,
            signal_date=row['date']
        )

    print(f"ROWCOUNT={len(new_high_candidates)}")
    print(f"CODECOUNT={len(new_high_candidates)}")
    print(f"RESULT_ID={result_id}")

    print(f"✅ [DB저장완료] {len(new_high_candidates)}건 (result_id={result_id})")

else:
    print("\n💤 [주봉] 52주 종가 신고가 ‘첫 발생’ 종목 없음 — DB 저장 생략.")
