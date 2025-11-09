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

# -----------------------------
# 2️⃣ 최근 5일 내 거래량 기준 상위 종목 탐색
# -----------------------------
volume_rank = []
start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty:
            continue

        # ✅ 날짜 정렬 및 NaN 방어
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        df = df.sort_index()

        last = df.iloc[-1]

        # ✅ 결측 데이터 방어
        if pd.isna(last['volume']) or pd.isna(last['close']):
            continue

        volume_rank.append({
            'code': name_to_code.get(s, 'UNKNOWN'),
            'name': s,
            'date': last.name.strftime('%Y-%m-%d'),
            'close': float(last['close']),
            'volume': int(last['volume'])
        })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 거래량 기준 정렬 및 상위 20개 저장
# -----------------------------
if volume_rank:
    df_top = pd.DataFrame(volume_rank)

    # ✅ 최신 거래일 기준 필터링 (혹시 날짜가 엇갈린 종목 제외)
    latest_date = df_top['date'].max()
    df_top = df_top[df_top['date'] == latest_date]

    df_top.sort_values(by='volume', ascending=False, inplace=True)
    df_top20 = df_top.head(20).reset_index(drop=True)

    print(f"📊 [거래량 TOP20] ({latest_date}) 상위 20개 종목 리스트:\n")
    print(df_top20[['code', 'name', 'close', 'volume']].to_string(index=False))
    print(f"\n총 {len(df_top20)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_TOP20_VOLUME_US"
    signal_type = "FLOW"

    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=today,
        signal_type=signal_type,
        total_return=None,
        total_risk=None,
        total_sharpe=None
    )

    print(f"🧾 [RESULT_ID] 이번 실행으로 저장된 result_id = {result_id}\n")

    for idx, row in enumerate(df_top20.itertuples(), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row.code,
            name=row.name,
            action='FLOW',
            price=row.close,
            old_price=None,
            returns=row.volume,   # ✅ 거래량을 returns에 임시 저장
            rank_order=idx,
            signal_date=row.date
        )

    print(f"ROWCOUNT={len(df_top20)}")
    print(f"CODECOUNT={len(df_top20)}")
    print(f"RESULT_ID={result_id}")
    print(f"✅ [DB저장완료] {len(df_top20)}건 (result_id={result_id})")

else:
    print("\n💤 최근 거래량 데이터 부족 — 상위 종목 추출 불가.")
