import pandas as pd
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
# 2️⃣ 오늘 기준 거래량 상위 20개 종목 추출
# -----------------------------
volume_rank = []
start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')  # 최근 5일 버퍼 조회

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty:
            continue

        last = df.iloc[-1]
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
    df_top.sort_values(by='volume', ascending=False, inplace=True)
    df_top20 = df_top.head(20)

    print("📊 [일봉] 거래량 상위 20개 종목 리스트:\n")
    print(df_top20[['code', 'name', 'date', 'close', 'volume']].to_string(index=False))
    print(f"\n총 {len(df_top20)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_TOP20_VOLUME"
    signal_type = "FLOW"

    # 1) 전략 요약 저장
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
    for idx, row in enumerate(df_top20.itertuples(), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row.code,
            name=row.name,
            action='FLOW',
            price=row.close,
            old_price=None,
            returns=row.volume,  # ✅ 거래량 저장
            rank_order=idx,
            signal_date=row.date
        )

    print(f"ROWCOUNT={len(df_top20)}")
    print(f"CODECOUNT={len(df_top20)}")
    print(f"RESULT_ID={result_id}")

    print(f"✅ [DB저장완료] {len(df_top20)}건 (result_id={result_id})")

else:
    print("\n💤 [일봉] 거래량 상위 20개 종목 없음 — DB 저장 생략.")
