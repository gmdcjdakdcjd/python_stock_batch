import pandas as pd
import warnings
from API import ETFAnalyzer
from datetime import datetime
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal

warnings.filterwarnings("ignore", category=RuntimeWarning)

# -----------------------------
# 1️⃣ DB 연결 및 기본 세팅
# -----------------------------
mk = ETFAnalyzer.MarketDB()
company = mk.get_etf_info_optimization()
stocks = list(company['name'])
name_to_code = {v: k for k, v in mk.codes.items()}

print(f"총 {len(stocks)}개 ETF 스캔 시작...")

# -----------------------------
# 2️⃣ 오늘 기준 거래량 상위 20개 추출
# -----------------------------
volume_rank = []
start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')  # 최근 5일 안전 버퍼

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
            'close': last['close'],
            'volume': int(last['volume'])
        })
    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 거래량 상위 20개 정렬 및 DB 저장
# -----------------------------
if volume_rank:
    df_etf = pd.DataFrame(volume_rank)
    df_etf.sort_values(by='volume', ascending=False, inplace=True)
    df_top20 = df_etf.head(20)

    print("📊 [ETF] 거래량 상위 20개 리스트:\n")
    print(df_top20.to_string(index=False))
    print(f"\n총 {len(df_top20)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "ETF_TOP20_VOLUME"
    signal_type = "FLOW"

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

    # 2) 상세 저장 (기존 구조 그대로 재사용)
    for idx, row in enumerate(df_top20.itertuples(), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row.code,
            name=row.name,
            action='FLOW',
            price=row.close,
            old_price=None,
            returns=None,
            rank_order=idx,
            signal_date=row.date
        )

    print(f"ROWCOUNT={len(df_top20)}")
    print(f"CODECOUNT={len(df_top20)}")
    print(f"RESULT_ID={result_id}")

    print(f"✅ [DB저장완료] {len(df_top20)}건 (result_id={result_id})")

else:
    print("\n💤 [ETF] 거래량 상위 ETF 없음 — DB 저장 생략.")
