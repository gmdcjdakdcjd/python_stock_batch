import pandas as pd
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
# 2️⃣ 전일 대비 7% 이상 상승 종목 탐색
# -----------------------------
rise_candidates = []

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

        if len(df) < 2:
            continue

        # ✅ 종가 컬럼 선택 (adj_close > close)
        price_col = 'adj_close' if 'adj_close' in df.columns else 'close'
        prev_close = df.iloc[-2][price_col]
        last_close = df.iloc[-1][price_col]

        if pd.isna(prev_close) or pd.isna(last_close) or prev_close == 0:
            continue

        # ✅ 전일 대비 상승률 계산
        rate = ((last_close - prev_close) / prev_close) * 100

        # ✅ 조건: 전일 대비 +7% 이상 & 종가 $10 이상
        if rate >= 7 and last_close >= 10:
            rise_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': df.index[-1].strftime('%Y-%m-%d'),
                'prev_close': round(prev_close, 2),
                'close': round(last_close, 2),
                'rate(%)': round(rate, 2)
            })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 3️⃣ 결과 출력 및 DB 저장
# -----------------------------
if rise_candidates:
    df_rise = pd.DataFrame(rise_candidates)
    df_rise.sort_values(by='rate(%)', ascending=False, inplace=True)

    print("📈 [일봉] 전일 대비 7% 이상 상승 & 종가 ≥ $10 종목 리스트:\n")
    print(df_rise.to_string(index=False))
    print(f"\n총 {len(df_rise)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "DAILY_RISE_SPIKE_US"
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
    for idx, row in enumerate(df_rise.to_dict(orient='records'), start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row['code'],
            name=row['name'],
            action='BUY',
            price=row['close'],
            old_price=row['prev_close'],
            returns=row['rate(%)'],
            rank_order=idx,
            signal_date=row['date']
        )
    print(f"ROWCOUNT={len(df_rise)}")
    print(f"CODECOUNT={len(df_rise)}")
    print(f"RESULT_ID={result_id}")
    print(f"✅ [DB저장완료] {len(df_rise)}건 (result_id={result_id})")

else:
    print("\n💤 [일봉] 전일 대비 7% 이상 상승한 종목 없음 — DB 저장 생략.")
