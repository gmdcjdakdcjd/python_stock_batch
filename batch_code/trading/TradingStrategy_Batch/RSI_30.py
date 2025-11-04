import pandas as pd
import numpy as np
import warnings
from API import Analyzer
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

print(f"총 {len(stocks)}개 종목 스캔 시작...\n")

# -----------------------------
# 2️⃣ RSI 계산 함수
# -----------------------------
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# -----------------------------
# 3️⃣ RSI 30 이하 & 종가 10,000 이상 탐색
# -----------------------------
rsi_candidates = []
start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

for s in stocks:
    try:
        df = mk.get_daily_price(s, start_date)
        if df is None or df.empty or len(df) < 20:
            continue

        df['rsi'] = compute_rsi(df['close'])
        last = df.iloc[-1]
        date = df.index[-1].strftime('%Y-%m-%d')

        # ✅ 조건: RSI ≤ 30 AND 종가 ≥ 10,000원
        if last['rsi'] <= 30 and last['close'] >= 10000:
            rsi_candidates.append({
                'code': name_to_code.get(s, 'UNKNOWN'),
                'name': s,
                'date': date,
                'close': round(last['close'], 2),
                'rsi': round(last['rsi'], 2)
            })

    except Exception as e:
        print(f"{s} 처리 실패: {e}")

# -----------------------------
# 4️⃣ 결과 출력 및 DB 저장
# -----------------------------
if rsi_candidates:
    df_rsi = pd.DataFrame(rsi_candidates)
    df_rsi.sort_values(by='rsi', inplace=True)

    print("📉 [RSI] 30 이하 & 종가 10,000원 이상 종목 리스트:\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    # ✅ DB 저장
    today = datetime.now().strftime('%Y-%m-%d')
    strategy_name = "RSI_30_UNHEATED"
    signal_type = "BUY"

    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=today,
        signal_type=signal_type,
        total_return=None,
        total_risk=None,
        total_sharpe=None
    )

    for idx, row in enumerate(rsi_candidates, start=1):
        save_strategy_signal(
            result_id=result_id,
            code=row['code'],
            name=row['name'],
            action='BUY',
            price=row['close'],
            old_price=row['rsi'],  # RSI 값 저장
            returns=None,
            rank_order=idx,
            signal_date=row['date']
        )
    print(f"ROWCOUNT={len(rsi_candidates)}")
    print(f"CODECOUNT={len(rsi_candidates)}")
    print(f"RESULT_ID={result_id}")
    print(f"✅ [DB저장완료] {len(rsi_candidates)}건 (result_id={result_id})")

else:
    print("\n💤 RSI 30 이하 & 종가 10,000원 이상 종목 없음 — DB 저장 생략.")
