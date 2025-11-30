import pandas as pd
import numpy as np
import warnings
from API import AnalyzeUS as Analyzer
from datetime import datetime
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = Analyzer.MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])   # 빠른 조회

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "RSI_30_UNHEATED_US"

# =======================================================
# 2. 전체 가격 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

# =======================================================
# 3. RSI 계산 함수
# =======================================================
def compute_rsi(close_series, period=14):
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


rsi_list = []

# =======================================================
# 4. 종목별 RSI 계산 + 조건 탐색
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    df = group.sort_values("date").copy()
    df.set_index("date", inplace=True)

    df["rsi"] = compute_rsi(df["close"])

    prev = df.iloc[-2]
    last = df.iloc[-1]

    if pd.isna(last["rsi"]):
        continue

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: RSI 30 이하 + 종가 ≥ 100달러
    if last["rsi"] <= 30 and last["close"] >= 10:

        rsi_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(last["rsi"]), 2)   # RSI 값 저장
        })

# =======================================================
# 5. 정렬 + DB 저장
# =======================================================
if rsi_list:

    df_rsi = pd.DataFrame(rsi_list).sort_values(by="special_value")  # RSI 낮은 순
    print("\n📉 [US] RSI 30 이하 종목\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    # SUMMARY 저장
    last_date = df_rsi.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_rsi)
    )

    # DETAIL 저장
    for row in df_rsi.to_dict("records"):
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["rate"],
            volume=row["volume"],
            special_value=row["special_value"],  # RSI 숫자만 저장
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_rsi)}\n")

else:
    print("\n💤 RSI 30 이하 종목 없음 — 저장 생략\n")
