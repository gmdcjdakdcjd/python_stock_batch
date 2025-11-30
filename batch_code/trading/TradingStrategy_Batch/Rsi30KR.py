import pandas as pd
import numpy as np
import warnings
from datetime import datetime
from API import AnalyzeKR
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = AnalyzeKR.MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "RSI_30_UNHEATED_KR"

rsi_candidates = []

# =======================================================
# 2. RSI 함수
# =======================================================
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=14).mean()
    avg_loss = loss.rolling(window=period, min_periods=14).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


# =======================================================
# 3. 전체 일봉 데이터를 단 1번만 가져오기
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

# =======================================================
# 4. 그룹별 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    group = group.sort_values("date").set_index("date")

    # RSI 계산
    group["rsi"] = compute_rsi(group["close"])

    last = group.iloc[-1]
    prev = group.iloc[-2]

    if pd.isna(last["rsi"]):
        continue

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건 충족
    if last["rsi"] <= 30 and last["close"] >= 10000:

        rsi_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(last["rsi"]), 2)  # RSI 값만 저장
        })


# =======================================================
# 5. 저장
# =======================================================
if rsi_candidates:

    df_rsi = pd.DataFrame(rsi_candidates).sort_values(by="special_value")
    print("\n📉 [RSI] 30 이하 & 종가 10,000 이상 종목\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    last_date = rsi_candidates[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(rsi_candidates)
    )

    for row in rsi_candidates:
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["rate"],
            volume=row["volume"],
            special_value=row["special_value"],   # RSI 저장
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(rsi_candidates)}\n")

else:
    print("\n💤 RSI 30 이하 종목 없음 — 저장 생략\n")
