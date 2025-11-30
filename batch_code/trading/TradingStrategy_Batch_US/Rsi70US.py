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
stocks = list(company_df["code"])

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "RSI_70_OVERHEATED_US"

rsi_list = []
# =======================================================
# 2. RSI 계산 함수
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
# 3. 전체 일봉 데이터 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

# =======================================================
# 4. 그룹별 RSI 계산
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

    diff = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: RSI 70 이상 + 종가 ≥ 10,000
    if last["rsi"] >= 70 and last["close"] >= 10:

        rsi_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": round(diff, 2),
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(last["rsi"]), 2)
        })


# =======================================================
# 4. 정렬 + DB 저장
# =======================================================
if rsi_list:

    df_rsi = pd.DataFrame(rsi_list).sort_values(by="special_value", ascending=False)
    print("\n📈 [RSI] 70 이상 과열 종목 (종가 ≥ 10,000원)\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    today = datetime.now().strftime("%Y-%m-%d")

    # SUMMARY 저장
    last_date = df_rsi.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(rsi_list)
    )

    # DETAIL 저장
    for row in rsi_list:
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["diff"],            # 공통 등락률
            volume=row["volume"],
            special_value=row["special_value"],  # ★ RSI 값
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(rsi_list)}\n")

else:
    print("\n💤 RSI 70 이상 과열 종목 없음 — 저장 생략\n")
