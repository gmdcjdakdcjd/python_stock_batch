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

stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=200)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_120D_NEW_LOW_US"

# =======================================================
# 2. 전체 가격 1번만 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 종목 필터링 + 정렬
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

low_list = []

# =======================================================
# 3. 종목별 120일 신저가 첫 발생 탐지
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 120:
        continue

    df = group.copy()
    df = df.sort_values("date")
    df.set_index("date", inplace=True)

    # 120일 최저 종가
    df["LOW_120_CLOSE"] = df["close"].rolling(window=120).min()

    prev = df.iloc[-2]
    last = df.iloc[-1]

    # 오늘 처음 120일 신저가 돌파 + 종가 ≥ 10달러
    if (
            last["LOW_120_CLOSE"] >= last["close"] >= 10
            and prev["close"] > prev["LOW_120_CLOSE"]
    ):
        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        low_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last["volume"]),
            "diff": diff,
            "special_value": float(last["LOW_120_CLOSE"])   # ⭐ 120일 신저가 저장
        })

# =======================================================
# 4. 정렬 + 저장
# =======================================================
if low_list:

    df_low = pd.DataFrame(low_list).sort_values(by="close", ascending=True)
    print("\n📉 [미국] 120일 종가 신저가 첫 발생 종목\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    last_date = df_low.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(low_list)
    )

    for row in low_list:
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=row["special_value"],
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(low_list)}\n")

else:
    print("\n😴 120일 종가 신저가 첫 발생 종목 없음 — 저장 생략\n")
