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

stocks = set(company_df["code"])   # 빠른 조회를 위해 set 사용

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=400)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "WEEKLY_52W_NEW_LOW_US"

# =======================================================
# 2. 전체 가격 1회 조회 (초고속)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 미국 종목만 필터링
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

low_list = []

# =======================================================
# 3. 종목별 groupby로 주봉 변환 + 신저가 판단
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 260:
        # 주봉 최소 52개 확보 불가
        continue

    df = group.copy()
    df = df.sort_values("date")
    df.set_index("date", inplace=True)

    # 주봉 변환
    weekly = pd.DataFrame()
    weekly["open"] = df["open"].resample("W-SAT").first()
    weekly["high"] = df["high"].resample("W-SAT").max()
    weekly["low"] = df["low"].resample("W-SAT").min()
    weekly["close"] = df["close"].resample("W-SAT").last()
    weekly["volume"] = df["volume"].resample("W-SAT").sum()
    weekly.dropna(inplace=True)

    if len(weekly) < 52:
        continue

    # 52주 최저 종가
    weekly["LOW_52_CLOSE"] = weekly["close"].rolling(window=52).min()

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    # ⭐ 조건: 신저가 첫 발생 + 10달러 이상
    if (
            last["LOW_52_CLOSE"] >= last["close"] >= 10  # 이번주 종가가 52주 신저가 이하
            and prev["close"] > prev["LOW_52_CLOSE"]
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
            "special_value": float(last["LOW_52_CLOSE"])  # 52주 최저 종가
        })

# =======================================================
# 4. 정렬 + 저장
# =======================================================
if low_list:

    df_low = pd.DataFrame(low_list).sort_values(by="close", ascending=True)
    print("\n📉 [US] 52주 신저가 ‘첫 발생’ 주봉 종목\n")
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
            signal_date=row["date"],
            special_value=row["special_value"]  # 52주 최저치 저장
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(low_list)}\n")

else:
    print("\n😴 52주 신저가 종목 없음 — 저장 생략\n")
