import pandas as pd
import warnings
from API import AnalyzeUS as Analyzer
from datetime import datetime, timedelta
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = Analyzer.MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=400)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "WEEKLY_52W_NEW_HIGH_US"

# =======================================================
# 2. 전체 가격 데이터 1회 조회 (초고속)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

new_high_list = []

# =======================================================
# 3. 종목별 주봉 변환 + 52주 신고가 탐색
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 260:
        continue

    df = group.copy()
    df = df.sort_values("date")

    df.set_index("date", inplace=True)

    # ------------------------
    # 주봉 변환
    # ------------------------
    weekly = pd.DataFrame()
    weekly["open"] = df["open"].resample("W-SAT").first()
    weekly["high"] = df["high"].resample("W-SAT").max()
    weekly["low"] = df["low"].resample("W-SAT").min()
    weekly["close"] = df["close"].resample("W-SAT").last()
    weekly["volume"] = df["volume"].resample("W-SAT").sum()
    weekly.dropna(inplace=True)

    if len(weekly) < 52:
        continue

    weekly["HIGH_52_CLOSE"] = weekly["close"].rolling(window=52).max()

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # ------------------------
    # “첫” 52주 신고가 돌파 조건
    # ------------------------
    if (
        last["close"] >= last["HIGH_52_CLOSE"] and
        prev["close"] < prev["HIGH_52_CLOSE"] and
        last["close"] >= 10     # 최소 가격 조건 ($10)
    ):
        new_high_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last["volume"]),
            "diff": diff,
            "special_value": float(last["HIGH_52_CLOSE"])   # ★ 신고가 값
        })


# =======================================================
# 4. 정렬 + 저장
# =======================================================
if new_high_list:

    df_weekly = pd.DataFrame(new_high_list).sort_values(by="close", ascending=False)

    print("\n🚀 [US] 주봉 52주 종가 신고가 ‘첫 발생’ 종목\n")
    print(df_weekly.to_string(index=False))
    print(f"\n총 {len(df_weekly)}건 감지됨.\n")

    last_date = df_weekly.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_weekly)
    )

    for row in new_high_list:
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
            special_value=row["special_value"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_weekly)}\n")

else:
    print("\n😴 주봉 52주 신고가 ‘첫 발생’ 없음 — 저장 생략\n")
