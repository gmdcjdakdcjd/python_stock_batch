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
stocks = set(company_df["code"])   # 빠른 조회 위해 set 사용

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=400)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime('%Y-%m-%d')
strategy_name = "WEEKLY_52W_NEW_HIGH_KR"

# =======================================================
# 2. MongoDB 전체 일봉 1회 조회 (핵심)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 대상 종목만 필터링
df_all = df_all[df_all["code"].isin(stocks)]

# 날짜 정렬
df_all = df_all.sort_values(["code", "date"])

weekly_candidates = []

# =======================================================
# 3. 종목별 주봉 변환 + 52주 신고가 계산
# =======================================================
for code, group in df_all.groupby("code"):

    group = group.set_index("date")

    if len(group) < 260:
        continue

    # 주봉 변환
    weekly = pd.DataFrame({
        "open": group["close"].resample("W-SAT").first(),
        "high": group["close"].resample("W-SAT").max(),
        "low":  group["close"].resample("W-SAT").min(),
        "close": group["close"].resample("W-SAT").last(),
        "volume": group["volume"].resample("W-SAT").sum(),
    }).dropna()

    if len(weekly) < 52:
        continue

    # 52주 신고가
    weekly["HIGH_52_CLOSE"] = weekly["close"].rolling(52).max()

    prev = weekly.iloc[-2]   # 지난 주
    last = weekly.iloc[-1]   # 이번 주

    # 주봉 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # 신고가 첫 발생 조건
    if (
        last["close"] >= last["HIGH_52_CLOSE"] and
        prev["close"] < prev["HIGH_52_CLOSE"] and
        last["close"] >= 10000
    ):
        weekly_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": last["close"],
            "prev_close": prev["close"],
            "volume": last["volume"],
            "diff": diff,
            "special_value": float(last["HIGH_52_CLOSE"])
        })

# =======================================================
# 4. 저장
# =======================================================
if weekly_candidates:

    df_weekly = pd.DataFrame(weekly_candidates).sort_values(by="close", ascending=False)
    print("\n🚀 [주봉] 52주 신고가 ‘첫 발생’ 종목\n")
    print(df_weekly.to_string(index=False))
    print(f"\n총 {len(df_weekly)}건 감지됨.\n")

    last_date = df_weekly.iloc[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_weekly)
    )

    for row in df_weekly.to_dict("records"):
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["diff"],
            volume=row["volume"],
            action=strategy_name,
            signal_date=row["date"],
            special_value=row["special_value"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_weekly)}\n")

else:
    print("\n😴 주봉 52주 신고가 ‘첫 발생’ 종목 없음 — 저장 생략\n")
