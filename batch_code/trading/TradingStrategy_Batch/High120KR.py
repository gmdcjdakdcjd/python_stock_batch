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
stocks = set(company_df["code"])   # set → 빠른 검색

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=200)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_120D_NEW_HIGH_KR"

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

high_candidates = []

# =======================================================
# 3. 종목별 120일 신고가 스캔 (메모리 처리 → 초고속)
# =======================================================
for code, group in df_all.groupby("code"):

    group = group.set_index("date").sort_index()

    if len(group) < 120:
        continue

    # 120일 신고가 계산
    group["HIGH_120_CLOSE"] = group["close"].rolling(120).max()

    prev = group.iloc[-2]
    last = group.iloc[-1]

    # 신고가 첫 돌파 조건
    if (
        last["close"] >= last["HIGH_120_CLOSE"] and
        prev["close"] < prev["HIGH_120_CLOSE"] and
        last["close"] >= 10000
    ):
        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        high_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last.get("volume", 0)),
            "diff": diff,
            "special_value": float(last["HIGH_120_CLOSE"])   # 120일 신고가 저장
        })


# =======================================================
# 4. 저장
# =======================================================
if high_candidates:

    df_high = pd.DataFrame(high_candidates).sort_values(by="close", ascending=False)

    print("\n🚀 [일봉] 120일 종가 신고가 ‘첫 발생’ 종목\n")
    print(df_high.to_string(index=False))
    print(f"\n총 {len(df_high)}건 감지됨.\n")

    last_date = df_high.iloc[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_high)
    )

    for row in df_high.to_dict("records"):
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
    print(f"ROWCOUNT  = {len(df_high)}\n")

else:
    print("\n😴 120일 종가 신고가 ‘첫 발생’ 종목 없음 — 저장 생략\n")
