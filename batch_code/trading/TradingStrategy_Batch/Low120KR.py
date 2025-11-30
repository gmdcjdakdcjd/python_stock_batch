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

start_date = (pd.Timestamp.today() - pd.DateOffset(days=200)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_120D_NEW_LOW_KR"

# =======================================================
# 2. MongoDB 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 스캔 대상 필터링
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

low_candidates = []

# =======================================================
# 3. 종목별 120일 신저가 판단 (메모리 처리)
# =======================================================
for code, group in df_all.groupby("code"):

    group = group.set_index("date").sort_index()

    if len(group) < 120:
        continue

    # 120일 신저가
    group["LOW_120_CLOSE"] = group["close"].rolling(120).min()

    prev = group.iloc[-2]
    last = group.iloc[-1]

    # ⭐ 조건: 오늘 처음 신저가 갱신 + 종가 10,000원 이상
    if last["close"] <= last["LOW_120_CLOSE"] and prev["close"] > prev["LOW_120_CLOSE"] and last["close"] >= 10000:

        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        low_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last.get("volume", 0)),
            "diff": diff,
            "special_value": float(last["LOW_120_CLOSE"])
        })

# =======================================================
# 4. 저장
# =======================================================
if low_candidates:

    df_low = pd.DataFrame(low_candidates).sort_values(by="close", ascending=True)

    print("\n📉 [일봉] 120일 종가 신저가 ‘첫 발생’ 종목\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    last_date = df_low.iloc[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_low)
    )

    for row in df_low.to_dict("records"):
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
    print(f"ROWCOUNT  = {len(df_low)}\n")

else:
    print("\n😴 120일 종가 신저가 발생 종목 없음 — 저장 생략\n")
