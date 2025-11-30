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
strategy_name = "DAILY_TOUCH_MA60_KR"

# =======================================================
# 2. MongoDB 전체 일봉을 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

touch_candidates = []

# =======================================================
# 3. 종목별 60일선 터치 계산
# =======================================================
for code, group in df_all.groupby("code"):

    group = group.sort_values("date").set_index("date")

    if len(group) < 60:
        continue

    group["MA60"] = group["close"].rolling(window=60).mean()

    prev = group.iloc[-2]
    last = group.iloc[-1]

    if np.isnan(prev["MA60"]) or prev["MA60"] == 0:
        continue

    # 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # MA60 기준 터치 여부 (±1%)
    touch_rate = ((last["close"] - prev["MA60"]) / prev["MA60"]) * 100

    if -1.0 <= touch_rate <= 1.0 and last["close"] >= 10000:

        touch_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),     # 어제 종가
            "diff": diff,                           # 공통 등락률
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(prev["MA60"]), 2)   # MA60 저장
        })


# =======================================================
# 4. 저장
# =======================================================
if touch_candidates:

    df_touch = pd.DataFrame(touch_candidates).sort_values(by="diff")
    print("\n📊 [일봉] 60일선 터치 종목 리스트\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = touch_candidates[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(touch_candidates)
    )

    for row in df_touch.to_dict("records"):
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
    print(f"ROWCOUNT  = {len(df_touch)}\n")

else:
    print("\n💤 [일봉] 60일선 터치 종목 없음 — 저장 생략\n")
