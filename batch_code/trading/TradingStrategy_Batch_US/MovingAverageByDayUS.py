import pandas as pd
import numpy as np
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
name_map = mk.codes  # code → name

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

# 최근 6개월 조회 → 일봉 60일선 충분
start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")

strategy_name = "DAILY_TOUCH_MA60_US"

# =======================================================
# 2. 전체 가격 1번만 MongoDB에서 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

touch_list = []

# =======================================================
# 3. 종목별 60일선 터치 탐색 (빠름)
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 60:
        continue

    df = group.copy()
    df = df.sort_values("date")
    df.set_index("date", inplace=True)

    # MA60
    df["MA60"] = df["close"].rolling(60).mean()

    prev = df.iloc[-2]
    last = df.iloc[-1]

    if np.isnan(prev["MA60"]) or prev["MA60"] == 0:
        continue

    # 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # 60일선 터치 조건 (±1%)
    touch_rate = ((last["close"] - prev["MA60"]) / prev["MA60"]) * 100

    if -1.0 <= touch_rate <= 1.0 and last["close"] >= 10:

        touch_list.append({
            "code": code,
            "name": name_map.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(prev["MA60"]), 2)
        })

# =======================================================
# 4. 저장
# =======================================================
if touch_list:

    df_touch = pd.DataFrame(touch_list).sort_values(by="diff")
    print("\n📊 [US] 일봉 60일선 터치 종목\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    # SUMMARY 저장
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=today_str,
        total_data=len(df_touch)
    )

    # DETAIL 저장
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
            special_value=row["special_value"],  # MA60
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT = {len(df_touch)}\n")

else:
    print("\n💤 [일봉] 60일선 터치 종목 없음 — 저장 생략\n")
