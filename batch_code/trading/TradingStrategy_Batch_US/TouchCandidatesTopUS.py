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

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_BB_UPPER_TOUCH_US"


# =======================================================
# 2. 전체 가격 한 번에 조회
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
# 3. 종목별 볼린저 상단 터치 탐색
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    df = group.sort_values("date").copy()
    df.set_index("date", inplace=True)

    # 볼린저 계산
    df["MA20"] = df["close"].rolling(20).mean()
    df["std"] = df["close"].rolling(20).std()
    df["upper"] = df["MA20"] + df["std"] * 2

    prev = df.iloc[-2]
    last = df.iloc[-1]

    upper_band = last["upper"]
    close_price = last["close"]

    if pd.isna(upper_band):
        continue

    # 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # 상단선 대비 괴리율
    gap_rate = ((close_price - upper_band) / upper_band) * 100

    # 조건
    if -1.0 <= gap_rate <= 1.0 and close_price >= 10:
        touch_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(close_price),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(upper_band), 2)
        })


# =======================================================
# 4. DB 저장
# =======================================================
if touch_list:

    df_touch = pd.DataFrame(touch_list).sort_values(by="diff", ascending=False)
    print("\n📉 [US] 일봉 볼린저 상단 터치 종목 리스트\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = df_touch.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_touch)
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
            special_value=row["special_value"],   # upper 밴드 값
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_touch)}\n")

else:
    print("\n💤 [일봉] 볼린저 상단 터치 종목 없음 — 저장 생략\n")
