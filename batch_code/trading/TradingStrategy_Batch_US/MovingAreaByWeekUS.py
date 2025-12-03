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

start_date = (pd.Timestamp.today() - pd.DateOffset(years=2)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime('%Y-%m-%d')
strategy_name = "WEEKLY_TOUCH_MA60_US"

# =======================================================
# 2. 전체 가격 1번 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

touch_list = []

# =======================================================
# 3. 종목별 주봉 변환 + 60주선 터치 탐색
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 60:
        continue

    df = group.copy()
    df = df.sort_values("date")
    df.set_index("date", inplace=True)

    # ----- 주봉 변환 -----
    weekly = pd.DataFrame()
    weekly["open"] = df["open"].resample("W-SAT").first()
    weekly["high"] = df["high"].resample("W-SAT").max()
    weekly["low"] = df["low"].resample("W-SAT").min()
    weekly["close"] = df["close"].resample("W-SAT").last()
    weekly["volume"] = df["volume"].resample("W-SAT").sum()
    weekly.dropna(inplace=True)

    if len(weekly) < 60:
        continue

    # 60주선 계산
    weekly["MA60"] = weekly["close"].rolling(window=60).mean()
    if len(weekly) < 2:
        continue

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    if np.isnan(prev["MA60"]) or prev["MA60"] == 0:
        continue

    # 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # 터치 조건
    if -1.0 <= diff <= 5.0 and last["close"] >= 10:
        touch_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),        # 지난주 주봉 종가
            "diff": diff,
            "volume": float(last["volume"]),
            "special_value": float(prev["MA60"])   # MA60 저장
        })

# =======================================================
# 4. 정렬 + DB 저장
# =======================================================
if touch_list:

    df_touch = pd.DataFrame(touch_list).sort_values(by="diff")
    print("\n📊 [주봉] 60주선 터치 종목 리스트\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = touch_list[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(touch_list)
    )

    for row in touch_list:
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
    print(f"ROWCOUNT  = {len(touch_list)}\n")

else:
    print("\n💤 60주선 터치 종목 없음 — 저장 생략\n")
