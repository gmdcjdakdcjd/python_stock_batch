import pandas as pd
import numpy as np
import warnings
from API import AnalyzeKR
from datetime import datetime
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
strategy_name = "DAILY_BB_UPPER_TOUCH_KR"

touch_candidates = []


# =======================================================
# 2. 전체 가격 데이터 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])


# =======================================================
# 3. 그룹별 볼린저밴드 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    group = group.sort_values("date").set_index("date")

    # 볼린저밴드 계산
    group["MA20"] = group["close"].rolling(window=20).mean()
    group["STDDEV"] = group["close"].rolling(window=20).std()
    group["UPPER"] = group["MA20"] + (group["STDDEV"] * 2)

    if pd.isna(group["UPPER"].iloc[-1]):
        continue

    prev = group.iloc[-2]
    last = group.iloc[-1]

    close_price = last["close"]
    upper_band = last["UPPER"]

    # 등락률 (어제 대비)
    diff = round(((close_price - prev["close"]) / prev["close"]) * 100, 2)

    # 상단선 대비 괴리율
    gap_rate = ((close_price - upper_band) / upper_band) * 100

    # 조건:
    # - gap_rate ±1%
    # - 종가 ≥ 10,000원
    if -1.0 <= gap_rate <= 1.0 and close_price >= 10000:
        touch_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(close_price),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(upper_band), 2)   # ⭐ 볼린저 상단 저장
        })


# =======================================================
# 4. 결과 저장
# =======================================================
if touch_candidates:

    df_touch = pd.DataFrame(touch_candidates).sort_values(by="diff", ascending=False)
    print("\n📈 [일봉] 볼린저 상단 터치 종목 리스트 (±1%)\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = touch_candidates[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(touch_candidates)
    )

    for row in touch_candidates:
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=row["special_value"],   # ⭐ 볼린저 상단값
            signal_date=row["date"]
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(touch_candidates)}\n")

else:
    print("\n💤 볼린저 상단 터치 종목 없음 — 저장 생략\n")
