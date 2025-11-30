import pandas as pd
import numpy as np
import warnings
from API import AnalyzeUS as Analyzer
from datetime import datetime
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail

warnings.filterwarnings("ignore", category=RuntimeWarning)

# -----------------------------
# 1️⃣ 기본 세팅
# -----------------------------
mk = Analyzer.MarketDB()
company = mk.get_comp_info_optimization()
stocks = list(company["code"])

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_TOP20_VOLUME_US"
volume_candidates = []


# =======================================================
# 2. 전체 데이터 1번 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

# =======================================================
# 3. 그룹별 어제/오늘 비교
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    group = group.sort_values("date").set_index("date")

    prev = group.iloc[-2]
    last = group.iloc[-1]

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    volume_candidates.append({
        "code": code,
        "name": mk.codes.get(code, "UNKNOWN"),
        "date": last.name.strftime("%Y-%m-%d"),
        "prev_close": float(prev["close"]),
        "close": float(last["close"]),
        "rate": round(rate, 2),
        "volume": float(last["volume"])
    })

# =======================================================
# 4. TOP20 추출
# =======================================================
if volume_candidates:

    df_top20 = (
        pd.DataFrame(volume_candidates)
        .sort_values(by="volume", ascending=False)
        .head(20)
    )

    print("\n📊 [일봉] 거래량 TOP20 종목 리스트\n")
    print(df_top20[["code", "name", "date", "close", "volume"]].to_string(index=False))
    print(f"\n총 {len(df_top20)}건 감지됨.\n")

    # --------------------------
    # SUMMARY 저장
    # --------------------------
    last_date = df_top20.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_top20)
    )

    # --------------------------
    # DETAIL 저장 (순위 저장)
    # --------------------------
    for rank, row in enumerate(df_top20.to_dict("records"), start=1):
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["rate"],
            volume=row["volume"],
            signal_date=row["date"],
            special_value=rank    # ⭐ 거래량 순위 저장
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_top20)}\n")

else:
    print("\n😴 거래량 TOP20 없음 — 저장 생략\n")
