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

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_RISE_SPIKE_KR"

# =======================================================
# 2. MongoDB 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 종목 필터링
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

rise_candidates = []

# =======================================================
# 3. 상승 스파이크 계산
# =======================================================
for code, group in df_all.groupby("code"):

    group = group.sort_values("date").set_index("date")

    if len(group) < 2:
        continue

    prev = group.iloc[-2]      # 어제
    last = group.iloc[-1]      # 오늘

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: 전일 대비 +7% AND 종가 10,000 이상
    if rate >= 7 and last["close"] >= 10000:

        rise_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "prev_close": float(prev["close"]),
            "close": float(last["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0))
        })


# =======================================================
# 4. 정렬 + 저장
# =======================================================
if rise_candidates:

    df_rise = pd.DataFrame(rise_candidates).sort_values(by="rate", ascending=False)

    print("\n📈 [일봉] 전일 대비 7% 이상 상승 종목 목록\n")
    print(df_rise.to_string(index=False))
    print(f"\n총 {len(df_rise)}건 감지됨.\n")

    last_date = df_rise.iloc[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_rise)
    )

    # DETAIL 저장 (상승률 순위 포함)
    for rank, row in enumerate(df_rise.to_dict("records"), start=1):
        save_strategy_detail(
            result_id=result_id,
            code=row["code"],
            name=row["name"],
            action=strategy_name,
            price=row["close"],
            prev_close=row["prev_close"],
            diff=row["rate"],      # 상승률
            volume=row["volume"],
            signal_date=row["date"],
            special_value=rank     # 순위 저장
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_rise)}\n")

else:
    print("\n😴 전일 대비 7% 이상 상승 종목 없음 — 저장 생략\n")
