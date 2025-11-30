import pandas as pd
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
stocks = set(company_df["code"])    # 빠른 조회 위해 set 사용

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_DROP_SPIKE_KR"

# =======================================================
# 2. MongoDB에서 전체 가격 한 번에 조회 (핵심)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 필요한 종목만 필터링 (우량 필터)
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

drop_candidates = []

# =======================================================
# 3. 종목별 하락률 계산 (메모리 처리 → 초고속)
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    prev = group.iloc[-2]  # 어제
    last = group.iloc[-1]  # 오늘

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    if rate <= -7 and last["close"] >= 10000:
        drop_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last["date"].strftime("%Y-%m-%d"),
            "prev_close": float(prev["close"]),
            "close": float(last["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0))
        })


# =======================================================
# 4. 출력 + Mongo 저장
# =======================================================
if drop_candidates:

    df_drop = pd.DataFrame(drop_candidates).sort_values(by="rate", ascending=True)

    print("\n📉 [일봉] 전일 대비 7% 이상 하락 종목\n")
    print(df_drop.to_string(index=False))

    # SUMMARY 저장
    last_date = df_drop.iloc[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_drop)
    )

    # DETAIL 저장 (rank 포함)
    for rank, row in enumerate(df_drop.to_dict("records"), start=1):
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
            special_value=rank
        )

    print(f"\n⚡ 저장 완료 → RESULT_ID = {result_id}, ROWCOUNT = {len(df_drop)}\n")

else:
    print("\n😴 전일 대비 7% 이상 하락 종목 없음 — 저장 생략\n")
