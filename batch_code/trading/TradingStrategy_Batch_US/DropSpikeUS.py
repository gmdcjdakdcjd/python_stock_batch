import pandas as pd
import warnings
from datetime import datetime
from API import AnalyzeUS as Analyzer
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = Analyzer.MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])  # 빠른 contains 검색용 set

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_DROP_SPIKE_US"

# =======================================================
# 2. 전체 가격 데이터 한 번에 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

drop_list = []

# =======================================================
# 3. 전일 대비 등락률 계산 (초고속)
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    prev = group.iloc[-2]
    last = group.iloc[-1]

    # 등락률 계산
    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: -7% 이하 하락 + 종가 ≥ $10
    if rate <= -7 and last["close"] >= 10:
        drop_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last["date"].strftime("%Y-%m-%d"),
            "prev_close": float(prev["close"]),
            "close": float(last["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0))
        })

# =======================================================
# 4. 정렬 + 저장
# =======================================================
if drop_list:

    df_drop = pd.DataFrame(drop_list).sort_values(by="rate", ascending=True)

    print("\n📉 [미국] 전일 대비 7% 이상 하락 종목\n")
    print(df_drop.to_string(index=False))

    # SUMMARY 저장
    last_date = df_drop.iloc[0]["date"]  # 미국 종가일
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

    print(f"\n⚡ 저장 완료 — RESULT_ID = {result_id}, ROWCOUNT = {len(df_drop)}\n")

else:
    print("\n😴 전일 대비 7% 이상 하락 종목 없음 — 저장 생략\n")
