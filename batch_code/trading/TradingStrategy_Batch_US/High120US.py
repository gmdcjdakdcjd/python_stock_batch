import pandas as pd
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

# set → 빠른 코드 필터링
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

# 안전하게 200일 조회
start_date = (pd.Timestamp.today() - pd.DateOffset(days=200)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_120D_NEW_HIGH_US"

# =======================================================
# 2. 전체 시세 1회 조회 (초고속)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# 미국 종목만 남기기
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

high_list = []

# =======================================================
# 3. 종목별 groupby 후 120일 신고가 탐색
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 120:
        continue

    df = group.copy()
    df = df.sort_values("date")
    df.set_index("date", inplace=True)

    df["HIGH_120_CLOSE"] = df["close"].rolling(window=120).max()

    prev = df.iloc[-2]
    last = df.iloc[-1]

    # 120일 신고가 첫 발생
    if (
        last["close"] >= last["HIGH_120_CLOSE"] and
        prev["close"] < prev["HIGH_120_CLOSE"] and
        last["close"] >= 10
    ):
        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        high_list.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": df.index[-1].strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last.get("volume", 0)),
            "diff": diff,
            "special_value": float(last["HIGH_120_CLOSE"])
        })


# =======================================================
# 4. 정렬 + Mongo 저장
# =======================================================
if high_list:

    df_high = pd.DataFrame(high_list).sort_values(by="close", ascending=False)
    print("\n🚀 [US] 120일 종가 신고가 ‘첫 발생’ 종목\n")
    print(df_high.to_string(index=False))
    print(f"\n총 {len(df_high)}건 감지됨.\n")

    last_date = df_high.iloc[0]["date"]  # 미국 종가일
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(high_list)
    )

    for row in high_list:
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
    print(f"ROWCOUNT  = {len(high_list)}\n")

else:
    print("\n😴 120일 종가 신고가 ‘첫 발생’ 종목 없음 — 저장 생략\n")
