import pandas as pd
import warnings
from API import ETFAnalyzeKR
from datetime import datetime
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = ETFAnalyzeKR.MarketDB()
etf_df = mk.get_etf_info_optimization()

etf = set(etf_df["code"])
name_map = dict(zip(etf_df["code"], etf_df["name"]))

print(f"\n총 {len(etf)}개 ETF 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "ETF_TOP20_VOLUME_KR"


# =======================================================
# 2. 전체 가격 한 번에 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(etf)]
df_all = df_all.sort_values(["code", "date"])

volume_list = []


# =======================================================
# 3. ETF별 거래량 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    df = group.sort_values("date")

    prev = df.iloc[-2]
    last = df.iloc[-1]

    if pd.isna(last["volume"]) or last["volume"] == 0:
        continue

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    volume_list.append({
        "code": code,
        "name": name_map.get(code, "UNKNOWN"),
        "date": last["date"].strftime("%Y-%m-%d"),
        "prev_close": float(prev["close"]),
        "close": float(last["close"]),
        "diff": round(rate, 2),
        "volume": float(last.get("volume", 0))
    })


# =======================================================
# 4. 정렬 + TOP20 + DB 저장
# =======================================================
if volume_list:

    df_final = (
        pd.DataFrame(volume_list)
        .sort_values(by="volume", ascending=False)
        .head(20)
    )

    print("\n📊 [KR ETF] 거래량 TOP20 리스트\n")
    print(df_final.to_string(index=False))
    print(f"\n총 {len(df_final)}건 감지됨.\n")

    # SUMMARY 저장
    last_date = df_final.iloc[0]["date"]
    result_id = save_strategy_summary(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_final)
    )

    # DETAIL 저장 (special_value = rank)
    for rank, row in enumerate(df_final.to_dict("records"), start=1):
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
            special_value=rank
        )

    print("\n⚡ MongoDB 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_final)}\n")

else:
    print("\n😴 KR ETF 거래량 TOP20 없음 — 저장 생략\n")
