import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from datetime import datetime

# ---------------------------------------------
# 1️⃣ MongoDB 연결
# ---------------------------------------------
client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
db = client["investar"]

col_etf = db["etf_info_us"]           # 미국 ETF 메타 정보
col_price = db["etf_daily_price_us"]  # 미국 ETF 일별 시세

# ---------------------------------------------
# 2️⃣ 종목 코드 불러오기 (MongoDB)
# ---------------------------------------------
codes_cursor = col_etf.find(
    {"issuer": "BlackRock (iShares)"},
    {"code": 1, "name": 1}
)
codes_df = pd.DataFrame(list(codes_cursor))

print(f"불러온 ETF 수: {len(codes_df)}개")
print(codes_df.head())

# ---------------------------------------------
# 3️⃣ yfinance 수집 + MongoDB 저장
# ---------------------------------------------
total_count = 0
processed_codes = 0

for idx, row in codes_df.iterrows():
    code = row["code"]
    name = row["name"]

    print(f"\n[{idx+1}/{len(codes_df)}] {name} ({code}) 시세 수집 중...")
    processed_codes += 1

    try:
        df = yf.download(
            code,
            period="3d",
            interval="1d",
            auto_adjust=True,
            threads=False,
            progress=False
        )

        if df.empty:
            print(f"데이터 없음: {code}")
            continue

        df.reset_index(inplace=True)

        # MultiIndex flatten
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        # 날짜 문자열 처리
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["code"] = code

        df = df[["code", "date", "open", "high", "low", "close", "volume"]]

        print(df.tail(3))

        # -------------------------
        # MongoDB 저장 (upsert)
        # -------------------------
        for r in df.itertuples(index=False):
            dt = pd.to_datetime(r.date).to_pydatetime()  # ← 날짜 통일 처리

            doc = {
                "code": r.code,
                "date": dt,
                "open": float(r.open) if pd.notna(r.open) else None,
                "high": float(r.high) if pd.notna(r.high) else None,
                "low": float(r.low) if pd.notna(r.low) else None,
                "close": float(r.close) if pd.notna(r.close) else None,
                "volume": int(r.volume) if pd.notna(r.volume) else None,
                "last_update": datetime.now()
            }

            col_price.update_one(
                {"code": r.code, "date": dt},  # ← 조건도 datetime
                {"$set": doc},
                upsert=True
            )

            total_count += 1

        print(f"{name} ({code}) 저장 완료")

    except Exception as e:
        print(f"{name} ({code}) 처리 중 오류: {e}")

# ---------------------------------------------
# 4️⃣ 전체 완료 출력
# ---------------------------------------------
print("\n모든 업데이트 완료.")
print(f"총 저장된 행 수: {total_count}")
print(f"총 처리된 ETF 수: {processed_codes}")

print(f"ROWCOUNT={total_count}")
print(f"CODECOUNT={processed_codes}")

client.close()
