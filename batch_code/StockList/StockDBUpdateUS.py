import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
db = client["investar"]
col_company = db["company_info_us"]
col_price = db["daily_price_us"]

# 종목코드 불러오기
codes_cursor = col_company.find({}, {"code": 1, "name": 1})
codes_df = pd.DataFrame(list(codes_cursor))

print(f"불러온 종목 수: {len(codes_df)}개")

total_count = 0
processed_codes = 0

for idx, row in codes_df.iterrows():
    code = row["code"]
    name = row["name"]

    print(f"\n[{idx+1}/{len(codes_df)}] {name} ({code}) 데이터 수집 중...")
    processed_codes += 1

    try:
        df = yf.download(
            code,
            period="3d",
            interval="1d",
            auto_adjust=True,
            progress=False
        )

        if df.empty:
            print(f"{code}: 데이터 비어 있음")
            continue

        # 인덱스 초기화
        df.reset_index(inplace=True)

        # -------------- ⭐ MultiIndex 컬럼을 평탄화 ⭐ --------------
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # 컬럼 rename
        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        # 날짜 문자열화
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # code 추가
        df["code"] = code

        # 필요한 컬럼 순서로 재정렬
        df = df[["code", "date", "open", "high", "low", "close", "volume"]]
        print(df.tail(3))

        # MongoDB 저장(upsert)
        for r in df.itertuples(index=False):
            dt = pd.to_datetime(r.date).to_pydatetime()  # ← 날짜 파싱(중요)

            doc = {
                "code": r.code,
                "date": dt,  # ← datetime 저장
                "open": float(r.open) if pd.notna(r.open) else None,
                "high": float(r.high) if pd.notna(r.high) else None,
                "low": float(r.low) if pd.notna(r.low) else None,
                "close": float(r.close) if pd.notna(r.close) else None,
                "volume": int(r.volume) if pd.notna(r.volume) else None,
                "last_update": datetime.now()  # ← datetime 저장
            }

            col_price.update_one(
                {"code": r.code, "date": dt},  # ← 조건도 datetime
                {"$set": doc},
                upsert=True
            )

            total_count += 1

        print(f"{name} ({code}) 저장 완료")

    except Exception as e:
        print(f"{name} ({code}) 오류: {e}")


print("\n모든 업데이트 완료.")
print(f"총 저장된 행 수: {total_count}")
print(f"총 처리된 종목 수: {processed_codes}")
print(f"ROWCOUNT={total_count}")
print(f"CODECOUNT={processed_codes}")

client.close()
