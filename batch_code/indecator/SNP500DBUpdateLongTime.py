import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from datetime import datetime

# ---------------------------------------------
# 1) MongoDB 연결
# ---------------------------------------------
client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
db = client["investar"]
col_price = db["daily_price_indicator"]

# ---------------------------------------------
# 2) SNP500 데이터 수집
# ---------------------------------------------
ticker = "^GSPC"
print(f"[SNP500] {ticker} 수집 중...")

df = yf.download(
    ticker,
    period="3y",
    interval="1d",
    auto_adjust=True,
    progress=False,
    threads=False
)

if df.empty:
    print("데이터 없음")
    exit()

df.reset_index(inplace=True)

# MultiIndex 방지
df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

# 날짜 컬럼 통일
date_col = None
for cand in ["Date", "Datetime", "date"]:
    if cand in df.columns:
        date_col = cand
        break

df.rename(columns={date_col: "date"}, inplace=True)

# 컬럼 이름 통일
df.rename(columns={
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
}, inplace=True)

# 날짜를 YYYY-MM-DD로 변환
df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

# code 부여
df["code"] = "SNP500"

# ---------------------------------------------
# 변동 계산 (change_amount, change_rate)
# ---------------------------------------------
df["change_amount"] = df["close"].diff().fillna(0)

df["change_rate"] = (
    df["change_amount"] /
    (df["close"] - df["change_amount"]).replace(0, pd.NA)
) * 100
df["change_rate"] = df["change_rate"].fillna(0)

df = df[["code", "date", "close", "change_amount", "change_rate"]]

# ---------------------------------------------
# 3) MongoDB 저장 (datetime 변환 + upsert)
# ---------------------------------------------
total_count = 0

for r in df.itertuples(index=False):

    dt = datetime.strptime(r.date, "%Y-%m-%d")

    doc = {
        "code": r.code,
        "date": dt,
        "close": float(r.close),
        "change_amount": float(r.change_amount),
        "change_rate": float(r.change_rate),
        "last_update": datetime.now()
    }

    col_price.update_one(
        {"code": r.code, "date": dt},
        {"$set": doc},
        upsert=True
    )

    total_count += 1

print(f"[SNP500 저장 완료] {total_count} rows")
client.close()
