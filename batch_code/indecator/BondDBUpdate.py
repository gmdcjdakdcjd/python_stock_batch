import pandas as pd
from datetime import datetime, timedelta
from FinanceDataReader.investing.data import InvestingDailyReader
from pymongo import MongoClient

from common.mongo_util import MongoDB

mongo = MongoDB()
db = mongo.db
col_bond = db["bond_info"]
col_yield = db["bond_daily_price"]


# -------------------------------------------------------------
# 1) bond_info 불러오기
# -------------------------------------------------------------
def load_bond_info():
    cursor = col_bond.find({}, {"ticker": 1, "name": 1, "_id": 0})
    return pd.DataFrame(list(cursor))


# -------------------------------------------------------------
# 2) 초경량 Investing.com 요청 (어제~오늘만)
# -------------------------------------------------------------
def fetch_latest_yield(ticker: str):
    try:
        today = datetime.now()
        yesterday = today - timedelta(days=1)

        end = today.strftime("%Y-%m-%d")
        start = yesterday.strftime("%Y-%m-%d")

        reader = InvestingDailyReader(symbol=ticker, start=start, end=end)
        df = reader.read()

        if df is None or df.empty:
            print(f"[WARN] No new data for {ticker}")
            return None

        df = df.reset_index()

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Price": "close"
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        return df[["date", "open", "high", "low", "close"]]

    except Exception as e:
        print(f"[ERROR] {ticker} → {e}")
        return None

# -------------------------------------------------------------
# 2) 초경량 Investing.com 요청 (어제~오늘만)
# -------------------------------------------------------------
def fetch_full_5y_yield(ticker: str):
    try:
        today = datetime.now()

        end = today.strftime("%Y-%m-%d")
        start = (today - timedelta(days=365 * 5)).strftime("%Y-%m-%d")

        reader = InvestingDailyReader(symbol=ticker, start=start, end=end)
        df = reader.read()

        if df is None or df.empty:
            print(f"[WARN] No new data for {ticker}")
            return None

        df = df.reset_index()

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Price": "close"
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        return df[["date", "open", "high", "low", "close"]]

    except Exception as e:
        print(f"[ERROR] {ticker} → {e}")
        return None


# -------------------------------------------------------------
# 3) MongoDB 저장 (Upsert)
# -------------------------------------------------------------
def save_daily_yield(code, df):
    df = df.sort_values("date").reset_index(drop=True)
    df["diff"] = df["close"].diff()

    count = 0

    for _, r in df.iterrows():
        dt = datetime.strptime(r.date, "%Y-%m-%d")

        doc = {
            "code": code,
            "date": dt,
            "open": float(r["open"]) if pd.notna(r["open"]) else None,
            "high": float(r["high"]) if pd.notna(r["high"]) else None,
            "low": float(r["low"]) if pd.notna(r["low"]) else None,
            "close": float(r["close"]) if pd.notna(r["close"]) else None,
            "diff": float(r["diff"]) if pd.notna(r["diff"]) else None,
            "last_update": datetime.now()
        }

        col_yield.update_one(
            {"code": code, "date": dt},
            {"$setOnInsert": doc},
            upsert=True
        )

        count += 1

    print(f"[DONE] {code} → {count} rows 저장")


# -------------------------------------------------------------
# 4) 전체 실행
# -------------------------------------------------------------
def run():
    print("=== 채권 금리 배치 (초경량 버전) 시작 ===\n")

    bonds = load_bond_info()

    for _, row in bonds.iterrows():
        ticker = row["ticker"]
        name = row["name"]

        print(f"[FETCH] {name} ({ticker}) 최신 금리 수집…")

        df = fetch_latest_yield(ticker)
        #df = fetch_full_5y_yield(ticker) # 5년치 전체 데이터 수집

        if df is None or df.empty:
            continue

        save_daily_yield(ticker, df)
        print()

    print("=== 완료 ===")


if __name__ == "__main__":
    run()
