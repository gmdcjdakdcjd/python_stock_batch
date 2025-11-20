import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from FinanceDataReader.investing.data import InvestingDailyReader

db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
engine = create_engine(db_url)


def load_bond_info():
    with engine.connect() as conn:
        return pd.read_sql(text("SELECT ticker, name FROM bond_info"), conn)


def fetch_investing_yield(ticker: str):
    try:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")

        reader = InvestingDailyReader(symbol=ticker, start=start, end=end)
        df = reader.read()

        if df is None or df.empty:
            print(f"[WARN] No data: {ticker}")
            return None

        df = df.reset_index()

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Price": "close",
            "Volume": "volume"
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["volume"] = df.get("volume", 0).fillna(0).astype(int)

        for c in ["open", "high", "low"]:
            if c not in df.columns:
                df[c] = None

        return df[["date", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        print(f"[ERROR] {ticker} → {e}")
        return None


def save_daily_yield(ticker, df):
    df = df.sort_values("date").reset_index(drop=True)
    df["diff"] = df["close"].diff()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO daily_yield (ticker, date, open, high, low, close, diff, volume)
                VALUES (:ticker, :date, :open, :high, :low, :close, :diff, :volume)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    diff=VALUES(diff),
                    volume=VALUES(volume)
            """), {
                "ticker": ticker,
                "date": str(row["date"]),
                "open": None if pd.isna(row["open"]) else float(row["open"]),
                "high": None if pd.isna(row["high"]) else float(row["high"]),
                "low": None if pd.isna(row["low"]) else float(row["low"]),
                "close": None if pd.isna(row["close"]) else float(row["close"]),
                "diff": None if pd.isna(row["diff"]) else float(row["diff"]),
                "volume": int(row["volume"]) if not pd.isna(row["volume"]) else 0
            })

        print(f"[DONE] {ticker} saved → {len(df)} rows")


def run():
    print("=== 채권 금리 배치 시작 ===\n")

    bonds = load_bond_info()

    for _, row in bonds.iterrows():
        ticker = row["ticker"]
        name = row["name"]

        print(f"[FETCH] {name} ({ticker}) collecting...")
        df = fetch_investing_yield(ticker)

        if df is None or df.empty:
            print(f"[WARN] No data for {ticker}")
            continue

        save_daily_yield(ticker, df)
        print()

    print("=== 완료 ===")


if __name__ == "__main__":
    run()
