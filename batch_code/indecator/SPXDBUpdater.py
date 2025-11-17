import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime

# ---------------------------------------------
# 1) DB 연결
# ---------------------------------------------
db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
engine = create_engine(db_url)

# ---------------------------------------------
# 2) SPX 데이터 수집 (야후: ^GSPC)
# ---------------------------------------------
ticker = "^GSPC"
name = "S&P500"

print(f"\n[SPX] {name} ({ticker}) 주가 불러오는 중...")

df = yf.download(
    ticker,
    period="5y",      # ← 원하는 기간 설정
    interval="1d",
    auto_adjust=True,
    threads=False,
    progress=False
)

if df.empty:
    print("[SPX] 데이터 없음")
    exit()

df.reset_index(inplace=True)

# 날짜 변환
df['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

# code 고정
df['code'] = "SPX"

# ---------------------------------------------
# change_amount 계산: 오늘 종가 - 어제 종가
# change_rate 계산: change_amount / 이전 종가 * 100
# ---------------------------------------------
df['close'] = df['Close']

df['change_amount'] = df['close'].diff()          # 오늘 - 어제
df['change_amount'] = df['change_amount'].fillna(0)

df['change_rate'] = df['change_amount'] / (df['close'] - df['change_amount']) * 100
df['change_rate'] = df['change_rate'].fillna(0)

# 필요한 컬럼만 유지
df = df[['code', 'date', 'close', 'change_amount', 'change_rate']]

print(df.tail(3))

# ---------------------------------------------
# 3) DB 저장 (market_indicator)
# ---------------------------------------------
total_count = 0

with engine.begin() as conn:
    for r in df.itertuples(index=False, name=None):
        (
            code,
            date,
            close,
            change_amount,
            change_rate
        ) = r

        sql = f"""
            REPLACE INTO market_indicator
            (code, date, close, change_amount, change_rate)
            VALUES (
                '{code}',
                '{date}',
                {close:.4f},
                {change_amount:.4f},
                {change_rate:.4f}
            );
        """

        conn.execute(text(sql))
        total_count += 1

print(f"\n[SPX 저장 완료] 총 {total_count} rows")
print(f"ROWCOUNT={total_count}")
print(f"CODECOUNT=1")
