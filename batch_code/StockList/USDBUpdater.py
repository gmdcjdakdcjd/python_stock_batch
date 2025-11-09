import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime

# ---------------------------------------------
# ✅ 1️⃣ DB 연결 설정
# ---------------------------------------------
db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
engine = create_engine(db_url)

# ---------------------------------------------
# ✅ 2️⃣ 종목 코드 불러오기
# ---------------------------------------------
with engine.connect() as conn:
    query = text("SELECT code, name FROM company_info_us;")
    codes_df = pd.read_sql(query, conn)

print(f"✅ 불러온 종목 수: {len(codes_df)}개")
print(codes_df.head())

# ---------------------------------------------
# ✅ 3️⃣ yfinance 데이터 수집 및 DB 저장
# ---------------------------------------------
for idx, row in codes_df.iterrows():
    code = row['code']
    name = row['name']

    print(f"\n[{idx+1}/{len(codes_df)}] {name} ({code}) 주가 불러오는 중...")

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
            print(f"{code}: 데이터가 비어 있습니다.")
            continue

        # 인덱스 초기화 및 컬럼 정리
        df.reset_index(inplace=True)
        df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)

        # 날짜 문자열 변환
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # 코드 컬럼 추가 + 순서 정리
        df['code'] = code
        df = df[['code', 'date', 'open', 'high', 'low', 'close', 'volume']]

        print(df.tail(3))

        # ---------------------------------------------
        # DB 저장 (SQLAlchemy 트랜잭션)
        # ---------------------------------------------
        with engine.begin() as conn:
            for row in df.itertuples(index=False, name=None):
                (
                    code,
                    date,
                    open_,
                    high,
                    low,
                    close,
                    volume
                ) = row

                # NaN-safe 변환
                val_open = 'NULL' if pd.isna(open_) else f"{open_:.4f}"
                val_high = 'NULL' if pd.isna(high) else f"{high:.4f}"
                val_low = 'NULL' if pd.isna(low) else f"{low:.4f}"
                val_close = 'NULL' if pd.isna(close) else f"{close:.4f}"
                val_vol = 'NULL' if pd.isna(volume) else int(volume)

                sql = f"""
                    REPLACE INTO daily_price_us
                    (code, date, open, high, low, close, volume)
                    VALUES (
                        '{code}',
                        '{date}',
                        {val_open},
                        {val_high},
                        {val_low},
                        {val_close},
                        {val_vol}
                    )
                """
                conn.execute(text(sql))

        print(f"{name} ({code}) 저장 완료")

    except Exception as e:
        print(f"{name} ({code}) 처리 중 오류 발생: {e}")

print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 모든 종목 업데이트 완료.")
