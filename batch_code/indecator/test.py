from datetime import datetime, timedelta
from FinanceDataReader.investing.data import InvestingDailyReader

# 조회 범위 넉넉하게
end = datetime.now().strftime("%Y-%m-%d")
start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

ticker = "US2YT=X"

reader = InvestingDailyReader(symbol=ticker, start=start, end=end)
df = reader.read()

print("=== 수집된 원본 데이터 ===")
print(df)

# ----------------------------
# 🔥 특정 날짜 존재 여부 체크
# ----------------------------
target = "2025-11-22"

# df의 인덱스(Date)가 Timestamp → 문자열로 변환 후 비교
df["Date"] = df.index.strftime("%Y-%m-%d")

if target in df["Date"].values:
    print(f"✔ {target} 데이터 있음")
else:
    print(f"❌ {target} 데이터 없음")
