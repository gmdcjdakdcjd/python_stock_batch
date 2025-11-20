from datetime import datetime, timedelta
from FinanceDataReader.investing.data import InvestingDailyReader

end = datetime.now().strftime("%Y-%m-%d")
start = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")

reader = InvestingDailyReader(symbol="US2YT=X", start=start, end=end)
df = reader.read()
print(df.head())
