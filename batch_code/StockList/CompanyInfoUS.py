import pandas as pd
import urllib.request
from pymongo import MongoClient
from datetime import datetime

# ------------------------------------------------------------
# 1. 미국 S&P500 리스트 수집
# ------------------------------------------------------------
headers = {'User-Agent': 'Mozilla/5.0'}
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

req = urllib.request.Request(url, headers=headers)
html = urllib.request.urlopen(req).read()

# 모든 테이블 읽기
tables = pd.read_html(html)

# Symbol 컬럼이 있는 테이블 자동 선택
sp500 = None
for t in tables:
    if "Symbol" in t.columns:
        sp500 = t
        break

if sp500 is None:
    raise Exception("S&P500 테이블을 찾을 수 없습니다.")

# 필요한 컬럼만 사용
sp500 = sp500[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry','CIK']].copy()

sp500.columns = ['code', 'name', 'sector', 'industry', 'cik']
sp500['market'] = 'S&P500'

# 티커 변환
sp500['code'] = sp500['code'].str.replace('.', '-', regex=False)

print(f"총 {len(sp500)}개 종목 수집 완료")


# ------------------------------------------------------------
# 2. MongoDB 저장 (UPSERT)
# ------------------------------------------------------------
def save_us_company_info(df):
    client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
    col = client["investar"]["company_info_us"]

    today = datetime.now().strftime('%Y-%m-%d')

    for _, row in df.iterrows():
        doc = {
            "code": row['code'],
            "name": row['name'],
            "market": row['market'],
            "sector": row['sector'],
            "industry": row['industry'],
            "cik": row['cik'],
            "last_update": today
        }

        col.update_one({"code": row['code']}, {"$set": doc}, upsert=True)

    client.close()
    print(f"{len(df)}건 저장 완료")


# ------------------------------------------------------------
# 3. 실행
# ------------------------------------------------------------
save_us_company_info(sp500)
print("S&P500 전체 저장 완료")
