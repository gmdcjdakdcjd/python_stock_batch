import pandas as pd
import urllib.request
import pymysql
from datetime import datetime

# ------------------------------------------------------------
# 🧭 1. 미국 S&P500 종목 리스트 (GICS Sector, Sub-Industry 포함)
# ------------------------------------------------------------
headers = {'User-Agent': 'Mozilla/5.0'}

url_sp500 = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
req_sp500 = urllib.request.Request(url_sp500, headers=headers)
html_sp500 = urllib.request.urlopen(req_sp500).read()

# ✅ 위키피디아 S&P500 테이블 파싱
sp500 = pd.read_html(html_sp500)[0]

# 필요한 컬럼 선택
sp500 = sp500[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']]
sp500.columns = ['code', 'name', 'sector', 'industry']
sp500['market'] = 'S&P500'

sp500['code'] = sp500['code'].str.replace('.', '-', regex=False)

print(f"📊 총 {len(sp500)}개 종목 수집 완료 (S&P500 전용)")
print(sp500.head(10))


# ------------------------------------------------------------
# 💾 2. DB 저장 함수 (UPSERT)
# ------------------------------------------------------------
def save_us_company_info(df):
    """S&P500 종목 리스트를 company_info_us 테이블에 저장 (UPSERT)"""
    conn = pymysql.connect(host='localhost', user='root', password='0806',
                           db='INVESTAR', charset='utf8')

    with conn.cursor() as curs:
        for _, row in df.iterrows():
            sql = """
                INSERT INTO company_info_us (code, name, market, sector, industry, last_update)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    market = VALUES(market),
                    sector = VALUES(sector),
                    industry = VALUES(industry),
                    last_update = VALUES(last_update)
            """
            curs.execute(sql, (
                row['code'],
                row['name'],
                row['market'],
                row['sector'],
                row['industry'],
                datetime.now()
            ))

    conn.commit()
    conn.close()
    print(f"💾 {len(df)}건 DB 저장 완료 ✅")


# ------------------------------------------------------------
# 🚀 3. 전체 저장 실행
# ------------------------------------------------------------
print("DB 저장 시작 ...")
save_us_company_info(sp500)
print("S&P500 전체 저장 완료 ✅")
