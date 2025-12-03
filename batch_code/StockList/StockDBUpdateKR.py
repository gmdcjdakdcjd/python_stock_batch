import os

import pandas as pd
from bs4 import BeautifulSoup
# import pymysql  # MariaDB 사용 안 함
import calendar, time, json
import requests
from datetime import datetime
from pymongo import MongoClient

from common.mongo_util import MongoDB


class DBUpdater:
    def __init__(self):
        """MongoDB 연결"""

        # MariaDB 연결 제거
        # self.conn = pymysql.connect(host='localhost', user='root',
        #                             password='0806', db='INVESTAR', charset='utf8')

        # MongoDB 연결
        mongo = MongoDB()
        self.mongo = mongo
        self.db = mongo.db
        self.col_company = self.db["company_info_kr"]
        self.col_daily = self.db["daily_price_kr"]

        self.codes = {}  # {'005930': '삼성전자'}

    # ------------------------------------------------------------
    # 네이버 일별 시세 수집
    # ------------------------------------------------------------
    def read_naver(self, code, company, pages_to_fetch):
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html = BeautifulSoup(
                requests.get(url, headers={'User-agent': 'Mozilla/5.0'}).text,
                "lxml"
            )
            pgrr = html.find("td", class_="pgRR")

            if pgrr is None:
                print(f"[WARN] {company} ({code}) 페이지 구조 이상 - 1페이지만 수집")
                lastpage = 1
            else:
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]

            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)

            for page in range(1, pages + 1):
                pg_url = f"{url}&page={page}"
                page_df = pd.read_html(
                    requests.get(pg_url, headers={'User-agent': 'Mozilla/5.0'}).text
                )[0]
                df = pd.concat([df, page_df], ignore_index=True)

                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f"[{tmnow}] {company} ({code}) : {page:04d}/{pages:04d} pages...", end="\r")

            # 컬럼 정리
            df = df.rename(columns={
                '날짜': 'date',
                '종가': 'close',
                '전일비': 'diff',
                '시가': 'open',
                '고가': 'high',
                '저가': 'low',
                '거래량': 'volume'
            })

            df['date'] = df['date'].replace('.', '-')
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['date'])

            df['diff'] = df['diff'].astype(str).str.extract(r'(\d+)')
            df = df.dropna()

            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[
                ['close', 'diff', 'open', 'high', 'low', 'volume']
            ].astype(int)

            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
            return df

        except Exception as e:
            print("Exception occurred:", str(e))
            return None

    # ------------------------------------------------------------
    # MongoDB 저장 (REPLACE INTO → upsert)
    # ------------------------------------------------------------
    def save_daily_price_to_mongo(self, df, idx, code, company):
        for r in df.itertuples():
            doc = {
                "code": code,
                "date": pd.to_datetime(r.date).to_pydatetime(),
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "diff": r.diff,
                "volume": r.volume,
                "last_update": datetime.now()  # ★ datetime 타입
            }

            # upsert: (code + date) 기준으로 update or insert
            self.col_daily.update_one(
                {"code": code, "date": pd.to_datetime(r.date).to_pydatetime()},  # ← 수정
                {"$set": doc},
                upsert=True
            )

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] "
              f"#{idx+1:04d} {company} ({code}) : {len(df)} rows saved")
        print(f"ROWCOUNT={len(df)}")
        return len(df)

    # ------------------------------------------------------------
    # MongoDB에서 회사 코드 읽기
    # ------------------------------------------------------------
    def load_codes_from_db(self):
        cursor = self.col_company.find({"stock_type": "보통주"}, {"code": 1, "name": 1})
        self.codes = {doc["code"]: doc["name"] for doc in cursor}

        print(f"[INFO] {len(self.codes)}개 종목 로드 완료 (보통주)")

    # ------------------------------------------------------------
    # 전체 실행
    # ------------------------------------------------------------
    def update_daily_price(self, pages_to_fetch):
        total_count = 0
        processed = 0

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue

            total_count += self.save_daily_price_to_mongo(df, idx, code, self.codes[code])
            processed += 1

        print(f"ROWCOUNT={total_count}")
        print(f"CODECOUNT={processed}")

    # ------------------------------------------------------------
    # daily 실행
    # ------------------------------------------------------------
    def execute_daily(self):
        self.load_codes_from_db()

        # 현재 파일 경로 기반 절대 경로 만들기
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                pages_to_fetch = json.load(f).get("pages_to_fetch", 1)
        except FileNotFoundError:
            pages_to_fetch = 1
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump({"pages_to_fetch": 1}, f, indent=4, ensure_ascii=False)

        self.update_daily_price(pages_to_fetch)


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
    dbu.mongo.close()
