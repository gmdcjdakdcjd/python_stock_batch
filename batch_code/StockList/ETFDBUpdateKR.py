import os

import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
from datetime import datetime
from pymongo import MongoClient

from common.mongo_util import MongoDB


class DBUpdater:
    def __init__(self):
        """생성자: MongoDB 연결"""

        # 기존 MariaDB 코드 (사용 안 함)
        # self.conn = pymysql.connect(...)
        # self.conn.commit()

        mongo = MongoDB()
        self.mongo = mongo  # 🔥 추가해야 함
        self.col_etf_info = mongo.db["etf_info_kr"]
        self.col_etf_daily = mongo.db["etf_daily_price_kr"]

        self.codes = dict()  # {'코드': '이름'}

    # -------------------------------------------------
    # 네이버에서 ETF 일별 시세 읽기
    # -------------------------------------------------
    def read_naver(self, code, company, pages_to_fetch):
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html = BeautifulSoup(
                requests.get(url, headers={'User-agent': 'Mozilla/5.0'}).text, "lxml"
            )

            # 페이지 계산
            pgrr = html.find("td", class_="pgRR")
            if pgrr is None:
                lastpage = 1
            else:
                lastpage = str(pgrr.a['href']).split('=')[-1]

            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)

            for page in range(1, pages + 1):
                pg_url = f"{url}&page={page}"
                page_df = pd.read_html(
                    requests.get(pg_url, headers={'User-agent': 'Mozilla/5.0'}).text
                )[0]

                df = pd.concat([df, page_df], ignore_index=True)

                print(f"[INFO] {company}({code}) {page}/{pages} 페이지 수집 중...", end="\r")

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

            # 날짜 정규화
            df['date'] = df['date'].astype(str).str.replace('.', '-', regex=False)
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['date'])

            # 숫자 변환
            df['diff'] = df['diff'].astype(str).str.extract(r'(\d+)')
            df = df.dropna()

            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[
                ['close', 'diff', 'open', 'high', 'low', 'volume']
            ].astype(int)

            return df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]

        except Exception as e:
            print(f"[ERROR] {company}({code}) 수집 실패 → {e}")
            return None

    # -------------------------------------------------
    # MongoDB 저장 (REPLACE INTO → upsert)
    # -------------------------------------------------
    def replace_into_db(self, df, num, code, company):
        for r in df.itertuples():
            dt = pd.to_datetime(r.date).to_pydatetime()  # datetime 변환

            doc = {
                "code": code,
                "date": dt,  # datetime
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "diff": r.diff,
                "volume": r.volume,
                "last_update": datetime.now()  # ★ datetime
            }

            self.col_etf_daily.update_one(
                {"code": code, "date": dt},  # 조건도 datetime
                {"$set": doc},
                upsert=True
            )

        print(f"[OK] #{num + 1:04d} {company}({code}) {len(df)} rows 저장")
        print(f"ROWCOUNT={len(df)}")
        return len(df)

    # -------------------------------------------------
    # ETF 코드 로드 (MongoDB에서)
    # -------------------------------------------------
    def load_codes_from_db(self):
        """KODEX ETF만 로드"""
        cursor = self.col_etf_info.find(
            {"manager": "삼성자산운용"},  # 운용사 기준
            {"code": 1, "name": 1}
        )

        self.codes = {doc["code"]: doc["name"] for doc in cursor}
        print(f"[INFO] {len(self.codes)}개 ETF 로드 완료 (삼성자산운용)")

    # -------------------------------------------------
    # 전체 업데이트
    # -------------------------------------------------
    def update_daily_price(self, pages_to_fetch):
        total_count = 0
        processed = 0

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None or df.empty:
                continue
            total_count += self.replace_into_db(df, idx, code, self.codes[code])
            processed += 1

        print(f"ROWCOUNT={total_count}")
        print(f"CODECOUNT={processed}")

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
    dbu.mongo.close()   # 명시적 종료