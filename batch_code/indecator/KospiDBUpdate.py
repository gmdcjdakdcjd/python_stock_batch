import os

import pandas as pd
from bs4 import BeautifulSoup
# import pymysql   # ← MariaDB는 사용하지 않으므로 주석 처리
import requests
import json
from pymongo import MongoClient
from datetime import datetime

from common.mongo_util import MongoDB


class DBUpdater:
    def __init__(self):
        """MongoDB 연결 + config_fx.json 로드"""

        # ------------------------------------------
        # MariaDB 연결 (사용 안함 → 주석 처리)
        # ------------------------------------------
        # self.conn = pymysql.connect(
        #     host='localhost',
        #     user='root',
        #     password='0806',
        #     db='INVESTAR',
        #     charset='utf8'
        # )

        # ------------------------------------------
        # MongoDB 연결
        # ------------------------------------------
        mongo = MongoDB()
        self.db = mongo.db
        self.col_indicator = self.db["daily_price_indicator"]

        # ------------------------------------------
        # config_fx.json 로드
        # ------------------------------------------
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CONFIG_PATH = os.path.join(BASE_DIR, "config_fx.json")

        # config_fx.json 로드
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
                self.pages_to_fetch = config.get("pages_to_fetch", 1)
        except FileNotFoundError:
            self.pages_to_fetch = 1
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump({"pages_to_fetch": 1}, f, indent=4, ensure_ascii=False)

        print(f"[INFO] KOSPI pages_to_fetch = {self.pages_to_fetch}")

    def __del__(self):
        """MongoDB는 자동 close되므로 별도 처리 없음"""
        pass
        # self.conn.close()   # MariaDB 사용 안함

    # ---------------------------------------------------------------------
    # 1) KOSPI 지수 일별 시세 (단일 페이지)
    # ---------------------------------------------------------------------
    def read_kospi_page(self, page):
        try:
            url = f"https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI&page={page}"
            html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text

            df = pd.read_html(html)[0]
            return df

        except Exception:
            return pd.DataFrame()

    # ---------------------------------------------------------------------
    # 2) pages_to_fetch 전체 페이지 수집
    # ---------------------------------------------------------------------
    def collect_all_pages(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] KOSPI {page}/{self.pages_to_fetch} 페이지 수집 중...", end="\r")

            df = self.read_kospi_page(page)
            if df.empty:
                continue

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)

        # 컬럼명 변환
        df_all = df_all.rename(columns={
            '날짜': 'date',
            '체결가': 'close',
            '전일비': 'change_amount',
            '등락률': 'change_rate'
        })

        # 날짜 필터링 및 정제
        df_all['date'] = df_all['date'].astype(str)
        df_all = df_all[df_all['date'].str.contains(r"\d{4}\.\d{2}\.\d{2}")]
        df_all['date'] = df_all['date'].str.replace(".", "-", regex=False)

        # 숫자 처리
        df_all['close'] = df_all['close'].astype(str).str.replace(",", "").astype(float)

        # change_amount 정리
        df_all['change_amount'] = (
            df_all['change_amount']
            .astype(str)
            .str.replace(",", "")
            .str.replace("▲", "")
            .str.replace("▼", "")
            .str.extract(r"(-?\d+\.?\d*)")[0]
            .astype(float)
        )

        df_all['change_rate'] = (
            df_all['change_rate']
            .astype(str)
            .str.replace("%", "")
            .astype(float)
        )

        return df_all[['date', 'close', 'change_amount', 'change_rate']]

    # ---------------------------------------------------------------------
    # 3) MongoDB 저장
    # ---------------------------------------------------------------------
    def replace_kospi_into_db(self, df):
        df_sorted = df.sort_values("date")

        for r in df_sorted.itertuples():
            # 문자열 → datetime 변환
            dt = datetime.strptime(r.date, "%Y-%m-%d")

            doc = {
                "code": "KOSPI",
                "date": dt,  # datetime으로 저장
                "close": float(r.close),
                "change_amount": float(r.change_amount),
                "change_rate": float(r.change_rate),
                "last_update": datetime.now()
            }

            self.col_indicator.update_one(
                {"code": "KOSPI", "date": dt},  # 조건도 datetime
                {"$set": doc},
                upsert=True
            )

        print(f"\n[INFO] KOSPI {len(df)} rows 저장 완료 (MongoDB)")

    # ---------------------------------------------------------------------
    # 4) 실행
    # ---------------------------------------------------------------------
    def update_kospi(self):
        print("[INFO] KOSPI 지수 수집 시작")

        df = self.collect_all_pages()
        if df is not None and not df.empty:
            self.replace_kospi_into_db(df)
            print("[INFO] KOSPI 지수 업데이트 완료")
        else:
            print("[WARN] KOSPI 데이터 없음")


# 실행부
if __name__ == '__main__':
    DBUpdater().update_kospi()
