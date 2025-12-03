import os

import pandas as pd
from bs4 import BeautifulSoup
# import pymysql     # ← MariaDB 사용 안함 (주석 처리)
import requests
import json
import re
from pymongo import MongoClient
from datetime import datetime

from common.mongo_util import MongoDB


class USDJPYDBUpdater:
    def __init__(self):
        """DB 연결 + config_fx.json 로드"""

        # ------------------------------------------
        # MariaDB 연결 부분 (사용 안 함 → 주석 처리)
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
        self.col_indicator = self.db["daily_price_indicator"]  # USDJPY 저장 컬렉션

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

        print(f"[INFO] USD_JPY pages_to_fetch = {self.pages_to_fetch}")

    def __del__(self):
        """MongoDB는 자동 close 되므로 pass"""
        pass
        # self.conn.close()   # MariaDB 사용 안함 → 주석 처리

    # ----------------------------------------------------------------
    # 1) USDJPY 페이지 스크래핑
    # ----------------------------------------------------------------
    def read_usd_jpy(self, page=1):
        try:
            url = (
                f"https://finance.naver.com/marketindex/worldDailyQuote.naver"
                f"?marketindexCd=FX_USDJPY&fdtc=4&page={page}"
            )
            headers = {"User-Agent": "Mozilla/5.0"}
            html = requests.get(url, headers=headers).text
            soup = BeautifulSoup(html, "lxml")

            rows = soup.select("table.tbl_exchange.today tbody tr")
            data = []

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                date_raw = cols[0].get_text(strip=True)
                if date_raw.count('.') != 2:
                    continue
                date = date_raw.replace(".", "-")

                close_raw = cols[1].get_text(strip=True).replace(",", "")
                close = float(close_raw)

                diff_td = cols[2]
                sign = 1
                img = diff_td.find("img")
                if img and "하락" in img.get("alt", ""):
                    sign = -1

                m = re.search(r"-?\d+\.?\d*", diff_td.get_text(strip=True))
                if not m:
                    continue
                change_amount = sign * float(m.group())

                rate_raw = cols[3].get_text(strip=True)
                rate_raw = rate_raw.replace("%", "").replace("+", "").replace(",", "")
                change_rate = float(rate_raw)

                data.append([date, close, change_amount, change_rate])

            return pd.DataFrame(
                data,
                columns=["date", "close", "change_amount", "change_rate"]
            )

        except Exception as e:
            print("Exception (USD_JPY page):", e)
            return pd.DataFrame()

    # ----------------------------------------------------------------
    # 2) 전체 페이지 수집
    # ----------------------------------------------------------------
    def collect_all_pages(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] USD_JPY {page}/{self.pages_to_fetch} 페이지 수집 중...")
            df = self.read_usd_jpy(page)

            if df.empty:
                break

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        return df_all

    # ----------------------------------------------------------------
    # 3) MongoDB 저장 (MariaDB 대신)
    # ----------------------------------------------------------------
    def replace_usd_jpy_into_db(self, df):
        df_sorted = df.sort_values("date")

        for r in df_sorted.itertuples():
            # 1) 문자열 → datetime 변환
            dt = datetime.strptime(r.date, "%Y-%m-%d")

            doc = {
                "code": "USD_JPY",
                "date": dt,  # 2) datetime 저장
                "close": float(r.close),
                "change_amount": float(r.change_amount),
                "change_rate": float(r.change_rate),
                "last_update": datetime.now()
            }

            # 4) upsert 조건도 datetime으로
            self.col_indicator.update_one(
                {"code": "USD_JPY", "date": dt},
                {"$set": doc},
                upsert=True
            )

        print(f"[INFO] USD_JPY {len(df)} rows 저장 완료 (MongoDB)")

    # ----------------------------------------------------------------
    # 4) 실행
    # ----------------------------------------------------------------
    def update_usd_jpy(self):
        print("[INFO] USD_JPY 시세 수집 시작...")
        df = self.collect_all_pages()

        if df is not None and not df.empty:
            self.replace_usd_jpy_into_db(df)
            print("[INFO] USD_JPY 업데이트 완료")
        else:
            print("[WARN] USD_JPY 데이터 없음")


if __name__ == '__main__':
    updater = USDJPYDBUpdater()
    updater.update_usd_jpy()
