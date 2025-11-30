import pandas as pd
from bs4 import BeautifulSoup
# import pymysql   # ← MariaDB 사용 안 함
import requests
import json
import re
from pymongo import MongoClient
from datetime import datetime


class GoldKRWDBUpdater:
    def __init__(self):
        """MongoDB 연결 + config_fx.json 로드"""

        # ------------------------------------------
        # MariaDB 연결부 (사용 안함 → 주석 처리)
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
        self.client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
        self.db = self.client["investar"]
        self.col_indicator = self.db["daily_price_indicator"]  # GOLD_KR 저장

        # ------------------------------------------
        # config_fx.json 로드
        # ------------------------------------------
        try:
            with open('config_fx.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.pages_to_fetch = config.get("pages_to_fetch", 1)
        except FileNotFoundError:
            self.pages_to_fetch = 1
            with open('config_fx.json', 'w', encoding='utf-8') as f:
                json.dump({"pages_to_fetch": 1}, f, indent=4, ensure_ascii=False)

        print(f"[INFO] GOLD_KR pages_to_fetch = {self.pages_to_fetch}")

    def __del__(self):
        """MongoDB는 자동 close → 특별히 필요 없음"""
        pass
        # self.conn.close()  # (MariaDB 사용 안함)

    # ------------------------------------------------------------------------------------
    # 1) 국내 금 시세 한 페이지 스크래핑
    # ------------------------------------------------------------------------------------
    def read_gold_krw_page(self, page=1):
        try:
            url = f"https://finance.naver.com/marketindex/goldDailyQuote.naver?page={page}"
            headers = {"User-Agent": "Mozilla/5.0"}

            html = requests.get(url, headers=headers).text
            soup = BeautifulSoup(html, "lxml")

            rows = soup.select("table.tbl_exchange.today tbody tr")
            data = []

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                # 날짜
                date_raw = cols[0].text.strip()
                if date_raw == "" or date_raw.count(".") != 2:
                    continue
                date = date_raw.replace(".", "-")

                # 종가
                close_raw = cols[1].text.strip().replace(",", "")
                if close_raw == "":
                    continue
                close = float(close_raw)

                # 전일대비(change_amount)
                diff_td = cols[2]
                sign = 1
                img = diff_td.find("img")
                if img and "하락" in img.get("alt", ""):
                    sign = -1

                diff_text = (
                    diff_td.get_text(strip=True)
                    .replace("상승", "")
                    .replace("하락", "")
                    .replace(",", "")
                    .strip()
                )

                m = re.search(r"-?\d+\.?\d*", diff_text)
                if not m:
                    continue

                change_amount = sign * float(m.group())

                data.append([date, close, change_amount])

            df = pd.DataFrame(data, columns=["date", "close", "change_amount"])
            return df

        except Exception as e:
            print("Exception (GOLD_KR PAGE):", e)
            return pd.DataFrame()

    # ------------------------------------------------------------------------------------
    # 2) pages_to_fetch 반복 수집
    # ------------------------------------------------------------------------------------
    def collect_all_pages(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] GOLD_KR {page}/{self.pages_to_fetch} 페이지 수집 중...", end="\r")

            df = self.read_gold_krw_page(page)
            if df.empty:
                break

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)

        # 등락률 계산
        df_all["change_rate"] = df_all.apply(
            lambda row: (row["change_amount"] /
                         (row["close"] - row["change_amount"]) * 100)
            if (row["close"] != row["change_amount"]) else 0.0,
            axis=1
        )

        return df_all

    # ------------------------------------------------------------------------------------
    # 3) MongoDB 저장 (과거 → 최신)
    # ------------------------------------------------------------------------------------
    def replace_gold_into_db(self, df):
        df_sorted = df.sort_values("date")

        for r in df_sorted.itertuples():
            # 문자열 → datetime 변환
            dt = datetime.strptime(r.date, "%Y-%m-%d")

            doc = {
                "code": "GOLD_KR",
                "date": dt,  # datetime으로 저장!
                "close": float(r.close),
                "change_amount": float(r.change_amount),
                "change_rate": float(r.change_rate),
                "last_update": datetime.now()  # datetime
            }

            self.col_indicator.update_one(
                {"code": "GOLD_KR", "date": dt},  # 조건도 datetime
                {"$set": doc},
                upsert=True
            )

        print(f"\n[INFO] GOLD_KR {len(df)} rows 저장 완료 (MongoDB)")

    # ------------------------------------------------------------------------------------
    # 4) 실행
    # ------------------------------------------------------------------------------------
    def update_gold_krw(self):
        print("[INFO] 국내 금(KRW) 시세 수집 시작...")

        df = self.collect_all_pages()

        if df is not None and not df.empty:
            self.replace_gold_into_db(df)
            print("[INFO] GOLD_KR 업데이트 완료")
        else:
            print("[WARN] GOLD_KR 데이터 없음")


# 실행부
if __name__ == '__main__':
    updater = GoldKRWDBUpdater()
    updater.update_gold_krw()
