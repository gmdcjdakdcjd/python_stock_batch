import pandas as pd
import requests
from datetime import datetime
# import pymysql   # ← MariaDB 사용 안함 (주석 처리)
from pymongo import MongoClient


class SP500DBUpdater:
    def __init__(self):
        """MongoDB 연결"""

        # ----------------------------------------------------
        # MariaDB 연결 (사용 안 함 → 전체 주석 처리)
        # ----------------------------------------------------
        # self.conn = pymysql.connect(
        #     host='localhost',
        #     user='root',
        #     password='0806',
        #     db='INVESTAR',
        #     charset='utf8'
        # )

        # ----------------------------------------------------
        # MongoDB 연결
        # ----------------------------------------------------
        self.client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
        self.db = self.client["investar"]
        self.col_indicator = self.db["daily_price_indicator"]   # SNP500 저장 컬렉션

    def __del__(self):
        """MongoDB는 자동 close → pass"""
        pass
        # self.conn.close()   # MariaDB 사용 안 함

    # ===============================================================
    # 1) S&P500 일별 시세 (네이버 worldDayListJson API)
    # ===============================================================
    def read_sp500(self):
        try:
            url = "https://finance.naver.com/world/worldDayListJson.naver?symbol=SPI@SPX&fdtc=0"
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.naver.com/",
            }

            res = requests.get(url, headers=headers)
            data = res.json()

            # JSON 구조 정규화
            if isinstance(data, dict) and "worldDayList" in data:
                data = data["worldDayList"]

            elif isinstance(data, list):
                data = data

            else:
                print("[ERROR] JSON 구조 이상:", data)
                return None

            df = pd.DataFrame(data)

            # 날짜 변환
            if 'day' in df.columns:
                df['date'] = df['day'].str.replace(".", "-")
            elif 'xymd' in df.columns:
                df['date'] = pd.to_datetime(df['xymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            else:
                raise Exception("date field not found")

            # 종가
            if 'close' in df.columns:
                df['close'] = df['close'].astype(str).str.replace(",", "").astype(float)
            elif 'clos' in df.columns:
                df['close'] = df['clos'].astype(str).str.replace(",", "").astype(float)
            else:
                raise Exception("close field not found")

            # 전일비
            if 'diff' in df.columns:
                df['change_amount'] = df['diff'].astype(str).str.replace(",", "").astype(float)
            elif 'dff' in df.columns:
                df['change_amount'] = df['dff'].astype(str).str.replace(",", "").astype(float)
            else:
                raise Exception("diff field not found")

            # 등락률
            if 'rate' in df.columns:
                df['change_rate'] = df['rate'].astype(float)
            else:
                df['change_rate'] = 0.0

            return df[['date', 'close', 'change_amount', 'change_rate']]

        except Exception as e:
            print("Exception (SP500 read):", e)
            return None

    # ===============================================================
    # 2) MongoDB 저장
    # ===============================================================
    def replace_into_db(self, df):
        df_sorted = df.sort_values("date")

        for r in df_sorted.itertuples():
            # 문자열 → datetime 변환
            dt = datetime.strptime(r.date, "%Y-%m-%d")

            doc = {
                "code": "SNP500",
                "date": dt,  # ★ datetime 저장
                "close": float(r.close),
                "change_amount": float(r.change_amount),
                "change_rate": float(r.change_rate),
                "last_update": datetime.now()  # ★ datetime 저장
            }

            self.col_indicator.update_one(
                {"code": "SNP500", "date": dt},  # ★ 조건도 datetime
                {"$set": doc},
                upsert=True
            )

        print(f"[INFO] SNP500 {len(df)} rows 저장 완료 (MongoDB)")

    # ===============================================================
    # 3) 실행
    # ===============================================================
    def update_sp500(self):
        print("[INFO] SNP500 데이터 수집 시작...")

        df = self.read_sp500()
        if df is not None and not df.empty:
            self.replace_into_db(df)
            print("[INFO] SNP500 업데이트 완료")
        else:
            print("[WARN] SNP500 데이터 수집 실패")


# 실행부
if __name__ == '__main__':
    dbu = SP500DBUpdater()
    dbu.update_sp500()
