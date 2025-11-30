import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import re


class MarketDB:
    def __init__(self):

        # ------------------------------
        # MongoDB (실제 사용)
        # ------------------------------
        self.mongo = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
        self.mdb = self.mongo["investar"]

        self.col_etf = self.mdb["etf_info_kr"]
        self.col_daily = self.mdb["etf_daily_price_kr"]

        self.codes = {}
        self.get_etf_info()

    def __del__(self):
        pass

    # -------------------------------------------------------------
    # ETF 기본 정보 - 삼성자산운용만 불러오기
    # -------------------------------------------------------------
    def get_etf_info(self):

        # ------------------------------
        # MongoDB 코드 (삼성자산운용 필터 추가)
        # ------------------------------
        cursor = self.col_etf.find(
            {"manager": "삼성자산운용"},  # 🔥 여기가 핵심
            {"_id": 0, "code": 1, "name": 1}
        )

        df = pd.DataFrame(list(cursor))
        if df.empty:
            print("⚠ 삼성자산운용 ETF 정보 없음")
            return

        self.codes = dict(zip(df["code"], df["name"]))

    # -------------------------------------------------------------
    # ETF 일별 시세
    # -------------------------------------------------------------
    def get_daily_price(self, code, start_date=None, end_date=None):

        if start_date is None:
            start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        else:
            start_date = self._normalize_date(start_date)

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        else:
            end_date = self._normalize_date(end_date)

        keys = list(self.codes.keys())
        vals = list(self.codes.values())

        if code in keys:
            pass
        elif code in vals:
            code = keys[vals.index(code)]
        else:
            print(f"⚠ Code({code}) doesn't exist.")
            return None

        cursor = self.col_daily.find(
            {"code": code, "date": {"$gte": start_date, "$lte": end_date}},
            {"_id": 0}
        ).sort("date", 1)

        df = pd.DataFrame(list(cursor))
        if df.empty:
            print(f"⚠ ETF 시세({code}) 없음")
            return None

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df

    # -------------------------------------------------------------
    def _normalize_date(self, date_str):
        lst = re.split(r'\D+', date_str)
        year, month, day = map(int, lst[:3])
        return f"{year:04d}-{month:02d}-{day:02d}"
