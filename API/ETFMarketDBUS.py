import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime


class MarketDB:
    def __init__(self):

        # ------------------------------------------
        # MariaDB (주석)
        # ------------------------------------------
        # db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
        # self.engine = create_engine(db_url)

        # ------------------------------------------
        # MongoDB (실제 사용)
        # ------------------------------------------
        self.mongo = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
        self.mdb = self.mongo["investar"]

        self.col_comp = self.mdb["etf_info_us"]
        self.col_daily = self.mdb["etf_daily_price_us"]

        self.codes = dict()
        self.getCompanyInfo()

    def __del__(self):
        pass
        # if self.engine:
        #     self.engine.dispose()

    # =====================================================================
    # BlackRock iShares ETF 기본 정보
    # =====================================================================
    def getCompanyInfo(self):

        # ------------------------------------------
        # 기존 MariaDB 코드 (주석)
        # ------------------------------------------
        # sql = text("""
        #     SELECT code, name
        #     FROM etf_info_us
        #     WHERE issuer = 'BlackRock (iShares)';
        # """)
        # with self.engine.connect() as conn:
        #     companyInfo = pd.read_sql(sql, conn)
        # self.codes = dict(zip(companyInfo['code'], companyInfo['name']))

        # ------------------------------------------
        # MongoDB 코드 (실제 사용)
        # ------------------------------------------
        cursor = self.col_comp.find(
            {"issuer": "BlackRock (iShares)"},   # 🔥 유지
            {"_id": 0, "code": 1, "name": 1}
        )

        df = pd.DataFrame(list(cursor))

        if df.empty:
            print("⚠ BlackRock (iShares) ETF 정보 없음")
            return

        self.codes = dict(zip(df["code"], df["name"]))

    # =====================================================================
    # 미국 ETF 일별 시세
    # =====================================================================
    def getDailyPrice(self, code, startDate, endDate):

        # ------------------------------------------
        # 기존 MariaDB SQL (주석)
        # ------------------------------------------
        # sql = text(f"""
        #     SELECT *
        #     FROM etf_daily_price_us
        #     WHERE code = '{code}'
        #       AND date >= '{startDate}'
        #       AND date <= '{endDate}'
        # """)
        # with self.engine.connect() as conn:
        #     df = pd.read_sql(sql, conn)
        # df.index = df['date']
        # return df

        # ------------------------------------------
        # MongoDB 버전 (실제 사용)
        # ------------------------------------------
        cursor = self.col_daily.find(
            {"code": code, "date": {"$gte": startDate, "$lte": endDate}},
            {"_id": 0}
        ).sort("date", 1)

        df = pd.DataFrame(list(cursor))

        if df.empty:
            print(f"⚠ 시세 데이터 없음 ({code})")
            return None

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df
