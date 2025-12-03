import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime

from common.mongo_util import MongoDB


class MarketDB:
    def __init__(self):

        # ------------------------------------------
        # MariaDB 연결 (주석)
        # ------------------------------------------
        # db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
        # self.engine = create_engine(db_url)

        # ------------------------------------------
        # MongoDB 연결 (실제 사용)
        # ------------------------------------------
        mongo = MongoDB()
        self.mongo = mongo  # 종료 위해 저장
        self.mdb = mongo.db

        self.col_comp = self.mdb["company_info_us"]     # 미국 종목 리스트
        self.col_daily = self.mdb["daily_price_us"]     # 미국 일별 시세

        self.codes = dict()
        self.getCompanyInfo()

    def __del__(self):
        try:
            self.mongo.close()
        except:
            pass

    # =====================================================================
    # 미국 기업 정보 로딩
    # =====================================================================
    def getCompanyInfo(self):

        # ------------------------------------------
        # 기존 MariaDB 코드 (주석)
        # ------------------------------------------
        # sql = text("""
        #     SELECT code, name
        #     FROM company_info_us
        # """)
        # with self.engine.connect() as conn:
        #     companyInfo = pd.read_sql(sql, conn)
        # self.codes = dict(zip(companyInfo['code'], companyInfo['name']))

        # ------------------------------------------
        # MongoDB 코드 (실제 사용)
        # ------------------------------------------
        cursor = self.col_comp.find({}, {"_id": 0, "code": 1, "name": 1})
        df = pd.DataFrame(list(cursor))

        if df.empty:
            print("⚠ company_info_us 컬렉션이 비어 있습니다.")
            return

        self.codes = dict(zip(df["code"], df["name"]))

    # =====================================================================
    # 미국 종목 일별 시세 조회
    # =====================================================================
    def getDailyPrice(self, code, startDate, endDate):

        # ------------------------------------------
        # 기존 MariaDB 코드 (주석)
        # ------------------------------------------
        # sql = text(f"""
        #     SELECT *
        #     FROM daily_price_us
        #     WHERE code = '{code}'
        #     AND date >= '{startDate}'
        #     AND date <= '{endDate}'
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
            print(f"⚠ 미국 시세({code}) 없음")
            return None

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        return df
