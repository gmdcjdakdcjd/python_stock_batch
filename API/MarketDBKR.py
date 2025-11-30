import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from datetime import datetime


class MarketDB:
    def __init__(self):
        """
        생성자
        기존: MariaDB 연결
        변경: MongoDB만 사용 (Maria 코드는 주석)
        """

        # ----------------------------------------
        # 기존 MariaDB 연결 (주석 처리)
        # ----------------------------------------
        # db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
        # self.engine = create_engine(db_url)

        # ----------------------------------------
        # MongoDB 연결 (실제 동작)
        # ----------------------------------------
        self.mongo = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
        self.mdb = self.mongo["investar"]

        self.col_comp = self.mdb["company_info_kr"]
        self.col_daily = self.mdb["daily_price_kr"]

        # 종목코드 저장 딕셔너리
        self.codes = dict()
        self.getCompanyInfo()

    def __del__(self):
        """MariaDB 사용 안 함 → 주석"""
        pass
        # if self.engine:
        #     self.engine.dispose()

    # ======================================================================
    # getCompanyInfo
    # ======================================================================
    def getCompanyInfo(self):
        """
        기존: MariaDB company_info 조회
        변경: MongoDB company_info_kr에서 조회
        """

        # ----------------------------------------------------
        # 기존 MariaDB 코드 (주석)
        # ----------------------------------------------------
        # sql = text("""
        #     SELECT code, name
        #     FROM company_info
        #     WHERE stock_type = '보통주'
        # """)
        # with self.engine.connect() as conn:
        #     companyInfo = pd.read_sql(sql, conn)
        # self.codes = dict(zip(companyInfo['code'], companyInfo['name']))

        # ----------------------------------------------------
        # MongoDB 버전 (실동)
        # ----------------------------------------------------
        cursor = self.col_comp.find({}, {"_id": 0, "code": 1, "name": 1})
        df = pd.DataFrame(list(cursor))

        if df.empty:
            print("⚠ company_info_kr 컬렉션이 비어 있습니다.")
            return

        self.codes = dict(zip(df["code"], df["name"]))

    # ======================================================================
    # getDailyPrice
    # ======================================================================
    def getDailyPrice(self, code, startDate, endDate):
        """
        기존: daily_price 테이블에서 읽어옴
        변경: MongoDB daily_price_kr에서 읽어옴
        """

        # ----------------------------------------------------
        # 기존 MariaDB SQL (주석)
        # ----------------------------------------------------
        # sql = text(f"""
        #     SELECT *
        #     FROM daily_price
        #     WHERE code = '{code}'
        #       AND date >= '{startDate}'
        #       AND date <= '{endDate}'
        # """)
        # with self.engine.connect() as conn:
        #     df = pd.read_sql(sql, conn)
        # df.index = df['date']
        # return df

        # ----------------------------------------------------
        # MongoDB 버전 (실동)
        # ----------------------------------------------------
        cursor = self.col_daily.find(
            {
                "code": code,
                "date": {"$gte": startDate, "$lte": endDate}
            },
            {"_id": 0}
        ).sort("date", 1)

        df = pd.DataFrame(list(cursor))

        if df.empty:
            print(f"⚠ MongoDB: 종목 {code} 날짜 [{startDate} ~ {endDate}] 데이터 없음")
            return None

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        return df
