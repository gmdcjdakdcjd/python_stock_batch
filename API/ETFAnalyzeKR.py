import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import re


class MarketDB:
    def __init__(self):

        # -------------------------------------------------------
        # MariaDB (주석 처리)
        # -------------------------------------------------------
        # db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
        # self.engine = create_engine(db_url)

        # -------------------------------------------------------
        # MongoDB 연결 (실제 사용)
        # -------------------------------------------------------
        self.mongo = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
        self.mdb = self.mongo["investar"]

        self.col_etf = self.mdb["etf_info_kr"]            # ETF 기본 정보
        self.col_daily = self.mdb["etf_daily_price_kr"]   # ETF 일별 시세

        self.codes = {}
        self.get_etf_info()   # ⭐ 삼성자산운용만 로딩

    def __del__(self):
        pass
        # if self.engine:
        #     self.engine.dispose()

    # =====================================================================
    # ETF 기본 정보 (삼성자산운용만)
    # =====================================================================
    def get_etf_info(self):

        # -------------------------------------------------------
        # MariaDB 코드 (주석)
        # -------------------------------------------------------
        # sql = text("""
        #     SELECT code, name
        #     FROM etf_info
        #     WHERE name LIKE '%KODEX%'
        # """)
        # with self.engine.connect() as conn:
        #     etf_info = pd.read_sql(sql, conn)
        # self.codes = dict(zip(etf_info['code'], etf_info['name']))

        # -------------------------------------------------------
        # MongoDB 코드 (삼성자산운용만)
        # -------------------------------------------------------
        cursor = self.col_etf.find(
            {"manager": "삼성자산운용"},        # ⭐ 핵심 필터
            {"_id": 0, "code": 1, "name": 1}
        )

        df = pd.DataFrame(list(cursor))
        if df.empty:
            print("⚠ 삼성자산운용 ETF 기본 정보 없음")
            return

        self.codes = dict(zip(df["code"], df["name"]))

    # =====================================================================
    # ETF 일별 시세 (MongoDB)
    # =====================================================================
    def get_daily_price(self, code, start_date=None, end_date=None):

        # 날짜 변환
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        else:
            start_date = self._normalize_date(start_date)

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        else:
            end_date = self._normalize_date(end_date)

        # 코드 매핑
        keys = list(self.codes.keys())
        vals = list(self.codes.values())

        if code in keys:
            pass
        elif code in vals:
            code = keys[vals.index(code)]
        else:
            print(f"⚠ Code({code}) doesn't exist.")
            return None

        # 🔥 날짜를 datetime으로 변환 (이게 핵심!)
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        # MongoDB 조회
        cursor = self.col_daily.find(
            {"code": code, "date": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0}
        ).sort("date", 1)

        df = pd.DataFrame(list(cursor))
        if df.empty:
            print(f"⚠ ETF 시세({code}) 없음")
            return None

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df

    # =====================================================================
    def _normalize_date(self, date_str):
        lst = re.split(r'\D+', date_str)
        year, month, day = map(int, lst[:3])
        return f"{year:04d}-{month:02d}-{day:02d}"

    def get_etf_info_optimization(self):
        """
        종목코드/이름을 DataFrame 형태로 반환하는 버전
        (전략 스캐너용)
        """
        cursor = self.col_etf.find(
            {"manager": "삼성자산운용"},   # ⭐ 보통주만 조회
            {"_id": 0, "code": 1, "name": 1}
        )

        df = pd.DataFrame(list(cursor))

        if df.empty:
            print("⚠ MongoDB etf_info_kr 데이터 없음")
            return pd.DataFrame(columns=["code", "name"])

        # self.codes 업데이트
        self.codes = dict(zip(df["code"], df["name"]))

        return df[["code", "name"]]


    # ----------------------------------------------------------------------
    # 🔥 날짜 보정: date <= 기준일 중 가장 최근 날짜
    # ----------------------------------------------------------------------
    def get_latest_date(self, date_str):
        """
        date <= date_str 인 가장 최근 거래일 반환
        """
        try:
            target = datetime.strptime(date_str, "%Y-%m-%d")

            doc = self.col_daily.find_one(
                {"date": {"$lte": target}},
                sort=[("date", -1)],
                projection={"_id": 0, "date": 1}
            )

            if doc:
                return doc["date"].strftime("%Y-%m-%d")
            return None

        except Exception as e:
            print(f"[Mongo ERROR] get_latest_date: {e}")
            return None


    # ----------------------------------------------------------------------
    # 🔥 전체 가격 데이터 조회 (기간 내 전체 종목 한 번에 가져오기)
    # ----------------------------------------------------------------------
    def get_all_daily_prices(self, start_date, end_date):
        """
        start_date ~ end_date 사이 전체 종목의 가격 정보를
        MongoDB에서 단 1회 조회하여 반환.
        """
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            cursor = self.col_daily.find(
                {"date": {"$gte": start, "$lte": end}},
                {
                    "_id": 0,
                    "code": 1,
                    "date": 1,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "diff": 1,
                    "last_update": 1
                }
            )

            df = pd.DataFrame(list(cursor))

            if df.empty:
                return df

            # date를 datetime 변환
            df["date"] = pd.to_datetime(df["date"])

            return df

        except Exception as e:
            print(f"[Mongo ERROR] get_all_daily_prices: {e}")
            return pd.DataFrame()
