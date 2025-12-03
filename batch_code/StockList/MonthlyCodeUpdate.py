import pandas as pd
from datetime import datetime
from pymongo import MongoClient

from common.mongo_util import MongoDB


class MonthlyCodeUpdater:
    def __init__(self):
        """MongoDB 연결 (공통 유틸)"""
        mongo = MongoDB()
        self.mongo = mongo
        self.db = mongo.db
        self.col_company = self.db["company_info_kr"]
        self.col_etf = self.db["etf_info_kr"]

        self.codes = dict()

    # ------------------------------------------------------
    # ETF CSV 읽기
    # ------------------------------------------------------
    def read_etf_code(self):
        # https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010105
        path_etf = r'D:\STOCK_PROJECT\python_stock_batch\batch_code\csvDir\data_3649_20251121.csv'
        etf = pd.read_csv(path_etf, encoding="cp949", dtype={'한글종목약명': str})

        etf = etf[['표준코드', '단축코드', '한글종목약명', '기초지수명',
                   '지수산출기관', '추적배수', '복제방법', '기초시장분류',
                   '기초자산분류', '운용사', '과세유형']]

        etf = etf.rename(columns={
            '표준코드': 'std_code',
            '단축코드': 'code',
            '한글종목약명': 'name',
            '기초지수명': 'base_index',
            '지수산출기관': 'index_provider',
            '추적배수': 'leverage',
            '복제방법': 'replication_method',
            '기초시장분류': 'market_type',
            '기초자산분류': 'asset_type',
            '운용사': 'manager',
            '과세유형': 'tax_type'
        })

        etf['code'] = etf['code'].astype(str).str.zfill(6)
        etf['name'] = etf['name'].astype(str)
        return etf

    # ------------------------------------------------------
    # ETF 저장
    # ------------------------------------------------------
    def update_etf_info(self):
        today = datetime.today().strftime('%Y-%m-%d')
        etf = self.read_etf_code()

        for idx, row in etf.iterrows():
            doc = {
                "std_code": row["std_code"],
                "code": row["code"],
                "name": row["name"],  # 수정: row.name → row["name"]
                "base_index": row["base_index"],
                "index_provider": row["index_provider"],
                "leverage": row["leverage"],
                "replication_method": row["replication_method"],
                "market_type": row["market_type"],
                "asset_type": row["asset_type"],
                "manager": row["manager"],
                "tax_type": row["tax_type"],
                "last_update": today
            }

            self.col_etf.update_one(
                {"code": row["code"]},
                {"$set": doc},
                upsert=True
            )

            tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
            print(f"[{tmnow}] #{idx+1:04d} {row['name']} ({row['code']}) > UPSERT etf_info_kr OK")






    # ------------------------------------------------------
    # 회사 CSV 읽기
    # ------------------------------------------------------
    def read_krx_code(self):
        # https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010105
        path_krx = r'D:\STOCK_PROJECT\python_stock_batch\batch_code\csvDir\data_0251_20251203.csv'
        krx = pd.read_csv(path_krx, encoding="cp949", dtype={'한글 종목약명': str})

        krx = krx[['표준코드', '단축코드', '한글 종목약명', '시장구분',
                   '증권구분', '주식종류']]

        krx = krx.rename(columns={
            '표준코드': 'std_code',
            '단축코드': 'code',
            '한글 종목약명': 'name',
            '시장구분': 'market_type',
            '증권구분': 'security_type',
            '주식종류': 'stock_type'
        })

        krx['code'] = krx['code'].astype(str).str.zfill(6)
        krx['name'] = krx['name'].astype(str)
        return krx

    # ------------------------------------------------------
    # 회사 저장
    # ------------------------------------------------------
    def update_comp_info(self):
        today = datetime.today().strftime('%Y-%m-%d')
        krx = self.read_krx_code()

        for idx, row in krx.iterrows():
            doc = {
                "code": row["code"],
                "name": row["name"],  # 수정: row.name → row["name"]
                "market_type": row["market_type"],
                "security_type": row["security_type"],
                "stock_type": row["stock_type"],
                "std_code": row["std_code"],
                "last_update": today
            }

            self.col_company.update_one(
                {"code": row["code"]},
                {"$set": doc},
                upsert=True
            )

            tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
            print(f"[{tmnow}] #{idx+1:04d} {row['name']} ({row['code']}) > UPSERT company_info_kr OK")

    # ------------------------------------------------------
    # 전체 업데이트
    # ------------------------------------------------------
    def update_all(self):
        self.update_comp_info()
        self.update_etf_info()


if __name__ == '__main__':
    updater = MonthlyCodeUpdater()
    updater.update_all()
    updater.mongo.close()
