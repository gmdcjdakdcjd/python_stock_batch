import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


class MarketDB:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
        self.engine = create_engine(db_url)
        self.codes = dict()
        self.getCompanyInfo()

    def __del__(self):
        """소멸자: SQLAlchemy 연결 해제"""
        if self.engine:
            self.engine.dispose()

    def getCompanyInfo(self):
        """company_info 테이블에서 읽어와서 codes에 저장"""
        sql = text("""       -- ✅ text()로 감싸줘야 함
            SELECT code, name
            FROM company_info_us
        """)
        with self.engine.connect() as conn:
            companyInfo = pd.read_sql(sql, conn)  # ✅ conn + text() 조합만 허용됨
        self.codes = dict(zip(companyInfo['code'], companyInfo['name']))

    def getDailyPrice(self, code, startDate, endDate):
        """daily_price 테이블에서 읽어와서 데이터프레임으로 반환"""
        sql = text(f"""       -- ✅ text() 필수
            SELECT *
            FROM daily_price_us
            WHERE code = '{code}'
            AND date >= '{startDate}'
            AND date <= '{endDate}'
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        df.index = df['date']
        return df
