import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import re


class MarketDB:
    def __init__(self):
        """생성자: SQLAlchemy 엔진 연결 및 종목코드 딕셔너리 생성"""
        db_url = "mysql+pymysql://root:0806@localhost/INVESTAR?charset=utf8"
        self.engine = create_engine(db_url)
        self.codes = {}
        self.get_comp_info()

    def __del__(self):
        """소멸자: SQLAlchemy 연결 해제"""
        if self.engine:
            self.engine.dispose()

    # ----------------------------------------------------------------------

    def get_comp_info(self):
        """company_info 테이블에서 읽어와서 codes에 저장"""
        sql = text("""
            SELECT code, name
            FROM company_info_us
        """)
        # ✅ SQLAlchemy 2.x 방식
        with self.engine.connect() as conn:
            us = pd.read_sql(sql, conn)

        self.codes = dict(zip(us['code'], us['name']))

    def get_comp_info_optimization(self):
        """company_info 테이블에서 읽어와서 codes에 저장하고 DataFrame 반환"""
        sql = text("""
            SELECT code, name
            FROM company_info_us
        """)
        # ✅ 동일하게 conn으로 실행
        with self.engine.connect() as conn:
            us = pd.read_sql(sql, conn)

        self.codes = dict(zip(us['code'], us['name']))
        return us[['code', 'name']]

    # ----------------------------------------------------------------------

    def get_daily_price(self, code, start_date=None, end_date=None):


        # ✅ 날짜 유효성 처리
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
            print(f"start_date is initialized to '{start_date}'")
        else:
            start_date = self._normalize_date(start_date)

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print(f"end_date is initialized to '{end_date}'")
        else:
            end_date = self._normalize_date(end_date)

        # ✅ 코드/이름 매핑
        codes_keys = list(self.codes.keys())
        codes_values = list(self.codes.values())

        if code in codes_keys:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print(f"ValueError: Code({code}) doesn't exist.")
            return None

        # ✅ SQLAlchemy 2.x 방식
        sql = text(f"""
            SELECT *
            FROM daily_price_us
            WHERE code = '{code}'
            AND date >= '{start_date}'
            AND date <= '{end_date}'
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        df.index = df['date']
        return df

    # ----------------------------------------------------------------------

    def _normalize_date(self, date_str):
        """날짜 문자열을 YYYY-MM-DD 형태로 변환 및 검증"""
        lst = re.split(r'\D+', date_str)
        lst = [x for x in lst if x]  # 빈 문자열 제거
        year, month, day = map(int, lst[:3])

        if not (1900 <= year <= 2200):
            raise ValueError(f"Invalid year: {year}")
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {month}")
        if not (1 <= day <= 31):
            raise ValueError(f"Invalid day: {day}")

        return f"{year:04d}-{month:02d}-{day:02d}"
