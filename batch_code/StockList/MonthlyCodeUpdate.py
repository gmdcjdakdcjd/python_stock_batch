import pandas as pd
import pymysql
from datetime import datetime


class MonthlyCodeUpdater:
    def __init__(self):
        """생성자: MariaDB 연결"""
        self.conn = pymysql.connect(
            host='localhost', user='root', password='0806',
            db='INVESTAR', charset='utf8'
        )
        self.codes = dict()

    def read_etf_code(self):
        """ETF 코드 CSV 읽기"""
        path_etf = r'D:\STOCK_PROJECT\python_stock_batch\batch_code\csvDir\data_3116_20251004.csv'
        # https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010105
        # 여기서 분기별로 수동 업데이트
        etf = pd.read_csv(path_etf, encoding="cp949")
        etf = etf[['표준코드', '단축코드', '한글종목약명', '기초지수명', '지수산출기관',
                   '추적배수', '복제방법', '기초시장분류', '기초자산분류', '운용사', '과세유형']]
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
        return etf

    def update_etf_info(self):
        """ETF 코드 정보 갱신"""
        sql = "SELECT * FROM etf_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['name'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM etf_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            # ✅ 날짜 비교 안정화 (rs[0] None 대비)
            if rs[0] is None or str(rs[0]) < today:
                etf = self.read_etf_code()
                for idx in range(len(etf)):
                    std_code = etf.std_code.values[idx]
                    code = etf.code.values[idx]
                    name = etf.name.values[idx]
                    base_index = etf.base_index.values[idx]
                    index_provider = etf.index_provider.values[idx]
                    leverage = etf.leverage.values[idx]
                    replication_method = etf.replication_method.values[idx]
                    market_type = etf.market_type.values[idx]
                    asset_type = etf.asset_type.values[idx]
                    manager = etf.manager.values[idx]
                    tax_type = etf.tax_type.values[idx]

                    sql = (
                        "REPLACE INTO etf_info "
                        "(std_code, code, name, base_index, index_provider, leverage, replication_method, "
                        "market_type, asset_type, manager, tax_type, last_update) "
                        f"VALUES ('{std_code}', '{code}', '{name}', '{base_index}', '{index_provider}', "
                        f"'{leverage}', '{replication_method}', '{market_type}', '{asset_type}', "
                        f"'{manager}', '{tax_type}', '{today}')"
                    )
                    curs.execute(sql)
                    self.codes[code] = name
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx + 1:04d} {name} ({code}) > REPLACE INTO etf_info [OK]")

                self.conn.commit()
                print(f"ROWCOUNT={len(etf)}\n")

    def read_krx_code(self):
        """KRX 종목 코드 CSV 읽기"""
        path_krx = r'D:\STOCK_PROJECT\python_stock_batch\batch_code\csvDir\data_2716_20251004.csv'
        # https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010105
        # 여기서 분기별로 수동 업데이트
        krx = pd.read_csv(path_krx, encoding="cp949")
        krx = krx[['표준코드', '단축코드', '한글 종목약명', '시장구분', '증권구분', '주식종류']]
        krx = krx.rename(columns={
            '표준코드': 'std_code',
            '단축코드': 'code',
            '한글 종목약명': 'name',
            '시장구분': 'market_type',
            '증권구분': 'security_type',
            '주식종류': 'stock_type'
        })
        krx['code'] = krx['code'].astype(str).str.zfill(6)
        return krx

    def update_comp_info(self):
        """KRX 상장법인 정보 갱신"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['name'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] is None or str(rs[0]) < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    name = krx.name.values[idx]
                    market_type = krx.market_type.values[idx]
                    security_type = krx.security_type.values[idx]
                    stock_type = krx.stock_type.values[idx]
                    std_code = krx.std_code.values[idx]

                    sql = (
                        "REPLACE INTO company_info "
                        "(code, name, market_type, security_type, stock_type, std_code, last_update) "
                        f"VALUES ('{code}', '{name}', '{market_type}', '{security_type}', "
                        f"'{stock_type}', '{std_code}', '{today}')"
                    )
                    curs.execute(sql)
                    self.codes[code] = name
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx + 1:04d} {name} ({code}) > REPLACE INTO company_info [OK]")

                self.conn.commit()
                print(f"ROWCOUNT={len(krx)}\n")

    def update_all(self):
        """전체 코드 (회사 + ETF) 갱신"""
        self.update_comp_info()
        self.update_etf_info()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()


if __name__ == '__main__':
    updater = MonthlyCodeUpdater()
    updater.update_all()
