import pandas as pd
from bs4 import BeautifulSoup
import pymysql, calendar, time, json
import requests
from datetime import datetime
from threading import Timer


class DBUpdater:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='0806',
            db='INVESTAR',
            charset='utf8'
        )
        self.conn.commit()
        self.codes = dict()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 ETF 시세를 읽어서 DataFrame으로 반환"""
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html = BeautifulSoup(
                requests.get(url, headers={'User-agent': 'Mozilla/5.0'}).text, "lxml"
            )

            # ✅ 페이지 수 계산
            pgrr = html.find("td", class_="pgRR")
            if pgrr is None:
                print(f"[WARN] {company} ({code}) 페이지 구조 이상 - 기본 1페이지만 수집")
                lastpage = 1
            else:
                s = str(pgrr.a["href"]).split("=")
                lastpage = s[-1]

            # ✅ 페이지 순회
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)

            for page in range(1, pages + 1):
                pg_url = f"{url}&page={page}"
                page_df = pd.read_html(
                    requests.get(pg_url, headers={'User-agent': 'Mozilla/5.0'}).text
                )[0]
                df = pd.concat([df, page_df], ignore_index=True)

                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f"[{tmnow}] {company} ({code}) : {page:04d}/{pages:04d} pages downloading...", end="\r")

            # ✅ 컬럼명 변경 및 데이터 정리
            df = df.rename(columns={
                '날짜': 'date',
                '종가': 'close',
                '전일비': 'diff',
                '시가': 'open',
                '고가': 'high',
                '저가': 'low',
                '거래량': 'volume'
            })

            # ✅ 날짜 정규화
            df['date'] = df['date'].replace('.', '-')
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['date'])

            # ✅ 결측치/형식 변환
            df['diff'] = df['diff'].astype(str).str.extract(r'(\d+)')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[
                ['close', 'diff', 'open', 'high', 'low', 'volume']
            ].astype(int)

            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]

        except Exception as e:
            print(f"Exception occured : {company} ({code}) - {str(e)}")
            return None

        return df

    def replace_into_db(self, df, num, code, company):
        """네이버에서 읽어온 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"""
                    REPLACE INTO etf_daily_price
                    VALUES ('{code}', '{r.date}', {r.open}, {r.high}, {r.low},
                            {r.close}, {r.diff}, {r.volume})
                """
                curs.execute(sql)
            self.conn.commit()

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] "
            f"#{num + 1:04d} {company} ({code}) : {len(df)} rows > REPLACE INTO etf_daily_price [OK]"
        )
        print(f"ROWCOUNT={len(df)}")
        return len(df)

    def update_daily_price(self, pages_to_fetch):
        """ETF 시세를 네이버로부터 읽어서 DB에 업데이트"""
        total_count = 0
        processed_codes = 0

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None or df.empty:
                continue
            total_count += self.replace_into_db(df, idx, code, self.codes[code])
            processed_codes += 1

        print(f"ROWCOUNT={total_count}")
        print(f"CODECOUNT={processed_codes}")

    def load_codes_from_db(self):
        """KODEX/TIGER ETF만 로드"""
        with self.conn.cursor() as curs:
            sql = """
                SELECT code, name
                FROM etf_info
                WHERE name LIKE '%KODEX%' OR name LIKE '%TIGER%'
            """
            curs.execute(sql)
            rows = curs.fetchall()
            self.codes = {code: name for code, name in rows}
        print(f"[INFO] {len(self.codes)}개 ETF 로드 완료 (KODEX/TIGER)")

    def execute_daily(self):
        """ETF 데이터 업데이트 실행"""
        self.load_codes_from_db()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config.get('pages_to_fetch', 1)
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 1
                json.dump({'pages_to_fetch': 1}, out_file)

        self.update_daily_price(pages_to_fetch)


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
