import pandas as pd
from bs4 import BeautifulSoup
import pymysql, calendar, time, json
import requests
from datetime import datetime
from threading import Timer


class DBUpdater:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root',
                                    password='0806', db='INVESTAR', charset='utf8')



    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()


    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html = BeautifulSoup(requests.get(url,
                                              headers={'User-agent': 'Mozilla/5.0'}).text, "lxml")
            pgrr = html.find("td", class_="pgRR")

            if pgrr is None:
                print(f"[WARN] {company} ({code}) 페이지 구조 이상 - 기본 1페이지만 수집")
                lastpage = 1
            else:
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]

           #  s = str(pgrr.a["href"]).split('=')
           #  lastpage = s[-1]

            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                # ✅ append() → concat()으로 변경 (pandas 최신 대응)
                page_df = pd.read_html(requests.get(pg_url,
                                                    headers={'User-agent': 'Mozilla/5.0'}).text)[0]
                df = pd.concat([df, page_df], ignore_index=True)
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
                      format(tmnow, company, code, page, pages), end="\r")

            df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff'
                , '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})

            # ✅ 날짜 정규화 (불완전 데이터 방지)
            df['date'] = df['date'].replace('.', '-')
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['date'])

            # ✅ diff 문자열 처리 강화
            df['diff'] = df['diff'].astype(str).str.extract(r'(\d+)')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close',
                                                                         'diff', 'open', 'high', 'low',
                                                                         'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occured :', str(e))
            return None
        return df

    def replace_into_db(self, df, num, code, company):
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', " \
                      f"'{r.date}', {r.open}, {r.high}, {r.low}, {r.close}, " \
                      f"{r.diff}, {r.volume})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price [OK]'
                  .format(datetime.now().strftime('%Y-%m-%d %H:%M'),
                          num + 1, company, code, len(df)))
            # ✅ 자바에서 파싱할 수 있는 형식
            print(f"ROWCOUNT={len(df)}")
            return len(df)  # 👈 row count 반환

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        total_count = 0
        processed_codes = 0  # 👈 종목 개수 카운터

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            total_count += self.replace_into_db(df, idx, code, self.codes[code])
            processed_codes += 1  # 👈 종목 하나 성공 시 카운트

        # ✅ 자바에서 파싱할 수 있는 형식
        print(f"ROWCOUNT={total_count}")
        print(f"CODECOUNT={processed_codes}")

    def load_codes_from_db(self):
        with self.conn.cursor() as curs:
            sql = "SELECT code, name FROM company_info WHERE stock_type = '보통주'"
            curs.execute(sql)
            rows = curs.fetchall()
            self.codes = {code: name for code, name in rows}
        print(f"[INFO] {len(self.codes)}개 KOSPI 로드 완료 (보통주)")

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트"""
        self.load_codes_from_db()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)


#        tmnow = datetime.now()
#        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]

#        if tmnow.month == 12 and tmnow.day == lastday:
#            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1, hour=17, minute=0, second=0)
#        elif tmnow.day == lastday:
#            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17, minute=0, second=0)
#        else:
#            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0, second=0)
#       tmdiff = tmnext - tmnow
#        secs = tmdiff.seconds
#        t = Timer(secs, self.execute_daily)
#        print("Waiting for next update ({}) ... ".format(tmnext.strftime ('%Y-%m-%d %H:%M')))
#        t.start()


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
