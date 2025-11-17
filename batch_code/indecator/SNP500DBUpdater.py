import pandas as pd
import pymysql
import requests
from datetime import datetime


class SP500DBUpdater:
    def __init__(self):
        """MariaDB 연결"""
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='0806',
            db='INVESTAR',
            charset='utf8'
        )

    def __del__(self):
        """DB 연결 해제"""
        self.conn.close()

    # ===============================================================
    # 1) S&P500 일별 시세 (네이버 worldDayListJson API)
    # ===============================================================
    def read_sp500(self):
        try:
            url = "https://finance.naver.com/world/worldDayListJson.naver?symbol=SPI@SPX&fdtc=0"
            ## https://finance.naver.com/world/worldDayListJson.naver?symbol=SPI@SPX&fdtc=0 해당 링크 사용
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.naver.com/",
            }

            res = requests.get(url, headers=headers)
            data = res.json()

            # CASE1: { "worldDayList": [...] }
            if isinstance(data, dict) and "worldDayList" in data:
                data = data["worldDayList"]

            # CASE2: [ {...}, {...} ]
            elif isinstance(data, list):
                data = data

            else:
                print("[ERROR] JSON 구조 이상:", data)
                return None

            df = pd.DataFrame(data)

            # -------------------------
            # 날짜 컬럼 감지
            # -------------------------
            if 'day' in df.columns:
                df['date'] = df['day'].str.replace(".", "-")
            elif 'xymd' in df.columns:
                df['date'] = pd.to_datetime(df['xymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            else:
                raise Exception("date field not found")

            # -------------------------
            # 종가 컬럼 감지
            # -------------------------
            if 'close' in df.columns:
                df['close'] = df['close'].astype(str).str.replace(",", "").astype(float)
            elif 'clos' in df.columns:
                df['close'] = df['clos'].astype(str).str.replace(",", "").astype(float)
            else:
                raise Exception("close field not found")

            # -------------------------
            # 전일비 컬럼 감지
            # -------------------------
            if 'diff' in df.columns:
                df['change_amount'] = df['diff'].astype(str).str.replace(",", "").astype(float)
            elif 'dff' in df.columns:
                df['change_amount'] = df['dff'].astype(str).str.replace(",", "").astype(float)
            else:
                raise Exception("diff field not found")

            # -------------------------
            # 등락률 컬럼 감지
            # -------------------------
            if 'rate' in df.columns:
                df['change_rate'] = df['rate'].astype(float)
            else:
                df['change_rate'] = 0.0

            # -------------------------
            # 필요한 컬럼만 반환
            # -------------------------
            return df[['date', 'close', 'change_amount', 'change_rate']]

        except Exception as e:
            print("Exception (SP500):", e)
            return None

    # ===============================================================
    # 2) DB 저장
    # ===============================================================
    def replace_into_db(self, df):
        """market_indicator 테이블에 S&P500 지수 저장"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"""
                REPLACE INTO market_indicator
                (code, date, close, change_amount, change_rate)
                VALUES ('SPX', '{r.date}', {r.close}, {r.change_amount}, {r.change_rate});
                """
                curs.execute(sql)

            self.conn.commit()
            print(f"[INFO] SPX {len(df)} rows 저장 완료")

    # ===============================================================
    # 3) 전체 실행
    # ===============================================================
    def update_sp500(self):
        print("[INFO] SPX 데이터 수집 시작...")

        df = self.read_sp500()
        if df is not None and not df.empty:
            self.replace_into_db(df)
            print("[INFO] SPX 업데이트 완료")
        else:
            print("[WARN] SPX 데이터 수집 실패")


# 실행부
if __name__ == '__main__':
    dbu = SP500DBUpdater()
    dbu.update_sp500()
