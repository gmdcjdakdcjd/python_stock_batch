import pandas as pd
import pymysql
from datetime import datetime, timedelta
from API import AnalyzeKR
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal


class DualMomentum:
    def __init__(self):
        """생성자: KRX 종목코드(codes)를 구하기 위한 MarkgetDB 객체 생성"""
        self.mk = Analyzer.MarketDB()

    def get_rltv_momentum(self, start_date, end_date, stock_count):
        """특정 기간 동안 수익률이 제일 높았던 stock_count 개의 종목들 (상대 모멘텀)"""
        connection = pymysql.connect(host='localhost', port=3306,
                                     db='INVESTAR', user='root', passwd='0806', autocommit=True)
        cursor = connection.cursor()

        # 날짜 보정
        sql = f"select max(date) from daily_price where date <= '{start_date}'"
        cursor.execute(sql)
        result = cursor.fetchone()
        if not result or result[0] is None:
            print("⚠️ start_date 반환값 없음")
            return
        start_date = result[0].strftime('%Y-%m-%d')

        sql = f"select max(date) from daily_price where date <= '{end_date}'"
        cursor.execute(sql)
        result = cursor.fetchone()
        if not result or result[0] is None:
            print("⚠️ end_date 반환값 없음")
            return
        end_date = result[0].strftime('%Y-%m-%d')

        # 수익률 계산
        rows = []
        columns = ['code', 'name', 'old_price', 'new_price', 'returns']
        for _, code in enumerate(self.mk.codes):
            try:
                sql = f"select close from daily_price where code='{code}' and date='{start_date}'"
                cursor.execute(sql)
                start_val = cursor.fetchone()
                if start_val is None:
                    continue

                sql = f"select close from daily_price where code='{code}' and date='{end_date}'"
                cursor.execute(sql)
                end_val = cursor.fetchone()
                if end_val is None:
                    continue

                old_price = float(start_val[0])
                new_price = float(end_val[0])
                returns = (new_price / old_price - 1) * 100
                rows.append([code, self.mk.codes[code], old_price, new_price, returns])
            except Exception as e:
                continue

        df = pd.DataFrame(rows, columns=columns)
        df = df.sort_values(by='returns', ascending=False).head(stock_count)
        connection.close()

        # 결과 출력
        print(df)
        print(f"\nRelative momentum ({start_date} ~ {end_date}) : {df['returns'].mean():.2f}%\n")

        # 🚫 DB 저장 생략 (상대 모멘텀은 계산용이므로 저장 안 함)
        # result_id = save_strategy_summary(
        #     strategy_name='DualMomentum',
        #     signal_date=end_date,
        #     signal_type='RELATIVE',
        #     total_return=float(df['returns'].mean())
        # )
        #
        # for _, row in df.iterrows():
        #     save_strategy_signal(
        #         result_id=result_id,
        #         code=row['code'],
        #         name=row['name'],
        #         action='TOP_RLT',
        #         price=float(row['new_price']),
        #         signal_date=end_date
        #     )
        #
        # print(f"💾 상대 모멘텀 저장 완료 (result_id={result_id})")

        return df

    def get_abs_momentum(self, rltv_momentum, start_date, end_date):
        """특정 기간 동안 상대 모멘텀 종목들의 절대 모멘텀 계산"""
        stockList = list(rltv_momentum['code'])
        connection = pymysql.connect(host='localhost', port=3306,
                                     db='INVESTAR', user='root', passwd='0806', autocommit=True)
        cursor = connection.cursor()

        # 날짜 보정
        sql = f"select max(date) from daily_price where date <= '{start_date}'"
        cursor.execute(sql)
        result = cursor.fetchone()
        if not result or result[0] is None:
            print("⚠️ start_date 반환값 없음")
            return
        start_date = result[0].strftime('%Y-%m-%d')

        sql = f"select max(date) from daily_price where date <= '{end_date}'"
        cursor.execute(sql)
        result = cursor.fetchone()
        if not result or result[0] is None:
            print("⚠️ end_date 반환값 없음")
            return
        end_date = result[0].strftime('%Y-%m-%d')

        # 수익률 계산
        rows = []
        columns = ['code', 'name', 'old_price', 'new_price', 'returns']
        for _, code in enumerate(stockList):
            try:
                sql = f"select close from daily_price where code='{code}' and date='{start_date}'"
                cursor.execute(sql)
                start_val = cursor.fetchone()
                if start_val is None:
                    continue

                sql = f"select close from daily_price where code='{code}' and date='{end_date}'"
                cursor.execute(sql)
                end_val = cursor.fetchone()
                if end_val is None:
                    continue

                old_price = float(start_val[0])
                new_price = float(end_val[0])
                returns = (new_price / old_price - 1) * 100
                rows.append([code, self.mk.codes[code], old_price, new_price, returns])
            except Exception as e:
                continue

        df = pd.DataFrame(rows, columns=columns)
        df = df.sort_values(by='returns', ascending=False)
        connection.close()

        # 출력
        print(df)
        print(f"\nAbsolute momentum ({start_date} ~ {end_date}) : {df['returns'].mean():.2f}%")

        # ✅ DB 저장 (절대 모멘텀만 저장)
        result_id = save_strategy_summary(
            strategy_name='DualMomentum',
            signal_date=end_date,
            signal_type='ABSOLUTE',
            total_return=float(df['returns'].mean())
        )

        for _, row in df.iterrows():
            save_strategy_signal(
                result_id=result_id,
                code=row['code'],
                name=row['name'],
                action='TOP_ABS',
                price=float(row['new_price']),
                signal_date=end_date
            )

        print(f"💾 절대 모멘텀 저장 완료 (result_id={result_id})")
        return df


if __name__ == '__main__':
    dm = DualMomentum()

    today = datetime.today()

    # 상대 모멘텀 기간: 최근 6개월
    start_date_rltv = (today - timedelta(days=180)).strftime('%Y-%m-%d')
    end_date_rltv = today.strftime('%Y-%m-%d')

    # 절대 모멘텀 기간: 최근 3개월
    start_date_abs = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    end_date_abs = today.strftime('%Y-%m-%d')

    # ✅ 1. 상대 모멘텀 (DB 저장 안 함)
    rltv = dm.get_rltv_momentum(start_date_rltv, end_date_rltv, 10)

    # ✅ 2. 절대 모멘텀 (DB 저장)
    dm.get_abs_momentum(rltv, start_date_abs, end_date_abs)
