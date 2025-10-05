import pandas as pd
import pymysql
from datetime import datetime, timedelta
from API import Analyzer


class DualMomentum:
    def __init__(self):
        """생성자: KRX 종목코드(codes)를 구하기 위한 MarketDB 객체 생성"""
        self.mk = Analyzer.MarketDB()

    def get_rltv_momentum(self, start_date, end_date, stock_count=100):
        """시장 전체 종목 중 특정 기간 동안 수익률이 가장 높았던 상위 N개 종목 추출 (상대 모멘텀)"""
        connection = pymysql.connect(
            host='localhost', port=3306,
            db='INVESTAR', user='root', passwd='0806', autocommit=True
        )
        cursor = connection.cursor()

        def adjust_date(date_str):
            sql = f"SELECT MAX(date) FROM daily_price WHERE date <= '{date_str}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            return result[0].strftime('%Y-%m-%d') if result and result[0] else None

        start_date = adjust_date(start_date)
        end_date = adjust_date(end_date)
        if not start_date or not end_date:
            print("⚠️ 날짜 보정 실패 (데이터 없음)")
            return None

        total_count = len(self.mk.codes)
        rows = []
        print(f"\n📊 [상대 모멘텀 계산 시작] {start_date} ~ {end_date}")
        print(f"총 {total_count:,}개 종목 계산 중...")

        for idx, code in enumerate(self.mk.codes):
            try:
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{start_date}'")
                start_val = cursor.fetchone()
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{end_date}'")
                end_val = cursor.fetchone()
                if not start_val or not end_val:
                    continue

                old_price, new_price = float(start_val[0]), float(end_val[0])
                returns = (new_price / old_price - 1) * 100
                rows.append([code, self.mk.codes[code], old_price, new_price, returns])

                if (idx + 1) % 500 == 0:
                    print(f"  → {idx + 1:,}/{total_count:,} 종목 처리 완료")
            except Exception:
                continue

        connection.close()
        df = pd.DataFrame(rows, columns=['code', 'name', 'old_price', 'new_price', 'returns'])
        df = df.sort_values(by='returns', ascending=False).head(stock_count)

        print(f"\n🏁 상위 {stock_count}개 종목 추출 완료")
        print("=" * 70)
        print(f"{'순위':<4} {'종목명':<20} {'수익률(%)':>10} {'시작가':>10} {'종가':>10}")
        print("-" * 70)

        for rank, row in enumerate(df.itertuples(), start=1):
            print(f"{rank:<4} {row.name:<20} {row.returns:>10.2f} {row.old_price:>10.0f} {row.new_price:>10.0f}")

        print("-" * 70)
        print(f"📈 평균 수익률: {df['returns'].mean():.2f}%")
        print("=" * 70)

        return df

    def get_abs_momentum(self, rltv_momentum, start_date, end_date):
        """상대 모멘텀 상위 종목들의 절대 모멘텀 계산 (DB 저장 없음)"""
        stockList = list(rltv_momentum['code'])

        connection = pymysql.connect(
            host='localhost', port=3306,
            db='INVESTAR', user='root', passwd='0806', autocommit=True
        )
        cursor = connection.cursor()

        def adjust_date(date_str):
            sql = f"SELECT MAX(date) FROM daily_price WHERE date <= '{date_str}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            return result[0].strftime('%Y-%m-%d') if result and result[0] else None

        start_date = adjust_date(start_date)
        end_date = adjust_date(end_date)

        rows = []
        print(f"\n📈 [절대 모멘텀 계산 시작] {start_date} ~ {end_date}")
        print(f"대상: 상대 모멘텀 상위 {len(stockList):,}개 종목 계산 중...")

        for idx, code in enumerate(stockList):
            try:
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{start_date}'")
                start_val = cursor.fetchone()
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{end_date}'")
                end_val = cursor.fetchone()
                if not start_val or not end_val:
                    continue

                old_price, new_price = float(start_val[0]), float(end_val[0])
                returns = (new_price / old_price - 1) * 100
                rows.append([code, self.mk.codes[code], old_price, new_price, returns])
            except Exception:
                continue

        connection.close()
        df = pd.DataFrame(rows, columns=['code', 'name', 'old_price', 'new_price', 'returns'])
        df = df.sort_values(by='returns', ascending=False)

        print(f"\n🏁 절대 모멘텀 계산 완료 (상위 {len(df)}개)")
        print("=" * 70)
        print(f"{'순위':<4} {'종목명':<20} {'수익률(%)':>10} {'시작가':>10} {'종가':>10}")
        print("-" * 70)

        for rank, row in enumerate(df.itertuples(), start=1):
            print(f"{rank:<4} {row.name:<20} {row.returns:>10.2f} {row.old_price:>10.0f} {row.new_price:>10.0f}")

        print("-" * 70)
        print(f"📈 평균 수익률: {df['returns'].mean():.2f}%")
        print("=" * 70)
        print("💬 (참고) DB 저장은 생략되었습니다 — 로그 전용 실행 모드입니다.")
        return df


if __name__ == '__main__':
    dm = DualMomentum()
    today = datetime.today()

    start_date = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    rltv = dm.get_rltv_momentum(start_date, end_date, 100)

    if rltv is not None and not rltv.empty:
        dm.get_abs_momentum(rltv, start_date, end_date)
    else:
        print("⚠️ 상대 모멘텀 결과가 비어있어 절대 모멘텀 계산을 건너뜀.")
