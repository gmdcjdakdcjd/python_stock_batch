import pandas as pd
import pymysql
from datetime import datetime, timedelta
from API import AnalyzeKR
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal

# --------------------------------------------------
# DualMomentumBatch: 듀얼 모멘텀 전략 배치 실행 클래스
# --------------------------------------------------
class DualMomentumBatch:
    def __init__(self):
        self.mk = Analyzer.MarketDB()  # 마켓DB 객체 (종목 정보 및 가격 데이터)
        self.MIN_ABS_RETURN = 40.0  # 절대 모멘텀 필터 기준 (%)

    def run_dual_momentum_batch(self, start_date, end_date, top_n=20):
        """
        듀얼 모멘텀 배치 실행 (상대 모멘텀 상위 N개 + 절대 모멘텀 필터)
        - DB에서 가격 데이터 조회
        - 상대 모멘텀 상위 N개 추출
        - 절대 모멘텀 기준 통과 종목만 DB 저장
        """
        connection = pymysql.connect(
            host='localhost', port=3306,
            db='INVESTAR', user='root', passwd='0806', autocommit=True
        )
        cursor = connection.cursor()

        # 날짜 보정 함수: 실제 거래일로 보정
        def adjust_date(date_str):
            sql = f"SELECT MAX(date) FROM daily_price WHERE date <= '{date_str}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            return result[0].strftime('%Y-%m-%d') if result and result[0] else None

        start_date = adjust_date(start_date)
        end_date = adjust_date(end_date)
        print(f"\n[DualMomentum 배치 실행] ({start_date} ~ {end_date})")

        # 전체 종목 수익률 계산
        rows = []  # [code, name, old_price, new_price, returns]
        for code, name in self.mk.codes.items():
            try:
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{start_date}'")
                start_val = cursor.fetchone()
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{end_date}'")
                end_val = cursor.fetchone()
                if not start_val or not end_val:
                    continue  # 가격 데이터 없으면 스킵

                old_price, new_price = float(start_val[0]), float(end_val[0])
                returns = (new_price / old_price - 1) * 100  # 수익률 계산
                rows.append([code, name, old_price, new_price, returns])
            except Exception:
                continue  # 예외 발생시 해당 종목 스킵

        connection.close()

        if not rows:
            print("데이터 없음: 수익률 계산 불가.")
            print("ROWCOUNT=0")
            print("CODECOUNT=0")
            return pd.DataFrame()

        # 상대 모멘텀 상위 N개 추출
        df = pd.DataFrame(rows, columns=['code', 'name', 'old_price', 'new_price', 'returns'])
        df_top = df.sort_values(by='returns', ascending=False).head(top_n)

        print("\n[상대 모멘텀 상위 종목]")
        print("=" * 70)
        print(f"{'순위':<4} {'종목명':<20} {'수익률(%)':>10} {'시작가':>10} {'종가':>10}")
        print("-" * 70)
        for rank, row in enumerate(df_top.itertuples(), start=1):
            print(f"{rank:<4} {row.name:<20} {row.returns:>10.2f} {row.old_price:>10.0f} {row.new_price:>10.0f}")
        print("-" * 70)
        print(f"상대 모멘텀 평균 수익률: {df_top['returns'].mean():.2f}%")
        print("=" * 70)

        # 절대 모멘텀 필터 적용 (상위 N개 중 기준 초과만)
        df_abs = df_top[df_top['returns'] > self.MIN_ABS_RETURN].copy()

        print(f"\n[절대 모멘텀 통과 종목] (기준: {self.MIN_ABS_RETURN:.1f}%)")
        print("=" * 70)
        print(f"{'순위':<4} {'종목명':<20} {'수익률(%)':>10} {'시작가':>10} {'종가':>10}")
        print("-" * 70)
        for rank, row in enumerate(df_abs.itertuples(), start=1):
            print(f"{rank:<4} {row.name:<20} {row.returns:>10.2f} {row.old_price:>10.0f} {row.new_price:>10.0f}")
        print("-" * 70)
        print(f"절대 모멘텀 평균 수익률: {df_abs['returns'].mean():.2f}%")
        print("=" * 70)

        # DB 저장 (절대 모멘텀 통과 종목만)
        print(f"\n[DB 저장 대상] 절대 모멘텀 통과 {len(df_abs)}개 종목 (상위 {top_n} 중)")
        result_id = save_strategy_summary(
            strategy_name='DualMomentum',
            signal_date=end_date,
            signal_type='BATCH',
            total_return=float(df_abs['returns'].mean()) if not df_abs.empty else 0.0
        )

        success_count = 0  # 저장 성공 종목 수
        for rank, row in enumerate(df_abs.itertuples(), start=1):
            try:
                save_strategy_signal(
                    result_id=result_id,
                    code=row.code,
                    name=row.name,
                    action='TOP_ABS',
                    price=float(row.new_price),
                    old_price=float(row.old_price),
                    returns=float(row.returns),
                    rank_order=rank,
                    signal_date=end_date
                )
                success_count += 1
            except Exception as e:
                print(f"저장 실패: {row.name}, 사유: {e}")

        print(f"[DualMomentum 배치 저장 완료] (result_id={result_id})")
        print(f"ROWCOUNT={len(df_abs)}")
        print(f"CODECOUNT={success_count}")
        print("=" * 70)
        return df_abs


if __name__ == '__main__':
    # 6개월 기준 날짜 자동 계산
    dm = DualMomentumBatch()
    today = datetime.today()
    start = (today - timedelta(days=90)).strftime('%Y-%m-%d')  # 90일(약 3개월)로 설정
    end = today.strftime('%Y-%m-%d')
    dm.run_dual_momentum_batch(start, end, top_n=20)
