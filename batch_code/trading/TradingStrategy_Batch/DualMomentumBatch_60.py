import pandas as pd
import pymysql
from datetime import datetime, timedelta
from API import Analyzer
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal


class DualMomentumBatch:
    def __init__(self):
        self.mk = Analyzer.MarketDB()
        self.MIN_ABS_RETURN = 40.0  # 절대 모멘텀 필터 기준 (%)
        self.TOP_RELATIVE = 40      # 상대 모멘텀 선별 개수
        self.FINAL_TOP = 20         # 최종 출력/저장 개수

    def run_dual_momentum_batch(self, start_date, end_date):
        """듀얼 모멘텀 배치 실행 (상대 모멘텀 상위 40개 → 절대 모멘텀 필터 → 상위 20개 저장)"""
        connection = pymysql.connect(
            host='localhost', port=3306,
            db='INVESTAR', user='root', passwd='0806', autocommit=True
        )
        cursor = connection.cursor()

        # ✅ 날짜 보정 함수
        def adjust_date(date_str):
            sql = f"SELECT MAX(date) FROM daily_price WHERE date <= '{date_str}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            return result[0].strftime('%Y-%m-%d') if result and result[0] else None

        start_date = adjust_date(start_date)
        end_date = adjust_date(end_date)
        print(f"\n🚀 [DUAL MOMENTUM] ({start_date} ~ {end_date}) 실행 시작\n")

        # ✅ 전체 종목 수익률 계산
        rows = []
        for code, name in self.mk.codes.items():
            try:
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{start_date}'")
                start_val = cursor.fetchone()
                cursor.execute(f"SELECT close FROM daily_price WHERE code='{code}' AND date='{end_date}'")
                end_val = cursor.fetchone()
                if not start_val or not end_val:
                    continue

                old_price, new_price = float(start_val[0]), float(end_val[0])
                returns = (new_price / old_price - 1) * 100
                rows.append([code, name, old_price, new_price, returns])
            except Exception:
                continue

        connection.close()

        if not rows:
            print("💤 데이터 부족 — 수익률 계산 불가.")
            return pd.DataFrame()

        # ✅ 상대 모멘텀 상위 40개
        df = pd.DataFrame(rows, columns=['code', 'name', 'old_price', 'new_price', 'returns'])
        df_top40 = df.sort_values(by='returns', ascending=False).head(self.TOP_RELATIVE)

        # ✅ 절대 모멘텀 기준 (40%) 통과한 종목만
        df_abs = df_top40[df_top40['returns'] > self.MIN_ABS_RETURN].copy()

        # ✅ 최종 상위 20개만 출력 및 DB 저장
        df_final = df_abs.sort_values(by='returns', ascending=False).head(self.FINAL_TOP)

        # ✅ 콘솔 리포트 출력 (최종 20개만)
        print(f"📈 [DUAL MOMENTUM] 상대모멘텀 상위 {self.TOP_RELATIVE}개 → 절대모멘텀({self.MIN_ABS_RETURN:.1f}%) 통과 후 상위 {self.FINAL_TOP}개:\n")
        if df_final.empty:
            print("💤 절대모멘텀 통과 종목 없음.\n")
            return pd.DataFrame()
        else:
            print(df_final[['code', 'name', 'old_price', 'new_price', 'returns']].to_string(index=False))
            ratio = (len(df_abs) / len(df_top40)) * 100
            print(f"\n📊 절대모멘텀 통과율: {ratio:.1f}% ({len(df_abs)}/{len(df_top40)})")
            print(f"총 {len(df_final)}건 최종 선정.\n")

        # ✅ DB 저장
        strategy_name = "DUAL_MOMENTUM_3M"
        signal_type = "FLOW"

        result_id = save_strategy_summary(
            strategy_name=strategy_name,
            signal_date=end_date,
            signal_type=signal_type,
            total_return=float(df_final['returns'].mean()) if not df_final.empty else None,
            total_risk=None,
            total_sharpe=None
        )

        print(f"🧾 [RESULT_ID] 이번 실행으로 저장된 result_id = {result_id}\n")

        # ✅ 세부 결과 저장
        for idx, row in enumerate(df_final.itertuples(), start=1):
            save_strategy_signal(
                result_id=result_id,
                code=row.code,
                name=row.name,
                action='BUY',
                price=float(row.new_price),
                old_price=float(row.old_price),
                returns=float(row.returns),
                rank_order=idx,
                signal_date=end_date
            )

        avg_return = df_final['returns'].mean() if not df_final.empty else 0

        print(f"ROWCOUNT={len(df_final)}")
        print(f"CODECOUNT={len(df_final)}")
        print(f"RESULT_ID={result_id}")

        print(f"✅ [DB저장완료] 절대모멘텀 통과 {len(df_final)}건 (result_id={result_id})")
        print(f"📊 평균 수익률: {avg_return:.2f}%")
        print(f"📅 저장일자: {end_date}")
        print("=" * 70)

        return df_final


if __name__ == '__main__':
    dm = DualMomentumBatch()
    today = datetime.today()
    start = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    end = today.strftime('%Y-%m-%d')
    dm.run_dual_momentum_batch(start, end)
