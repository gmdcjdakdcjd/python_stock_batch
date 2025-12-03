import pandas as pd
from datetime import datetime, timedelta
from API import AnalyzeUS
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_detail


class DualMomentumBatch:

    # 전략 이름
    strategy_name = "DUAL_MOMENTUM_1M_US"

    def __init__(self):

        # MongoDB MarketDB
        self.mk = AnalyzeUS.MarketDB()

        self.MIN_ABS_RETURN = 20.0   # 절대모멘텀 필터 (%)
        self.TOP_RELATIVE = 40       # 상대모멘텀 상위 40
        self.FINAL_TOP = 20          # 최종 TOP 20 선택

    # ---------------------------------------------------------
    # 가장 최근 거래일 찾기 (고속)
    # ---------------------------------------------------------
    def adjust_date(self, date_str):
        latest = self.mk.get_latest_date(date_str)
        if latest is None:
            print(f"⚠ 거래일 없음: {date_str}")
            return None
        return latest

    # ---------------------------------------------------------
    # 전체 종목 수익률 계산 (고속 버전)
    # ---------------------------------------------------------
    def calculate_returns(self, start_date, end_date):

        df_all = self.mk.get_all_daily_prices(start_date, end_date)

        if df_all.empty:
            print("⚠ 전체 가격 데이터 없음")
            return pd.DataFrame()

        # date × code pivot
        pivot = df_all.pivot(index="date", columns="code", values="close")

        start_prices = pivot.iloc[0].dropna()
        end_prices = pivot.iloc[-1].dropna()

        common_codes = start_prices.index.intersection(end_prices.index)

        result = []
        for code in common_codes:

            old = float(start_prices[code])
            new = float(end_prices[code])

            r = (new / old - 1) * 100

            name = self.mk.codes.get(code, "")

            result.append([code, name, old, new, r])

        return pd.DataFrame(
            result,
            columns=["code", "name", "old_price", "new_price", "returns"]
        )

    # ---------------------------------------------------------
    # 듀얼 모멘텀 실행
    # ---------------------------------------------------------
    def run_dual_momentum_batch(self, start_date, end_date):

        # 날짜 보정 (Mongo 기준)
        start_date = self.adjust_date(start_date)
        end_date = self.adjust_date(end_date)

        print(f"\n⚡ [DUAL MOMENTUM - 1M] ({start_date} ~ {end_date}) 실행 시작\n")

        df = self.calculate_returns(start_date, end_date)

        if df.empty:
            print("데이터 없음 → 종료")
            return pd.DataFrame()

        # 상대모멘텀 상위 40
        df_top40 = df.sort_values("returns", ascending=False).head(self.TOP_RELATIVE)

        # 절대모멘텀 필터 (>20%)
        df_abs = df_top40[df_top40["returns"] > self.MIN_ABS_RETURN]

        # 최종 TOP 20
        df_final = df_abs.sort_values("returns", ascending=False).head(self.FINAL_TOP)

        if df_final.empty:
            print("절대모멘텀 통과 없음")
            return pd.DataFrame()

        print(df_final.to_string(index=False), "\n")

        # ---------------------------------------------------------
        # 저장
        # ---------------------------------------------------------
        result_id = save_strategy_summary(
            strategy_name=self.strategy_name,
            signal_date=end_date,
            total_data=len(df_final)
        )

        for rank, row in enumerate(df_final.to_dict("records"), start=1):
            save_strategy_detail(
                result_id=result_id,
                code=row["code"],
                name=row["name"],
                action=self.strategy_name,
                price=row["new_price"],
                prev_close=row["old_price"],
                diff=row["returns"],
                volume=None,
                signal_date=end_date,
                special_value=rank
            )

        print("⚡ 저장 완료")
        print(f"RESULT_ID = {result_id}")
        print(f"ROWCOUNT  = {len(df_final)}\n")

        return df_final



# ================== 실행 ==================
if __name__ == "__main__":
    dm = DualMomentumBatch()
    today = datetime.today()
    start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    dm.run_dual_momentum_batch(start, end)
