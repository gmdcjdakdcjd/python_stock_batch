import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt  # 🚫 배치 환경에서는 불필요
from API import Analyzer
from batch_code.trading.db_saver import save_strategy_summary, save_strategy_signal
from datetime import datetime

# -----------------------------
# 1. 데이터 준비
# -----------------------------
mk = Analyzer.MarketDB()
company = mk.get_comp_info_optimization()
stocks = list(company['name'])
name_to_code = {v: k for k, v in mk.codes.items()}

df = pd.DataFrame()
valid_stocks, invalid_stocks = [], []

print(f"📊 총 {len(stocks)}개 종목 데이터 불러오는 중...")

for s in stocks:
    try:
        price_data = mk.get_daily_price(s, '2024-09-19', '2025-10-02')
        # ✅ 데이터 유효성 검사 (10영업일 이상 존재해야 유효)
        if price_data is not None and not price_data.empty and len(price_data) > 10:
            df[s] = price_data['close']
            valid_stocks.append(s)
        else:
            invalid_stocks.append(s)
    except Exception:
        invalid_stocks.append(s)

print(f"✅ 유효 종목 수: {len(valid_stocks)} / ❌ 비유효 종목 수: {len(invalid_stocks)}")

# -----------------------------
# 2. 결측치 제거 및 보정
# -----------------------------
df = df.dropna(axis=1, thresh=len(df) * 0.5)
df = df.fillna(method='ffill').fillna(method='bfill')
print(f"📈 결측치 보정 후 최종 유효 종목 수: {len(df.columns)}")

# -----------------------------
# 3. 수익률 & 분산 계산
# -----------------------------
daily_ret = df.pct_change().dropna()
annual_ret = daily_ret.mean() * 252
daily_cov = daily_ret.cov()
annual_cov = daily_cov * 252

# -----------------------------
# 4. 몬테카를로 시뮬레이션
# -----------------------------
port_ret, port_risk, port_weights, sharpe_ratio = [], [], [], []
n_assets = len(df.columns)

for _ in range(20000):  # 샘플 2만 회 시뮬레이션
    weights = np.random.random(n_assets)
    weights /= np.sum(weights)

    returns = np.dot(weights, annual_ret)
    risk = np.sqrt(np.dot(weights.T, np.dot(annual_cov, weights)))
    sharpe = returns / risk if risk > 0 else np.nan

    port_ret.append(returns)
    port_risk.append(risk)
    port_weights.append(weights)
    sharpe_ratio.append(sharpe)

# -----------------------------
# 5. 포트폴리오 DataFrame 구성
# -----------------------------
portfolio = {'Returns': port_ret, 'Risk': port_risk, 'Sharpe': sharpe_ratio}
for i, s in enumerate(df.columns):
    portfolio[s] = [w[i] for w in port_weights]

df_port = pd.DataFrame(portfolio).dropna(subset=['Sharpe'])
df_port = df_port[['Returns', 'Risk', 'Sharpe'] + list(df.columns)]

# -----------------------------
# 6. 최적 포트폴리오 추출
# -----------------------------
max_sharpe = df_port.loc[df_port['Sharpe'].idxmax()]
min_risk = df_port.loc[df_port['Risk'].idxmin()]

# -----------------------------
# 🚫 7. 그래프 시각화 (배치 환경에서는 제외)
# -----------------------------
# df_port.plot.scatter(x='Risk', y='Returns', c='Sharpe', cmap='viridis',
#                      edgecolors='k', figsize=(11, 7), grid=True)
# plt.scatter(x=max_sharpe['Risk'], y=max_sharpe['Returns'], c='r', marker='*', s=300)
# plt.scatter(x=min_risk['Risk'], y=min_risk['Returns'], c='r', marker='X', s=200)
# plt.title('Portfolio Optimization')
# plt.xlabel('Risk')
# plt.ylabel('Expected Returns')
# plt.show()

# -----------------------------
# 8. 결과 DB 저장 (상위 N개만)
# -----------------------------
TOP_N = 10  # ✅ 상위 10개 종목만 DB에 저장

def save_topN_portfolio(signal_type, portfolio_row):
    """샤프비율 or 리스크 기준 포트폴리오 DB 저장"""
    result_id = save_strategy_summary(
        strategy_name='PortfolioOptimization',
        signal_date=datetime.today().strftime('%Y-%m-%d'),
        signal_type=signal_type,
        total_return=float(portfolio_row['Returns']),
        total_risk=float(portfolio_row['Risk']),
        total_sharpe=float(portfolio_row['Sharpe'])
    )

    topN_stocks = portfolio_row[df.columns].sort_values(ascending=False)[:TOP_N]

    print(f"\n💾 [{signal_type}] 상위 {TOP_N}개 종목 DB 저장 중...")
    for stock_name, weight in topN_stocks.items():
        save_strategy_signal(
            result_id=result_id,
            code=name_to_code.get(stock_name, 'UNKNOWN'),
            name=stock_name,
            action='WEIGHT',
            returns=float(weight)
        )
        print(f"   - {stock_name}: {weight:.4f}")

    print(f"✅ [{signal_type}] 저장 완료 (result_id={result_id})")


# -----------------------------
# 9. 저장 실행
# -----------------------------
save_topN_portfolio('MAX_SHARPE', max_sharpe)
save_topN_portfolio('MIN_RISK', min_risk)

print("\n✅ DB 저장 완료 (상위 10개, Max Sharpe + Min Risk)")
