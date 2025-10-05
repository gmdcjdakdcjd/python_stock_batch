import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from API import ETFAnalyzer

# -----------------------------
# 1. 데이터 준비
# -----------------------------
mk = ETFAnalyzer.MarketDB()
stocks = ['KODEX 반도체레버리지', 'TIGER 반도체TOP10레버리지', 'KODEX 차이나심천ChiNext(합성)', 'TIGER 한중반도체(합성)']
df = pd.DataFrame()

for s in stocks:
    price_data = mk.get_daily_price(s, '2023-01-04', '2025-04-27')
    if price_data is not None and not price_data.empty:
        df[s] = price_data['close']

# -----------------------------
# 2. 수익률 및 분산 계산
# -----------------------------
daily_ret = df.pct_change().dropna()
annual_ret = daily_ret.mean() * 252
annual_cov = daily_ret.cov() * 252

# -----------------------------
# 3. 몬테카를로 시뮬레이션
# -----------------------------
port_ret, port_risk, sharpe_ratio, port_weights = [], [], [], []
n_assets = len(stocks)

for _ in range(20000):
    w = np.random.random(n_assets)
    w /= np.sum(w)

    ret = np.dot(w, annual_ret)
    risk = np.sqrt(np.dot(w.T, np.dot(annual_cov, w)))
    sharpe = ret / risk

    port_ret.append(ret)
    port_risk.append(risk)
    sharpe_ratio.append(sharpe)
    port_weights.append(w)

# -----------------------------
# 4. 결과 DataFrame 구성
# -----------------------------
portfolio = {'RETURNS': port_ret, 'RISK': port_risk, 'SHARPE': sharpe_ratio}
for i, s in enumerate(stocks):
    portfolio[s] = [w[i] for w in port_weights]
df_port = pd.DataFrame(portfolio)

# -----------------------------
# 5. 최적 포트폴리오 추출
# -----------------------------
max_sharpe = df_port.loc[df_port['SHARPE'].idxmax()]
min_risk = df_port.loc[df_port['RISK'].idxmin()]

# -----------------------------
# 6. 콘솔 출력 (KEY-VALUE 형식)
# -----------------------------
def print_portfolio(label, row):
    print(f"\n📊 [{label}]")
    print(f"RETURNS: {row['RETURNS']:.4f}")
    print(f"RISK: {row['RISK']:.4f}")
    print(f"SHARPE: {row['SHARPE']:.4f}")
    for s in stocks:
        print(f"{s}: {row[s]:.4f}")

print_portfolio("최대 샤프 포트폴리오 (Max Sharpe)", max_sharpe)
print_portfolio("최소 리스크 포트폴리오 (Min Risk)", min_risk)

