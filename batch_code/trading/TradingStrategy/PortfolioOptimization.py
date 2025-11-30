import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from API import AnalyzeKR

# -----------------------------
# 1. 데이터 준비: 종목별 가격 데이터 수집
# -----------------------------
mk = Analyzer.MarketDB()
stocks = ['삼성전자', 'SK하이닉스', '현대차', 'NAVER']
df = pd.DataFrame()

for s in stocks:
    price_data = mk.get_daily_price(s, '2023-01-04', '2025-04-27')  # 각 종목의 기간별 종가 데이터
    if price_data is not None and not price_data.empty:
        df[s] = price_data['close']  # 종가만 추출하여 컬럼별로 저장

# -----------------------------
# 2. 수익률 및 분산 계산
# -----------------------------
daily_ret = df.pct_change().dropna()  # 일별 수익률 계산
annual_ret = daily_ret.mean() * 252   # 연간 기대수익률 (거래일 기준)
annual_cov = daily_ret.cov() * 252    # 연간 공분산 행렬

# -----------------------------
# 3. 몬테카를로 시뮬레이션: 다양한 포트폴리오 무작위 생성
# -----------------------------
port_ret, port_risk, sharpe_ratio, port_weights = [], [], [], []
n_assets = len(stocks)

for _ in range(20000):
    w = np.random.random(n_assets)      # 무작위 비중 생성
    w /= np.sum(w)                     # 비중 합 1로 정규화

    ret = np.dot(w, annual_ret)        # 기대수익률
    risk = np.sqrt(np.dot(w.T, np.dot(annual_cov, w)))  # 기대 리스크(표준편차)
    sharpe = ret / risk                # 샤프지수 계산

    port_ret.append(ret)
    port_risk.append(risk)
    sharpe_ratio.append(sharpe)
    port_weights.append(w)

# -----------------------------
# 4. 결과 DataFrame 구성: 각 포트폴리오의 성과 및 비중
# -----------------------------
portfolio = {'RETURNS': port_ret, 'RISK': port_risk, 'SHARPE': sharpe_ratio}
for i, s in enumerate(stocks):
    portfolio[s] = [w[i] for w in port_weights]
df_port = pd.DataFrame(portfolio)

# -----------------------------
# 5. 최적 포트폴리오 추출: 최대 샤프/최소 리스크
# -----------------------------
max_sharpe = df_port.loc[df_port['SHARPE'].idxmax()]  # 샤프지수 최대
min_risk = df_port.loc[df_port['RISK'].idxmin()]      # 리스크 최소

# -----------------------------
# 6. 콘솔 출력 (KEY-VALUE 형식)
# -----------------------------
def print_portfolio(label, row):
    print(f"\n[{label}]")
    print(f"RETURNS: {row['RETURNS']:.4f}")
    print(f"RISK: {row['RISK']:.4f}")
    print(f"SHARPE: {row['SHARPE']:.4f}")
    for s in stocks:
        print(f"{s}: {row[s]:.4f}")

print_portfolio("최대 샤프 포트폴리오 (Max Sharpe)", max_sharpe)
print_portfolio("최소 리스크 포트폴리오 (Min Risk)", min_risk)
