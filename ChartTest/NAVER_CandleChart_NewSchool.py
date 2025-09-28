import pandas as pd
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import mplfinance as mpf

url = 'https://finance.naver.com/item/sise_day.nhn?code=035420&page=1'
req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
with urlopen(req) as doc:
    html = BeautifulSoup(doc, 'lxml')
    pgrr = html.find('td', class_='pgRR')
    s = str(pgrr.a['href']).split('=')
    last_page = int(s[-1])

dfs = []
sise_url = 'https://finance.naver.com/item/sise_day.nhn?code=035420'
# 최근 3페이지만 읽음 (필요시 5로 늘릴 수 있음)
for page in range(1, 4):
    page_url = f'{sise_url}&page={page}'
    req = Request(page_url, headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req) as response:
        html = response.read()
        try:
            df = pd.read_html(html, header=0)[0]
            dfs.append(df)
        except ValueError:
            continue

df = pd.concat(dfs, ignore_index=True)
df = df.dropna()
df = df.rename(columns={'날짜':'Date', '시가':'Open', '고가':'High', '저가':'Low', '종가':'Close', '거래량':'Volume'})
df = df.sort_values(by='Date')
df.index = pd.to_datetime(df.Date)
df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
df = df.tail(30)  # 최근 30개만 사용

mpf.plot(df, title='NAVER candle chart', type='candle')

mpf.plot(df, title='NAVER ohlc chart', type='ohlc')

kwargs = dict(title='NAVER customized chart', type='candle',
    mav=(2, 4, 6), volume=True, ylabel='ohlc candles')
mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
s  = mpf.make_mpf_style(marketcolors=mc)
mpf.plot(df, **kwargs, style=s)