import pandas as pd
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from matplotlib import pyplot as plt
from matplotlib import dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
from datetime import datetime

url = 'https://finance.naver.com/item/sise_day.nhn?code=035420&page=1'
req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
with urlopen(req) as doc:
    html = BeautifulSoup(doc, 'lxml')
    pgrr = html.find('td', class_='pgRR')
    s = str(pgrr.a['href']).split('=')
    last_page = s[-1]

dfs = []
sise_url = 'https://finance.naver.com/item/sise_day.nhn?code=035420'
for page in range(1, int(last_page)+1):
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
df = df.iloc[0:30]
df = df.sort_values(by='날짜')
df['날짜'] = df['날짜'].apply(lambda x: mdates.date2num(datetime.strptime(x, '%Y.%m.%d').date()))
ohlc = df[['날짜','시가','고가','저가','종가']]

plt.figure(figsize=(9, 6))
ax = plt.subplot(1, 1, 1)
plt.title('NAVER (mpl_finance candle stick)')
candlestick_ohlc(ax, ohlc.values, width=0.7, colorup='red', colordown='blue')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.xticks(rotation=45)
plt.grid(color='gray', linestyle='--')
plt.show()