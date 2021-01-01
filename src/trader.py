import requests
import numpy as np
import pandas as pd
from utils import split, fetch_sp500
from keys import IEX_CLOUD_API_TOKEN as token

PRODUCTION = False

BASE_URL = "https://cloud.iexapis.com/" if PRODUCTION else "https://sandbox.iexapis.com/"
BATCH_SIZE = 100
url = BASE_URL + 'stable/stock/{symbol}/{endpoint}/?token=' + f'{token}'
batch_url = BASE_URL + \
    'stable/stock/market/batch?symbols={symbols}&types={endpoints}&token=' + f'{token}'

tickers = fetch_sp500()

cols = ['Ticker', 'Price', 'PE Ratio', 'PB Ratio', 'PS Ratio',
        'EV/EBITDA', 'EV/GP', 'RV Score']
df = pd.DataFrame(columns=cols)

for i, batch in enumerate(split(tickers, BATCH_SIZE)):
    print(f"Fetching batch {i + 1}...")
    response = requests.get(batch_url.format(symbols=','.join(batch),
                                             endpoints='quote,advanced-stats'))
    if response.status_code == 200:
        data = response.json()
        for symbol in batch:
            ev = data[symbol]['advanced-stats']['enterpriseValue']
            ebitda = data[symbol]['advanced-stats']['EBITDA']
            gp = data[symbol]['advanced-stats']['grossProfit']
            try:
                ev_ebitda = ev / ebitda
                ev_gp = ev / gp
            except TypeError:
                ev_ebitda = np.NaN
                ev_gp = np.NaN

            df = df.append(
                pd.Series([
                    symbol,
                    data[symbol]['quote']['latestPrice'],
                    data[symbol]['quote']['peRatio'],
                    data[symbol]['advanced-stats']['priceToBook'],
                    data[symbol]['advanced-stats']['priceToSales'],
                    ev_ebitda,
                    ev_gp,
                    'N/A'
                ], index=cols),
                ignore_index=True
            )
