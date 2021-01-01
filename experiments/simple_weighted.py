import math
import requests
import xlsxwriter
import numpy as np
import pandas as pd
from scipy import stats
from keys import IEX_CLOUD_API_TOKEN

PORTFOLIO_SIZE = 10000
SELECTIVITY = 0.1
BATCH_SIZE = 100
url = 'https://sandbox.iexapis.com/stable/stock/{symbol}/{endpoint}/?token=' + f'{IEX_CLOUD_API_TOKEN}'
batch_url = 'https://sandbox.iexapis.com/stable/stock/market/batch?symbols={symbols}&types={endpoints}&token=' + f'{IEX_CLOUD_API_TOKEN}'

def split(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


# Load stocks from S&P 500 index
stocks = pd.read_csv('sp_500_stocks.csv')

# Initialize a dataframe for the data we will get from the API
cols = ['Ticker', 'Price', 'One-Year Price Return', 'Number of Shares to Buy']
dataset = pd.DataFrame(columns=cols)

# Get the data from the API in batches and store it in our dataframe
for i, batch in enumerate(split(stocks['Ticker'], BATCH_SIZE)):
    print(f"Fetching batch {i + 1}...")
    response = requests.get(batch_url.format(symbols=','.join(batch), endpoints='price,stats'))
    if response.status_code == 200:
        data = response.json()
        for symbol in batch:
            dataset = dataset.append(
                pd.Series([
                    symbol,
                    data[symbol]['price'],
                    data[symbol]['stats']['year1ChangePercent'],
                    'N/A'
                ], index=cols),
                ignore_index=True
            )

# Keep only the top x% of stocks based on selectivity
dataset.sort_values('One-Year Price Return', ascending=False, inplace=True)
dataset = dataset[:int(len(dataset) * SELECTIVITY)]
dataset.reset_index(inplace=True, drop=True)

# Calculate number of shares based on portfolio size
pos_size = PORTFOLIO_SIZE / len(dataset.index)
for i in range(len(dataset)):
    dataset.loc[i, 'Number of Shares to Buy'] = math.floor(pos_size / dataset.loc[i, 'Price'])


print(dataset)