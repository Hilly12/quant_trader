import math
import requests
import numpy as np
import pandas as pd
from scipy.stats import percentileofscore
from statistics import mean
from keys import IEX_CLOUD_API_TOKEN

PORTFOLIO_SIZE = 10000
SELECTIVITY = 0.02
BATCH_SIZE = 100
url = 'https://sandbox.iexapis.com/stable/stock/{symbol}/{endpoint}/?token=' + f'{IEX_CLOUD_API_TOKEN}'
batch_url = 'https://sandbox.iexapis.com/stable/stock/market/batch?symbols={symbols}&types={endpoints}&token=' + f'{IEX_CLOUD_API_TOKEN}'

def split(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


# Load stocks from S&P 500 index
stocks = pd.read_csv('sp_500_stocks.csv')

# Initialize a dataframe for the data we will get from the API
cols = ['Ticker', 'Price', 'Number of Shares to Buy', '1Y Price Return',
        '1Y Return Percentile', '6M Price Return', '6M Return Percentile',
        '3M Price Return', '3M Return Percentile', '1M Price Return',
        '1M Return Percentile', 'HQM Score']
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
                    data[symbol]['price'] or 0,
                    'N/A',
                    data[symbol]['stats']['year1ChangePercent'] or 0,
                    'N/A', 
                    data[symbol]['stats']['month6ChangePercent'] or 0,
                    'N/A',
                    data[symbol]['stats']['month3ChangePercent'] or 0,
                    'N/A',
                    data[symbol]['stats']['month1ChangePercent'] or 0,
                    'N/A',
                    'N/A'
                ], index=cols),
                ignore_index=True
            )

# Compute the return percentile relative to all other stocks, for each time period
time_periods = ['1Y', '6M', '3M', '1M']
for i in dataset.index:
    for tp in time_periods:
        change_col = f"{tp} Price Return"
        percentile_col = f"{tp} Return Percentile"
        dataset.loc[i, percentile_col] = percentileofscore(dataset[change_col], dataset.loc[i, change_col])

# Compute the HQM Score
for i in dataset.index:
    dataset.loc[i, 'HQM Score'] = mean([dataset.loc[i, f"{tp} Return Percentile"] for tp in time_periods])

# Keep only the top x% of stocks based on selectivity
dataset.sort_values('HQM Score', ascending=False, inplace=True)
dataset = dataset[:int(len(dataset) * SELECTIVITY)]
dataset.reset_index(inplace=True, drop=True)

# Calculate number of shares based on portfolio size
pos_size = PORTFOLIO_SIZE / len(dataset.index)
for i in range(len(dataset)):
    dataset.loc[i, 'Number of Shares to Buy'] = math.floor(pos_size / dataset.loc[i, 'Price'])

print(dataset)