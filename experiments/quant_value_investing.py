import math
import requests
import numpy as np
import pandas as pd
from statistics import mean
from scipy.stats import percentileofscore
from keys import IEX_CLOUD_API_TOKEN

PORTFOLIO_SIZE = 10000
MAX_STOCKS = 50
BATCH_SIZE = 100
url = 'https://sandbox.iexapis.com/stable/stock/{symbol}/{endpoint}/?token=' + f'{IEX_CLOUD_API_TOKEN}'
batch_url = 'https://sandbox.iexapis.com/stable/stock/market/batch?symbols={symbols}&types={endpoints}&token=' + f'{IEX_CLOUD_API_TOKEN}'

def split(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


# Load stocks from S&P 500 index
stocks = pd.read_csv('sp_500_stocks.csv')

# Initialize a dataframe for the data we will get from the API
cols = ['Ticker', 'Price', 'Shares to Buy', 'PE Ratio', 'PE Percentile',
        'PB Ratio', 'PB Percentile', 'PS Ratio', 'PS Percentile', 'EV/EBITDA', 
        'EV/EBITDA Percentile', 'EV/GP', 'EV/GP Percentile', 'RV Score']
dataset = pd.DataFrame(columns=cols)

# Get the data from the API in batches and store it in our dataframe
for i, batch in enumerate(split(stocks['Ticker'], BATCH_SIZE)):
    print(f"Fetching batch {i + 1}...")
    response = requests.get(batch_url.format(symbols=','.join(batch), endpoints='quote,advanced-stats'))
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

            dataset = dataset.append(
                pd.Series([
                    symbol,
                    data[symbol]['quote']['latestPrice'],
                    'N/A',
                    data[symbol]['quote']['peRatio'],
                    'N/A',
                    data[symbol]['advanced-stats']['priceToBook'],
                    'N/A',
                    data[symbol]['advanced-stats']['priceToSales'],
                    'N/A',
                    ev_ebitda,
                    'N/A',
                    ev_gp,
                    'N/A',
                    'N/A'
                ], index=cols),
                ignore_index=True
            )

# Replace missing data points in numerical columns with column mean
numerical_cols = ['PE Ratio', 'PB Ratio', 'PS Ratio', 'EV/EBITDA', 'EV/GP']
for col in numerical_cols:
    dataset[col].fillna(dataset[col].mean(), inplace=True)

# Drop null datapoints
dataset.dropna(inplace=True)

# Check for null datapoints
assert len(dataset[dataset.isnull().any(axis=1)]) == 0

# Compute percentiles for numeric data
metrics = {
    'PE Ratio' : 'PE Percentile',
    'PB Ratio' : 'PB Percentile',
    'PS Ratio' : 'PS Percentile', 
    'EV/EBITDA' : 'EV/EBITDA Percentile',
    'EV/GP' : 'EV/GP Percentile'
}
for val, percentile in metrics.items():
    for i in dataset.index:
        dataset.loc[i, percentile] = percentileofscore(dataset[val], dataset.loc[i, val])

# Compute RV Score
for i in dataset.index:
    percentiles = [dataset.loc[i, p] for p in metrics.values()]
    dataset.loc[i, 'RV Score'] = mean(percentiles)

# Remove rows w ith a negative pe, pb ratio
dataset = dataset[dataset['PE Ratio'] > 0]
dataset = dataset[dataset['PB Ratio'] > 0]

# Keep only the top n stocks where n = MAX_STOCKS
dataset.sort_values('RV Score', inplace=True)
dataset = dataset[:MAX_STOCKS]
dataset.reset_index(inplace=True, drop=True)

# Calculate number of shares based on portfolio size
pos_size = PORTFOLIO_SIZE / len(dataset.index)
for i in range(len(dataset)):
    dataset.loc[i, 'Shares to Buy'] = math.floor(pos_size / dataset.loc[i, 'Price'])

print(dataset)