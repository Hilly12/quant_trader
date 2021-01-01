import requests
import bs4 as bs
import pandas as pd

def split(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]

def fetch_sp500():
    try:
        response = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'wikitable sortable'})
    except Exception:
        raise Exception("Unable to fetch S&P500 companies")

    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.strip()
        tickers.append(ticker)

    return tickers