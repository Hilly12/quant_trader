import requests

url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/v2/get-quotes"

querystring = {"region":"US","symbols":"AMD,IBM,AAPL"}

headers = {
    'x-rapidapi-key': "a2e723dde3msh3dffe12e283852bp1e2b69jsna1a9276be081",
    'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)